//! Runs of our own on object storage: the store under the memory tier.
//!
//! At each barrier a lane writes its epoch's changes as one run, an object
//! of its range of key groups named by generation, epoch and range, and
//! never overwritten. A run has a section per kind of state, each with its
//! own index and filters. The lane's snapshot names every
//! section it reads, run by run ([`SectionRef`]): a key group's sections of
//! a kind are read newest first, and the first that holds a key, or its
//! delete, answers. Nothing else is a commit point: the record names what
//! counts.
//!
//! A restore reads nothing at once and writes nothing. A lane takes, of
//! the snapshots a record kept, the sections that hold its key groups, and
//! opens a run, its sections' indexes and filters in one read, when a read
//! first needs it, keeping only its own groups' filters. Blocks are
//! cached, by a bound of bytes all the lanes of a [`RunStores`] share, and
//! the blocks a batch of reads needs from a run are read in as few
//! requests as their gaps allow.
//!
//! This step neither merges nor drops sections: a lane's snapshot names
//! every run it or the lanes before it wrote. Runs that no kept record
//! names, from a barrier never recorded or an older attempt, are orphans
//! for retention.

use std::collections::{BTreeMap, HashMap};
use std::ops::{Add, Range, Sub};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions};

use crate::layout::{self, SectionKind};
use crate::store::{BoxFuture, Change, Snapshot, Store, Stores};
use crate::{Error, KeyGroupRange, LAYOUT, Result};

mod cache;
mod codec;
mod filter;
mod format;
mod handle;

use cache::{BlockCache, BlockId};
pub use format::FORMAT;
use format::{BlockMeta, OpenSection};
pub use handle::{RunName, RunRef, SectionRef, encode, named_runs, runs};

/// How runs are written and read. Every lane of a [`RunStores`] shares it.
#[derive(Clone, Debug)]
pub struct RunConfig {
    /// A block's entries, uncompressed, about: what a lookup reads.
    pub block_bytes: usize,
    /// Blocks are compressed with LZ4 where it makes them smaller.
    pub compress: bool,
    /// Bits a key in each kind's filters, by [`SectionKind`] in
    /// [`SectionKind::ALL`]'s order; 0 for none. A row's filter holds its
    /// key and time, so a read of a key's rows at a time asks it.
    pub bits: [u8; 4],
    /// The block cache's bound, for all of its lanes.
    pub cache_bytes: usize,
    /// Blocks of a run this close are read in one request.
    pub gap_bytes: u64,
    /// The most one request reads, unless a block is larger.
    pub max_read_bytes: u64,
    /// Requests in flight at once, in one call.
    pub concurrency: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            block_bytes: 4096,
            compress: true,
            bits: [10, 10, 20, 10],
            cache_bytes: 64 << 20,
            gap_bytes: 64 << 10,
            max_read_bytes: 8 << 20,
            concurrency: 32,
        }
    }
}

impl RunConfig {
    pub fn bits(&self, kind: SectionKind) -> u8 {
        self.bits[kind.index()]
    }
}

/// Requests of a kind, and their bytes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Requests {
    pub count: u64,
    pub bytes: u64,
}

/// What a [`RunStores`]' lanes asked of storage, and why lookups reached
/// it, since it was made.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counts {
    /// Runs written at barriers.
    pub barrier: Requests,
    /// Reads of blocks, for lookups and scans.
    pub lookup: Requests,
    /// Reads of runs' indexes and filters, opening what a record named.
    pub restore: Requests,
    /// Keys' and values' bytes that barriers wrote: the changes, before the
    /// format's overhead and compression.
    pub changed_bytes: u64,
    /// Probes of a section for a key that its filter or index ruled out.
    pub filtered: u64,
    /// Probes past the filter that a cached block answered.
    pub cached: u64,
    /// Probes past the filter that read a block, which held the key.
    pub hits: u64,
    /// Probes past the filter that read a block, which didn't: the
    /// filter's false positives that reached storage.
    pub false_positives: u64,
}

impl Add for Requests {
    type Output = Requests;
    fn add(self, o: Requests) -> Requests {
        Requests {
            count: self.count + o.count,
            bytes: self.bytes + o.bytes,
        }
    }
}

impl Add for Counts {
    type Output = Counts;
    fn add(self, o: Counts) -> Counts {
        Counts {
            barrier: self.barrier + o.barrier,
            lookup: self.lookup + o.lookup,
            restore: self.restore + o.restore,
            changed_bytes: self.changed_bytes + o.changed_bytes,
            filtered: self.filtered + o.filtered,
            cached: self.cached + o.cached,
            hits: self.hits + o.hits,
            false_positives: self.false_positives + o.false_positives,
        }
    }
}

impl Sub for Requests {
    type Output = Requests;
    fn sub(self, o: Requests) -> Requests {
        Requests {
            count: self.count.saturating_sub(o.count),
            bytes: self.bytes.saturating_sub(o.bytes),
        }
    }
}

impl Sub for Counts {
    type Output = Counts;
    fn sub(self, o: Counts) -> Counts {
        Counts {
            barrier: self.barrier - o.barrier,
            lookup: self.lookup - o.lookup,
            restore: self.restore - o.restore,
            changed_bytes: self.changed_bytes.saturating_sub(o.changed_bytes),
            filtered: self.filtered.saturating_sub(o.filtered),
            cached: self.cached.saturating_sub(o.cached),
            hits: self.hits.saturating_sub(o.hits),
            false_positives: self.false_positives.saturating_sub(o.false_positives),
        }
    }
}

#[derive(Default)]
struct Counters {
    barrier: (AtomicU64, AtomicU64),
    lookup: (AtomicU64, AtomicU64),
    restore: (AtomicU64, AtomicU64),
    changed_bytes: AtomicU64,
    filtered: AtomicU64,
    cached: AtomicU64,
    hits: AtomicU64,
    false_positives: AtomicU64,
}

fn add(a: &AtomicU64, n: u64) {
    a.fetch_add(n, Ordering::Relaxed);
}

fn request(r: &(AtomicU64, AtomicU64), bytes: usize) {
    add(&r.0, 1);
    add(&r.1, bytes as u64);
}

impl Counters {
    fn counts(&self) -> Counts {
        let l = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let r = |r: &(AtomicU64, AtomicU64)| Requests {
            count: l(&r.0),
            bytes: l(&r.1),
        };
        Counts {
            barrier: r(&self.barrier),
            lookup: r(&self.lookup),
            restore: r(&self.restore),
            changed_bytes: l(&self.changed_bytes),
            filtered: l(&self.filtered),
            cached: l(&self.cached),
            hits: l(&self.hits),
            false_positives: l(&self.false_positives),
        }
    }
}

/// Where runs are kept: object storage, or a stand-in in a test. Names are
/// a run's, relative to wherever the storage keeps them.
pub trait Objects: Send + Sync {
    /// Writes `name`, and refuses with [`Error::Store`] if it exists.
    fn put_new<'a>(&'a self, name: &'a str, data: Bytes) -> BoxFuture<'a, Result<()>>;

    /// Bytes `range` of `name`, all of them.
    fn get_range<'a>(&'a self, name: &'a str, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>>;
}

/// Runs in an object store, under a prefix.
#[derive(Clone, Debug)]
pub struct ObjectStorage {
    store: Arc<dyn ObjectStore>,
    prefix: Path,
}

impl ObjectStorage {
    pub fn new(store: Arc<dyn ObjectStore>, prefix: Path) -> Self {
        Self { store, prefix }
    }

    fn path(&self, name: &str) -> Path {
        name.split('/')
            .fold(self.prefix.clone(), |p, part| p.join(part))
    }
}

impl Objects for ObjectStorage {
    fn put_new<'a>(&'a self, name: &'a str, data: Bytes) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let opts = PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            };
            match self
                .store
                .put_opts(&self.path(name), data.into(), opts)
                .await
            {
                Ok(_) => Ok(()),
                Err(object_store::Error::AlreadyExists { .. }) => {
                    Err(Error::Store(format!("run {name} exists")))
                }
                Err(e) => Err(Error::Store(format!("writing run {name}: {e}"))),
            }
        })
    }

    fn get_range<'a>(&'a self, name: &'a str, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            let want = range.end - range.start;
            let opts = GetOptions::new().with_range(Some(range));
            let read = async {
                self.store
                    .get_opts(&self.path(name), opts)
                    .await?
                    .bytes()
                    .await
            };
            let b = read
                .await
                .map_err(|e| Error::Store(format!("reading run {name}: {e}")))?;
            match b.len() as u64 == want {
                true => Ok(b),
                false => Err(Error::Corrupt(format!(
                    "run {name}: {} bytes where {want} were asked for",
                    b.len()
                ))),
            }
        })
    }
}

struct Shared {
    objects: Arc<dyn Objects>,
    config: RunConfig,
    counters: Counters,
    cache: Mutex<BlockCache>,
    /// The next run a lane opens or writes: its blocks' key in the cache.
    next_id: AtomicU64,
}

impl Shared {
    fn id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Stores of runs, for the lanes of a job or a worker: they share its
/// config, its block cache and its counts.
#[derive(Clone)]
pub struct RunStores {
    shared: Arc<Shared>,
}

impl RunStores {
    pub fn new(objects: Arc<dyn Objects>, config: RunConfig) -> Self {
        let cache = Mutex::new(BlockCache::new(config.cache_bytes));
        Self {
            shared: Arc::new(Shared {
                objects,
                config,
                counters: Counters::default(),
                cache,
                next_id: AtomicU64::new(0),
            }),
        }
    }

    pub fn counts(&self) -> Counts {
        self.shared.counters.counts()
    }

    /// The block cache's bytes.
    pub fn cache_bytes(&self) -> usize {
        lock(&self.shared.cache).bytes()
    }

    fn open_store(
        &self,
        generation: u64,
        range: KeyGroupRange,
        snapshots: &[Snapshot],
    ) -> Result<RunStore> {
        for (i, a) in snapshots.iter().enumerate() {
            if let Some(b) = snapshots[i + 1..]
                .iter()
                .find(|b| a.range.overlaps(&b.range))
            {
                return Err(Error::Snapshot(format!(
                    "snapshots of key groups {} to {} and {} to {} overlap",
                    a.range.first, a.range.last, b.range.first, b.range.last
                )));
            }
        }
        if !snapshots.is_empty()
            && let Some(g) =
                (range.first..=range.last).find(|g| !snapshots.iter().any(|s| s.range.contains(*g)))
        {
            return Err(Error::Snapshot(format!("no snapshot holds key group {g}")));
        }
        let mut runs: BTreeMap<String, RunBuilder> = BTreeMap::new();
        for s in snapshots.iter().filter(|s| s.range.overlaps(&range)) {
            for r in handle::runs(s)? {
                if r.format != FORMAT {
                    return Err(Error::Snapshot(format!(
                        "run {} is of format {}, where this engine reads {FORMAT}",
                        r.name, r.format
                    )));
                }
                let b = runs.entry(r.name.clone()).or_insert_with(|| RunBuilder {
                    name: r.name.clone(),
                    size: r.size,
                    named: Vec::new(),
                    sections: Vec::new(),
                });
                if b.size != r.size {
                    return Err(Error::Snapshot(format!(
                        "run {} named with sizes {} and {}",
                        r.name, b.size, r.size
                    )));
                }
                for sec in r.sections {
                    let Some(groups) =
                        intersect(sec.groups, s.range).and_then(|g| intersect(g, range))
                    else {
                        continue;
                    };
                    match b.named.iter().find(|n| n.kind == sec.kind) {
                        Some(n) if n.meta != sec.meta || n.epoch != sec.epoch => {
                            return Err(Error::Snapshot(format!(
                                "run {} named with two {:?} sections",
                                r.name, sec.kind
                            )));
                        }
                        Some(_) => {}
                        None => b.named.push(sec.clone()),
                    }
                    b.sections.push(SectionRef { groups, ..sec });
                }
            }
        }
        let mut store = RunStore {
            shared: self.shared.clone(),
            generation,
            range,
            sections: Default::default(),
            runs: Vec::new(),
            unopened: AtomicUsize::new(0),
        };
        for (_, b) in runs {
            if b.sections.is_empty() {
                continue;
            }
            let mut keep: [Vec<KeyGroupRange>; 4] = Default::default();
            for s in &b.sections {
                keep[s.kind.index()].push(s.groups);
            }
            let run = Arc::new(Run {
                id: self.shared.id(),
                name: b.name,
                size: b.size,
                named: b.named,
                keep,
                open: OnceLock::new(),
            });
            for s in b.sections {
                store.sections[s.kind.index()].push(Sec {
                    run: run.clone(),
                    handle: s,
                });
            }
            store.runs.push(run);
        }
        store.unopened.store(store.runs.len(), Ordering::Relaxed);
        for secs in &mut store.sections {
            secs.sort_by(|a, b| b.handle.epoch.cmp(&a.handle.epoch));
            for (i, a) in secs.iter().enumerate() {
                let tie = secs[i + 1..]
                    .iter()
                    .take_while(|b| b.handle.epoch == a.handle.epoch)
                    .find(|b| b.handle.groups.overlaps(&a.handle.groups));
                if let Some(b) = tie {
                    return Err(Error::Snapshot(format!(
                        "runs {} and {} both hold epoch {}'s {:?} for key groups {} to {}",
                        a.run.name,
                        b.run.name,
                        a.handle.epoch,
                        a.handle.kind,
                        b.handle.groups.first,
                        b.handle.groups.last
                    )));
                }
            }
        }
        Ok(store)
    }
}

impl Stores for RunStores {
    fn open<'a>(
        &'a self,
        generation: u64,
        range: KeyGroupRange,
        snapshots: &'a [Snapshot],
    ) -> BoxFuture<'a, Result<Box<dyn Store>>> {
        let store = self
            .open_store(generation, range, snapshots)
            .map(|s| Box::new(s) as Box<dyn Store>);
        Box::pin(std::future::ready(store))
    }
}

fn intersect(a: KeyGroupRange, b: KeyGroupRange) -> Option<KeyGroupRange> {
    let r = KeyGroupRange::new(a.first.max(b.first), a.last.min(b.last));
    (r.first <= r.last).then_some(r)
}

struct RunBuilder {
    name: String,
    size: u64,
    /// Each named section once, as the record has it.
    named: Vec<SectionRef>,
    /// Each section as this lane reads it.
    sections: Vec<SectionRef>,
}

/// A run some section of which the lane reads.
struct Run {
    id: u64,
    name: String,
    size: u64,
    /// Its sections the lane reads, as the record names them.
    named: Vec<SectionRef>,
    /// The key groups the lane reads each kind of section for.
    keep: [Vec<KeyGroupRange>; 4],
    open: OnceLock<Opened>,
}

struct Opened {
    sections: [Option<OpenSection>; 4],
    bytes: usize,
}

impl Run {
    fn section(&self, kind: SectionKind) -> Result<&OpenSection> {
        self.open
            .get()
            .and_then(|o| o.sections[kind.index()].as_ref())
            .ok_or_else(|| Error::Store(format!("run {} read before it was opened", self.name)))
    }
}

/// A section as a lane reads it.
struct Sec {
    run: Arc<Run>,
    /// What the lane's snapshot names: its groups are the lane's.
    handle: SectionRef,
}

impl Sec {
    fn counts_for(&self, group: u16) -> bool {
        self.handle.groups.contains(group.into())
    }
}

/// A lane's runs.
struct RunStore {
    shared: Arc<Shared>,
    generation: u64,
    range: KeyGroupRange,
    /// Each kind's sections, newest first.
    sections: [Vec<Sec>; 4],
    runs: Vec<Arc<Run>>,
    /// Runs not opened yet.
    unopened: AtomicUsize,
}

/// A key a lookup asks for, and how far down its sections it got.
struct Ask {
    at: usize,
    kind: SectionKind,
    group: u16,
    hash: u64,
    next: usize,
}

/// A section a scan reads, and its blocks in the range.
struct Cursor<'s> {
    sec: &'s Sec,
    open: &'s OpenSection,
    next: usize,
    end: usize,
    /// The last key of the last block read.
    read_to: Option<Bytes>,
}

impl RunStore {
    fn counters(&self) -> &Counters {
        &self.shared.counters
    }

    /// The runs of `secs` not opened yet, each once.
    fn unopened<'a>(&self, secs: impl Iterator<Item = &'a Sec>) -> Vec<&'a Run> {
        if self.unopened.load(Ordering::Relaxed) == 0 {
            return Vec::new();
        }
        let mut need: Vec<&Run> = secs
            .map(|s| s.run.as_ref())
            .filter(|r| r.open.get().is_none())
            .collect();
        need.sort_by_key(|r| r.id);
        need.dedup_by_key(|r| r.id);
        need
    }

    /// Opens `runs`, each in one read.
    async fn open_runs(&self, runs: Vec<&Run>) -> Result<()> {
        if runs.is_empty() {
            return Ok(());
        }
        let opens: Vec<_> = runs.into_iter().map(|r| self.open_run(r)).collect();
        futures_util::stream::iter(opens)
            .buffer_unordered(self.shared.config.concurrency.max(1))
            .try_collect::<Vec<()>>()
            .await?;
        Ok(())
    }

    async fn open_run(&self, run: &Run) -> Result<()> {
        let start = run.named.iter().map(|s| s.meta.0).min().unwrap_or(run.size);
        if start >= run.size {
            return Err(Error::Snapshot(format!(
                "run {} named past its end",
                run.name
            )));
        }
        let tail = self
            .shared
            .objects
            .get_range(&run.name, start..run.size)
            .await?;
        request(&self.counters().restore, tail.len());
        let listed = format::footer(&tail)?;
        let mut sections: [Option<OpenSection>; 4] = Default::default();
        let mut bytes = 0;
        for named in &run.named {
            let (offset, len) = named.meta;
            if !listed.contains(&(named.kind, offset, len)) {
                return Err(Error::Corrupt(format!(
                    "run {} has no {:?} section where its snapshot says",
                    run.name, named.kind
                )));
            }
            let at = usize::try_from(offset - start).unwrap_or(usize::MAX);
            let chunk = usize::try_from(len)
                .ok()
                .and_then(|len| tail.get(at..at.checked_add(len)?))
                .ok_or_else(|| Error::Corrupt(format!("run {}: a section's meta", run.name)))?;
            let (info, open) = format::open_section(chunk, &run.keep[named.kind.index()])?;
            if info.kind != named.kind || info.entries != named.entries {
                return Err(Error::Corrupt(format!(
                    "run {}'s {:?} section isn't what its snapshot says",
                    run.name, named.kind
                )));
            }
            bytes += open.memory_bytes();
            sections[named.kind.index()] = Some(open);
        }
        if run.open.set(Opened { sections, bytes }).is_ok() {
            self.unopened.fetch_sub(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// The blocks `want` names, each once, from the cache or read: a run's
    /// that are near each other in one request. Each comes with whether it
    /// was read.
    async fn blocks(
        &self,
        want: Vec<(&Arc<Run>, &BlockMeta)>,
    ) -> Result<HashMap<BlockId, (Bytes, bool)>> {
        let mut out = HashMap::new();
        let mut misses: BTreeMap<u64, (&Arc<Run>, Vec<&BlockMeta>)> = BTreeMap::new();
        {
            let mut cache = lock(&self.shared.cache);
            for (run, b) in want {
                let id = (run.id, b.offset);
                if out.contains_key(&id) {
                    continue;
                }
                match cache.get(id) {
                    Some(hit) => {
                        out.insert(id, (hit, false));
                    }
                    None => misses.entry(run.id).or_insert((run, Vec::new())).1.push(b),
                }
            }
        }
        let config = &self.shared.config;
        let mut reads: Vec<(&Arc<Run>, Range<u64>, Vec<&BlockMeta>)> = Vec::new();
        for (_, (run, mut blocks)) in misses {
            blocks.sort_by_key(|b| b.offset);
            blocks.dedup_by_key(|b| b.offset);
            for b in blocks {
                match reads.last_mut() {
                    Some((r, span, bs))
                        if r.id == run.id
                            && b.offset <= span.end + config.gap_bytes
                            && b.end() - span.start <= config.max_read_bytes =>
                    {
                        span.end = span.end.max(b.end());
                        bs.push(b);
                    }
                    _ => reads.push((run, b.offset..b.end(), vec![b])),
                }
            }
        }
        let reads: Vec<_> = reads
            .into_iter()
            .map(|(run, span, bs)| self.read_blocks(run, span, bs))
            .collect();
        let read = futures_util::stream::iter(reads)
            .buffer_unordered(config.concurrency.max(1))
            .try_collect::<Vec<_>>()
            .await?;
        let mut cache = lock(&self.shared.cache);
        for (id, block) in read.into_iter().flatten() {
            cache.put(id, block.clone());
            out.insert(id, (block, true));
        }
        Ok(out)
    }

    /// The blocks `bs` of `run`, all within `span`, in one request.
    async fn read_blocks(
        &self,
        run: &Run,
        span: Range<u64>,
        bs: Vec<&BlockMeta>,
    ) -> Result<Vec<(BlockId, Bytes)>> {
        let data = self
            .shared
            .objects
            .get_range(&run.name, span.clone())
            .await?;
        request(&self.counters().lookup, data.len());
        let mut got = Vec::with_capacity(bs.len());
        for b in bs {
            let at = usize::try_from(b.offset - span.start).unwrap_or(usize::MAX);
            let stored = data
                .get(at..at.saturating_add(b.stored as usize))
                .ok_or_else(|| Error::Corrupt(format!("run {}: a block", run.name)))?;
            let block = format::decode_block(stored, b.raw).map_err(|e| match e {
                Error::Corrupt(m) => Error::Corrupt(format!("run {}: {m}", run.name)),
                e => e,
            })?;
            got.push(((run.id, b.offset), block));
        }
        Ok(got)
    }

    async fn lookup(&self, keys: &[Bytes]) -> Result<Vec<Option<Bytes>>> {
        let mut out = vec![None; keys.len()];
        let mut asks = Vec::new();
        for (at, k) in keys.iter().enumerate() {
            let (Some(kind), Some(group)) = (layout::section_of(k), layout::group_of(k)) else {
                continue;
            };
            if self.range.contains(group.into()) {
                let hash = filter::hash(layout::filtered(k));
                asks.push(Ask {
                    at,
                    kind,
                    group,
                    hash,
                    next: 0,
                });
            }
        }
        let mut secs = Vec::new();
        for a in &asks {
            secs.extend(
                self.sections[a.kind.index()]
                    .iter()
                    .filter(|s| s.counts_for(a.group)),
            );
        }
        let need = self.unopened(secs.into_iter());
        self.open_runs(need).await?;
        let (mut filtered, mut cached, mut hits, mut misses) = (0, 0, 0, 0);
        while !asks.is_empty() {
            let mut probes = Vec::new();
            for mut a in asks {
                let secs = &self.sections[a.kind.index()];
                while let Some(s) = secs.get(a.next) {
                    a.next += 1;
                    if !s.counts_for(a.group) {
                        continue;
                    }
                    let open = s.run.section(a.kind)?;
                    let block = match open.may_contain(a.group, a.hash) {
                        true => open.block_for(&keys[a.at]),
                        false => None,
                    };
                    match block {
                        Some(b) => {
                            probes.push((a, &s.run, &open.blocks[b]));
                            break;
                        }
                        None => filtered += 1,
                    }
                }
            }
            if probes.is_empty() {
                break;
            }
            let want = probes.iter().map(|(_, run, b)| (*run, *b)).collect();
            let got = self.blocks(want).await?;
            asks = Vec::new();
            for (a, run, b) in probes {
                let (block, read) = got
                    .get(&(run.id, b.offset))
                    .ok_or_else(|| Error::Store("a block asked for and not read".into()))?;
                let found = format::find(block, &keys[a.at])?;
                match (found.is_some(), read) {
                    (_, false) => cached += 1,
                    (true, true) => hits += 1,
                    (false, true) => misses += 1,
                }
                match found {
                    Some(v) => out[a.at] = v,
                    None => asks.push(a),
                }
            }
        }
        let c = self.counters();
        add(&c.filtered, filtered);
        add(&c.cached, cached);
        add(&c.hits, hits);
        add(&c.false_positives, misses);
        Ok(out)
    }

    async fn scan_range(
        &self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Bytes, Bytes)>> {
        let mut out = Vec::new();
        if limit == 0 || start >= end {
            return Ok(out);
        }
        let kinds = layout::sections_between(start, end);
        let span = match (layout::keyed_group(start), layout::keyed_group(end)) {
            (Some(a), Some(b)) => {
                match intersect(KeyGroupRange::new(a.into(), b.into()), self.range) {
                    Some(span) => span,
                    None => return Ok(out),
                }
            }
            _ => self.range,
        };
        let candidates: Vec<&Sec> = SectionKind::ALL
            .into_iter()
            .filter(|k| kinds[k.index()])
            .flat_map(|k| self.sections[k.index()].iter())
            .filter(|s| s.handle.groups.overlaps(&span))
            .collect();
        let need = self.unopened(candidates.iter().copied());
        self.open_runs(need).await?;
        let slot = match layout::is_slot(start, end) {
            true => layout::group_of(start).map(|g| (g, filter::hash(start))),
            false => None,
        };
        let mut cursors = Vec::new();
        for sec in candidates {
            let open = sec.run.section(sec.handle.kind)?;
            if let Some((g, h)) = slot
                && !(sec.counts_for(g) && open.may_contain(g, h))
            {
                add(&self.counters().filtered, 1);
                continue;
            }
            let blocks = open.blocks_between(start, end);
            if !blocks.is_empty() {
                cursors.push(Cursor {
                    sec,
                    open,
                    next: blocks.start,
                    end: blocks.end,
                    read_to: None,
                });
            }
        }
        // Each key's newest version so far: its section's epoch, and its
        // value or delete.
        let mut merged: BTreeMap<Bytes, (u64, Option<Bytes>)> = BTreeMap::new();
        loop {
            let upto = bound(&cursors);
            let need = limit - out.len();
            let mut want = Vec::new();
            for (i, c) in cursors.iter().enumerate() {
                let behind = match (&c.read_to, &upto) {
                    (None, _) => true,
                    (Some(r), Some(b)) => r <= b,
                    (Some(_), None) => false,
                };
                if c.next >= c.end || !behind {
                    continue;
                }
                let mut entries = 0usize;
                let mut n = c.next;
                while n < c.end && (n == c.next || entries < need) && n - c.next < 64 {
                    entries += c.open.blocks[n].entries as usize;
                    n += 1;
                }
                want.push((i, c.next..n));
            }
            if want.is_empty() {
                break;
            }
            let asked = want
                .iter()
                .flat_map(|(i, r)| {
                    let c = &cursors[*i];
                    c.open.blocks[r.clone()]
                        .iter()
                        .map(move |b| (&c.sec.run, b))
                })
                .collect();
            let got = self.blocks(asked).await?;
            for (i, r) in want {
                let c = &mut cursors[i];
                for b in &c.open.blocks[r.clone()] {
                    let (block, _) = got
                        .get(&(c.sec.run.id, b.offset))
                        .ok_or_else(|| Error::Store("a block asked for and not read".into()))?;
                    let mut at = 0;
                    while at < block.len() {
                        let (k, v) = format::entry(block, &mut at)?;
                        if k.as_ref() >= end {
                            break;
                        }
                        if k.as_ref() < start {
                            continue;
                        }
                        if !layout::group_of(&k).is_some_and(|g| c.sec.counts_for(g)) {
                            continue;
                        }
                        let epoch = c.sec.handle.epoch;
                        match merged.get(&k) {
                            Some((e, _)) if *e >= epoch => {}
                            _ => {
                                merged.insert(k, (epoch, v));
                            }
                        }
                    }
                    c.read_to = Some(b.last.clone());
                }
                c.next = r.end;
            }
            let upto = bound(&cursors);
            while let Some(entry) = merged.first_entry() {
                if upto.as_ref().is_some_and(|b| entry.key() > b) {
                    break;
                }
                let (k, (_, v)) = entry.remove_entry();
                if let Some(v) = v {
                    out.push((k, v));
                    if out.len() == limit {
                        return Ok(out);
                    }
                }
            }
        }
        out.extend(
            merged
                .into_iter()
                .filter_map(|(k, (_, v))| Some((k, v?)))
                .take(limit - out.len()),
        );
        Ok(out)
    }

    /// Whether some section may hold `k`, so its delete must be kept. A
    /// run not opened may hold anything.
    fn may_hold(&self, k: &[u8]) -> bool {
        let (Some(kind), Some(g)) = (layout::section_of(k), layout::group_of(k)) else {
            return true;
        };
        let h = filter::hash(layout::filtered(k));
        self.sections[kind.index()]
            .iter()
            .filter(|s| s.counts_for(g))
            .any(|s| match s.run.open.get() {
                None => true,
                Some(o) => o.sections[kind.index()]
                    .as_ref()
                    .is_none_or(|open| open.may_contain(g, h)),
            })
    }

    async fn write(&mut self, epoch: u64, mut changes: Vec<Change>) -> Result<Vec<Snapshot>> {
        for (k, _) in &changes {
            if !layout::group_of(k).is_some_and(|g| self.range.contains(g.into())) {
                return Err(Error::Invalid(format!(
                    "a change outside key groups {} to {}",
                    self.range.first, self.range.last
                )));
            }
        }
        let newest = self
            .sections
            .iter()
            .filter_map(|s| s.first())
            .map(|s| s.handle.epoch)
            .max();
        if newest.is_some_and(|n| n >= epoch) {
            return Err(Error::Invalid(format!(
                "epoch {epoch}'s barrier, where the lane reads a section of a later one"
            )));
        }
        changes.retain(|(k, v)| v.is_some() || self.may_hold(k));
        if changes.is_empty() {
            return Ok(vec![self.snapshot()]);
        }
        let changed: usize = changes
            .iter()
            .map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()))
            .sum();
        let built = format::build(&changes, &self.shared.config)?;
        let name = RunName {
            generation: self.generation,
            epoch,
            range: self.range,
        }
        .to_string();
        let size = built.bytes.len();
        self.shared.objects.put_new(&name, built.bytes).await?;
        request(&self.counters().barrier, size);
        add(&self.counters().changed_bytes, changed as u64);
        let id = self.shared.id();
        let mut named = Vec::new();
        let mut opened: [Option<OpenSection>; 4] = Default::default();
        let mut bytes = 0;
        {
            let mut cache = lock(&self.shared.cache);
            for s in built.sections {
                for (offset, block) in s.blocks {
                    cache.put((id, offset), block);
                }
                named.push(SectionRef {
                    kind: s.info.kind,
                    epoch,
                    groups: s.info.groups,
                    entries: s.info.entries,
                    raw_bytes: s.info.raw_bytes,
                    stored_bytes: s.info.stored_bytes,
                    times: s.info.times,
                    meta: s.info.meta,
                });
                bytes += s.open.memory_bytes();
                opened[s.info.kind.index()] = Some(s.open);
            }
        }
        let open = OnceLock::new();
        let _ = open.set(Opened {
            sections: opened,
            bytes,
        });
        let mut keep: [Vec<KeyGroupRange>; 4] = Default::default();
        for s in &named {
            keep[s.kind.index()].push(s.groups);
        }
        let run = Arc::new(Run {
            id,
            name,
            size: size as u64,
            named: named.clone(),
            keep,
            open,
        });
        for s in named {
            self.sections[s.kind.index()].insert(
                0,
                Sec {
                    run: run.clone(),
                    handle: s,
                },
            );
        }
        self.runs.push(run);
        Ok(vec![self.snapshot()])
    }

    /// What the lane's next record names: every section it reads.
    fn snapshot(&self) -> Snapshot {
        let mut by_run: BTreeMap<(std::cmp::Reverse<u64>, &str), RunRef> = BTreeMap::new();
        for secs in &self.sections {
            for s in secs {
                let newest = s.run.named.iter().map(|n| n.epoch).max().unwrap_or(0);
                by_run
                    .entry((std::cmp::Reverse(newest), &s.run.name))
                    .or_insert_with(|| RunRef {
                        name: s.run.name.clone(),
                        format: FORMAT,
                        size: s.run.size,
                        sections: Vec::new(),
                    })
                    .sections
                    .push(s.handle.clone());
            }
        }
        let runs: Vec<RunRef> = by_run.into_values().collect();
        Snapshot {
            range: self.range,
            layout: LAYOUT,
            data: handle::encode(&runs),
        }
    }
}

/// The greatest key every cursor with blocks left has read to: no key at
/// or before it is in a block not read yet. `None` when none has blocks
/// left.
fn bound(cursors: &[Cursor<'_>]) -> Option<Bytes> {
    let mut bound: Option<Bytes> = None;
    for c in cursors.iter().filter(|c| c.next < c.end) {
        let Some(r) = &c.read_to else {
            return Some(Bytes::new());
        };
        if bound.as_ref().is_none_or(|b| r < b) {
            bound = Some(r.clone());
        }
    }
    bound
}

impl Store for RunStore {
    fn get<'a>(&'a self, keys: &'a [Bytes]) -> BoxFuture<'a, Result<Vec<Option<Bytes>>>> {
        Box::pin(self.lookup(keys))
    }

    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(Bytes, Bytes)>>> {
        Box::pin(self.scan_range(start, end, limit))
    }

    fn commit(&mut self, epoch: u64, changes: Vec<Change>) -> BoxFuture<'_, Result<Vec<Snapshot>>> {
        Box::pin(self.write(epoch, changes))
    }

    /// Nothing goes: every run this lane reads stays named by its next
    /// record, until sections are merged and dropped.
    fn release(&mut self, _epoch: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn memory_bytes(&self) -> usize {
        self.runs
            .iter()
            .filter_map(|r| r.open.get())
            .map(|o| o.bytes)
            .sum()
    }
}
