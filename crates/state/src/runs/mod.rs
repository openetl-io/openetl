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
//! A snapshot leaves out the sections that are dead, and nothing is
//! merged. A section of rows, timers or a set is dead once each of its
//! operators' horizons in each of its key groups has passed its latest
//! entry: a horizon hides what's at or before it, so a section that holds
//! some of that and later entries too stays, and reads pass over what it
//! hid. A values section is dead once each of its entries is overridden,
//! by the same key in a newer section the lane names, or is a delete that
//! no older section the lane names may hold the key of. A lane knows what
//! a values section holds once it writes it, or reads it whole: those it
//! restored, at its first barrier that writes a run. Until then such a
//! section may hold anything. A run all of whose sections are dead is
//! never written. Runs that no kept record names, from a barrier never
//! recorded, an older attempt, or sections no longer named, are orphans
//! for retention.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::ops::{Add, Range, Sub};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use bytes::Bytes;
use futures_util::stream::FuturesUnordered;
use futures_util::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{GetOptions, ObjectStore, PutMode, PutOptions};
use twox_hash::XxHash3_128;

use crate::layout::{self, SectionKind};
use crate::store::{BoxFuture, Change, Horizons, Snapshot, Store, Stores};
use crate::{Error, KeyGroupRange, LAYOUT, NEVER, OperatorId, Result};

mod cache;
mod codec;
mod filter;
mod format;
mod handle;

use cache::{BlockCache, BlockId};
pub use format::FORMAT;
use format::{BlockMeta, OpenSection};
pub use handle::{
    Horizon, Named, RunName, RunRef, SectionRef, VERSION, encode, encode_named, named, named_runs,
    runs,
};

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
    /// Snapshots leave dead sections out. Off, a lane names every section
    /// it or the lanes before it wrote.
    pub drop: bool,
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
            drop: true,
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
    /// Reads of runs' indexes and filters, opening what a record named,
    /// and of the values sections a lane restored, each read whole once to
    /// tell when it's dead.
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

    /// Bytes `range` of `name`, all of them. A missing `name` is
    /// [`Error::Corrupt`]: only a snapshot names a run to read.
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
            // A run a snapshot names that isn't there won't be on a retry.
            let b = read.await.map_err(|e| match e {
                object_store::Error::NotFound { .. } => {
                    Error::Corrupt(format!("run {name}, which a snapshot names, is missing"))
                }
                e => Error::Store(format!("reading run {name}: {e}")),
            })?;
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
    /// The most bytes a lane held at once reading values sections whole.
    learn_peak: AtomicU64,
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
                learn_peak: AtomicU64::new(0),
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

    /// The most bytes a lane has held at once reading the values sections
    /// it restored whole, read and decoded: within the block cache's
    /// bound, or one block where that's larger.
    pub fn learned_peak(&self) -> u64 {
        self.shared.learn_peak.load(Ordering::Relaxed)
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
        let mut horizons = Horizons::new();
        for s in snapshots.iter().filter(|s| s.range.overlaps(&range)) {
            let named = handle::named(s)?;
            if let Some(mine) = intersect(s.range, range) {
                horizons.extend(named.horizons_in(mine));
            }
            for r in named.runs {
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
                // Every run is a barrier's, holding that epoch's changes of
                // its lane's key groups. A run a merge writes will need a
                // name of its own, and a relation of its own here.
                let name = RunName::parse(&r.name)
                    .ok_or_else(|| Error::Snapshot(format!("{} isn't a run's name", r.name)))?;
                let lying = r.sections.iter().find(|sec| {
                    sec.epoch != name.epoch || intersect(sec.groups, name.range) != Some(sec.groups)
                });
                if let Some(sec) = lying {
                    return Err(Error::Snapshot(format!(
                        "run {} named with a {:?} section of epoch {} for key groups {} to {}",
                        r.name, sec.kind, sec.epoch, sec.groups.first, sec.groups.last
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
            horizons,
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
                    content: None,
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
    /// For a values section, what it holds in its groups, once the lane
    /// knows.
    content: Option<Content>,
}

impl Sec {
    fn counts_for(&self, group: u16) -> bool {
        self.handle.groups.contains(group.into())
    }
}

/// An entry of a [`Content`] that's a delete.
const DELETE: u8 = 1;
/// An entry of a [`Content`] that's dead: overridden by a newer section, or
/// a delete of what no older one may hold.
const DEAD: u8 = 2;

/// What a lane knows a values section holds, in the key groups it reads it
/// for: each key's hash, 128 bits so that no two keys share one.
struct Content {
    /// Sorted.
    keys: Vec<u128>,
    /// Each entry's [`DELETE`] and [`DEAD`].
    flags: Vec<u8>,
    /// Entries not dead.
    alive: usize,
    /// Known since the lane last looked for dead sections.
    fresh: bool,
}

fn key_hash(k: &[u8]) -> u128 {
    XxHash3_128::oneshot(k)
}

impl Content {
    /// Of entries' key hashes, each with whether it's a delete.
    fn new(mut entries: Vec<(u128, bool)>) -> Content {
        entries.sort_unstable_by_key(|e| e.0);
        Content {
            alive: entries.len(),
            keys: entries.iter().map(|e| e.0).collect(),
            flags: entries.iter().map(|e| u8::from(e.1) * DELETE).collect(),
            fresh: true,
        }
    }

    /// Where `h`'s entries are.
    fn at(&self, h: u128) -> Range<usize> {
        let lo = self.keys.partition_point(|k| *k < h);
        let hi = self.keys.partition_point(|k| *k <= h);
        lo..hi
    }

    fn holds(&self, h: u128) -> bool {
        !self.at(h).is_empty()
    }

    fn kill(&mut self, i: usize) {
        if self.flags[i] & DEAD == 0 {
            self.flags[i] |= DEAD;
            self.alive -= 1;
        }
    }

    /// Kills its entries whose keys `newer`, a newer section, holds.
    fn overridden_by(&mut self, newer: &Content) {
        if self.keys.len() < newer.keys.len() {
            for i in 0..self.keys.len() {
                if newer.holds(self.keys[i]) {
                    self.kill(i);
                }
            }
        } else {
            for &h in &newer.keys {
                for i in self.at(h) {
                    self.kill(i);
                }
            }
        }
    }

    /// Its live deletes, of `keys` or of all.
    fn live_deletes(&self, keys: Option<&BTreeSet<u128>>) -> Vec<usize> {
        let live = |i: &usize| self.flags[*i] & (DELETE | DEAD) == DELETE;
        match keys {
            None => (0..self.keys.len()).filter(live).collect(),
            Some(keys) => keys.iter().flat_map(|h| self.at(*h)).filter(live).collect(),
        }
    }

    fn memory_bytes(&self) -> usize {
        self.keys.len() * (std::mem::size_of::<u128>() + 1) + std::mem::size_of::<Self>()
    }
}

/// Whether a section of rows, timers or a set is dead: each of its
/// operators' horizons, in each of its groups, has passed its latest
/// entry. A set's entry that never expires never dies.
fn passed(s: &SectionRef, horizons: &Horizons) -> bool {
    let (Some(ops), Some((_, latest))) = (&s.operators, s.times) else {
        return false;
    };
    if s.kind == SectionKind::Values || s.kind == SectionKind::Set && latest == NEVER {
        return false;
    }
    !ops.is_empty()
        && ops.iter().all(|&op| {
            (s.groups.first..=s.groups.last).all(|g| {
                horizons
                    .get(&(OperatorId(op), g))
                    .is_some_and(|&h| h > i64::MIN && latest <= h)
            })
        })
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
    /// Each operator's horizon in the lane's groups, as its last snapshot
    /// keeps them.
    horizons: Horizons,
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
            let keep = &run.keep[named.kind.index()];
            let (info, open) = format::open_section(chunk, offset, keep)?;
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
        for (_, (run, blocks)) in misses {
            for (span, bs) in self.spans(blocks) {
                reads.push((run, span, bs));
            }
        }
        let lookup = &self.counters().lookup;
        let reads: Vec<_> = reads
            .into_iter()
            .map(|(run, span, bs)| self.read_blocks(run, span, bs, lookup))
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

    /// Blocks of one run, as reads: those near each other in one, up to
    /// the most a read takes.
    fn spans<'b>(&self, mut blocks: Vec<&'b BlockMeta>) -> Vec<(Range<u64>, Vec<&'b BlockMeta>)> {
        let config = &self.shared.config;
        blocks.sort_by_key(|b| b.offset);
        blocks.dedup_by_key(|b| b.offset);
        let mut out: Vec<(Range<u64>, Vec<&BlockMeta>)> = Vec::new();
        for b in blocks {
            match out.last_mut() {
                Some((span, bs))
                    if b.offset <= span.end + config.gap_bytes
                        && b.end() - span.start <= config.max_read_bytes =>
                {
                    span.end = span.end.max(b.end());
                    bs.push(b);
                }
                _ => out.push((b.offset..b.end(), vec![b])),
            }
        }
        out
    }

    /// The blocks `bs` of `run`, all within `span`, in one request, which
    /// counts as one of `kind`.
    async fn read_blocks(
        &self,
        run: &Run,
        span: Range<u64>,
        bs: Vec<&BlockMeta>,
        kind: &(AtomicU64, AtomicU64),
    ) -> Result<Vec<(BlockId, Bytes)>> {
        let data = self
            .shared
            .objects
            .get_range(&run.name, span.clone())
            .await?;
        request(kind, data.len());
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

    async fn write(
        &mut self,
        epoch: u64,
        mut changes: Vec<Change>,
        horizons: &Horizons,
    ) -> Result<Vec<Snapshot>> {
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
        self.horizons = horizons
            .iter()
            .filter(|((_, g), h)| self.range.contains(*g) && **h > i64::MIN)
            .map(|(k, h)| (*k, *h))
            .collect();
        let drop = self.shared.config.drop;
        let mut written = None;
        if !changes.is_empty() {
            let content = drop.then(|| {
                let values = changes
                    .iter()
                    .filter(|(k, _)| layout::section_of(k) == Some(SectionKind::Values));
                Content::new(values.map(|(k, v)| (key_hash(k), v.is_none())).collect())
            });
            written = Some(self.add_run(epoch, &changes, content)?);
        }
        if drop {
            if written.is_some() {
                self.learn().await?;
            }
            self.drop_dead();
        }
        if let Some((run, bytes, blocks)) = written
            && self.runs.iter().any(|r| r.id == run.id)
        {
            let size = bytes.len();
            self.shared.objects.put_new(&run.name, bytes).await?;
            request(&self.counters().barrier, size);
            let changed: usize = changes
                .iter()
                .map(|(k, v)| k.len() + v.as_ref().map_or(0, |v| v.len()))
                .sum();
            add(&self.counters().changed_bytes, changed as u64);
            let mut cache = lock(&self.shared.cache);
            for (offset, block) in blocks {
                cache.put((run.id, offset), block);
            }
        }
        Ok(vec![self.snapshot()])
    }

    /// Builds the run of epoch `epoch`'s changes, and reads its sections as
    /// the newest, `content` its values'. Gives the run with its bytes and
    /// blocks, to write.
    #[allow(clippy::type_complexity)]
    fn add_run(
        &mut self,
        epoch: u64,
        changes: &[Change],
        mut content: Option<Content>,
    ) -> Result<(Arc<Run>, Bytes, Vec<(u64, Bytes)>)> {
        let built = format::build(changes, &self.shared.config)?;
        let name = RunName {
            generation: self.generation,
            epoch,
            range: self.range,
        }
        .to_string();
        let size = built.bytes.len();
        let id = self.shared.id();
        let mut named = Vec::new();
        let mut opened: [Option<OpenSection>; 4] = Default::default();
        let mut bytes = 0;
        let mut blocks = Vec::new();
        for s in built.sections {
            blocks.extend(s.blocks);
            named.push(SectionRef {
                kind: s.info.kind,
                epoch,
                groups: s.info.groups,
                entries: s.info.entries,
                raw_bytes: s.info.raw_bytes,
                stored_bytes: s.info.stored_bytes,
                times: s.info.times,
                meta: s.info.meta,
                operators: Some(s.operators),
            });
            bytes += s.open.memory_bytes();
            opened[s.info.kind.index()] = Some(s.open);
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
            let content = match s.kind {
                SectionKind::Values => content.take(),
                _ => None,
            };
            self.sections[s.kind.index()].insert(
                0,
                Sec {
                    run: run.clone(),
                    handle: s,
                    content,
                },
            );
        }
        self.runs.push(run.clone());
        Ok((run, built.bytes, blocks))
    }

    /// Reads whole each values section the lane doesn't know yet, to learn
    /// what it holds. The reads count as a restore's.
    async fn learn(&mut self) -> Result<()> {
        let v = SectionKind::Values.index();
        let unknown: Vec<usize> = (0..self.sections[v].len())
            .filter(|&i| self.sections[v][i].content.is_none())
            .collect();
        if unknown.is_empty() {
            return Ok(());
        }
        let need = self.unopened(unknown.iter().map(|&i| &self.sections[v][i]));
        self.open_runs(need).await?;
        // What it reads and decodes at once stays within the block cache's
        // bound, the share of the job's memory the store's blocks have, or
        // one block where that's larger. Each read's blocks go once their
        // keys are taken.
        let config = &self.shared.config;
        let budget = u64::try_from(config.cache_bytes).unwrap_or(u64::MAX).max(1);
        // Each read: a section's blocks near each other, with what reading
        // and decoding them holds, its weight, within the budget.
        let mut queue: VecDeque<(usize, Range<u64>, Vec<BlockMeta>, u64)> = VecDeque::new();
        for &i in &unknown {
            let sec = &self.sections[v][i];
            let groups = sec.handle.groups;
            let open = sec.run.section(SectionKind::Values)?;
            let mut mine: Vec<&BlockMeta> = open
                .blocks
                .iter()
                .filter(
                    |b| match (layout::group_of(&b.first), layout::group_of(&b.last)) {
                        (Some(a), Some(z)) => {
                            groups.overlaps(&KeyGroupRange::new(a.into(), z.into()))
                        }
                        _ => true,
                    },
                )
                .collect();
            mine.sort_by_key(|b| b.offset);
            mine.dedup_by_key(|b| b.offset);
            let mut read: Option<(Range<u64>, Vec<BlockMeta>, u64)> = None;
            for b in mine {
                let raw = u64::from(b.raw);
                if let Some((span, bs, decoded)) = &mut read {
                    let end = span.end.max(b.end());
                    if b.offset <= span.end + config.gap_bytes
                        && end - span.start <= config.max_read_bytes
                        && end - span.start + *decoded + raw <= budget
                    {
                        span.end = end;
                        bs.push(b.clone());
                        *decoded += raw;
                        continue;
                    }
                }
                if let Some((span, bs, decoded)) = read.take() {
                    let weight = span.end - span.start + decoded;
                    queue.push_back((i, span, bs, weight));
                }
                read = Some((b.offset..b.end(), vec![b.clone()], raw));
            }
            if let Some((span, bs, decoded)) = read {
                let weight = span.end - span.start + decoded;
                queue.push_back((i, span, bs, weight));
            }
        }
        let mut entries: HashMap<usize, Vec<(u128, bool)>> =
            unknown.iter().map(|&i| (i, Vec::new())).collect();
        let peak = {
            let this: &RunStore = self;
            let restore = &this.counters().restore;
            let mut reading = FuturesUnordered::new();
            let (mut held, mut peak) = (0u64, 0u64);
            loop {
                while let Some(&(i, _, _, weight)) = queue.front() {
                    let full = held.saturating_add(weight) > budget
                        || reading.len() >= config.concurrency.max(1);
                    if full && !reading.is_empty() {
                        break;
                    }
                    let Some((_, span, bs, _)) = queue.pop_front() else {
                        break;
                    };
                    held += weight;
                    peak = peak.max(held);
                    let run = this.sections[v][i].run.clone();
                    reading.push(async move {
                        let refs: Vec<&BlockMeta> = bs.iter().collect();
                        let blocks = this.read_blocks(&run, span, refs, restore).await?;
                        Ok::<_, Error>((i, weight, blocks))
                    });
                }
                let Some(done) = reading.next().await else {
                    break;
                };
                let (i, weight, blocks) = done?;
                let groups = this.sections[v][i].handle.groups;
                let mine = entries.entry(i).or_default();
                for (_, block) in blocks {
                    let mut at = 0;
                    while at < block.len() {
                        let (k, val) = format::entry(&block, &mut at)?;
                        if layout::group_of(&k).is_some_and(|g| groups.contains(g.into())) {
                            mine.push((key_hash(&k), val.is_none()));
                        }
                    }
                }
                held -= weight;
            }
            peak
        };
        self.shared.learn_peak.fetch_max(peak, Ordering::Relaxed);
        for (i, e) in entries {
            self.sections[v][i].content = Some(Content::new(e));
        }
        Ok(())
    }

    /// Stops reading the sections that are dead, and the runs that hold no
    /// other.
    fn drop_dead(&mut self) {
        for kind in [SectionKind::Rows, SectionKind::Set, SectionKind::Timers] {
            let horizons = &self.horizons;
            self.sections[kind.index()].retain(|s| !passed(&s.handle, horizons));
        }
        self.drop_values();
        let read: HashSet<u64> = self.sections.iter().flatten().map(|s| s.run.id).collect();
        self.runs.retain(|r| read.contains(&r.id));
        let unopened = self.runs.iter().filter(|r| r.open.get().is_none()).count();
        self.unopened.store(unopened, Ordering::Relaxed);
    }

    /// Stops reading the values sections that are dead, once what the lane
    /// knows of them has changed: a section it wrote or learned.
    fn drop_values(&mut self) {
        let secs = &mut self.sections[SectionKind::Values.index()];
        let fresh: Vec<usize> = (0..secs.len())
            .filter(|&i| secs[i].content.as_ref().is_some_and(|c| c.fresh))
            .collect();
        if fresh.is_empty() {
            return;
        }
        for &i in &fresh {
            let (newer, rest) = secs.split_at_mut(i);
            let (this, older) = rest.split_at_mut(1);
            let Some(c) = this[0].content.as_mut() else {
                continue;
            };
            for o in older.iter_mut() {
                if let Some(oc) = o.content.as_mut() {
                    oc.overridden_by(c);
                }
            }
            // What newer sections hold overrides this one's; a fresh one's
            // own pass over older sections does so.
            for w in newer.iter() {
                if let Some(wc) = &w.content
                    && !wc.fresh
                {
                    c.overridden_by(wc);
                }
            }
        }
        // A learned section, older than the newest, may tell deletes in
        // newer ones that it doesn't hold their keys: look at every delete.
        let mut keys: Option<BTreeSet<u128>> = match fresh.iter().any(|&i| i > 0) {
            true => None,
            false => secs[0].content.as_ref().map(|c| {
                c.live_deletes(None)
                    .into_iter()
                    .map(|i| c.keys[i])
                    .collect()
            }),
        };
        loop {
            for i in 0..secs.len() {
                let (head, older) = secs.split_at_mut(i + 1);
                let Some(c) = head[i].content.as_mut() else {
                    continue;
                };
                for j in c.live_deletes(keys.as_ref()) {
                    let h = c.keys[j];
                    let held = older
                        .iter()
                        .any(|o| o.content.as_ref().is_none_or(|oc| oc.holds(h)));
                    if !held {
                        c.kill(j);
                    }
                }
            }
            let dead: Vec<usize> = (0..secs.len())
                .filter(|&i| secs[i].content.as_ref().is_some_and(|c| c.alive == 0))
                .collect();
            if dead.is_empty() {
                break;
            }
            let mut gone = BTreeSet::new();
            for &i in dead.iter().rev() {
                let s = secs.remove(i);
                if let Some(c) = s.content {
                    gone.extend(c.keys);
                }
            }
            keys = Some(gone);
        }
        for s in secs.iter_mut() {
            if let Some(c) = s.content.as_mut() {
                c.fresh = false;
            }
        }
    }

    /// What the lane's next record names: every section it reads, and its
    /// operators' horizons.
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
        let named = Named {
            runs: by_run.into_values().collect(),
            horizons: handle::ranges_of(&self.horizons),
        };
        Snapshot {
            range: self.range,
            layout: LAYOUT,
            data: handle::encode_named(&named),
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

    fn horizons(&self) -> Horizons {
        self.horizons.clone()
    }

    fn commit<'a>(
        &'a mut self,
        epoch: u64,
        changes: Vec<Change>,
        horizons: &'a Horizons,
    ) -> BoxFuture<'a, Result<Vec<Snapshot>>> {
        Box::pin(self.write(epoch, changes, horizons))
    }

    /// Nothing goes: what retention deletes, no kept record names.
    fn release(&mut self, _epoch: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }

    fn memory_bytes(&self) -> usize {
        let runs: usize = self
            .runs
            .iter()
            .filter_map(|r| r.open.get())
            .map(|o| o.bytes)
            .sum();
        let known: usize = self.sections[SectionKind::Values.index()]
            .iter()
            .filter_map(|s| s.content.as_ref())
            .map(Content::memory_bytes)
            .sum();
        runs + known
    }
}
