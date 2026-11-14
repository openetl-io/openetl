//! Runs of our own: immutable sorted files per key-group range, named by the
//! checkpoint record. A prototype with only what the measurements need.
//!
//! - A barrier writes the epoch's changes as one run per chunk of key groups,
//!   at `g{generation}/e{epoch}/kg{first}-{last}.run`, so no two attempts or
//!   epochs write the same name. The snapshot the record keeps lists every
//!   run of the range, newest first: no other manifest, no second commit
//!   point.
//! - A run is 4 KiB blocks of entries, then an index of each block's last
//!   key and a bloom filter. Opening a run reads its index and filter, in
//!   one request; a lookup reads one block.
//! - Runs merge in the background, size-tiered: `fanout` runs of a level
//!   become one of the next. A merge drops shadowed versions, set entries
//!   that expired by the watermark of the epoch it ran after, and tombstones
//!   when its output is the oldest run.
//! - A restore opens the runs the record names that overlap the lane's range,
//!   on any number of lanes: nothing is copied.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{StreamExt, TryStreamExt};
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use twox_hash::XxHash3_64;

use crate::key::{self, Kind};
use crate::{Change, Error, KeyGroup, KeyGroupRange, Result, Snapshot, Store};

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub block_bytes: usize,
    pub bloom_bits_per_key: u32,
    /// Runs of one level that merge into one of the next.
    pub fanout: usize,
    /// Key groups per chunk, aligned from group 0; a chunk's changes go to a
    /// run of their own. The pipeline's key groups: one run per lane.
    pub groups_per_run: u32,
    pub block_cache_bytes: usize,
    /// Lookups in flight at once.
    pub concurrency: usize,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            block_bytes: 4096,
            bloom_bits_per_key: 10,
            fanout: 4,
            groups_per_run: 256,
            block_cache_bytes: 64 << 20,
            concurrency: 64,
        }
    }
}

/// A run as the record names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunMeta {
    pub path: String,
    pub range: KeyGroupRange,
    pub level: u32,
    pub size: u64,
    pub entries: u64,
    /// Where the index and filter start; they run to the end.
    pub index_offset: u64,
}

/// A run, open: its index and filter in memory.
struct Run {
    id: u64,
    meta: RunMeta,
    /// Each block's offset, length and last key.
    blocks: Vec<(u64, u32, Bytes)>,
    bloom: Bloom,
}

/// Background work, in requests and bytes, and what merges did.
#[derive(Default, Debug)]
pub struct MergeStats {
    pub merges: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub entries_dropped: AtomicU64,
    pub nanos: AtomicU64,
}

struct Chunk {
    range: KeyGroupRange,
    /// Newest first.
    runs: Vec<Arc<Run>>,
    merging: bool,
}

struct Shared {
    store: Arc<dyn ObjectStore>,
    merge_store: Arc<dyn ObjectStore>,
    base: Path,
    config: RunConfig,
    chunks: Mutex<Vec<Chunk>>,
    cache: Mutex<BlockCache>,
    next_id: AtomicU64,
    merges: Arc<MergeStats>,
    watermark: AtomicU64,
    generation: u64,
    epoch: AtomicU64,
    /// Merges in flight.
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    /// Paths this store wrote, so retention deletes only its own.
    written: Mutex<HashSet<String>>,
}

pub struct RunStore {
    shared: Arc<Shared>,
    range: KeyGroupRange,
    /// The runs each kept record names, by epoch.
    named: BTreeMap<u64, Vec<String>>,
}

impl RunStore {
    /// Opens the runs of `range` that `restore` names, from the snapshots of
    /// one record, whatever ranges they were written for. `merge_store` is
    /// the same store, counted apart.
    pub async fn open(
        store: Arc<dyn ObjectStore>,
        merge_store: Arc<dyn ObjectStore>,
        base: Path,
        generation: u64,
        range: KeyGroupRange,
        restore: &[Snapshot],
        config: RunConfig,
    ) -> Result<Self> {
        let mut metas = Vec::new();
        for s in restore.iter().filter(|s| s.range.overlaps(&range)) {
            metas.extend(decode_snapshot(&s.data)?);
        }
        let next_id = AtomicU64::new(0);
        let opens: Vec<_> = metas
            .into_iter()
            .map(|m| {
                let id = next_id.fetch_add(1, Ordering::Relaxed);
                open_run(&store, &base, m, id)
            })
            .collect();
        let opened: Vec<Run> = crate::unordered(opens, config.concurrency).await?;
        let mut chunks = chunk_ranges(range, config.groups_per_run)
            .into_iter()
            .map(|r| Chunk {
                range: r,
                runs: Vec::new(),
                merging: false,
            })
            .collect::<Vec<_>>();
        // Snapshots list runs newest first; keep that order in each chunk.
        let opened: Vec<Arc<Run>> = opened.into_iter().map(Arc::new).collect();
        for c in &mut chunks {
            c.runs = opened
                .iter()
                .filter(|r| r.meta.range.overlaps(&c.range))
                .cloned()
                .collect();
        }
        let cache = BlockCache::new(config.block_cache_bytes);
        Ok(Self {
            shared: Arc::new(Shared {
                store,
                merge_store,
                base,
                config,
                chunks: Mutex::new(chunks),
                cache: Mutex::new(cache),
                next_id,
                merges: Arc::new(MergeStats::default()),
                watermark: AtomicU64::new(0),
                generation,
                epoch: AtomicU64::new(0),
                tasks: Mutex::new(Vec::new()),
                written: Mutex::new(HashSet::new()),
            }),
            range,
            named: BTreeMap::new(),
        })
    }

    pub fn merges(&self) -> Arc<MergeStats> {
        self.shared.merges.clone()
    }

    /// Runs open, and the bytes their indexes and filters hold in memory.
    pub fn footprint(&self) -> (usize, usize) {
        let chunks = lock(&self.shared.chunks);
        let mut runs = 0;
        let mut bytes = 0;
        for c in chunks.iter() {
            for r in &c.runs {
                runs += 1;
                bytes +=
                    r.bloom.bits.len() * 8 + r.blocks.iter().map(|b| b.2.len() + 32).sum::<usize>();
            }
        }
        (runs, bytes)
    }

    /// Waits for the merges in flight.
    pub async fn settle(&self) {
        loop {
            let tasks: Vec<_> = std::mem::take(&mut *lock(&self.shared.tasks));
            if tasks.is_empty() {
                return;
            }
            for t in tasks {
                let _ = t.await;
            }
        }
    }

    fn snapshot(&self) -> Vec<Snapshot> {
        let chunks = lock(&self.shared.chunks);
        let mut runs: Vec<RunMeta> = Vec::new();
        let mut seen = HashSet::new();
        for c in chunks.iter() {
            for r in &c.runs {
                if seen.insert(r.meta.path.clone()) {
                    runs.push(r.meta.clone());
                }
            }
        }
        vec![Snapshot {
            range: self.range,
            data: encode_snapshot(&runs),
        }]
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

fn chunk_ranges(range: KeyGroupRange, per: u32) -> Vec<KeyGroupRange> {
    let per = per.max(1);
    let mut out = Vec::new();
    let mut g = u32::from(range.first);
    let last = u32::from(range.last);
    while g <= last {
        let end = ((g / per) + 1) * per - 1;
        let end = end.min(last);
        out.push(KeyGroupRange::new(group(g), group(end)));
        g = end + 1;
    }
    out
}

fn group(g: u32) -> KeyGroup {
    KeyGroup::try_from(g).unwrap_or(KeyGroup::MAX)
}

impl Store for RunStore {
    async fn get_many(&self, keys: Vec<Bytes>) -> Result<Vec<Option<Bytes>>> {
        let gets: Vec<_> = keys.iter().map(|k| self.shared.get(k)).collect();
        crate::unordered(gets, self.shared.config.concurrency).await
    }

    async fn scan(&self, start: Bytes, end: Bytes) -> Result<Vec<(Bytes, Bytes)>> {
        self.shared.scan(&start, &end).await
    }

    async fn commit(
        &mut self,
        epoch: u64,
        watermark: i64,
        changes: Vec<Change>,
    ) -> Result<Vec<Snapshot>> {
        let shared = self.shared.clone();
        shared.epoch.store(epoch, Ordering::Relaxed);
        shared.watermark.store(flip(watermark), Ordering::Relaxed);
        // Split by chunk.
        let ranges: Vec<KeyGroupRange> = lock(&shared.chunks).iter().map(|c| c.range).collect();
        let mut parts: Vec<Vec<Change>> = vec![Vec::new(); ranges.len()];
        let mut ci = 0;
        for (k, v) in changes {
            let g = key::group_of(&k);
            while ci < ranges.len() && ranges[ci].last < g {
                ci += 1;
            }
            if ci < ranges.len() && ranges[ci].contains(g) {
                parts[ci].push((k, v));
            }
        }
        let written: Vec<(usize, Run)> = futures::stream::iter(parts.into_iter().enumerate())
            .filter(|(_, p)| futures::future::ready(!p.is_empty()))
            .map(|(i, p)| {
                let shared = shared.clone();
                let range = ranges[i];
                async move {
                    let path = format!(
                        "g{}/e{}/kg{}-{}.run",
                        shared.generation, epoch, range.first, range.last
                    );
                    let entries = p.iter().map(|(k, v)| (k.clone(), v.clone()));
                    let run = shared
                        .write_run(&shared.store, path, range, 0, entries)
                        .await?;
                    Ok::<_, Error>((i, run))
                }
            })
            .buffered(shared.config.concurrency)
            .try_collect()
            .await?;
        {
            let mut chunks = lock(&shared.chunks);
            for (i, run) in written {
                chunks[i].runs.insert(0, Arc::new(run));
            }
        }
        let snaps = self.snapshot();
        let mut names = Vec::new();
        for s in &snaps {
            names.extend(decode_snapshot(&s.data)?.into_iter().map(|m| m.path));
        }
        self.named.insert(epoch, names);
        Shared::schedule(&shared);
        Ok(snaps)
    }

    async fn release(&mut self, epoch: u64) -> Result<()> {
        let mut released = Vec::new();
        while let Some((&e, _)) = self.named.first_key_value() {
            if e >= epoch {
                break;
            }
            if let Some((_, names)) = self.named.pop_first() {
                released.extend(names);
            }
        }
        let mut live: HashSet<String> = self.named.values().flatten().cloned().collect();
        for c in lock(&self.shared.chunks).iter() {
            live.extend(c.runs.iter().map(|r| r.meta.path.clone()));
        }
        let doomed: Vec<String> = {
            let mut written = lock(&self.shared.written);
            let doomed: Vec<String> = released
                .into_iter()
                .collect::<HashSet<_>>()
                .into_iter()
                .filter(|p| !live.contains(p) && written.contains(p))
                .collect();
            for p in &doomed {
                written.remove(p);
            }
            doomed
        };
        for p in doomed {
            self.shared
                .merge_store
                .delete(&self.shared.base.clone().join_path(&p))
                .await?;
        }
        Ok(())
    }
}

trait JoinPath {
    fn join_path(self, rel: &str) -> Path;
}

impl JoinPath for Path {
    fn join_path(self, rel: &str) -> Path {
        let mut p = self;
        for part in rel.split('/') {
            p = p.join(part);
        }
        p
    }
}

impl Shared {
    async fn get(&self, k: &Bytes) -> Result<Option<Bytes>> {
        let g = key::group_of(k);
        let runs: Vec<Arc<Run>> = {
            let chunks = lock(&self.chunks);
            match chunks.iter().find(|c| c.range.contains(g)) {
                Some(c) => c
                    .runs
                    .iter()
                    .filter(|r| r.meta.range.contains(g))
                    .cloned()
                    .collect(),
                None => return Ok(None),
            }
        };
        let h = hash(k);
        for r in runs {
            if !r.bloom.may_contain(h) {
                continue;
            }
            let Some(b) = r.block_for(k) else {
                continue;
            };
            let block = self.block(&self.store, &r, b).await?;
            if let Some(v) = find(&block, k)? {
                return Ok(v);
            }
        }
        Ok(None)
    }

    async fn scan(&self, start: &Bytes, end: &Bytes) -> Result<Vec<(Bytes, Bytes)>> {
        let g = key::group_of(start);
        let runs: Vec<Arc<Run>> = {
            let chunks = lock(&self.chunks);
            chunks
                .iter()
                .filter(|c| c.range.contains(g))
                .flat_map(|c| c.runs.iter().filter(|r| r.meta.range.contains(g)).cloned())
                .collect()
        };
        // Oldest first, so newer versions overwrite.
        let mut merged: BTreeMap<Bytes, Option<Bytes>> = BTreeMap::new();
        for r in runs.iter().rev() {
            for b in r.blocks_between(start, end) {
                let block = self.block(&self.store, r, b).await?;
                for e in Entries::new(block) {
                    let (k, v) = e?;
                    if k >= *start && (end.is_empty() || k < *end) {
                        merged.insert(k, v);
                    }
                }
            }
        }
        Ok(merged
            .into_iter()
            .filter_map(|(k, v)| v.map(|v| (k, v)))
            .collect())
    }

    async fn block(&self, store: &Arc<dyn ObjectStore>, r: &Run, b: usize) -> Result<Bytes> {
        if let Some(hit) = lock(&self.cache).get(r.id, b) {
            return Ok(hit);
        }
        let (off, len, _) = &r.blocks[b];
        let path = self.base.clone().join_path(&r.meta.path);
        let data = store.get_range(&path, *off..*off + u64::from(*len)).await?;
        lock(&self.cache).put(r.id, b, data.clone());
        Ok(data)
    }

    async fn write_run(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: String,
        range: KeyGroupRange,
        level: u32,
        entries: impl Iterator<Item = (Bytes, Option<Bytes>)>,
    ) -> Result<Run> {
        let mut w = RunWriter::new(self.config.block_bytes);
        for (k, v) in entries {
            w.add(&k, v.as_deref());
        }
        self.put_run(store, path, range, level, w).await
    }

    async fn put_run(
        &self,
        store: &Arc<dyn ObjectStore>,
        path: String,
        range: KeyGroupRange,
        level: u32,
        w: RunWriter,
    ) -> Result<Run> {
        let (file, index_offset, n, blocks, bloom) = w.finish(self.config.bloom_bits_per_key);
        let size = file.len() as u64;
        store
            .put(&self.base.clone().join_path(&path), file.clone().into())
            .await?;
        lock(&self.written).insert(path.clone());
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        // A barrier's run is the hottest state there is, as SlateDB's
        // default caches a flush's blocks; a merge's output isn't cached.
        if level == 0 {
            let mut cache = lock(&self.cache);
            for (i, (off, len, _)) in blocks.iter().enumerate() {
                let off = usize::try_from(*off).unwrap_or(0);
                cache.put(id, i, file.slice(off..off + *len as usize));
            }
        }
        Ok(Run {
            id,
            meta: RunMeta {
                path,
                range,
                level,
                size,
                entries: n,
                index_offset,
            },
            blocks,
            bloom,
        })
    }

    /// Starts a merge in each chunk that has `fanout` runs of one level.
    fn schedule(shared: &Arc<Shared>) {
        let fanout = shared.config.fanout.max(2);
        let mut jobs = Vec::new();
        {
            let mut chunks = lock(&shared.chunks);
            for (ci, c) in chunks.iter_mut().enumerate() {
                if c.merging {
                    continue;
                }
                // The oldest `fanout` runs sharing a level, contiguous.
                let n = c.runs.len();
                let mut pick = None;
                let mut i = n;
                while i >= fanout {
                    let level = c.runs[i - 1].meta.level;
                    let same = c.runs[i - fanout..i].iter().all(|r| r.meta.level == level);
                    if same {
                        pick = Some((i - fanout, i));
                        break;
                    }
                    i -= 1;
                }
                // Runs another layout wrote merge when their level comes up,
                // each read only for the chunk's span: a rescale rewrites
                // nothing at once.
                if let Some((a, b)) = pick {
                    c.merging = true;
                    let inputs: Vec<Arc<Run>> = c.runs[a..b].to_vec();
                    jobs.push((ci, c.range, inputs, b == n));
                }
            }
        }
        for (ci, range, inputs, oldest) in jobs {
            let s = shared.clone();
            let t = tokio::spawn(async move {
                if let Err(e) = s.merge(ci, range, inputs, oldest).await {
                    eprintln!("merge failed: {e}");
                }
                lock(&s.chunks)[ci].merging = false;
                Shared::schedule(&s);
            });
            lock(&shared.tasks).push(t);
        }
    }

    async fn merge(
        &self,
        ci: usize,
        range: KeyGroupRange,
        inputs: Vec<Arc<Run>>,
        oldest: bool,
    ) -> Result<()> {
        let started = std::time::Instant::now();
        let watermark = unflip(self.watermark.load(Ordering::Relaxed));
        let epoch = self.epoch.load(Ordering::Relaxed);
        let (start, end) = key::group_range(range.first, range.last);
        // Each input's span of the chunk, in one request, newest first.
        let mut spans = Vec::with_capacity(inputs.len());
        let mut read = 0u64;
        for r in &inputs {
            let bs = r.blocks_between(&start, &end);
            let (Some(&first), Some(&last)) = (bs.first(), bs.last()) else {
                spans.push(Entries::new(Bytes::new()).peekable());
                continue;
            };
            let from = r.blocks[first].0;
            let to = r.blocks[last].0 + u64::from(r.blocks[last].1);
            let path = self.base.clone().join_path(&r.meta.path);
            let data = self.merge_store.get_range(&path, from..to).await?;
            read += data.len() as u64;
            spans.push(Entries::new(data).peekable());
        }
        // A k-way merge: the smallest key next, its newest version kept.
        let mut out = RunWriter::new(self.config.block_bytes);
        let mut dropped = 0u64;
        loop {
            let mut best: Option<(usize, Bytes)> = None;
            for (i, s) in spans.iter_mut().enumerate() {
                match s.peek() {
                    None => {}
                    Some(Err(_)) => {
                        return Err(Error::Corrupt("a run's entry runs past its end".into()));
                    }
                    Some(Ok((k, _))) => {
                        if best.as_ref().is_none_or(|(_, b)| k < b) {
                            best = Some((i, k.clone()));
                        }
                    }
                }
            }
            let Some((i, k)) = best else {
                break;
            };
            let mut newest = None;
            for (j, s) in spans.iter_mut().enumerate() {
                if let Some(Ok((sk, _))) = s.peek()
                    && *sk == k
                {
                    let e = s.next();
                    if j == i {
                        newest = e;
                    } else {
                        dropped += 1;
                    }
                }
            }
            let Some(Ok((k, v))) = newest else {
                continue;
            };
            if k < start || (!end.is_empty() && k >= end) {
                continue;
            }
            let keep = match &v {
                None => !oldest,
                Some(v) => {
                    !(key::kind_of(&k) == Some(Kind::Set) && key::parse_expiry(v) <= watermark)
                }
            };
            if keep {
                out.add(&k, v.as_deref());
            } else {
                dropped += 1;
            }
        }
        drop(spans);
        let level = inputs.iter().map(|r| r.meta.level).max().unwrap_or(0) + 1;
        let path = format!(
            "g{}/m{}-{}/kg{}-{}.run",
            self.generation,
            epoch,
            self.next_id.load(Ordering::Relaxed),
            range.first,
            range.last
        );
        let run = self
            .put_run(&self.merge_store, path, range, level, out)
            .await?;
        self.merges
            .bytes_written
            .fetch_add(run.meta.size, Ordering::Relaxed);
        self.merges.bytes_read.fetch_add(read, Ordering::Relaxed);
        self.merges
            .entries_dropped
            .fetch_add(dropped, Ordering::Relaxed);
        self.merges.merges.fetch_add(1, Ordering::Relaxed);
        let ids: HashSet<u64> = inputs.iter().map(|r| r.id).collect();
        {
            let mut chunks = lock(&self.chunks);
            let c = &mut chunks[ci];
            let at = c.runs.iter().position(|r| ids.contains(&r.id));
            c.runs.retain(|r| !ids.contains(&r.id));
            let at = at.unwrap_or(c.runs.len()).min(c.runs.len());
            c.runs.insert(at, Arc::new(run));
        }
        lock(&self.cache).drop_runs(&ids);
        self.merges.nanos.fetch_add(
            u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(())
    }
}

async fn open_run(
    store: &Arc<dyn ObjectStore>,
    base: &Path,
    meta: RunMeta,
    id: u64,
) -> Result<Run> {
    let path = base.clone().join_path(&meta.path);
    let tail = store.get_range(&path, meta.index_offset..meta.size).await?;
    let (blocks, bloom) = parse_index(tail)?;
    Ok(Run {
        id,
        meta,
        blocks,
        bloom,
    })
}

impl Run {
    /// The block whose keys would include `k`.
    fn block_for(&self, k: &[u8]) -> Option<usize> {
        let i = self
            .blocks
            .partition_point(|(_, _, last)| last.as_ref() < k);
        (i < self.blocks.len()).then_some(i)
    }

    fn blocks_between(&self, start: &[u8], end: &[u8]) -> Vec<usize> {
        let first = self
            .blocks
            .partition_point(|(_, _, last)| last.as_ref() < start);
        let mut out = Vec::new();
        for i in first..self.blocks.len() {
            out.push(i);
            if !end.is_empty() && self.blocks[i].2.as_ref() >= end {
                break;
            }
        }
        out
    }
}

/// Entries of a block: key, then the value or `None` for a delete.
struct Entries {
    b: Bytes,
}

impl Entries {
    fn new(b: Bytes) -> Self {
        Self { b }
    }
}

impl Iterator for Entries {
    type Item = Result<(Bytes, Option<Bytes>)>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.b.is_empty() {
            return None;
        }
        Some(self.entry())
    }
}

impl Entries {
    fn entry(&mut self) -> Result<(Bytes, Option<Bytes>)> {
        let corrupt = || Error::Corrupt("a block's entry runs past its end".into());
        if self.b.len() < 2 {
            return Err(corrupt());
        }
        let kl = usize::from(self.b.get_u16());
        if self.b.len() < kl + 5 {
            return Err(corrupt());
        }
        let k = self.b.split_to(kl);
        let tag = self.b.get_u8();
        let vl = self.b.get_u32() as usize;
        if self.b.len() < vl {
            return Err(corrupt());
        }
        let v = self.b.split_to(vl);
        Ok((k, (tag == 0).then_some(v)))
    }
}

fn find(block: &Bytes, k: &[u8]) -> Result<Option<Option<Bytes>>> {
    for e in Entries::new(block.clone()) {
        let (ek, v) = e?;
        if ek.as_ref() == k {
            return Ok(Some(v));
        }
        if ek.as_ref() > k {
            break;
        }
    }
    Ok(None)
}

struct RunWriter {
    block_bytes: usize,
    out: BytesMut,
    block_start: usize,
    blocks: Vec<(u64, u32, Bytes)>,
    /// Where the last key added starts in `out`, and its length.
    last: (usize, usize),
    hashes: Vec<u64>,
}

impl RunWriter {
    fn new(block_bytes: usize) -> Self {
        Self {
            block_bytes,
            out: BytesMut::new(),
            block_start: 0,
            blocks: Vec::new(),
            last: (0, 0),
            hashes: Vec::new(),
        }
    }

    fn add(&mut self, k: &[u8], v: Option<&[u8]>) {
        self.out.put_u16(u16::try_from(k.len()).unwrap_or(u16::MAX));
        self.last = (self.out.len(), k.len());
        self.out.put_slice(k);
        match v {
            Some(v) => {
                self.out.put_u8(0);
                self.out.put_u32(u32::try_from(v.len()).unwrap_or(u32::MAX));
                self.out.put_slice(v);
            }
            None => {
                self.out.put_u8(1);
                self.out.put_u32(0);
            }
        }
        self.hashes.push(hash(k));
        if self.out.len() - self.block_start >= self.block_bytes {
            self.end_block();
        }
    }

    fn end_block(&mut self) {
        let len = self.out.len() - self.block_start;
        if len == 0 {
            return;
        }
        let (at, n) = self.last;
        self.blocks.push((
            self.block_start as u64,
            u32::try_from(len).unwrap_or(u32::MAX),
            Bytes::copy_from_slice(&self.out[at..at + n]),
        ));
        self.block_start = self.out.len();
    }

    #[allow(clippy::type_complexity)]
    fn finish(mut self, bits_per_key: u32) -> (Bytes, u64, u64, Vec<(u64, u32, Bytes)>, Bloom) {
        self.end_block();
        let index_offset = self.out.len() as u64;
        let bloom = Bloom::build(&self.hashes, bits_per_key);
        self.out
            .put_u32(u32::try_from(self.blocks.len()).unwrap_or(u32::MAX));
        for (off, len, last) in &self.blocks {
            self.out.put_u64(*off);
            self.out.put_u32(*len);
            self.out
                .put_u16(u16::try_from(last.len()).unwrap_or(u16::MAX));
            self.out.put_slice(last);
        }
        self.out.put_u32(bloom.k);
        self.out
            .put_u32(u32::try_from(bloom.bits.len()).unwrap_or(u32::MAX));
        for w in &bloom.bits {
            self.out.put_u64(*w);
        }
        let n = self.hashes.len() as u64;
        (self.out.freeze(), index_offset, n, self.blocks, bloom)
    }
}

#[allow(clippy::type_complexity)]
fn parse_index(mut b: Bytes) -> Result<(Vec<(u64, u32, Bytes)>, Bloom)> {
    let corrupt = || Error::Corrupt("a run's index runs past its end".into());
    if b.len() < 4 {
        return Err(corrupt());
    }
    let n = b.get_u32() as usize;
    let mut blocks = Vec::with_capacity(n);
    for _ in 0..n {
        if b.len() < 14 {
            return Err(corrupt());
        }
        let off = b.get_u64();
        let len = b.get_u32();
        let kl = usize::from(b.get_u16());
        if b.len() < kl {
            return Err(corrupt());
        }
        blocks.push((off, len, b.split_to(kl)));
    }
    if b.len() < 8 {
        return Err(corrupt());
    }
    let k = b.get_u32();
    let words = b.get_u32() as usize;
    if b.len() < words * 8 {
        return Err(corrupt());
    }
    let bits = (0..words).map(|_| b.get_u64()).collect();
    Ok((blocks, Bloom { k, bits }))
}

fn hash(k: &[u8]) -> u64 {
    XxHash3_64::oneshot(k)
}

struct Bloom {
    k: u32,
    bits: Vec<u64>,
}

impl Bloom {
    fn build(hashes: &[u64], bits_per_key: u32) -> Self {
        let nbits = (hashes.len() as u64 * u64::from(bits_per_key)).max(64);
        let words = nbits.div_ceil(64) as usize;
        let k = ((f64::from(bits_per_key) * std::f64::consts::LN_2).round() as u32).clamp(1, 30);
        let mut b = Bloom {
            k,
            bits: vec![0; words],
        };
        for &h in hashes {
            for bit in b.probes(h) {
                b.bits[(bit / 64) as usize] |= 1 << (bit % 64);
            }
        }
        b
    }

    fn probes(&self, h: u64) -> impl Iterator<Item = u64> + use<> {
        let n = self.bits.len() as u64 * 64;
        let h1 = h & 0xffff_ffff;
        let h2 = (h >> 32) | 1;
        (0..u64::from(self.k)).map(move |i| h1.wrapping_add(i.wrapping_mul(h2)) % n)
    }

    fn may_contain(&self, h: u64) -> bool {
        self.probes(h)
            .all(|bit| self.bits[(bit / 64) as usize] & (1 << (bit % 64)) != 0)
    }
}

/// Data blocks in memory, by run and block, about `bytes` of them.
struct BlockCache {
    bytes: usize,
    young: HashMap<(u64, usize), Bytes>,
    old: HashMap<(u64, usize), Bytes>,
    young_bytes: usize,
}

impl BlockCache {
    fn new(bytes: usize) -> Self {
        Self {
            bytes,
            young: HashMap::new(),
            old: HashMap::new(),
            young_bytes: 0,
        }
    }

    fn get(&mut self, run: u64, b: usize) -> Option<Bytes> {
        if let Some(v) = self.young.get(&(run, b)) {
            return Some(v.clone());
        }
        let v = self.old.remove(&(run, b))?;
        self.put(run, b, v.clone());
        Some(v)
    }

    fn put(&mut self, run: u64, b: usize, v: Bytes) {
        self.young_bytes += v.len() + 32;
        self.young.insert((run, b), v);
        if self.young_bytes > self.bytes / 2 {
            self.old = std::mem::take(&mut self.young);
            self.young_bytes = 0;
        }
    }

    fn drop_runs(&mut self, ids: &HashSet<u64>) {
        self.young.retain(|(r, _), _| !ids.contains(r));
        self.old.retain(|(r, _), _| !ids.contains(r));
    }
}

fn flip(t: i64) -> u64 {
    (t as u64) ^ (1 << 63)
}

fn unflip(t: u64) -> i64 {
    (t ^ (1 << 63)) as i64
}

fn encode_snapshot(runs: &[RunMeta]) -> Bytes {
    let mut b = BytesMut::new();
    b.put_u32(u32::try_from(runs.len()).unwrap_or(u32::MAX));
    for r in runs {
        b.put_u16(u16::try_from(r.path.len()).unwrap_or(u16::MAX));
        b.put_slice(r.path.as_bytes());
        b.put_u16(r.range.first);
        b.put_u16(r.range.last);
        b.put_u32(r.level);
        b.put_u64(r.size);
        b.put_u64(r.entries);
        b.put_u64(r.index_offset);
    }
    b.freeze()
}

/// The runs a snapshot names, newest first.
pub fn decode_snapshot(data: &Bytes) -> Result<Vec<RunMeta>> {
    let corrupt = || Error::Corrupt("a snapshot runs past its end".into());
    let mut b = data.clone();
    if b.len() < 4 {
        return Err(corrupt());
    }
    let n = b.get_u32() as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if b.len() < 2 {
            return Err(corrupt());
        }
        let pl = usize::from(b.get_u16());
        if b.len() < pl + 32 {
            return Err(corrupt());
        }
        let path = String::from_utf8(b.split_to(pl).to_vec())
            .map_err(|_| Error::Corrupt("a run's path isn't UTF-8".into()))?;
        out.push(RunMeta {
            path,
            range: KeyGroupRange::new(b.get_u16(), b.get_u16()),
            level: b.get_u32(),
            size: b.get_u64(),
            entries: b.get_u64(),
            index_offset: b.get_u64(),
        });
    }
    Ok(out)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::KeyRef;
    use object_store::memory::InMemory;

    fn k(g: KeyGroup, s: &str) -> Bytes {
        key::value(
            0,
            KeyRef {
                group: g,
                key: s.as_bytes(),
            },
        )
    }

    #[tokio::test]
    async fn runs_restore_onto_other_ranges() {
        let os: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let config = RunConfig {
            fanout: 2,
            ..Default::default()
        };
        let mut snaps = Vec::new();
        for (i, r) in KeyGroupRange::split(8, 2).into_iter().enumerate() {
            let mut s = RunStore::open(
                os.clone(),
                os.clone(),
                Path::from("s"),
                1,
                r,
                &[],
                config.clone(),
            )
            .await
            .unwrap();
            for e in 1..=3u64 {
                let mut ch: Vec<Change> = (r.first..=r.last)
                    .map(|g| (k(g, &format!("k{i}")), Some(Bytes::from(format!("{e}")))))
                    .collect();
                ch.sort();
                let s1 = s.commit(e, 0, ch).await.unwrap();
                if e == 3 {
                    s.settle().await;
                    snaps.extend(s.snapshot());
                    let _ = s1;
                }
            }
        }
        let wide = RunStore::open(
            os.clone(),
            os.clone(),
            Path::from("s"),
            2,
            KeyGroupRange::new(2, 5),
            &snaps,
            config,
        )
        .await
        .unwrap();
        let got = wide
            .get_many(vec![k(2, "k0"), k(5, "k1"), k(5, "k0"), k(1, "k0")])
            .await
            .unwrap();
        assert_eq!(
            got,
            vec![Some(Bytes::from("3")), Some(Bytes::from("3")), None, None]
        );
    }
}
