//! The memory tier over a store: the epoch's changes, a cache of hot keys,
//! and the timers of the lane's range.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use bytes::Bytes;

use crate::key::{self, Kind, OperatorId};
use crate::{Change, KeyGroup, KeyGroupRange, KeyRef, Result, Snapshot, StateBackend, Store};

/// How many bytes of hot keys the cache keeps, about.
pub const DEFAULT_CACHE_BYTES: usize = 64 << 20;

/// Scans of the store in flight at once.
const SCANS: usize = 64;

/// Counts of what the layer served from memory and what it asked the store.
#[derive(Clone, Copy, Default, Debug)]
pub struct LayerStats {
    pub lookups: u64,
    /// Lookups the epoch's changes or the cache answered.
    pub hits: u64,
    /// Lookups that went to the store.
    pub misses: u64,
    /// Bytes of changes committed: keys and values as the store got them.
    pub committed_bytes: u64,
    pub committed_entries: u64,
}

pub struct Layer<S> {
    store: S,
    range: KeyGroupRange,
    /// The epoch's changes: a value, or `None` for a delete.
    dirty: BTreeMap<Bytes, Option<Bytes>>,
    /// Hot keys as the store has them; `None` for a key it doesn't have.
    /// Two generations, so lookups keep the hot ones as the cache turns over.
    young: HashMap<Bytes, Option<Bytes>>,
    old: HashMap<Bytes, Option<Bytes>>,
    young_bytes: usize,
    cache_bytes: usize,
    /// Every timer of the range: its time, then its store key.
    timers: BTreeSet<(i64, Bytes)>,
    stats: LayerStats,
    /// What the last barrier returned.
    last: Vec<Snapshot>,
}

impl<S: Store> Layer<S> {
    /// A layer over a store opened for `range`. `timers` are the restored
    /// timers' store keys, from [`load_timers`].
    pub fn new(store: S, range: KeyGroupRange, cache_bytes: usize, timers: Vec<Bytes>) -> Self {
        let timers = timers
            .into_iter()
            .filter_map(|k| key::parse_timer(&k).map(|(t, _, _)| (t, k)))
            .collect();
        Self {
            store,
            range,
            dirty: BTreeMap::new(),
            young: HashMap::new(),
            old: HashMap::new(),
            young_bytes: 0,
            cache_bytes,
            timers,
            stats: LayerStats::default(),
            last: Vec::new(),
        }
    }

    pub fn store(&self) -> &S {
        &self.store
    }

    pub fn store_mut(&mut self) -> &mut S {
        &mut self.store
    }

    pub fn range(&self) -> KeyGroupRange {
        self.range
    }

    pub fn stats(&self) -> LayerStats {
        self.stats
    }

    /// What the last barrier returned, for the record.
    pub fn last_snapshots(&self) -> &[Snapshot] {
        &self.last
    }

    /// Changes waiting for the barrier.
    pub fn dirty_len(&self) -> usize {
        self.dirty.len()
    }

    fn remember(&mut self, k: Bytes, v: Option<Bytes>) {
        let size = k.len() + v.as_ref().map_or(0, |v| v.len()) + 48;
        self.young_bytes += size;
        self.young.insert(k, v);
        if self.young_bytes > self.cache_bytes / 2 {
            self.old = std::mem::take(&mut self.young);
            self.young_bytes = 0;
        }
    }

    fn cached(&mut self, k: &Bytes) -> Option<Option<Bytes>> {
        if let Some(v) = self.young.get(k) {
            return Some(v.clone());
        }
        let v = self.old.remove(k)?;
        self.remember(k.clone(), v.clone());
        Some(v)
    }

    fn forget(&mut self, k: &Bytes) {
        self.young.remove(k);
        self.old.remove(k);
    }

    fn change(&mut self, k: Bytes, v: Option<Bytes>) {
        self.forget(&k);
        self.dirty.insert(k, v);
    }

    /// The newest value of each store key: the epoch's changes, the cache,
    /// then the store for what's left, in one call.
    async fn lookup(&mut self, keys: Vec<Bytes>) -> Result<Vec<Option<Bytes>>> {
        let mut out = vec![None; keys.len()];
        let mut missing = Vec::new();
        let mut at = Vec::new();
        for (i, k) in keys.into_iter().enumerate() {
            self.stats.lookups += 1;
            if let Some(v) = self.dirty.get(&k) {
                self.stats.hits += 1;
                out[i] = v.clone();
            } else if let Some(v) = self.cached(&k) {
                self.stats.hits += 1;
                out[i] = v;
            } else {
                self.stats.misses += 1;
                missing.push(k);
                at.push(i);
            }
        }
        if missing.is_empty() {
            return Ok(out);
        }
        let got = self.store.get_many(missing.clone()).await?;
        for ((k, v), i) in missing.into_iter().zip(got).zip(at) {
            self.remember(k, v.clone());
            out[i] = v;
        }
        Ok(out)
    }
}

/// The store keys of every timer in `range`, for [`Layer::new`].
pub async fn load_timers<S: Store>(
    store: &S,
    range: KeyGroupRange,
    ops: &[OperatorId],
) -> Result<Vec<Bytes>> {
    let ranges: Vec<(Bytes, Bytes)> = range
        .groups()
        .flat_map(|g| {
            ops.iter()
                .map(move |&op| key::kind_range(g, op, Kind::Timer))
        })
        .collect();
    let found: Vec<Vec<(Bytes, Bytes)>> = {
        let scans: Vec<_> = ranges.into_iter().map(|(s, e)| store.scan(s, e)).collect();
        crate::unordered(scans, SCANS).await?
    };
    Ok(found.into_iter().flatten().map(|(k, _)| k).collect())
}

impl<S: Store> StateBackend for Layer<S> {
    async fn get(&mut self, op: OperatorId, keys: &[KeyRef<'_>]) -> Result<Vec<Option<Bytes>>> {
        let ks = keys.iter().map(|k| key::value(op, *k)).collect();
        self.lookup(ks).await
    }

    fn put(&mut self, op: OperatorId, k: KeyRef<'_>, value: Bytes) {
        self.change(key::value(op, k), Some(value));
    }

    fn delete(&mut self, op: OperatorId, k: KeyRef<'_>) {
        self.change(key::value(op, k), None);
    }

    fn append(&mut self, op: OperatorId, k: KeyRef<'_>, seq: u64, row: Bytes) {
        self.dirty.insert(key::row(op, k, seq), Some(row));
    }

    async fn take_rows(
        &mut self,
        op: OperatorId,
        keys: &[KeyRef<'_>],
    ) -> Result<Vec<Vec<(u64, Bytes)>>> {
        let ranges: Vec<(Bytes, Bytes)> = keys
            .iter()
            .map(|k| {
                let p = key::rows(op, *k);
                let e = key::successor(&p);
                (p, e)
            })
            .collect();
        let store = &self.store;
        let stored: Vec<Vec<(Bytes, Bytes)>> = {
            let scans: Vec<_> = ranges
                .iter()
                .map(|(s, e)| store.scan(s.clone(), e.clone()))
                .collect();
            crate::unordered(scans, SCANS).await?
        };
        let mut out = Vec::with_capacity(keys.len());
        for ((s, e), stored) in ranges.into_iter().zip(stored) {
            let mut rows: BTreeMap<Bytes, Option<Bytes>> =
                stored.into_iter().map(|(k, v)| (k, Some(v))).collect();
            for (k, v) in self.dirty.range(s..e) {
                rows.insert(k.clone(), v.clone());
            }
            let mut taken = Vec::with_capacity(rows.len());
            for (rk, v) in rows {
                if let Some(v) = v {
                    taken.push((key::row_seq(&rk), v));
                    self.dirty.insert(rk, None);
                }
            }
            out.push(taken);
        }
        Ok(out)
    }

    async fn insert_new(
        &mut self,
        op: OperatorId,
        ids: &[KeyRef<'_>],
        at: &[i64],
        expires: &[i64],
    ) -> Result<Vec<bool>> {
        let ks: Vec<Bytes> = ids.iter().map(|k| key::set(op, *k)).collect();
        let found = self.lookup(ks.clone()).await?;
        let mut out = Vec::with_capacity(ks.len());
        for (i, (k, v)) in ks.into_iter().zip(found).enumerate() {
            let live = match self.dirty.get(&k) {
                Some(Some(v)) => key::parse_expiry(v) > at[i],
                Some(None) => false,
                None => v.is_some_and(|v| key::parse_expiry(&v) > at[i]),
            };
            if !live {
                self.dirty.insert(k, Some(key::expiry(expires[i])));
            }
            out.push(!live);
        }
        Ok(out)
    }

    fn set_timer(&mut self, op: OperatorId, time: i64, k: KeyRef<'_>) {
        let tk = key::timer(op, time, k);
        if self.timers.insert((time, tk.clone())) {
            self.dirty.insert(tk, Some(Bytes::new()));
        }
    }

    fn fire(&mut self, op: OperatorId, watermark: i64) -> Vec<(i64, KeyGroup, Bytes)> {
        let mut fired = Vec::new();
        let due: Vec<(i64, Bytes)> = self
            .timers
            .range(..(watermark.saturating_add(1), Bytes::new()))
            .filter(|(_, k)| k.len() > 4 && u16::from_be_bytes([k[2], k[3]]) == op)
            .cloned()
            .collect();
        for (t, k) in due {
            self.timers.remove(&(t, k.clone()));
            if let Some((time, group, user)) = key::parse_timer(&k) {
                fired.push((time, group, user));
            }
            self.dirty.insert(k, None);
        }
        fired
    }

    async fn barrier(&mut self, epoch: u64, watermark: i64) -> Result<Vec<Snapshot>> {
        let dirty = std::mem::take(&mut self.dirty);
        let mut changes: Vec<Change> = Vec::with_capacity(dirty.len());
        for (k, v) in dirty {
            self.stats.committed_entries += 1;
            self.stats.committed_bytes += (k.len() + v.as_ref().map_or(0, |v| v.len())) as u64;
            changes.push((k, v));
        }
        let snaps = self.store.commit(epoch, watermark, changes.clone()).await?;
        // What the epoch changed is the hottest state there is.
        for (k, v) in changes {
            if matches!(key::kind_of(&k), Some(Kind::Value | Kind::Set)) {
                self.remember(k, v);
            }
        }
        self.last = snaps.clone();
        Ok(snaps)
    }
}
