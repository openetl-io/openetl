//! A store that keeps its versions in the process's memory, for tests.
//!
//! A [`MemoryBucket`] stands where object storage would: lanes commit
//! versions to it by name, `g{generation}/e{epoch}/kg{first}-{last}`, and
//! never overwrite one, so an older attempt's versions can't be taken for a
//! newer one's. A test kills lanes by dropping their layers, and restores
//! from the bucket with the snapshots a record kept, on any number of
//! lanes. Each key group is a map of its own, shared by the versions that
//! didn't change it, and a barrier copies each group the epoch changed in
//! full. Nothing in the bucket outlives the process.

use std::collections::{BTreeMap, HashMap};
use std::ops::Bound;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::Bytes;

use crate::layer::ENTRY_OVERHEAD;
use crate::layout;
use crate::store::{BoxFuture, Change, Horizons, PendingLimit, Snapshot, Store, Stores};
use crate::{Error, KeyGroupRange, LAYOUT, NEVER, Result};

/// Versions by name, shared by every handle of the bucket.
#[derive(Clone, Default)]
pub struct MemoryBucket {
    versions: Arc<Mutex<HashMap<String, Arc<Version>>>>,
    pending: PendingLimit,
}

impl MemoryBucket {
    pub fn new() -> Self {
        Self::default()
    }

    /// The bucket, with lanes that carry their pending sets in records
    /// within `limit`; [`MemoryBucket::new`]'s commit at every barrier.
    pub fn with_pending(mut self, limit: PendingLimit) -> Self {
        self.pending = limit;
        self
    }

    /// How many versions it holds.
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, Arc<Version>>> {
        self.versions.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn open_store(
        &self,
        generation: u64,
        range: KeyGroupRange,
        snapshots: &[Snapshot],
    ) -> Result<MemoryStore> {
        let mut sources: Vec<Arc<Version>> = Vec::with_capacity(snapshots.len());
        {
            let versions = self.lock();
            for s in snapshots {
                let name = std::str::from_utf8(&s.data)
                    .map_err(|_| Error::Snapshot("not a memory store's".into()))?;
                let v = versions
                    .get(name)
                    .ok_or_else(|| Error::Snapshot(format!("no version {name}")))?;
                if v.range != s.range {
                    return Err(Error::Snapshot(format!(
                        "{name} holds key groups {} to {}, and its snapshot says {} to {}",
                        v.range.first, v.range.last, s.range.first, s.range.last
                    )));
                }
                sources.push(v.clone());
            }
        }
        for (i, a) in sources.iter().enumerate() {
            if let Some(b) = sources[i + 1..].iter().find(|b| a.range.overlaps(&b.range)) {
                return Err(Error::Snapshot(format!(
                    "snapshots of key groups {} to {} and {} to {} overlap",
                    a.range.first, a.range.last, b.range.first, b.range.last
                )));
            }
        }
        let mut groups = Vec::new();
        let horizons: Horizons = sources
            .iter()
            .flat_map(|v| v.horizons.iter())
            .filter(|((_, g), _)| range.contains(*g))
            .map(|(k, h)| (*k, *h))
            .collect();
        for g in range.first..=range.last {
            let group = match sources.iter().find(|v| v.range.contains(g)) {
                Some(v) => v.group(g)?,
                None if sources.is_empty() => Group::default(),
                None => {
                    return Err(Error::Snapshot(format!("no snapshot holds key group {g}")));
                }
            };
            groups.push(group);
        }
        let bytes = groups.iter().map(|g| g.bytes).sum();
        Ok(MemoryStore {
            bucket: self.clone(),
            generation,
            range,
            groups,
            bytes,
            horizons,
            committed: Vec::new(),
        })
    }
}

impl Stores for MemoryBucket {
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

    fn pending_share(&self, range: KeyGroupRange) -> usize {
        self.pending.share(range)
    }
}

/// One key group's entries, with their size.
#[derive(Clone, Default)]
struct Group {
    map: Arc<BTreeMap<Bytes, Bytes>>,
    bytes: usize,
}

struct Version {
    range: KeyGroupRange,
    groups: Vec<Group>,
    horizons: Horizons,
}

impl Version {
    fn group(&self, g: u32) -> Result<Group> {
        g.checked_sub(self.range.first)
            .and_then(|i| usize::try_from(i).ok())
            .and_then(|i| self.groups.get(i))
            .cloned()
            .ok_or_else(|| Error::Corrupt(format!("a version without key group {g}")))
    }
}

pub struct MemoryStore {
    bucket: MemoryBucket,
    generation: u64,
    range: KeyGroupRange,
    /// Group `range.first + i` at `i`.
    groups: Vec<Group>,
    bytes: usize,
    horizons: Horizons,
    /// The versions it committed, by epoch.
    committed: Vec<(u64, String)>,
}

fn size(k: &[u8], v: &[u8]) -> usize {
    k.len() + v.len() + ENTRY_OVERHEAD
}

impl MemoryStore {
    /// The index of a key's group, if the store holds it.
    fn index(&self, k: &[u8]) -> Option<usize> {
        let g = u32::from(layout::group_of(k)?);
        if !self.range.contains(g) {
            return None;
        }
        usize::try_from(g - self.range.first).ok()
    }

    /// The groups a scan of `[start, end)` reads: for keyed state, those
    /// from its start's group to its end's, which bound every key between
    /// them; for timers, which are in every group, all of them.
    fn span(&self, start: &[u8], end: &[u8]) -> &[Group] {
        let (Some(a), Some(b)) = (layout::keyed_group(start), layout::keyed_group(end)) else {
            return &self.groups;
        };
        let a = u32::from(a).max(self.range.first);
        let b = u32::from(b).min(self.range.last);
        let at = |g: u32| usize::try_from(g - self.range.first).unwrap_or(usize::MAX);
        match a <= b {
            true => self.groups.get(at(a)..=at(b)).unwrap_or(&[]),
            false => &[],
        }
    }

    fn apply(
        &mut self,
        epoch: u64,
        changes: Vec<Change>,
        horizons: &Horizons,
    ) -> Result<Vec<Snapshot>> {
        let name = format!(
            "g{}/e{epoch}/kg{}-{}",
            self.generation, self.range.first, self.range.last
        );
        if self.bucket.lock().contains_key(&name) {
            return Err(Error::Store(format!("version {name} exists")));
        }
        let mut placed = Vec::with_capacity(changes.len());
        for (k, v) in changes {
            let i = self.index(&k).ok_or_else(|| {
                Error::Invalid(format!(
                    "a change outside key groups {} to {}",
                    self.range.first, self.range.last
                ))
            })?;
            placed.push((i, k, v));
        }
        let mut kept = Vec::new();
        for (i, k, v) in placed {
            let g = &mut self.groups[i];
            let map = Arc::make_mut(&mut g.map);
            if let (Some((group, op)), Some(v)) = (layout::parse_horizon(&k), &v) {
                let h = layout::parse_time_value(v)
                    .ok_or_else(|| Error::Corrupt("a horizon that isn't a time".into()))?;
                kept.push((i, group, op, h));
            }
            let old = match v {
                Some(v) => {
                    g.bytes += size(&k, &v);
                    map.insert(k.clone(), v)
                }
                None => map.remove(&k),
            };
            if let Some(old) = old {
                g.bytes -= size(&k, &old);
            }
        }
        // Set entries at or before their group's new horizon can never be
        // live again.
        for (i, group, op, h) in kept {
            let (start, end) = layout::set_range(group, op);
            let g = &mut self.groups[i];
            let map = Arc::make_mut(&mut g.map);
            let gone: Vec<Bytes> = map
                .range::<[u8], _>((Bound::Included(&start[..]), Bound::Excluded(&end[..])))
                .filter(|(_, v)| layout::parse_time_value(v).is_some_and(|e| e != NEVER && e <= h))
                .map(|(k, _)| k.clone())
                .collect();
            for k in gone {
                if let Some(v) = map.remove(&k) {
                    g.bytes -= size(&k, &v);
                }
            }
        }
        // What an operator's horizon moved past is gone: its rows and
        // timers, and its set's entries that expire.
        for (&(op, group), &h) in horizons {
            let moved = self.horizons.get(&(op, group)).is_none_or(|was| h > *was);
            if !moved || h == i64::MIN {
                continue;
            }
            let (Some(i), Ok(gr)) = (
                group
                    .checked_sub(self.range.first)
                    .and_then(|i| usize::try_from(i).ok())
                    .filter(|_| self.range.contains(group)),
                u16::try_from(group),
            ) else {
                continue;
            };
            self.horizons.insert((op, group), h);
            let g = &mut self.groups[i];
            let map = Arc::make_mut(&mut g.map);
            let (set, set_end) = layout::set_range(gr, op);
            let rows = layout::rows_due(gr, op, h);
            let timers = layout::timers_due(op, h);
            let mut gone: Vec<Bytes> = Vec::new();
            for (start, end) in [(set, set_end), rows, timers] {
                let range = (Bound::Included(&start[..]), Bound::Excluded(&end[..]));
                gone.extend(
                    map.range::<[u8], _>(range)
                        .filter(|(k, v)| match layout::section_of(k) {
                            Some(layout::SectionKind::Set) => {
                                layout::parse_time_value(v).is_some_and(|e| e != NEVER && e <= h)
                            }
                            _ => true,
                        })
                        .map(|(k, _)| k.clone()),
                );
            }
            for k in gone {
                if let Some(v) = map.remove(&k) {
                    g.bytes -= size(&k, &v);
                }
            }
        }
        self.bytes = self.groups.iter().map(|g| g.bytes).sum();
        let version = Arc::new(Version {
            range: self.range,
            groups: self.groups.clone(),
            horizons: self.horizons.clone(),
        });
        self.bucket.lock().insert(name.clone(), version);
        self.committed.push((epoch, name.clone()));
        Ok(vec![Snapshot {
            range: self.range,
            layout: LAYOUT,
            data: Bytes::from(name),
            pending: Bytes::new(),
        }])
    }
}

impl Store for MemoryStore {
    fn get<'a>(&'a self, keys: &'a [Bytes]) -> BoxFuture<'a, Result<Vec<Option<Bytes>>>> {
        let got = keys
            .iter()
            .map(|k| {
                let g = self.groups.get(self.index(k)?)?;
                g.map.get(k).cloned()
            })
            .collect();
        Box::pin(std::future::ready(Ok(got)))
    }

    fn scan<'a>(
        &'a self,
        start: &'a [u8],
        end: &'a [u8],
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<(Bytes, Bytes)>>> {
        let mut out: Vec<(Bytes, Bytes)> = Vec::new();
        if start < end {
            for g in self.span(start, end) {
                let range = (Bound::Included(start), Bound::Excluded(end));
                out.extend(
                    g.map
                        .range::<[u8], _>(range)
                        .take(limit)
                        .map(|(k, v)| (k.clone(), v.clone())),
                );
            }
            out.sort_by(|a, b| a.0.cmp(&b.0));
            out.truncate(limit);
        }
        Box::pin(std::future::ready(Ok(out)))
    }

    fn horizons(&self) -> Horizons {
        self.horizons.clone()
    }

    fn may_hold(&self, key: &[u8]) -> bool {
        self.index(key)
            .and_then(|i| self.groups.get(i))
            .is_some_and(|g| g.map.contains_key(key))
    }

    fn commit<'a>(
        &'a mut self,
        epoch: u64,
        changes: Vec<Change>,
        horizons: &'a Horizons,
    ) -> BoxFuture<'a, Result<Vec<Snapshot>>> {
        let snapshots = self.apply(epoch, changes, horizons);
        Box::pin(std::future::ready(snapshots))
    }

    fn release(&mut self, epoch: u64) -> BoxFuture<'_, Result<()>> {
        let bucket = self.bucket.clone();
        self.committed.retain(|(e, name)| {
            if *e < epoch {
                bucket.lock().remove(name);
            }
            *e >= epoch
        });
        Box::pin(std::future::ready(Ok(())))
    }

    fn memory_bytes(&self) -> usize {
        self.bytes
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::OperatorId;

    const OP: OperatorId = OperatorId(1);
    static NONE: Horizons = Horizons::new();

    fn counted(s: &MemoryStore) -> usize {
        s.groups
            .iter()
            .flat_map(|g| g.map.iter())
            .map(|(k, v)| size(k, v))
            .sum()
    }

    fn next(seed: &mut u64) -> u64 {
        *seed = seed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = *seed;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// Its count of bytes is what its entries hold, through inserts,
    /// replacements, deletes and expiry, and a version doesn't change once
    /// committed.
    #[test]
    fn bytes_follow_the_entries_and_versions_stay() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(4, 7);
        let mut s = bucket.open_store(1, range, &[]).unwrap();
        let mut seed = 11;
        let mut kept: Vec<(Snapshot, BTreeMap<Bytes, Bytes>)> = Vec::new();
        for epoch in 1..=40u64 {
            let mut changes = BTreeMap::new();
            for _ in 0..(next(&mut seed) % 20) {
                let g = 4 + (next(&mut seed) % 4) as u16;
                let id = [b'a' + (next(&mut seed) % 5) as u8];
                let k = match next(&mut seed) % 3 {
                    0 => layout::value(g, OP, &id),
                    1 => layout::set(g, OP, &id),
                    _ => layout::horizon(g, OP),
                };
                let v = match next(&mut seed) % 4 {
                    0 => None,
                    _ => Some(layout::time_value((next(&mut seed) % 50) as i64)),
                };
                changes.insert(k, v);
            }
            let snaps = s
                .apply(epoch, changes.into_iter().collect(), &NONE)
                .unwrap();
            assert_eq!(s.memory_bytes(), counted(&s), "epoch {epoch}");
            let all: BTreeMap<Bytes, Bytes> = s
                .groups
                .iter()
                .flat_map(|g| g.map.iter().map(|(k, v)| (k.clone(), v.clone())))
                .collect();
            kept.push((snaps[0].clone(), all));
        }
        for (snap, want) in kept {
            let r = bucket.open_store(2, range, &[snap]).unwrap();
            let got: BTreeMap<Bytes, Bytes> = r
                .groups
                .iter()
                .flat_map(|g| g.map.iter().map(|(k, v)| (k.clone(), v.clone())))
                .collect();
            assert_eq!(got, want);
            assert_eq!(r.memory_bytes(), counted(&r));
        }
    }

    #[test]
    fn a_horizon_drops_what_it_expired_and_nothing_else() {
        let bucket = MemoryBucket::new();
        let mut s = bucket.open_store(1, KeyGroupRange::new(0, 1), &[]).unwrap();
        let entries = [(b"a", 5), (b"b", 6), (b"c", NEVER)];
        let mut changes: Vec<Change> = entries
            .iter()
            .map(|(id, e)| (layout::set(0, OP, *id), Some(layout::time_value(*e))))
            .collect();
        changes.push((layout::set(1, OP, b"a"), Some(layout::time_value(1))));
        changes.sort();
        s.apply(1, changes, &NONE).unwrap();
        s.apply(
            2,
            vec![(layout::horizon(0, OP), Some(layout::time_value(5)))],
            &NONE,
        )
        .unwrap();
        let ids = [b"a", b"b", b"c"].map(|id| layout::set(0, OP, id));
        let left: Vec<bool> = ids
            .iter()
            .map(|k| s.groups[0].map.contains_key(k))
            .collect();
        assert_eq!(left, [false, true, true]);
        // Another group's horizon is its own.
        assert!(s.groups[1].map.contains_key(&layout::set(1, OP, b"a")));
        s.apply(
            3,
            vec![(layout::horizon(0, OP), Some(layout::time_value(i64::MAX)))],
            &NONE,
        )
        .unwrap();
        assert!(s.groups[0].map.contains_key(&ids[2]));
        assert!(!s.groups[0].map.contains_key(&ids[1]));
    }

    /// A keyed scan reads the groups its bounds select, so draining every
    /// group of a lane reads each once, not every group per group.
    #[test]
    fn a_scan_reads_only_the_groups_its_bounds_select() {
        let bucket = MemoryBucket::new();
        let all = bucket
            .open_store(1, KeyGroupRange::new(0, u32::from(u16::MAX)), &[])
            .unwrap();
        let reads = |s: &MemoryStore, (start, end): (Bytes, Bytes)| s.span(&start, &end).len();
        let mut visits = 0;
        for g in 0..=u16::MAX {
            visits += reads(&all, layout::rows_due(g, OP, 5));
        }
        assert_eq!(visits, 1 << 16);
        assert_eq!(reads(&all, layout::rows_due(u16::MAX, OP, i64::MAX)), 1);
        assert_eq!(reads(&all, layout::set_range(9, OP)), 1);
        assert_eq!(reads(&all, layout::timers_due(OP, 5)), 1 << 16);
        let part = bucket
            .open_store(1, KeyGroupRange::new(100, 199), &[])
            .unwrap();
        assert_eq!(reads(&part, layout::rows_due(150, OP, 5)), 1);
        assert_eq!(reads(&part, layout::rows_due(99, OP, 5)), 0);
        assert_eq!(reads(&part, layout::rows_due(200, OP, 5)), 0);
        let wide = (layout::set_range(50, OP).0, layout::set_range(150, OP).1);
        assert_eq!(reads(&part, wide), 51);
        assert_eq!(reads(&part, layout::timers_due(OP, i64::MAX)), 100);
    }

    /// An operator's horizon drops its rows, timers and expiring set
    /// entries at or before it, in its group alone, and a version keeps it
    /// for the stores that open it.
    #[test]
    fn an_operators_horizon_drops_what_it_passed_and_stays_with_the_version() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(0, 1);
        let mut s = bucket.open_store(1, range, &[]).unwrap();
        let other = OperatorId(2);
        let mut changes: Vec<Change> = Vec::new();
        for g in 0..2u16 {
            for op in [OP, other] {
                for t in [5, 15] {
                    changes.push((layout::row(g, op, t, b"k", 1), Some(Bytes::new())));
                    changes.push((layout::timer(op, t, g, b"k"), Some(Bytes::new())));
                }
                changes.push((layout::set(g, op, b"x"), Some(layout::time_value(5))));
                changes.push((layout::set(g, op, b"y"), Some(layout::time_value(NEVER))));
            }
        }
        changes.sort();
        s.apply(1, changes.clone(), &NONE).unwrap();
        let horizons = Horizons::from([((OP, 0), 10)]);
        let snap = s.apply(2, vec![], &horizons).unwrap();
        let held = |s: &MemoryStore| -> usize { s.groups.iter().map(|g| g.map.len()).sum() };
        assert_eq!(held(&s), changes.len() - 3);
        for gone in [
            layout::row(0, OP, 5, b"k", 1),
            layout::timer(OP, 5, 0, b"k"),
            layout::set(0, OP, b"x"),
        ] {
            assert!(!s.groups[0].map.contains_key(&gone));
        }
        let r = bucket.open_store(2, range, &snap).unwrap();
        assert_eq!(r.horizons(), horizons);
        assert_eq!(held(&r), changes.len() - 3);
        assert_eq!(r.memory_bytes(), counted(&r));
    }

    #[test]
    fn a_version_is_never_overwritten() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(0, 0);
        let mut a = bucket.open_store(3, range, &[]).unwrap();
        let mut b = bucket.open_store(3, range, &[]).unwrap();
        a.apply(1, vec![], &NONE).unwrap();
        assert!(matches!(b.apply(1, vec![], &NONE), Err(Error::Store(_))));
        let mut c = bucket.open_store(4, range, &[]).unwrap();
        c.apply(1, vec![], &NONE).unwrap();
        assert_eq!(bucket.len(), 2);
    }

    #[tokio::test]
    async fn release_drops_only_its_own_older_versions() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(0, 3);
        let mut old = bucket.open_store(1, range, &[]).unwrap();
        let snap = old.apply(1, vec![], &NONE).unwrap();
        let mut s = bucket.open_store(2, range, &snap).unwrap();
        for e in 2..=5 {
            s.apply(e, vec![], &NONE).unwrap();
        }
        assert_eq!(bucket.len(), 5);
        s.release(4).await.unwrap();
        assert_eq!(bucket.len(), 3);
        assert!(bucket.open_store(9, range, &snap).is_ok());
    }
}
