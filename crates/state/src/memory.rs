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
use crate::store::{BoxFuture, Change, Snapshot, Store, Stores};
use crate::{Error, KeyGroupRange, LAYOUT, NEVER, Result};

/// Versions by name, shared by every handle of the bucket.
#[derive(Clone, Default)]
pub struct MemoryBucket {
    versions: Arc<Mutex<HashMap<String, Arc<Version>>>>,
}

impl MemoryBucket {
    pub fn new() -> Self {
        Self::default()
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

    fn apply(&mut self, epoch: u64, changes: Vec<Change>) -> Result<Vec<Snapshot>> {
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
        let mut horizons = Vec::new();
        for (i, k, v) in placed {
            let g = &mut self.groups[i];
            let map = Arc::make_mut(&mut g.map);
            if let (Some((group, op)), Some(v)) = (layout::parse_horizon(&k), &v) {
                let h = layout::parse_time_value(v)
                    .ok_or_else(|| Error::Corrupt("a horizon that isn't a time".into()))?;
                horizons.push((i, group, op, h));
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
        for (i, group, op, h) in horizons {
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
        self.bytes = self.groups.iter().map(|g| g.bytes).sum();
        let version = Arc::new(Version {
            range: self.range,
            groups: self.groups.clone(),
        });
        self.bucket.lock().insert(name.clone(), version);
        self.committed.push((epoch, name.clone()));
        Ok(vec![Snapshot {
            range: self.range,
            layout: LAYOUT,
            data: Bytes::from(name),
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
        // Keyed state's key groups are ranges of keys; timers are in every
        // group.
        let (lo, hi) = match (layout::keyed_group(start), layout::keyed_group(end)) {
            (Some(a), Some(b)) => (u32::from(a), u32::from(b)),
            _ => (self.range.first, self.range.last),
        };
        if start < end {
            for (n, g) in (self.range.first..).zip(&self.groups) {
                if n < lo || n > hi {
                    continue;
                }
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

    fn commit(&mut self, epoch: u64, changes: Vec<Change>) -> BoxFuture<'_, Result<Vec<Snapshot>>> {
        let snapshots = self.apply(epoch, changes);
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
            let snaps = s.apply(epoch, changes.into_iter().collect()).unwrap();
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
        s.apply(1, changes).unwrap();
        s.apply(
            2,
            vec![(layout::horizon(0, OP), Some(layout::time_value(5)))],
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
        )
        .unwrap();
        assert!(s.groups[0].map.contains_key(&ids[2]));
        assert!(!s.groups[0].map.contains_key(&ids[1]));
    }

    #[test]
    fn a_version_is_never_overwritten() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(0, 0);
        let mut a = bucket.open_store(3, range, &[]).unwrap();
        let mut b = bucket.open_store(3, range, &[]).unwrap();
        a.apply(1, vec![]).unwrap();
        assert!(matches!(b.apply(1, vec![]), Err(Error::Store(_))));
        let mut c = bucket.open_store(4, range, &[]).unwrap();
        c.apply(1, vec![]).unwrap();
        assert_eq!(bucket.len(), 2);
    }

    #[tokio::test]
    async fn release_drops_only_its_own_older_versions() {
        let bucket = MemoryBucket::new();
        let range = KeyGroupRange::new(0, 3);
        let mut old = bucket.open_store(1, range, &[]).unwrap();
        let snap = old.apply(1, vec![]).unwrap();
        let mut s = bucket.open_store(2, range, &snap).unwrap();
        for e in 2..=5 {
            s.apply(e, vec![]).unwrap();
        }
        assert_eq!(bucket.len(), 5);
        s.release(4).await.unwrap();
        assert_eq!(bucket.len(), 3);
        assert!(bucket.open_store(9, range, &snap).is_ok());
    }
}
