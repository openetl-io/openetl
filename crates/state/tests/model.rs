//! The layer against a model of a few maps, over random sequences of the
//! primitives, barriers, recorded or not, failed epochs, kills and restores
//! of a recorded epoch onto other numbers of lanes, older attempts still
//! writing, and released versions, over each store. Each seed is a
//! sequence: `OPENETL_STATE_SEEDS=n` runs n of them, and
//! `OPENETL_STATE_SEED=s` the one.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::{
    DueRow, KeyGroupRange, KeyRef, Layer, MemoryBucket, NEVER, ObjectStorage, OperatorId, Result,
    RunConfig, RunStores, Snapshot, StateBackend, Stores, Timer,
};

const GROUPS: u32 = 16;
const OPS: [OperatorId; 2] = [OperatorId(3), OperatorId(u32::MAX)];
const KEYS: [&[u8]; 7] = [b"", b"a", b"b", b"ab", b"ba", b"\x00", b"\xff\xff"];
const LANES: [u32; 6] = [1, 2, 3, 4, 5, 8];
const STEPS: usize = 400;

type Key = Vec<u8>;

/// The state every lane of an attempt holds together, as plain maps.
#[derive(Clone, Default, Debug)]
struct Logical {
    values: BTreeMap<(OperatorId, u32, Key), Bytes>,
    /// By operator, group, time, key length and key: the order rows leave
    /// in. Each key's in the order appended.
    rows: BTreeMap<(OperatorId, u32, i64, usize, Key), Vec<Bytes>>,
    set: BTreeMap<(OperatorId, u32, Key), i64>,
    horizons: BTreeMap<(OperatorId, u32), i64>,
    /// By operator, time, group, key length and key.
    timers: BTreeSet<(OperatorId, i64, u32, usize, Key)>,
}

impl Logical {
    fn horizon(&self, op: OperatorId, g: u32) -> i64 {
        self.horizons.get(&(op, g)).copied().unwrap_or(i64::MIN)
    }

    /// Whether it holds anything in `range` that a store must keep: all
    /// but set entries the horizon has expired.
    fn holds(&self, range: KeyGroupRange) -> bool {
        self.values.keys().any(|k| range.contains(k.1))
            || self.rows.keys().any(|k| range.contains(k.1))
            || self.horizons.keys().any(|k| range.contains(k.1))
            || self.timers.iter().any(|k| range.contains(k.2))
            || self.set.iter().any(|((op, g, _), &e)| {
                range.contains(*g) && (e == NEVER || e > self.horizon(*op, *g))
            })
    }
}

/// The model as one lane sees it.
struct View<'a> {
    s: &'a mut Logical,
    range: KeyGroupRange,
}

fn kid(k: &KeyRef<'_>) -> Key {
    k.key.to_vec()
}

impl StateBackend for View<'_> {
    async fn get(&mut self, op: OperatorId, keys: &[KeyRef<'_>]) -> Result<Vec<Option<Bytes>>> {
        Ok(keys
            .iter()
            .map(|k| self.s.values.get(&(op, k.group, kid(k))).cloned())
            .collect())
    }

    fn put(&mut self, op: OperatorId, key: KeyRef<'_>, value: Bytes) {
        self.s.values.insert((op, key.group, kid(&key)), value);
    }

    fn delete(&mut self, op: OperatorId, key: KeyRef<'_>) {
        self.s.values.remove(&(op, key.group, kid(&key)));
    }

    fn append(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64, row: Bytes) {
        let k = (op, key.group, time, key.key.len(), kid(&key));
        self.s.rows.entry(k).or_default().push(row);
    }

    async fn rows(
        &mut self,
        op: OperatorId,
        slots: &[(KeyRef<'_>, i64)],
    ) -> Result<Vec<Vec<Bytes>>> {
        Ok(slots
            .iter()
            .map(|(k, t)| {
                let k = (op, k.group, *t, k.key.len(), kid(k));
                self.s.rows.get(&k).cloned().unwrap_or_default()
            })
            .collect())
    }

    async fn take_rows(
        &mut self,
        op: OperatorId,
        watermark: i64,
        limit: usize,
    ) -> Result<Vec<DueRow>> {
        let mut out = Vec::new();
        let mut taken = Vec::new();
        for (k, rows) in &self.s.rows {
            if out.len() >= limit {
                break;
            }
            let (o, g, t, _, key) = k;
            if *o != op || !self.range.contains(*g) || *t > watermark {
                continue;
            }
            let n = rows.len().min(limit - out.len());
            out.extend(rows[..n].iter().map(|row| DueRow {
                group: *g,
                time: *t,
                key: Bytes::from(key.clone()),
                row: row.clone(),
            }));
            taken.push((k.clone(), n));
        }
        for (k, n) in taken {
            let rows = self.s.rows.get_mut(&k).unwrap();
            rows.drain(..n);
            if rows.is_empty() {
                self.s.rows.remove(&k);
            }
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
        let mut out = Vec::new();
        for (i, id) in ids.iter().enumerate() {
            let k = (op, id.group, kid(id));
            let now = at[i].max(self.s.horizon(op, id.group));
            let live = self.s.set.get(&k).is_some_and(|&e| e == NEVER || e > now);
            if !live && (expires[i] == NEVER || expires[i] > now) {
                self.s.set.insert(k, expires[i]);
            }
            out.push(!live);
        }
        Ok(out)
    }

    fn expire(&mut self, op: OperatorId, horizon: i64) {
        for g in self.range.first..=self.range.last {
            if horizon > self.s.horizon(op, g) {
                self.s.horizons.insert((op, g), horizon);
            }
        }
    }

    fn set_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64) {
        self.s
            .timers
            .insert((op, time, key.group, key.key.len(), kid(&key)));
    }

    fn delete_timer(&mut self, op: OperatorId, key: KeyRef<'_>, time: i64) {
        self.s
            .timers
            .remove(&(op, time, key.group, key.key.len(), kid(&key)));
    }

    async fn fire(&mut self, op: OperatorId, watermark: i64, limit: usize) -> Result<Vec<Timer>> {
        let due: Vec<_> = self
            .s
            .timers
            .iter()
            .filter(|(o, t, g, _, _)| *o == op && *t <= watermark && self.range.contains(*g))
            .take(limit)
            .cloned()
            .collect();
        for t in &due {
            self.s.timers.remove(t);
        }
        Ok(due
            .into_iter()
            .map(|(_, time, group, _, key)| Timer {
                time,
                group,
                key: Bytes::from(key),
            })
            .collect())
    }
}

/// SplitMix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }

    fn pick<T: Copy>(&mut self, of: &[T]) -> T {
        of[self.below(of.len())]
    }

    fn between(&mut self, lo: i64, hi: i64) -> i64 {
        lo + self.below((hi - lo + 1) as usize) as i64
    }

    /// Times that collide often, and the extremes.
    fn time(&mut self) -> i64 {
        match self.below(10) {
            0 => i64::MIN,
            1 => i64::MAX,
            2 => self.between(-100, 100),
            _ => self.between(-4, 12),
        }
    }

    fn watermark(&mut self) -> i64 {
        match self.below(20) {
            0 => i64::MIN,
            1 | 2 => i64::MAX,
            _ => self.between(-6, 14),
        }
    }

    fn limit(&mut self) -> usize {
        match self.chance(50) {
            true => usize::MAX,
            false => self.below(5),
        }
    }
}

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

struct Lane {
    range: KeyGroupRange,
    layer: Layer,
}

struct World {
    seed: u64,
    rng: Rng,
    stores: Arc<dyn Stores>,
    /// Whether the store holds in memory exactly what it keeps, as the
    /// memory bucket does, and runs don't.
    exact: bool,
    generation: u64,
    /// The open epoch.
    epoch: u64,
    lanes: Vec<Lane>,
    /// Lanes of older attempts, still writing.
    zombies: Vec<Lane>,
    now: Logical,
    /// As the last barrier left it.
    last: Logical,
    /// The records that are kept, with the state each names.
    records: BTreeMap<u64, (Logical, Vec<Snapshot>)>,
    step: usize,
}

impl World {
    fn ctx(&self) -> String {
        format!(
            "seed {}, step {}, generation {}, epoch {}",
            self.seed, self.step, self.generation, self.epoch
        )
    }

    async fn open(&mut self, lanes: u32, from: Option<u64>) {
        let snapshots = match from {
            Some(e) => self.records[&e].1.clone(),
            None => Vec::new(),
        };
        self.generation += 1;
        self.epoch = from.map_or(1, |e| e + 1);
        let old = std::mem::take(&mut self.lanes);
        if self.rng.chance(30) {
            self.zombies.extend(old);
            let excess = self.zombies.len().saturating_sub(4);
            self.zombies.drain(..excess);
        }
        for range in ranges(lanes) {
            let layer = Layer::open(
                &*self.stores,
                self.generation,
                self.epoch,
                range,
                &snapshots,
            )
            .await
            .unwrap_or_else(|e| panic!("{}: open: {e}", self.ctx()));
            self.lanes.push(Lane { range, layer });
        }
        self.now = from.map_or_else(Logical::default, |e| self.records[&e].0.clone());
        self.last = self.now.clone();
        self.records.retain(|&e, _| from.is_some_and(|f| e <= f));
    }

    fn lane_of(&self, g: u32) -> usize {
        self.lanes.iter().position(|l| l.range.contains(g)).unwrap()
    }

    fn some_key(&mut self, range: KeyGroupRange) -> (u32, &'static [u8]) {
        let g = range.first + self.rng.below((range.last - range.first + 1) as usize) as u32;
        (g, self.rng.pick(&KEYS))
    }

    async fn step(&mut self) {
        let op = self.rng.pick(&OPS);
        let value = Bytes::from(format!("{}", self.step));
        let all = KeyGroupRange::new(0, GROUPS - 1);
        let (g, key) = self.some_key(all);
        let k = KeyRef::new(g, key);
        let lane = self.lane_of(g);
        let ctx = self.ctx();
        let l = &mut self.lanes[lane];
        let mut m = View {
            s: &mut self.now,
            range: l.range,
        };
        match self.rng.below(100) {
            0..=7 => {
                l.layer.put(op, k, value.clone());
                m.put(op, k, value);
            }
            8..=11 => {
                l.layer.delete(op, k);
                m.delete(op, k);
            }
            12..=19 => {
                let range = l.range;
                let keys: Vec<(u32, &[u8])> = (0..self.rng.below(6))
                    .map(|_| self.some_key(range))
                    .collect();
                let keys: Vec<KeyRef> = keys.iter().map(|(g, k)| KeyRef::new(*g, k)).collect();
                let l = &mut self.lanes[lane];
                let mut m = View {
                    s: &mut self.now,
                    range,
                };
                let got = l.layer.get(op, &keys).await.unwrap();
                assert_eq!(got, m.get(op, &keys).await.unwrap(), "{ctx}: get");
            }
            20..=29 => {
                let time = self.rng.time();
                l.layer.append(op, k, time, value.clone());
                m.append(op, k, time, value);
            }
            30..=33 => {
                let range = l.range;
                let mut slots = Vec::new();
                for _ in 0..self.rng.below(6) {
                    let (g, key) = self.some_key(range);
                    slots.push((g, key, self.rng.time()));
                }
                let slots: Vec<(KeyRef, i64)> = slots
                    .iter()
                    .map(|(g, k, t)| (KeyRef::new(*g, k), *t))
                    .collect();
                let l = &mut self.lanes[lane];
                let mut m = View {
                    s: &mut self.now,
                    range,
                };
                let got = l.layer.rows(op, &slots).await.unwrap();
                assert_eq!(got, m.rows(op, &slots).await.unwrap(), "{ctx}: rows");
            }
            34..=39 => {
                let (w, n) = (self.rng.watermark(), self.rng.limit());
                let got = l.layer.take_rows(op, w, n).await.unwrap();
                assert_eq!(
                    got,
                    m.take_rows(op, w, n).await.unwrap(),
                    "{ctx}: take_rows"
                );
                assert!(got.len() <= n);
                let order: Vec<_> = got.iter().map(|r| (r.group, r.time, r.key.len())).collect();
                assert!(order.is_sorted(), "{ctx}: rows out of order");
            }
            40..=51 => {
                let range = l.range;
                let mut ids = Vec::new();
                for _ in 0..self.rng.below(7) {
                    let (g, key) = self.some_key(range);
                    let at = self.rng.time();
                    let expires = match self.rng.below(7) {
                        0 => NEVER,
                        1 => self.rng.time(),
                        _ => at.saturating_add(self.rng.between(0, 10)),
                    };
                    ids.push((g, key, at, expires));
                }
                let keys: Vec<KeyRef> = ids.iter().map(|(g, k, _, _)| KeyRef::new(*g, k)).collect();
                let at: Vec<i64> = ids.iter().map(|i| i.2).collect();
                let expires: Vec<i64> = ids.iter().map(|i| i.3).collect();
                let l = &mut self.lanes[lane];
                let mut m = View {
                    s: &mut self.now,
                    range,
                };
                let got = l.layer.insert_new(op, &keys, &at, &expires).await.unwrap();
                let want = m.insert_new(op, &keys, &at, &expires).await.unwrap();
                assert_eq!(got, want, "{ctx}: insert_new {ids:?}");
            }
            52..=55 => {
                let h = self.rng.watermark();
                l.layer.expire(op, h);
                m.expire(op, h);
            }
            56..=62 => {
                let time = self.rng.time();
                l.layer.set_timer(op, k, time);
                m.set_timer(op, k, time);
            }
            63..=65 => {
                let time = self.rng.time();
                l.layer.delete_timer(op, k, time);
                m.delete_timer(op, k, time);
            }
            66..=71 => {
                let (w, n) = (self.rng.watermark(), self.rng.limit());
                let got = l.layer.fire(op, w, n).await.unwrap();
                assert_eq!(got, m.fire(op, w, n).await.unwrap(), "{ctx}: fire");
                assert!(got.len() <= n && got.iter().all(|t| t.time <= w));
                let order: Vec<_> = got.iter().map(|t| (t.time, t.group, t.key.len())).collect();
                assert!(order.is_sorted(), "{ctx}: timers out of order");
            }
            72..=81 => self.barrier().await,
            82..=85 => {
                for l in &mut self.lanes {
                    l.layer.discard();
                    assert_eq!(l.layer.memory().changes, 0, "{ctx}");
                }
                self.now = self.last.clone();
            }
            86..=91 => {
                let from = match self.records.keys().copied().collect::<Vec<_>>() {
                    r if r.is_empty() => None,
                    r if self.rng.chance(70) => r.last().copied(),
                    r => Some(self.rng.pick(&r)),
                };
                let lanes = self.rng.pick(&LANES);
                self.open(lanes, from).await;
            }
            92..=94 => self.zombie().await,
            95..=96 => {
                let kept: Vec<u64> = self.records.keys().copied().collect();
                if let Some(&latest) = kept.last() {
                    let before = self.rng.pick(&kept).min(latest);
                    for l in &mut self.lanes {
                        l.layer.release(before).await.unwrap();
                    }
                    self.records.retain(|&e, _| e >= before);
                }
            }
            _ => self.sweep(false).await,
        }
    }

    /// Every lane's barrier of the open epoch, and its record, or none.
    async fn barrier(&mut self) {
        let ctx = self.ctx();
        let mut snapshots = Vec::new();
        for l in &mut self.lanes {
            let got = l.layer.barrier(self.epoch).await;
            snapshots.extend(got.unwrap_or_else(|e| panic!("{ctx}: barrier: {e}")));
            let memory = l.layer.memory();
            assert_eq!(memory.changes, 0, "{ctx}");
            if self.exact {
                assert_eq!(
                    memory.store > 0,
                    self.now.holds(l.range),
                    "{ctx}: {memory:?}"
                );
            }
        }
        self.last = self.now.clone();
        if self.rng.chance(75) {
            self.records
                .insert(self.epoch, (self.now.clone(), snapshots));
        }
        self.epoch += 1;
    }

    /// An older attempt's lane writes and commits an epoch, which no record
    /// will name.
    async fn zombie(&mut self) {
        if self.zombies.is_empty() {
            return;
        }
        let i = self.rng.below(self.zombies.len());
        let range = self.zombies[i].range;
        let (g, key) = self.some_key(range);
        let k = KeyRef::new(g, key);
        let (op, t) = (self.rng.pick(&OPS), self.rng.time());
        let z = &mut self.zombies[i].layer;
        z.put(op, k, Bytes::from_static(b"zombie"));
        z.append(op, k, t, Bytes::from_static(b"zombie"));
        z.set_timer(op, k, t);
        z.expire(op, i64::MAX);
        z.insert_new(op, &[k], &[0], &[NEVER]).await.unwrap();
        let epoch = z.epoch();
        z.barrier(epoch).await.unwrap();
    }

    /// Compares every value, and some rows; `last` takes every row, fires
    /// every timer and asks for every id too.
    async fn sweep(&mut self, last: bool) {
        let ctx = self.ctx();
        for lane in 0..self.lanes.len() {
            let range = self.lanes[lane].range;
            let mut slots = Vec::new();
            for _ in 0..20 {
                let (g, key) = self.some_key(range);
                slots.push((KeyRef::new(g, key), self.rng.time()));
            }
            let l = &mut self.lanes[lane];
            let mut m = View {
                s: &mut self.now,
                range,
            };
            for op in OPS {
                let keys: Vec<KeyRef> = (range.first..=range.last)
                    .flat_map(|g| KEYS.iter().map(move |k| KeyRef::new(g, k)))
                    .collect();
                let got = l.layer.get(op, &keys).await.unwrap();
                assert_eq!(got, m.get(op, &keys).await.unwrap(), "{ctx}: sweep");
                let got = l.layer.rows(op, &slots).await.unwrap();
                assert_eq!(got, m.rows(op, &slots).await.unwrap(), "{ctx}: sweep");
                if !last {
                    continue;
                }
                let got = l.layer.fire(op, i64::MAX, usize::MAX).await.unwrap();
                assert_eq!(
                    got,
                    m.fire(op, i64::MAX, usize::MAX).await.unwrap(),
                    "{ctx}"
                );
                let got = l.layer.take_rows(op, i64::MAX, usize::MAX).await.unwrap();
                let want = m.take_rows(op, i64::MAX, usize::MAX).await.unwrap();
                assert_eq!(got, want, "{ctx}");
                let at = vec![3; keys.len()];
                let expires = vec![4; keys.len()];
                let got = l.layer.insert_new(op, &keys, &at, &expires).await.unwrap();
                let want = m.insert_new(op, &keys, &at, &expires).await.unwrap();
                assert_eq!(got, want, "{ctx}");
            }
        }
    }
}

/// Runs over an in-memory object store, with blocks, filters, a cache
/// and reads that vary by seed: tiny blocks and filters that let most keys
/// through, so lookups go past false positives and scans across blocks.
fn runs(seed: u64) -> Arc<dyn Stores> {
    let mut rng = Rng(seed ^ 0x5eed);
    let config = RunConfig {
        block_bytes: rng.pick(&[1, 40, 200, 4096]),
        compress: rng.chance(50),
        bits: match rng.below(3) {
            0 => [1, 1, 2, 1],
            1 => [0, 0, 0, 0],
            _ => [10, 10, 20, 10],
        },
        cache_bytes: rng.pick(&[0, 2048, 1 << 20]),
        gap_bytes: rng.pick(&[0, 100, 1 << 16]),
        max_read_bytes: rng.pick(&[1, 300, 1 << 20]),
        concurrency: rng.pick(&[1, 4]),
    };
    let objects = ObjectStorage::new(Arc::new(InMemory::new()), Path::from("state"));
    Arc::new(RunStores::new(Arc::new(objects), config))
}

async fn run(seed: u64, stores: Arc<dyn Stores>, exact: bool) {
    let mut w = World {
        seed,
        rng: Rng(seed),
        stores,
        exact,
        generation: 0,
        epoch: 1,
        lanes: Vec::new(),
        zombies: Vec::new(),
        now: Logical::default(),
        last: Logical::default(),
        records: BTreeMap::new(),
        step: 0,
    };
    let lanes = w.rng.pick(&LANES);
    w.open(lanes, None).await;
    for step in 0..STEPS {
        w.step = step;
        w.step().await;
    }
    w.sweep(true).await;
}

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("OPENETL_STATE_SEED") {
        return vec![s.parse().unwrap()];
    }
    let n = std::env::var("OPENETL_STATE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    (1..=n).collect()
}

#[test]
fn the_layer_matches_the_model() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for seed in seeds() {
        rt.block_on(run(seed, Arc::new(MemoryBucket::new()), true));
    }
}

#[test]
fn the_layer_matches_the_model_over_runs() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    for seed in seeds() {
        rt.block_on(run(seed, runs(seed), false));
    }
}
