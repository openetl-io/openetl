//! Merges: a merge's output counts only once a record names it, a kill at
//! any point of one loses nothing, and retention, deleting what no kept
//! record names as the runner's does, never deletes what a restore reads.
//!
//! Each seed runs lanes of values, a set with expiry and a set kept for
//! good through epochs with merges at every chance, and a request to
//! storage that fails at random: a read, a put that writes nothing, or a
//! put that writes and fails. A failure, or a record that's never written,
//! kills the attempt; the next restores a kept record onto any number of
//! lanes, after retention, and must read exactly the state that record
//! holds. `OPENETL_STATE_SEEDS=n` runs n seeds, and `OPENETL_STATE_SEED=s`
//! the one.

#![allow(clippy::unwrap_used)]

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use futures_util::TryStreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use openetl_state::runs::{Counts, RunName, named_runs};
use openetl_state::{
    BoxFuture, Error, KeyGroupRange, KeyRef, Layer, MergeConfig, NEVER, ObjectStorage, Objects,
    OperatorId, Result, RunConfig, RunStores, Snapshot, StateBackend,
};

const VALUES: OperatorId = OperatorId(1);
const IDS: OperatorId = OperatorId(2);
const KEPT: OperatorId = OperatorId(3);
const GROUPS: u32 = 8;
const KEYS: u32 = 24;
const EPOCHS: u64 = 60;
/// Records retention keeps, as the runner's least.
const KEEP: usize = 2;
const INJECTED: &str = "injected";

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// An in-memory bucket whose `n`th request from now fails, and lists and
/// deletes as retention does.
struct Bucket {
    memory: Arc<InMemory>,
    storage: ObjectStorage,
    /// Requests until one fails; 0 for none.
    countdown: AtomicU64,
    /// Whether a put that fails writes first.
    writes: AtomicU64,
    failed: AtomicU64,
}

impl Bucket {
    fn new() -> Arc<Self> {
        let memory = Arc::new(InMemory::new());
        let storage = ObjectStorage::new(memory.clone(), Path::from("state"));
        Arc::new(Self {
            memory,
            storage,
            countdown: AtomicU64::new(0),
            writes: AtomicU64::new(0),
            failed: AtomicU64::new(0),
        })
    }

    /// Whether this request fails.
    fn trips(&self) -> bool {
        let fail = self
            .countdown
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n > 0).then(|| n - 1)
            })
            .is_ok_and(|n| n == 1);
        if fail {
            self.failed.fetch_add(1, Ordering::Relaxed);
        }
        fail
    }

    async fn names(&self) -> BTreeSet<String> {
        let prefix = Path::from("state");
        let listed: Vec<_> = self.memory.list(Some(&prefix)).try_collect().await.unwrap();
        listed
            .into_iter()
            .map(|m| m.location.as_ref()["state/".len()..].to_string())
            .collect()
    }

    /// What the runner's retention deletes, for an attempt of generation
    /// `generation` whose newest record is of epoch `upto`: every run no
    /// kept record names, of an older generation, or of its own and an
    /// epoch no later than `upto`.
    async fn prune(&self, kept: &[&[Snapshot]], generation: u64, upto: u64) -> usize {
        let mut named = BTreeSet::new();
        for s in kept {
            named.extend(named_runs(s).unwrap());
        }
        let mut deleted = 0;
        for name in self.names().await {
            let run = RunName::parse(&name).unwrap();
            let theirs = run.generation < generation;
            let ours = run.generation == generation && run.epoch <= upto;
            if !named.contains(&name) && (theirs || ours) {
                let path = Path::from(format!("state/{name}"));
                self.memory.delete(&path).await.unwrap();
                deleted += 1;
            }
        }
        deleted
    }
}

impl Objects for Bucket {
    fn put_new<'a>(&'a self, name: &'a str, data: Bytes) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            if !self.trips() {
                return self.storage.put_new(name, data).await;
            }
            if self.writes.load(Ordering::Relaxed) == 1 {
                self.storage.put_new(name, data).await?;
            }
            Err(Error::Store(format!("{INJECTED}: a put of {name}")))
        })
    }

    fn get_range<'a>(&'a self, name: &'a str, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            match self.trips() {
                true => Err(Error::Store(format!("{INJECTED}: a read of {name}"))),
                false => self.storage.get_range(name, range).await,
            }
        })
    }
}

/// The state every lane holds together.
#[derive(Clone, Default, Debug, PartialEq)]
struct Model {
    values: BTreeMap<(u32, u32), Bytes>,
    /// Each id's expiry, by operator, group and id.
    ids: BTreeMap<(OperatorId, u32, u32), i64>,
    /// [`IDS`]'s horizon; [`KEPT`] has none.
    horizon: i64,
}

impl Model {
    fn now(&self, op: OperatorId, at: i64) -> i64 {
        match op {
            IDS => at.max(self.horizon),
            _ => at,
        }
    }

    /// As [`StateBackend::insert_new`] for one id.
    fn insert_new(&mut self, op: OperatorId, g: u32, id: u32, at: i64, expires: i64) -> bool {
        let now = self.now(op, at);
        let live = self
            .ids
            .get(&(op, g, id))
            .is_some_and(|&e| e == NEVER || e > now);
        if !live && (expires == NEVER || expires > now) {
            self.ids.insert((op, g, id), expires);
        }
        !live
    }
}

fn id(n: u32) -> Vec<u8> {
    format!("id-{n:04}").into_bytes()
}

fn key(k: u32) -> Vec<u8> {
    format!("key-{k:03}").into_bytes()
}

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

/// A record: its epoch, the state it holds, and its snapshots.
type Record = (u64, Model, Vec<Snapshot>);

struct World {
    seed: u64,
    rng: Rng,
    bucket: Arc<Bucket>,
    stores: RunStores,
    generation: u64,
    epoch: u64,
    lanes: Vec<Layer>,
    now: Model,
    /// The kept records, oldest first.
    records: Vec<Record>,
    kills: u64,
    restores: u64,
    pruned: usize,
    /// Values compared after restores.
    compared: u64,
}

fn failed(e: &Error) -> bool {
    matches!(e, Error::Store(m) if m.contains(INJECTED))
}

impl World {
    fn ctx(&self) -> String {
        format!(
            "seed {}, generation {}, epoch {}",
            self.seed, self.generation, self.epoch
        )
    }

    fn lane(&mut self, g: u32) -> &mut Layer {
        let i = self
            .lanes
            .iter()
            .position(|l| l.range().contains(g))
            .unwrap();
        &mut self.lanes[i]
    }

    /// A new attempt: retention, then a restore of a kept record, the
    /// newest or the one before, onto any number of lanes.
    async fn restart(&mut self) {
        self.lanes.clear();
        self.bucket.countdown.store(0, Ordering::Relaxed);
        self.generation += 1;
        let upto = self.records.last().map_or(0, |r| r.0);
        let kept: Vec<&[Snapshot]> = self.records.iter().map(|r| r.2.as_slice()).collect();
        self.pruned += self.bucket.prune(&kept, self.generation, upto).await;
        let from = match self.records.len() {
            0 => None,
            n if n > 1 && self.rng.chance(25) => Some(n - 2),
            n => Some(n - 1),
        };
        let (epoch, model, snapshots) = match from {
            Some(i) => self.records[i].clone(),
            None => (0, Model::default(), Vec::new()),
        };
        if let Some(i) = from {
            self.records.truncate(i + 1);
        }
        self.epoch = epoch + 1;
        self.now = model;
        let lanes = [1, 2, 3, 8][self.rng.below(4) as usize];
        for r in ranges(lanes) {
            let mine: Vec<Snapshot> = snapshots
                .iter()
                .filter(|s| s.range.overlaps(&r))
                .cloned()
                .collect();
            let l = Layer::open(&self.stores, self.generation, self.epoch, r, &mine)
                .await
                .unwrap_or_else(|e| panic!("{}: open: {e}", self.ctx()));
            self.lanes.push(l);
        }
        self.restores += 1;
        if let Err(e) = self.check().await {
            assert!(failed(&e), "{}: after a restore: {e}", self.ctx());
            self.kills += 1;
            Box::pin(self.restart()).await;
        }
    }

    /// Every value and id the lanes hold against the model.
    async fn check(&mut self) -> Result<()> {
        let ctx = self.ctx();
        for g in 0..GROUPS {
            let keys: Vec<Vec<u8>> = (0..KEYS).map(key).collect();
            let refs: Vec<KeyRef> = keys.iter().map(|k| KeyRef::new(g, k)).collect();
            let got = self.lane(g).get(VALUES, &refs).await?;
            for (k, v) in (0..KEYS).zip(got) {
                assert_eq!(
                    v,
                    self.now.values.get(&(g, k)).cloned(),
                    "{ctx}: value {g}/{k}"
                );
                self.compared += 1;
            }
            // An id asked for with an expiry already passed is never kept.
            for (op, n) in [(IDS, 60), (KEPT, 400)] {
                let ids: Vec<Vec<u8>> = (0..n).map(id).collect();
                let refs: Vec<KeyRef> = ids.iter().map(|i| KeyRef::new(g, i)).collect();
                let at = vec![self.now.horizon; refs.len()];
                let never = vec![i64::MIN; refs.len()];
                let got = self.lane(g).insert_new(op, &refs, &at, &never).await?;
                for (n, fresh) in (0..n).zip(got) {
                    let want = self.now.insert_new(op, g, n, self.now.horizon, i64::MIN);
                    assert_eq!(fresh, want, "{ctx}: id {op:?} {g}/{n}");
                    self.compared += 1;
                }
            }
        }
        Ok(())
    }

    /// One epoch of changes on every lane, its barriers and, unless the
    /// attempt dies first, its record.
    async fn epoch(&mut self) -> Result<()> {
        let e = self.epoch;
        for _ in 0..self.rng.below(40) {
            let (g, k) = (
                self.rng.below(GROUPS.into()) as u32,
                self.rng.below(KEYS.into()) as u32,
            );
            let kk = key(k);
            match self.rng.below(10) {
                0..=4 => {
                    let v = Bytes::from(format!(
                        "{e}:{k}:{}",
                        "x".repeat(self.rng.below(30) as usize)
                    ));
                    self.lane(g).put(VALUES, KeyRef::new(g, &kk), v.clone());
                    self.now.values.insert((g, k), v);
                }
                5..=6 => {
                    self.lane(g).delete(VALUES, KeyRef::new(g, &kk));
                    self.now.values.remove(&(g, k));
                }
                _ => {
                    let (op, n, expires) = match self.rng.chance(70) {
                        true => (IDS, self.rng.below(60) as u32, (e as i64) * 10 + 30),
                        false => (KEPT, self.rng.below(400) as u32, NEVER),
                    };
                    let i = id(n);
                    let at = (e as i64) * 10 + self.rng.below(10) as i64;
                    let got = self
                        .lane(g)
                        .insert_new(op, &[KeyRef::new(g, &i)], &[at], &[expires])
                        .await?;
                    let want = self.now.insert_new(op, g, n, at, expires);
                    assert_eq!(got, [want], "{}: insert_new {op:?} {g}/{n}", self.ctx());
                }
            }
        }
        let horizon = (e as i64) * 10;
        for l in &mut self.lanes {
            l.expire(IDS, horizon);
        }
        self.now.horizon = horizon;
        let mut snapshots = Vec::new();
        for l in &mut self.lanes {
            snapshots.extend(l.barrier(e).await?);
        }
        // Merges in flight go on.
        if self.rng.chance(50) {
            tokio::task::yield_now().await;
        }
        self.epoch += 1;
        if self.rng.chance(10) {
            // The record is never written: what the barriers wrote, merges
            // too, is never named.
            return Err(Error::Store(format!("{INJECTED}: the record")));
        }
        self.records.push((e, self.now.clone(), snapshots));
        let excess = self.records.len().saturating_sub(KEEP);
        self.records.drain(..excess);
        if self.rng.chance(30) {
            let kept: Vec<&[Snapshot]> = self.records.iter().map(|r| r.2.as_slice()).collect();
            self.pruned += self.bucket.prune(&kept, self.generation, e).await;
        }
        if self.rng.chance(20) {
            self.check().await?;
        }
        Ok(())
    }
}

async fn run(seed: u64) -> (Counts, World) {
    let mut rng = Rng(seed);
    let bucket = Bucket::new();
    let config = RunConfig {
        block_bytes: [40, 200, 4096][rng.below(3) as usize],
        merge: Some(MergeConfig {
            values_bound: rng.below(3) as usize,
            fanout: 2,
            small_bytes: 1 << 20,
            buckets: 1 + rng.below(4),
            max_bytes: [2_000, 1 << 20][rng.below(2) as usize],
            wait: rng.chance(50),
        }),
        ..RunConfig::default()
    };
    let stores = RunStores::new(bucket.clone(), config);
    let mut w = World {
        seed,
        rng,
        bucket,
        stores,
        generation: 0,
        epoch: 1,
        lanes: Vec::new(),
        now: Model::default(),
        records: Vec::new(),
        kills: 0,
        restores: 0,
        pruned: 0,
        compared: 0,
    };
    w.restart().await;
    while w.epoch <= EPOCHS || w.records.last().is_none_or(|r| r.0 < EPOCHS) {
        if w.rng.chance(4) {
            w.bucket.writes.store(w.rng.below(2), Ordering::Relaxed);
            w.bucket
                .countdown
                .store(1 + w.rng.below(30), Ordering::Relaxed);
        }
        if let Err(e) = w.epoch().await {
            assert!(failed(&e), "{}: {e}", w.ctx());
            w.kills += 1;
            w.restart().await;
        }
    }
    w.bucket.countdown.store(0, Ordering::Relaxed);
    w.restart().await;
    (w.stores.counts(), w)
}

fn seeds() -> Vec<u64> {
    if let Ok(s) = std::env::var("OPENETL_STATE_SEED") {
        return vec![s.parse().unwrap()];
    }
    let n = std::env::var("OPENETL_STATE_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);
    (1..=n).collect()
}

#[test]
fn a_kill_at_any_point_of_a_merge_loses_nothing() {
    let (mut merges, mut written, mut kills, mut failed, mut pruned, mut compared) =
        (0, 0, 0, 0, 0, 0);
    for seed in seeds() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let (c, w) = rt.block_on(run(seed));
        merges += c.merges;
        written += c.merge_write.count;
        kills += w.kills;
        failed += w.bucket.failed.load(Ordering::Relaxed);
        pruned += w.pruned;
        compared += w.compared;
    }
    let n = seeds().len() as u64;
    println!(
        "{n} seeds: {merges} merges, {written} written, {kills} kills, {failed} failed requests, {pruned} runs pruned, {compared} values compared"
    );
    assert!(merges >= 5 * n, "{merges} merges");
    assert!(written >= 3 * n, "{written} merges written");
    assert!(kills >= n, "{kills} kills");
    assert!(compared >= 1000 * n, "{compared} values compared");
}
