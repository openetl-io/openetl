//! A seeded model of the schema protocol on workers, its invariants checked
//! after every step. The engines are real: each a Plan with remote schemas,
//! its Owner, and its lanes' LaneStreams, shaping raw rows with the real
//! Shaper under block, holding what the policy doesn't take. The remote
//! owner is a model of internal/runner's schemaOwner: it takes a proposal
//! made from its newest version, answers one made from an older version
//! with that version, and as it merges an epoch's shares records the stream,
//! and seals it once it has columns. A scheduler picks each next step from
//! the seed: a lane shapes a batch, which may wait on its proposal; the
//! owner answers a proposal; an engine takes the next version the owner
//! sent it, which may also come while a lane's batch is shaped, with a
//! follow as another lane would make; a lane passes its next barrier; the
//! owner merges the epoch every lane has passed the barrier of; or the
//! attempt loses a worker, and the next restores from the last record on
//! other engines and lanes. One engine of an attempt may start late, with
//! no rows until a record has the stream's columns. The invariants are
//! schemaOwner's (internal/runner/workers.go), with one of the lanes': a
//! lane returns a batch shaped by its engine's newest version. Each broken
//! variant puts a past bug back, and must break one.
//!
//! A follow on another lane is one step here: the model runs on one
//! thread, so it can't stop a follow between its writes, as
//! `another_lanes_follow_is_seen_whole` does with two.
//!
//! `OPENETL_SCHEMA_SEEDS=n` runs n seeds, and `OPENETL_SCHEMA_SEED=s` the
//! one.

#![allow(clippy::unwrap_used)]

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll, Waker};

use arrow_schema::{DataType, Field, Schema, SchemaRef};
use openetl_normalize::{Shaped, widens};
use openetl_proto::engine::v1::{
    ColumnHold, HoldPlan, SchemaPolicy, SchemasRequest, SchemasResponse,
};

use super::{LaneStream, Owner, Quarantined, shape_error};
use crate::error::JobError;
use crate::ipc;
use crate::plan::Plan;
use crate::remote::{Proposals, Remote};

const STEPS: usize = 150;

/// A past bug, put back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Broken {
    /// The owner never seals at a merge: only an engine whose lanes had
    /// rows seals, at its own barrier, as before a late worker was guarded.
    NoMergeSeal,
    /// The engines don't see the seal on a version.
    SealUnseen,
    /// The owner seals without a version of its own, so the engines never
    /// hear of it, and a proposal made before it is taken.
    SealInPlace,
    /// The owner takes a proposal made from an older version.
    StaleTaken,
    /// A record has the first engine's schema as it merges, not the
    /// owner's newest, as a share's snapshot at its barrier is.
    RecordFromEngine,
    /// A restore starts the stream's engines unsealed.
    RestoreUnsealed,
    /// A lane returns a batch that needed no change without checking that
    /// its engine's version is the one it shaped by.
    NoRecheck,
}

const BROKEN: [Broken; 7] = [
    Broken::NoMergeSeal,
    Broken::SealUnseen,
    Broken::SealInPlace,
    Broken::StaleTaken,
    Broken::RecordFromEngine,
    Broken::RestoreUnsealed,
    Broken::NoRecheck,
];

/// splitmix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        usize::try_from(self.next() % u64::try_from(n).unwrap()).unwrap()
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// Stream a as schemaOwner keeps it.
#[derive(Default)]
struct Owned {
    version: u64,
    schema: Vec<u8>,
    delivered: Vec<u8>,
    holding: Vec<ColumnHold>,
    /// The attempt restored the stream's columns.
    restored: bool,
    sealed: bool,
    /// By version, the epoch of the rows whose change made it; 0 for
    /// version 0 and the seal.
    epochs: Vec<u64>,
}

impl Owned {
    fn request(&self) -> SchemasRequest {
        SchemasRequest {
            stream: 0,
            version: self.version,
            arrow_schema: self.schema.clone(),
            holding: self.holding.clone(),
            sealed: self.sealed,
            ..Default::default()
        }
    }
}

/// A source column and the type the destination gets it at.
type Columns = Vec<(String, DataType)>;

fn columns(s: &Schema) -> Columns {
    s.fields()
        .iter()
        .filter(|f| !f.name().starts_with("_openetl_"))
        .map(|f| (f.name().clone(), f.data_type().clone()))
        .collect()
}

/// Whether the destination takes a column at `t` where it has `to`.
fn fits(name: &str, t: &DataType, to: &DataType) -> bool {
    t == to
        || widens(
            &Field::new(name, t.clone(), true),
            &Field::new(name, to.clone(), true),
        )
}

/// What `rows` has that `to` doesn't take; None if it takes them all.
fn unfit(rows: &Columns, to: &Columns) -> Option<String> {
    rows.iter()
        .find_map(|(c, t)| match to.iter().find(|(n, _)| n == c) {
            Some((_, to)) if fits(c, t, to) => None,
            Some((_, to)) => Some(format!("{c} at {t}, where the destination has {to}")),
            None => Some(format!("{c} at {t}, which the destination hasn't")),
        })
}

/// An epoch's record of stream a.
struct Record {
    schema: Option<SchemaRef>,
    delivered: Option<SchemaRef>,
    holding: Vec<ColumnHold>,
}

type Shaping = Pin<Box<dyn Future<Output = (LaneStream, Result<Shaped, JobError>)>>>;

/// A lane's batch being shaped.
struct Op {
    lane: usize,
    epoch: u64,
    fut: Shaping,
}

struct Engine {
    plan: Arc<Plan>,
    remote: Arc<Remote>,
    proposals: Proposals,
    owner: Arc<Owner>,
    /// Each lane's stream, out while its batch is shaped, and the barrier
    /// it passes next.
    lanes: Vec<(Option<LaneStream>, u64)>,
    op: Option<Op>,
    /// Proposals not answered yet, oldest first, each with its rows' epoch.
    pending: VecDeque<(SchemasResponse, u64)>,
    /// Versions the owner sent it, not taken yet.
    inbox: Rc<RefCell<VecDeque<SchemasRequest>>>,
    /// Of its batch being shaped: the versions it took meanwhile, the
    /// follows then, and how often it was shaped.
    counts: Option<Rc<Cell<(usize, usize, usize)>>>,
    late: bool,
}

/// A batch a lane gave its writer lane.
struct Emitted {
    epoch: u64,
    columns: Columns,
}

/// How often the seeds reached each path.
#[derive(Default, Debug)]
struct Stats {
    seeds: usize,
    steps: usize,
    batches: usize,
    /// A proposal made from an older version, answered with the newest.
    stale: usize,
    /// A version sent to every engine while another engine's batch waited.
    released: usize,
    /// A seal while a lane's batch waited on its proposal.
    seal_waiting: usize,
    /// A late engine's first rows held a column under the seal.
    late_held: usize,
    /// An engine sealed at its barrier before the owner sealed.
    local_seal_first: usize,
    /// A change taken of a later epoch than the stream's first change, before
    /// the first merge with columns: the one exception.
    window: usize,
    /// A batch of epoch n shaped by a version with a change of a later
    /// epoch, so a record of n can't leave that change out.
    later_in_earlier: usize,
    /// A record of epoch n with a change of a later epoch.
    record_later: usize,
    restored_with: usize,
    restored_without: usize,
    /// A version taken while a lane's batch was shaped, and one followed
    /// then too.
    mid_shape: usize,
    mid_follow: usize,
    /// A batch shaped again, as a version came while it was shaped.
    reshaped: usize,
}

struct Model {
    rng: Rng,
    broken: Option<Broken>,
    dir: tempfile::TempDir,
    owned: Owned,
    engines: Vec<Engine>,
    merged: u64,
    records: Vec<Record>,
    /// What the destination got with the first record with columns.
    first: Option<Columns>,
    /// This attempt's batches of epochs not merged yet.
    emitted: Vec<Emitted>,
    /// The epoch of the stream's first change this attempt.
    first_change: Option<u64>,
    restores: usize,
    ids: u64,
}

impl Model {
    fn new(seed: u64, broken: Option<Broken>) -> Self {
        let mut m = Self {
            rng: Rng(seed),
            broken,
            dir: tempfile::tempdir().unwrap(),
            owned: Owned {
                epochs: vec![0],
                ..Default::default()
            },
            engines: Vec::new(),
            merged: 0,
            records: Vec::new(),
            first: None,
            emitted: Vec::new(),
            first_change: None,
            restores: 0,
            ids: 0,
        };
        m.engines = m.attempt(1, None);
        m
    }

    /// An attempt's engines, from `first_epoch` on, restoring `restored`.
    fn attempt(&mut self, first_epoch: u64, restored: Option<&Record>) -> Vec<Engine> {
        let n = 1 + self.rng.below(3);
        let late = (n > 1 && self.rng.chance(50)).then(|| self.rng.below(n));
        (0..n)
            .map(|i| {
                let lanes = 1 + u32::try_from(self.rng.below(3)).unwrap();
                let mut req = crate::plan::tests::lanes_request(&["a"], lanes, lanes);
                req.remote_schemas = true;
                req.first_epoch = first_epoch;
                req.schema_policy = SchemaPolicy::Block as i32;
                req.streams[0].primary_key = vec!["id".into()];
                let mut holds = HoldPlan::default();
                if let Some(r) = restored {
                    if let Some(s) = &r.schema {
                        req.streams[0].restored_schema = ipc::encode_schema(s).unwrap();
                    }
                    holds.holding = r.holding.clone();
                }
                req.streams[0].holds = Some(holds);
                req.held_url = format!("file://{}/held", self.dir.path().display());
                let mut plan = Plan::from_request(req).unwrap();
                plan.no_recheck = self.broken == Some(Broken::NoRecheck);
                let plan = Arc::new(plan);
                let remote = plan.remote.clone().unwrap();
                let proposals = remote.take_proposals().unwrap();
                let owner = Arc::new(Owner::new(&plan, 0).unwrap());
                if restored.is_some() && self.broken == Some(Broken::RestoreUnsealed) {
                    owner.lock().shaper.unseal();
                }
                let lanes = (0..lanes)
                    .map(|_| {
                        let o = owner.lock();
                        let ls = LaneStream {
                            stream: 0,
                            shaper: o.shaper.fork(),
                            version: o.version,
                            quarantined: Quarantined::default(),
                            cells: BTreeMap::new(),
                            replay: None,
                        };
                        (Some(ls), first_epoch)
                    })
                    .collect();
                Engine {
                    plan,
                    remote,
                    proposals,
                    owner,
                    lanes,
                    op: None,
                    pending: VecDeque::new(),
                    inbox: Rc::default(),
                    counts: None,
                    late: late == Some(i),
                }
            })
            .collect()
    }

    fn step(&mut self, stats: &mut Stats) -> Result<(), String> {
        stats.steps += 1;
        match self.rng.below(100) {
            0..35 => self.shape(stats),
            35..55 => self.answer(stats),
            55..70 => self.deliver(stats),
            70..85 => self.cross(stats),
            85..97 => self.merge(stats),
            _ => self.restore(stats),
        }
    }

    /// One or two rows: x an integer most often, a float or text sometimes,
    /// and now and then a new column y.
    fn rows(&mut self) -> Vec<Vec<u8>> {
        (0..1 + self.rng.below(2))
            .map(|_| {
                self.ids += 1;
                let x = match self.rng.below(100) {
                    0..70 => "1",
                    70..85 => "1.5",
                    _ => "\"t\"",
                };
                let y = match self.rng.chance(10) {
                    true => ", \"y\": 2",
                    false => "",
                };
                format!(r#"{{"id": {}, "x": {x}{y}}}"#, self.ids).into_bytes()
            })
            .collect()
    }

    fn shape(&mut self, stats: &mut Stats) -> Result<(), String> {
        let recorded = self.first.is_some();
        let ready: Vec<usize> = (0..self.engines.len())
            .filter(|&e| self.engines[e].op.is_none() && (recorded || !self.engines[e].late))
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let e = ready[self.rng.below(ready.len())];
        let rows = self.rows();
        let lane = self.rng.below(self.engines[e].lanes.len());
        let engine = &mut self.engines[e];
        let epoch = engine.lanes[lane].1;
        let mut ls = engine.lanes[lane].0.take().unwrap();
        let (plan, owner) = (engine.plan.clone(), engine.owner.clone());
        let (inbox, remote) = (engine.inbox.clone(), engine.remote.clone());
        // Each time the batch is shaped, the engine may take the versions
        // sent it meanwhile, and another lane may follow them.
        let dice = Cell::new(self.rng.next());
        let counts = Rc::new(Cell::new((0usize, 0usize, 0usize)));
        let shaped = counts.clone();
        let fut: Shaping = Box::pin(async move {
            let out = ls
                .shape_own(
                    &plan,
                    &owner,
                    |sh| {
                        let mut r = Rng(dice.get());
                        let (mut mid, mut follows, mut tries) = shaped.get();
                        tries += 1;
                        if r.chance(30) && !inbox.borrow().is_empty() {
                            mid += 1;
                            for u in inbox.borrow_mut().drain(..) {
                                remote.update(&u).unwrap();
                            }
                            if r.chance(50) {
                                follows += 1;
                                owner.follow(&plan, 0).unwrap();
                            }
                        }
                        dice.set(r.next());
                        shaped.set((mid, follows, tries));
                        sh.type_rows(rows.iter().map(Vec::as_slice))
                    },
                    |_, e| shape_error("a", e),
                )
                .await;
            (ls, out)
        });
        engine.counts = Some(counts);
        engine.op = Some(Op { lane, epoch, fut });
        self.poll(e, stats)
    }

    /// Polls engine e's batch, once: it's shaped, or waits on its proposal.
    fn poll(&mut self, e: usize, stats: &mut Stats) -> Result<(), String> {
        let engine = &mut self.engines[e];
        let Some(op) = engine.op.as_mut() else {
            return Ok(());
        };
        let epoch = op.epoch;
        let mut cx = Context::from_waker(Waker::noop());
        let done = match op.fut.as_mut().poll(&mut cx) {
            Poll::Ready(v) => Some(v),
            Poll::Pending => None,
        };
        while let Ok(p) = engine.proposals.try_recv() {
            engine.pending.push_back((p.unwrap(), epoch));
        }
        let Some((ls, out)) = done else {
            return Ok(());
        };
        let lane = engine.op.take().unwrap().lane;
        if let Some(c) = engine.counts.take() {
            let (mid, follows, tries) = c.get();
            stats.mid_shape += mid;
            stats.mid_follow += follows;
            stats.reshaped += usize::from(tries > 1);
        }
        // The lane shaped the batch by its engine's newest version.
        let newest = engine.remote.stream(0).unwrap().newest();
        let followed = engine.owner.remote_version.load(Ordering::Acquire);
        let version = engine.owner.version.load(Ordering::Acquire);
        if out.is_ok() && (ls.version != version || followed != newest) {
            return Err(format!(
                "a lane returned a batch shaped by version {} of its engine's {version}, which follows {followed} of {newest}",
                ls.version
            ));
        }
        engine.lanes[lane].0 = Some(ls);
        let shaped = out.map_err(|e| format!("a batch failed: {e}"))?;
        let shaped_by = engine.owner.remote_version.load(Ordering::Acquire);
        stats.batches += 1;
        let by = usize::try_from(shaped_by).unwrap();
        if self.owned.epochs.iter().take(by + 1).any(|&x| x > epoch) {
            stats.later_in_earlier += 1;
        }
        let cols = columns(&shaped.batch.schema());
        // After the first record, the destination takes no other column and
        // no other type.
        if let Some(first) = &self.first
            && let Some(what) = unfit(&cols, first)
        {
            return Err(format!("a lane delivered {what} after the first record"));
        }
        self.emitted.push(Emitted {
            epoch,
            columns: cols,
        });
        Ok(())
    }

    fn poll_all(&mut self, stats: &mut Stats) -> Result<(), String> {
        for e in 0..self.engines.len() {
            self.poll(e, stats)?;
        }
        Ok(())
    }

    /// Sends engine e the owner's newest version, which it takes later.
    fn send(&self, e: usize) {
        let mut req = self.owned.request();
        if self.broken == Some(Broken::SealUnseen) {
            req.sealed = false;
        }
        self.engines[e].inbox.borrow_mut().push_back(req);
    }

    /// An engine takes the next version the owner sent it.
    fn deliver(&mut self, stats: &mut Stats) -> Result<(), String> {
        let ready: Vec<usize> = (0..self.engines.len())
            .filter(|&e| !self.engines[e].inbox.borrow().is_empty())
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let e = ready[self.rng.below(ready.len())];
        let u = self.engines[e].inbox.borrow_mut().pop_front().unwrap();
        self.engines[e].remote.update(&u).unwrap();
        self.poll(e, stats)
    }

    fn broadcast(&self, stats: &mut Stats) {
        if self.engines.iter().filter(|e| e.op.is_some()).count() > 1 {
            stats.released += 1;
        }
        for e in 0..self.engines.len() {
            self.send(e);
        }
    }

    fn answer(&mut self, stats: &mut Stats) -> Result<(), String> {
        let ready: Vec<usize> = (0..self.engines.len())
            .filter(|&e| !self.engines[e].pending.is_empty())
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let e = ready[self.rng.below(ready.len())];
        let (p, epoch) = self.engines[e].pending.pop_front().unwrap();
        if p.base != self.owned.version && self.broken != Some(Broken::StaleTaken) {
            stats.stale += 1;
            self.send(e);
            return self.poll_all(stats);
        }
        let o = &mut self.owned;
        if !o.sealed && !o.restored && self.first_change.is_some_and(|f| epoch > f) {
            stats.window += 1;
        }
        self.first_change = Some(self.first_change.map_or(epoch, |f| f.min(epoch)));
        if self.engines[e].late && p.holding.len() > o.holding.len() {
            stats.late_held += 1;
        }
        o.version += 1;
        o.schema = p.arrow_schema;
        o.delivered = p.delivered;
        o.holding = p.holding;
        o.epochs.push(epoch);
        self.broadcast(stats);
        self.poll_all(stats)
    }

    fn cross(&mut self, stats: &mut Stats) -> Result<(), String> {
        let merged = self.merged;
        let ready: Vec<(usize, usize)> = (0..self.engines.len())
            .flat_map(|e| {
                let lanes = &self.engines[e].lanes;
                (0..lanes.len())
                    .filter(move |&l| lanes[l].0.is_some() && lanes[l].1 <= merged + 2)
                    .map(move |l| (e, l))
            })
            .collect();
        if ready.is_empty() {
            return Ok(());
        }
        let (e, l) = ready[self.rng.below(ready.len())];
        let engine = &mut self.engines[e];
        let n = engine.lanes[l].1;
        let was = engine.owner.lock().shaper.sealed();
        engine
            .owner
            .cross(&engine.plan, 0, n, None, BTreeMap::new(), None)
            .map_err(|e| format!("a barrier failed: {e}"))?;
        engine.lanes[l].1 = n + 1;
        if !was && engine.owner.lock().shaper.sealed() && !self.owned.sealed {
            stats.local_seal_first += 1;
        }
        Ok(())
    }

    fn merge(&mut self, stats: &mut Stats) -> Result<(), String> {
        let n = self.merged + 1;
        let passed = self
            .engines
            .iter()
            .all(|e| e.lanes.iter().all(|(_, next)| *next > n));
        if !passed {
            return Ok(());
        }
        let decode = |b: &[u8]| ipc::decode_schema(b).unwrap();
        let o = &mut self.owned;
        let has = o.version > 0 || o.restored;
        let (schema, delivered) = match self.broken {
            Some(Broken::RecordFromEngine) => {
                let first = self.engines[0].owner.lock();
                let s = first.shaper.schema();
                (s.clone(), first.shaper.delivered().or(s))
            }
            _ if has => {
                let s = decode(&o.schema);
                let d = match o.delivered.is_empty() {
                    true => s.clone(),
                    false => decode(&o.delivered),
                };
                (Some(s), Some(d))
            }
            _ => (None, None),
        };
        if o.version > 0 && !o.sealed && !o.restored && self.broken != Some(Broken::NoMergeSeal) {
            o.sealed = true;
            if self.broken != Some(Broken::SealInPlace) {
                o.version += 1;
                o.epochs.push(0);
            }
            stats.seal_waiting += self.engines.iter().filter(|e| e.op.is_some()).count();
            self.broadcast(stats);
        }
        let o = &self.owned;
        if o.epochs.iter().any(|&x| x > n) {
            stats.record_later += 1;
        }
        // The record covers every row of the epoch.
        let got = delivered.as_deref().map(columns).unwrap_or_default();
        for b in self.emitted.iter().filter(|b| b.epoch == n) {
            if let Some(what) = unfit(&b.columns, &got) {
                return Err(format!("epoch {n}'s record doesn't cover a row's {what}"));
            }
        }
        // From the first record with columns, the destination gets the same.
        match &self.first {
            None if !got.is_empty() => self.first = Some(got),
            Some(first) if got != *first => {
                return Err(format!(
                    "epoch {n}'s record gives the destination {got:?}, after {first:?}"
                ));
            }
            _ => {}
        }
        self.records.push(Record {
            schema,
            delivered,
            holding: self.owned.holding.clone(),
        });
        self.emitted.retain(|b| b.epoch > n);
        self.merged = n;
        self.poll_all(stats)
    }

    /// The attempt loses a worker: the next starts from the last record, on
    /// other engines, with what it restored.
    fn restore(&mut self, stats: &mut Stats) -> Result<(), String> {
        if self.merged == 0 || self.restores == 2 {
            return Ok(());
        }
        self.restores += 1;
        let r = self.records.pop().unwrap();
        let columns = r.schema.is_some();
        self.owned = Owned {
            schema: r
                .schema
                .as_ref()
                .map(|s| ipc::encode_schema(s).unwrap())
                .unwrap_or_default(),
            delivered: match (&r.delivered, r.holding.is_empty()) {
                (Some(d), false) => ipc::encode_schema(d).unwrap(),
                _ => Vec::new(),
            },
            holding: r.holding.clone(),
            restored: columns,
            epochs: vec![0],
            ..Default::default()
        };
        self.emitted.clear();
        self.first_change = None;
        self.engines = self.attempt(self.merged + 1, Some(&r));
        self.records.push(r);
        match columns {
            true => stats.restored_with += 1,
            false => stats.restored_without += 1,
        }
        // A restore starts the stream sealed exactly where the record has
        // its columns.
        for (i, e) in self.engines.iter().enumerate() {
            if e.owner.lock().shaper.sealed() != columns {
                return Err(format!(
                    "engine {i} restored the stream sealed {}, from a record with columns {columns}",
                    !columns
                ));
            }
        }
        Ok(())
    }
}

/// Runs seed's steps; why the first invariant broke, if one did.
fn run(seed: u64, broken: Option<Broken>, stats: &mut Stats) -> Result<(), String> {
    let mut m = Model::new(seed, broken);
    stats.seeds += 1;
    for step in 0..STEPS {
        m.step(stats)
            .map_err(|e| format!("seed {seed}, step {step}: {e}"))?;
    }
    Ok(())
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("OPENETL_SCHEMA_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let n = std::env::var("OPENETL_SCHEMA_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    (0..n).collect()
}

#[test]
fn the_schema_protocol_keeps_its_invariants() {
    let mut stats = Stats::default();
    for seed in seeds() {
        if let Err(e) = run(seed, None, &mut stats) {
            panic!("{e}");
        }
    }
    eprintln!("{stats:#?}");
    let reached = [
        stats.stale,
        stats.released,
        stats.seal_waiting,
        stats.late_held,
        stats.local_seal_first,
        stats.window,
        stats.later_in_earlier,
        stats.record_later,
        stats.restored_with,
        stats.restored_without,
        stats.mid_shape,
        stats.mid_follow,
        stats.reshaped,
    ];
    if seeds().len() >= 300 {
        assert!(reached.iter().all(|&n| n > 0), "{stats:#?}");
    }
}

#[test]
fn each_broken_variant_breaks_an_invariant() {
    // A seed replays alone; a sweep runs at least 300.
    let replay = std::env::var("OPENETL_SCHEMA_SEED").is_ok();
    let seeds: Vec<u64> = match replay {
        true => seeds(),
        false => (0..u64::try_from(seeds().len().max(300)).unwrap()).collect(),
    };
    for broken in BROKEN {
        let mut stats = Stats::default();
        let mut caught = Vec::new();
        for &seed in &seeds {
            if let Err(e) = run(seed, Some(broken), &mut stats) {
                caught.push(e);
            }
        }
        eprintln!(
            "{broken:?}: {} of {} seeds broke an invariant; the first: {:?}",
            caught.len(),
            seeds.len(),
            caught.first()
        );
        assert!(
            replay || !caught.is_empty(),
            "{broken:?} kept every invariant"
        );
    }
}
