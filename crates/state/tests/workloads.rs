//! What runs cost on three workloads: a dedupe of mostly new ids, a
//! windowed aggregate that keeps its rows, and one that doesn't. Each
//! epoch prints its requests, bytes, why lookups reached storage and the
//! write amplification; then the state restores onto the same lanes and
//! others, each from a cold cache, and an epoch that closes windows after
//! each prints too.
//!
//! Run one with `cargo test --release -p openetl-state --test workloads --
//! --ignored --nocapture dedupe`. `OPENETL_STATE_LANES`, `_EPOCHS`,
//! `_EVENTS` (an epoch's), `_KEYS` (the windows' key space), `_LATENCY_MS`
//! (added to each request), `_CACHE_MB` (the block cache, for all lanes),
//! `_RETENTION` (the dedupe's, in epochs) and `_RESCALE` (lanes to restore
//! onto, as `3,8`) change its size. `_DROP=0` names every section, and
//! the windows don't move their horizon, as before dead sections were
//! dropped.
//!
//! The columns: the epoch's time and its slowest barrier's; runs written
//! and their KiB; reads of blocks, reads opening runs, and the KiB of both;
//! probes of a section past its filter that read a block holding the key,
//! that read one that didn't (false positives), and that the cache
//! answered; probes the filters ruled out; the KiB of changes; write
//! amplification, written over changed; the lanes' indexes and filters,
//! and the cache, in MB; windows' accumulators and rows emitted; the
//! dedupe's repeats; and the sections the lanes' snapshots name.

#![allow(clippy::unwrap_used)]

use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use object_store::memory::InMemory;
use object_store::path::Path;
use openetl_state::runs::Counts;
use openetl_state::{
    BoxFuture, KeyGroupRange, KeyRef, Layer, ObjectStorage, Objects, OperatorId, Result, RunConfig,
    RunStores, Snapshot, StateBackend,
};
use twox_hash::XxHash3_64;

const GROUPS: u32 = 256;
const EPOCH: i64 = 10_000_000;
const DELAY: i64 = EPOCH / 2;
const WINDOW: i64 = 6 * EPOCH;
const BATCH: usize = 4096;
const OP: OperatorId = OperatorId(1);

#[derive(Clone, Debug)]
struct Args {
    lanes: u32,
    epochs: u64,
    events: u64,
    keys: u64,
    latency: Duration,
    cache_bytes: usize,
    retention: i64,
    rescale: Vec<u32>,
    drop: bool,
}

fn env(name: &str, default: u64) -> u64 {
    std::env::var(format!("OPENETL_STATE_{name}"))
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn args() -> Args {
    let rescale = std::env::var("OPENETL_STATE_RESCALE").unwrap_or_else(|_| "3,8".into());
    Args {
        lanes: env("LANES", 4) as u32,
        epochs: env("EPOCHS", 20),
        events: env("EVENTS", 100_000),
        keys: env("KEYS", 200_000),
        latency: Duration::from_millis(env("LATENCY_MS", 20)),
        cache_bytes: (env("CACHE_MB", 64) as usize) << 20,
        retention: env("RETENTION", 32) as i64,
        drop: env("DROP", 1) != 0,
        rescale: rescale.split(',').filter_map(|s| s.parse().ok()).collect(),
    }
}

/// An in-memory bucket that adds a latency to each request.
struct Slow {
    inner: ObjectStorage,
    latency: Duration,
}

impl Objects for Slow {
    fn put_new<'a>(&'a self, name: &'a str, data: Bytes) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            tokio::time::sleep(self.latency).await;
            self.inner.put_new(name, data).await
        })
    }

    fn get_range<'a>(&'a self, name: &'a str, range: Range<u64>) -> BoxFuture<'a, Result<Bytes>> {
        Box::pin(async move {
            tokio::time::sleep(self.latency).await;
            self.inner.get_range(name, range).await
        })
    }
}

fn ranges(lanes: u32) -> Vec<KeyGroupRange> {
    (0..lanes)
        .map(|i| KeyGroupRange::new(GROUPS * i / lanes, GROUPS * (i + 1) / lanes - 1))
        .collect()
}

fn group_of(k: &[u8]) -> u32 {
    (XxHash3_64::oneshot(k) % u64::from(GROUPS)) as u32
}

fn lane_of(ranges: &[KeyGroupRange], g: u32) -> usize {
    ranges.iter().position(|r| r.contains(g)).unwrap()
}

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
        if n == 0 { 0 } else { self.next() % n }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Workload {
    Dedupe,
    Window { rows: bool },
}

/// What a lane does in an epoch: its events, keyed and grouped.
#[derive(Clone)]
struct Event {
    key: Bytes,
    group: u32,
    time: i64,
    value: i64,
}

/// The windows' events: keys from a space that moves on by a tenth each
/// epoch, so keys arrive and leave.
fn window_events(a: &Args, epoch: u64) -> Vec<Event> {
    let mut r = Rng(epoch.wrapping_mul(7919) ^ 0xa5a5);
    let base = epoch * (a.keys / 10);
    (0..a.events)
        .map(|_| {
            let key = Bytes::from(format!("user-{:012}", base + r.below(a.keys)));
            let group = group_of(&key);
            let time = epoch as i64 * EPOCH + r.below(EPOCH as u64) as i64;
            let value = r.below(1000) as i64;
            Event {
                key,
                group,
                time,
                value,
            }
        })
        .collect()
}

fn new_id(epoch: u64, i: u64) -> Bytes {
    let mut b = BytesMut::with_capacity(16);
    b.put_u64(XxHash3_64::oneshot(
        &[epoch.to_le_bytes(), i.to_le_bytes()].concat(),
    ));
    b.put_u64(XxHash3_64::oneshot_with_seed(7, &i.to_le_bytes()));
    b.freeze()
}

/// The dedupe's ids: 98% new, 1% repeats of the last epoch's, 1% of an
/// older one within the retention. Each carries its time as `value`.
fn dedupe_ids(a: &Args, epoch: u64) -> Vec<Event> {
    let mut r = Rng(epoch.wrapping_mul(104_729) ^ 0x5a5a);
    (0..a.events)
        .map(|i| {
            let roll = r.below(100);
            let id = if roll == 0 && epoch > 0 {
                new_id(epoch - 1, r.below(a.events))
            } else if roll == 1 && epoch > 2 {
                let back = 2 + r.below(((a.retention as u64).saturating_sub(4)).min(epoch - 2));
                new_id(epoch - back, r.below(a.events))
            } else {
                new_id(epoch, i)
            };
            let group = group_of(&id);
            let time = epoch as i64 * EPOCH + r.below(EPOCH as u64) as i64;
            Event {
                key: id,
                group,
                time,
                value: 0,
            }
        })
        .collect()
}

fn window_key(e: &Event) -> Bytes {
    let start = e.time.div_euclid(WINDOW) * WINDOW;
    let mut b = BytesMut::with_capacity(e.key.len() + 8);
    b.put_slice(&e.key);
    b.put_i64(start);
    b.freeze()
}

/// A count, a sum, a least and a greatest.
fn acc(prev: Option<&Bytes>, v: i64) -> Bytes {
    let (mut c, mut s, mut lo, mut hi) = (0i64, 0i64, i64::MAX, i64::MIN);
    if let Some(p) = prev.filter(|p| p.len() == 32) {
        let f = |i: usize| i64::from_le_bytes(p[i * 8..i * 8 + 8].try_into().unwrap());
        (c, s, lo, hi) = (f(0), f(1), f(2), f(3));
    }
    let mut b = BytesMut::with_capacity(32);
    for x in [c + 1, s + v, lo.min(v), hi.max(v)] {
        b.put_slice(&x.to_le_bytes());
    }
    b.freeze()
}

#[derive(Default, Clone, Copy)]
struct LaneOut {
    barrier: Duration,
    emitted: u64,
    repeats: u64,
    /// Sections the lane's snapshot names.
    named: usize,
}

fn named(snapshots: &[Snapshot]) -> usize {
    snapshots
        .iter()
        .flat_map(|s| openetl_state::runs::runs(s).unwrap())
        .map(|r| r.sections.len())
        .sum()
}

async fn window_epoch(
    l: &mut Layer,
    events: Vec<Event>,
    epoch: u64,
    rows: bool,
    drop: bool,
) -> LaneOut {
    let mut out = LaneOut::default();
    for chunk in events.chunks(BATCH) {
        let keys: Vec<Bytes> = chunk.iter().map(window_key).collect();
        let refs: Vec<KeyRef> = chunk
            .iter()
            .zip(&keys)
            .map(|(e, k)| KeyRef::new(e.group, k))
            .collect();
        let prev = l.get(OP, &refs).await.unwrap();
        let mut seen: HashMap<&[u8], Bytes> = HashMap::new();
        for ((e, k), p) in chunk.iter().zip(&refs).zip(prev) {
            let before = seen.get(k.key).cloned().or(p);
            let end = e.time.div_euclid(WINDOW) * WINDOW + WINDOW;
            if before.is_none() {
                l.set_timer(OP, *k, end);
            }
            let next = acc(before.as_ref(), e.value);
            seen.insert(k.key, next.clone());
            l.put(OP, *k, next);
            if rows {
                let mut row = BytesMut::with_capacity(32);
                row.put_i64(e.time);
                row.put_i64(e.value);
                row.put_slice(&e.key[..16.min(e.key.len())]);
                l.append(OP, *k, end, row.freeze());
            }
        }
    }
    let watermark = (epoch as i64 + 1) * EPOCH - DELAY;
    let fired = l.fire(OP, watermark, usize::MAX).await.unwrap();
    for chunk in fired.chunks(BATCH) {
        let refs: Vec<KeyRef> = chunk.iter().map(|t| KeyRef::new(t.group, &t.key)).collect();
        let accs = l.get(OP, &refs).await.unwrap();
        out.emitted += accs.iter().filter(|a| a.is_some()).count() as u64;
        for k in &refs {
            l.delete(OP, *k);
        }
    }
    if rows {
        let taken = l.take_rows(OP, watermark, usize::MAX).await.unwrap();
        out.emitted += taken.len() as u64;
    }
    if drop {
        l.expire(OP, watermark);
    }
    let t = Instant::now();
    out.named = named(&l.barrier(epoch).await.unwrap());
    out.barrier = t.elapsed();
    out
}

async fn dedupe_epoch(l: &mut Layer, ids: Vec<Event>, epoch: u64, retention: i64) -> LaneOut {
    let mut out = LaneOut::default();
    for chunk in ids.chunks(BATCH) {
        let refs: Vec<KeyRef> = chunk.iter().map(|i| KeyRef::new(i.group, &i.key)).collect();
        let at: Vec<i64> = chunk.iter().map(|i| i.time).collect();
        let expires: Vec<i64> = at.iter().map(|t| t + retention).collect();
        let fresh = l.insert_new(OP, &refs, &at, &expires).await.unwrap();
        out.repeats += fresh.iter().filter(|f| !**f).count() as u64;
    }
    let watermark = (epoch as i64 + 1) * EPOCH - DELAY;
    l.expire(OP, watermark);
    let t = Instant::now();
    out.named = named(&l.barrier(epoch).await.unwrap());
    out.barrier = t.elapsed();
    out
}

struct Run {
    args: Args,
    workload: Workload,
    bucket: Arc<InMemory>,
}

impl Run {
    fn stores(&self) -> RunStores {
        let inner = ObjectStorage::new(self.bucket.clone(), Path::from("state"));
        let slow = Slow {
            inner,
            latency: self.args.latency,
        };
        let config = RunConfig {
            cache_bytes: self.args.cache_bytes,
            drop: self.args.drop,
            ..Default::default()
        };
        RunStores::new(Arc::new(slow), config)
    }

    /// An epoch on every lane at once, and a line of what it cost.
    async fn epoch(
        &self,
        stores: &RunStores,
        lanes: Vec<Layer>,
        epoch: u64,
        label: &str,
    ) -> (Vec<Layer>, Counts) {
        let ranges: Vec<KeyGroupRange> = lanes.iter().map(|l| l.range()).collect();
        let events = match self.workload {
            Workload::Dedupe => dedupe_ids(&self.args, epoch),
            Workload::Window { .. } => window_events(&self.args, epoch),
        };
        let mut per: Vec<Vec<Event>> = vec![Vec::new(); ranges.len()];
        for e in events {
            per[lane_of(&ranges, e.group)].push(e);
        }
        let before = stores.counts();
        let started = Instant::now();
        let mut tasks = Vec::new();
        for (mut l, evs) in lanes.into_iter().zip(per) {
            let (workload, retention) = (self.workload, self.args.retention * EPOCH);
            let drop = self.args.drop;
            tasks.push(tokio::spawn(async move {
                let out = match workload {
                    Workload::Dedupe => dedupe_epoch(&mut l, evs, epoch, retention).await,
                    Workload::Window { rows } => window_epoch(&mut l, evs, epoch, rows, drop).await,
                };
                (l, out)
            }));
        }
        let mut lanes = Vec::new();
        let mut outs = Vec::new();
        for t in tasks {
            let (l, o) = t.await.unwrap();
            lanes.push(l);
            outs.push(o);
        }
        let wall = started.elapsed();
        let c = stores.counts() - before;
        let barrier = outs.iter().map(|o| o.barrier).max().unwrap_or_default();
        let memory: usize = lanes.iter().map(|l| l.memory().store).sum();
        let emitted: u64 = outs.iter().map(|o| o.emitted).sum();
        let repeats: u64 = outs.iter().map(|o| o.repeats).sum();
        let named: usize = outs.iter().map(|o| o.named).sum();
        println!(
            "{label:>9} {epoch:>3} {:>7.0} {:>6.0} {:>4} {:>7.0} {:>6} {:>4} {:>8.0} {:>6} {:>6} {:>6} {:>8} {:>8.0} {:>5.2} {:>6.1} {:>6.1} {:>7} {:>7} {:>5}",
            ms(wall),
            ms(barrier),
            c.barrier.count,
            kib(c.barrier.bytes),
            c.lookup.count,
            c.restore.count,
            kib(c.lookup.bytes + c.restore.bytes),
            c.hits,
            c.false_positives,
            c.cached,
            c.filtered,
            kib(c.changed_bytes),
            amplification(&c),
            mib(memory as u64),
            mib(stores.cache_bytes() as u64),
            emitted,
            repeats,
            named,
        );
        (lanes, c)
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn kib(b: u64) -> f64 {
    b as f64 / 1024.0
}

fn mib(b: u64) -> f64 {
    b as f64 / (1024.0 * 1024.0)
}

fn amplification(c: &Counts) -> f64 {
    match c.changed_bytes {
        0 => 0.0,
        n => c.barrier.bytes as f64 / n as f64,
    }
}

fn header() {
    println!(
        "{:>9} {:>3} {:>7} {:>6} {:>4} {:>7} {:>6} {:>4} {:>8} {:>6} {:>6} {:>6} {:>8} {:>8} {:>5} {:>6} {:>6} {:>7} {:>7} {:>5}",
        "",
        "ep",
        "wall ms",
        "bar ms",
        "PUT",
        "PUT KiB",
        "GET",
        "open",
        "GET KiB",
        "hits",
        "fp",
        "cached",
        "filtered",
        "chg KiB",
        "WA",
        "mem MB",
        "cch MB",
        "emitted",
        "repeats",
        "named"
    );
}

async fn open(
    stores: &RunStores,
    generation: u64,
    epoch: u64,
    lanes: u32,
    from: &[Snapshot],
) -> Vec<Layer> {
    let mut out = Vec::new();
    for r in ranges(lanes) {
        out.push(
            Layer::open(stores, generation, epoch, r, from)
                .await
                .unwrap(),
        );
    }
    out
}

async fn measure(workload: Workload) {
    let args = args();
    println!("{workload:?} {args:?}");
    let run = Run {
        args: args.clone(),
        workload,
        bucket: Arc::new(InMemory::new()),
    };
    header();
    let stores = run.stores();
    let mut lanes = open(&stores, 1, 1, args.lanes, &[]).await;
    let mut total = Counts::default();
    for e in 1..=args.epochs {
        let (ls, c) = run.epoch(&stores, lanes, e, "run").await;
        lanes = ls;
        total = total + c;
    }
    let mut record = Vec::new();
    for l in &mut lanes {
        record.extend(l.barrier(args.epochs + 1).await.unwrap());
    }
    println!(
        "totals: {} PUTs, {:.0} KiB written of {:.0} KiB changed (WA {:.2}); {} lookup GETs, {} opening; {} hits, {} false positives",
        total.barrier.count,
        kib(total.barrier.bytes),
        kib(total.changed_bytes),
        amplification(&total),
        total.lookup.count,
        total.restore.count,
        total.hits,
        total.false_positives
    );
    drop(lanes);
    // The first epoch after the record whose watermark closes windows.
    let windows = (WINDOW / EPOCH) as u64;
    let next = (args.epochs + 2).div_ceil(windows) * windows;
    for (i, n) in std::iter::once(args.lanes)
        .chain(args.rescale.iter().copied())
        .enumerate()
    {
        let stores = run.stores();
        let t = Instant::now();
        let lanes = open(&stores, 2 + i as u64, next, n, &record).await;
        println!(
            "restore onto {n} lanes: {:.1} ms, {:?}",
            ms(t.elapsed()),
            stores.counts()
        );
        run.epoch(&stores, lanes, next, &format!("after {n}")).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement: run it with --ignored --nocapture"]
async fn dedupe() {
    measure(Workload::Dedupe).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement: run it with --ignored --nocapture"]
async fn window_with_rows() {
    measure(Workload::Window { rows: true }).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement: run it with --ignored --nocapture"]
async fn window_without_rows() {
    measure(Workload::Window { rows: false }).await;
}
