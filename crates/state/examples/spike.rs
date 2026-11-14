//! Measures the two stores behind the state layer on two workloads: a
//! windowed aggregate (a value, rows and a timer per key and window, keys
//! arriving and leaving with time) and a dedupe set with expiry.
//!
//! `spike <window|dedupe> <runs|slate> [--dir D] [--lanes N] [--epochs N]
//! [--events N] [--latency-ms N] [--rescale 3,8] [--groups-per-run N]
//! [--no-rows]`
//!
//! Prints a JSON object per epoch and one per restore. Objects live in a
//! local directory behind a store that counts requests and bytes and adds a
//! fixed latency to each. Other flags: --keys, --retention-epochs, --keep,
//! --interval-ms, --gc-secs, --compactor-poll-ms, --l0-max-ssts.

// A measurement: any failure ends it.
#![allow(clippy::expect_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use openetl_state::counting::{Counters, Counting, Counts, Shared};
use openetl_state::layer::{self, DEFAULT_CACHE_BYTES, Layer};
use openetl_state::runs::{MergeStats, RunConfig, RunStore};
use openetl_state::slate::{SlateConfig, SlateStore, Stores};
use openetl_state::{
    Change, KeyGroup, KeyGroupRange, KeyRef, Result, Snapshot, StateBackend, Store,
};
use twox_hash::XxHash3_64;

const GROUPS: u32 = 256;
const EPOCH_MICROS: i64 = 10_000_000;
const BATCH: usize = 4096;

#[derive(Clone, Debug)]
struct Args {
    workload: String,
    store: String,
    dir: String,
    lanes: u32,
    epochs: u64,
    events: u64,
    latency: Duration,
    rescale: Vec<u32>,
    groups_per_run: u32,
    rows: bool,
    keys: u64,
    retention_epochs: i64,
    gc_secs: u64,
    keep: u64,
    /// The least time an epoch takes, as a checkpoint interval would.
    interval: Duration,
    compactor_poll: Duration,
    l0_max_ssts: usize,
}

fn args() -> Args {
    let a: Vec<String> = std::env::args().collect();
    let mut out = Args {
        workload: a.get(1).cloned().unwrap_or_else(|| "dedupe".into()),
        store: a.get(2).cloned().unwrap_or_else(|| "runs".into()),
        dir: "/Volumes/SM_SSD1/openetl-state-spike/run".into(),
        lanes: 4,
        epochs: 20,
        events: 100_000,
        latency: Duration::from_millis(20),
        rescale: vec![3, 8],
        groups_per_run: GROUPS,
        rows: true,
        keys: 200_000,
        retention_epochs: 32,
        gc_secs: 10,
        keep: 2,
        interval: Duration::from_millis(2000),
        compactor_poll: Duration::from_secs(1),
        l0_max_ssts: 32,
    };
    let mut i = 3;
    while i < a.len() {
        let v = a.get(i + 1).cloned().unwrap_or_default();
        let n = || v.parse::<u64>().unwrap_or(0);
        match a[i].as_str() {
            "--dir" => out.dir = v.clone(),
            "--lanes" => out.lanes = n() as u32,
            "--epochs" => out.epochs = n(),
            "--events" => out.events = n(),
            "--keys" => out.keys = n(),
            "--latency-ms" => out.latency = Duration::from_millis(n()),
            "--groups-per-run" => out.groups_per_run = n() as u32,
            "--retention-epochs" => out.retention_epochs = n() as i64,
            "--gc-secs" => out.gc_secs = n(),
            "--keep" => out.keep = n(),
            "--interval-ms" => out.interval = Duration::from_millis(n()),
            "--compactor-poll-ms" => out.compactor_poll = Duration::from_millis(n()),
            "--l0-max-ssts" => out.l0_max_ssts = n() as usize,
            "--rescale" => {
                out.rescale = v.split(',').filter_map(|s| s.parse().ok()).collect();
            }
            "--no-rows" => {
                out.rows = false;
                i += 1;
                continue;
            }
            other => panic!("unknown flag {other}"),
        }
        i += 2;
    }
    out
}

/// Where objects go, counted by who asks.
struct Env {
    fg: Arc<dyn ObjectStore>,
    bg: Arc<dyn ObjectStore>,
    gc: Arc<dyn ObjectStore>,
    fg_counts: Arc<Counters>,
    bg_counts: Arc<Counters>,
    gc_counts: Arc<Counters>,
    merges: std::sync::Mutex<Vec<Arc<MergeStats>>>,
}

impl Env {
    fn new(dir: &str, latency: Duration) -> Self {
        std::fs::create_dir_all(dir).expect("create the directory");
        let inner: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dir).expect("open the directory"));
        let shared = Arc::new(Shared::default());
        let (fg, fg_counts) = Counting::new(inner.clone(), latency, shared.clone());
        let (bg, bg_counts) = Counting::new(inner.clone(), latency, shared.clone());
        let (gc, gc_counts) = Counting::new(inner, latency, shared);
        Self {
            fg,
            bg,
            gc,
            fg_counts,
            bg_counts,
            gc_counts,
            merges: std::sync::Mutex::new(Vec::new()),
        }
    }

    fn counts(&self) -> (Counts, Counts, Counts) {
        (
            self.fg_counts.counts(),
            self.bg_counts.counts(),
            self.gc_counts.counts(),
        )
    }
}

enum AnyStore {
    Runs(RunStore),
    Slate(SlateStore),
}

impl Store for AnyStore {
    async fn get_many(&self, keys: Vec<Bytes>) -> Result<Vec<Option<Bytes>>> {
        match self {
            AnyStore::Runs(s) => s.get_many(keys).await,
            AnyStore::Slate(s) => s.get_many(keys).await,
        }
    }
    async fn scan(&self, start: Bytes, end: Bytes) -> Result<Vec<(Bytes, Bytes)>> {
        match self {
            AnyStore::Runs(s) => s.scan(start, end).await,
            AnyStore::Slate(s) => s.scan(start, end).await,
        }
    }
    async fn commit(&mut self, epoch: u64, wm: i64, changes: Vec<Change>) -> Result<Vec<Snapshot>> {
        match self {
            AnyStore::Runs(s) => s.commit(epoch, wm, changes).await,
            AnyStore::Slate(s) => s.commit(epoch, wm, changes).await,
        }
    }
    async fn release(&mut self, epoch: u64) -> Result<()> {
        match self {
            AnyStore::Runs(s) => s.release(epoch).await,
            AnyStore::Slate(s) => s.release(epoch).await,
        }
    }
}

impl AnyStore {
    async fn close(&self) {
        match self {
            AnyStore::Runs(s) => s.settle().await,
            AnyStore::Slate(s) => {
                let _ = s.close().await;
            }
        }
    }

    fn footprint(&self) -> String {
        match self {
            AnyStore::Runs(s) => {
                let (runs, bytes) = s.footprint();
                format!("{{\"runs\":{runs},\"pinned_bytes\":{bytes}}}")
            }
            AnyStore::Slate(s) => {
                let (l0, srs, cps) = s.footprint();
                format!("{{\"l0\":{l0},\"sorted_runs\":{srs},\"checkpoints\":{cps}}}")
            }
        }
    }
}

async fn open(
    a: &Args,
    env: &Env,
    generation: u64,
    range: KeyGroupRange,
    restore: &[Snapshot],
) -> AnyStore {
    let base = Path::from("state");
    match a.store.as_str() {
        "runs" => {
            let s = RunStore::open(
                env.fg.clone(),
                env.bg.clone(),
                base,
                generation,
                range,
                restore,
                RunConfig {
                    groups_per_run: a.groups_per_run,
                    ..Default::default()
                },
            )
            .await
            .expect("open runs");
            env.merges.lock().expect("lock").push(s.merges());
            AnyStore::Runs(s)
        }
        "slate" => AnyStore::Slate(
            SlateStore::open(
                Stores {
                    db: env.fg.clone(),
                    compactor: env.bg.clone(),
                    gc: env.gc.clone(),
                },
                &base,
                generation,
                range,
                restore,
                SlateConfig {
                    gc_interval: Duration::from_secs(a.gc_secs),
                    gc_min_age: Duration::from_secs(1),
                    compactor_poll: a.compactor_poll,
                    l0_max_ssts: a.l0_max_ssts,
                    ..Default::default()
                },
            )
            .await
            .expect("open slatedb"),
        ),
        other => panic!("unknown store {other}"),
    }
}

fn h(parts: &[&[u8]]) -> u64 {
    let mut b = Vec::new();
    for p in parts {
        b.extend_from_slice(p);
    }
    XxHash3_64::oneshot(&b)
}

fn group_of(k: &[u8]) -> KeyGroup {
    (XxHash3_64::oneshot(k) % u64::from(GROUPS)) as KeyGroup
}

fn lane_of(ranges: &[KeyGroupRange], g: KeyGroup) -> usize {
    ranges.iter().position(|r| r.contains(g)).unwrap_or(0)
}

/// A deterministic stream of numbers.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        if n == 0 { 0 } else { self.next() % n }
    }
}

/// An event of the windowed aggregate: its key, event time and value.
#[derive(Clone)]
struct Event {
    key: Bytes,
    group: KeyGroup,
    time: i64,
    value: i64,
    seq: u64,
}

const WINDOW: i64 = 6 * EPOCH_MICROS;
const DELAY: i64 = EPOCH_MICROS / 2;

fn window_events(a: &Args, epoch: u64) -> Vec<Event> {
    let mut r = Rng(epoch.wrapping_mul(7919) ^ 0xA5A5);
    let churn = a.keys / 10;
    let base = epoch * churn;
    (0..a.events)
        .map(|i| {
            let id = base + r.below(a.keys);
            let key = Bytes::from(format!("user-{id:012}"));
            let group = group_of(&key);
            let time = epoch as i64 * EPOCH_MICROS + r.below(EPOCH_MICROS as u64) as i64;
            Event {
                key,
                group,
                time,
                value: r.below(1000) as i64,
                seq: epoch * a.events + i,
            }
        })
        .collect()
}

/// A key and its window, as the operator keys its state.
fn window_key(e: &Event) -> Bytes {
    let w = e.time.div_euclid(WINDOW) * WINDOW;
    let mut b = BytesMut::with_capacity(e.key.len() + 8);
    b.put_slice(&e.key);
    b.put_i64(w);
    b.freeze()
}

fn acc(prev: Option<&Bytes>, v: i64) -> Bytes {
    let (mut c, mut s, mut lo, mut hi) = (0i64, 0i64, i64::MAX, i64::MIN);
    if let Some(p) = prev
        && p.len() == 32
    {
        let f = |i: usize| i64::from_le_bytes(p[i * 8..i * 8 + 8].try_into().unwrap_or([0; 8]));
        (c, s, lo, hi) = (f(0), f(1), f(2), f(3));
    }
    c += 1;
    s += v;
    lo = lo.min(v);
    hi = hi.max(v);
    let mut b = BytesMut::with_capacity(32);
    for x in [c, s, lo, hi] {
        b.put_slice(&x.to_le_bytes());
    }
    b.freeze()
}

#[derive(Default, Clone, Copy)]
struct LaneOut {
    work: Duration,
    barrier: Duration,
    emitted: u64,
    inserted: u64,
    repeats: u64,
}

/// One lane's epoch of the windowed aggregate.
async fn window_epoch(
    l: &mut Layer<AnyStore>,
    events: Vec<Event>,
    epoch: u64,
    rows: bool,
) -> LaneOut {
    let mut out = LaneOut::default();
    let t = Instant::now();
    for chunk in events.chunks(BATCH) {
        let wkeys: Vec<Bytes> = chunk.iter().map(window_key).collect();
        let refs: Vec<KeyRef<'_>> = chunk
            .iter()
            .zip(&wkeys)
            .map(|(e, k)| KeyRef {
                group: e.group,
                key: k,
            })
            .collect();
        let prev = l.get(0, &refs).await.expect("get");
        let mut seen: HashMap<Bytes, Bytes> = HashMap::new();
        for ((e, k), p) in chunk.iter().zip(&refs).zip(prev) {
            let before = seen
                .get(&Bytes::copy_from_slice(k.key))
                .cloned()
                .or(p.clone());
            if before.is_none() {
                let end = e.time.div_euclid(WINDOW) * WINDOW + WINDOW;
                l.set_timer(0, end, *k);
            }
            let next = acc(before.as_ref(), e.value);
            seen.insert(Bytes::copy_from_slice(k.key), next.clone());
            l.put(0, *k, next);
            if rows {
                let mut row = BytesMut::with_capacity(32);
                row.put_i64(e.time);
                row.put_i64(e.value);
                row.put_u64(e.seq);
                row.put_u64(0);
                l.append(0, *k, e.seq, row.freeze());
            }
        }
    }
    let watermark = (epoch as i64 + 1) * EPOCH_MICROS - DELAY;
    let fired = l.fire(0, watermark);
    for chunk in fired.chunks(BATCH) {
        let refs: Vec<KeyRef<'_>> = chunk
            .iter()
            .map(|(_, g, k)| KeyRef { group: *g, key: k })
            .collect();
        let accs = l.get(0, &refs).await.expect("get");
        if rows {
            let taken = l.take_rows(0, &refs).await.expect("rows");
            out.emitted += taken.iter().filter(|r| !r.is_empty()).count() as u64;
        } else {
            out.emitted += accs.iter().filter(|a| a.is_some()).count() as u64;
        }
        for k in &refs {
            l.delete(0, *k);
        }
    }
    out.work = t.elapsed();
    let t = Instant::now();
    l.barrier(epoch, watermark).await.expect("barrier");
    out.barrier = t.elapsed();
    out
}

#[derive(Clone)]
struct Id {
    id: Bytes,
    group: KeyGroup,
    at: i64,
}

fn new_id(epoch: u64, i: u64) -> Bytes {
    let a = h(&[&epoch.to_le_bytes(), &i.to_le_bytes(), b"a"]);
    let b = h(&[&epoch.to_le_bytes(), &i.to_le_bytes(), b"b"]);
    let mut v = BytesMut::with_capacity(16);
    v.put_u64(a);
    v.put_u64(b);
    v.freeze()
}

/// The epoch's ids: mostly new, 1% repeats of the last epoch's, and 1% of an
/// older one inside the retention. Returns them and how many repeat an id
/// that hasn't expired.
fn dedupe_ids(a: &Args, epoch: u64) -> (Vec<Id>, u64) {
    let mut r = Rng(epoch.wrapping_mul(104_729) ^ 0x5A5A);
    let mut repeats = 0;
    let at0 = epoch as i64 * EPOCH_MICROS;
    let ids = (0..a.events)
        .map(|i| {
            let roll = r.below(100);
            let id = if roll == 0 && epoch > 0 {
                repeats += 1;
                new_id(epoch - 1, a.events + r.below(a.events))
            } else if roll == 1 && epoch > 2 {
                let back = 2 + r.below((a.retention_epochs as u64 - 4).min(epoch - 2));
                repeats += 1;
                new_id(epoch - back, a.events + r.below(a.events))
            } else {
                new_id(epoch, a.events + i)
            };
            let group = group_of(&id);
            Id {
                id,
                group,
                at: at0 + r.below(EPOCH_MICROS as u64) as i64,
            }
        })
        .collect();
    (ids, repeats)
}

async fn dedupe_epoch(
    l: &mut Layer<AnyStore>,
    ids: Vec<Id>,
    epoch: u64,
    retention: i64,
) -> LaneOut {
    let mut out = LaneOut::default();
    let t = Instant::now();
    for chunk in ids.chunks(BATCH) {
        let refs: Vec<KeyRef<'_>> = chunk
            .iter()
            .map(|i| KeyRef {
                group: i.group,
                key: &i.id,
            })
            .collect();
        let at: Vec<i64> = chunk.iter().map(|i| i.at).collect();
        let exp: Vec<i64> = chunk.iter().map(|i| i.at + retention).collect();
        let fresh = l.insert_new(1, &refs, &at, &exp).await.expect("insert");
        let n = fresh.iter().filter(|f| **f).count() as u64;
        out.inserted += n;
        out.repeats += chunk.len() as u64 - n;
    }
    out.work = t.elapsed();
    let watermark = (epoch as i64 + 1) * EPOCH_MICROS - DELAY;
    let t = Instant::now();
    l.barrier(epoch, watermark).await.expect("barrier");
    out.barrier = t.elapsed();
    out
}

fn counts_json(c: &Counts) -> String {
    format!(
        "{{\"puts\":{},\"put_bytes\":{},\"gets\":{},\"get_bytes\":{},\"heads\":{},\"lists\":{},\"deletes\":{},\"meta_puts\":{},\"meta_gets\":{}}}",
        c.puts,
        c.put_bytes,
        c.gets,
        c.get_bytes,
        c.heads,
        c.lists,
        c.deletes,
        c.meta_puts,
        c.meta_gets
    )
}

/// Runs an epoch on every lane at once, and prints what it cost.
async fn epoch(
    a: &Args,
    env: &Env,
    lanes: Vec<Layer<AnyStore>>,
    ranges: &[KeyGroupRange],
    e: u64,
    label: &str,
) -> Vec<Layer<AnyStore>> {
    let before = env.counts();
    let stats_before: Vec<_> = lanes.iter().map(|l| l.stats()).collect();
    let started = Instant::now();
    let mut tasks = Vec::new();
    let mut expected_repeats = 0;
    match a.workload.as_str() {
        "window" => {
            let mut per: Vec<Vec<Event>> = vec![Vec::new(); ranges.len()];
            for ev in window_events(a, e) {
                per[lane_of(ranges, ev.group)].push(ev);
            }
            for (mut l, evs) in lanes.into_iter().zip(per) {
                let rows = a.rows;
                tasks.push(tokio::spawn(async move {
                    let o = window_epoch(&mut l, evs, e, rows).await;
                    (l, o)
                }));
            }
        }
        _ => {
            let (ids, repeats) = dedupe_ids(a, e);
            expected_repeats = repeats;
            let mut per: Vec<Vec<Id>> = vec![Vec::new(); ranges.len()];
            for id in ids {
                per[lane_of(ranges, id.group)].push(id);
            }
            let retention = a.retention_epochs * EPOCH_MICROS;
            for (mut l, ids) in lanes.into_iter().zip(per) {
                tasks.push(tokio::spawn(async move {
                    let o = dedupe_epoch(&mut l, ids, e, retention).await;
                    (l, o)
                }));
            }
        }
    }
    let mut lanes = Vec::new();
    let mut outs = Vec::new();
    for t in tasks {
        let (l, o) = t.await.expect("lane");
        lanes.push(l);
        outs.push(o);
    }
    let wall = started.elapsed();
    let after = env.counts();
    let (fg, bg, gc) = (after.0 - before.0, after.1 - before.1, after.2 - before.2);
    let mut lookups = 0;
    let mut misses = 0;
    let mut committed = 0;
    for (l, s) in lanes.iter().zip(stats_before) {
        let n = l.stats();
        lookups += n.lookups - s.lookups;
        misses += n.misses - s.misses;
        committed += n.committed_bytes - s.committed_bytes;
    }
    let barrier_max = outs.iter().map(|o| o.barrier).max().unwrap_or_default();
    let barrier_mean = outs.iter().map(|o| o.barrier).sum::<Duration>() / outs.len().max(1) as u32;
    let work_max = outs.iter().map(|o| o.work).max().unwrap_or_default();
    let emitted: u64 = outs.iter().map(|o| o.emitted).sum();
    let inserted: u64 = outs.iter().map(|o| o.inserted).sum();
    let repeats: u64 = outs.iter().map(|o| o.repeats).sum();
    let merges = merge_totals(env);
    println!(
        "{{\"kind\":\"epoch\",\"label\":\"{label}\",\"workload\":\"{}\",\"store\":\"{}\",\"epoch\":{e},\"lanes\":{},\"wall_ms\":{:.1},\"work_max_ms\":{:.1},\"barrier_max_ms\":{:.1},\"barrier_mean_ms\":{:.1},\"lookups\":{lookups},\"misses\":{misses},\"committed_bytes\":{committed},\"emitted\":{emitted},\"inserted\":{inserted},\"repeats\":{repeats},\"expected_repeats\":{expected_repeats},\"fg\":{},\"bg\":{},\"gc\":{},\"merges\":{merges},\"footprint\":[{}]}}",
        a.workload,
        a.store,
        lanes.len(),
        ms(wall),
        ms(work_max),
        ms(barrier_max),
        ms(barrier_mean),
        counts_json(&fg),
        counts_json(&bg),
        counts_json(&gc),
        lanes
            .iter()
            .map(|l| l.store().footprint())
            .collect::<Vec<_>>()
            .join(","),
    );
    lanes
}

fn merge_totals(env: &Env) -> String {
    let m = env.merges.lock().expect("lock");
    let sum = |f: fn(&MergeStats) -> u64| m.iter().map(|s| f(s)).sum::<u64>();
    use std::sync::atomic::Ordering::Relaxed;
    format!(
        "{{\"merges\":{},\"read\":{},\"written\":{},\"dropped\":{},\"ms\":{}}}",
        sum(|s| s.merges.load(Relaxed)),
        sum(|s| s.bytes_read.load(Relaxed)),
        sum(|s| s.bytes_written.load(Relaxed)),
        sum(|s| s.entries_dropped.load(Relaxed)),
        sum(|s| s.nanos.load(Relaxed)) / 1_000_000,
    )
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Opens `n` lanes from a record's snapshots, and prints what it cost.
async fn restore(
    a: &Args,
    env: &Env,
    generation: u64,
    n: u32,
    snaps: &[Snapshot],
    label: &str,
) -> (Vec<Layer<AnyStore>>, Vec<KeyGroupRange>) {
    let ranges = KeyGroupRange::split(GROUPS, n);
    let before = env.counts();
    let started = Instant::now();
    let opened = futures::future::join_all(ranges.iter().map(|&r| async move {
        let t = Instant::now();
        let s = open(a, env, generation, r, snaps).await;
        let opened = t.elapsed();
        let timers = if a.workload == "window" {
            layer::load_timers(&s, r, &[0]).await.expect("timers")
        } else {
            Vec::new()
        };
        (r, s, timers, opened, t.elapsed())
    }))
    .await;
    let wall = started.elapsed();
    let after = env.counts();
    let fg = after.0 - before.0;
    let mut lanes = Vec::new();
    let mut open_max = Duration::ZERO;
    let mut timers_n = 0;
    for (r, s, timers, o, _) in opened {
        open_max = open_max.max(o);
        timers_n += timers.len();
        lanes.push(Layer::new(s, r, DEFAULT_CACHE_BYTES, timers));
    }
    println!(
        "{{\"kind\":\"restore\",\"label\":\"{label}\",\"workload\":\"{}\",\"store\":\"{}\",\"lanes\":{n},\"wall_ms\":{:.1},\"open_max_ms\":{:.1},\"timers\":{timers_n},\"fg\":{}}}",
        a.workload,
        a.store,
        ms(wall),
        ms(open_max),
        counts_json(&fg)
    );
    (lanes, ranges)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    let a = args();
    let env = Env::new(&a.dir, a.latency);
    let ranges = KeyGroupRange::split(GROUPS, a.lanes);
    let mut lanes = Vec::new();
    for r in &ranges {
        let s = open(&a, &env, 1, *r, &[]).await;
        lanes.push(Layer::new(s, *r, DEFAULT_CACHE_BYTES, Vec::new()));
    }
    let mut last: Vec<Snapshot> = Vec::new();
    for e in 0..a.epochs {
        let started = Instant::now();
        lanes = epoch(&a, &env, lanes, &ranges, e, "run").await;
        // The snapshots of epoch e are what the record of e names.
        last = lanes
            .iter()
            .flat_map(|l| l.last_snapshots().to_vec())
            .collect();
        if e >= a.keep {
            for l in &mut lanes {
                l.store_mut()
                    .release(e + 1 - a.keep)
                    .await
                    .expect("release");
            }
        }
        if let Some(rest) = a.interval.checked_sub(started.elapsed()) {
            tokio::time::sleep(rest).await;
        }
    }
    let committed: u64 = lanes.iter().map(|l| l.stats().committed_bytes).sum();
    let (fg, bg, gc) = env.counts();
    println!(
        "{{\"kind\":\"totals\",\"workload\":\"{}\",\"store\":\"{}\",\"committed_bytes\":{committed},\"fg\":{},\"bg\":{},\"gc\":{},\"merges\":{}}}",
        a.workload,
        a.store,
        counts_json(&fg),
        counts_json(&bg),
        counts_json(&gc),
        merge_totals(&env)
    );
    for l in &lanes {
        l.store().close().await;
    }
    drop(lanes);
    // Restore onto the same lanes, then onto others, each from the last record.
    let mut generation = 2;
    for n in std::iter::once(a.lanes).chain(a.rescale.iter().copied()) {
        let label = format!("restore-{n}");
        let (ls, rs) = restore(&a, &env, generation, n, &last, &label).await;
        let ls = epoch(&a, &env, ls, &rs, a.epochs, &format!("after-{n}")).await;
        for l in &ls {
            l.store().close().await;
        }
        generation += 1;
    }
}
