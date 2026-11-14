//! The dataflow. Each lane reads its inbound edge and normalizes what it
//! carries, raw rows to Arrow, on a task of its own: it parses and types,
//! then names, adds metadata columns and holds. With a SQL transform, the
//! streams it reads are typed, and each of their batches goes through every
//! statement that reads it, and to the output it is if it passes through,
//! before each output's batch is named, given metadata columns and held. A
//! lane hands what it shaped to a writer lane: its own, or for a stream
//! routed by key group, the one owning each row's group. Each writer lane aligns the lanes
//! feeding it, and writes to a runner sink, with barriers in band, or to a
//! Parquet or staged sink, which stages each epoch and reports it in
//! EpochReady.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use openetl_formats::ParquetStaging;
use openetl_normalize::{
    Error as NormalizeError, Policy, Rejected, Shaped, Shaper, Typed, by_key_group, logical_type,
    naming::Naming, widens,
};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, CellHolds, ColumnHold, EpochReady, EventsResponse, FrameKind, HoldCause, LaneMetrics,
    Metrics, QuarantineFile, ReplayProgress, Rows, SchemaChange, StagedStream, StreamHolds,
    StreamMetrics, events_response, hold_cause,
};
use prost::Message;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinSet;
use tonic::Status;
use tracing::{Instrument as _, Span, debug_span, info_span};

use crate::error::JobError;
use crate::frame;
use crate::ipc;
use crate::plan::{InputPlan, Plan, Sink};
use crate::replay::Replayer;
use crate::stage::{Applied, Stage, pushed_rows};
use crate::staging::Staging;
use crate::transform::Route;

pub type Events = mpsc::Sender<Result<EventsResponse, Status>>;

/// Sends a failure on Events. Gives up after a while if nobody reads them.
pub async fn report(events: &Events, err: &JobError) {
    let _ = tokio::time::timeout(Duration::from_secs(5), events.send(Err(err.to_status()))).await;
}

#[derive(Default)]
struct Counters {
    rows_in: AtomicU64,
    bytes_in: AtomicU64,
    rows_out: AtomicU64,
    bytes_out: AtomicU64,
    rows_quarantined: AtomicU64,
    cells_held: AtomicU64,
}

/// What a lane is doing.
#[derive(Clone, Copy)]
enum Doing {
    Busy = 0,
    /// Waiting for a writer lane to take what it made.
    Backpressured = 1,
    /// Waiting for its edge.
    Idle = 2,
}

/// Where a lane's time goes, in nanoseconds. What it's doing
/// and since when, from the job's start, share one word: the top two bits
/// and the rest.
struct LaneClock {
    spent: [AtomicU64; 3],
    rows_in: AtomicU64,
    now: AtomicU64,
}

const SINCE: u64 = (1 << 62) - 1;

impl LaneClock {
    fn new() -> Self {
        LaneClock {
            spent: Default::default(),
            rows_in: AtomicU64::new(0),
            now: AtomicU64::new((Doing::Idle as u64) << 62),
        }
    }

    fn enter(&self, start: Instant, doing: Doing) {
        let at = nanos(start);
        let was = self.now.swap(((doing as u64) << 62) | at, Ordering::AcqRel);
        self.spent[(was >> 62) as usize % 3]
            .fetch_add(at.saturating_sub(was & SINCE), Ordering::Relaxed);
    }

    /// What it has spent, with what it's doing now counted so far.
    fn metrics(&self, start: Instant) -> LaneMetrics {
        let at = nanos(start);
        let was = self.now.load(Ordering::Acquire);
        let mut spent = [0; 3];
        for (i, s) in self.spent.iter().enumerate() {
            spent[i] = s.load(Ordering::Relaxed);
        }
        spent[(was >> 62) as usize % 3] += at.saturating_sub(was & SINCE);
        LaneMetrics {
            busy_nanos: spent[Doing::Busy as usize],
            backpressured_nanos: spent[Doing::Backpressured as usize],
            idle_nanos: spent[Doing::Idle as usize],
            rows_in: self.rows_in.load(Ordering::Relaxed),
        }
    }
}

fn nanos(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_nanos()).unwrap_or(SINCE) & SINCE
}

/// What a lane hands to a writer lane. Streams are indexes into
/// Plan::streams.
enum Out {
    Batch(usize, RecordBatch),
    /// An encoded SchemaChange message.
    SchemaChange(usize, Vec<u8>),
    /// Held values of the stream's current epoch.
    Held(usize, RecordBatch),
    Patch(usize, RecordBatch),
    Barrier(usize, Epoch),
    End(usize),
}

impl Out {
    fn stream(&self) -> usize {
        match self {
            Out::Batch(s, _)
            | Out::SchemaChange(s, _)
            | Out::Held(s, _)
            | Out::Patch(s, _)
            | Out::Barrier(s, _)
            | Out::End(s) => *s,
        }
    }
}

/// A stream's state at a barrier or its end.
#[derive(Clone, Default)]
struct At {
    schema: Option<SchemaRef>,
    /// Where columns are held whole.
    delivered: Option<SchemaRef>,
    /// Streams that hold: without files, which the sink's writer seals.
    holds: Option<StreamHolds>,
}

/// A stream's one schema, whatever its lanes. Each lane shapes
/// with a copy of the owner's shaper. A lane that changes its copy publishes
/// it if nobody has since it copied; otherwise it copies the latest and shapes
/// its batch again. Where the plan's schemas are remote, the owner follows
/// the remote owner's newest version, and a lane's change to the schema is
/// proposed there.
struct Owner {
    version: AtomicU64,
    /// The remote owner's version the shaper is; 0 where there's none.
    remote_version: AtomicU64,
    state: Mutex<OwnerState>,
}

struct OwnerState {
    shaper: Shaper,
    version: u64,
    remote_version: u64,
    /// By epoch, until it's ready.
    crossings: BTreeMap<u64, Crossing>,
    /// Lanes that ended the stream.
    ended: usize,
    at_end: At,
}

/// What a stream's lanes did in an epoch, as they pass its barrier.
#[derive(Default)]
struct Crossing {
    lanes: usize,
    cells: BTreeMap<String, CellHolds>,
    quarantine: Vec<QuarantineFile>,
    replayed: Option<ReplayProgress>,
    /// Once every lane has passed it.
    at: Option<At>,
}

/// Stream s's shaper, restored from restored.
fn shaper(plan: &Plan, s: usize, restored: Option<SchemaRef>) -> Result<Shaper, JobError> {
    let bad = |e: NormalizeError| JobError::config(format!("stream {}: {e}", plan.streams[s]));
    Ok(Shaper::new(
        plan.naming,
        plan.tables[s].clone(),
        plan.identities[s].clone(),
        plan.policy,
        restored,
        plan.declared[s].clone(),
        plan.types[s],
    )
    .map_err(bad)?
    .with_soft_deletes(plan.soft_deletes[s])
    .with_changelog(plan.changelog[s]))
}

impl Owner {
    fn new(plan: &Plan, s: usize) -> Result<Self, JobError> {
        let bad = |e: NormalizeError| JobError::config(format!("stream {}: {e}", plan.streams[s]));
        let mut shaper = shaper(plan, s, plan.restored[s].clone())?;
        if let Some(h) = &plan.holds[s] {
            shaper = shaper.with_holds(h.clone()).map_err(bad)?;
        }
        if let (Some(r), Some(_)) = (&plan.replays[s], &plan.held_url) {
            // Patches go out at the types the destination will have.
            shaper
                .version(&r.versioned)
                .and_then(|_| shaper.declare())
                .map_err(|e| shape_error(&plan.streams[s], e))?;
        }
        shaper.take_changed();
        Ok(Self {
            version: AtomicU64::new(0),
            remote_version: AtomicU64::new(0),
            state: Mutex::new(OwnerState {
                shaper,
                version: 0,
                remote_version: 0,
                crossings: BTreeMap::new(),
                ended: 0,
                at_end: At::default(),
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, OwnerState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Takes the remote owner's newest schema of stream s, if the plan's
    /// schemas are remote and it's newer than the shaper's: lanes then shape
    /// by it. The schema policy applies as it did.
    fn follow(&self, plan: &Plan, s: usize) -> Result<(), JobError> {
        let Some(r) = plan.remote.as_ref().and_then(|r| r.stream(s)) else {
            return Ok(());
        };
        r.check()?;
        if r.newest() <= self.remote_version.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut o = self.lock();
        let (v, schema) = r.latest();
        if v <= o.remote_version {
            return Ok(());
        }
        let mut sh = shaper(plan, s, schema)?;
        if !o.shaper.sealed() {
            sh.unseal();
        }
        sh.take_changed();
        o.shaper = sh;
        o.remote_version = v;
        self.remote_version.store(v, Ordering::Release);
        o.version += 1;
        self.version.store(o.version, Ordering::Release);
        Ok(())
    }

    /// A lane passed the stream's barrier n. The last to pass it fixes the
    /// stream's state there: from then on, the schema policy applies.
    fn cross(
        &self,
        plan: &Plan,
        stream: usize,
        n: u64,
        quarantine: Option<QuarantineFile>,
        cells: BTreeMap<String, CellHolds>,
        replayed: Option<ReplayProgress>,
    ) -> Result<(), JobError> {
        let mut guard = self.lock();
        let o = &mut *guard;
        let c = o.crossings.entry(n).or_default();
        c.lanes += 1;
        c.quarantine.extend(quarantine);
        for (column, held) in cells {
            let e = c.cells.entry(column).or_insert_with(|| CellHolds {
                cells: 0,
                reason: held.reason,
            });
            e.cells += held.cells;
        }
        c.replayed = c.replayed.take().or(replayed);
        if c.lanes < plan.inbound.len() {
            return Ok(());
        }
        if o.shaper.seal() {
            o.version += 1;
            self.version.store(o.version, Ordering::Release);
        }
        let mut state = at(plan, stream, &o.shaper)?;
        if let Some(h) = &mut state.holds {
            h.cells = std::mem::take(&mut c.cells).into_iter().collect();
            h.replayed = c.replayed.take();
        }
        c.at = Some(state);
        Ok(())
    }

    /// A lane ended the stream.
    fn end(&self, plan: &Plan, stream: usize) -> Result<(), JobError> {
        let mut o = self.lock();
        o.ended += 1;
        if o.ended == plan.inbound.len() {
            o.at_end = at(plan, stream, &o.shaper)?;
        }
        Ok(())
    }

    /// The stream's state in ready epoch n, and what it quarantined in it.
    fn take(&self, n: u64) -> (At, Vec<QuarantineFile>) {
        let mut o = self.lock();
        match o.crossings.remove(&n) {
            Some(Crossing {
                at: Some(at),
                quarantine,
                ..
            }) => (at, quarantine),
            _ => (o.at_end.clone(), Vec::new()),
        }
    }
}

/// A stream a transform reads, whatever its lanes: what its rows are typed
/// by, and its schema as its statements read it, which each lane widens.
struct InputOwner {
    shaper: Shaper,
    state: Mutex<InputState>,
}

#[derive(Default)]
struct InputState {
    schema: Option<SchemaRef>,
    /// By epoch, until it's ready.
    crossings: BTreeMap<u64, InputCrossing>,
    ended: usize,
    at_end: Option<SchemaRef>,
}

/// What a stream's lanes did in an epoch, as they pass its barrier.
#[derive(Default)]
struct InputCrossing {
    lanes: usize,
    quarantine: Vec<QuarantineFile>,
    /// The stream's schema, once every lane has passed it.
    at: Option<Option<SchemaRef>>,
}

impl InputOwner {
    /// `cells` are the cell holds of the output the stream passes through
    /// as, if it holds cells: its rows are typed holding them.
    fn new(input: &InputPlan, cells: Option<HashSet<String>>) -> Result<Self, JobError> {
        let shaper = Shaper::new(
            Naming {
                keep_source: true,
                max_len: 0,
            },
            String::new(),
            input.identity.clone(),
            Policy::AllowAll,
            input.restored.clone(),
            input.declared.clone(),
            input.types,
        )
        .map_err(|e| JobError::config(format!("stream {}: {e}", input.name)))?
        .with_changelog(input.changelog)
        .with_cell_holds(cells);
        Ok(Self {
            shaper,
            state: Mutex::new(InputState {
                schema: input.restored.clone(),
                ..InputState::default()
            }),
        })
    }

    fn lock(&self) -> MutexGuard<'_, InputState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A lane typed a batch at `schema`: columns it adds join the stream's,
    /// and a type that holds the stream's replaces it.
    fn observe(&self, schema: &SchemaRef) {
        let mut st = self.lock();
        let Some(old) = &st.schema else {
            st.schema = Some(schema.clone());
            return;
        };
        let mut fields: Vec<arrow_schema::Field> = Vec::with_capacity(old.fields().len());
        let mut changed = false;
        for f in old.fields() {
            match schema.field_with_name(f.name()) {
                Ok(g) if g.data_type() != f.data_type() && !widens(g, f) => {
                    fields.push(g.clone());
                    changed = true;
                }
                _ => fields.push(f.as_ref().clone()),
            }
        }
        for g in schema.fields() {
            if old.field_with_name(g.name()).is_err() {
                fields.push(g.as_ref().clone());
                changed = true;
            }
        }
        if changed {
            st.schema = Some(Arc::new(arrow_schema::Schema::new(fields)));
        }
    }

    fn cross(&self, lanes: usize, n: u64, q: Option<QuarantineFile>) {
        let mut st = self.lock();
        let schema = st.schema.clone();
        let c = st.crossings.entry(n).or_default();
        c.lanes += 1;
        c.quarantine.extend(q);
        if c.lanes == lanes {
            c.at = Some(schema);
        }
    }

    fn end(&self, lanes: usize) {
        let mut st = self.lock();
        st.ended += 1;
        if st.ended == lanes {
            st.at_end = st.schema.clone();
        }
    }

    /// The stream's schema in ready epoch n, and what it quarantined in it.
    fn take(&self, n: u64) -> (Option<SchemaRef>, Vec<QuarantineFile>) {
        let mut st = self.lock();
        match st.crossings.remove(&n) {
            Some(InputCrossing {
                quarantine,
                at: Some(schema),
                ..
            }) => (schema, quarantine),
            // It ended, or feeds no output and keeps its restored schema.
            _ => (st.at_end.clone().or(st.schema.clone()), Vec::new()),
        }
    }
}

/// What every lane and writer lane of a job share.
struct Flow {
    plan: Arc<Plan>,
    owners: Vec<Owner>,
    counters: Vec<Counters>,
    /// With a transform, the streams it reads, which `owners` and
    /// `counters` don't count.
    inputs: Vec<InputOwner>,
    input_counters: Vec<Counters>,
    lanes: Vec<LaneClock>,
    start: Instant,
    /// Held across sending epochs, so they go out in order.
    align: tokio::sync::Mutex<Alignment>,
    events: Events,
}

impl Flow {
    /// Each stream's counts. A transform's input and an output of the same
    /// name are one stream: rows in are the input's, and rows out the
    /// output's.
    fn metrics(&self) -> EventsResponse {
        let load = |name: &str, c: &Counters| StreamMetrics {
            stream: name.to_owned(),
            rows_in: c.rows_in.load(Ordering::Relaxed),
            bytes_in: c.bytes_in.load(Ordering::Relaxed),
            rows_out: c.rows_out.load(Ordering::Relaxed),
            bytes_out: c.bytes_out.load(Ordering::Relaxed),
            rows_quarantined: c.rows_quarantined.load(Ordering::Relaxed),
            cells_held: c.cells_held.load(Ordering::Relaxed),
        };
        let mut streams: Vec<StreamMetrics> = self
            .plan
            .streams
            .iter()
            .zip(&self.counters)
            .map(|(name, c)| load(name, c))
            .collect();
        if let Some(t) = &self.plan.transform {
            for (input, c) in t.inputs.iter().zip(&self.input_counters) {
                let m = load(&input.name, c);
                match streams.iter_mut().find(|s| s.stream == m.stream) {
                    Some(s) => {
                        s.rows_in += m.rows_in;
                        s.bytes_in += m.bytes_in;
                        s.rows_quarantined += m.rows_quarantined;
                    }
                    None => streams.push(m),
                }
            }
        }
        EventsResponse {
            event: Some(events_response::Event::Metrics(Metrics {
                streams,
                memory_bytes: crate::memory::held(),
                lanes: self.lanes.iter().map(|l| l.metrics(self.start)).collect(),
            })),
        }
    }
}

type Task = Pin<Box<dyn Future<Output = Result<bool, JobError>> + Send>>;

/// Runs the job until every stream has ended at the sink and every inbound
/// edge has closed. `inbound` has an edge per lane, in order, and `outbound`
/// the runner sink's, per writer lane. Failures are reported on `events`
/// before the edges close.
pub async fn run<R, W>(
    plan: Arc<Plan>,
    inbound: Vec<R>,
    outbound: Vec<W>,
    events: Events,
) -> Result<(), JobError>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let owners: Result<Vec<_>, _> = (0..plan.streams.len())
        .map(|s| Owner::new(&plan, s))
        .collect();
    let owners = match owners {
        Ok(o) => o,
        Err(e) => {
            report(&events, &e).await;
            return Err(e);
        }
    };
    let inputs: Result<Vec<_>, _> = plan
        .transform
        .iter()
        .flat_map(|t| t.inputs.iter().zip(&t.routes))
        .map(|(input, routes)| {
            let cells = routes
                .iter()
                .find(|(_, r)| *r == Route::Pass)
                .and_then(|&(o, _)| owners[o].lock().shaper.cell_holds());
            InputOwner::new(input, cells)
        })
        .collect();
    let inputs = match inputs {
        Ok(i) => i,
        Err(e) => {
            report(&events, &e).await;
            return Err(e);
        }
    };
    let flow = Arc::new(Flow {
        counters: plan.streams.iter().map(|_| Counters::default()).collect(),
        input_counters: inputs.iter().map(|_| Counters::default()).collect(),
        inputs,
        lanes: inbound.iter().map(|_| LaneClock::new()).collect(),
        start: Instant::now(),
        owners,
        align: tokio::sync::Mutex::new(Alignment::new(
            plan.streams.len(),
            plan.writer_lanes,
            plan.first_epoch,
        )),
        events: events.clone(),
        plan: plan.clone(),
    });
    let mut senders = Vec::with_capacity(plan.writer_lanes);
    let mut feeds = Vec::with_capacity(plan.writer_lanes);
    for w in 0..plan.writer_lanes {
        let (tx, rx) = mpsc::channel(16);
        senders.push(tx);
        feeds.push(Feed::new(&plan, w, rx));
    }
    let mut writers: JoinSet<Result<bool, JobError>> = JoinSet::new();
    let mut outbound = outbound.into_iter();
    for (writer, feed) in feeds.into_iter().enumerate() {
        let flow = flow.clone();
        let task: Task = match (&plan.sink, outbound.next()) {
            (Sink::Runner { .. }, Some(w)) => Box::pin(write_edge(flow, writer, w, feed)),
            (Sink::Parquet { url, pipeline }, None) => {
                let (url, pipeline) = (url.clone(), pipeline.clone());
                let span = Span::current();
                let rt = tokio::runtime::Handle::current();
                Box::pin(async move {
                    tokio::task::spawn_blocking(move || {
                        span.in_scope(|| write_parquet(&flow, writer, &url, &pipeline, feed, &rt))
                    })
                    .await
                    .map_err(|e| JobError::internal(format!("the Parquet sink failed: {e}")))?
                })
            }
            (Sink::Staged { url, dir }, None) => {
                Box::pin(write_staged(flow, writer, url.clone(), dir.clone(), feed))
            }
            (Sink::Discard, None) => Box::pin(write_discard(flow, writer, feed)),
            _ => {
                let e = JobError::internal("the outbound edge doesn't match the sink");
                report(&events, &e).await;
                return Err(e);
            }
        };
        writers.spawn(task.in_current_span());
    }
    let mut readers = JoinSet::new();
    for (lane, r) in inbound.into_iter().enumerate() {
        let lane = Lane {
            flow: flow.clone(),
            lane,
            tx: senders.clone(),
        };
        readers.spawn(read_edge(lane, r).in_current_span());
    }
    drop(senders);

    let mut complete = true;
    let res = loop {
        tokio::select! {
            Some(joined) = writers.join_next() => match joined {
                Ok(Ok(c)) => complete &= c,
                Ok(Err(e)) => break Err(e),
                Err(e) => break Err(JobError::internal(format!("a writer lane failed: {e}"))),
            },
            Some(joined) = readers.join_next() => match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) if e.stopped => {}
                Ok(Err(e)) => break Err(e),
                Err(e) => break Err(JobError::internal(format!("an edge reader failed: {e}"))),
            },
            else => break match complete {
                true => Ok(()),
                false => Err(JobError::internal("the inbound edges stopped early")),
            },
        }
    };
    if let Err(e) = &res {
        report(&events, e).await;
    }
    res
}

/// One lane's copy of a stream the destination gets.
struct LaneStream {
    stream: usize,
    shaper: Shaper,
    version: u64,
    /// Quarantined lines since its last barrier, and how many.
    quarantined: (Vec<u8>, u64),
    /// Held cells since its last barrier, by column.
    cells: BTreeMap<String, CellHolds>,
    /// Lane 0 replays released holds.
    replay: Option<Replayer>,
}

impl LaneStream {
    /// Takes the owner's latest schema, if another lane changed it.
    fn catch_up(&mut self, owner: &Owner) {
        if owner.version.load(Ordering::Acquire) != self.version {
            let o = owner.lock();
            self.shaper = o.shaper.fork();
            self.version = o.version;
        }
    }

    /// Types with `f` and shapes, with the owner's latest schema, and
    /// publishes what that changes.
    fn shape(
        &mut self,
        owner: &Owner,
        f: impl Fn(&mut Shaper) -> Result<Typed, NormalizeError>,
    ) -> Result<Shaped, NormalizeError> {
        loop {
            self.catch_up(owner);
            let out = f(&mut self.shaper).and_then(|t| self.shaper.shape(t));
            let changed = self.shaper.take_changed();
            if out.is_err() {
                if owner.version.load(Ordering::Acquire) == self.version {
                    return out;
                }
                continue;
            }
            if !changed {
                return out;
            }
            let mut o = owner.lock();
            if o.version != self.version {
                continue;
            }
            o.version += 1;
            self.version = o.version;
            o.shaper = self.shaper.fork();
            owner.version.store(o.version, Ordering::Release);
            return out;
        }
    }

    /// As shape, where the plan's schemas may be remote: a change to the
    /// schema is proposed to the remote owner from the version the owner
    /// follows, and once a newer version comes, the batch is shaped again by
    /// it. So is a change to what the schema's metadata knows of the
    /// values, such as a column that held integers past ±2^53, which a later
    /// float in it, here or in another engine, turns to text. A change that
    /// leaves the schema as it is is published here.
    async fn shape_own(
        &mut self,
        plan: &Plan,
        owner: &Owner,
        f: impl Fn(&mut Shaper) -> Result<Typed, NormalizeError>,
    ) -> Result<Shaped, JobError> {
        let name = &plan.streams[self.stream];
        let Some(remote) = plan.remote.as_ref().and_then(|r| r.stream(self.stream)) else {
            return self.shape(owner, f).map_err(|e| shape_error(name, e));
        };
        loop {
            owner.follow(plan, self.stream)?;
            self.catch_up(owner);
            let before = self.shaper.schema();
            let out = f(&mut self.shaper).and_then(|t| self.shaper.shape(t));
            let changed = self.shaper.take_changed();
            if out.is_err() {
                if owner.version.load(Ordering::Acquire) == self.version {
                    return out.map_err(|e| shape_error(name, e));
                }
                continue;
            }
            let schema = self.shaper.schema();
            // The shaper gives a new schema only when something in it changed.
            let renewed = match (&before, &schema) {
                (Some(a), Some(b)) => !Arc::ptr_eq(a, b),
                (a, b) => a.is_some() != b.is_some(),
            };
            if !changed && !renewed {
                return out.map_err(|e| shape_error(name, e));
            }
            let base = {
                let mut o = owner.lock();
                if o.version != self.version {
                    continue;
                }
                if o.shaper.schema() == schema {
                    if changed {
                        o.version += 1;
                        self.version = o.version;
                        o.shaper = self.shaper.fork();
                        owner.version.store(o.version, Ordering::Release);
                    }
                    return out.map_err(|e| shape_error(name, e));
                }
                o.remote_version
            };
            drop(out);
            if let Some(schema) = schema {
                remote.propose(base, &schema).await?;
            }
        }
    }
}

/// One lane's copy of a stream it reads.
struct LaneInput {
    next_epoch: u64,
    ended: bool,
    kind: Kind,
}

enum Kind {
    /// Without a transform, the stream is the destination's, and one shaper
    /// types and shapes it.
    Own(Box<LaneStream>),
    Transformed(Box<Transformed>),
}

/// A lane's copy of a stream a transform reads.
struct Transformed {
    input: usize,
    typer: Shaper,
    /// Quarantined lines since its last barrier, and how many.
    quarantined: (Vec<u8>, u64),
    /// The schema the lane last told the stream's owner of.
    seen: Option<SchemaRef>,
    /// The outputs it feeds.
    outputs: Vec<(Stage, LaneStream)>,
}

/// A lane, with a sender to each writer lane.
struct Lane {
    flow: Arc<Flow>,
    lane: usize,
    tx: Vec<mpsc::Sender<(usize, Out)>>,
}

impl Lane {
    fn enter(&self, doing: Doing) {
        self.flow.lanes[self.lane].enter(self.flow.start, doing);
    }

    async fn send_to(&self, w: usize, out: Out) -> Result<(), JobError> {
        let stopped = |_| JobError::stopped("the outbound edge stopped");
        let out = match self.tx[w].try_send((self.lane, out)) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(out)) => out,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped(())),
        };
        self.enter(Doing::Backpressured);
        let sent = self.tx[w].send(out).await.map_err(|_| stopped(()));
        self.enter(Doing::Busy);
        sent
    }

    /// Sends to the lane's own writer lane.
    async fn send(&self, out: Out) -> Result<(), JobError> {
        self.send_to(self.lane % self.tx.len(), out).await
    }

    fn routed(&self, stream: usize) -> bool {
        self.flow.plan.routed[stream] && self.tx.len() > 1
    }

    /// Sends a batch's rows to their writer lanes.
    async fn rows(
        &self,
        stream: usize,
        key: &[String],
        batch: RecordBatch,
    ) -> Result<(), JobError> {
        if !self.routed(stream) {
            return self.send(Out::Batch(stream, batch)).await;
        }
        let parts = by_key_group(&batch, key, self.flow.plan.key_groups, self.tx.len())
            .map_err(|e| shape_error(&self.flow.plan.streams[stream], NormalizeError::Arrow(e)))?;
        for (w, part) in parts.into_iter().enumerate() {
            if let Some(part) = part {
                self.send_to(w, Out::Batch(stream, part)).await?;
            }
        }
        Ok(())
    }

    /// Sends a schema change, a barrier or an end to every writer lane the
    /// stream's rows on this lane can reach.
    async fn all(&self, stream: usize, out: impl Fn() -> Out) -> Result<(), JobError> {
        if !self.routed(stream) {
            return self.send(out()).await;
        }
        for w in 0..self.tx.len() {
            self.send_to(w, out()).await?;
        }
        Ok(())
    }
}

async fn read_edge<R>(lane: Lane, mut r: R) -> Result<(), JobError>
where
    R: AsyncBufRead + Unpin,
{
    let flow = lane.flow.clone();
    let plan = &flow.plan;
    let id = &plan.inbound[lane.lane].id;
    // Lane 0 replays a stream's released holds.
    let lane_stream = |s: usize| -> Result<LaneStream, JobError> {
        let replay = match (lane.lane, &plan.replays[s], &plan.held_url) {
            (0, Some(r), Some(url)) => Some(Replayer::open(url, r.clone())?),
            _ => None,
        };
        let o = flow.owners[s].lock();
        Ok(LaneStream {
            stream: s,
            shaper: o.shaper.fork(),
            version: o.version,
            quarantined: (Vec::new(), 0),
            cells: BTreeMap::new(),
            replay,
        })
    };
    let mut streams = Vec::with_capacity(plan.inbound[lane.lane].streams.len());
    for &s in &plan.inbound[lane.lane].streams {
        let kind = match &plan.transform {
            None => Kind::Own(Box::new(lane_stream(s)?)),
            Some(t) => Kind::Transformed(Box::new(Transformed {
                input: s,
                typer: flow.inputs[s].shaper.fork(),
                quarantined: (Vec::new(), 0),
                seen: None,
                outputs: t.routes[s]
                    .iter()
                    .map(|&(o, route)| {
                        let stage = match route {
                            Route::Pass => Stage::Identity,
                            Route::Statement(i) => Stage::statement(&t.sql, i),
                        };
                        Ok((stage, lane_stream(o)?))
                    })
                    .collect::<Result<_, JobError>>()?,
            })),
        };
        streams.push(LaneInput {
            next_epoch: plan.first_epoch,
            ended: false,
            kind,
        });
    }
    let protocol = |msg: String| JobError::internal(format!("edge {id}: {msg}"));

    loop {
        lane.enter(Doing::Idle);
        let read = frame::read(&mut r).await;
        lane.enter(Doing::Busy);
        let f = match read {
            Ok(Some(f)) => f,
            Ok(None) if streams.iter().all(|s| s.ended) => return Ok(()),
            Ok(None) => {
                return Err(JobError::runner_gone(format!(
                    "edge {id} closed before its streams ended"
                )));
            }
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                return Err(protocol(e.to_string()));
            }
            Err(e) => return Err(JobError::runner_gone(format!("edge {id}: {e}"))),
        };
        let local = f.stream as usize;
        let Some(li) = streams.get_mut(local) else {
            return Err(protocol(format!(
                "frame for stream {local}, which the edge doesn't carry"
            )));
        };
        let input = plan.inbound[lane.lane].streams[local];
        let name = match &plan.transform {
            Some(t) => &t.inputs[input].name,
            None => &plan.streams[input],
        };
        if li.ended {
            return Err(protocol(format!("frame after the end of stream {name}")));
        }
        let c = match plan.transform {
            Some(_) => &flow.input_counters[input],
            None => &flow.counters[input],
        };
        c.bytes_in
            .fetch_add(f.payload.len() as u64, Ordering::Relaxed);
        match f.kind {
            FrameKind::Rows if plan.passthrough => {
                return Err(protocol("rows in a passthrough job".into()));
            }
            FrameKind::Rows => {
                let rows =
                    Rows::decode(f.payload.as_slice()).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(rows.rows.len() as u64, Ordering::Relaxed);
                flow.lanes[lane.lane]
                    .rows_in
                    .fetch_add(rows.rows.len() as u64, Ordering::Relaxed);
                let record = |row: usize| String::from_utf8_lossy(&rows.rows[row]).into_owned();
                let span = debug_span!("batch", openetl.stream = %name, rows = rows.rows.len());
                match &mut li.kind {
                    Kind::Own(ls) => {
                        let owner = &flow.owners[input];
                        let n = ls
                            .shape_own(plan, owner, |sh| {
                                sh.type_rows(rows.rows.iter().map(Vec::as_slice))
                            })
                            .instrument(span)
                            .await?;
                        hold(plan, name, &n.rejected, record, &mut ls.quarantined, c)?;
                        shaped(&lane, ls, n, c).await?;
                    }
                    // A stream that feeds no output is read, and dropped.
                    Kind::Transformed(t) if t.outputs.is_empty() => {}
                    Kind::Transformed(t) => {
                        let typed = span
                            .in_scope(|| t.typer.type_rows(rows.rows.iter().map(Vec::as_slice)))
                            .map_err(|e| shape_error(name, e))?;
                        transform(&lane, t, typed, &record, c).await?;
                    }
                }
            }
            FrameKind::Batch => {
                let batch = ipc::decode_batch(f.payload).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                flow.lanes[lane.lane]
                    .rows_in
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                if plan.passthrough {
                    lane.send(Out::Batch(input, batch)).await?;
                    continue;
                }
                let record = |row: usize| openetl_normalize::render_row(&batch, row);
                let span = debug_span!("batch", openetl.stream = %name, rows = batch.num_rows());
                match &mut li.kind {
                    Kind::Own(ls) => {
                        let owner = &flow.owners[input];
                        let n = ls
                            .shape_own(plan, owner, |sh| sh.type_batch(&batch))
                            .instrument(span)
                            .await?;
                        hold(plan, name, &n.rejected, record, &mut ls.quarantined, c)?;
                        shaped(&lane, ls, n, c).await?;
                    }
                    Kind::Transformed(t) if t.outputs.is_empty() => {}
                    Kind::Transformed(t) => {
                        let typed = span
                            .in_scope(|| t.typer.type_batch(&batch))
                            .map_err(|e| shape_error(name, e))?;
                        transform(&lane, t, typed, &record, c).await?;
                    }
                }
            }
            FrameKind::Barrier => {
                let epoch = Barrier::decode(f.payload.as_slice())
                    .map_err(|e| protocol(e.to_string()))?
                    .epoch
                    .unwrap_or_default();
                if epoch.generation != plan.generation || epoch.n != li.next_epoch {
                    return Err(protocol(format!(
                        "stream {name} crossed barrier ({}, {}) while in ({}, {})",
                        epoch.generation, epoch.n, plan.generation, li.next_epoch
                    )));
                }
                li.next_epoch += 1;
                match &mut li.kind {
                    Kind::Own(ls) => {
                        let q = quarantine_file(
                            plan.quarantine.as_deref(),
                            epoch,
                            name,
                            &lane,
                            &mut ls.quarantined,
                            false,
                        )
                        .await?;
                        let replayed = ls.replay.as_ref().map(Replayer::progress);
                        let owner = &flow.owners[input];
                        owner.cross(
                            plan,
                            input,
                            epoch.n,
                            q,
                            std::mem::take(&mut ls.cells),
                            replayed,
                        )?;
                        lane.all(input, || Out::Barrier(input, epoch)).await?;
                        if ls.replay.is_some() {
                            ls.catch_up(owner);
                            replay(&lane, ls).await?;
                        }
                    }
                    // It has no part in an epoch: what's ready waits only
                    // on the outputs.
                    Kind::Transformed(t) if t.outputs.is_empty() => {}
                    Kind::Transformed(t) => {
                        let q = quarantine_file(
                            plan.quarantine.as_deref(),
                            epoch,
                            name,
                            &lane,
                            &mut t.quarantined,
                            false,
                        )
                        .await?;
                        flow.inputs[input].cross(plan.inbound.len(), epoch.n, q);
                        let outputs_q = plan
                            .transform
                            .as_ref()
                            .and_then(|t| t.quarantine.as_deref());
                        for (_, ls) in &mut t.outputs {
                            let s = ls.stream;
                            let q = quarantine_file(
                                outputs_q,
                                epoch,
                                &plan.streams[s],
                                &lane,
                                &mut ls.quarantined,
                                true,
                            )
                            .await?;
                            let replayed = ls.replay.as_ref().map(Replayer::progress);
                            let owner = &flow.owners[s];
                            owner.cross(
                                plan,
                                s,
                                epoch.n,
                                q,
                                std::mem::take(&mut ls.cells),
                                replayed,
                            )?;
                            lane.all(s, || Out::Barrier(s, epoch)).await?;
                            if ls.replay.is_some() {
                                ls.catch_up(owner);
                                replay(&lane, ls).await?;
                            }
                        }
                    }
                }
            }
            FrameKind::SchemaChange => {
                let payload = f.payload;
                match &li.kind {
                    Kind::Own(_) => {
                        lane.all(input, || Out::SchemaChange(input, payload.clone()))
                            .await?;
                    }
                    // It's the stream's, so it's an output's only where the
                    // stream passes through.
                    Kind::Transformed(t) => {
                        let passed: Vec<usize> = t
                            .outputs
                            .iter()
                            .filter(|(stage, _)| matches!(stage, Stage::Identity))
                            .map(|(_, ls)| ls.stream)
                            .collect();
                        for s in passed {
                            lane.all(s, || Out::SchemaChange(s, payload.clone()))
                                .await?;
                        }
                    }
                }
            }
            FrameKind::EndOfStream => {
                li.ended = true;
                match &li.kind {
                    Kind::Own(_) => {
                        flow.owners[input].end(plan, input)?;
                        lane.all(input, || Out::End(input)).await?;
                    }
                    Kind::Transformed(t) if t.outputs.is_empty() => {}
                    Kind::Transformed(t) => {
                        flow.inputs[input].end(plan.inbound.len());
                        let outputs: Vec<usize> =
                            t.outputs.iter().map(|(_, ls)| ls.stream).collect();
                        for s in outputs {
                            flow.owners[s].end(plan, s)?;
                            lane.all(s, || Out::End(s)).await?;
                        }
                    }
                }
            }
            k @ (FrameKind::Hello | FrameKind::Unspecified | FrameKind::Patch) => {
                return Err(protocol(format!("unexpected {} frame", k.as_str_name())));
            }
        }
    }
}

/// Writes a stream's quarantined lines for the epoch whose barrier it just
/// passed, if it has any, and describes the file.
async fn quarantine_file(
    q: Option<&crate::quarantine::Quarantine>,
    epoch: Epoch,
    stream: &str,
    lane: &Lane,
    held: &mut (Vec<u8>, u64),
    output: bool,
) -> Result<Option<QuarantineFile>, JobError> {
    let (lines, rows) = std::mem::take(held);
    let Some(q) = q.filter(|_| rows > 0) else {
        return Ok(None);
    };
    let url = q
        .put(
            epoch.generation,
            epoch.n,
            stream,
            lane.flow.plan.first_lane + lane.lane,
            lines,
        )
        .await?;
    Ok(Some(QuarantineFile {
        stream: stream.to_owned(),
        url,
        rows,
        output,
    }))
}

/// Runs a batch of a stream a transform reads through each of the outputs
/// it feeds. What the stream, a statement or an output's second half can't
/// take is quarantined, the stream's apart from each output's.
async fn transform(
    lane: &Lane,
    t: &mut Transformed,
    typed: Typed,
    record: &(dyn Fn(usize) -> String + Sync),
    c: &Counters,
) -> Result<(), JobError> {
    let flow = &lane.flow;
    let plan = &flow.plan;
    let Some(job) = &plan.transform else {
        return Err(JobError::internal(
            "a transformed stream without a transform",
        ));
    };
    let input = &job.inputs[t.input];
    hold(
        plan,
        &input.name,
        &typed.rejected,
        record,
        &mut t.quarantined,
        c,
    )?;
    let schema = typed.batch.schema();
    if t.seen
        .as_ref()
        .is_none_or(|s| !Arc::ptr_eq(s, &schema) && **s != *schema)
    {
        flow.inputs[t.input].observe(&schema);
        t.seen = Some(schema);
    }
    let pushed = pushed_rows(&typed);
    let cursor = input.identity.cursor.as_deref();
    for (stage, ls) in &mut t.outputs {
        let s = ls.stream;
        let name = &plan.streams[s];
        let oc = &flow.counters[s];
        let Applied { typed: out, failed } = stage.apply(&typed, &pushed, cursor)?;
        let failed: Vec<Rejected> = failed
            .into_iter()
            .map(|(row, reason)| Rejected { row, reason })
            .collect();
        hold(plan, name, &failed, record, &mut ls.quarantined, oc)?;
        let n = ls.shape(&flow.owners[s], |_| Ok(copy(&out))).map_err(|e| {
            let drift = matches!(e, NormalizeError::Drift(_));
            let err = shape_error(name, e);
            match stage.planned() {
                Some(st) if drift => {
                    statement_drift(st, &ls.shaper.live(), &out.batch.schema(), err)
                }
                _ => err,
            }
        })?;
        hold(plan, name, &n.rejected, record, &mut ls.quarantined, oc)?;
        shaped(lane, ls, n, oc).await?;
    }
    Ok(())
}

/// A change to an output a statement writes that the schema policy doesn't
/// take: `err`, with the statement, and for each output column whose type
/// changed from one `live` has, the cast that keeps it.
fn statement_drift(
    st: &crate::transform::Statement,
    live: &arrow_schema::Schema,
    now: &arrow_schema::Schema,
    err: JobError,
) -> JobError {
    let casts: Vec<String> = now
        .fields()
        .iter()
        .filter_map(|f| {
            let was = live.field_with_name(f.name()).ok()?;
            if was.data_type() == f.data_type() {
                return None;
            }
            let t = openetl_sql::cast_type(was)?;
            Some(format!(
                "CAST(… AS {t}) AS {}",
                openetl_sql::ident(f.name())
            ))
        })
        .collect();
    let mut msg = format!("{}; transform.sql {} writes it", err.message, st.at);
    if !casts.is_empty() {
        msg.push_str(&format!(
            ": to keep the type the output has, write {} there",
            casts.join(", ")
        ));
    }
    JobError::new(err.code, msg)
}

fn copy(t: &Typed) -> Typed {
    Typed {
        batch: t.batch.clone(),
        ids: t.ids.clone(),
        control: t.control.clone(),
        cells: t.cells.clone(),
        rejected: t.rejected.clone(),
        rows: t.rows.clone(),
    }
}

/// Adds rejected rows to the stream's quarantine for the epoch, or fails the
/// job if it has no quarantine.
fn hold(
    plan: &Plan,
    stream: &str,
    rejected: &[Rejected],
    record: impl Fn(usize) -> String,
    held: &mut (Vec<u8>, u64),
    c: &Counters,
) -> Result<(), JobError> {
    let Some(first) = rejected.first() else {
        return Ok(());
    };
    if plan.quarantine.is_none() {
        return Err(JobError::new(
            ErrorCode::DataInvalid,
            format!("stream {stream}: {}", first.reason),
        ));
    }
    let (lines, count) = held;
    for r in rejected {
        let line = serde_json::json!({
            "stream": stream,
            "reason": r.reason,
            "record": record(r.row),
        });
        lines.extend_from_slice(line.to_string().as_bytes());
        lines.push(b'\n');
    }
    *count += rejected.len() as u64;
    c.rows_quarantined
        .fetch_add(rejected.len() as u64, Ordering::Relaxed);
    Ok(())
}

/// Sends a shaped batch: its schema change, its held values and its rows.
async fn shaped(lane: &Lane, ls: &mut LaneStream, n: Shaped, c: &Counters) -> Result<(), JobError> {
    let stream = ls.stream;
    for v in &n.versioned {
        tracing::warn!(stream = %lane.flow.plan.streams[stream], "{v}");
    }
    if n.schema_changed {
        let change = schema_change(stream, &n.batch)?;
        lane.all(stream, || match &change {
            Out::SchemaChange(s, p) => Out::SchemaChange(*s, p.clone()),
            _ => unreachable!(),
        })
        .await?;
    }
    for (column, reason) in n.cells {
        let e = ls
            .cells
            .entry(column)
            .or_insert_with(|| CellHolds { cells: 0, reason });
        e.cells += 1;
        c.cells_held.fetch_add(1, Ordering::Relaxed);
    }
    for b in n.held {
        lane.send(Out::Held(stream, b)).await?;
    }
    if n.batch.num_rows() > 0 {
        let key = match lane.routed(stream) {
            true => ls.shaper.key_columns(),
            false => Vec::new(),
        };
        lane.rows(stream, &key, n.batch).await?;
    }
    Ok(())
}

/// A stream's state, with its holds if it holds.
fn at(plan: &Plan, stream: usize, shaper: &Shaper) -> Result<At, JobError> {
    let mut at = At {
        schema: shaper.schema(),
        delivered: shaper.delivered(),
        holds: None,
    };
    if plan.holds[stream].is_some() {
        let mut holding = Vec::new();
        for h in shaper.holding() {
            let delivered = match &h.delivered {
                Some(f) => {
                    ipc::encode_schema(&Arc::new(arrow_schema::Schema::new(vec![f.clone()])))
                        .map_err(|e| JobError::internal(e.to_string()))?
                }
                None => Vec::new(),
            };
            let cause = cause(&h, at.schema.as_ref());
            holding.push(ColumnHold {
                column: h.column,
                delivered,
                reason: h.reason,
                cause: Some(cause),
            });
        }
        at.holds = Some(StreamHolds {
            holding,
            cells: HashMap::new(),
            files: Vec::new(),
            replayed: None,
        });
    }
    Ok(at)
}

/// Why `h` is held, with its change in logical types.
fn cause(h: &openetl_normalize::hold::ColumnHold, schema: Option<&SchemaRef>) -> HoldCause {
    use hold_cause::Kind;
    use openetl_normalize::hold::Why;
    let (kind, policy, with) = match &h.why {
        Why::Unknown => (Kind::Unspecified, "", ""),
        Why::Policy(p) => (Kind::Policy, p.as_str(), ""),
        Why::Refused => (Kind::Refused, "", ""),
        Why::Fixed => (Kind::Fixed, "", ""),
        Why::Full => (Kind::ColumnLimit, "", ""),
        Why::Group(w) => (Kind::Group, "", w.as_str()),
    };
    let to = schema.and_then(|s| s.field_with_name(&h.column).ok());
    HoldCause {
        kind: kind as i32,
        policy: policy.to_owned(),
        with: with.to_owned(),
        source: h.source.clone(),
        from: h.delivered.as_ref().map(logical_type).unwrap_or_default(),
        to: to.map(logical_type).unwrap_or_default(),
    }
}

/// Sends the replay's next chunk of patches, which land in the next epoch.
async fn replay(lane: &Lane, ls: &mut LaneStream) -> Result<(), JobError> {
    let Some(r) = &mut ls.replay else {
        return Ok(());
    };
    for b in r.chunk().await? {
        let (patch, still) = ls
            .shaper
            .patch(&b, r.columns())
            .map_err(|e| JobError::internal(format!("patches: {e}")))?;
        r.still_held(still);
        if let Some(p) = patch {
            lane.send(Out::Patch(ls.stream, p)).await?;
        }
    }
    Ok(())
}

fn schema_change(stream: usize, batch: &RecordBatch) -> Result<Out, JobError> {
    let encoded =
        ipc::encode_schema(&batch.schema()).map_err(|e| JobError::internal(e.to_string()))?;
    let msg = SchemaChange {
        arrow_schema: encoded,
    };
    Ok(Out::SchemaChange(stream, msg.encode_to_vec()))
}

fn shape_error(stream: &str, e: NormalizeError) -> JobError {
    use arrow_schema::ArrowError as A;
    let code = match &e {
        NormalizeError::Json { .. } | NormalizeError::NotAnObject { .. } => ErrorCode::DataInvalid,
        NormalizeError::Drift(_) => ErrorCode::SchemaDrift,
        NormalizeError::Unsupported { .. }
        | NormalizeError::Restored(_)
        | NormalizeError::Declared(_)
        | NormalizeError::Mismatch { .. }
        | NormalizeError::Arrow(A::SchemaError(_) | A::NotYetImplemented(_)) => {
            ErrorCode::ConfigInvalid
        }
        NormalizeError::Arrow(_) => ErrorCode::Internal,
    };
    JobError::new(code, format!("stream {stream}: {e}"))
}

/// How a stream's lanes stand at a writer lane.
#[derive(Clone, Copy, PartialEq)]
enum Standing {
    Open,
    AtBarrier(Epoch),
    Ended,
}

/// A stream at a writer lane: the lanes feeding it, how each stands, and
/// what each sent after its barrier, which waits until they're all there.
struct StreamFeed {
    feeders: Vec<usize>,
    standing: Vec<Standing>,
    waiting: Vec<VecDeque<Out>>,
}

/// One writer lane's input: what its lanes send, aligned on each stream's
/// barriers. It never stops reading to align, so what a lane sends past a
/// barrier can't hold up the lanes that haven't reached it: it waits in memory,
/// under the job's limit.
struct Feed {
    rx: mpsc::Receiver<(usize, Out)>,
    streams: Vec<StreamFeed>,
    ready: VecDeque<Out>,
}

enum Next {
    Item(Out),
    Empty,
    Closed,
}

impl Feed {
    fn new(plan: &Plan, writer: usize, rx: mpsc::Receiver<(usize, Out)>) -> Self {
        let lanes = plan.inbound.len();
        let streams = (0..plan.streams.len())
            .map(|s| {
                let feeders: Vec<usize> = (0..lanes)
                    .filter(|l| {
                        plan.routed[s] && plan.writer_lanes > 1 || l % plan.writer_lanes == writer
                    })
                    .collect();
                StreamFeed {
                    standing: vec![Standing::Open; feeders.len()],
                    waiting: feeders.iter().map(|_| VecDeque::new()).collect(),
                    feeders,
                }
            })
            .collect();
        Self {
            rx,
            streams,
            ready: VecDeque::new(),
        }
    }

    async fn next(&mut self) -> Result<Option<Out>, JobError> {
        loop {
            if let Some(out) = self.ready.pop_front() {
                return Ok(Some(out));
            }
            match self.rx.recv().await {
                Some((lane, out)) => self.take(lane, out)?,
                None => return Ok(None),
            }
        }
    }

    fn try_next(&mut self) -> Result<Next, JobError> {
        loop {
            if let Some(out) = self.ready.pop_front() {
                return Ok(Next::Item(out));
            }
            match self.rx.try_recv() {
                Ok((lane, out)) => self.take(lane, out)?,
                Err(TryRecvError::Empty) => return Ok(Next::Empty),
                Err(TryRecvError::Disconnected) => return Ok(Next::Closed),
            }
        }
    }

    fn take(&mut self, lane: usize, out: Out) -> Result<(), JobError> {
        let s = out.stream();
        let st = &mut self.streams[s];
        if st.feeders.len() == 1 {
            self.ready.push_back(out);
            return Ok(());
        }
        let Some(i) = st.feeders.iter().position(|f| *f == lane) else {
            return Err(JobError::internal(format!(
                "lane {lane} sent stream {s} to a writer lane it doesn't feed"
            )));
        };
        if st.standing[i] == Standing::Open {
            Self::stand(st, i, out, &mut self.ready);
        } else {
            st.waiting[i].push_back(out);
        }
        self.align(s)
    }

    /// Takes what lane i sent while it's open.
    fn stand(st: &mut StreamFeed, i: usize, out: Out, ready: &mut VecDeque<Out>) {
        match out {
            Out::Barrier(_, e) => st.standing[i] = Standing::AtBarrier(e),
            Out::End(_) => st.standing[i] = Standing::Ended,
            out => ready.push_back(out),
        }
    }

    /// Passes the stream's barrier, or its end, as long as every lane is there.
    fn align(&mut self, s: usize) -> Result<(), JobError> {
        let st = &mut self.streams[s];
        loop {
            // Rows after their last barrier are never committed, so a lane's
            // end also ends those waiting at a barrier it won't reach.
            if st.standing.contains(&Standing::Ended) {
                for i in 0..st.standing.len() {
                    if st.waiting[i].iter().any(|o| matches!(o, Out::End(_))) {
                        st.standing[i] = Standing::Ended;
                    }
                    if st.standing[i] == Standing::Ended {
                        st.waiting[i].clear();
                    }
                }
                if st.standing.iter().all(|t| *t == Standing::Ended) {
                    self.ready.push_back(Out::End(s));
                    st.standing.iter_mut().for_each(|t| *t = Standing::Open);
                }
                return Ok(());
            }
            let Some(Standing::AtBarrier(e)) = st.standing.first().copied() else {
                return Ok(());
            };
            if !st
                .standing
                .iter()
                .all(|t| matches!(t, Standing::AtBarrier(_)))
            {
                return Ok(());
            }
            if st.standing.iter().any(|t| *t != Standing::AtBarrier(e)) {
                return Err(JobError::internal(format!(
                    "stream {s}'s lanes passed different barriers"
                )));
            }
            self.ready.push_back(Out::Barrier(s, e));
            for i in 0..st.standing.len() {
                st.standing[i] = Standing::Open;
                while st.standing[i] == Standing::Open {
                    let Some(out) = st.waiting[i].pop_front() else {
                        break;
                    };
                    Self::stand(st, i, out, &mut self.ready);
                }
            }
        }
    }
}

/// What a writer lane staged in an epoch.
#[derive(Default)]
struct Pieces {
    /// By writer lane.
    staged: BTreeMap<usize, Vec<u8>>,
    files: Vec<StagedStream>,
    held: Vec<StagedStream>,
}

/// Which epochs each writer lane has crossed or ended on each stream.
struct Alignment {
    /// By stream, then writer lane.
    crossed: Vec<Vec<u64>>,
    ended: Vec<Vec<bool>>,
    /// By writer lane: the epochs it has crossed on every stream.
    own: Vec<u64>,
    ready: u64,
    highest: u64,
    pieces: BTreeMap<u64, Pieces>,
    /// The job's span, and the span of the next epoch to become ready,
    /// which opened when the one before it was ready.
    job: Span,
    epoch: Span,
}

impl Alignment {
    fn new(streams: usize, writers: usize, first_epoch: u64) -> Self {
        let job = Span::current();
        Self {
            epoch: info_span!(parent: &job, "epoch", openetl.epoch = first_epoch),
            job,
            crossed: vec![vec![first_epoch - 1; writers]; streams],
            ended: vec![vec![false; writers]; streams],
            own: vec![first_epoch - 1; writers],
            ready: first_epoch - 1,
            highest: first_epoch - 1,
            pieces: BTreeMap::new(),
        }
    }

    fn cross(&mut self, stream: usize, writer: usize, n: u64) {
        self.crossed[stream][writer] = n;
        self.highest = self.highest.max(n);
    }

    fn end(&mut self, stream: usize, writer: usize) {
        self.ended[stream][writer] = true;
    }

    fn done(&self) -> bool {
        self.ended.iter().flatten().all(|e| *e)
    }

    fn pieces(&mut self, n: u64) -> &mut Pieces {
        self.pieces.entry(n).or_default()
    }

    /// The epochs the writer lane has crossed on every stream since the last
    /// call.
    fn own(&mut self, writer: usize) -> RangeInclusive<u64> {
        let aligned = (0..self.crossed.len())
            .filter(|&s| !self.ended[s][writer])
            .map(|s| self.crossed[s][writer])
            .min()
            .unwrap_or(self.highest)
            .min(self.highest);
        let own = self.own[writer] + 1..=aligned;
        self.own[writer] = self.own[writer].max(aligned);
        own
    }

    /// The epochs that became ready since the last call.
    fn advance(&mut self) -> RangeInclusive<u64> {
        let aligned = self
            .crossed
            .iter()
            .flatten()
            .zip(self.ended.iter().flatten())
            .filter(|(_, ended)| !**ended)
            .map(|(crossed, _)| *crossed)
            .min()
            .unwrap_or(self.highest)
            .min(self.highest);
        let ready = self.ready + 1..=aligned;
        self.ready = self.ready.max(aligned);
        ready
    }
}

impl Flow {
    /// Reports the epochs that became ready.
    async fn settle(&self, align: &mut Alignment) -> Result<(), JobError> {
        let ready = align.advance();
        if ready.is_empty() {
            return Ok(());
        }
        for n in ready {
            // The runner records every EpochReady, so this waits.
            let ev = self.epoch_ready(align, n)?;
            let _ = self.events.send(Ok(ev)).await;
        }
        let _ = self.events.try_send(Ok(self.metrics()));
        Ok(())
    }

    fn epoch_ready(&self, align: &mut Alignment, n: u64) -> Result<EventsResponse, JobError> {
        align.epoch = if align.done() && n >= align.highest {
            Span::none()
        } else {
            info_span!(parent: &align.job, "epoch", openetl.epoch = n + 1)
        };
        let encode = |schema: &SchemaRef| {
            ipc::encode_schema(schema)
                .map_err(|e| JobError::internal(format!("encode a schema: {e}")))
        };
        let plan = &self.plan;
        let Pieces {
            staged,
            files,
            held,
        } = align.pieces.remove(&n).unwrap_or_default();
        let mut by_stream: HashMap<String, Vec<_>> = HashMap::new();
        for s in held {
            by_stream.entry(s.stream).or_default().extend(s.files);
        }
        let mut schemas = HashMap::new();
        let mut output_schemas = HashMap::new();
        let mut delivered = HashMap::new();
        let mut holds = HashMap::new();
        let mut quarantine = Vec::new();
        if let Some(t) = &plan.transform {
            for (input, owner) in t.inputs.iter().zip(&self.inputs) {
                let (schema, q) = owner.take(n);
                quarantine.extend(q);
                if let Some(sc) = &schema {
                    schemas.insert(input.name.clone(), encode(sc)?);
                }
            }
        }
        for (s, owner) in self.owners.iter().enumerate() {
            let (at, q) = owner.take(n);
            quarantine.extend(q);
            let name = &plan.streams[s];
            if let Some(sc) = &at.schema {
                match plan.transform {
                    Some(_) => output_schemas.insert(name.clone(), encode(sc)?),
                    None => schemas.insert(name.clone(), encode(sc)?),
                };
            }
            if let Some(d) = &at.delivered {
                delivered.insert(name.clone(), encode(d)?);
            }
            if let Some(mut h) = at.holds {
                h.files = by_stream.remove(name).unwrap_or_default();
                holds.insert(name.clone(), h);
            }
        }
        Ok(EventsResponse {
            event: Some(events_response::Event::EpochReady(EpochReady {
                epoch: Some(Epoch {
                    generation: plan.generation,
                    n,
                }),
                staged: staged.into_values().collect(),
                snapshots: Vec::new(),
                schemas,
                quarantine,
                files,
                holds,
                delivered,
                output_schemas,
            })),
        })
    }
}

/// Returns false if the inbound edges closed before every stream ended.
async fn write_edge<W>(
    flow: Arc<Flow>,
    writer: usize,
    w: W,
    mut feed: Feed,
) -> Result<bool, JobError>
where
    W: AsyncWrite + Unpin,
{
    let plan = &flow.plan;
    let counters = &flow.counters;
    let Sink::Runner { edges, out_index } = &plan.sink else {
        return Err(JobError::internal("the sink has no edge"));
    };
    let (id, out_index) = (&edges[writer].id, &out_index[writer]);
    let gone = |e: std::io::Error| JobError::runner_gone(format!("edge {id}: {e}"));
    let mut w = BufWriter::with_capacity(1 << 16, w);
    let mut held = match &plan.held_url {
        Some(url) => Some(Staging::open(
            url,
            "",
            plan.generation,
            plan.first_epoch,
            &plan.streams,
            writer,
            plan.writer_lanes,
        )?),
        None => None,
    };

    let mut open = plan.streams.len();
    while open > 0 {
        let item = match feed.try_next()? {
            Next::Item(item) => item,
            Next::Empty => {
                w.flush().await.map_err(gone)?;
                match feed.next().await? {
                    Some(item) => item,
                    None => return Ok(false),
                }
            }
            // A reader failed, and its error says why.
            Next::Closed => return Ok(false),
        };
        let (kind, stream, payload) = match item {
            Out::Batch(s, batch) => {
                let payload =
                    ipc::encode_batch(&batch).map_err(|e| JobError::internal(e.to_string()))?;
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                counters[s]
                    .bytes_out
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                (FrameKind::Batch, s, payload)
            }
            Out::SchemaChange(s, payload) => (FrameKind::SchemaChange, s, payload),
            Out::Held(s, batch) => {
                let Some(h) = &mut held else {
                    return Err(JobError::internal("held values without a held URL"));
                };
                h.write(s, &batch).await?;
                continue;
            }
            Out::Patch(s, batch) => {
                let payload =
                    ipc::encode_batch(&batch).map_err(|e| JobError::internal(e.to_string()))?;
                counters[s]
                    .bytes_out
                    .fetch_add(payload.len() as u64, Ordering::Relaxed);
                (FrameKind::Patch, s, payload)
            }
            Out::Barrier(s, epoch) => {
                if let Some(h) = &mut held {
                    h.seal(s).await?;
                }
                let payload = Barrier { epoch: Some(epoch) }.encode_to_vec();
                frame::write(&mut w, FrameKind::Barrier, out_index[s], &payload)
                    .await
                    .map_err(gone)?;
                w.flush().await.map_err(gone)?;
                let mut a = flow.align.lock().await;
                a.cross(s, writer, epoch.n);
                if let Some(h) = &mut held {
                    let files = h.take(epoch.n);
                    a.pieces(epoch.n).held.extend(files);
                }
                flow.settle(&mut a).await?;
                continue;
            }
            Out::End(s) => {
                if let Some(h) = &mut held {
                    h.end(s).await?;
                }
                frame::write(&mut w, FrameKind::EndOfStream, out_index[s], &[])
                    .await
                    .map_err(gone)?;
                w.flush().await.map_err(gone)?;
                open -= 1;
                let mut a = flow.align.lock().await;
                a.end(s, writer);
                flow.settle(&mut a).await?;
                continue;
            }
        };
        frame::write(&mut w, kind, out_index[stream], &payload)
            .await
            .map_err(gone)?;
    }
    w.flush().await.map_err(gone)?;
    w.shutdown().await.map_err(gone)?;
    let _ = flow.events.try_send(Ok(flow.metrics()));
    Ok(true)
}

/// Stages every stream as Parquet, and reports each epoch's staged files in
/// its EpochReady. Runs on a blocking thread. Returns false if the inbound
/// edges closed before every stream ended.
fn write_parquet(
    flow: &Flow,
    writer: usize,
    url: &str,
    pipeline: &str,
    mut feed: Feed,
    rt: &tokio::runtime::Handle,
) -> Result<bool, JobError> {
    let plan = &flow.plan;
    let counters = &flow.counters;
    let streams: Vec<(String, String)> = plan
        .streams
        .iter()
        .cloned()
        .zip(plan.tables.iter().cloned())
        .collect();
    let mut staging = ParquetStaging::new(
        url,
        pipeline,
        plan.generation,
        plan.first_epoch,
        &streams,
        writer,
        plan.writer_lanes,
    )
    .map_err(sink_error)?;

    let mut open = plan.streams.len();
    while open > 0 {
        let Some(item) = rt.block_on(feed.next())? else {
            return Ok(false);
        };
        let mark: Mark = match item {
            Out::Batch(s, batch) => {
                staging.write(s, &batch).map_err(sink_error)?;
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                continue;
            }
            // Each batch carries its schema.
            Out::SchemaChange(..) => continue,
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch) => {
                let bytes = staging.seal(s).map_err(sink_error)?;
                counters[s].bytes_out.fetch_add(bytes, Ordering::Relaxed);
                Mark::Cross(s, epoch.n)
            }
            Out::End(s) => {
                staging.end(s).map_err(sink_error)?;
                open -= 1;
                Mark::End(s)
            }
        };
        rt.block_on(async {
            let mut a = flow.align.lock().await;
            mark.apply(&mut a, writer);
            for n in a.own(writer) {
                let staged = serde_json::to_vec(&staging.take(n)).map_err(|e| {
                    JobError::internal(format!("encode the staged descriptor: {e}"))
                })?;
                a.pieces(n).staged.insert(writer, staged);
            }
            flow.settle(&mut a).await
        })?;
    }
    let _ = flow.events.try_send(Ok(flow.metrics()));
    Ok(true)
}

/// A writer lane's crossing or end of a stream.
enum Mark {
    Cross(usize, u64),
    End(usize),
}

impl Mark {
    fn apply(&self, a: &mut Alignment, writer: usize) {
        match *self {
            Mark::Cross(s, n) => a.cross(s, writer, n),
            Mark::End(s) => a.end(s, writer),
        }
    }
}

/// Stages every stream as Parquet for a destination connector, and lists each
/// epoch's files in its EpochReady. Returns false if the inbound edges closed
/// before every stream ended.
async fn write_staged(
    flow: Arc<Flow>,
    writer: usize,
    url: String,
    dir: String,
    mut feed: Feed,
) -> Result<bool, JobError> {
    let plan = &flow.plan;
    let counters = &flow.counters;
    let mut staging = Staging::open(
        &url,
        &dir,
        plan.generation,
        plan.first_epoch,
        &plan.streams,
        writer,
        plan.writer_lanes,
    )?;
    let mut open = plan.streams.len();
    while open > 0 {
        let Some(item) = feed.next().await? else {
            return Ok(false);
        };
        let mark = match item {
            Out::Batch(s, batch) => {
                staging.write(s, &batch).await?;
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                continue;
            }
            // Each file carries its schema.
            Out::SchemaChange(..) => continue,
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch) => {
                let bytes = staging.seal(s).await?;
                counters[s].bytes_out.fetch_add(bytes, Ordering::Relaxed);
                Mark::Cross(s, epoch.n)
            }
            Out::End(s) => {
                staging.end(s).await?;
                open -= 1;
                Mark::End(s)
            }
        };
        let mut a = flow.align.lock().await;
        mark.apply(&mut a, writer);
        if let Mark::Cross(_, n) = mark {
            let files = staging.take(n);
            a.pieces(n).files.extend(files);
        }
        flow.settle(&mut a).await?;
    }
    let _ = flow.events.try_send(Ok(flow.metrics()));
    Ok(true)
}

/// Drops every batch, and reports each epoch ready with nothing staged.
/// Returns false if the inbound edges closed before every stream ended.
async fn write_discard(flow: Arc<Flow>, writer: usize, mut feed: Feed) -> Result<bool, JobError> {
    let plan = &flow.plan;
    let counters = &flow.counters;
    let mut open = plan.streams.len();
    while open > 0 {
        let Some(item) = feed.next().await? else {
            return Ok(false);
        };
        let mark = match item {
            Out::Batch(s, batch) => {
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                continue;
            }
            Out::SchemaChange(..) => continue,
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch) => Mark::Cross(s, epoch.n),
            Out::End(s) => {
                open -= 1;
                Mark::End(s)
            }
        };
        let mut a = flow.align.lock().await;
        mark.apply(&mut a, writer);
        for n in a.own(writer) {
            a.pieces(n).staged.insert(writer, Vec::new());
        }
        flow.settle(&mut a).await?;
    }
    let _ = flow.events.try_send(Ok(flow.metrics()));
    Ok(true)
}

fn no_holds() -> JobError {
    JobError::internal("only a runner sink takes holds")
}

fn sink_error(e: openetl_formats::Error) -> JobError {
    use openetl_formats::Error as E;
    use std::io::ErrorKind as K;
    let code = match &e {
        E::Config(_) => ErrorCode::ConfigInvalid,
        E::Io { source, .. } => match source.kind() {
            K::PermissionDenied => ErrorCode::PermissionDenied,
            K::StorageFull | K::QuotaExceeded => ErrorCode::DestinationQuota,
            _ => ErrorCode::DestinationUnavailable,
        },
        E::Parquet { .. } => ErrorCode::Internal,
    };
    JobError::new(code, format!("Parquet sink: {e}"))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    use openetl_normalize::{Normalizer, naming};
    use tokio::io::{BufReader, DuplexStream, duplex};

    use crate::plan::tests::request;

    struct Harness {
        to_engine: DuplexStream,
        from_engine: BufReader<DuplexStream>,
        events: mpsc::Receiver<Result<EventsResponse, Status>>,
        job: tokio::task::JoinHandle<Result<(), JobError>>,
    }

    /// Streams a and b arrive on one edge; the outbound edge lists them as b, a.
    fn start(first_epoch: u64) -> Harness {
        let mut req = request(&["a", "b"]);
        req.first_epoch = first_epoch;
        start_with(req)
    }

    fn start_with(req: openetl_proto::engine::v1::StartJobRequest) -> Harness {
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let (to_engine, engine_in) = duplex(1 << 16);
        let (engine_out, from_engine) = duplex(1 << 16);
        let (tx, events) = mpsc::channel(64);
        let job = tokio::spawn(run(
            plan,
            vec![BufReader::new(engine_in)],
            vec![engine_out],
            tx,
        ));
        Harness {
            to_engine,
            from_engine: BufReader::new(from_engine),
            events,
            job,
        }
    }

    async fn put(w: &mut DuplexStream, kind: FrameKind, stream: u32, payload: Vec<u8>) {
        frame::write(w, kind, stream, &payload).await.unwrap();
    }

    fn rows(rows: &[&str]) -> Vec<u8> {
        Rows {
            rows: rows.iter().map(|r| r.as_bytes().to_vec()).collect(),
        }
        .encode_to_vec()
    }

    fn barrier(n: u64) -> Vec<u8> {
        Barrier {
            epoch: Some(Epoch { generation: 1, n }),
        }
        .encode_to_vec()
    }

    async fn drain(r: &mut BufReader<DuplexStream>) -> Vec<frame::Frame> {
        let mut out = Vec::new();
        while let Some(f) = frame::read(r).await.unwrap() {
            out.push(f);
        }
        out
    }

    struct LaneHarness {
        ins: Vec<DuplexStream>,
        outs: Vec<BufReader<DuplexStream>>,
        events: mpsc::Receiver<Result<EventsResponse, Status>>,
        job: tokio::task::JoinHandle<Result<(), JobError>>,
    }

    fn start_lanes(req: openetl_proto::engine::v1::StartJobRequest) -> LaneHarness {
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let (mut ins, mut readers, mut outs, mut writers) = (vec![], vec![], vec![], vec![]);
        for _ in 0..plan.inbound.len() {
            let (w, r) = duplex(1 << 16);
            ins.push(w);
            readers.push(BufReader::new(r));
        }
        if matches!(plan.sink, Sink::Runner { .. }) {
            for _ in 0..plan.writer_lanes {
                let (w, r) = duplex(1 << 16);
                writers.push(w);
                outs.push(BufReader::new(r));
            }
        }
        let (tx, events) = mpsc::channel(64);
        let job = tokio::spawn(run(plan, readers, writers, tx));
        LaneHarness {
            ins,
            outs,
            events,
            job,
        }
    }

    impl LaneHarness {
        async fn end_all(&mut self, streams: u32) {
            for w in &mut self.ins {
                for s in 0..streams {
                    put(w, FrameKind::EndOfStream, s, vec![]).await;
                }
                w.shutdown().await.unwrap();
            }
        }

        async fn finish(self) -> (Vec<Vec<frame::Frame>>, Vec<EpochReady>) {
            let (out, mut events) = self.finish_events().await;
            (out, ready(&mut events).await)
        }

        async fn finish_events(
            mut self,
        ) -> (
            Vec<Vec<frame::Frame>>,
            mpsc::Receiver<Result<EventsResponse, Status>>,
        ) {
            let mut drains = JoinSet::new();
            let n = self.outs.len();
            for (i, mut r) in std::mem::take(&mut self.outs).into_iter().enumerate() {
                drains.spawn(async move { (i, drain(&mut r).await) });
            }
            let mut out: Vec<Vec<frame::Frame>> = (0..n).map(|_| Vec::new()).collect();
            while let Some(d) = drains.join_next().await {
                let (i, frames) = d.unwrap();
                out[i] = frames;
            }
            self.job.await.unwrap().unwrap();
            (out, self.events)
        }
    }

    fn data_frames(frames: &[frame::Frame]) -> Vec<(FrameKind, u32)> {
        frames
            .iter()
            .filter(|f| f.kind != FrameKind::SchemaChange)
            .map(|f| (f.kind, f.stream))
            .collect()
    }

    fn int_column(f: &frame::Frame, name: &str) -> Vec<i64> {
        let b = ipc::decode_batch(f.payload.clone()).unwrap();
        let schema = b.schema();
        let c = b.column(schema.index_of(name).unwrap());
        c.as_primitive::<arrow_array::types::Int64Type>()
            .iter()
            .flatten()
            .collect()
    }

    #[tokio::test]
    async fn straight_lanes_write_on_their_own_writer_lanes() {
        let mut h = start_lanes(crate::plan::tests::lanes_request(&["a"], 2, 2));
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"x": 2}"#])).await;
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(1)).await;
        // A writer lane doesn't wait for another: lane 1's rows after its
        // barrier go out before lane 0 has reached it.
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"x": 3}"#])).await;
        let mut seen = Vec::new();
        while seen.len() < 4 {
            seen.push(frame::read(&mut h.outs[1]).await.unwrap().unwrap().kind);
        }
        use FrameKind as K;
        assert_eq!(seen, [K::SchemaChange, K::Batch, K::Barrier, K::Batch]);
        assert!(h.events.try_recv().is_err(), "the epoch isn't ready yet");

        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(1)).await;
        h.end_all(1).await;
        let (out, ready) = h.finish().await;
        assert_eq!(
            data_frames(&out[0]),
            [(K::Batch, 0), (K::Barrier, 0), (K::EndOfStream, 0)]
        );
        let batch = out[0].iter().find(|f| f.kind == K::Batch).unwrap();
        assert_eq!(int_column(batch, "x"), [1]);
        assert_eq!(out[1].last().unwrap().kind, K::EndOfStream);
        let ns: Vec<_> = ready.iter().map(|r| r.epoch.unwrap().n).collect();
        assert_eq!(ns, [1]);
    }

    #[test]
    fn a_lanes_clock_counts_what_it_is_doing_so_far() {
        let start = Instant::now();
        let c = LaneClock::new();
        std::thread::sleep(Duration::from_millis(20));
        c.enter(start, Doing::Busy);
        std::thread::sleep(Duration::from_millis(10));
        c.enter(start, Doing::Backpressured);
        std::thread::sleep(Duration::from_millis(10));
        let m = c.metrics(start);
        assert!(m.idle_nanos >= 20_000_000, "{m:?}");
        assert!(m.busy_nanos >= 10_000_000, "{m:?}");
        assert!(m.backpressured_nanos >= 10_000_000, "{m:?}");
        let spent = m.idle_nanos + m.busy_nanos + m.backpressured_nanos;
        assert!(spent <= nanos(start), "{m:?}");
    }

    #[tokio::test]
    async fn metrics_count_each_lanes_rows() {
        let mut h = start_lanes(crate::plan::tests::lanes_request(&["a"], 2, 2));
        let two = rows(&[r#"{"x": 1}"#, r#"{"x": 2}"#]);
        put(&mut h.ins[0], FrameKind::Rows, 0, two).await;
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"x": 3}"#])).await;
        for i in 0..2 {
            put(&mut h.ins[i], FrameKind::Barrier, 0, barrier(1)).await;
        }
        h.end_all(1).await;
        let (_, mut events) = h.finish_events().await;
        let mut last = None;
        while let Some(Ok(ev)) = events.recv().await {
            if let Some(events_response::Event::Metrics(m)) = ev.event {
                last = Some(m);
            }
        }
        let lanes = last.unwrap().lanes;
        let rows: Vec<_> = lanes.iter().map(|l| l.rows_in).collect();
        assert_eq!(rows, [2, 1]);
        assert!(lanes.iter().all(|l| l.idle_nanos > 0 && l.busy_nanos > 0));
    }

    #[tokio::test]
    async fn a_writer_lane_aligns_the_lanes_feeding_it() {
        let mut h = start_lanes(crate::plan::tests::lanes_request(&["a"], 2, 1));
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(1)).await;
        // After its barrier: it waits for lane 1's, with its edge still read.
        for i in 0..2000 {
            let row = format!(r#"{{"x": {}}}"#, 100 + i);
            put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[&row])).await;
        }
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(2)).await;
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"x": 2}"#])).await;
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(1)).await;
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(2)).await;
        h.end_all(1).await;
        let (out, ready) = h.finish().await;
        let frames = &out[0];
        let barrier1 = frames
            .iter()
            .position(|f| f.kind == FrameKind::Barrier)
            .unwrap();
        let before: Vec<i64> = frames[..barrier1]
            .iter()
            .filter(|f| f.kind == FrameKind::Batch)
            .flat_map(|f| int_column(f, "x"))
            .collect();
        assert_eq!(before.len(), 2);
        assert!(before.contains(&1) && before.contains(&2));
        let after: Vec<i64> = frames[barrier1 + 1..]
            .iter()
            .filter(|f| f.kind == FrameKind::Batch)
            .flat_map(|f| int_column(f, "x"))
            .collect();
        assert_eq!(after, (100..2100).collect::<Vec<i64>>());
        let barriers = frames.iter().filter(|f| f.kind == FrameKind::Barrier);
        assert_eq!(barriers.count(), 2);
        let ns: Vec<_> = ready.iter().map(|r| r.epoch.unwrap().n).collect();
        assert_eq!(ns, [1, 2]);
    }

    #[tokio::test]
    async fn rows_routed_by_key_group_meet_on_one_writer_lane() {
        let mut req = crate::plan::tests::lanes_request(&["a"], 2, 2);
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].routing = openetl_proto::engine::v1::Routing::KeyGroup as i32;
        let mut h = start_lanes(req);
        let all: Vec<String> = (0..200).map(|i| format!(r#"{{"id": {i}}}"#)).collect();
        let all: Vec<&str> = all.iter().map(String::as_str).collect();
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&all)).await;
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&all)).await;
        for w in &mut h.ins {
            put(w, FrameKind::Barrier, 0, barrier(1)).await;
        }
        h.end_all(1).await;
        let (out, ready) = h.finish().await;
        let mut seen = [Vec::new(), Vec::new()];
        for (w, frames) in out.iter().enumerate() {
            let barriers = frames.iter().filter(|f| f.kind == FrameKind::Barrier);
            assert_eq!(barriers.count(), 1);
            seen[w] = frames
                .iter()
                .filter(|f| f.kind == FrameKind::Batch)
                .flat_map(|f| int_column(f, "id"))
                .collect();
            assert!(!seen[w].is_empty());
        }
        assert_eq!(seen[0].len() + seen[1].len(), 400);
        for id in &seen[0] {
            assert!(!seen[1].contains(id), "{id} reached both writer lanes");
        }
        assert_eq!(ready.len(), 1);
    }

    #[tokio::test]
    async fn lanes_share_one_schema_and_the_epoch_keeps_its_latest() {
        let mut h = start_lanes(crate::plan::tests::lanes_request(&["a"], 2, 2));
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(1)).await;
        put(
            &mut h.ins[1],
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 2.5, "y": "s"}"#]),
        )
        .await;
        while frame::read(&mut h.outs[1]).await.unwrap().unwrap().kind != FrameKind::Batch {}
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(1)).await;
        // Lane 0 takes the owner's schema for an integer it would otherwise
        // infer on its own.
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"x": 3}"#])).await;
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(2)).await;
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(2)).await;
        h.end_all(1).await;
        let (out, ready) = h.finish().await;
        let batches: Vec<_> = out[0]
            .iter()
            .filter(|f| f.kind == FrameKind::Batch)
            .map(|f| ipc::decode_batch(f.payload.clone()).unwrap())
            .collect();
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Int64);
        assert_eq!(batches[1].schema().field(0).data_type(), &DataType::Float64);
        assert!(batches[1].schema().field_with_name("y").is_ok());
        use DataType as T;
        let shapes: Vec<_> = ready.iter().map(schema_types).collect();
        let full = vec![("a".to_string(), vec![T::Float64, T::Utf8])];
        assert_eq!(shapes, [full.clone(), full]);
    }

    #[tokio::test]
    async fn the_schema_policy_starts_once_every_lane_has_passed_the_barrier() {
        let mut req = crate::plan::tests::lanes_request(&["a"], 2, 2);
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::Block as i32;
        let mut h = start_lanes(req);
        put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(&mut h.ins[0], FrameKind::Barrier, 0, barrier(1)).await;
        // Still in the epoch that establishes the schema on this lane.
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"y": 1}"#])).await;
        put(&mut h.ins[1], FrameKind::Barrier, 0, barrier(1)).await;
        put(&mut h.ins[1], FrameKind::Rows, 0, rows(&[r#"{"z": 1}"#])).await;
        h.ins[1].flush().await.unwrap();
        let err = h.job.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::SchemaDrift, "{err}");
        assert!(err.message.contains('z'), "{err}");
    }

    #[tokio::test]
    async fn parquet_and_staged_sinks_stage_a_part_per_writer_lane() {
        let dir = tempfile::tempdir().unwrap();
        for sink in ["parquet", "staged"] {
            let url = format!("file://{}/{sink}", dir.path().display());
            let mut req = crate::plan::tests::lanes_request(&["a"], 2, 2);
            req.edges
                .retain(|e| e.direction() == openetl_proto::engine::v1::EdgeDirection::ToEngine);
            req.sink = Some(openetl_proto::engine::v1::SinkPlan {
                sink: Some(match sink {
                    "parquet" => openetl_proto::engine::v1::sink_plan::Sink::Parquet(
                        openetl_proto::engine::v1::ParquetSink { url: url.clone() },
                    ),
                    _ => openetl_proto::engine::v1::sink_plan::Sink::Staged(
                        openetl_proto::engine::v1::StagedSink {
                            url: url.clone(),
                            dir: String::new(),
                        },
                    ),
                }),
            });
            let mut h = start_lanes(req);
            for w in 0..2 {
                let row = format!(r#"{{"x": {w}}}"#);
                put(&mut h.ins[w], FrameKind::Rows, 0, rows(&[&row])).await;
                put(&mut h.ins[w], FrameKind::Barrier, 0, barrier(1)).await;
            }
            h.end_all(1).await;
            let (_, ready) = h.finish().await;
            assert_eq!(ready.len(), 1, "{sink}");
            match sink {
                "parquet" => {
                    assert_eq!(ready[0].staged.len(), 2);
                    let parts: Vec<String> = ready[0]
                        .staged
                        .iter()
                        .map(|s| {
                            let d: openetl_formats::Descriptor = serde_json::from_slice(s).unwrap();
                            d.streams[0].files[0].staged.clone()
                        })
                        .collect();
                    assert!(parts[0].ends_with("part-00000.parquet"), "{parts:?}");
                    assert!(parts[1].ends_with("part-00001.parquet"), "{parts:?}");
                }
                _ => {
                    let mut files: Vec<_> = ready[0]
                        .files
                        .iter()
                        .map(|f| (f.lane, f.files[0].path.clone()))
                        .collect();
                    files.sort();
                    assert_eq!(
                        files,
                        [
                            (0, "1/1/a/part-00000.parquet".to_string()),
                            (1, "1/1/a/part-00001.parquet".to_string())
                        ]
                    );
                }
            }
        }
    }

    #[test]
    fn lanes_settle_on_one_schema_without_publishing_again() {
        let plan = Plan::from_request(crate::plan::tests::lanes_request(&["a"], 4, 4)).unwrap();
        let owner = Owner::new(&plan, 0).unwrap();
        let mut lanes: Vec<LaneStream> = (0..4)
            .map(|_| {
                let o = owner.lock();
                LaneStream {
                    stream: 0,
                    shaper: o.shaper.fork(),
                    version: o.version,
                    quarantined: (Vec::new(), 0),
                    cells: BTreeMap::new(),
                    replay: None,
                }
            })
            .collect();
        let rows: Vec<String> = (0..50)
            .map(|i| format!(r#"{{"id": {i}, "price": {i}.25, "name": "n{i}", "at": "2026-01-01T00:00:{:02}Z"}}"#, i % 60))
            .collect();
        for round in 0..20 {
            for ls in &mut lanes {
                ls.shape(&owner, |sh| sh.type_rows(rows.iter().map(|r| r.as_bytes())))
                    .unwrap();
            }
            if round == 0 {
                continue;
            }
            assert!(
                owner.version.load(Ordering::Acquire) <= 4,
                "the schema was published {} times by round {round}",
                owner.version.load(Ordering::Acquire)
            );
        }
    }

    /// The next batch of outbound stream index s, its columns' names.
    async fn next_columns(r: &mut BufReader<DuplexStream>, s: u32) -> Vec<String> {
        loop {
            let f = frame::read(r).await.unwrap().unwrap();
            if f.kind == FrameKind::Batch && f.stream == s {
                let b = ipc::decode_batch(f.payload).unwrap();
                return b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|n| !n.starts_with("_openetl"))
                    .collect();
            }
        }
    }

    /// A job on streams a and b whose schemas a test owns, as the owner
    /// outside the engine.
    struct RemoteHarness {
        plan: Arc<Plan>,
        remote: Arc<crate::remote::Remote>,
        proposals: crate::remote::Proposals,
        w: DuplexStream,
        out: BufReader<DuplexStream>,
        job: tokio::task::JoinHandle<Result<(), JobError>>,
    }

    fn start_remote() -> RemoteHarness {
        let mut req = request(&["a", "b"]);
        req.remote_schemas = true;
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let remote = plan.remote.clone().unwrap();
        let proposals = remote.take_proposals().unwrap();
        let (to_engine, engine_in) = duplex(1 << 16);
        let (engine_out, from_engine) = duplex(1 << 16);
        let (tx, _events) = mpsc::channel(64);
        let job = tokio::spawn(run(
            plan.clone(),
            vec![BufReader::new(engine_in)],
            vec![engine_out],
            tx,
        ));
        RemoteHarness {
            plan,
            remote,
            proposals,
            w: to_engine,
            out: BufReader::new(from_engine),
            job,
        }
    }

    /// Stream a's version, as the owner sends it.
    fn take(version: u64, schema: Vec<u8>) -> openetl_proto::engine::v1::SchemasRequest {
        openetl_proto::engine::v1::SchemasRequest {
            stream: 0,
            version,
            arrow_schema: schema,
        }
    }

    /// The next batch of outbound stream index s, its column's type.
    async fn column_type(r: &mut BufReader<DuplexStream>, s: u32, name: &str) -> DataType {
        loop {
            let f = frame::read(r).await.unwrap().unwrap();
            if f.kind == FrameKind::Batch && f.stream == s {
                let b = ipc::decode_batch(f.payload).unwrap();
                return b
                    .schema()
                    .field_with_name(name)
                    .unwrap()
                    .data_type()
                    .clone();
            }
        }
    }

    // A column that held integers past ±2^53 changes no type, but the owner
    // learns of it, so a float in it from another engine makes it text, as
    // in one engine.
    #[tokio::test]
    async fn a_remote_owner_learns_of_big_integers() {
        let RemoteHarness {
            plan,
            remote,
            mut proposals,
            mut w,
            mut out,
            job,
        } = start_remote();
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        let p = proposals.recv().await.unwrap().unwrap();
        remote.update(&take(1, p.arrow_schema)).unwrap();
        assert_eq!(column_type(&mut out, 1, "x").await, DataType::Int64);

        put(
            &mut w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 9007199254740993}"#]),
        )
        .await;
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!(p.base, 1);
        let big = ipc::decode_schema(&p.arrow_schema).unwrap();
        assert_eq!(
            big.field_with_name("x").unwrap().data_type(),
            &DataType::Int64
        );
        assert_eq!(
            big.metadata()
                .get(openetl_normalize::BIG_INTS_KEY)
                .map(String::as_str),
            Some(r#"["x"]"#)
        );
        remote.update(&take(2, p.arrow_schema)).unwrap();
        assert_eq!(column_type(&mut out, 1, "x").await, DataType::Int64);

        // Another engine, at version 2, gets a float in x.
        let mut other = shaper(&plan, 0, Some(big)).unwrap();
        other.rows([br#"{"x": 1.5}"#.as_slice()]).unwrap();
        let text = other.schema().unwrap();
        assert_eq!(
            text.field_with_name("x").unwrap().data_type(),
            &DataType::Utf8
        );
        remote
            .update(&take(3, ipc::encode_schema(&text).unwrap()))
            .unwrap();
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 2}"#])).await;
        assert_eq!(column_type(&mut out, 1, "x").await, DataType::Utf8);
        assert!(proposals.try_recv().is_err());

        for s in 0..2 {
            put(&mut w, FrameKind::EndOfStream, s, vec![]).await;
        }
        w.shutdown().await.unwrap();
        drain(&mut out).await;
        job.await.unwrap().unwrap();
    }

    // A schemas call that fails releases the lane waiting for its proposal's
    // answer, which fails the job with why.
    #[tokio::test]
    async fn a_failed_schemas_call_fails_the_job() {
        let RemoteHarness {
            remote,
            mut proposals,
            mut w,
            mut out,
            job,
            ..
        } = start_remote();
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        proposals.recv().await.unwrap().unwrap();
        remote.fail(JobError::internal("the owner is gone"));
        let err = tokio::time::timeout(Duration::from_secs(10), job)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(err.message, "the owner is gone");
        drop(w);
        drain(&mut out).await;
    }

    #[tokio::test]
    async fn a_remote_owner_decides_a_streams_schema() {
        let RemoteHarness {
            plan,
            remote,
            mut proposals,
            mut w,
            mut out,
            job,
        } = start_remote();

        // The lane's change is proposed from version 0, and taken.
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!((p.stream, p.base), (0, 0));
        remote.update(&take(1, p.arrow_schema.clone())).unwrap();
        assert_eq!(next_columns(&mut out, 1).await, ["x"]);
        let mut owner =
            shaper(&plan, 0, Some(ipc::decode_schema(&p.arrow_schema).unwrap())).unwrap();

        // Another engine adds z: the lane shapes by it, proposing nothing.
        owner.rows([br#"{"x": 2, "z": "q"}"#.as_slice()]).unwrap();
        let v2 = ipc::encode_schema(&owner.schema().unwrap()).unwrap();
        remote.update(&take(2, v2)).unwrap();
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 3}"#])).await;
        assert_eq!(next_columns(&mut out, 1).await, ["x", "z"]);

        // The lane adds w from version 2, as another engine adds v, which the
        // owner takes first: the lane proposes w again from version 3.
        put(
            &mut w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 4, "w": true}"#]),
        )
        .await;
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!(p.base, 2);
        owner.rows([br#"{"x": 5, "v": 1.5}"#.as_slice()]).unwrap();
        remote
            .update(&take(
                3,
                ipc::encode_schema(&owner.schema().unwrap()).unwrap(),
            ))
            .unwrap();
        let p = proposals.recv().await.unwrap().unwrap();
        assert_eq!(p.base, 3);
        remote.update(&take(4, p.arrow_schema)).unwrap();
        assert_eq!(next_columns(&mut out, 1).await, ["x", "z", "v", "w"]);
        assert!(proposals.try_recv().is_err());

        for s in 0..2 {
            put(&mut w, FrameKind::EndOfStream, s, vec![]).await;
        }
        w.shutdown().await.unwrap();
        drain(&mut out).await;
        job.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn passes_streams_through_with_barriers_in_band() {
        let mut h = start(1);
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#, r#"{"x": 2}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Rows, 1, rows(&[r#"{"y": "s"}"#])).await;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 2.5}"#])).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        // Outbound indexes: a is 1, b is 0.
        let shape: Vec<_> = frames.iter().map(|f| (f.kind, f.stream)).collect();
        use FrameKind as K;
        assert_eq!(
            shape,
            vec![
                (K::SchemaChange, 1),
                (K::Batch, 1),
                (K::Barrier, 1),
                (K::SchemaChange, 0),
                (K::Batch, 0),
                (K::SchemaChange, 1),
                (K::Batch, 1),
                (K::Barrier, 0),
                (K::EndOfStream, 1),
                (K::EndOfStream, 0),
            ]
        );
        let b = ipc::decode_batch(frames[1].payload.clone()).unwrap();
        assert_eq!(b.schema().field(0).data_type(), &DataType::Int64);
        let b = ipc::decode_batch(frames[6].payload.clone()).unwrap();
        assert_eq!(b.schema().field(0).data_type(), &DataType::Float64);
        let sc = SchemaChange::decode(frames[5].payload.as_slice()).unwrap();
        assert_eq!(ipc::decode_schema(&sc.arrow_schema).unwrap(), b.schema());

        let mut ready = Vec::new();
        while let Some(Ok(ev)) = h.events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                ready.push(r.epoch.unwrap().n);
            }
        }
        assert_eq!(ready, vec![1]);
    }

    async fn ready(events: &mut mpsc::Receiver<Result<EventsResponse, Status>>) -> Vec<EpochReady> {
        let mut out = Vec::new();
        while let Some(Ok(ev)) = events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                out.push(r);
            }
        }
        out
    }

    fn schema_types(r: &EpochReady) -> Vec<(String, Vec<DataType>)> {
        let mut out: Vec<_> = r
            .schemas
            .iter()
            .map(|(name, encoded)| {
                let schema = ipc::decode_schema(encoded).unwrap();
                let types = schema
                    .fields()
                    .iter()
                    .filter(|f| !f.name().starts_with("_openetl_"))
                    .map(|f| f.data_type().clone());
                (name.clone(), types.collect())
            })
            .collect();
        out.sort();
        out
    }

    #[tokio::test]
    async fn epochs_carry_each_streams_schema_at_its_barrier() {
        let mut h = start(1);
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 2.5}"#])).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::Rows, 1, rows(&[r#"{"y": "s"}"#])).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        put(w, FrameKind::Barrier, 0, barrier(3)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
        drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let ready = ready(&mut h.events).await;
        let shapes: Vec<_> = ready.iter().map(schema_types).collect();
        use DataType as T;
        assert_eq!(
            shapes,
            vec![
                vec![("a".into(), vec![T::Int64])],
                vec![("a".into(), vec![T::Float64]), ("b".into(), vec![T::Utf8])],
                vec![("a".into(), vec![T::Float64]), ("b".into(), vec![T::Utf8])],
            ]
        );
        // Without a transform, the streams are the outputs, as a runner that
        // predates outputs reads them.
        assert!(ready.iter().all(|r| r.output_schemas.is_empty()));
    }

    #[tokio::test]
    async fn inference_continues_from_the_restored_schema() {
        let mut first = Normalizer::default();
        first.push([br#"{"x": 1.5}"#.as_slice()]).unwrap();
        let mut req = request(&["a", "b"]);
        req.streams[0].restored_schema = ipc::encode_schema(&first.schema()).unwrap();
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let batch = frames.iter().find(|f| f.kind == FrameKind::Batch).unwrap();
        let b = ipc::decode_batch(batch.payload.clone()).unwrap();
        assert_eq!(b.schema().field(0).data_type(), &DataType::Float64);

        let ready = ready(&mut h.events).await;
        let shapes: Vec<_> = ready.iter().map(schema_types).collect();
        let a = vec![("a".to_string(), vec![DataType::Float64])];
        assert_eq!(shapes, vec![a.clone(), a]);
    }

    #[tokio::test]
    async fn arrow_batches_pass_through_renamed() {
        let mut h = start(1);
        let mut n = Normalizer::default();
        let batch = n.push([br#"{"Key": "v"}"#.as_slice()]).unwrap().batch;
        let w = &mut h.to_engine;
        put(w, FrameKind::Batch, 1, ipc::encode_batch(&batch).unwrap()).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        assert_eq!(
            (frames[0].kind, frames[0].stream),
            (FrameKind::SchemaChange, 0)
        );
        assert_eq!((frames[1].kind, frames[1].stream), (FrameKind::Batch, 0));
        let out = ipc::decode_batch(frames[1].payload.clone()).unwrap();
        assert_eq!(out.column(0), batch.column(0));
        let schema = out.schema();
        assert_eq!(schema.field(0).name(), "key");
        let names = naming::source_names(&schema).unwrap();
        assert_eq!(names.get("key").map(String::as_str), Some("Key"));
        assert_eq!(
            schema.metadata().get(naming::TABLE_KEY).map(String::as_str),
            Some("b")
        );
    }

    #[tokio::test]
    async fn a_passthrough_job_leaves_batches_as_they_came() {
        let mut req = request(&["a", "b"]);
        req.passthrough = true;
        let mut h = start_with(req);
        let mut n = Normalizer::default();
        let batch = n.push([br#"{"Key": "v"}"#.as_slice()]).unwrap().batch;
        let w = &mut h.to_engine;
        put(w, FrameKind::Batch, 1, ipc::encode_batch(&batch).unwrap()).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        assert_eq!((frames[0].kind, frames[0].stream), (FrameKind::Batch, 0));
        assert_eq!(ipc::decode_batch(frames[0].payload.clone()).unwrap(), batch);
    }

    #[tokio::test]
    async fn a_passthrough_job_takes_no_rows() {
        let mut req = request(&["a", "b"]);
        req.passthrough = true;
        let mut h = start_with(req);
        put(&mut h.to_engine, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        let (err, _) = failure(h).await;
        assert!(err.to_string().contains("passthrough"), "{err}");
    }

    #[tokio::test]
    async fn an_ended_stream_does_not_hold_back_epochs() {
        let mut h = start(4);
        let w = &mut h.to_engine;
        put(w, FrameKind::Barrier, 0, barrier(4)).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        put(w, FrameKind::Barrier, 0, barrier(5)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
        drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let mut ready = Vec::new();
        while let Some(Ok(ev)) = h.events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                ready.push(r.epoch.unwrap().n);
            }
        }
        assert_eq!(ready, vec![4, 5]);
    }

    async fn failure(h: Harness) -> (JobError, Status) {
        let Harness {
            mut events,
            job,
            from_engine,
            ..
        } = h;
        let err = job.await.unwrap().unwrap_err();
        drop(from_engine);
        loop {
            match events.recv().await.unwrap() {
                Ok(_) => continue,
                Err(st) => return (err, st),
            }
        }
    }

    #[tokio::test]
    async fn a_closed_edge_means_the_runner_is_gone() {
        let mut h = start(1);
        put(&mut h.to_engine, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        h.to_engine.shutdown().await.unwrap();
        let (err, _) = failure(h).await;
        assert!(err.runner_gone);
    }

    #[tokio::test]
    async fn rejects_out_of_order_barriers() {
        let mut h = start(1);
        put(&mut h.to_engine, FrameKind::Barrier, 0, barrier(2)).await;
        let (err, st) = failure(h).await;
        assert_eq!(err.code, ErrorCode::Internal);
        assert!(!err.runner_gone);
        assert!(st.message().contains("crossed barrier"), "{err} / {st:?}");
    }

    #[tokio::test]
    async fn bad_rows_are_quarantined_per_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = request(&["a", "b"]);
        req.quarantine_url = format!("file://{}", dir.path().display());
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 1}"#, "[1]", r#"{"x": 2}"#]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::Rows, 0, rows(&["nope"])).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let batch = frames.iter().find(|f| f.kind == FrameKind::Batch).unwrap();
        assert_eq!(
            ipc::decode_batch(batch.payload.clone()).unwrap().num_rows(),
            2
        );

        let ready = ready(&mut h.events).await;
        assert_eq!(ready.len(), 1);
        let q = &ready[0].quarantine;
        assert_eq!(q.len(), 1);
        assert_eq!(
            (q[0].stream.as_str(), q[0].rows, q[0].output),
            ("a", 1, false)
        );
        assert!(q[0].url.ends_with("/1/1/a.jsonl"), "{}", q[0].url);
        let text = std::fs::read_to_string(dir.path().join("1/1/a.jsonl")).unwrap();
        let line: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(line["stream"], "a");
        assert_eq!(line["record"], "[1]");
        assert!(
            line["reason"]
                .as_str()
                .unwrap()
                .contains("not a JSON object")
        );
        // Rows after the last barrier are never committed.
        assert!(!dir.path().join("1/2").exists());
    }

    #[tokio::test]
    async fn typed_rows_that_dont_fit_their_declared_types_are_quarantined() {
        use arrow_array::types::Decimal128Type;
        use arrow_array::{RecordBatch, StringArray, cast::AsArray};
        use arrow_schema::{Field, Schema};

        let dir = tempfile::tempdir().unwrap();
        let mut req = request(&["a", "b"]);
        req.quarantine_url = format!("file://{}", dir.path().display());
        let declared = Schema::new(vec![Field::new("price", DataType::Decimal128(38, 9), true)]);
        req.streams[0].arrow_schema = ipc::encode_schema(&declared).unwrap();
        let mut h = start_with(req);
        let schema = Arc::new(Schema::new(vec![Field::new("price", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                "1.5",
                "NaN",
                "0.0000000001",
            ]))],
        )
        .unwrap();
        let w = &mut h.to_engine;
        put(w, FrameKind::Batch, 0, ipc::encode_batch(&batch).unwrap()).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let batch = frames.iter().find(|f| f.kind == FrameKind::Batch).unwrap();
        let out = ipc::decode_batch(batch.payload.clone()).unwrap();
        assert_eq!(out.num_rows(), 1);
        let price = out.column(0).as_primitive::<Decimal128Type>();
        assert_eq!(price.values()[0], 1_500_000_000);

        let ready = ready(&mut h.events).await;
        assert_eq!(ready[0].quarantine[0].rows, 2);
        let text = std::fs::read_to_string(dir.path().join("1/1/a.jsonl")).unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines[0]["record"], r#"{"price":"NaN"}"#);
        assert!(lines[0]["reason"].as_str().unwrap().starts_with("nan:"));
        assert!(
            lines[1]["reason"]
                .as_str()
                .unwrap()
                .starts_with("precision:")
        );
    }

    #[tokio::test]
    async fn a_blocked_change_is_schema_drift() {
        let mut req = request(&["a", "b"]);
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::Block as i32;
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"x": 1, "y": 2}"#])).await;
        let (err, st) = failure(h).await;
        assert_eq!(err.code, ErrorCode::SchemaDrift);
        assert_eq!(st.code(), tonic::Code::FailedPrecondition);
        assert!(err.message.contains("\"y\" is new"), "{}", err.message);
    }

    #[tokio::test]
    async fn bad_rows_are_data_invalid() {
        let mut h = start(1);
        put(&mut h.to_engine, FrameKind::Rows, 1, rows(&["[1]"])).await;
        let (err, st) = failure(h).await;
        assert_eq!(err.code, ErrorCode::DataInvalid);
        assert_eq!(st.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn a_parquet_sink_reports_staged_files_per_epoch() {
        use openetl_formats::Descriptor;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}", dir.path().display());
        let plan = Arc::new(
            Plan::from_request(crate::plan::tests::parquet_request(&["a", "b"], &url)).unwrap(),
        );
        let (mut w, engine_in) = duplex(1 << 16);
        let (tx, mut events) = mpsc::channel(64);
        let job = tokio::spawn(run::<_, DuplexStream>(
            plan,
            vec![BufReader::new(engine_in)],
            Vec::<DuplexStream>::new(),
            tx,
        ));
        put(
            &mut w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 1}"#, r#"{"x": 2}"#]),
        )
        .await;
        put(&mut w, FrameKind::Rows, 1, rows(&[r#"{"y": "s"}"#])).await;
        put(&mut w, FrameKind::Barrier, 0, barrier(1)).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(1)).await;
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 3.5}"#])).await;
        put(&mut w, FrameKind::Barrier, 0, barrier(2)).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(2)).await;
        // Rows after the last barrier are never committed.
        put(&mut w, FrameKind::Rows, 1, rows(&[r#"{"y": "t"}"#])).await;
        put(&mut w, FrameKind::EndOfStream, 0, vec![]).await;
        put(&mut w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        job.await.unwrap().unwrap();

        let mut ready = Vec::new();
        while let Some(Ok(ev)) = events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                let d: Descriptor = serde_json::from_slice(&r.staged[0]).unwrap();
                assert_eq!(d.epoch, r.epoch.unwrap().n);
                ready.push(d);
            }
        }
        let shape: Vec<Vec<(&str, u64)>> = ready
            .iter()
            .map(|d| {
                d.streams
                    .iter()
                    .map(|s| (s.stream.as_str(), s.files.iter().map(|f| f.rows).sum()))
                    .collect()
            })
            .collect();
        assert_eq!(shape, vec![vec![("a", 2), ("b", 1)], vec![("a", 1)]]);

        let f = &ready[1].streams[0].files[0];
        let file = std::fs::File::open(dir.path().join(&f.staged)).unwrap();
        let batches: Vec<_> = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(batches[0].schema().field(0).data_type(), &DataType::Float64);
        let left = std::fs::read_dir(dir.path().join("_openetl/staging/p/1/3/b"))
            .unwrap()
            .count();
        assert_eq!(left, 0, "the unsealed file is removed");
    }

    #[tokio::test]
    async fn a_staged_sink_lists_each_epochs_files() {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("file://{}/_staging", dir.path().display());
        let plan = Arc::new(
            Plan::from_request(crate::plan::tests::staged_request(&["a", "b"], &url)).unwrap(),
        );
        let (mut w, engine_in) = duplex(1 << 16);
        let (tx, mut events) = mpsc::channel(64);
        let job = tokio::spawn(run::<_, DuplexStream>(
            plan,
            vec![BufReader::new(engine_in)],
            Vec::<DuplexStream>::new(),
            tx,
        ));
        put(
            &mut w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"x": 1}"#, r#"{"x": 2}"#]),
        )
        .await;
        put(&mut w, FrameKind::Barrier, 0, barrier(1)).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(1)).await;
        put(&mut w, FrameKind::Rows, 1, rows(&[r#"{"y": "s"}"#])).await;
        put(&mut w, FrameKind::Barrier, 0, barrier(2)).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(2)).await;
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 3}"#])).await;
        put(&mut w, FrameKind::EndOfStream, 0, vec![]).await;
        put(&mut w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        job.await.unwrap().unwrap();

        let mut shape = Vec::new();
        while let Some(Ok(ev)) = events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                assert!(r.staged.is_empty());
                let n = r.epoch.unwrap().n;
                for st in r.files {
                    for f in st.files {
                        assert!(dir.path().join("_staging").join(&f.path).is_file());
                        shape.push((n, st.stream.clone(), f.path, f.rows));
                    }
                }
            }
        }
        assert_eq!(
            shape,
            vec![
                (1, "a".into(), "1/1/a/part-00000.parquet".into(), 2),
                (2, "b".into(), "1/2/b/part-00000.parquet".into(), 1),
            ]
        );
        assert!(!dir.path().join("_staging/1/3").exists());
    }

    #[tokio::test]
    async fn a_discard_sink_reports_every_epoch() {
        let plan =
            Arc::new(Plan::from_request(crate::plan::tests::discard_request(&["a", "b"])).unwrap());
        let (mut w, engine_in) = duplex(1 << 16);
        let (tx, mut events) = mpsc::channel(64);
        let job = tokio::spawn(run::<_, DuplexStream>(
            plan,
            vec![BufReader::new(engine_in)],
            Vec::<DuplexStream>::new(),
            tx,
        ));
        put(&mut w, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        put(&mut w, FrameKind::Barrier, 0, barrier(1)).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(1)).await;
        put(&mut w, FrameKind::Rows, 1, rows(&[r#"{"y": 2}"#])).await;
        put(&mut w, FrameKind::Barrier, 1, barrier(2)).await;
        put(&mut w, FrameKind::EndOfStream, 1, vec![]).await;
        put(&mut w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
        job.await.unwrap().unwrap();

        let mut ready = Vec::new();
        while let Some(Ok(ev)) = events.recv().await {
            if let Some(events_response::Event::EpochReady(r)) = ev.event {
                assert_eq!(r.staged, vec![Vec::<u8>::new()]);
                ready.push(r.epoch.unwrap().n);
            }
        }
        assert_eq!(ready, vec![1, 2]);
    }

    fn holding(dir: &std::path::Path, precision: u8) -> openetl_proto::engine::v1::StartJobRequest {
        use arrow_schema::{Field, Schema};
        let mut req = request(&["a", "b"]);
        req.held_url = format!("file://{}/held", dir.display());
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Decimal128(precision, 2), true),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].holds = Some(openetl_proto::engine::v1::HoldPlan {
            cells: true,
            ..Default::default()
        });
        req
    }

    fn priced(prices: &[&str]) -> Vec<u8> {
        use arrow_array::{Int64Array, StringArray};
        let ids: Vec<i64> = (1..=prices.len() as i64).collect();
        let b = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(ids)) as arrow_array::ArrayRef,
            ),
            ("price", Arc::new(StringArray::from(prices.to_vec()))),
        ])
        .unwrap();
        ipc::encode_batch(&b).unwrap()
    }

    #[tokio::test]
    async fn held_cells_are_kept_per_epoch_and_patched_once_they_fit() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = start_with(holding(dir.path(), 10));
        let w = &mut h.to_engine;
        put(w, FrameKind::Batch, 0, priced(&["1.5", "12345678901.5"])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let batch = frames.iter().find(|f| f.kind == FrameKind::Batch).unwrap();
        let b = ipc::decode_batch(batch.payload.clone()).unwrap();
        assert_eq!(b.num_rows(), 2, "the row with a held cell is delivered");
        assert!(b.schema().index_of("_openetl_held").is_ok());
        let first = ready(&mut h.events).await;
        let held = &first[0].holds["a"];
        assert_eq!(held.cells["price"].cells, 1);
        assert!(held.cells["price"].reason.starts_with("precision:"));
        assert_eq!(held.files.len(), 1);
        let file = held.files[0].path.clone();
        assert_eq!(file, "1/1/a/part-00000.parquet");
        assert!(dir.path().join("held").join(&file).is_file());
        assert!(!first[0].holds.contains_key("b"), "b holds nothing");

        // A wider declared type takes the value, so a release patches it in.
        let mut req = holding(dir.path(), 20);
        req.generation = 2;
        req.first_epoch = 2;
        req.streams[0].restored_schema = first[0].schemas["a"].clone();
        req.streams[0].holds.as_mut().unwrap().replay = Some(openetl_proto::engine::v1::Replay {
            files: vec![file],
            columns: vec!["price".into()],
            ..Default::default()
        });
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        let barrier2 = |n| {
            Barrier {
                epoch: Some(Epoch { generation: 2, n }),
            }
            .encode_to_vec()
        };
        for n in [2, 3] {
            put(w, FrameKind::Barrier, 0, barrier2(n)).await;
            put(w, FrameKind::Barrier, 1, barrier2(n)).await;
        }
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let a: Vec<FrameKind> = frames
            .iter()
            .filter(|f| f.stream == 1)
            .map(|f| f.kind)
            .collect();
        use FrameKind as K;
        assert_eq!(a, [K::Barrier, K::Patch, K::Barrier, K::EndOfStream]);
        let p = frames.iter().find(|f| f.kind == K::Patch).unwrap();
        let p = ipc::decode_batch(p.payload.clone()).unwrap();
        assert_eq!(p.num_rows(), 1);
        let price = p.column(p.schema().index_of("price").unwrap());
        let price = price.as_primitive::<arrow_array::types::Decimal128Type>();
        assert_eq!(price.iter().next(), Some(Some(1_234_567_890_150)));

        let second = ready(&mut h.events).await;
        let at = |i: usize| second[i].holds["a"].replayed.unwrap();
        assert!(!at(0).done, "epoch 2 ends before the chunk");
        assert!(at(1).done && at(1).still_held == 0, "{:?}", at(1));
    }

    #[tokio::test]
    async fn block_holds_a_new_column_and_reports_what_the_destination_gets() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = request(&["a", "b"]);
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::Block as i32;
        req.held_url = format!("file://{}/held", dir.path().display());
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].holds = Some(Default::default());
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"id": 1, "x": 1}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"id": 2, "x": 2, "y": "new"}"#]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::Barrier, 1, barrier(2)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let last = frames.iter().rfind(|f| f.kind == FrameKind::Batch).unwrap();
        let b = ipc::decode_batch(last.payload.clone()).unwrap();
        assert!(b.schema().index_of("y").is_err(), "y isn't delivered");
        let ready = ready(&mut h.events).await;
        assert!(ready[0].delivered.is_empty());
        let delivered = ipc::decode_schema(&ready[1].delivered["a"]).unwrap();
        assert_eq!(delivered.fields(), b.schema().fields());
        let stream = ipc::decode_schema(&ready[1].schemas["a"]).unwrap();
        assert!(stream.index_of("y").is_ok(), "the stream's schema has it");
        let holds = &ready[1].holds["a"];
        assert_eq!(holds.holding.len(), 1);
        assert_eq!(holds.holding[0].column, "y");
        assert!(holds.holding[0].delivered.is_empty());
        let cause = holds.holding[0].cause.clone().unwrap();
        assert_eq!(
            (
                cause.kind(),
                cause.policy.as_str(),
                cause.source.as_str(),
                cause.from.as_str(),
                cause.to.as_str()
            ),
            (
                openetl_proto::engine::v1::hold_cause::Kind::Policy,
                "block",
                "y",
                "",
                "string"
            )
        );
        assert_eq!(holds.files.len(), 1);
    }

    #[tokio::test]
    async fn rejects_frames_after_the_end() {
        let mut h = start(1);
        put(&mut h.to_engine, FrameKind::EndOfStream, 0, vec![]).await;
        put(&mut h.to_engine, FrameKind::Rows, 0, rows(&[r#"{"x": 1}"#])).await;
        let (err, _) = failure(h).await;
        assert!(err.message.contains("after the end"));
    }

    /// The transform of plan::tests::transform_request with other SQL and
    /// outputs, each keyed as given; no outputs are expected.
    fn sql_request(
        sql: &str,
        outputs: &[(&str, &[&str])],
    ) -> openetl_proto::engine::v1::StartJobRequest {
        let mut req = crate::plan::tests::transform_request();
        let t = req.transform.as_mut().unwrap();
        t.sql = sql.into();
        t.expected_outputs.clear();
        req.outputs = outputs
            .iter()
            .map(|(name, key)| openetl_proto::engine::v1::StreamPlan {
                name: name.to_string(),
                primary_key: key.iter().map(|k| k.to_string()).collect(),
                ..Default::default()
            })
            .collect();
        req.edges[1].streams = outputs.iter().map(|(name, _)| name.to_string()).collect();
        req
    }

    fn batches(frames: &[frame::Frame], stream: u32) -> Vec<RecordBatch> {
        frames
            .iter()
            .filter(|f| f.kind == FrameKind::Batch && f.stream == stream)
            .map(|f| ipc::decode_batch(f.payload.clone()).unwrap())
            .collect()
    }

    fn column_strings(b: &RecordBatch, name: &str) -> Vec<Option<String>> {
        b.column_by_name(name)
            .unwrap()
            .as_string::<i32>()
            .iter()
            .map(|v| v.map(str::to_owned))
            .collect()
    }

    fn column_ints(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
        b.column_by_name(name)
            .unwrap()
            .as_primitive::<arrow_array::types::Int64Type>()
            .iter()
            .collect()
    }

    fn field_names(encoded: &[u8]) -> Vec<String> {
        ipc::decode_schema(encoded)
            .unwrap()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect()
    }

    /// Statements read typed rows under source names, and outputs get
    /// destination names and metadata columns; a stream no statement
    /// writes passes through. Each epoch reports the streams' schemas as
    /// the statements read them, and the outputs' apart.
    #[tokio::test]
    async fn statements_write_outputs_and_streams_pass_through() {
        let mut req = crate::plan::tests::transform_request();
        req.transform.as_mut().unwrap().sql =
            "INSERT INTO a SELECT id, lower(email) AS email FROM a WHERE id > 1; INSERT INTO c SELECT id FROM b".into();
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"id": 1, "email": "A"}"#, r#"{"id": 2, "email": "B"}"#]),
        )
        .await;
        put(w, FrameKind::Rows, 1, rows(&[r#"{"id": 5, "email": "X"}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();

        // The outbound edge carries c, b and a.
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let a = batches(&frames, 2);
        assert_eq!(a.len(), 1);
        assert_eq!(column_ints(&a[0], "id"), [Some(2)]);
        assert_eq!(column_strings(&a[0], "email"), [Some("b".into())]);
        assert_eq!(
            column_strings(&a[0], "_openetl_op"),
            [Some("insert".into())]
        );
        let b = batches(&frames, 1);
        assert_eq!(column_strings(&b[0], "email"), [Some("X".into())]);
        let c = batches(&frames, 0);
        assert_eq!(column_ints(&c[0], "id"), [Some(5)]);
        // c's rows keep b's IDs, and the sequence b's cursor gives.
        assert_eq!(
            c[0].column_by_name("_openetl_id"),
            b[0].column_by_name("_openetl_id")
        );
        assert_eq!(column_ints(&c[0], "_openetl_seq"), [Some(5)]);

        let ready = ready(&mut h.events).await;
        assert_eq!(ready.len(), 1);
        let r = &ready[0];
        let mut inputs: Vec<&String> = r.schemas.keys().collect();
        inputs.sort();
        assert_eq!(inputs, ["a", "b"]);
        assert_eq!(field_names(&r.schemas["a"]), ["id", "email"]);
        let mut outputs: Vec<&String> = r.output_schemas.keys().collect();
        outputs.sort();
        assert_eq!(outputs, ["a", "b", "c"]);
        let c = ipc::decode_schema(&r.output_schemas["c"]).unwrap();
        assert!(c.field_with_name("_openetl_id").is_ok());
        assert_eq!(
            c.metadata().get(naming::TABLE_KEY).map(String::as_str),
            Some("t_c")
        );
    }

    /// A batch of a changelog: id, status, and op and seq control columns.
    fn changelog_batch(rows: &[(i64, Option<&str>, &str)]) -> Vec<u8> {
        use arrow_array::{Int64Array, StringArray};
        let control = |kind: &str| {
            HashMap::from([(openetl_normalize::CONTROL_KEY.to_owned(), kind.to_owned())])
        };
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("email", DataType::Utf8, true),
            arrow_schema::Field::new("op", DataType::Utf8, false).with_metadata(control("op")),
            arrow_schema::Field::new("seq", DataType::Int64, false).with_metadata(control("seq")),
        ]));
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(rows.iter().map(|r| Some(r.0)).collect::<Int64Array>()),
                Arc::new(rows.iter().map(|r| r.1).collect::<StringArray>()),
                Arc::new(rows.iter().map(|r| Some(r.2)).collect::<StringArray>()),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows.len()).map(|i| i64::try_from(i).unwrap()),
                )),
            ],
        )
        .unwrap();
        ipc::encode_batch(&b).unwrap()
    }

    /// On a changelog, deletes pass with their key, and an update the
    /// filter drops becomes a delete of its key.
    #[tokio::test]
    async fn a_changelogs_deletes_pass_and_filtered_rows_become_deletes() {
        let mut req = sql_request(
            "INSERT INTO a SELECT id, upper(email) AS email FROM a WHERE email <> 'void'",
            &[("a", &["id"])],
        );
        req.streams[0].changelog = true;
        req.outputs[0].changelog = true;
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(
            w,
            FrameKind::Batch,
            0,
            changelog_batch(&[
                (1, Some("new"), "insert"),
                (2, Some("void"), "update"),
                (3, None, "delete"),
            ]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let a = batches(&frames, 0);
        assert_eq!(column_ints(&a[0], "id"), [Some(1), Some(2), Some(3)]);
        assert_eq!(
            column_strings(&a[0], "_openetl_op"),
            [
                Some("insert".into()),
                Some("delete".into()),
                Some("delete".into())
            ]
        );
        assert_eq!(
            column_strings(&a[0], "email"),
            [Some("NEW".into()), None, None]
        );
        assert_eq!(
            column_ints(&a[0], "_openetl_seq"),
            [Some(0), Some(1), Some(2)]
        );
        assert!(a[0].column_by_name("_openetl_unchanged").is_some());
    }

    /// A row a statement can't evaluate is quarantined under the output's
    /// name, apart from the streams', and the rest land.
    #[tokio::test]
    async fn rows_a_statement_cant_evaluate_are_quarantined_as_the_outputs() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = sql_request(
            "INSERT INTO c SELECT id, CAST(email AS BIGINT) AS n FROM b",
            &[("c", &["id"])],
        );
        req.quarantine_url = format!("file://{}", dir.path().display());
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        put(
            w,
            FrameKind::Rows,
            1,
            rows(&[r#"{"id": 1, "email": "12"}"#, r#"{"id": 2, "email": "x"}"#]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let c = batches(&frames, 0);
        assert_eq!(column_ints(&c[0], "n"), [Some(12)]);
        let ready = ready(&mut h.events).await;
        let q = &ready[0].quarantine;
        assert_eq!(q.len(), 1);
        assert_eq!(
            (q[0].stream.as_str(), q[0].rows, q[0].output),
            ("c", 1, true)
        );
        assert!(q[0].url.ends_with("/_outputs/1/1/c.jsonl"), "{}", q[0].url);
        let text = std::fs::read_to_string(dir.path().join("_outputs/1/1/c.jsonl")).unwrap();
        let line: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(line["stream"], "c");
        assert_eq!(line["record"], r#"{"id": 2, "email": "x"}"#);
        assert!(
            line["reason"]
                .as_str()
                .unwrap()
                .starts_with("transform.sql 1:1, writing c: "),
            "{}",
            line["reason"]
        );
    }

    #[tokio::test]
    async fn without_a_quarantine_a_row_a_statement_cant_evaluate_fails_the_job() {
        let mut h = start_with(sql_request(
            "INSERT INTO c SELECT id, CAST(email AS BIGINT) AS n FROM b",
            &[("c", &["id"])],
        ));
        put(
            &mut h.to_engine,
            FrameKind::Rows,
            1,
            rows(&[r#"{"id": 2, "email": "x"}"#]),
        )
        .await;
        let (err, status) = failure(h).await;
        assert_eq!(err.code, ErrorCode::DataInvalid, "{}", err.message);
        assert!(
            status.message().contains("stream c: transform.sql 1:1"),
            "{}",
            status.message()
        );
    }

    /// A stream no statement reads and that doesn't pass through isn't
    /// typed: its rows can't fail the job, and its epochs wait on nothing.
    #[tokio::test]
    async fn a_stream_that_feeds_no_output_is_read_and_dropped() {
        let mut h = start_with(sql_request(
            "INSERT INTO c SELECT id FROM a",
            &[("c", &["id"])],
        ));
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"id": 1, "email": "a"}"#])).await;
        put(w, FrameKind::Rows, 1, rows(&["[1]"])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        assert_eq!(column_ints(&batches(&frames, 0)[0], "id"), [Some(1)]);
        let ready = ready(&mut h.events).await;
        assert_eq!(ready.len(), 1);
        assert!(ready[0].quarantine.is_empty());
        assert!(!ready[0].schemas.contains_key("b"));
    }

    /// A column the stream gains mid-run reaches an output through `*`.
    #[tokio::test]
    async fn a_new_column_reaches_star_mid_run() {
        let mut h = start_with(sql_request(
            "INSERT INTO everything SELECT * FROM a",
            &[("everything", &["id"])],
        ));
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"id": 1, "email": "a"}"#])).await;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"id": 2, "email": "b", "extra": "z"}"#]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let out = batches(&frames, 0);
        assert_eq!(out.len(), 2);
        assert!(out[0].column_by_name("extra").is_none());
        assert_eq!(column_strings(&out[1], "extra"), [Some("z".into())]);
        let ready = ready(&mut h.events).await;
        assert!(field_names(&ready[0].output_schemas["everything"]).contains(&"extra".into()));
        assert!(field_names(&ready[0].schemas["a"]).contains(&"extra".into()));
    }

    /// Statements run in every lane, and each writer lane aligns the lanes
    /// feeding it on the outputs' barriers.
    #[tokio::test]
    async fn statements_run_in_every_lane() {
        let mut req = sql_request(
            "INSERT INTO ids SELECT id AS uid FROM a WHERE id % 2 = 0",
            &[("ids", &["uid"]), ("b", &["id"])],
        );
        req.lanes = 2;
        req.writer_lanes = 2;
        let out = req.edges.pop().unwrap();
        let inbound = req.edges.pop().unwrap();
        for l in 0..2 {
            let mut e = inbound.clone();
            e.id = format!("in-{l}");
            e.lane = l;
            req.edges.push(e);
            let mut e = out.clone();
            e.id = format!("out-{l}");
            e.lane = l;
            req.edges.push(e);
        }
        let mut h = start_lanes(req);
        for (l, w) in h.ins.iter_mut().enumerate() {
            let rows_of: Vec<String> = (0..4)
                .map(|i| format!(r#"{{"id": {}, "email": "e"}}"#, l * 10 + i))
                .collect();
            let refs: Vec<&str> = rows_of.iter().map(String::as_str).collect();
            put(w, FrameKind::Rows, 0, rows(&refs)).await;
            put(w, FrameKind::Rows, 1, rows(&[r#"{"id": 7, "email": "e"}"#])).await;
            put(w, FrameKind::Barrier, 0, barrier(1)).await;
            put(w, FrameKind::Barrier, 1, barrier(1)).await;
        }
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        let mut uids: Vec<Option<i64>> = Vec::new();
        for (w, frames) in outs.iter().enumerate() {
            let ids = batches(frames, 0);
            assert_eq!(ids.len(), 1, "writer lane {w}");
            uids.extend(column_ints(&ids[0], "uid"));
            assert_eq!(batches(frames, 1).len(), 1, "writer lane {w}");
        }
        uids.sort();
        assert_eq!(uids, [Some(0), Some(2), Some(10), Some(12)]);
        assert_eq!(ready.len(), 1);
    }

    /// Stream b gains n, an integer, then n's values widen to doubles.
    async fn widen_n(w: &mut DuplexStream) {
        let row = |id: i64, n: &str| format!(r#"{{"id": {id}, "email": "e", "n": {n}}}"#);
        put(w, FrameKind::Rows, 1, rows(&[&row(1, "1")])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::Rows, 1, rows(&[&row(2, "1.5")])).await;
    }

    /// An output a statement writes holds a column whose type changes
    /// under a schema policy that doesn't take it, as a stream does.
    #[tokio::test]
    async fn an_output_holds_a_column_whose_type_changes() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = sql_request(
            "INSERT INTO b SELECT * FROM b",
            &[("b", &["id"]), ("a", &["id"])],
        );
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::Block as i32;
        req.held_url = format!("file://{}/held", dir.path().display());
        req.outputs[0].holds = Some(Default::default());
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        widen_n(w).await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::Barrier, 1, barrier(2)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let ready = ready(&mut h.events).await;
        assert!(ready[0].holds["b"].holding.is_empty());
        let holds = &ready[1].holds["b"];
        assert_eq!(holds.holding.len(), 1);
        assert_eq!(holds.holding[0].column, "n");
        let cause = holds.holding[0].cause.clone().unwrap();
        assert_eq!(
            (cause.kind(), cause.from.as_str(), cause.to.as_str()),
            (
                openetl_proto::engine::v1::hold_cause::Kind::Policy,
                "int64",
                "float64"
            )
        );
        assert_eq!(holds.files.len(), 1);
        let delivered = ipc::decode_schema(&ready[1].delivered["b"]).unwrap();
        assert_eq!(
            delivered.field_with_name("n").unwrap().data_type(),
            &DataType::Int64,
            "the destination keeps n as it was"
        );
        assert!(!ready[1].holds.contains_key("a"));
    }

    /// Where the output can't hold the column, the job stops with
    /// SCHEMA_DRIFT, naming the statement and the cast that keeps the type.
    #[tokio::test]
    async fn an_outputs_type_change_it_cant_hold_gives_the_cast_that_keeps_it() {
        let mut req = sql_request("INSERT INTO b SELECT * FROM b", &[("b", &["id"])]);
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::Block as i32;
        let mut h = start_with(req);
        widen_n(&mut h.to_engine).await;
        let (err, _) = failure(h).await;
        assert_eq!(err.code, ErrorCode::SchemaDrift, "{}", err.message);
        assert!(
            err.message.ends_with(
                "; transform.sql 1:1 writes it: to keep the type the output has, write CAST(… AS BIGINT) AS n there"
            ),
            "{}",
            err.message
        );
    }

    /// The alias that keeps an output column's type quotes a name that
    /// needs it, and the statement it gives plans.
    #[test]
    fn the_cast_that_keeps_a_type_quotes_a_name_that_needs_it() {
        use arrow_schema::{Field, Schema};
        let input = |t: DataType| crate::transform::Input {
            name: "a".into(),
            schema: Some(Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("Email Address", t, true),
            ]))),
            key: vec!["id".into()],
            changelog: false,
        };
        let t = crate::transform::Transform::plan(
            "INSERT INTO c SELECT * FROM a",
            vec![input(DataType::Int64)],
        )
        .unwrap();
        let live = Schema::new(vec![Field::new("Email Address", DataType::Int64, true)]);
        let now = Schema::new(vec![Field::new("Email Address", DataType::Float64, true)]);
        let err = statement_drift(
            &t.statements[0],
            &live,
            &now,
            JobError::new(ErrorCode::SchemaDrift, "drift"),
        );
        let alias = r#"CAST(… AS BIGINT) AS "Email Address""#;
        assert!(err.message.contains(alias), "{}", err.message);
        let fixed =
            r#"INSERT INTO c SELECT id, CAST("Email Address" AS BIGINT) AS "Email Address" FROM a"#;
        let planned =
            crate::transform::Transform::plan(fixed, vec![input(DataType::Float64)]).unwrap();
        assert_eq!(
            planned.statements[0].schema.field(1).data_type(),
            &DataType::Int64
        );
    }

    /// The SQL request for a stream a whose price is declared a decimal of
    /// `precision` digits: a passes through, holding cells, and c reads it.
    fn priced_sql(
        dir: &std::path::Path,
        precision: u8,
    ) -> openetl_proto::engine::v1::StartJobRequest {
        use arrow_schema::{Field, Schema};
        let mut req = sql_request(
            "INSERT INTO c SELECT id, price FROM a",
            &[("c", &["id"]), ("a", &["id"])],
        );
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("price", DataType::Decimal128(precision, 2), true),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        req.held_url = format!("file://{}/held", dir.display());
        req.quarantine_url = format!("file://{}/q", dir.display());
        req.outputs[1].holds = Some(openetl_proto::engine::v1::HoldPlan {
            cells: true,
            ..Default::default()
        });
        req
    }

    /// A stream that passes through holding cells holds them as without a
    /// transform, and a statement reading it quarantines their rows, which
    /// have no value there to evaluate. A release patches the output's held
    /// cells in.
    #[tokio::test]
    async fn a_stream_passed_through_holds_cells_and_releases_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = start_with(priced_sql(dir.path(), 10));
        let w = &mut h.to_engine;
        put(w, FrameKind::Batch, 0, priced(&["1.5", "12345678901.5"])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let a = batches(&frames, 1);
        assert_eq!(a[0].num_rows(), 2, "the row with a held cell is delivered");
        assert!(a[0].schema().index_of("_openetl_held").is_ok());
        assert_eq!(column_ints(&batches(&frames, 0)[0], "id"), [Some(1)]);
        let first = ready(&mut h.events).await;
        let held = &first[0].holds["a"];
        assert_eq!(held.cells["price"].cells, 1);
        assert_eq!(held.files.len(), 1);
        let file = held.files[0].path.clone();
        let q = &first[0].quarantine;
        assert_eq!(q.len(), 1);
        assert_eq!(
            (q[0].stream.as_str(), q[0].rows, q[0].output),
            ("c", 1, true)
        );
        let text = std::fs::read_to_string(dir.path().join("q/_outputs/1/1/c.jsonl")).unwrap();
        let line: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
        assert!(
            line["reason"].as_str().unwrap().contains(
                "column price has a value that doesn't fit its type, which the stream holds"
            ),
            "{}",
            line["reason"]
        );

        let mut req = priced_sql(dir.path(), 20);
        req.generation = 2;
        req.first_epoch = 2;
        req.outputs[1].restored_schema = first[0].output_schemas["a"].clone();
        req.outputs[1].holds.as_mut().unwrap().replay = Some(openetl_proto::engine::v1::Replay {
            files: vec![file],
            columns: vec!["price".into()],
            ..Default::default()
        });
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        let barrier2 = Barrier {
            epoch: Some(Epoch {
                generation: 2,
                n: 2,
            }),
        }
        .encode_to_vec();
        put(w, FrameKind::Barrier, 0, barrier2.clone()).await;
        put(w, FrameKind::Barrier, 1, barrier2).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let patch = frames
            .iter()
            .find(|f| f.kind == FrameKind::Patch && f.stream == 1);
        let Some(p) = patch else {
            panic!("a's held cell isn't patched in");
        };
        let p = ipc::decode_batch(p.payload.clone()).unwrap();
        assert_eq!(p.num_rows(), 1);
        let second = ready(&mut h.events).await;
        assert!(second[0].holds["a"].replayed.is_some());
    }
}
