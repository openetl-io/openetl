//! The dataflow. Each lane reads its inbound edge and normalizes what it
//! carries, raw rows to Arrow, on a task of its own: it parses and types,
//! then names, adds metadata columns and holds. With a SQL transform, the
//! streams it reads are typed, and each of their batches goes through every
//! statement that reads it, and to the output it is if it passes through,
//! before each output's batch is named, given metadata columns and held. A
//! lane hands what it shaped to a writer lane: the one whose key groups hold
//! the lane's (see [`crate::layout`]), or past a stream's exchange, the one
//! owning each row's key group. Each writer lane aligns the lanes
//! feeding it, and writes to a runner sink, with barriers in band, or to a
//! Parquet or staged sink, which stages each epoch and reports it in
//! EpochReady. A group window's statement runs on keyed lanes, past an
//! exchange of its own (see [`keyed`]).

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use openetl_formats::ParquetStaging;
use openetl_normalize::{
    Cause, Error as NormalizeError, NotHeld, Policy, Rejected, Shaped, Shaper, Typed, by_key_group,
    in_key_groups, logical_type, naming::Naming, widens,
};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, CellHolds, ColumnHold, EpochReady, EventsResponse, FrameKind, HoldCause, LaneMetrics,
    Metrics, QuarantineCause, QuarantineFile, ReplayProgress, Rows, SchemaChange, Split,
    SplitWatermarks, StagedStream, StreamHolds, StreamMetrics, events_response, hold_cause,
};
use prost::Message;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinSet;
use tonic::Status;
use tracing::{Instrument as _, Span, debug_span, info_span};

use crate::error::JobError;
use crate::frame;
use crate::ipc;
use crate::mesh::{Links, Room};
use crate::plan::{Exchange, InputPlan, Plan, Sink};
use crate::remote::{Holding, RemoteStream};
use crate::replay::Replayer;
use crate::stage::{Applied, Stage, pushed_rows};
use crate::staging::Staging;
use crate::transform::Route;
use crate::watermark::{self, Senders, Splits};

mod keyed;

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
pub(crate) enum Out {
    Batch(usize, RecordBatch),
    /// An encoded SchemaChange message.
    SchemaChange(usize, Vec<u8>),
    /// Held values of the stream's current epoch.
    Held(usize, RecordBatch),
    Patch(usize, RecordBatch),
    Barrier(usize, Epoch),
    /// The sender's watermark of the stream, which never goes back.
    Watermark(usize, i64),
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
            | Out::Watermark(s, _)
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

/// Stream s's shaper as its owner has it, from `schema`, holding whole the
/// plan's columns, or where the remote owner says, its `holding`.
fn owned_shaper(
    plan: &Plan,
    s: usize,
    schema: Option<SchemaRef>,
    holding: Option<&[ColumnHold]>,
) -> Result<Shaper, JobError> {
    let bad = |e: NormalizeError| JobError::config(format!("stream {}: {e}", plan.streams[s]));
    // The columns the stream added since it was restored that the
    // destination takes: not those held whole that it has no field of, as a
    // column added while held isn't counted where it's added.
    let restored = plan.restored[s].as_ref();
    let added = schema.as_ref().map_or(0, |sch| {
        sch.fields()
            .iter()
            .filter(|f| {
                !f.name().starts_with("_openetl_")
                    && restored.is_none_or(|r| r.field_with_name(f.name()).is_err())
                    && !holding.is_some_and(|h| {
                        h.iter()
                            .any(|c| &c.column == f.name() && c.delivered.is_empty())
                    })
            })
            .count()
    });
    let has = |column: &str| {
        schema
            .as_ref()
            .is_some_and(|sch| sch.field_with_name(column).is_ok())
    };
    let config = match (&plan.holds[s], holding) {
        (Some(h), Some(holding)) => {
            let mut config = h.clone();
            let owned = crate::plan::column_holds(&plan.streams[s], holding)?;
            // A column the plan holds that the owner's version hasn't got
            // is one no batch has carried yet, so its hold goes on; the
            // owner's version names only the holds it has seen. Of a column
            // it has, its word wins: unheld there, it was let go.
            let pending: Vec<_> = h
                .holding
                .iter()
                .filter(|c| !owned.iter().any(|o| o.column == c.column) && !has(&c.column))
                .cloned()
                .collect();
            config.holding = owned;
            config.holding.extend(pending);
            // Columns the stream added since it was restored count toward
            // what the destination's table takes.
            config.limits.columns += added;
            Some(config)
        }
        (h, _) => h.clone(),
    };
    let mut shaper = shaper(plan, s, schema)?;
    if let Some(config) = config {
        shaper = shaper.with_holds(config).map_err(bad)?;
    }
    if let (Some(r), Some(_)) = (&plan.replays[s], &plan.held_url) {
        // Patches go out at the types the destination will have. Once a
        // released column has its version, this only finds it again.
        shaper
            .version(&r.versioned)
            .and_then(|_| shaper.declare())
            .map_err(|e| shape_error(&plan.streams[s], e))?;
    }
    shaper.take_changed();
    Ok(shaper)
}

/// What a change to stream s holds beside its schema, for its remote
/// owner: the columns held whole and the schema the destination gets.
fn holding_of(plan: &Plan, s: usize, shaper: &Shaper) -> Result<Holding, JobError> {
    if plan.holds[s].is_none() {
        return Ok(Holding::default());
    }
    let delivered = match shaper.delivered() {
        Some(d) => ipc::encode_schema(&d).map_err(|e| JobError::internal(e.to_string()))?,
        None => Vec::new(),
    };
    Ok(Holding {
        columns: column_holds(shaper)?,
        delivered,
    })
}

/// The columns a shaper holds whole, as the protocol carries them.
fn column_holds(shaper: &Shaper) -> Result<Vec<ColumnHold>, JobError> {
    let schema = shaper.schema();
    let mut holding = Vec::new();
    for h in shaper.holding() {
        let delivered = match &h.delivered {
            Some(f) => ipc::encode_schema(&Arc::new(arrow_schema::Schema::new(vec![f.clone()])))
                .map_err(|e| JobError::internal(e.to_string()))?,
            None => Vec::new(),
        };
        let cause = cause(&h, schema.as_ref());
        holding.push(ColumnHold {
            column: h.column,
            delivered,
            reason: h.reason,
            cause: Some(cause),
        });
    }
    Ok(holding)
}

impl Owner {
    fn new(plan: &Plan, s: usize) -> Result<Self, JobError> {
        let shaper = owned_shaper(plan, s, plan.restored[s].clone(), None)?;
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

    /// Takes the remote owner's newest schema of stream s, and what it holds,
    /// if the plan's schemas are remote and it's newer than the shaper's:
    /// lanes then shape by it. The schema policy applies as it did.
    fn follow(&self, plan: &Plan, s: usize) -> Result<(), JobError> {
        let Some(r) = plan.remote.as_ref().and_then(|r| r.stream(s)) else {
            return Ok(());
        };
        r.check()?;
        if r.newest() <= self.remote_version.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut o = self.lock();
        let owned = r.latest();
        let v = owned.version;
        if v <= o.remote_version {
            return Ok(());
        }
        let mut sh = owned_shaper(plan, s, owned.schema, Some(&owned.holding))?;
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
    /// Each split's watermark, as its lane passed the barrier.
    marks: BTreeMap<String, i64>,
    /// The stream's schema, once every lane has passed it.
    at: Option<Option<SchemaRef>>,
}

impl InputOwner {
    /// `cells` are the cell holds of the output the stream passes through
    /// as, if it holds cells: its rows are typed holding them.
    fn new(input: &InputPlan, cells: Option<HashMap<String, NotHeld>>) -> Result<Self, JobError> {
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
    /// and a type that holds the stream's replaces it. Where the stream's
    /// schema is remote, it's proposed to the remote owner until the owner's
    /// holds what the engine saw.
    async fn observe(
        &self,
        remote: Option<&RemoteStream>,
        schema: &SchemaRef,
    ) -> Result<(), JobError> {
        loop {
            let (base, wanted) = {
                let mut st = self.lock();
                let seen = match &st.schema {
                    Some(old) => joined(old, schema).unwrap_or_else(|| old.clone()),
                    None => schema.clone(),
                };
                st.schema = Some(seen.clone());
                let Some(r) = remote else { return Ok(()) };
                r.check()?;
                let owned = r.latest();
                let wanted = match &owned.schema {
                    Some(theirs) => match joined(theirs, &seen) {
                        Some(w) => w,
                        None => {
                            st.schema = Some(theirs.clone());
                            return Ok(());
                        }
                    },
                    None => seen,
                };
                (owned.version, wanted)
            };
            if let Some(r) = remote {
                r.propose(base, &wanted, Holding::default()).await?;
            }
        }
    }

    fn cross(&self, lanes: usize, n: u64, q: Option<QuarantineFile>, marks: Option<&Splits>) {
        let mut st = self.lock();
        let schema = st.schema.clone();
        let c = st.crossings.entry(n).or_default();
        c.lanes += 1;
        c.quarantine.extend(q);
        // A split without a watermark is missing.
        for (id, wm) in marks.into_iter().flat_map(Splits::marks) {
            if wm != watermark::NONE {
                c.marks.insert(id.to_owned(), wm);
            }
        }
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

    /// The stream's schema in ready epoch n, what it quarantined in it, and
    /// its splits' watermarks at its barrier.
    fn take(
        &self,
        n: u64,
    ) -> (
        Option<SchemaRef>,
        Vec<QuarantineFile>,
        BTreeMap<String, i64>,
    ) {
        let mut st = self.lock();
        match st.crossings.remove(&n) {
            Some(InputCrossing {
                quarantine,
                marks,
                at: Some(schema),
                ..
            }) => (schema, quarantine, marks),
            // It ended, or feeds no output and keeps its restored schema.
            _ => (
                st.at_end.clone().or(st.schema.clone()),
                Vec::new(),
                BTreeMap::new(),
            ),
        }
    }
}

/// `old` with what `new` adds to it: its new columns, its types that hold
/// `old`'s, and its schema metadata, the columns that held integers past
/// ±2^53 as the union of both's; None if it adds nothing.
fn joined(old: &SchemaRef, new: &SchemaRef) -> Option<SchemaRef> {
    let mut fields: Vec<arrow_schema::Field> = Vec::with_capacity(old.fields().len());
    let mut changed = false;
    for f in old.fields() {
        match new.field_with_name(f.name()) {
            Ok(g) if g.data_type() != f.data_type() && !widens(g, f) => {
                fields.push(g.clone());
                changed = true;
            }
            _ => fields.push(f.as_ref().clone()),
        }
    }
    for g in new.fields() {
        if old.field_with_name(g.name()).is_err() {
            fields.push(g.as_ref().clone());
            changed = true;
        }
    }
    let mut metadata = old.metadata().clone();
    for (k, v) in new.metadata() {
        match metadata.get(k) {
            None => {
                metadata.insert(k.clone(), v.clone());
                changed = true;
            }
            Some(was) if k == openetl_normalize::BIG_INTS_KEY && was != v => {
                let names =
                    |s: &str| serde_json::from_str::<BTreeSet<String>>(s).unwrap_or_default();
                let (was, both) = (
                    names(was),
                    names(was)
                        .union(&names(v))
                        .cloned()
                        .collect::<BTreeSet<_>>(),
                );
                if both != was
                    && let Ok(v) = serde_json::to_string(&both)
                {
                    metadata.insert(k.clone(), v);
                    changed = true;
                }
            }
            Some(_) => {}
        }
    }
    changed.then(|| Arc::new(arrow_schema::Schema::new_with_metadata(fields, metadata)))
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
    /// Each input's watermark on each lane, and each stream's on each writer
    /// lane, NONE before it has one: for metrics.
    input_marks: Vec<Vec<AtomicI64>>,
    marks: Vec<Vec<AtomicI64>>,
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
        let load = |name: &str, c: &Counters, marks: &[AtomicI64]| StreamMetrics {
            stream: name.to_owned(),
            rows_in: c.rows_in.load(Ordering::Relaxed),
            bytes_in: c.bytes_in.load(Ordering::Relaxed),
            rows_out: c.rows_out.load(Ordering::Relaxed),
            bytes_out: c.bytes_out.load(Ordering::Relaxed),
            rows_quarantined: c.rows_quarantined.load(Ordering::Relaxed),
            cells_held: c.cells_held.load(Ordering::Relaxed),
            watermark: marks
                .iter()
                .map(|m| m.load(Ordering::Relaxed))
                .min()
                .filter(|wm| *wm != watermark::NONE),
        };
        let mut streams: Vec<StreamMetrics> = self
            .plan
            .streams
            .iter()
            .zip(&self.counters)
            .zip(&self.marks)
            .map(|((name, c), marks)| load(name, c, marks))
            .collect();
        if let Some(t) = &self.plan.transform {
            for ((input, c), marks) in t
                .inputs
                .iter()
                .zip(&self.input_counters)
                .zip(&self.input_marks)
            {
                let marks = match input.event_time {
                    Some(_) => marks.as_slice(),
                    None => &[],
                };
                let m = load(&input.name, c, marks);
                match streams.iter_mut().find(|s| s.stream == m.stream) {
                    Some(s) => {
                        s.rows_in += m.rows_in;
                        s.bytes_in += m.bytes_in;
                        s.rows_quarantined += m.rows_quarantined;
                        // As the stream reads, not as its output writes.
                        if m.watermark.is_some() {
                            s.watermark = m.watermark;
                        }
                    }
                    None => streams.push(m),
                }
            }
        }
        EventsResponse {
            event: Some(events_response::Event::Metrics(Metrics {
                streams,
                memory_bytes: crate::memory::held(),
                lanes: self
                    .lanes
                    .iter()
                    .take(self.plan.inbound.len())
                    .map(|l| l.metrics(self.start))
                    .collect(),
            })),
        }
    }
}

type Task = Pin<Box<dyn Future<Output = Result<bool, JobError>> + Send>>;

/// Runs the job until every stream has ended at the sink and every inbound
/// edge has closed. `inbound` has an edge per lane, in order, and `outbound`
/// the runner sink's, per writer lane. With a mesh, the engine also listens
/// for the others, and its exchanges reach their writer lanes. Failures are
/// reported on `events` before the edges close.
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
    let listener = match crate::mesh::bind(&plan).await {
        Ok(l) => l,
        Err(e) => {
            report(&events, &e).await;
            return Err(e);
        }
    };
    run_with(plan, inbound, outbound, events, listener).await
}

/// As run, with a mesh's listener already bound.
pub async fn run_with<R, W>(
    plan: Arc<Plan>,
    inbound: Vec<R>,
    outbound: Vec<W>,
    events: Events,
    listener: Option<TcpListener>,
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
    let windowed = plan
        .transform
        .as_ref()
        .is_some_and(|t| t.sql.statements.iter().any(|s| s.window.is_some()));
    // Keyed lanes have clocks of their own, after the lanes'.
    let clocks = match windowed {
        true => 2 * inbound.len(),
        false => inbound.len(),
    };
    let none = |n: usize| (0..n).map(|_| AtomicI64::new(watermark::NONE)).collect();
    let flow = Arc::new(Flow {
        counters: plan.streams.iter().map(|_| Counters::default()).collect(),
        input_counters: inputs.iter().map(|_| Counters::default()).collect(),
        input_marks: inputs.iter().map(|_| none(inbound.len())).collect(),
        marks: plan
            .streams
            .iter()
            .map(|_| none(plan.writer_lanes))
            .collect(),
        inputs,
        lanes: (0..clocks).map(|_| LaneClock::new()).collect(),
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
    let mesh = Links::start(&plan, &mut readers).and_then(|m| match (&m, listener) {
        (Some(_), Some(l)) => {
            readers.spawn(crate::mesh::inbound(plan.clone(), l, senders.clone()).in_current_span());
            Ok(m)
        }
        (None, None) => Ok(m),
        _ => Err(JobError::internal(
            "a mesh's links and listener go together",
        )),
    });
    let mesh = match mesh {
        Ok(m) => m,
        Err(e) => {
            readers.abort_all();
            report(&events, &e).await;
            return Err(e);
        }
    };
    let mut keyed = Vec::new();
    if windowed {
        for lane in 0..inbound.len() {
            let (tx, rx) = mpsc::channel(16);
            keyed.push(tx);
            let k = Lane {
                flow: flow.clone(),
                lane,
                clock: inbound.len() + lane,
                tx: senders.clone(),
                keyed: Vec::new(),
                mesh: None,
            };
            readers.spawn(keyed::run(flow.clone(), k, rx).in_current_span());
        }
    }
    for (lane, r) in inbound.into_iter().enumerate() {
        let lane = Lane {
            flow: flow.clone(),
            lane,
            clock: lane,
            tx: senders.clone(),
            keyed: keyed.clone(),
            mesh: mesh.clone(),
        };
        readers.spawn(read_edge(lane, r).in_current_span());
    }
    drop(keyed);
    drop(senders);
    drop(mesh);

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
    /// Quarantined since its last barrier.
    quarantined: Quarantined,
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
    /// schema, to what its metadata knows of the values, such as a column
    /// that held integers past ±2^53, which a later float in it, here or in
    /// another engine, turns to text, or to the columns held whole, is
    /// proposed to the remote owner from the version the owner follows, and
    /// once a newer version comes, the batch is shaped again by it. A change
    /// that leaves those as they are is published here. `fail` says why a
    /// batch couldn't be shaped.
    async fn shape_own(
        &mut self,
        plan: &Plan,
        owner: &Owner,
        f: impl Fn(&mut Shaper) -> Result<Typed, NormalizeError>,
        fail: impl Fn(&Shaper, NormalizeError) -> JobError,
    ) -> Result<Shaped, JobError> {
        let Some(remote) = plan.remote.as_ref().and_then(|r| r.stream(self.stream)) else {
            return self.shape(owner, f).map_err(|e| fail(&self.shaper, e));
        };
        loop {
            owner.follow(plan, self.stream)?;
            self.catch_up(owner);
            let out = f(&mut self.shaper).and_then(|t| self.shaper.shape(t));
            let changed = self.shaper.take_changed();
            if out.is_err() {
                // The owner may have taken a version since follow, which the
                // batch fits: it's shaped again by that.
                let behind = remote.newest() > owner.remote_version.load(Ordering::Acquire);
                if owner.version.load(Ordering::Acquire) == self.version && !behind {
                    return out.map_err(|e| fail(&self.shaper, e));
                }
                continue;
            }
            if !changed {
                return out.map_err(|e| fail(&self.shaper, e));
            }
            let schema = self.shaper.schema();
            let base = {
                let mut o = owner.lock();
                if o.version != self.version {
                    continue;
                }
                if o.shaper.schema() == schema && o.shaper.holding() == self.shaper.holding() {
                    o.version += 1;
                    self.version = o.version;
                    o.shaper = self.shaper.fork();
                    owner.version.store(o.version, Ordering::Release);
                    return out.map_err(|e| fail(&self.shaper, e));
                }
                o.remote_version
            };
            drop(out);
            if let Some(schema) = schema {
                let holding = holding_of(plan, self.stream, &self.shaper)?;
                remote.propose(base, &schema, holding).await?;
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
    /// Quarantined since its last barrier.
    quarantined: Quarantined,
    /// The schema the lane last told the stream's owner of.
    seen: Option<SchemaRef>,
    /// The outputs it feeds.
    outputs: Vec<(Stage, LaneStream)>,
    /// The group windows' statements that read it, which keyed lanes run.
    windows: Vec<usize>,
    /// Its splits on the lane, where it has event time.
    marks: Option<Splits>,
}

impl Transformed {
    /// It feeds nothing, so its rows are read and dropped.
    fn feeds_nothing(&self) -> bool {
        self.outputs.is_empty() && self.windows.is_empty()
    }
}

/// A lane, with a sender to each of the engine's writer lanes, and where a
/// mesh has them, links to the others', and to each keyed lane. It's the
/// attempt's lane `first_lane + lane` to every writer lane.
struct Lane {
    flow: Arc<Flow>,
    lane: usize,
    /// Its clock, among the job's.
    clock: usize,
    tx: Vec<mpsc::Sender<(usize, Out)>>,
    keyed: Vec<mpsc::Sender<(usize, keyed::In)>>,
    mesh: Option<Arc<Links>>,
}

impl Lane {
    fn enter(&self, doing: Doing) {
        self.flow.lanes[self.clock].enter(self.flow.start, doing);
    }

    /// Sends to the engine's writer lane w.
    async fn send_to(&self, w: usize, out: Out) -> Result<(), JobError> {
        let stopped = |_| JobError::stopped("the outbound edge stopped");
        let from = self.flow.plan.first_lane + self.lane;
        let out = match self.tx[w].try_send((from, out)) {
            Ok(()) => return Ok(()),
            Err(mpsc::error::TrySendError::Full(out)) => out,
            Err(mpsc::error::TrySendError::Closed(_)) => return Err(stopped(())),
        };
        self.enter(Doing::Backpressured);
        let sent = self.tx[w].send(out).await.map_err(|_| stopped(()));
        self.enter(Doing::Busy);
        sent
    }

    /// Sends to writer lane w of those an exchange routes to: the engine's,
    /// or with a mesh, the attempt's, which may be another engine's.
    async fn route(&self, w: usize, out: Out) -> Result<(), JobError> {
        let Some(mesh) = &self.mesh else {
            return self.send_to(w, out).await;
        };
        if let Some(local) = mesh.local(w) {
            return self.send_to(local, out).await;
        }
        let frame = crate::mesh::wire(out)?;
        let room = match mesh.room(w, frame.2.len())? {
            Room::Ready(r) => r,
            wait => {
                self.enter(Doing::Backpressured);
                let r = wait.wait().await;
                self.enter(Doing::Busy);
                r.ok_or_else(|| JobError::stopped("the link stopped"))?
            }
        };
        mesh.push(self.flow.plan.first_lane + self.lane, w, frame, room)
    }

    /// Sends to the lane's own writer lane.
    async fn send(&self, out: Out) -> Result<(), JobError> {
        self.send_to(self.flow.plan.layout.writer_of(self.lane), out)
            .await
    }

    /// The stream's exchange before the writer, where there's more than one
    /// writer lane: one merges every lane without it.
    fn exchange(&self, stream: usize) -> Option<&Exchange> {
        self.flow.plan.exchanges[stream]
            .as_ref()
            .filter(|_| self.flow.plan.routed_writers() > 1)
    }

    /// Sends a batch's rows to their writer lanes: those the stream keeps.
    /// `key` is the stream's key columns, as the batch names them, where it
    /// keeps some key groups or has an exchange.
    async fn rows(
        &self,
        stream: usize,
        key: &[String],
        batch: RecordBatch,
    ) -> Result<(), JobError> {
        let batch = match self.flow.plan.keep[stream] {
            None => batch,
            Some((first, last)) => {
                match in_key_groups(&batch, key, self.flow.plan.key_groups, first..=last).map_err(
                    |e| shape_error(&self.flow.plan.streams[stream], NormalizeError::Arrow(e)),
                )? {
                    Some(b) => b,
                    None => return Ok(()),
                }
            }
        };
        let Some(exchange) = self.exchange(stream) else {
            return self.send(Out::Batch(stream, batch)).await;
        };
        // Writer lane w owns the w-th of the writer lanes' ranges of key
        // groups, as the layout has it.
        let by = exchange.columns(key);
        let plan = &self.flow.plan;
        let parts = by_key_group(&batch, &by, plan.key_groups, plan.routed_writers())
            .map_err(|e| shape_error(&plan.streams[stream], NormalizeError::Arrow(e)))?;
        for (w, part) in parts.into_iter().enumerate() {
            if let Some(part) = part {
                self.route(w, Out::Batch(stream, part)).await?;
            }
        }
        Ok(())
    }

    /// Sends a schema change, a barrier or an end to every writer lane the
    /// stream's rows on this lane can reach.
    async fn all(&self, stream: usize, out: impl Fn() -> Out) -> Result<(), JobError> {
        if self.exchange(stream).is_none() {
            return self.send(out()).await;
        }
        for w in 0..self.flow.plan.routed_writers() {
            self.route(w, out()).await?;
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
    // The attempt's lane 0 replays a stream's released holds.
    let lane_stream = |s: usize| -> Result<LaneStream, JobError> {
        let replay = match (
            plan.first_lane + lane.lane,
            &plan.replays[s],
            &plan.held_url,
        ) {
            (0, Some(r), Some(url)) => Some(Replayer::open(url, r.clone())?),
            _ => None,
        };
        let o = flow.owners[s].lock();
        Ok(LaneStream {
            stream: s,
            shaper: o.shaper.fork(),
            version: o.version,
            quarantined: Quarantined::default(),
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
                quarantined: Quarantined::default(),
                seen: None,
                outputs: t.routes[s]
                    .iter()
                    .filter_map(|&(o, route)| {
                        let stage = match route {
                            Route::Pass => Stage::Identity,
                            Route::Statement(i) => Stage::statement(&t.sql, i),
                            Route::Window(_) => return None,
                        };
                        #[cfg(test)]
                        let stage = match (&plan.probe, stage) {
                            (Some(p), Stage::Identity) => Stage::Probe(p.clone()),
                            (_, stage) => stage,
                        };
                        Some(lane_stream(o).map(|ls| (stage, ls)))
                    })
                    .collect::<Result<_, JobError>>()?,
                windows: t.routes[s]
                    .iter()
                    .filter_map(|&(_, route)| match route {
                        Route::Window(i) => Some(i),
                        _ => None,
                    })
                    .collect(),
                marks: t.inputs[s].event_time.as_ref().map(|_| Splits::default()),
            })),
        };
        streams.push(LaneInput {
            next_epoch: plan.first_epoch,
            ended: false,
            kind,
        });
    }
    let protocol = |msg: String| JobError::internal(format!("edge {id}: {msg}"));
    let mut outs = OutputMarks::new(plan);

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
                            .shape_own(
                                plan,
                                owner,
                                |sh| sh.type_rows(rows.rows.iter().map(Vec::as_slice)),
                                |_, e| shape_error(name, e),
                            )
                            .instrument(span)
                            .await?;
                        hold(plan, name, &n.rejected, record, &mut ls.quarantined, c)?;
                        shaped(&lane, ls, n, c).await?;
                    }
                    // A stream that feeds no output is read, and dropped.
                    Kind::Transformed(t) if t.feeds_nothing() => {}
                    Kind::Transformed(t) => {
                        let typed = span
                            .in_scope(|| t.typer.type_rows(rows.rows.iter().map(Vec::as_slice)))
                            .map_err(|e| shape_error(name, e))?;
                        let latest = latest(plan, t, &typed)?;
                        if let Some(wm) = t.marks.as_mut().and_then(Splits::settle) {
                            advance(&lane, t, wm, &mut outs).await?;
                        }
                        transform(&lane, t, typed, &record, c).await?;
                        timed(&lane, t, latest, &mut outs).await?;
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
                            .shape_own(
                                plan,
                                owner,
                                |sh| sh.type_batch(&batch),
                                |_, e| shape_error(name, e),
                            )
                            .instrument(span)
                            .await?;
                        hold(plan, name, &n.rejected, record, &mut ls.quarantined, c)?;
                        shaped(&lane, ls, n, c).await?;
                    }
                    Kind::Transformed(t) if t.feeds_nothing() => {}
                    Kind::Transformed(t) => {
                        let typed = span
                            .in_scope(|| t.typer.type_batch(&batch))
                            .map_err(|e| shape_error(name, e))?;
                        let latest = latest(plan, t, &typed)?;
                        if let Some(wm) = t.marks.as_mut().and_then(Splits::settle) {
                            advance(&lane, t, wm, &mut outs).await?;
                        }
                        transform(&lane, t, typed, &record, c).await?;
                        timed(&lane, t, latest, &mut outs).await?;
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
                    Kind::Transformed(t) if t.feeds_nothing() => {}
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
                        if let Some(wm) = t.marks.as_mut().and_then(Splits::barrier) {
                            advance(&lane, t, wm, &mut outs).await?;
                        }
                        flow.inputs[input].cross(plan.inbound.len(), epoch.n, q, t.marks.as_ref());
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
                        keyed::all(&lane, &t.windows, |i| keyed::In::Barrier(i, epoch)).await?;
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
                    Kind::Transformed(t) if t.feeds_nothing() => {}
                    Kind::Transformed(t) => {
                        flow.inputs[input].end(plan.inbound.len());
                        let outputs: Vec<usize> =
                            t.outputs.iter().map(|(_, ls)| ls.stream).collect();
                        let windows = t.windows.clone();
                        for s in outputs {
                            flow.owners[s].end(plan, s)?;
                            lane.all(s, || Out::End(s)).await?;
                        }
                        keyed::all(&lane, &windows, keyed::In::End).await?;
                    }
                }
            }
            FrameKind::Split => {
                let sp =
                    Split::decode(f.payload.as_slice()).map_err(|e| protocol(e.to_string()))?;
                let marks = match &mut li.kind {
                    Kind::Transformed(t) => t.marks.as_mut(),
                    Kind::Own(_) => None,
                };
                let Some(marks) = marks else {
                    return Err(protocol(format!(
                        "a split of stream {name}, which has no event time"
                    )));
                };
                if let Some(wm) = marks.split(&sp.id, sp.watermark)
                    && let Kind::Transformed(t) = &mut li.kind
                    && !t.feeds_nothing()
                {
                    advance(&lane, t, wm, &mut outs).await?;
                }
            }
            k @ (FrameKind::Hello
            | FrameKind::Unspecified
            | FrameKind::Patch
            | FrameKind::Watermark
            | FrameKind::MeshHello
            | FrameKind::Route) => {
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
    held: &mut Quarantined,
    output: bool,
) -> Result<Option<QuarantineFile>, JobError> {
    let Quarantined {
        lines,
        rows,
        causes,
    } = std::mem::take(held);
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
        causes: causes
            .into_iter()
            .map(|((code, column, not_held), rows)| QuarantineCause {
                code: code.to_owned(),
                column,
                not_held: not_held.to_owned(),
                rows,
            })
            .collect(),
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
        let remote = plan.remote.as_ref().and_then(|r| r.input(t.input));
        flow.inputs[t.input].observe(remote, &schema).await?;
        t.seen = Some(schema);
    }
    let pushed = pushed_rows(&typed);
    let cursor = input.identity.cursor.as_deref();
    for (stage, ls) in &mut t.outputs {
        let applied = stage.apply(&typed, &pushed, cursor)?;
        output(lane, stage, ls, applied, record).await?;
    }
    keyed::rows(lane, &t.windows, &typed).await
}

/// Takes what a stage gave its output: what it couldn't evaluate is
/// quarantined, and the rest named, given metadata columns and held.
async fn output(
    lane: &Lane,
    stage: &Stage,
    ls: &mut LaneStream,
    applied: Applied,
    record: &(dyn Fn(usize) -> String + Sync),
) -> Result<(), JobError> {
    let flow = &lane.flow;
    let plan = &flow.plan;
    let s = ls.stream;
    let name = &plan.streams[s];
    let oc = &flow.counters[s];
    let Applied { typed: out, failed } = applied;
    let failed: Vec<Rejected> = failed
        .into_iter()
        .map(|(row, reason, cause)| Rejected { row, reason, cause })
        .collect();
    hold(plan, name, &failed, record, &mut ls.quarantined, oc)?;
    let planned = stage.planned();
    let mut n = ls
        .shape_own(
            plan,
            &flow.owners[s],
            |_| Ok(copy(&out)),
            |shaper, e| {
                let drift = matches!(e, NormalizeError::Drift(_));
                let err = shape_error(name, e);
                match planned {
                    Some(st) if drift => {
                        statement_drift(st, &shaper.live(), &out.batch.schema(), err)
                    }
                    _ => err,
                }
            },
        )
        .await?;
    // A delete whose values don't fit the output goes again with its key
    // alone, after the batch: its seq orders it among its key's. Where the
    // schema is remote, the owner may have a newer version by then, which
    // shapes it.
    let (rejected, keys) = stage.deletes_by_key(&out, std::mem::take(&mut n.rejected))?;
    hold(plan, name, &rejected, record, &mut ls.quarantined, oc)?;
    shaped(lane, ls, n, oc).await?;
    if let Some(keys) = keys {
        let n = ls
            .shape_own(
                plan,
                &flow.owners[s],
                |_| Ok(copy(&keys)),
                |_, e| shape_error(name, e),
            )
            .await?;
        hold(plan, name, &n.rejected, record, &mut ls.quarantined, oc)?;
        shaped(lane, ls, n, oc).await?;
    }
    Ok(())
}

/// The latest event time of a typed batch of a stream a transform reads:
/// None where the stream has none, or is held, or the batch has none.
fn latest(plan: &Plan, t: &Transformed, typed: &Typed) -> Result<Option<i64>, JobError> {
    let Some(job) = &plan.transform else {
        return Ok(None);
    };
    let input = &job.inputs[t.input];
    let Some(et) = &input.event_time else {
        return Ok(None);
    };
    // A held stream's column is a time too, though its rows move nothing.
    let latest = watermark::latest(&typed.batch, &et.column)
        .map_err(|e| JobError::config(format!("stream {}: {e}", input.name)))?;
    Ok(latest.filter(|_| !et.held))
}

/// The lane read a batch of the stream whose latest event time is `latest`.
async fn timed(
    lane: &Lane,
    t: &mut Transformed,
    latest: Option<i64>,
    outs: &mut OutputMarks,
) -> Result<(), JobError> {
    let (Some(latest), Some(job)) = (latest, &lane.flow.plan.transform) else {
        return Ok(());
    };
    let delay = job.inputs[t.input]
        .event_time
        .as_ref()
        .map_or(0, |et| et.delay);
    if let Some(wm) = t.marks.as_mut().and_then(|m| m.read(latest, delay)) {
        advance(lane, t, wm, outs).await?;
    }
    Ok(())
}

/// Each output's watermark on a lane: the least of the inputs it's written
/// from, for outputs whose every input has event time.
struct OutputMarks(HashMap<usize, (Vec<usize>, Senders)>);

impl OutputMarks {
    fn new(plan: &Plan) -> Self {
        let mut outs = HashMap::new();
        if let Some(t) = &plan.transform {
            for (o, floor) in t.floors.iter().enumerate() {
                if floor.is_none() {
                    continue;
                }
                let from: Vec<usize> = (0..t.inputs.len())
                    .filter(|&i| t.routes[i].iter().any(|&(to, _)| to == o))
                    .collect();
                let marks = Senders::new(from.len(), watermark::NONE);
                outs.insert(o, (from, marks));
            }
        }
        Self(outs)
    }

    /// Input `input`'s watermark on the lane is `wm`: output `o`'s, if that
    /// advances it.
    fn take(&mut self, o: usize, input: usize, wm: i64) -> Result<Option<i64>, JobError> {
        let Some((from, marks)) = self.0.get_mut(&o) else {
            return Ok(None);
        };
        let Some(i) = from.iter().position(|&f| f == input) else {
            return Ok(None);
        };
        marks
            .take(i, wm)
            .map_err(|e| JobError::internal(format!("output {o}'s inputs on a lane: {e}")))
    }
}

/// The stream's watermark on the lane advanced to `wm`: each output it
/// feeds is told, in order with its batches, and an output whose watermark
/// that advances sends it on to every writer lane it reaches. Each keyed
/// lane of a group window over it is told, after the rows that moved it.
async fn advance(
    lane: &Lane,
    t: &mut Transformed,
    wm: i64,
    outs: &mut OutputMarks,
) -> Result<(), JobError> {
    lane.flow.input_marks[t.input][lane.lane].store(wm, Ordering::Relaxed);
    keyed::all(lane, &t.windows, |i| keyed::In::Watermark(i, wm)).await?;
    for (stage, ls) in &mut t.outputs {
        if let Some(applied) = stage.watermark(wm)? {
            let batch = applied.typed.batch.clone();
            let record = move |row: usize| openetl_normalize::render_row(&batch, row);
            output(lane, stage, ls, applied, &record).await?;
        }
        let s = ls.stream;
        if let Some(w) = outs.take(s, t.input, wm)? {
            lane.all(s, || Out::Watermark(s, w)).await?;
        }
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

/// What a stream quarantined on a lane since its last barrier: its lines,
/// how many, and how many by cause.
#[derive(Default)]
pub(crate) struct Quarantined {
    lines: Vec<u8>,
    rows: u64,
    /// By code, column and why not held; "" for none.
    causes: BTreeMap<(&'static str, String, &'static str), u64>,
}

impl Quarantined {
    /// Adds a record's line. Only `record` holds its values.
    fn add(&mut self, stream: &str, r: &Rejected, record: String) {
        let Cause {
            code,
            column,
            not_held,
        } = &r.cause;
        let mut line = serde_json::Map::new();
        line.insert("stream".into(), stream.into());
        line.insert("code".into(), code.as_str().into());
        if let Some(c) = column {
            line.insert("column".into(), c.as_str().into());
        }
        if let Some(n) = not_held {
            line.insert("notHeld".into(), n.as_str().into());
        }
        line.insert("reason".into(), r.reason.as_str().into());
        line.insert("record".into(), record.into());
        self.lines
            .extend_from_slice(serde_json::Value::Object(line).to_string().as_bytes());
        self.lines.push(b'\n');
        self.rows += 1;
        let key = (
            code.as_str(),
            column.clone().unwrap_or_default(),
            not_held.map_or("", |n| n.as_str()),
        );
        *self.causes.entry(key).or_default() += 1;
    }
}

/// Adds rejected rows to the stream's quarantine for the epoch, or fails the
/// job if it has no quarantine.
fn hold(
    plan: &Plan,
    stream: &str,
    rejected: &[Rejected],
    record: impl Fn(usize) -> String,
    held: &mut Quarantined,
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
    for r in rejected {
        held.add(stream, r, record(r.row));
    }
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
        let key = match lane.exchange(stream).is_some() || lane.flow.plan.keep[stream].is_some() {
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
        at.holds = Some(StreamHolds {
            holding: column_holds(shaper)?,
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
    for (file, b) in r.chunk().await? {
        let (patch, still) = ls
            .shaper
            .patch(&b, &r.columns(file, &b))
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
    /// Its watermark, the least of its feeders'; None where it has none.
    marks: Option<Senders>,
}

/// One writer lane's input: what its lanes send, aligned on each stream's
/// barriers. It never stops reading to align, so what a lane sends past a
/// barrier can't hold up the lanes that haven't reached it: it waits in memory,
/// under the job's limit. Lanes are the attempt's, other engines' too.
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
        let streams = (0..plan.streams.len())
            .map(|s| {
                let feeders: Vec<usize> =
                    match plan.exchanges[s].is_some() && plan.routed_writers() > 1 {
                        true => plan.senders().collect(),
                        false => plan
                            .layout
                            .feeders(writer)
                            .map(|l| plan.first_lane + l)
                            .collect(),
                    };
                let floor = plan.transform.as_ref().and_then(|t| t.floors[s]);
                StreamFeed {
                    standing: vec![Standing::Open; feeders.len()],
                    waiting: feeders.iter().map(|_| VecDeque::new()).collect(),
                    marks: floor.map(|f| Senders::new(feeders.len(), f)),
                    feeders,
                }
            })
            .collect::<Vec<StreamFeed>>();
        // A stream restored with a watermark starts at it.
        let ready = streams
            .iter()
            .enumerate()
            .filter_map(|(s, st)| {
                let wm = st.marks.as_ref()?.watermark();
                (wm != watermark::NONE).then_some(Out::Watermark(s, wm))
            })
            .collect();
        Self { rx, streams, ready }
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
            return match out {
                Out::Watermark(..) => Self::mark(st, 0, out, &mut self.ready),
                out => {
                    self.ready.push_back(out);
                    Ok(())
                }
            };
        }
        let Some(i) = st.feeders.iter().position(|f| *f == lane) else {
            return Err(JobError::internal(format!(
                "lane {lane} sent stream {s} to a writer lane it doesn't feed"
            )));
        };
        if st.standing[i] == Standing::Open {
            Self::stand(st, i, out, &mut self.ready)?;
        } else {
            st.waiting[i].push_back(out);
        }
        self.align(s)
    }

    /// Takes what lane i sent while it's open.
    fn stand(
        st: &mut StreamFeed,
        i: usize,
        out: Out,
        ready: &mut VecDeque<Out>,
    ) -> Result<(), JobError> {
        match out {
            Out::Barrier(_, e) => st.standing[i] = Standing::AtBarrier(e),
            Out::End(_) => st.standing[i] = Standing::Ended,
            out @ Out::Watermark(..) => Self::mark(st, i, out, ready)?,
            out => ready.push_back(out),
        }
        Ok(())
    }

    /// Takes feeder i's watermark, and passes the stream's on if it
    /// advanced.
    fn mark(
        st: &mut StreamFeed,
        i: usize,
        out: Out,
        ready: &mut VecDeque<Out>,
    ) -> Result<(), JobError> {
        let (Out::Watermark(s, wm), Some(marks)) = (out, &mut st.marks) else {
            return Ok(());
        };
        if let Some(w) = marks
            .take(i, wm)
            .map_err(|e| JobError::internal(format!("stream {s} at a writer lane: {e}")))?
        {
            ready.push_back(Out::Watermark(s, w));
        }
        Ok(())
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
                    Self::stand(st, i, out, &mut self.ready)?;
                }
            }
        }
    }
}

/// What a writer lane staged in an epoch, and what keyed lanes kept.
#[derive(Default)]
struct Pieces {
    /// By writer lane.
    staged: BTreeMap<usize, Vec<u8>>,
    files: Vec<StagedStream>,
    held: Vec<StagedStream>,
    /// Each keyed lane's state, of each of its operators.
    snapshots: Vec<openetl_proto::engine::v1::StateSnapshot>,
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
            snapshots,
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
        let mut watermarks = HashMap::new();
        if let Some(t) = &plan.transform {
            for (input, owner) in t.inputs.iter().zip(&self.inputs) {
                let (schema, q, marks) = owner.take(n);
                quarantine.extend(q);
                if let Some(sc) = &schema {
                    schemas.insert(input.name.clone(), encode(sc)?);
                }
                if !marks.is_empty() {
                    let splits = marks.into_iter().collect();
                    watermarks.insert(input.name.clone(), SplitWatermarks { splits });
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
                snapshots,
                schemas,
                quarantine,
                files,
                holds,
                delivered,
                output_schemas,
                watermarks,
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
            plan.first_writer_lane + writer,
            plan.attempt_writer_lanes,
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
            Out::Watermark(s, wm) => {
                flow.marks[s][writer].store(wm, Ordering::Relaxed);
                continue;
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
            Out::Watermark(s, wm) => {
                flow.marks[s][writer].store(wm, Ordering::Relaxed);
                continue;
            }
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
            Out::Watermark(s, wm) => {
                flow.marks[s][writer].store(wm, Ordering::Relaxed);
                continue;
            }
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
            Out::Watermark(s, wm) => {
                flow.marks[s][writer].store(wm, Ordering::Relaxed);
                continue;
            }
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
pub(crate) mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_schema::DataType;
    use openetl_normalize::{Normalizer, naming};
    use tokio::io::{BufReader, DuplexStream, duplex};

    use crate::plan::tests::request;

    mod event_time;

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

    /// As start_with; where the request's schemas are remote, with an owner
    /// outside the engine that takes each proposal made from the version it
    /// holds, and answers one from an older version with that version.
    fn start_owned(req: openetl_proto::engine::v1::StartJobRequest) -> Harness {
        let remote = req.remote_schemas;
        let plan = Arc::new(Plan::from_request(req).unwrap());
        if remote {
            let owner = plan.remote.clone().unwrap();
            let mut proposals = owner.take_proposals().unwrap();
            tokio::spawn(async move {
                let mut held: HashMap<(bool, u32), openetl_proto::engine::v1::SchemasRequest> =
                    HashMap::new();
                while let Some(Ok(p)) = proposals.recv().await {
                    let v = held.entry((p.input, p.stream)).or_default();
                    if p.base == v.version {
                        *v = openetl_proto::engine::v1::SchemasRequest {
                            stream: p.stream,
                            input: p.input,
                            version: p.base + 1,
                            arrow_schema: p.arrow_schema,
                            holding: p.holding,
                        };
                    }
                    owner.update(v).unwrap();
                }
            });
        }
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
        req.streams[0].exchange = crate::plan::tests::exchange_by(&["id"]);
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

    /// A redo of writer lane 1 of two, on one lane: the rows of its key
    /// groups pass, as routing by key group gives them to it.
    #[tokio::test]
    async fn a_kept_range_of_key_groups_passes_alone() {
        async fn ids(keep: Option<(u32, u32)>, writers: u32) -> Vec<Vec<i64>> {
            let all: Vec<String> = (0..200).map(|i| format!(r#"{{"id": {i}}}"#)).collect();
            let all: Vec<&str> = all.iter().map(String::as_str).collect();
            let mut req = crate::plan::tests::lanes_request(&["a"], writers, writers);
            req.streams[0].primary_key = vec!["id".into()];
            if writers > 1 {
                req.streams[0].exchange = crate::plan::tests::exchange_by(&["id"]);
            }
            req.streams[0].key_groups =
                keep.map(|(first, last)| openetl_proto::engine::v1::KeyGroupRange { first, last });
            let mut h = start_lanes(req);
            put(&mut h.ins[0], FrameKind::Rows, 0, rows(&all)).await;
            for w in &mut h.ins {
                put(w, FrameKind::Barrier, 0, barrier(1)).await;
            }
            h.end_all(1).await;
            let (out, _) = h.finish().await;
            let mut ids: Vec<Vec<i64>> = out
                .iter()
                .map(|frames| {
                    frames
                        .iter()
                        .filter(|f| f.kind == FrameKind::Batch)
                        .flat_map(|f| int_column(f, "id"))
                        .collect()
                })
                .collect();
            for w in &mut ids {
                w.sort_unstable();
            }
            ids
        }
        let routed = ids(None, 2).await;
        let kept = ids(Some((128, 255)), 1).await;
        assert!(!routed[1].is_empty() && routed[1].len() < 200);
        assert_eq!(kept, [routed[1].clone()]);
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

    // A column that held integers past ±2^53 on one lane changes no type,
    // but the stream's owner learns of it, so a float in it on another lane
    // makes it text, as on one lane.
    #[tokio::test]
    async fn lanes_share_what_they_saw_of_big_integers() {
        let mut h = start_lanes(crate::plan::tests::lanes_request(&["a"], 2, 2));
        let column = |f: &frame::Frame| {
            let b = ipc::decode_batch(f.payload.clone()).unwrap();
            b.schema().field_with_name("x").unwrap().data_type().clone()
        };
        let next = async |r: &mut BufReader<DuplexStream>| loop {
            let f = frame::read(r).await.unwrap().unwrap();
            if f.kind == FrameKind::Batch {
                break column(&f);
            }
        };
        for (lane, row) in [
            (0, r#"{"x": 1}"#),
            (0, r#"{"x": 9007199254740993}"#),
            (1, r#"{"x": 1.5}"#),
        ] {
            put(&mut h.ins[lane], FrameKind::Rows, 0, rows(&[row])).await;
            let t = next(&mut h.outs[lane]).await;
            if lane == 1 {
                assert_eq!(t, DataType::Utf8, "a float after big integers");
            }
        }
        for w in &mut h.ins {
            put(w, FrameKind::Barrier, 0, barrier(1)).await;
        }
        h.end_all(1).await;
        let (_, ready) = h.finish().await;
        assert_eq!(schema_types(&ready[0])[0].1[0], DataType::Utf8);
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

    /// A request for stream a, keyed by id, whose plan holds x, a column
    /// the destination refused, which no batch has carried yet.
    fn pending_hold_request(dir: &std::path::Path) -> openetl_proto::engine::v1::StartJobRequest {
        use openetl_proto::engine::v1::{ColumnHold as Hold, HoldCause, HoldPlan, hold_cause};
        let mut req = request(&["a"]);
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].holds = Some(HoldPlan {
            holding: vec![Hold {
                column: "x".into(),
                reason: "the destination refused it".into(),
                cause: Some(HoldCause {
                    kind: hold_cause::Kind::Refused as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        });
        req.held_url = format!("file://{}", dir.display());
        req
    }

    // Where the schema is remote, a hold the plan gives a column no batch
    // has carried yet outlives the owner's versions that don't name it: the
    // engine follows its own change, then holds x as it first comes.
    #[tokio::test]
    async fn a_pending_hold_outlives_the_owners_versions() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = pending_hold_request(dir.path());
        req.remote_schemas = true;
        let mut h = start_owned(req);
        let w = &mut h.to_engine;
        put(w, FrameKind::Rows, 0, rows(&[r#"{"id": 1, "a": 1}"#])).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"id": 2, "a": 2, "x": 5}"#]),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        let b = batches(&frames, 0);
        let last = b.last().unwrap();
        assert!(
            last.schema().field_with_name("x").is_err(),
            "x was delivered: {:?}",
            last.schema()
        );
        let held = last
            .column_by_name(openetl_normalize::hold::HELD_COLUMN)
            .unwrap();
        assert!(
            openetl_normalize::render_row(last, 0).contains("x"),
            "{held:?}"
        );
    }

    // A rebuild from the owner's version keeps a planned hold of a column
    // the version hasn't got, and drops one of a column it has, unheld: the
    // owner's word for a column it knows wins over the plan's.
    #[test]
    fn a_rebuild_keeps_planned_holds_only_of_columns_the_owner_lacks() {
        let dir = tempfile::tempdir().unwrap();
        let plan = Plan::from_request(pending_hold_request(dir.path())).unwrap();
        let held = |sh: &Shaper, c: &str| sh.holding().iter().any(|h| h.column == c);
        let mut first = owned_shaper(&plan, 0, None, None).unwrap();
        first.rows([br#"{"id": 1, "a": 1}"#.as_slice()]).unwrap();
        let lacking = owned_shaper(&plan, 0, first.schema(), Some(&[])).unwrap();
        let mut lacking = lacking;
        lacking
            .rows([br#"{"id": 2, "a": 2, "x": 5}"#.as_slice()])
            .unwrap();
        assert!(held(&lacking, "x"), "{:?}", lacking.holding());
        // An owner whose version has x, unheld, as after a release.
        let mut known = owned_shaper(&plan, 0, None, None).unwrap();
        known.rows([br#"{"id": 1, "a": 1}"#.as_slice()]).unwrap();
        let unheld = {
            let mut free = shaper(&plan, 0, known.schema()).unwrap();
            free.rows([br#"{"id": 3, "a": 3, "x": 1}"#.as_slice()])
                .unwrap();
            free.schema()
        };
        let mut rebuilt = owned_shaper(&plan, 0, unheld, Some(&[])).unwrap();
        rebuilt
            .rows([br#"{"id": 4, "a": 4, "x": 6}"#.as_slice()])
            .unwrap();
        assert!(!held(&rebuilt, "x"), "{:?}", rebuilt.holding());
    }

    // A shaper rebuilt from the remote owner's version counts against the
    // destination's room only the columns it takes, as the shaper that
    // added them did: not a new column held whole.
    #[test]
    fn a_followed_shaper_counts_only_the_columns_the_destination_takes() {
        use openetl_proto::engine::v1::{ColumnHold as Hold, HoldCause, HoldPlan, hold_cause};
        let dir = tempfile::tempdir().unwrap();
        let mut req = request(&["a"]);
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].holds = Some(HoldPlan {
            holding: vec![Hold {
                column: "x".into(),
                reason: "the destination refused it".into(),
                cause: Some(HoldCause {
                    kind: hold_cause::Kind::Refused as i32,
                    ..Default::default()
                }),
                ..Default::default()
            }],
            limits: Some(openetl_proto::connector::v1::TableLimits {
                max_columns: 3,
                ..Default::default()
            }),
            ..Default::default()
        });
        req.held_url = format!("file://{}", dir.path().display());
        let plan = Plan::from_request(req).unwrap();
        let held = |sh: &Shaper, c: &str| sh.holding().iter().any(|h| h.column == c);
        // Here x is held as it arrives, and takes none of the table's room.
        let mut here = owned_shaper(&plan, 0, None, None).unwrap();
        here.rows([br#"{"id": 1, "a": 1, "x": 1}"#.as_slice()])
            .unwrap();
        assert!(held(&here, "x"));
        // An engine following it has room for y too.
        let holding = column_holds(&here).unwrap();
        let mut there = owned_shaper(&plan, 0, here.schema(), Some(&holding)).unwrap();
        there
            .rows([br#"{"id": 2, "a": 2, "x": 2, "y": 2}"#.as_slice()])
            .unwrap();
        assert!(!held(&there, "y"), "{:?}", there.holding());
    }

    // A join of a transform's input schemas keeps what either knew of the
    // columns that held integers past ±2^53, which alone is a change.
    #[test]
    fn a_join_keeps_what_both_knew_of_big_integers() {
        let schema = |big: &[&str]| -> SchemaRef {
            let mut md = HashMap::new();
            if !big.is_empty() {
                md.insert(
                    openetl_normalize::BIG_INTS_KEY.to_owned(),
                    serde_json::to_string(big).unwrap(),
                );
            }
            let fields = vec![
                arrow_schema::Field::new("x", DataType::Int64, true),
                arrow_schema::Field::new("y", DataType::Int64, true),
            ];
            Arc::new(arrow_schema::Schema::new_with_metadata(fields, md))
        };
        let big = |s: &SchemaRef| s.metadata().get(openetl_normalize::BIG_INTS_KEY).cloned();
        assert!(joined(&schema(&[]), &schema(&[])).is_none());
        assert!(joined(&schema(&["x"]), &schema(&[])).is_none());
        assert!(joined(&schema(&["x"]), &schema(&["x"])).is_none());
        let got = joined(&schema(&[]), &schema(&["x"])).unwrap();
        assert_eq!(big(&got).as_deref(), Some(r#"["x"]"#));
        let got = joined(&schema(&["y"]), &schema(&["x"])).unwrap();
        assert_eq!(big(&got).as_deref(), Some(r#"["x","y"]"#));
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
                    quarantined: Quarantined::default(),
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
            ..Default::default()
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

    // With a transform, a stream a statement reads has versions apart from
    // the destination's streams: a column a lane types first is proposed as
    // the input's, the statement's output as the output's, and the batch
    // goes out once the owner has taken both.
    #[tokio::test]
    async fn a_transform_proposes_its_inputs_apart_from_its_outputs() {
        let mut req = crate::plan::tests::transform_request();
        req.remote_schemas = true;
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let remote = plan.remote.clone().unwrap();
        let mut proposals = remote.take_proposals().unwrap();
        let (mut w, engine_in) = duplex(1 << 16);
        let (engine_out, from_engine) = duplex(1 << 16);
        let (tx, _events) = mpsc::channel(64);
        let job = tokio::spawn(run(
            plan,
            vec![BufReader::new(engine_in)],
            vec![engine_out],
            tx,
        ));
        let mut out = BufReader::new(from_engine);
        put(
            &mut w,
            FrameKind::Rows,
            0,
            rows(&[r#"{"id": 1, "email": "A", "extra": 2}"#]),
        )
        .await;
        let taken = |p: &openetl_proto::engine::v1::SchemasResponse| {
            openetl_proto::engine::v1::SchemasRequest {
                stream: p.stream,
                input: p.input,
                version: p.base + 1,
                arrow_schema: p.arrow_schema.clone(),
                holding: p.holding.clone(),
            }
        };
        let input = proposals.recv().await.unwrap().unwrap();
        assert_eq!((input.stream, input.input, input.base), (0, true, 0));
        assert_eq!(field_names(&input.arrow_schema), ["id", "email", "extra"]);
        remote.update(&taken(&input)).unwrap();
        let output = proposals.recv().await.unwrap().unwrap();
        assert_eq!((output.stream, output.input, output.base), (0, false, 0));
        let names = field_names(&output.arrow_schema);
        assert!(names.contains(&"email".to_string()) && !names.contains(&"extra".to_string()));
        remote.update(&taken(&output)).unwrap();
        // The outbound edge carries c, b and a.
        loop {
            let f = frame::read(&mut out).await.unwrap().unwrap();
            if f.kind == FrameKind::Batch && f.stream == 2 {
                break;
            }
        }
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

    // Under block, a batch with a new column fails against the version a
    // lane followed, but not when the owner took a version with the column
    // after that: the batch is shaped again by it.
    #[tokio::test]
    async fn a_batch_is_shaped_by_a_version_that_comes_while_it_is_shaped() {
        use openetl_proto::engine::v1::SchemaPolicy;
        use std::sync::atomic::AtomicBool;

        let mut req = request(&["a", "b"]);
        req.remote_schemas = true;
        req.schema_policy = SchemaPolicy::Block as i32;
        // Version 0, restored: a has x.
        let fresh = Plan::from_request(req.clone()).unwrap();
        let mut first = shaper(&fresh, 0, None).unwrap();
        first.rows([br#"{"x": 1}"#.as_slice()]).unwrap();
        let v0 = first.schema().unwrap();
        req.streams[0].restored_schema = ipc::encode_schema(&v0).unwrap();
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let remote = plan.remote.clone().unwrap();
        let mut proposals = remote.take_proposals().unwrap();
        // Version 1, another engine's, from before its stream was sealed:
        // a has x and y.
        let mut other = shaper(&plan, 0, Some(v0)).unwrap();
        other.unseal();
        other.rows([br#"{"x": 2, "y": 3}"#.as_slice()]).unwrap();
        let v1 = ipc::encode_schema(&other.schema().unwrap()).unwrap();

        let owner = Owner::new(&plan, 0).unwrap();
        let mut ls = {
            let o = owner.lock();
            LaneStream {
                stream: 0,
                shaper: o.shaper.fork(),
                version: o.version,
                quarantined: Quarantined::default(),
                cells: BTreeMap::new(),
                replay: None,
            }
        };
        let rows = [br#"{"x": 4, "y": 5}"#.as_slice()];
        // Without version 1, y fails the batch.
        let mut alone = ls.shaper.fork();
        assert!(alone.rows(rows).is_err());
        // Version 1 lands after the lane followed the owner, as it types the
        // batch.
        let landed = AtomicBool::new(false);
        let shaped = ls
            .shape_own(
                &plan,
                &owner,
                |sh| {
                    if !landed.swap(true, Ordering::SeqCst) {
                        remote.update(&take(1, v1.clone())).unwrap();
                    }
                    sh.type_rows(rows)
                },
                |_, e| shape_error("a", e),
            )
            .await
            .unwrap();
        assert!(shaped.batch.schema().field_with_name("y").is_ok());
        assert!(proposals.try_recv().is_err());
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

    /// A quarantine file's causes, as (code, column, notHeld, rows).
    fn causes(q: &QuarantineFile) -> Vec<(&str, &str, &str, u64)> {
        q.causes
            .iter()
            .map(|c| {
                (
                    c.code.as_str(),
                    c.column.as_str(),
                    c.not_held.as_str(),
                    c.rows,
                )
            })
            .collect()
    }

    /// A quarantine file's lines.
    fn lines_of(path: &std::path::Path) -> Vec<serde_json::Value> {
        std::fs::read_to_string(path)
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
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
            rows(&[r#"{"x": 1}"#, "[1]", "{nope", r#"{"x": 2}"#]),
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
            ("a", 2, false)
        );
        assert_eq!(causes(&q[0]), [("json", "", "", 1), ("object", "", "", 1)]);
        assert!(q[0].url.ends_with("/1/1/a.jsonl"), "{}", q[0].url);
        let lines = lines_of(&dir.path().join("1/1/a.jsonl"));
        let line = &lines[0];
        assert_eq!(line["stream"], "a");
        assert_eq!(line["code"], "object");
        assert_eq!(line["record"], "[1]");
        assert!(
            line["reason"]
                .as_str()
                .unwrap()
                .contains("not a JSON object")
        );
        // No column caused it, and the stream holds no cells.
        assert!(line.get("column").is_none() && line.get("notHeld").is_none());
        assert_eq!(lines[1]["code"], "json");
        assert_eq!(lines[1]["record"], "{nope");
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
        assert_eq!(
            (&lines[0]["code"], &lines[0]["column"]),
            (&"nan".into(), &"price".into())
        );
        assert_eq!(lines[1]["code"], "precision");
        assert_eq!(
            causes(&ready[0].quarantine[0]),
            [("nan", "price", "", 1), ("precision", "price", "", 1)]
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

    /// A stream that holds cells still quarantines a row whose value it
    /// can't hold, and says why it didn't.
    #[tokio::test]
    async fn what_a_stream_that_holds_cells_quarantines_says_why() {
        use arrow_schema::{Field, Schema};
        let dir = tempfile::tempdir().unwrap();
        let mut req = request(&["a", "b"]);
        req.quarantine_url = format!("file://{}/q", dir.path().display());
        req.held_url = format!("file://{}/held", dir.path().display());
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("price", DataType::Decimal128(10, 2), true),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        req.streams[0].primary_key = vec!["id".into()];
        req.streams[0].holds = Some(openetl_proto::engine::v1::HoldPlan {
            cells: true,
            ..Default::default()
        });
        let mut h = start_with(req);
        let w = &mut h.to_engine;
        let sent = [
            r#"{"id": "x", "price": "1.5"}"#,
            r#"{"id": 2, "price": "12345678901.5"}"#,
            "[1]",
        ];
        put(w, FrameKind::Rows, 0, rows(&sent)).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();

        let ready = ready(&mut h.events).await;
        assert_eq!(ready[0].holds["a"].cells["price"].cells, 1, "price is held");
        let q = &ready[0].quarantine;
        assert_eq!(q.len(), 1);
        assert_eq!(q[0].rows, 2, "{:?}", causes(&q[0]));
        assert_eq!(
            causes(&q[0]),
            [("object", "", "record", 1), ("type", "id", "key", 1)]
        );
        let lines = lines_of(&dir.path().join("q/1/1/a.jsonl"));
        let got: Vec<(&str, Option<&str>, &str)> = lines
            .iter()
            .map(|l| {
                (
                    l["code"].as_str().unwrap(),
                    l.get("column").and_then(|c| c.as_str()),
                    l["notHeld"].as_str().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            got,
            [("type", Some("id"), "key"), ("object", None, "record")]
        );
        for l in &lines {
            let fields: Vec<&str> = l.as_object().unwrap().keys().map(String::as_str).collect();
            assert!(
                fields.iter().all(
                    |f| ["stream", "code", "column", "notHeld", "reason", "record"].contains(f)
                ),
                "{fields:?}"
            );
            assert!(!l["reason"].as_str().unwrap().contains('x'), "{l}");
        }
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

    /// On a changelog, deletes pass projected, and an update the filter
    /// drops becomes a delete of its key alone.
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
                (3, Some("gone"), "delete"),
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
            [Some("NEW".into()), None, Some("GONE".into())]
        );
        assert_eq!(
            column_ints(&a[0], "_openetl_seq"),
            [Some(0), Some(1), Some(2)]
        );
        assert!(a[0].column_by_name("_openetl_unchanged").is_some());
    }

    /// A changelog's batch of id and n, with ops and seqs from `seq` on.
    fn changelog_n(ids: &[i64], n: arrow_array::ArrayRef, ops: &[&str], seq: i64) -> Vec<u8> {
        use arrow_array::{Int64Array, StringArray};
        let control = |kind: &str| {
            HashMap::from([(openetl_normalize::CONTROL_KEY.to_owned(), kind.to_owned())])
        };
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("n", n.data_type().clone(), true),
            arrow_schema::Field::new("op", DataType::Utf8, false).with_metadata(control("op")),
            arrow_schema::Field::new("seq", DataType::Int64, false).with_metadata(control("seq")),
        ]));
        let rows = ops.len();
        let b = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                n,
                Arc::new(StringArray::from(ops.to_vec())),
                Arc::new(Int64Array::from_iter_values(
                    (0..rows).map(|i| seq + i64::try_from(i).unwrap()),
                )),
            ],
        )
        .unwrap();
        ipc::encode_batch(&b).unwrap()
    }

    /// Under ignore_new, a statement over a changelog, then an insert, and
    /// an update and a delete whose n doesn't fit the type the insert gave
    /// the output.
    async fn a_delete_whose_n_doesnt_fit(
        holds: bool,
        remote: bool,
        dir: &std::path::Path,
    ) -> (Vec<frame::Frame>, Vec<EpochReady>) {
        use arrow_array::{Float64Array, Int64Array};
        let mut req = sql_request("INSERT INTO a SELECT * FROM a", &[("a", &["id"])]);
        req.streams[0].changelog = true;
        req.outputs[0].changelog = true;
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::IgnoreNew as i32;
        req.quarantine_url = format!("file://{}/q", dir.display());
        if holds {
            req.held_url = format!("file://{}/held", dir.display());
            req.outputs[0].holds = Some(openetl_proto::engine::v1::HoldPlan {
                cells: true,
                ..Default::default()
            });
        }
        req.remote_schemas = remote;
        let mut h = start_owned(req);
        let w = &mut h.to_engine;
        let ints = Arc::new(Int64Array::from(vec![1]));
        put(
            w,
            FrameKind::Batch,
            0,
            changelog_n(&[1], ints, &["insert"], 0),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        let floats = Arc::new(Float64Array::from(vec![2.5, 2.5]));
        put(
            w,
            FrameKind::Batch,
            0,
            changelog_n(&[1, 1], floats, &["update", "delete"], 1),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::Barrier, 1, barrier(2)).await;
        put(w, FrameKind::EndOfStream, 0, vec![]).await;
        put(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
        let frames = drain(&mut h.from_engine).await;
        h.job.await.unwrap().unwrap();
        (frames, ready(&mut h.events).await)
    }

    /// With cell holds, the delete passes with n held, as a passed-through
    /// stream's would.
    #[tokio::test]
    async fn a_delete_whose_values_dont_fit_the_output_passes_with_them_held() {
        let dir = tempfile::tempdir().unwrap();
        let (frames, ready) = a_delete_whose_n_doesnt_fit(true, false, dir.path()).await;
        let a = batches(&frames, 0);
        let ops: Vec<Option<String>> = a
            .iter()
            .flat_map(|b| column_strings(b, "_openetl_op"))
            .collect();
        assert_eq!(
            ops,
            [
                Some("insert".into()),
                Some("update".into()),
                Some("delete".into())
            ]
        );
        assert!(ready[1].quarantine.is_empty(), "{:?}", ready[1].quarantine);
        assert_eq!(ready[1].holds["a"].cells["n"].cells, 2);
    }

    /// Without holds, a delete whose projected values don't fit the
    /// output's established types passes as a delete of its key, where an
    /// update is quarantined; where the output's schema has an owner outside
    /// the engine too.
    #[tokio::test]
    async fn a_delete_whose_values_dont_fit_the_output_passes_with_its_key() {
        for remote in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let (frames, ready) = a_delete_whose_n_doesnt_fit(false, remote, dir.path()).await;
            let a = batches(&frames, 0);
            let ops: Vec<Option<String>> = a
                .iter()
                .flat_map(|b| column_strings(b, "_openetl_op"))
                .collect();
            assert_eq!(
                ops,
                [Some("insert".into()), Some("delete".into())],
                "remote {remote}"
            );
            let last = a.last().unwrap();
            assert_eq!(column_ints(last, "id"), [Some(1)]);
            assert_eq!(column_ints(last, "n"), [None]);
            assert_eq!(column_ints(last, "_openetl_seq"), [Some(2)]);
            let q = &ready[1].quarantine;
            assert_eq!((q.len(), q[0].rows), (1, 1), "remote {remote}: {q:?}");
        }
    }

    /// Across writer lanes, a delete that goes again with its key alone
    /// reaches the writer lane its key's other rows do.
    #[tokio::test]
    async fn a_delete_by_its_key_alone_meets_its_keys_rows_on_one_writer_lane() {
        use arrow_array::{Float64Array, Int64Array};
        let dir = tempfile::tempdir().unwrap();
        let mut req = sql_request("INSERT INTO a SELECT * FROM a", &[("a", &["id"])]);
        req.streams[0].changelog = true;
        req.outputs[0].changelog = true;
        req.outputs[0].exchange = crate::plan::tests::exchange_by(&["id"]);
        // Restored, so n is Int64 before any barrier seals it.
        let restored = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, true),
            arrow_schema::Field::new("email", DataType::Utf8, true),
            arrow_schema::Field::new("n", DataType::Int64, true),
        ]);
        req.outputs[0].restored_schema = ipc::encode_schema(&Arc::new(restored)).unwrap();
        req.schema_policy = openetl_proto::engine::v1::SchemaPolicy::IgnoreNew as i32;
        req.quarantine_url = format!("file://{}", dir.path().display());
        let (inbound, outbound) = (req.edges[0].clone(), req.edges[1].clone());
        req.lanes = 2;
        req.writer_lanes = 2;
        req.edges.clear();
        for (l, edge) in [(0, &inbound), (1, &inbound), (0, &outbound), (1, &outbound)] {
            let mut e = edge.clone();
            e.id = format!("{}-{l}", e.id);
            e.lane = l;
            req.edges.push(e);
        }
        let mut h = start_lanes(req);
        let ids: Vec<i64> = (1..=40).collect();
        let w = &mut h.ins[0];
        let ints = Arc::new(Int64Array::from(vec![1; ids.len()]));
        put(
            w,
            FrameKind::Batch,
            0,
            changelog_n(&ids, ints, &["insert"; 40], 0),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        put(w, FrameKind::Barrier, 1, barrier(1)).await;
        let floats = Arc::new(Float64Array::from(vec![2.5; ids.len()]));
        put(
            w,
            FrameKind::Batch,
            0,
            changelog_n(&ids, floats, &["delete"; 40], 40),
        )
        .await;
        put(w, FrameKind::Barrier, 0, barrier(2)).await;
        put(w, FrameKind::Barrier, 1, barrier(2)).await;
        let idle = &mut h.ins[1];
        for e in 1..=2 {
            put(idle, FrameKind::Barrier, 0, barrier(e)).await;
            put(idle, FrameKind::Barrier, 1, barrier(e)).await;
        }
        h.end_all(2).await;
        let (out, ready) = h.finish().await;
        let mut inserted = 0;
        for frames in &out {
            let mut by_op: BTreeMap<String, Vec<i64>> = BTreeMap::new();
            for f in frames
                .iter()
                .filter(|f| f.kind == FrameKind::Batch && f.stream == 0)
            {
                let b = ipc::decode_batch(f.payload.clone()).unwrap();
                let ops = column_strings(&b, "_openetl_op");
                if ops.iter().any(|o| o.as_deref() == Some("delete")) {
                    assert_eq!(column_ints(&b, "n"), vec![None; b.num_rows()]);
                }
                for (op, id) in ops.into_iter().zip(column_ints(&b, "id")) {
                    by_op.entry(op.unwrap()).or_default().push(id.unwrap());
                }
            }
            let mut ins = by_op.remove("insert").unwrap_or_default();
            let mut del = by_op.remove("delete").unwrap_or_default();
            ins.sort();
            del.sort();
            assert!(!ins.is_empty(), "each writer lane gets some keys");
            assert_eq!(ins, del, "a key's delete reaches its insert's writer lane");
            inserted += ins.len();
        }
        assert_eq!(inserted, ids.len());
        assert!(ready.iter().all(|r| r.quarantine.is_empty()));
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
        assert_eq!(line["code"], "statement");
        assert!(line.get("column").is_none() && line.get("notHeld").is_none());
        assert_eq!(line["record"], r#"{"id": 2, "email": "x"}"#);
        assert!(
            line["reason"]
                .as_str()
                .unwrap()
                .starts_with("transform.sql 1:1, writing c: "),
            "{}",
            line["reason"]
        );
        assert_eq!(causes(&q[0]), [("statement", "", "", 1)]);
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
            event_time: None,
            lateness: 0,
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
        assert_eq!(
            (&line["code"], &line["column"], &line["notHeld"]),
            (&"precision".into(), &"price".into(), &"transform".into())
        );
        assert_eq!(causes(&q[0]), [("precision", "price", "transform", 1)]);

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

    /// A job whose statement groups stream a by a window: a's rows have an
    /// event time, `at`, its watermark trails them by `delay` seconds, and
    /// `lanes` lanes and writer lanes read them. Stream b passes through.
    /// A job of `sql` over stream a, which keeps its state below `state`.
    fn window_request(
        state: &std::path::Path,
        sql: &str,
        lanes: u32,
        generation: u64,
        first_epoch: u64,
    ) -> openetl_proto::engine::v1::StartJobRequest {
        let email = arrow_schema::Field::new("email", DataType::Utf8, true);
        window_request_by(state, sql, email, lanes, generation, first_epoch)
    }

    /// As window_request, with `key` in place of email, the column the
    /// output is keyed by beside window_start.
    fn window_request_by(
        state: &std::path::Path,
        sql: &str,
        key: arrow_schema::Field,
        lanes: u32,
        generation: u64,
        first_epoch: u64,
    ) -> openetl_proto::engine::v1::StartJobRequest {
        let mut req = sql_request(
            sql,
            &[("minutes", &["window_start", key.name()]), ("b", &["id"])],
        );
        let declared = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            key,
            arrow_schema::Field::new("v", DataType::Int64, true),
            arrow_schema::Field::new(
                "at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        req.streams[0].event_time = Some(openetl_proto::engine::v1::EventTime {
            column: "at".into(),
            ..Default::default()
        });
        req.generation = generation;
        req.first_epoch = first_epoch;
        req.pipeline = "windows".into();
        req.state_url = format!("file://{}", state.display());
        let inputs: Vec<crate::transform::Input> = req
            .streams
            .iter()
            .map(|s| crate::transform::Input::of(s).unwrap())
            .collect();
        let planned = crate::transform::Transform::plan(sql, inputs).unwrap();
        req.transform.as_mut().unwrap().operators =
            planned.operators().map(str::to_owned).collect();
        req.lanes = lanes;
        req.writer_lanes = lanes;
        let out = req.edges.pop().unwrap();
        let inbound = req.edges.pop().unwrap();
        for l in 0..lanes {
            let mut e = inbound.clone();
            e.id = format!("in-{l}");
            e.lane = l;
            req.edges.push(e);
            let mut e = out.clone();
            e.id = format!("out-{l}");
            e.lane = l;
            req.edges.push(e);
        }
        req
    }

    fn event_time(
        req: &mut openetl_proto::engine::v1::StartJobRequest,
    ) -> &mut openetl_proto::engine::v1::EventTime {
        req.streams[0].event_time.as_mut().unwrap()
    }

    const MINUTES: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
                           FROM a GROUP BY tumble(at, INTERVAL '1 minute'), email";

    const SEC: i64 = 1_000_000;

    fn barrier_of(generation: u64, n: u64) -> Vec<u8> {
        Barrier {
            epoch: Some(Epoch { generation, n }),
        }
        .encode_to_vec()
    }

    /// A row of a at `second` seconds past the Unix epoch.
    fn minute_row(id: i64, email: &str, second: i64) -> String {
        format!(
            r#"{{"id": {id}, "email": "{email}", "v": {id}, "at": "1970-01-01T{:02}:{:02}:{:02}Z"}}"#,
            second / 3600,
            second / 60 % 60,
            second % 60
        )
    }

    /// Barrier n of every stream, on lane w.
    async fn barriers(w: &mut DuplexStream, generation: u64, n: u64) {
        put(w, FrameKind::Barrier, 0, barrier_of(generation, n)).await;
        put(w, FrameKind::Barrier, 1, barrier_of(generation, n)).await;
    }

    /// A window's row as a writer lane gave it: its start in seconds, its
    /// key, its count, and the epoch it came in.
    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    struct WindowRow {
        start: i64,
        email: String,
        n: i64,
        epoch: u64,
    }

    /// Each writer lane's window rows, in order. `first` is the epoch the
    /// job starts at.
    fn window_rows(outs: &[Vec<frame::Frame>], first: u64) -> Vec<Vec<WindowRow>> {
        outs.iter()
            .map(|frames| {
                let mut epoch = first;
                let mut got = Vec::new();
                for f in frames {
                    if f.kind == FrameKind::Barrier && f.stream == 0 {
                        epoch += 1;
                    }
                    if f.kind != FrameKind::Batch || f.stream != 0 {
                        continue;
                    }
                    let b = ipc::decode_batch(f.payload.clone()).unwrap();
                    let starts = b
                        .column_by_name("window_start")
                        .unwrap()
                        .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                        .iter()
                        .collect::<Vec<_>>();
                    for ((s, e), n) in starts
                        .iter()
                        .zip(column_strings(&b, "email"))
                        .zip(column_ints(&b, "n"))
                    {
                        got.push(WindowRow {
                            start: s.unwrap() / SEC,
                            email: e.unwrap(),
                            n: n.unwrap(),
                            epoch,
                        });
                    }
                }
                got
            })
            .collect()
    }

    fn all_rows(outs: &[Vec<frame::Frame>], first: u64) -> Vec<WindowRow> {
        let mut all: Vec<WindowRow> = window_rows(outs, first).into_iter().flatten().collect();
        all.sort();
        all
    }

    fn row(start: i64, email: &str, n: i64, epoch: u64) -> WindowRow {
        WindowRow {
            start,
            email: email.into(),
            n,
            epoch,
        }
    }

    fn quarantined(ready: &[EpochReady], stream: &str) -> u64 {
        ready
            .iter()
            .flat_map(|r| &r.quarantine)
            .filter(|q| q.stream == stream)
            .map(|q| q.rows)
            .sum()
    }

    /// A window's statement runs past an exchange on its keys inside the
    /// engine, and its rows reach the writer lane its keyed lane feeds with
    /// no second exchange: each key on one writer lane. A window closes once
    /// every lane's watermark has passed its end, in the epoch that happens
    /// in, and the rest at the end of input. Each epoch names each keyed
    /// lane's state, and a run of what the lane changed in it.
    #[tokio::test]
    async fn a_window_closes_once_every_lanes_watermark_passes_it() {
        let state = tempfile::tempdir().unwrap();
        let req = window_request(state.path(), MINUTES, 2, 1, 1);
        let mut h = start_lanes(req);
        for (l, w) in h.ins.iter_mut().enumerate() {
            let l = i64::try_from(l).unwrap();
            put(w, FrameKind::Split, 0, split(&format!("s{l}"), None)).await;
            // Rows at 0, 20, ..., 100 seconds: x at 0 and 60, y at 20 and
            // 80, z at 40 and 100.
            let rs: Vec<String> = (0..6)
                .map(|i| minute_row(l * 10 + i, ["x", "y", "z"][(i % 3) as usize], i * 20))
                .collect();
            let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
            put(w, FrameKind::Rows, 0, rows(&refs)).await;
            barriers(w, 1, 1).await;
            put(
                w,
                FrameKind::Split,
                0,
                split(&format!("s{l}"), Some(watermark::END)),
            )
            .await;
            barriers(w, 1, 2).await;
        }
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        // Both lanes are at 100 seconds by their first barrier, so the
        // first minute closes in epoch 1, and the second at the end.
        let per_lane = window_rows(&outs, 1);
        assert_eq!(
            all_rows(&outs, 1),
            [
                row(0, "x", 2, 1),
                row(0, "y", 2, 1),
                row(0, "z", 2, 1),
                row(60, "x", 2, 2),
                row(60, "y", 2, 2),
                row(60, "z", 2, 2),
            ]
        );
        for e in ["x", "y", "z"] {
            let lanes = per_lane
                .iter()
                .filter(|r| r.iter().any(|w| w.email == e))
                .count();
            assert_eq!(lanes, 1, "{e} on {lanes} writer lanes");
        }
        assert_eq!(ready.len(), 2);
        let mut named: Vec<BTreeSet<String>> = Vec::new();
        for (n, r) in ready.iter().enumerate() {
            let epoch = n as u64 + 1;
            assert_eq!(r.snapshots.len(), 2, "epoch {epoch}");
            let mut runs = BTreeSet::new();
            for s in &r.snapshots {
                let range = s.key_groups.unwrap();
                for run in &s.runs {
                    let (dir, name) = run.split_once('/').unwrap();
                    assert_eq!(dir, crate::state::dir(&s.operator));
                    let parsed = openetl_state::runs::RunName::parse(name).unwrap();
                    assert!(parsed.epoch <= epoch && parsed.generation == 1, "{run}");
                    assert_eq!(
                        (parsed.range.first, parsed.range.last),
                        (range.first, range.last)
                    );
                    assert!(state.path().join(run).is_file(), "{run} is written");
                    runs.insert(run.clone());
                }
            }
            named.push(runs);
        }
        assert!(named[0].iter().any(|r| r.contains("/e1/")), "{named:?}");
        assert!(named[1].iter().any(|r| r.contains("/e2/")), "{named:?}");
        assert!(named[1].is_superset(&named[0]), "{named:?}");
    }

    /// A lane holds a window open until each of its splits' watermarks
    /// passes it, and a split's Split frame, as the runner sends for an
    /// idle one, moves it on.
    #[tokio::test]
    async fn a_quiet_split_holds_windows_until_it_moves_on() {
        let state = tempfile::tempdir().unwrap();
        let req = window_request(state.path(), MINUTES, 1, 1, 1);
        let mut h = start_lanes(req);
        let w = &mut h.ins[0];
        put(w, FrameKind::Split, 0, split("busy", None)).await;
        put(w, FrameKind::Split, 0, split("quiet", None)).await;
        put(w, FrameKind::Split, 0, split("quiet", None)).await;
        put(w, FrameKind::Rows, 0, rows(&[&minute_row(1, "x", 5)])).await;
        put(w, FrameKind::Split, 0, split("busy", None)).await;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[&minute_row(2, "x", 10), &minute_row(3, "x", 150)]),
        )
        .await;
        barriers(w, 1, 1).await;
        put(w, FrameKind::Split, 0, split("quiet", Some(200 * SEC))).await;
        barriers(w, 1, 2).await;
        h.end_all(2).await;
        let (outs, _) = h.finish().await;
        // Quiet is at 5 seconds until its watermark moves to 200, by when
        // busy is at 150: the first two minutes close in epoch 2, and the
        // third never does.
        assert_eq!(all_rows(&outs, 1), [row(0, "x", 2, 2)]);
    }

    /// A row of a window that closed comes again within the lateness as an
    /// update of the window's row, and after it is quarantined.
    #[tokio::test]
    async fn a_late_row_updates_its_window_within_the_lateness() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = window_request(&dir.path().join("state"), MINUTES, 1, 1, 1);
        req.quarantine_url = format!("file://{}", dir.path().display());
        event_time(&mut req).lateness_micros = 30 * SEC;
        let mut h = start_lanes(req);
        let w = &mut h.ins[0];
        put(w, FrameKind::Split, 0, split("s", None)).await;
        for (id, second) in [(1, 10), (2, 70), (3, 20), (4, 100), (5, 25)] {
            put(w, FrameKind::Rows, 0, rows(&[&minute_row(id, "x", second)])).await;
        }
        put(w, FrameKind::Split, 0, split("s", Some(watermark::END))).await;
        barriers(w, 1, 1).await;
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        // At 70 the first minute closes; 20 is late, but within 30 seconds
        // of its end; 100 is past them, so 25 is quarantined.
        assert_eq!(
            window_rows(&outs, 1)[0],
            [row(0, "x", 1, 1), row(0, "x", 2, 1), row(60, "x", 2, 1)]
        );
        assert_eq!(quarantined(&ready, "minutes"), 1);
        let q: Vec<_> = ready.iter().flat_map(|r| &r.quarantine).collect();
        assert_eq!(causes(q[0]), [("late", "", "", 1)]);
    }

    /// A restore resumes a window's state from the record's snapshots, in
    /// another job, on another number of lanes, from the record's
    /// watermarks: a window that closed before it stays closed, and a row of
    /// it is late. The restored lanes name the runs they read on, beside
    /// their own.
    #[tokio::test]
    async fn a_restored_window_goes_on_and_a_closed_one_stays_closed() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let mut req = window_request(&state, MINUTES, 2, 1, 1);
        req.quarantine_url = format!("file://{}", dir.path().display());
        let mut h = start_lanes(req);
        for (l, w) in h.ins.iter_mut().enumerate() {
            put(w, FrameKind::Split, 0, split(&format!("s{l}"), None)).await;
            let later = match l {
                0 => minute_row(2, "y", 70),
                _ => minute_row(3, "z", 75),
            };
            put(
                w,
                FrameKind::Rows,
                0,
                rows(&[&minute_row(1, "x", 10), &later]),
            )
            .await;
            barriers(w, 1, 1).await;
        }
        h.end_all(2).await;
        let (outs, first) = h.finish().await;
        assert_eq!(all_rows(&outs, 1), [row(0, "x", 2, 1)]);
        let snapshots = first[0].snapshots.clone();
        assert_eq!(snapshots.len(), 2);
        let marks = split_marks(&first[0], "a");
        assert_eq!(
            marks,
            [("s0".to_string(), 70 * SEC), ("s1".to_string(), 75 * SEC)]
        );

        // The next attempt, or the next read of a stream read in the order of
        // its time, starts each split at its watermark, and the lanes past
        // the exchange at the least of them.
        let restored: BTreeSet<String> = snapshots.iter().flat_map(|s| s.runs.clone()).collect();
        assert!(!restored.is_empty());
        let mut req = window_request(&state, MINUTES, 3, 2, 2);
        req.quarantine_url = format!("file://{}", dir.path().display());
        req.restored_snapshots = snapshots;
        event_time(&mut req).floor = Some(70 * SEC);
        let mut h = start_lanes(req);
        for (l, w) in h.ins.iter_mut().enumerate() {
            let rs = match l {
                0 => vec![minute_row(4, "x", 30), minute_row(5, "y", 80)],
                1 => vec![minute_row(6, "z", 90)],
                _ => vec![],
            };
            if let Some((id, wm)) = marks.get(l) {
                put(w, FrameKind::Split, 0, split(id, Some(*wm))).await;
                let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                put(w, FrameKind::Rows, 0, rows(&refs)).await;
                put(w, FrameKind::Split, 0, split(id, Some(watermark::END))).await;
            }
            barriers(w, 2, 2).await;
        }
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        assert_eq!(all_rows(&outs, 2), [row(60, "y", 2, 2), row(60, "z", 2, 2)]);
        assert_eq!(quarantined(&ready, "minutes"), 1, "x at 30 is late");
        assert_eq!(ready[0].snapshots.len(), 3);
        let named: BTreeSet<String> = ready[0]
            .snapshots
            .iter()
            .flat_map(|s| s.runs.clone())
            .collect();
        assert!(named.is_superset(&restored), "{named:?}");
        assert!(named.iter().any(|r| r.contains("/g2/e2/")), "{named:?}");
    }

    /// Split `split`'s rows of part `part`, as ID, key and second: part 0
    /// of minute 1, which closes no window, and part 1 of minutes 1 and 2.
    fn spanning(split: usize, part: usize) -> Vec<(i64, &'static str, i64)> {
        let keys = ["x", "y", "z", "w"];
        (0..12)
            .map(|i| {
                let id = (split * 100 + part * 1000 + i) as i64;
                let second = match part {
                    0 => 60 + i as i64 * 3,
                    _ => 90 + i as i64 * 4,
                };
                (id, keys[(i + split) % keys.len()], second)
            })
            .collect()
    }

    fn spanning_rows(split: usize, part: usize) -> Vec<String> {
        spanning(split, part)
            .into_iter()
            .map(|(id, key, second)| minute_row(id, key, second))
            .collect()
    }

    /// What a GROUP BY of both splits' rows of `parts` gives, by start and
    /// key.
    fn spanning_counts(parts: &[usize]) -> Vec<(i64, String, i64)> {
        let mut want: BTreeMap<(i64, String), i64> = BTreeMap::new();
        for split in 0..2 {
            for &part in parts {
                for (_, key, second) in spanning(split, part) {
                    *want.entry((second - second % 60, key.into())).or_default() += 1;
                }
            }
        }
        want.into_iter().map(|((s, k), n)| (s, k, n)).collect()
    }

    fn counts(outs: &[Vec<frame::Frame>], first: u64) -> Vec<(i64, String, i64)> {
        all_rows(outs, first)
            .into_iter()
            .map(|r| (r.start, r.email, r.n))
            .collect()
    }

    /// The first job of the spanning rows: two lanes, a split each, part 0
    /// in epoch 1, which closes no window; part 1 in epoch 2 with the end of
    /// input, if `both`. Its window rows and EpochReadys.
    async fn first_spanning_job(
        state: &std::path::Path,
        both: bool,
    ) -> (Vec<(i64, String, i64)>, Vec<EpochReady>) {
        let mut h = start_lanes(window_request(state, MINUTES, 2, 1, 1));
        for (l, w) in h.ins.iter_mut().enumerate() {
            let id = format!("s{l}");
            put(w, FrameKind::Split, 0, split(&id, None)).await;
            let rs = spanning_rows(l, 0);
            let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
            put(w, FrameKind::Rows, 0, rows(&refs)).await;
            barriers(w, 1, 1).await;
            if both {
                let rs = spanning_rows(l, 1);
                let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                put(w, FrameKind::Rows, 0, rows(&refs)).await;
                put(w, FrameKind::Split, 0, split(&id, Some(watermark::END))).await;
                barriers(w, 1, 2).await;
            }
        }
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        assert!(
            all_rows(&outs, 1).iter().all(|r| r.epoch == 2),
            "no window closes in epoch 1"
        );
        (counts(&outs, 1), ready)
    }

    /// A job restored from `from`, epoch 1's, on `lanes` lanes, as
    /// generation `generation`, reads part 1 and the end of input, each
    /// split from its watermark in `from`. Its window rows and EpochReadys.
    async fn restored_spanning_job(
        state: &std::path::Path,
        from: &EpochReady,
        lanes: u32,
        generation: u64,
        change: impl FnOnce(&mut openetl_proto::engine::v1::StartJobRequest),
    ) -> (Vec<(i64, String, i64)>, Vec<EpochReady>) {
        let marks: BTreeMap<String, i64> = split_marks(from, "a").into_iter().collect();
        let mut req = window_request(state, MINUTES, lanes, generation, 2);
        req.restored_snapshots = from.snapshots.clone();
        event_time(&mut req).floor = marks.values().min().copied();
        change(&mut req);
        let mut h = start_lanes(req);
        let lanes = h.ins.len();
        for (l, w) in h.ins.iter_mut().enumerate() {
            // Split s is on lane s, or on the one lane; other lanes have
            // none. A lane has each of its splits before any ends.
            let mine: Vec<String> = (0..2)
                .filter(|s| s % lanes == l)
                .map(|s| format!("s{s}"))
                .collect();
            for s in &mine {
                put(w, FrameKind::Split, 0, split(s, marks.get(s).copied())).await;
            }
            for s in &mine {
                put(w, FrameKind::Split, 0, split(s, None)).await;
                let rs = spanning_rows(s[1..].parse().unwrap(), 1);
                let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                put(w, FrameKind::Rows, 0, rows(&refs)).await;
            }
            for s in &mine {
                put(w, FrameKind::Split, 0, split(s, Some(watermark::END))).await;
            }
            barriers(w, generation, 2).await;
        }
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        (counts(&outs, 2), ready)
    }

    /// State written by one job's lanes, which ended, restores in other
    /// jobs on one, two and four lanes, each lane opening the part of the
    /// record's runs its key groups hold: each gives every window whole, as
    /// one job reading every row does.
    #[tokio::test]
    async fn state_restores_onto_any_number_of_lanes() {
        let dir = tempfile::tempdir().unwrap();
        let want = spanning_counts(&[0, 1]);
        let starts: BTreeSet<i64> = want.iter().map(|(s, _, _)| *s).collect();
        assert_eq!(starts, BTreeSet::from([60, 120]), "{want:?}");
        let (whole, _) = first_spanning_job(&dir.path().join("whole"), true).await;
        assert_eq!(whole, want, "in one job");

        let state = dir.path().join("state");
        let (_, first) = first_spanning_job(&state, false).await;
        assert_eq!(first[0].snapshots.len(), 2);
        let mut compared = 0;
        for (generation, lanes) in [(2, 1), (3, 2), (4, 4)] {
            let (got, ready) =
                restored_spanning_job(&state, &first[0], lanes, generation, |_| {}).await;
            assert_eq!(got, want, "on {lanes} lanes");
            assert_eq!(ready[0].snapshots.len(), lanes as usize);
            compared += 1;
        }
        assert_eq!(compared, 3);
    }

    /// A run a lane wrote at a barrier whose record never landed is an
    /// orphan: a restore from the record before it doesn't read it, and the
    /// epoch's rows, read again, count once.
    #[tokio::test]
    async fn a_failed_epochs_run_is_an_orphan_and_isnt_read() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (_, first) = first_spanning_job(&state, true).await;
        let (one, two) = (&first[0], &first[1]);
        let orphans: Vec<&String> = two
            .snapshots
            .iter()
            .flat_map(|s| &s.runs)
            .filter(|r| r.contains("/g1/e2/"))
            .collect();
        assert!(!orphans.is_empty(), "epoch 2 wrote runs");
        for r in &orphans {
            assert!(state.join(r).is_file());
            assert!(one.snapshots.iter().all(|s| !s.runs.contains(r)));
        }
        let (got, _) = restored_spanning_job(&state, one, 2, 2, |_| {}).await;
        assert_eq!(got, spanning_counts(&[0, 1]));
    }

    /// A redo of an epoch starts from the state of the record before it,
    /// writes none and names none: its rows are the epoch's.
    #[tokio::test]
    async fn a_redo_reads_the_state_before_its_epoch_and_writes_none() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (_, first) = first_spanning_job(&state, true).await;
        let files = |d: &std::path::Path| -> usize {
            let mut n = 0;
            let mut stack = vec![d.to_path_buf()];
            while let Some(p) = stack.pop() {
                for e in std::fs::read_dir(&p).unwrap() {
                    let e = e.unwrap().path();
                    match e.is_dir() {
                        true => stack.push(e),
                        false => n += 1,
                    }
                }
            }
            n
        };
        let before = files(&state);
        assert!(before > 0);
        let (got, ready) = restored_spanning_job(&state, &first[0], 1, 2, |r| {
            r.state_read_only = true;
            r.first_lane = 1;
        })
        .await;
        assert_eq!(got, spanning_counts(&[0, 1]));
        assert!(ready.iter().all(|r| r.snapshots.is_empty()));
        assert_eq!(files(&state), before, "a redo writes no run");
    }

    /// A job with a group window keeps its state at the job's state URL, and
    /// is refused without one.
    #[test]
    fn a_group_window_needs_a_state_url() {
        let state = tempfile::tempdir().unwrap();
        let mut req = window_request(state.path(), MINUTES, 1, 1, 1);
        req.state_url.clear();
        let Err(e) = Plan::from_request(req) else {
            panic!("planned");
        };
        assert_eq!(e.code, ErrorCode::ConfigInvalid);
        assert!(e.message.contains("no state URL"), "{}", e.message);
    }

    /// A record whose snapshots of an operator don't hold every key group
    /// fails the job as a corrupt checkpoint, and says how to start the
    /// state again: on a lane that a snapshot holds part of, and on one
    /// that none does, which doesn't start empty.
    #[tokio::test]
    async fn state_the_record_names_in_part_fails_the_job() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (_, first) = first_spanning_job(&state, false).await;
        assert_eq!(first[0].snapshots.len(), 2);
        let mut failed = 0;
        for lanes in [1, 2] {
            let mut req = window_request(&state, MINUTES, lanes, 2, 2);
            req.restored_snapshots = first[0].snapshots[..1].to_vec();
            let mut h = start_lanes(req);
            h.end_all(2).await;
            let err = h.job.await.unwrap().unwrap_err();
            assert_eq!(err.code, ErrorCode::CheckpointCorrupt, "on {lanes} lanes");
            assert!(
                err.message
                    .contains("can't open the state the checkpoint names")
                    && err
                        .message
                        .contains("none of its snapshots holds key group")
                    && err.message.contains("openetl reset --stream a NAME"),
                "on {lanes} lanes: {}",
                err.message
            );
            failed += 1;
        }
        assert_eq!(failed, 2);
    }

    /// A run the record names that's gone fails the job as a corrupt
    /// checkpoint, which no retry reads.
    #[tokio::test]
    async fn a_missing_run_fails_the_job_as_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (_, first) = first_spanning_job(&state, false).await;
        let run = first[0]
            .snapshots
            .iter()
            .flat_map(|s| &s.runs)
            .next()
            .unwrap();
        std::fs::remove_file(state.join(run)).unwrap();
        let mut req = window_request(&state, MINUTES, 2, 2, 2);
        req.restored_snapshots = first[0].snapshots.clone();
        // Restored at the record's watermark, the lanes read their timers.
        event_time(&mut req).floor = Some(least_mark(&first[0]));
        let mut h = start_lanes(req);
        h.end_all(2).await;
        let err = h.job.await.unwrap().unwrap_err();
        assert_eq!(err.code, ErrorCode::CheckpointCorrupt);
        assert!(err.message.contains("is missing"), "{}", err.message);
    }

    fn least_mark(r: &EpochReady) -> i64 {
        split_marks(r, "a")
            .into_iter()
            .map(|(_, m)| m)
            .min()
            .unwrap()
    }

    /// A cell the stream holds, in a column the window doesn't read, leaves
    /// its row in the window: a's price passes through holding it, and the
    /// window counts every row by email.
    #[tokio::test]
    async fn a_held_cell_the_window_doesnt_read_leaves_its_row_in() {
        let dir = tempfile::tempdir().unwrap();
        let mut req = window_request(&dir.path().join("state"), MINUTES, 1, 1, 1);
        let declared = arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("id", DataType::Int64, false),
            arrow_schema::Field::new("email", DataType::Utf8, true),
            arrow_schema::Field::new("v", DataType::Int64, true),
            arrow_schema::Field::new("price", DataType::Decimal128(10, 2), true),
            arrow_schema::Field::new(
                "at",
                DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        req.streams[0].arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        let inputs: Vec<crate::transform::Input> = req
            .streams
            .iter()
            .map(|s| crate::transform::Input::of(s).unwrap())
            .collect();
        let planned = crate::transform::Transform::plan(MINUTES, inputs).unwrap();
        req.transform.as_mut().unwrap().operators =
            planned.operators().map(str::to_owned).collect();
        req.outputs.push(openetl_proto::engine::v1::StreamPlan {
            name: "a".into(),
            primary_key: vec!["id".into()],
            holds: Some(openetl_proto::engine::v1::HoldPlan {
                cells: true,
                ..Default::default()
            }),
            ..Default::default()
        });
        for e in &mut req.edges {
            if e.direction() == openetl_proto::engine::v1::EdgeDirection::FromEngine {
                e.streams.push("a".into());
            }
        }
        req.held_url = format!("file://{}/held", dir.path().display());
        req.quarantine_url = format!("file://{}/q", dir.path().display());
        let mut h = start_lanes(req);
        let w = &mut h.ins[0];
        let priced = |id: i64, price: &str, second: i64| {
            format!(
                r#"{{"id": {id}, "email": "x", "v": {id}, "price": "{price}", "at": "1970-01-01T00:00:{second:02}Z"}}"#
            )
        };
        put(w, FrameKind::Split, 0, split("s", None)).await;
        put(
            w,
            FrameKind::Rows,
            0,
            rows(&[&priced(1, "1.5", 10), &priced(2, "12345678901.5", 20)]),
        )
        .await;
        put(w, FrameKind::Split, 0, split("s", Some(watermark::END))).await;
        barriers(w, 1, 1).await;
        h.end_all(2).await;
        let (outs, ready) = h.finish().await;
        assert_eq!(all_rows(&outs, 1), [row(0, "x", 2, 1)]);
        assert_eq!(
            ready[0].holds["a"].cells["price"].cells, 1,
            "a holds the price"
        );
        assert_eq!(quarantined(&ready, "minutes"), 0);
    }

    /// JSON keys equal but for their spaces and their objects' order are one
    /// key: they meet on one keyed lane, from any lane, and give one row,
    /// whose key is JSON.
    #[tokio::test]
    async fn equal_json_keys_are_one_window_key() {
        let json = arrow_schema::Field::new("j", DataType::Utf8, true).with_metadata(
            [(
                openetl_normalize::EXTENSION_NAME_KEY.to_owned(),
                openetl_normalize::JSON_EXTENSION.to_owned(),
            )]
            .into(),
        );
        let sql = "INSERT INTO minutes SELECT window_start, j, count(*) AS n \
                   FROM a GROUP BY tumble(at, INTERVAL '1 minute'), j";
        let state = tempfile::tempdir().unwrap();
        let req = window_request_by(state.path(), sql, json, 3, 1, 1);
        let mut h = start_lanes(req);
        let spellings = [
            r#"{"a": 1, "b": {"y": 2, "x": [3, {"q": 4, "p": 5}]}}"#,
            r#"{"b": {"x": [3, {"p": 5, "q": 4}], "y": 2}, "a": 1}"#,
            r#"{ "b" : { "y" : 2 , "x" : [ 3 , { "q" : 4 , "p" : 5 } ] } , "a" : 1 }"#,
        ];
        for (l, w) in h.ins.iter_mut().enumerate() {
            put(w, FrameKind::Split, 0, split(&format!("s{l}"), None)).await;
            let row = format!(
                r#"{{"id": {l}, "j": {}, "v": 1, "at": "1970-01-01T00:00:1{l}Z"}}"#,
                spellings[l]
            );
            put(w, FrameKind::Rows, 0, rows(&[&row])).await;
            put(
                w,
                FrameKind::Split,
                0,
                split(&format!("s{l}"), Some(watermark::END)),
            )
            .await;
            barriers(w, 1, 1).await;
        }
        h.end_all(2).await;
        let (outs, _) = h.finish().await;
        let mut got = Vec::new();
        for frames in &outs {
            for b in batches(frames, 0) {
                let f = b.schema().field_with_name("j").unwrap().clone();
                assert_eq!(
                    f.metadata()
                        .get(openetl_normalize::EXTENSION_NAME_KEY)
                        .map(String::as_str),
                    Some(openetl_normalize::JSON_EXTENSION),
                    "the output's key is JSON"
                );
                for (j, n) in column_strings(&b, "j")
                    .into_iter()
                    .zip(column_ints(&b, "n"))
                {
                    got.push((j.unwrap(), n.unwrap()));
                }
            }
        }
        assert_eq!(
            got,
            [(r#"{"a":1,"b":{"x":[3,{"p":5,"q":4}],"y":2}}"#.to_owned(), 3)]
        );
    }

    /// A group window runs in one engine with every lane of the attempt, so
    /// an engine with a part of one is refused.
    #[test]
    fn a_group_window_runs_in_one_engine() {
        type Change = Box<dyn Fn(&mut openetl_proto::engine::v1::StartJobRequest)>;
        let refused: Vec<(&str, Change)> = vec![
            ("a worker's part", Box::new(|r| r.attempt_writer_lanes = 2)),
            ("a later worker's lanes", Box::new(|r| r.first_lane = 1)),
            (
                "a mesh",
                Box::new(|r| {
                    r.attempt_writer_lanes = 2;
                    r.mesh = Some(openetl_proto::engine::v1::Mesh {
                        engine: 0,
                        engines: (0..2)
                            .map(|i| openetl_proto::engine::v1::MeshEngine {
                                address: format!("127.0.0.1:{}", 7000 + i),
                                lanes: 1,
                                writer_lanes: 1,
                            })
                            .collect(),
                        listen: "127.0.0.1:7000".into(),
                        certificate: vec![1],
                        key: vec![1],
                        secret: vec![1],
                    });
                }),
            ),
        ];
        let state = tempfile::tempdir().unwrap();
        assert!(Plan::from_request(window_request(state.path(), MINUTES, 1, 1, 1)).is_ok());
        let mut redo = window_request(state.path(), MINUTES, 1, 1, 1);
        (redo.first_lane, redo.state_read_only) = (3, true);
        assert!(Plan::from_request(redo).is_ok(), "a redo runs on one lane");
        for (name, change) in refused {
            let mut req = window_request(state.path(), MINUTES, 1, 1, 1);
            change(&mut req);
            let Err(e) = Plan::from_request(req) else {
                panic!("{name}: planned");
            };
            assert!(
                e.message.contains("runs in one engine"),
                "{name}: {}",
                e.message
            );
        }
    }

    /// State the checkpoint names of an operator no statement keeps, as
    /// after the statement changed, is refused, saying how to drop it.
    #[test]
    fn a_restore_of_an_operator_no_statement_keeps_says_to_reset() {
        let state = tempfile::tempdir().unwrap();
        let mut req = window_request(state.path(), MINUTES, 1, 2, 2);
        req.restored_snapshots = vec![openetl_proto::engine::v1::StateSnapshot {
            operator: "window-0123456789abcdef".into(),
            key_groups: Some(openetl_proto::engine::v1::KeyGroupRange {
                first: 0,
                last: 255,
            }),
            layout: openetl_state::LAYOUT,
            data: openetl_state::runs::encode(&[]).to_vec(),
            runs: Vec::new(),
        }];
        let Err(e) = Plan::from_request(req) else {
            panic!("planned");
        };
        assert!(
            e.message.contains("no statement of transform.sql computes, and which the pipeline's version didn't drop")
                && e.message.contains("openetl reset --stream STREAM NAME"),
            "{}",
            e.message
        );
    }

    /// Splits spread over lanes, each lane's rows of its splits in an order
    /// of its own and the lanes in any order against each other, with
    /// barriers anywhere, and each split's rows within the delay of its
    /// latest, so none is late: each window's row comes once, with every
    /// row of it, whatever the window.
    #[tokio::test]
    async fn every_window_closes_once_with_all_its_rows_under_any_interleaving() {
        let queries = [
            "INSERT INTO minutes SELECT window_start, email, count(*) AS n, sum(v) AS total \
             FROM a GROUP BY tumble(at, INTERVAL '10 seconds'), email",
            "INSERT INTO minutes SELECT window_start, email, count(*) AS n, sum(v) AS total \
             FROM a GROUP BY hop(at, INTERVAL '10 seconds', INTERVAL '30 seconds'), email",
            "INSERT INTO minutes SELECT window_start, email, count(*) AS n, sum(v) AS total \
             FROM a GROUP BY session(at, INTERVAL '4 seconds'), email",
        ];
        let mut rng = crate::watermark::tests::Rng(7);
        for (q, sql) in queries.iter().enumerate() {
            for seed in 0..8 {
                let lanes = 1 + rng.below(3) as u32;
                let splits = 1 + rng.below(5);
                let delay = rng.below(6) as i64;
                // Each split's rows: times that rise, but within the delay of
                // the latest, each a key and a value.
                let mut all: Vec<(i64, &str, i64)> = Vec::new();
                let per: Vec<Vec<(i64, &str, i64)>> = (0..splits)
                    .map(|_| {
                        let mut top = delay + rng.below(20) as i64;
                        (0..rng.below(40))
                            .map(|_| {
                                top += rng.below(4) as i64;
                                let t = top - rng.below(delay as usize + 1) as i64;
                                let r = (t, ["x", "y", "z"][rng.below(3)], rng.below(100) as i64);
                                all.push(r);
                                r
                            })
                            .collect()
                    })
                    .collect();
                let state = tempfile::tempdir().unwrap();
                let mut req = window_request(state.path(), sql, lanes, 1, 1);
                event_time(&mut req).delay_micros = delay * SEC;
                let mut h = start_lanes(req);
                let mut epochs = 0;
                for (l, w) in h.ins.iter_mut().enumerate() {
                    let mine: Vec<usize> =
                        (0..splits).filter(|s| s % lanes as usize == l).collect();
                    for s in &mine {
                        put(w, FrameKind::Split, 0, split(&s.to_string(), None)).await;
                    }
                    let mut at = vec![0; splits];
                    let mut n = 0;
                    let mut lane_rng = crate::watermark::tests::Rng(seed * 31 + l as u64);
                    while mine.iter().any(|&s| at[s] < per[s].len()) {
                        let s = mine[lane_rng.below(mine.len())];
                        if at[s] == per[s].len() {
                            continue;
                        }
                        let k = (1 + lane_rng.below(4)).min(per[s].len() - at[s]);
                        let rs: Vec<String> = per[s][at[s]..at[s] + k]
                            .iter()
                            .map(|&(t, e, v)| {
                                format!(r#"{{"id": {v}, "email": "{e}", "v": {v}, "at": "1970-01-01T00:{:02}:{:02}Z"}}"#, t / 60, t % 60)
                            })
                            .collect();
                        at[s] += k;
                        put(w, FrameKind::Split, 0, split(&s.to_string(), None)).await;
                        let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                        put(w, FrameKind::Rows, 0, rows(&refs)).await;
                        // Every lane passes the same barriers: one after
                        // every few batches of its own, up to its share.
                        if n < 3 && lane_rng.below(4) == 0 {
                            n += 1;
                            barriers(w, 1, n).await;
                        }
                    }
                    for s in &mine {
                        put(
                            w,
                            FrameKind::Split,
                            0,
                            split(&s.to_string(), Some(watermark::END)),
                        )
                        .await;
                    }
                    while n < 4 {
                        n += 1;
                        barriers(w, 1, n).await;
                    }
                    epochs = n;
                }
                h.end_all(2).await;
                let (outs, ready) = h.finish().await;
                assert_eq!(ready.len() as u64, epochs);

                // What a GROUP BY of every row gives.
                let mut want: BTreeMap<(i64, String), (i64, i64)> = BTreeMap::new();
                let mut add = |start: i64, e: &str, v: i64| {
                    let w = want.entry((start, e.to_owned())).or_default();
                    w.0 += 1;
                    w.1 += v;
                };
                match q {
                    0 => all
                        .iter()
                        .for_each(|&(t, e, v)| add(t.div_euclid(10) * 10, e, v)),
                    1 => all.iter().for_each(|&(t, e, v)| {
                        let mut s = t.div_euclid(10) * 10;
                        while s > t - 30 {
                            add(s, e, v);
                            s -= 10;
                        }
                    }),
                    _ => {
                        for key in ["x", "y", "z"] {
                            let mut mine: Vec<(i64, i64)> = all
                                .iter()
                                .filter(|r| r.1 == key)
                                .map(|r| (r.0, r.2))
                                .collect();
                            mine.sort();
                            let mut start = None;
                            let mut last = i64::MIN;
                            for (t, v) in mine {
                                if start.is_none() || t > last + 4 {
                                    start = Some(t);
                                }
                                last = t;
                                add(start.unwrap(), key, v);
                            }
                        }
                    }
                }
                let mut got: BTreeMap<(i64, String), (i64, i64)> = BTreeMap::new();
                for frames in &outs {
                    for b in batches(frames, 0) {
                        let starts: Vec<i64> = b
                            .column_by_name("window_start")
                            .unwrap()
                            .as_primitive::<arrow_array::types::TimestampMicrosecondType>()
                            .iter()
                            .map(|s| s.unwrap() / SEC)
                            .collect();
                        let keys = column_strings(&b, "email");
                        let ns = column_ints(&b, "n");
                        let totals = column_ints(&b, "total");
                        for i in 0..b.num_rows() {
                            let k = (starts[i], keys[i].clone().unwrap());
                            let v = (ns[i].unwrap(), totals[i].unwrap());
                            assert!(
                                got.insert(k.clone(), v).is_none(),
                                "{sql}, seed {seed}: {k:?} came twice"
                            );
                        }
                    }
                }
                assert_eq!(got, want, "{sql}, seed {seed}");
            }
        }
    }

    /// Stream e, keyed by id, whose time is its column at, with a delay of
    /// 10µs: it passes through, and a statement writes c from it, which is
    /// exchanged by id where there's more than one writer lane.
    pub(crate) fn timed_request(
        lanes: u32,
        writers: u32,
        held: bool,
        floor: Option<i64>,
    ) -> openetl_proto::engine::v1::StartJobRequest {
        use arrow_schema::{Field, Schema, TimeUnit};
        let mut req = crate::plan::tests::lanes_request(&["e"], lanes, writers);
        let declared = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "at",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
        ]);
        let s = &mut req.streams[0];
        s.primary_key = vec!["id".into()];
        s.arrow_schema = ipc::encode_schema(&Arc::new(declared)).unwrap();
        s.event_time = Some(openetl_proto::engine::v1::EventTime {
            column: "at".into(),
            delay_micros: 10,
            held,
            floor,
            ..Default::default()
        });
        req.outputs = ["c", "e"]
            .iter()
            .map(|o| openetl_proto::engine::v1::StreamPlan {
                name: o.to_string(),
                primary_key: vec!["id".into()],
                ..Default::default()
            })
            .collect();
        if writers > 1 {
            req.outputs[0].exchange = Some(openetl_proto::engine::v1::Exchange {
                columns: vec!["id".into()],
            });
        }
        for e in &mut req.edges {
            if e.direction() == openetl_proto::engine::v1::EdgeDirection::FromEngine {
                e.streams = vec!["c".into(), "e".into()];
            }
        }
        req.transform = Some(openetl_proto::engine::v1::Transform {
            sql: "INSERT INTO c SELECT id, at FROM e".into(),
            ..Default::default()
        });
        req
    }

    /// A row of e at `us` microseconds since the Unix epoch.
    fn timed(id: i64, us: i64) -> String {
        format!(r#"{{"id": {id}, "at": "1970-01-01T00:00:00.{us:06}Z"}}"#)
    }

    pub(crate) fn split(id: &str, wm: Option<i64>) -> Vec<u8> {
        Split {
            id: id.into(),
            watermark: wm,
        }
        .encode_to_vec()
    }

    fn start_probed(
        req: openetl_proto::engine::v1::StartJobRequest,
    ) -> (LaneHarness, crate::stage::Probe) {
        let mut plan = Plan::from_request(req).unwrap();
        let probe = crate::stage::Probe::default();
        plan.probe = Some(probe.clone());
        let plan = Arc::new(plan);
        let (mut ins, mut readers, mut outs, mut writers) = (vec![], vec![], vec![], vec![]);
        for _ in 0..plan.inbound.len() {
            let (w, r) = duplex(1 << 16);
            ins.push(w);
            readers.push(BufReader::new(r));
        }
        for _ in 0..plan.writer_lanes {
            let (w, r) = duplex(1 << 16);
            writers.push(w);
            outs.push(BufReader::new(r));
        }
        let (tx, events) = mpsc::channel(64);
        let job = tokio::spawn(run(plan, readers, writers, tx));
        (
            LaneHarness {
                ins,
                outs,
                events,
                job,
            },
            probe,
        )
    }

    fn last_metrics(
        events: &mut mpsc::Receiver<Result<EventsResponse, Status>>,
    ) -> (Vec<EpochReady>, HashMap<String, Option<i64>>) {
        let (mut ready, mut marks) = (Vec::new(), HashMap::new());
        while let Ok(Ok(ev)) = events.try_recv() {
            match ev.event {
                Some(events_response::Event::EpochReady(r)) => ready.push(r),
                Some(events_response::Event::Metrics(m)) => {
                    marks = m
                        .streams
                        .into_iter()
                        .map(|s| (s.stream, s.watermark))
                        .collect();
                }
                None => {}
            }
        }
        (ready, marks)
    }

    fn split_marks(r: &EpochReady, stream: &str) -> Vec<(String, i64)> {
        let mut m: Vec<(String, i64)> = r.watermarks[stream]
            .splits
            .iter()
            .map(|(k, v)| (k.clone(), *v))
            .collect();
        m.sort();
        m
    }

    /// A split's watermark is the latest time it has read less the delay, or
    /// what its Split frames raise it to; a lane's is the least of its
    /// splits'. Each advance reaches the stream's statements after the batch
    /// that moved it, and the epoch reports each split's at its barrier.
    #[tokio::test]
    async fn a_lanes_watermark_is_the_least_of_its_splits_in_order_with_its_rows() {
        let (mut h, probe) = start_probed(timed_request(1, 1, false, None));
        use FrameKind as K;
        let w = &mut h.ins[0];
        put(w, K::Split, 0, split("a", None)).await;
        put(w, K::Split, 0, split("b", Some(50))).await;
        put(w, K::Split, 0, split("a", None)).await;
        put(w, K::Rows, 0, rows(&[&timed(1, 100)])).await;
        put(w, K::Rows, 0, rows(&[&timed(2, 300), &timed(3, 250)])).await;
        put(w, K::Split, 0, split("b", None)).await;
        put(w, K::Rows, 0, rows(&[&timed(4, 120)])).await;
        put(w, K::Barrier, 0, barrier(1)).await;
        put(w, K::Split, 0, split("b", Some(watermark::END))).await;
        put(w, K::Barrier, 0, barrier(2)).await;
        h.end_all(1).await;
        let (_, mut events) = h.finish_events().await;

        use crate::stage::Seen::{Rows as R, Watermark as W};
        assert_eq!(probe.take(), [R(1), W(50), R(2), R(1), W(110), W(290)]);
        let (ready, marks) = last_metrics(&mut events);
        assert_eq!(ready.len(), 2);
        let s = |v: &[(&str, i64)]| {
            v.iter()
                .map(|(k, w)| (k.to_string(), *w))
                .collect::<Vec<_>>()
        };
        assert_eq!(split_marks(&ready[0], "e"), s(&[("a", 290), ("b", 110)]));
        assert_eq!(
            split_marks(&ready[1], "e"),
            s(&[("a", 290), ("b", watermark::END)])
        );
        assert_eq!(marks["e"], Some(290));
        assert_eq!(marks["c"], Some(290), "the writer lane's");
    }

    /// Past an exchange, a writer lane starts at the stream's floor and
    /// takes the least of every lane that feeds it; a lane without splits
    /// holds none back from its first barrier.
    #[tokio::test]
    async fn an_exchange_takes_the_least_of_every_sender_from_the_floor() {
        let (mut h, _) = start_probed(timed_request(2, 2, false, Some(40)));
        use FrameKind as K;
        put(&mut h.ins[1], K::Split, 0, split("a", Some(60))).await;
        put(
            &mut h.ins[1],
            K::Rows,
            0,
            rows(&[&timed(1, 65), &timed(2, 66)]),
        )
        .await;
        put(&mut h.ins[1], K::Barrier, 0, barrier(1)).await;
        put(&mut h.ins[0], K::Barrier, 0, barrier(1)).await;
        h.end_all(1).await;
        let (_, mut events) = h.finish_events().await;
        let (ready, marks) = last_metrics(&mut events);
        assert_eq!(ready.len(), 1);
        assert_eq!(
            split_marks(&ready[0], "e"),
            [("a".to_string(), 60)],
            "lane 0 has no split to report"
        );
        assert_eq!(marks["c"], Some(60), "lane 0 holds none back");
        assert_eq!(marks["e"], Some(60));
    }

    /// A stream read in another order than its time moves only by its Split
    /// frames, and END closes it.
    #[tokio::test]
    async fn a_held_streams_rows_move_nothing() {
        let (mut h, probe) = start_probed(timed_request(1, 1, true, None));
        use FrameKind as K;
        let w = &mut h.ins[0];
        put(w, K::Split, 0, split("a", None)).await;
        put(w, K::Rows, 0, rows(&[&timed(1, 900)])).await;
        put(w, K::Split, 0, split("a", Some(watermark::END))).await;
        put(w, K::Barrier, 0, barrier(1)).await;
        h.end_all(1).await;
        let (_, mut events) = h.finish_events().await;
        use crate::stage::Seen::{Rows as R, Watermark as W};
        assert_eq!(probe.take(), [R(1), W(watermark::END)]);
        let (ready, _) = last_metrics(&mut events);
        assert_eq!(
            split_marks(&ready[0], "e"),
            [("a".to_string(), watermark::END)]
        );
    }

    /// A restore gives each split its recorded watermark on its first Split
    /// frame: the stream's statements are told the least of them before any
    /// batch.
    #[tokio::test]
    async fn a_restored_lane_starts_at_its_splits_watermarks() {
        let (mut h, probe) = start_probed(timed_request(1, 1, false, Some(70)));
        use FrameKind as K;
        let w = &mut h.ins[0];
        put(w, K::Split, 0, split("a", Some(80))).await;
        put(w, K::Split, 0, split("b", Some(70))).await;
        put(w, K::Rows, 0, rows(&[&timed(1, 75)])).await;
        put(w, K::Barrier, 0, barrier(1)).await;
        h.end_all(1).await;
        let _ = h.finish_events().await;
        use crate::stage::Seen::{Rows as R, Watermark as W};
        assert_eq!(probe.take(), [W(70), R(1)]);
    }

    #[tokio::test]
    async fn a_split_frame_for_a_stream_without_event_time_fails() {
        let mut req = timed_request(1, 1, false, None);
        req.streams[0].event_time = None;
        let (mut h, _) = start_probed(req);
        put(&mut h.ins[0], FrameKind::Split, 0, split("a", None)).await;
        let err = h.job.await.unwrap().unwrap_err();
        assert!(err.message.contains("no event time"), "{err:?}");
    }

    /// Held or not.
    #[tokio::test]
    async fn an_event_time_that_isnt_a_time_fails_the_job() {
        for held in [false, true] {
            let mut req = timed_request(1, 1, held, None);
            req.streams[0].event_time.as_mut().unwrap().column = "id".into();
            let (mut h, _) = start_probed(req);
            put(&mut h.ins[0], FrameKind::Split, 0, split("a", None)).await;
            put(&mut h.ins[0], FrameKind::Rows, 0, rows(&[&timed(1, 5)])).await;
            let err = h.job.await.unwrap().unwrap_err();
            assert_eq!(err.code, ErrorCode::ConfigInvalid, "held {held}: {err:?}");
            assert!(
                err.message.contains("not a timestamp"),
                "held {held}: {err:?}"
            );
        }
    }

    /// A split without a watermark is missing from the epoch's report.
    #[tokio::test]
    async fn a_split_without_a_watermark_isnt_reported() {
        let (mut h, _) = start_probed(timed_request(1, 1, false, None));
        let w = &mut h.ins[0];
        put(w, FrameKind::Split, 0, split("a", None)).await;
        put(w, FrameKind::Split, 0, split("b", Some(50))).await;
        put(w, FrameKind::Barrier, 0, barrier(1)).await;
        h.end_all(1).await;
        let (_, mut events) = h.finish_events().await;
        let (ready, _) = last_metrics(&mut events);
        assert_eq!(split_marks(&ready[0], "e"), [("b".to_string(), 50)]);
    }

    fn feed_items(feed: &mut Feed) -> Vec<String> {
        let mut out = Vec::new();
        while let Ok(Next::Item(o)) = feed.try_next() {
            out.push(match o {
                Out::Watermark(s, wm) => format!("{s}:wm {wm}"),
                Out::Barrier(s, e) => format!("{s}:barrier {}", e.n),
                Out::End(s) => format!("{s}:end"),
                _ => "other".into(),
            });
        }
        out
    }

    /// A writer lane aligns watermarks with barriers: what a lane sends past
    /// its barrier waits for the others', so at the barrier the writer lane
    /// has the least of what each lane had at its own.
    #[tokio::test]
    async fn a_writer_lanes_watermark_at_a_barrier_is_its_lanes_at_theirs() {
        let plan = Plan::from_request(timed_request(2, 1, false, None)).unwrap();
        let (tx, rx) = mpsc::channel(16);
        let mut feed = Feed::new(&plan, 0, rx);
        let c = 0;
        let epoch = Epoch {
            generation: 1,
            n: 1,
        };
        for (lane, out) in [
            (0, Out::Watermark(c, 100)),
            (0, Out::Barrier(c, epoch)),
            (0, Out::Watermark(c, 200)),
            (1, Out::Watermark(c, 150)),
        ] {
            tx.send((lane, out)).await.unwrap();
        }
        assert_eq!(feed_items(&mut feed), ["0:wm 100"]);
        tx.send((1, Out::Barrier(c, epoch))).await.unwrap();
        assert_eq!(feed_items(&mut feed), ["0:barrier 1", "0:wm 150"]);
        tx.send((1, Out::Watermark(c, 120))).await.unwrap();
        assert!(feed.try_next().is_err(), "a sender's watermark went back");
    }

    /// A writer lane restored with a floor starts at it, before anything a
    /// lane sends.
    #[tokio::test]
    async fn a_writer_lane_starts_at_the_floor() {
        let plan = Plan::from_request(timed_request(2, 1, false, Some(40))).unwrap();
        let (tx, rx) = mpsc::channel(16);
        let mut feed = Feed::new(&plan, 0, rx);
        tx.send((0, Out::Watermark(0, 30))).await.unwrap();
        tx.send((1, Out::Watermark(0, 50))).await.unwrap();
        assert_eq!(feed_items(&mut feed), ["0:wm 40", "1:wm 40"]);
        tx.send((0, Out::Watermark(0, 45))).await.unwrap();
        assert_eq!(feed_items(&mut feed), ["0:wm 45"]);
    }
}
