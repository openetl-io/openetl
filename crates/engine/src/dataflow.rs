//! The P0 dataflow: every inbound stream passes through to the sink, with raw
//! rows normalized to Arrow. A runner sink gets barriers in band; a Parquet
//! or staged sink stages each epoch and reports it in EpochReady.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::ops::RangeInclusive;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use openetl_formats::ParquetStaging;
use openetl_normalize::{Error as NormalizeError, Shaper};
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, CellHolds, ColumnHold, EpochReady, EventsResponse, FrameKind, Metrics, QuarantineFile,
    Rows, SchemaChange, StagedStream, StreamHolds, StreamMetrics, events_response,
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
use crate::plan::{Plan, Sink};
use crate::replay::Replayer;
use crate::staging::Staging;

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

/// What an inbound edge hands to the outbound one. Streams are indexes into
/// Plan::streams.
enum Out {
    Batch(usize, RecordBatch),
    /// An encoded SchemaChange message.
    SchemaChange(usize, Vec<u8>),
    /// Held values of the stream's current epoch.
    Held(usize, RecordBatch),
    Patch(usize, RecordBatch),
    /// With the stream's state at the barrier, and what it quarantined in
    /// the epoch.
    Barrier(usize, Epoch, At, Option<QuarantineFile>),
    End(usize, At),
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

type Writer = Pin<Box<dyn Future<Output = Result<bool, JobError>> + Send>>;

/// Runs the job until every stream has ended at the sink and every inbound
/// edge has closed. `outbound` is the runner sink's edge. Failures are
/// reported on `events` before the edges close.
pub async fn run<R, W>(
    plan: Arc<Plan>,
    inbound: Vec<R>,
    outbound: Option<W>,
    events: Events,
) -> Result<(), JobError>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let counters: Arc<Vec<Counters>> =
        Arc::new(plan.streams.iter().map(|_| Counters::default()).collect());
    let (tx, rx) = mpsc::channel(16);
    let mut writer: Writer = match (&plan.sink, outbound) {
        (Sink::Runner { .. }, Some(w)) => Box::pin(write_edge(
            plan.clone(),
            w,
            rx,
            events.clone(),
            counters.clone(),
        )),
        (Sink::Parquet { url }, None) => {
            let (plan, url, events, counters) =
                (plan.clone(), url.clone(), events.clone(), counters.clone());
            let span = Span::current();
            Box::pin(async move {
                tokio::task::spawn_blocking(move || {
                    span.in_scope(|| write_parquet(&plan, &url, rx, &events, &counters))
                })
                .await
                .map_err(|e| JobError::internal(format!("the Parquet sink failed: {e}")))?
            })
        }
        (Sink::Staged { url, dir }, None) => Box::pin(write_staged(
            plan.clone(),
            url.clone(),
            dir.clone(),
            rx,
            events.clone(),
            counters.clone(),
        )),
        (Sink::Discard, None) => Box::pin(write_discard(
            plan.clone(),
            rx,
            events.clone(),
            counters.clone(),
        )),
        _ => {
            let e = JobError::internal("the outbound edge doesn't match the sink");
            report(&events, &e).await;
            return Err(e);
        }
    };
    let mut readers = JoinSet::new();
    for (edge, r) in inbound.into_iter().enumerate() {
        readers.spawn(
            read_edge(plan.clone(), edge, r, tx.clone(), counters.clone()).in_current_span(),
        );
    }
    drop(tx);

    let mut written = None;
    let res = loop {
        tokio::select! {
            r = &mut writer, if written.is_none() => match r {
                Ok(complete) => written = Some(complete),
                Err(e) => break Err(e),
            },
            Some(joined) = readers.join_next() => match joined {
                Ok(Ok(())) => {}
                Ok(Err(e)) => break Err(e),
                Err(e) => break Err(JobError::internal(format!("an edge reader failed: {e}"))),
            },
            else => break match written {
                Some(true) => Ok(()),
                _ => Err(JobError::internal("the inbound edges stopped early")),
            },
        }
    };
    if let Err(e) = &res {
        report(&events, e).await;
    }
    res
}

async fn read_edge<R>(
    plan: Arc<Plan>,
    edge: usize,
    mut r: R,
    tx: mpsc::Sender<Out>,
    counters: Arc<Vec<Counters>>,
) -> Result<(), JobError>
where
    R: AsyncBufRead + Unpin,
{
    let id = &plan.inbound[edge].id;
    let streams = &plan.inbound[edge].streams;
    let mut shape = Vec::with_capacity(streams.len());
    for &s in streams {
        let sh = Shaper::new(
            plan.naming,
            plan.tables[s].clone(),
            plan.identities[s].clone(),
            plan.policy,
            plan.restored[s].clone(),
            plan.declared[s].clone(),
            plan.types[s],
        )
        .map_err(|e| JobError::config(format!("stream {}: {e}", plan.streams[s])))?
        .with_soft_deletes(plan.soft_deletes[s])
        .with_changelog(plan.changelog[s]);
        let sh = match &plan.holds[s] {
            Some(h) => sh
                .with_holds(h.clone())
                .map_err(|e| JobError::config(format!("stream {}: {e}", plan.streams[s])))?,
            None => sh,
        };
        shape.push(sh);
    }
    let mut replays = Vec::with_capacity(streams.len());
    for (local, &s) in streams.iter().enumerate() {
        replays.push(match (&plan.replays[s], &plan.held_url) {
            (Some(r), Some(url)) => {
                // Patches go out at the types the destination will have.
                shape[local]
                    .version(&r.versioned)
                    .and_then(|_| shape[local].declare())
                    .map_err(|e| shape_error(&plan.streams[s], e))?;
                Some(Replayer::open(url, r.clone())?)
            }
            _ => None,
        });
    }
    let mut next_epoch = vec![plan.first_epoch; streams.len()];
    let mut ended = vec![false; streams.len()];
    // Each stream's quarantined lines since its last barrier, and how many.
    let mut quarantined: Vec<(Vec<u8>, u64)> = vec![(Vec::new(), 0); streams.len()];
    // Each stream's held cells since its last barrier, by column.
    let mut cells: Vec<BTreeMap<String, CellHolds>> = vec![BTreeMap::new(); streams.len()];
    let protocol = |msg: String| JobError::internal(format!("edge {id}: {msg}"));

    loop {
        let f = match frame::read(&mut r).await {
            Ok(Some(f)) => f,
            Ok(None) if ended.iter().all(|e| *e) => return Ok(()),
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
        let Some(&stream) = streams.get(local) else {
            return Err(protocol(format!(
                "frame for stream {local}, which the edge doesn't carry"
            )));
        };
        if ended[local] {
            return Err(protocol(format!(
                "frame after the end of stream {}",
                plan.streams[stream]
            )));
        }
        let c = &counters[stream];
        c.bytes_in
            .fetch_add(f.payload.len() as u64, Ordering::Relaxed);
        let out = match f.kind {
            FrameKind::Rows if plan.passthrough => {
                return Err(protocol("rows in a passthrough job".into()));
            }
            FrameKind::Rows => {
                let rows =
                    Rows::decode(f.payload.as_slice()).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(rows.rows.len() as u64, Ordering::Relaxed);
                let n = debug_span!("batch", openetl.stream = %plan.streams[stream], rows = rows.rows.len())
                    .in_scope(|| shape[local].rows(rows.rows.iter().map(Vec::as_slice)))
                    .map_err(|e| shape_error(&plan.streams[stream], e))?;
                let record = |row: usize| String::from_utf8_lossy(&rows.rows[row]).into_owned();
                hold(
                    &plan,
                    stream,
                    &n.rejected,
                    record,
                    &mut quarantined[local],
                    c,
                )?;
                match shaped(&tx, stream, n, &plan, &mut cells[local], c).await? {
                    Some(out) => out,
                    None => continue,
                }
            }
            FrameKind::Batch => {
                let batch = ipc::decode_batch(f.payload).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                if plan.passthrough {
                    send(&tx, Out::Batch(stream, batch)).await?;
                    continue;
                }
                let n = debug_span!("batch", openetl.stream = %plan.streams[stream], rows = batch.num_rows())
                    .in_scope(|| shape[local].batch(&batch))
                    .map_err(|e| shape_error(&plan.streams[stream], e))?;
                let record = |row: usize| openetl_normalize::render_row(&batch, row);
                hold(
                    &plan,
                    stream,
                    &n.rejected,
                    record,
                    &mut quarantined[local],
                    c,
                )?;
                match shaped(&tx, stream, n, &plan, &mut cells[local], c).await? {
                    Some(out) => out,
                    None => continue,
                }
            }
            FrameKind::Barrier => {
                let epoch = Barrier::decode(f.payload.as_slice())
                    .map_err(|e| protocol(e.to_string()))?
                    .epoch
                    .unwrap_or_default();
                if epoch.generation != plan.generation || epoch.n != next_epoch[local] {
                    return Err(protocol(format!(
                        "stream {} crossed barrier ({}, {}) while in ({}, {})",
                        plan.streams[stream],
                        epoch.generation,
                        epoch.n,
                        plan.generation,
                        next_epoch[local]
                    )));
                }
                next_epoch[local] += 1;
                let (lines, rows) = std::mem::take(&mut quarantined[local]);
                let q = match &plan.quarantine {
                    Some(q) if rows > 0 => Some(QuarantineFile {
                        stream: plan.streams[stream].clone(),
                        url: q
                            .put(epoch.generation, epoch.n, &plan.streams[stream], lines)
                            .await?,
                        rows,
                    }),
                    _ => None,
                };
                shape[local].seal();
                let mut at = at(&plan, stream, &shape[local], replays[local].as_ref())?;
                if let Some(h) = &mut at.holds {
                    h.cells = std::mem::take(&mut cells[local]).into_iter().collect();
                }
                send(&tx, Out::Barrier(stream, epoch, at, q)).await?;
                if let Some(r) = &mut replays[local] {
                    replay(&tx, stream, r, &shape[local]).await?;
                }
                continue;
            }
            FrameKind::SchemaChange => Out::SchemaChange(stream, f.payload),
            FrameKind::EndOfStream => {
                ended[local] = true;
                Out::End(stream, at(&plan, stream, &shape[local], None)?)
            }
            k @ (FrameKind::Hello | FrameKind::Unspecified | FrameKind::Patch) => {
                return Err(protocol(format!("unexpected {} frame", k.as_str_name())));
            }
        };
        send(&tx, out).await?;
    }
}

/// Adds rejected rows to the stream's quarantine for the epoch, or fails the
/// job if it has no quarantine.
fn hold(
    plan: &Plan,
    stream: usize,
    rejected: &[openetl_normalize::Rejected],
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
            format!("stream {}: {}", plan.streams[stream], first.reason),
        ));
    }
    let (lines, count) = held;
    for r in rejected {
        let line = serde_json::json!({
            "stream": plan.streams[stream],
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

/// Sends a shaped batch's schema change and held values; returns the batch
/// to send, if it has rows.
async fn shaped(
    tx: &mpsc::Sender<Out>,
    stream: usize,
    n: openetl_normalize::Shaped,
    plan: &Plan,
    cells: &mut BTreeMap<String, CellHolds>,
    c: &Counters,
) -> Result<Option<Out>, JobError> {
    for v in &n.versioned {
        tracing::warn!(stream = %plan.streams[stream], "{v}");
    }
    if n.schema_changed {
        send(tx, schema_change(stream, &n.batch)?).await?;
    }
    for (column, reason) in n.cells {
        let e = cells
            .entry(column)
            .or_insert_with(|| CellHolds { cells: 0, reason });
        e.cells += 1;
        c.cells_held.fetch_add(1, Ordering::Relaxed);
    }
    for b in n.held {
        send(tx, Out::Held(stream, b)).await?;
    }
    Ok((n.batch.num_rows() > 0).then_some(Out::Batch(stream, n.batch)))
}

/// A stream's state, with its holds if it holds.
fn at(
    plan: &Plan,
    stream: usize,
    shaper: &Shaper,
    replay: Option<&Replayer>,
) -> Result<At, JobError> {
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
            holding.push(ColumnHold {
                column: h.column,
                delivered,
                reason: h.reason,
            });
        }
        at.holds = Some(StreamHolds {
            holding,
            cells: HashMap::new(),
            files: Vec::new(),
            replayed: replay.map(Replayer::progress),
        });
    }
    Ok(at)
}

/// Sends the replay's next chunk of patches, which land in the next epoch.
async fn replay(
    tx: &mpsc::Sender<Out>,
    stream: usize,
    r: &mut Replayer,
    shaper: &Shaper,
) -> Result<(), JobError> {
    for b in r.chunk().await? {
        let (patch, still) = shaper
            .patch(&b, r.columns())
            .map_err(|e| JobError::internal(format!("patches: {e}")))?;
        r.still_held(still);
        if let Some(p) = patch {
            send(tx, Out::Patch(stream, p)).await?;
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

async fn send(tx: &mpsc::Sender<Out>, out: Out) -> Result<(), JobError> {
    tx.send(out)
        .await
        .map_err(|_| JobError::internal("the outbound edge stopped"))
}

/// Which epochs every stream has crossed or ended, and each stream's schema
/// at those points.
struct Alignment {
    crossed: Vec<u64>,
    ended: Vec<bool>,
    ready: u64,
    highest: u64,
    /// States at barriers whose epoch isn't ready yet.
    at_barrier: Vec<VecDeque<(u64, At)>>,
    at_end: Vec<At>,
    quarantined: BTreeMap<u64, Vec<QuarantineFile>>,
    /// The job's span, and the span of the next epoch to become ready,
    /// which opened when the one before it was ready.
    job: Span,
    epoch: Span,
}

impl Alignment {
    fn new(streams: usize, first_epoch: u64) -> Self {
        let job = Span::current();
        Self {
            epoch: info_span!(parent: &job, "epoch", openetl.epoch = first_epoch),
            job,
            crossed: vec![first_epoch - 1; streams],
            ended: vec![false; streams],
            ready: first_epoch - 1,
            highest: first_epoch - 1,
            at_barrier: vec![VecDeque::new(); streams],
            at_end: vec![At::default(); streams],
            quarantined: BTreeMap::new(),
        }
    }

    fn cross(&mut self, stream: usize, n: u64, at: At, quarantined: Option<QuarantineFile>) {
        self.crossed[stream] = n;
        self.highest = self.highest.max(n);
        self.at_barrier[stream].push_back((n, at));
        if let Some(q) = quarantined {
            self.quarantined.entry(n).or_default().push(q);
        }
    }

    fn end(&mut self, stream: usize, at: At) {
        self.ended[stream] = true;
        self.at_end[stream] = at;
    }

    /// Each stream's state in ready epoch n. Call for each ready epoch, in
    /// order.
    fn states(&mut self, n: u64) -> Vec<(usize, At)> {
        let mut out = Vec::new();
        for (s, queue) in self.at_barrier.iter_mut().enumerate() {
            let mut at = None;
            while queue.front().is_some_and(|(m, _)| *m <= n) {
                at = queue.pop_front().map(|(_, a)| a);
            }
            out.push((s, at.unwrap_or_else(|| self.at_end[s].clone())));
        }
        out
    }

    fn done(&self) -> bool {
        self.ended.iter().all(|e| *e)
    }

    /// The epochs that became ready since the last call.
    fn advance(&mut self) -> RangeInclusive<u64> {
        let aligned = (0..self.crossed.len())
            .filter(|&s| !self.ended[s])
            .map(|s| self.crossed[s])
            .min()
            .unwrap_or(self.highest)
            .min(self.highest);
        let ready = self.ready + 1..=aligned;
        self.ready = self.ready.max(aligned);
        ready
    }
}

fn epoch_ready(
    plan: &Plan,
    align: &mut Alignment,
    n: u64,
    staged: Vec<u8>,
    files: Vec<StagedStream>,
    held: Vec<StagedStream>,
) -> Result<EventsResponse, JobError> {
    align.epoch = if align.done() && n >= align.highest {
        Span::none()
    } else {
        info_span!(parent: &align.job, "epoch", openetl.epoch = n + 1)
    };
    let encode = |schema: &SchemaRef| {
        ipc::encode_schema(schema).map_err(|e| JobError::internal(format!("encode a schema: {e}")))
    };
    let mut held: HashMap<String, Vec<_>> = held.into_iter().map(|s| (s.stream, s.files)).collect();
    let mut schemas = HashMap::new();
    let mut delivered = HashMap::new();
    let mut holds = HashMap::new();
    for (s, at) in align.states(n) {
        let name = &plan.streams[s];
        if let Some(sc) = &at.schema {
            schemas.insert(name.clone(), encode(sc)?);
        }
        if let Some(d) = &at.delivered {
            delivered.insert(name.clone(), encode(d)?);
        }
        if let Some(mut h) = at.holds {
            h.files = held.remove(name).unwrap_or_default();
            holds.insert(name.clone(), h);
        }
    }
    Ok(EventsResponse {
        event: Some(events_response::Event::EpochReady(EpochReady {
            epoch: Some(Epoch {
                generation: plan.generation,
                n,
            }),
            staged,
            snapshots: Vec::new(),
            schemas,
            quarantine: align.quarantined.remove(&n).unwrap_or_default(),
            files,
            holds,
            delivered,
        })),
    })
}

/// Returns false if the inbound edges closed before every stream ended.
async fn write_edge<W>(
    plan: Arc<Plan>,
    w: W,
    mut rx: mpsc::Receiver<Out>,
    events: Events,
    counters: Arc<Vec<Counters>>,
) -> Result<bool, JobError>
where
    W: AsyncWrite + Unpin,
{
    let Sink::Runner { edge, out_index } = &plan.sink else {
        return Err(JobError::internal("the sink has no edge"));
    };
    let id = &edge.id;
    let gone = |e: std::io::Error| JobError::runner_gone(format!("edge {id}: {e}"));
    let mut w = BufWriter::with_capacity(1 << 16, w);
    let mut align = Alignment::new(plan.streams.len(), plan.first_epoch);
    let mut held = match &plan.held_url {
        Some(url) => Some(Staging::open(
            url,
            "",
            plan.generation,
            plan.first_epoch,
            &plan.streams,
        )?),
        None => None,
    };

    while !align.done() {
        let item = match rx.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => {
                w.flush().await.map_err(gone)?;
                match rx.recv().await {
                    Some(item) => item,
                    None => return Ok(false),
                }
            }
            // A reader failed, and its error says why.
            Err(TryRecvError::Disconnected) => return Ok(false),
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
            Out::Barrier(s, epoch, at, quarantined) => {
                if let Some(h) = &mut held {
                    h.seal(s).await?;
                }
                align.cross(s, epoch.n, at, quarantined);
                (
                    FrameKind::Barrier,
                    s,
                    Barrier { epoch: Some(epoch) }.encode_to_vec(),
                )
            }
            Out::End(s, at) => {
                if let Some(h) = &mut held {
                    h.end(s).await?;
                }
                align.end(s, at);
                (FrameKind::EndOfStream, s, Vec::new())
            }
        };
        frame::write(&mut w, kind, out_index[stream], &payload)
            .await
            .map_err(gone)?;

        let ready = align.advance();
        if !ready.is_empty() {
            w.flush().await.map_err(gone)?;
            for n in ready {
                let held = held.as_mut().map(|h| h.take(n)).unwrap_or_default();
                // The runner records every EpochReady, so this waits.
                let ev = epoch_ready(&plan, &mut align, n, Vec::new(), Vec::new(), held)?;
                let _ = events.send(Ok(ev)).await;
            }
            let _ = events.try_send(Ok(metrics(&plan, &counters)));
        }
    }
    w.flush().await.map_err(gone)?;
    w.shutdown().await.map_err(gone)?;
    let _ = events.try_send(Ok(metrics(&plan, &counters)));
    Ok(true)
}

/// Stages every stream as Parquet, and reports each epoch's staged files in
/// its EpochReady. Runs on a blocking thread. Returns false if the inbound
/// edges closed before every stream ended.
fn write_parquet(
    plan: &Plan,
    url: &str,
    mut rx: mpsc::Receiver<Out>,
    events: &Events,
    counters: &[Counters],
) -> Result<bool, JobError> {
    let streams: Vec<(String, String)> = plan
        .streams
        .iter()
        .cloned()
        .zip(plan.tables.iter().cloned())
        .collect();
    let mut staging = ParquetStaging::new(url, plan.generation, plan.first_epoch, &streams)
        .map_err(sink_error)?;
    let mut align = Alignment::new(plan.streams.len(), plan.first_epoch);

    while !align.done() {
        let Some(item) = rx.blocking_recv() else {
            return Ok(false);
        };
        match item {
            Out::Batch(s, batch) => {
                staging.write(s, &batch).map_err(sink_error)?;
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            }
            // Each batch carries its schema.
            Out::SchemaChange(..) => {}
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch, at, quarantined) => {
                let bytes = staging.seal(s).map_err(sink_error)?;
                counters[s].bytes_out.fetch_add(bytes, Ordering::Relaxed);
                align.cross(s, epoch.n, at, quarantined);
            }
            Out::End(s, at) => {
                staging.end(s).map_err(sink_error)?;
                align.end(s, at);
            }
        }
        let ready = align.advance();
        if ready.is_empty() {
            continue;
        }
        for n in ready {
            let staged = serde_json::to_vec(&staging.take(n))
                .map_err(|e| JobError::internal(format!("encode the staged descriptor: {e}")))?;
            let ev = epoch_ready(plan, &mut align, n, staged, Vec::new(), Vec::new())?;
            let _ = events.blocking_send(Ok(ev));
        }
        let _ = events.try_send(Ok(metrics(plan, counters)));
    }
    let _ = events.try_send(Ok(metrics(plan, counters)));
    Ok(true)
}

/// Stages every stream as Parquet for a destination connector, and lists each
/// epoch's files in its EpochReady. Returns false if the inbound edges closed
/// before every stream ended.
async fn write_staged(
    plan: Arc<Plan>,
    url: String,
    dir: String,
    mut rx: mpsc::Receiver<Out>,
    events: Events,
    counters: Arc<Vec<Counters>>,
) -> Result<bool, JobError> {
    let mut staging = Staging::open(&url, &dir, plan.generation, plan.first_epoch, &plan.streams)?;
    let mut align = Alignment::new(plan.streams.len(), plan.first_epoch);
    while !align.done() {
        let Some(item) = rx.recv().await else {
            return Ok(false);
        };
        match item {
            Out::Batch(s, batch) => {
                staging.write(s, &batch).await?;
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            }
            // Each file carries its schema.
            Out::SchemaChange(..) => {}
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch, at, quarantined) => {
                let bytes = staging.seal(s).await?;
                counters[s].bytes_out.fetch_add(bytes, Ordering::Relaxed);
                align.cross(s, epoch.n, at, quarantined);
            }
            Out::End(s, at) => {
                staging.end(s).await?;
                align.end(s, at);
            }
        }
        let ready = align.advance();
        if ready.is_empty() {
            continue;
        }
        for n in ready {
            let ev = epoch_ready(
                &plan,
                &mut align,
                n,
                Vec::new(),
                staging.take(n),
                Vec::new(),
            )?;
            let _ = events.send(Ok(ev)).await;
        }
        let _ = events.try_send(Ok(metrics(&plan, &counters)));
    }
    let _ = events.try_send(Ok(metrics(&plan, &counters)));
    Ok(true)
}

/// Drops every batch, and reports each epoch ready with nothing staged.
/// Returns false if the inbound edges closed before every stream ended.
async fn write_discard(
    plan: Arc<Plan>,
    mut rx: mpsc::Receiver<Out>,
    events: Events,
    counters: Arc<Vec<Counters>>,
) -> Result<bool, JobError> {
    let mut align = Alignment::new(plan.streams.len(), plan.first_epoch);
    while !align.done() {
        let Some(item) = rx.recv().await else {
            return Ok(false);
        };
        match item {
            Out::Batch(s, batch) => {
                counters[s]
                    .rows_out
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            }
            Out::SchemaChange(..) => {}
            Out::Held(..) | Out::Patch(..) => return Err(no_holds()),
            Out::Barrier(s, epoch, at, quarantined) => align.cross(s, epoch.n, at, quarantined),
            Out::End(s, at) => align.end(s, at),
        }
        let ready = align.advance();
        if ready.is_empty() {
            continue;
        }
        for n in ready {
            let ev = epoch_ready(&plan, &mut align, n, Vec::new(), Vec::new(), Vec::new())?;
            let _ = events.send(Ok(ev)).await;
        }
        let _ = events.try_send(Ok(metrics(&plan, &counters)));
    }
    let _ = events.try_send(Ok(metrics(&plan, &counters)));
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

fn metrics(plan: &Plan, counters: &[Counters]) -> EventsResponse {
    let streams = plan
        .streams
        .iter()
        .zip(counters)
        .map(|(name, c)| StreamMetrics {
            stream: name.clone(),
            rows_in: c.rows_in.load(Ordering::Relaxed),
            bytes_in: c.bytes_in.load(Ordering::Relaxed),
            rows_out: c.rows_out.load(Ordering::Relaxed),
            bytes_out: c.bytes_out.load(Ordering::Relaxed),
            rows_quarantined: c.rows_quarantined.load(Ordering::Relaxed),
            cells_held: c.cells_held.load(Ordering::Relaxed),
        })
        .collect();
    EventsResponse {
        event: Some(events_response::Event::Metrics(Metrics {
            streams,
            memory_bytes: 0,
        })),
    }
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
            Some(engine_out),
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
        assert_eq!((q[0].stream.as_str(), q[0].rows), ("a", 1));
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
            None,
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
                let d: Descriptor = serde_json::from_slice(&r.staged).unwrap();
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
        let left = std::fs::read_dir(dir.path().join("_openetl/staging/1/3/b"))
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
            None,
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
            None,
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
                assert!(r.staged.is_empty());
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
}
