//! The P0 dataflow: every inbound stream passes through to the runner sink's
//! edge, with raw rows normalized to Arrow and barriers forwarded in band.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use openetl_normalize::Normalizer;
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, EpochReady, EventsResponse, FrameKind, Metrics, Rows, SchemaChange, StreamMetrics,
    events_response,
};
use prost::Message;
use tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task::JoinSet;
use tonic::Status;

use crate::error::JobError;
use crate::frame;
use crate::ipc;
use crate::plan::Plan;

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
}

/// What an inbound edge hands to the outbound one. Streams are indexes into
/// Plan::streams.
enum Out {
    Batch(usize, RecordBatch),
    /// An encoded SchemaChange message.
    SchemaChange(usize, Vec<u8>),
    Barrier(usize, Epoch),
    End(usize),
}

/// Runs the job until every stream has ended on the outbound edge and every
/// inbound edge has closed. Failures are reported on `events` before the
/// edges close.
pub async fn run<R, W>(
    plan: Arc<Plan>,
    inbound: Vec<R>,
    outbound: W,
    events: Events,
) -> Result<(), JobError>
where
    R: AsyncBufRead + Unpin + Send + 'static,
    W: AsyncWrite + Unpin + Send + 'static,
{
    let counters: Arc<Vec<Counters>> =
        Arc::new(plan.streams.iter().map(|_| Counters::default()).collect());
    let (tx, rx) = mpsc::channel(16);
    let mut readers = JoinSet::new();
    for (edge, r) in inbound.into_iter().enumerate() {
        readers.spawn(read_edge(
            plan.clone(),
            edge,
            r,
            tx.clone(),
            counters.clone(),
        ));
    }
    drop(tx);
    let writer = write_edge(plan.clone(), outbound, rx, events.clone(), counters.clone());
    tokio::pin!(writer);

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
    let mut norm: Vec<Normalizer> = streams.iter().map(|_| Normalizer::default()).collect();
    let mut next_epoch = vec![plan.first_epoch; streams.len()];
    let mut ended = vec![false; streams.len()];
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
            FrameKind::Rows => {
                let rows =
                    Rows::decode(f.payload.as_slice()).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(rows.rows.len() as u64, Ordering::Relaxed);
                let n = norm[local]
                    .push(rows.rows.iter().map(Vec::as_slice))
                    .map_err(|e| {
                        JobError::new(
                            ErrorCode::DataInvalid,
                            format!("stream {}: {e}", plan.streams[stream]),
                        )
                    })?;
                if n.schema_changed {
                    let schema = ipc::encode_schema(&n.batch.schema())
                        .map_err(|e| JobError::internal(e.to_string()))?;
                    let msg = SchemaChange {
                        arrow_schema: schema,
                    };
                    send(&tx, Out::SchemaChange(stream, msg.encode_to_vec())).await?;
                }
                if n.batch.num_rows() == 0 {
                    continue;
                }
                Out::Batch(stream, n.batch)
            }
            FrameKind::Batch => {
                let batch = ipc::decode_batch(f.payload).map_err(|e| protocol(e.to_string()))?;
                c.rows_in
                    .fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                Out::Batch(stream, batch)
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
                Out::Barrier(stream, epoch)
            }
            FrameKind::SchemaChange => Out::SchemaChange(stream, f.payload),
            FrameKind::EndOfStream => {
                ended[local] = true;
                Out::End(stream)
            }
            k @ (FrameKind::Hello | FrameKind::StagedFiles | FrameKind::Unspecified) => {
                return Err(protocol(format!("unexpected {} frame", k.as_str_name())));
            }
        };
        send(&tx, out).await?;
    }
}

async fn send(tx: &mpsc::Sender<Out>, out: Out) -> Result<(), JobError> {
    tx.send(out)
        .await
        .map_err(|_| JobError::internal("the outbound edge stopped"))
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
    let id = &plan.outbound.id;
    let gone = |e: std::io::Error| JobError::runner_gone(format!("edge {id}: {e}"));
    let mut w = BufWriter::with_capacity(1 << 16, w);
    let n = plan.streams.len();
    let mut ended = vec![false; n];
    // Each stream's last barrier, and the last epoch reported ready.
    let mut crossed = vec![plan.first_epoch - 1; n];
    let mut ready = plan.first_epoch - 1;
    let mut highest = ready;

    while ended.iter().any(|e| !e) {
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
            Out::Barrier(s, epoch) => {
                crossed[s] = epoch.n;
                highest = highest.max(epoch.n);
                (
                    FrameKind::Barrier,
                    s,
                    Barrier { epoch: Some(epoch) }.encode_to_vec(),
                )
            }
            Out::End(s) => {
                ended[s] = true;
                (FrameKind::EndOfStream, s, Vec::new())
            }
        };
        frame::write(&mut w, kind, plan.out_index[stream], &payload)
            .await
            .map_err(gone)?;

        // An epoch is ready once every stream has crossed its barrier or ended.
        let aligned = (0..n)
            .filter(|&s| !ended[s])
            .map(|s| crossed[s])
            .min()
            .unwrap_or(highest)
            .min(highest);
        if aligned > ready {
            w.flush().await.map_err(gone)?;
            for e in ready + 1..=aligned {
                let ev = events_response::Event::EpochReady(EpochReady {
                    epoch: Some(Epoch {
                        generation: plan.generation,
                        n: e,
                    }),
                    staged: Vec::new(),
                    snapshots: Vec::new(),
                });
                // Engine sinks will need every EpochReady, so this waits.
                let _ = events.send(Ok(EventsResponse { event: Some(ev) })).await;
            }
            ready = aligned;
            let _ = events.try_send(Ok(metrics(&plan, &counters)));
        }
    }
    w.flush().await.map_err(gone)?;
    w.shutdown().await.map_err(gone)?;
    let _ = events.try_send(Ok(metrics(&plan, &counters)));
    Ok(true)
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
            rows_quarantined: 0,
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
    use arrow_schema::DataType;
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
        let plan = Arc::new(Plan::from_request(req).unwrap());
        let (to_engine, engine_in) = duplex(1 << 16);
        let (engine_out, from_engine) = duplex(1 << 16);
        let (tx, events) = mpsc::channel(64);
        let job = tokio::spawn(run(plan, vec![BufReader::new(engine_in)], engine_out, tx));
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

    #[tokio::test]
    async fn arrow_batches_pass_through() {
        let mut h = start(1);
        let mut n = Normalizer::default();
        let batch = n.push([br#"{"k": "v"}"#.as_slice()]).unwrap().batch;
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
    async fn bad_rows_are_data_invalid() {
        let mut h = start(1);
        put(&mut h.to_engine, FrameKind::Rows, 1, rows(&["[1]"])).await;
        let (err, st) = failure(h).await;
        assert_eq!(err.code, ErrorCode::DataInvalid);
        assert_eq!(st.code(), tonic::Code::InvalidArgument);
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
