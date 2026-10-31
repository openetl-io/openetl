//! The control service and the data socket. One engine process runs one job.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openetl_proto::engine::v1::engine_service_server::EngineService;
use openetl_proto::engine::v1::{
    CommitRequest, CommitResponse, EventsRequest, EventsResponse, FrameKind, Hello, RecoverRequest,
    RecoverResponse, StartJobRequest, StartJobResponse, StopRequest, StopResponse,
};
use prost::Message;
use tokio::io::BufReader;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

use crate::dataflow::{self, Events};
use crate::error::JobError;
use crate::frame;
use crate::plan::{Plan, Sink};

/// How long the runner has to connect every edge after StartJob.
const EDGE_TIMEOUT: Duration = Duration::from_secs(30);
const HELLO_TIMEOUT: Duration = Duration::from_secs(10);
/// Unix sockets default to small buffers (8 KiB on macOS), which costs a
/// context switch every few KiB of a batch. internal/engineclient uses the
/// same size.
const EDGE_SOCKET_BUFFER: usize = 1 << 20;

pub struct Edge {
    pub reader: BufReader<OwnedReadHalf>,
    pub writer: OwnedWriteHalf,
}

#[derive(Default)]
struct Job {
    /// Edges waiting for their connection.
    edges: HashMap<String, oneshot::Sender<Edge>>,
    events: Option<mpsc::Receiver<Result<EventsResponse, Status>>>,
}

pub struct Engine {
    job: Mutex<Option<Job>>,
    /// Cancelled when the process should exit.
    shutdown: CancellationToken,
}

impl Engine {
    pub fn new(shutdown: CancellationToken) -> Arc<Self> {
        Arc::new(Self {
            job: Mutex::new(None),
            shutdown,
        })
    }

    fn job(&self) -> MutexGuard<'_, Option<Job>> {
        self.job.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn start(&self, req: StartJobRequest) -> Result<(), JobError> {
        let mut slot = self.job();
        if slot.is_some() {
            return Err(JobError::config("this engine already ran a job"));
        }
        let plan = Arc::new(Plan::from_request(req)?);
        let (events_tx, events_rx) = mpsc::channel(64);
        let mut job = Job {
            events: Some(events_rx),
            ..Job::default()
        };
        let mut inbound = Vec::new();
        for e in &plan.inbound {
            let (tx, rx) = oneshot::channel();
            job.edges.insert(e.id.clone(), tx);
            inbound.push((e.id.clone(), rx));
        }
        let outbound = match &plan.sink {
            Sink::Runner { edge, .. } => {
                let (tx, rx) = oneshot::channel();
                job.edges.insert(edge.id.clone(), tx);
                Some(rx)
            }
            Sink::Parquet { .. } | Sink::Discard => None,
        };
        *slot = Some(job);

        info!(
            generation = plan.generation,
            streams = plan.streams.len(),
            "job started"
        );
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let res = tokio::select! {
                r = run(plan, inbound, outbound, events_tx.clone()) => r,
                _ = shutdown.cancelled() => {
                    let _ = events_tx.try_send(Err(Status::aborted("the job was stopped")));
                    return;
                }
            };
            match res {
                Ok(()) => info!("job finished"),
                Err(e) if e.runner_gone => {
                    warn!(error = %e, "the runner went away; exiting");
                    shutdown.cancel();
                }
                Err(e) => warn!(error = %e, "job failed"),
            }
        });
        Ok(())
    }

    fn attach(&self, id: &str, edge: Edge) -> Result<(), String> {
        let tx = self
            .job()
            .as_mut()
            .and_then(|j| j.edges.remove(id))
            .ok_or_else(|| format!("edge {id:?} isn't planned or is already connected"))?;
        tx.send(edge)
            .map_err(|_| format!("edge {id:?} connected after the job ended"))
    }
}

async fn run(
    plan: Arc<Plan>,
    inbound: Vec<(String, oneshot::Receiver<Edge>)>,
    outbound: Option<oneshot::Receiver<Edge>>,
    events: Events,
) -> Result<(), JobError> {
    let connect = async {
        let mut readers = Vec::with_capacity(inbound.len());
        for (id, rx) in inbound {
            let edge = rx
                .await
                .map_err(|_| JobError::internal(format!("edge {id} was dropped")))?;
            readers.push(edge.reader);
        }
        let writer = match outbound {
            Some(rx) => Some(
                rx.await
                    .map_err(|_| JobError::internal("the outbound edge was dropped"))?
                    .writer,
            ),
            None => None,
        };
        Ok::<_, JobError>((readers, writer))
    };
    let (readers, writer) = match tokio::time::timeout(EDGE_TIMEOUT, connect).await {
        Ok(Ok(conns)) => conns,
        Ok(Err(e)) => {
            dataflow::report(&events, &e).await;
            return Err(e);
        }
        Err(_) => {
            let e = JobError::runner_gone("the runner didn't connect every edge in time");
            dataflow::report(&events, &e).await;
            return Err(e);
        }
    };
    dataflow::run(plan, readers, writer, events).await
}

pub async fn accept_edges(listener: UnixListener, engine: Arc<Engine>) {
    loop {
        match listener.accept().await {
            Ok((conn, _)) => {
                let engine = engine.clone();
                tokio::spawn(async move {
                    if let Err(e) = handshake(conn, &engine).await {
                        warn!(error = %e, "rejected an edge connection");
                    }
                });
            }
            Err(e) => {
                warn!(error = %e, "accepting an edge connection failed");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn handshake(conn: UnixStream, engine: &Engine) -> Result<(), String> {
    let sock = socket2::SockRef::from(&conn);
    if let Err(e) = sock
        .set_send_buffer_size(EDGE_SOCKET_BUFFER)
        .and_then(|()| sock.set_recv_buffer_size(EDGE_SOCKET_BUFFER))
    {
        warn!(error = %e, "sizing an edge's socket buffers failed");
    }
    let (r, w) = conn.into_split();
    let mut reader = BufReader::with_capacity(1 << 16, r);
    let f = tokio::time::timeout(HELLO_TIMEOUT, frame::read(&mut reader))
        .await
        .map_err(|_| "no HELLO in time".to_owned())?
        .map_err(|e| e.to_string())?
        .ok_or("closed before HELLO")?;
    if f.kind != FrameKind::Hello {
        return Err(format!(
            "first frame is {}, not HELLO",
            f.kind.as_str_name()
        ));
    }
    let hello = Hello::decode(f.payload.as_slice()).map_err(|e| e.to_string())?;
    engine.attach(&hello.edge, Edge { reader, writer: w })
}

pub struct Control(pub Arc<Engine>);

fn no_engine_sink() -> Status {
    Status::failed_precondition("the job has no engine sink")
}

#[tonic::async_trait]
impl EngineService for Control {
    async fn start_job(
        &self,
        req: Request<StartJobRequest>,
    ) -> Result<Response<StartJobResponse>, Status> {
        self.0.start(req.into_inner()).map_err(|e| e.to_status())?;
        Ok(Response::new(StartJobResponse {}))
    }

    async fn recover(
        &self,
        _: Request<RecoverRequest>,
    ) -> Result<Response<RecoverResponse>, Status> {
        Err(no_engine_sink())
    }

    type EventsStream = ReceiverStream<Result<EventsResponse, Status>>;

    async fn events(
        &self,
        _: Request<EventsRequest>,
    ) -> Result<Response<Self::EventsStream>, Status> {
        let rx = self
            .0
            .job()
            .as_mut()
            .and_then(|j| j.events.take())
            .ok_or_else(|| {
                Status::failed_precondition("no job, or its events are already being read")
            })?;
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn commit(&self, _: Request<CommitRequest>) -> Result<Response<CommitResponse>, Status> {
        Err(no_engine_sink())
    }

    async fn stop(&self, _: Request<StopRequest>) -> Result<Response<StopResponse>, Status> {
        info!("stop requested");
        self.0.shutdown.cancel();
        Ok(Response::new(StopResponse {}))
    }
}
