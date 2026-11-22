//! The exchange between engines, where several run an attempt
//! (StartJobRequest.mesh). A lane hands what an exchanged stream sends
//! another engine's writer lanes, and what a group window's statement sends
//! its keyed lanes, to that engine's link: a queue that holds at most a
//! bound of bytes, which the lane waits on when it's full, and a TLS
//! connection that writes the queue's frames in order, each after the route
//! it goes by. Each engine listens for the others, and each connection it
//! takes gives its frames to its writer lanes and keyed lanes, as its own
//! lanes would, tagged with the sending lane, so each aligns on every lane
//! of the attempt. A slow receiver slows its senders through its channel,
//! the connection's flow control and the link's bound.
//!
//! A frame kind crosses engines once `wire` and `unwire` carry it; a writer
//! lane's feed aligns it per sending lane as it does for its own lanes.

use std::collections::{BTreeSet, HashSet};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use openetl_proto::connector::v1::ErrorCode;
use openetl_proto::engine::v1::{Barrier, FrameKind, MeshHello, Route, StartJobRequest, Watermark};
use prost::Message;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, WebPkiSupportedAlgorithms};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, ServerConfig, SignatureScheme};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TryRecvError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError, mpsc};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};
use tracing::{debug, warn};

use crate::dataflow::Out;
use crate::dataflow::keyed;
use crate::error::JobError;
use crate::frame;
use crate::ipc;
use crate::plan::Plan;

/// The name in the attempt's certificate. Engines trust the certificate
/// itself, not the name.
const SERVER_NAME: &str = "openetl-exchange";
/// How long the other engines have to connect, and this one to reach them.
const PEER_TIMEOUT: Duration = Duration::from_secs(60);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest hello, in bytes: an attempt's ID and a secret are far
/// shorter.
const MAX_HELLO: usize = 4096;
/// The most connections a listener handshakes with at once.
const MAX_HANDSHAKES: usize = 64;
/// How long binding waits for its address while an engine of an earlier
/// generation on the same pod still holds it.
const BIND_TIMEOUT: Duration = Duration::from_secs(10);
/// What each link may queue, in bytes: an eighth of the job's memory, split
/// among its links, within these.
const MAX_BUFFER: u64 = 8 << 20;
const MIN_BUFFER: u64 = 256 << 10;
const BUFFER_SHARE: u64 = 8;
/// A dead peer is found when the connection is idle by TCP keepalive, and
/// on Linux when data waits unacknowledged.
const KEEPALIVE: Duration = Duration::from_secs(10);
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(5);
const KEEPALIVE_RETRIES: u32 = 3;
#[cfg(target_os = "linux")]
const USER_TIMEOUT: Duration = Duration::from_secs(30);

/// The attempt's engines, from StartJobRequest.mesh.
#[derive(Debug)]
pub struct MeshPlan {
    pub engine: usize,
    pub engines: Vec<Peer>,
    /// The attempt's lanes.
    pub lanes: usize,
    /// How this engine reaches the others; none where it runs the attempt
    /// alone.
    pub identity: Option<Identity>,
    pub timeout: Duration,
    /// What each link queues at most, in bytes.
    pub buffer: u64,
    /// The links, once started, for tests to watch.
    #[cfg(test)]
    pub links: Arc<std::sync::OnceLock<std::sync::Weak<Links>>>,
}

#[derive(Debug, Clone)]
pub struct Peer {
    pub address: String,
    pub first_lane: usize,
    pub lanes: usize,
    pub first_writer: usize,
    pub writers: usize,
}

pub struct Identity {
    pub listen: String,
    certificate: Vec<u8>,
    key: Vec<u8>,
    secret: Vec<u8>,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("listen", &self.listen)
            .finish_non_exhaustive()
    }
}

impl MeshPlan {
    /// The request's mesh, checked against its lanes and writer lanes.
    pub fn of(
        req: &StartJobRequest,
        lanes: usize,
        writers: usize,
    ) -> Result<Option<Self>, JobError> {
        let Some(m) = &req.mesh else {
            return Ok(None);
        };
        let me = m.engine as usize;
        let mut engines = Vec::with_capacity(m.engines.len());
        let (mut first_lane, mut first_writer) = (0, 0);
        for e in &m.engines {
            let (l, w) = (e.lanes as usize, e.writer_lanes as usize);
            if w < 1 || l < w {
                return Err(JobError::config(format!(
                    "engine {} of the mesh has {l} lanes and {w} writer lanes",
                    engines.len() + 1
                )));
            }
            engines.push(Peer {
                address: e.address.clone(),
                first_lane,
                lanes: l,
                first_writer,
                writers: w,
            });
            first_lane += l;
            first_writer += w;
        }
        let Some(own) = engines.get(me) else {
            return Err(JobError::config(format!(
                "engine {} of a mesh of {}",
                me + 1,
                engines.len()
            )));
        };
        let attempt_writers = (req.attempt_writer_lanes as usize).max(writers);
        if own.lanes != lanes
            || own.writers != writers
            || own.first_lane != req.first_lane as usize
            || own.first_writer != req.first_writer_lane as usize
            || first_writer != attempt_writers
        {
            return Err(JobError::config(format!(
                "engine {} of the mesh has lanes {} from {} and writer lanes {} from {} of {first_writer}, \
                 and the job {lanes} from {} and {writers} from {} of {attempt_writers}",
                me + 1,
                own.lanes,
                own.first_lane,
                own.writers,
                own.first_writer,
                req.first_lane,
                req.first_writer_lane
            )));
        }
        let identity = match engines.len() {
            1 => None,
            _ => {
                if m.listen.is_empty()
                    || m.certificate.is_empty()
                    || m.key.is_empty()
                    || m.secret.is_empty()
                {
                    return Err(JobError::config(
                        "a mesh of several engines needs where this one listens, the attempt's certificate, key and secret",
                    ));
                }
                if let Some(i) =
                    (0..engines.len()).find(|&i| i != me && engines[i].address.is_empty())
                {
                    return Err(JobError::config(format!(
                        "engine {} of the mesh has no address",
                        i + 1
                    )));
                }
                Some(Identity {
                    listen: m.listen.clone(),
                    certificate: m.certificate.clone(),
                    key: m.key.clone(),
                    secret: m.secret.clone(),
                })
            }
        };
        let links = (engines.len() as u64).saturating_sub(1).max(1);
        let buffer = match req.limits.as_ref().map_or(0, |l| l.memory_bytes) {
            0 => MAX_BUFFER,
            limit => (limit / BUFFER_SHARE / links).clamp(MIN_BUFFER, MAX_BUFFER),
        };
        Ok(Some(MeshPlan {
            engine: me,
            lanes: first_lane,
            engines,
            identity,
            timeout: PEER_TIMEOUT,
            buffer,
            #[cfg(test)]
            links: Default::default(),
        }))
    }

    /// Each engine's lanes and writer lanes, as the layout takes them.
    pub fn parts(&self) -> Vec<(usize, usize)> {
        self.engines.iter().map(|e| (e.lanes, e.writers)).collect()
    }

    fn me(&self) -> &Peer {
        &self.engines[self.engine]
    }
}

fn transient(msg: impl Into<String>) -> JobError {
    JobError::new(ErrorCode::Transient, msg)
}

/// A frame that crosses engines: its kind, stream and payload. Held values
/// and patches stay on their lane's own writer lane.
pub fn wire(out: Out) -> Result<(FrameKind, u32, Vec<u8>), JobError> {
    let index = |s: usize| u32::try_from(s).map_err(|_| JobError::internal("a stream past u32"));
    Ok(match out {
        Out::Batch(s, b) => (
            FrameKind::Batch,
            index(s)?,
            ipc::encode_batch(&b).map_err(|e| JobError::internal(e.to_string()))?,
        ),
        Out::SchemaChange(s, p) => (FrameKind::SchemaChange, index(s)?, p),
        Out::Barrier(s, e) => (
            FrameKind::Barrier,
            index(s)?,
            Barrier {
                epoch: Some(e),
                ..Default::default()
            }
            .encode_to_vec(),
        ),
        Out::Watermark(s, micros) => (
            FrameKind::Watermark,
            index(s)?,
            Watermark { micros }.encode_to_vec(),
        ),
        Out::End(s) => (FrameKind::EndOfStream, index(s)?, Vec::new()),
        Out::Held(..) | Out::Patch(..) => {
            return Err(JobError::internal(
                "held values and patches don't cross engines",
            ));
        }
    })
}

/// A frame's payload on a link: bytes, or a batch's stream in parts, which
/// the link writes from the batch's own buffers.
pub enum Payload {
    Bytes(Vec<u8>),
    Parts(Vec<Buffer>),
}

impl Payload {
    pub fn len(&self) -> usize {
        match self {
            Payload::Bytes(b) => b.len(),
            Payload::Parts(p) => p.iter().map(Buffer::len).sum(),
        }
    }

    fn slices(&self) -> Vec<&[u8]> {
        match self {
            Payload::Bytes(b) => vec![b],
            Payload::Parts(p) => p.iter().map(Buffer::as_slice).collect(),
        }
    }
}

impl From<Vec<u8>> for Payload {
    fn from(b: Vec<u8>) -> Self {
        Payload::Bytes(b)
    }
}

/// The frame `wire` makes of a batch, its payload in parts. The parts hold
/// the batch's buffers, which a link's room counts by the bytes it sends,
/// so `batch` holds no more than it sends, as a take's output does.
pub fn wire_batch(
    stream: usize,
    batch: &RecordBatch,
) -> Result<(FrameKind, u32, Payload), JobError> {
    Ok((
        FrameKind::Batch,
        u32::try_from(stream).map_err(|_| JobError::internal("a stream past u32"))?,
        Payload::Parts(
            ipc::encode_batch_parts(batch).map_err(|e| JobError::internal(e.to_string()))?,
        ),
    ))
}

/// What `wire` made of a frame, from an engine of generation `generation`.
pub fn unwire(f: frame::Frame, generation: u64) -> Result<Out, String> {
    let s = f.stream as usize;
    match f.kind {
        FrameKind::Batch => Ok(Out::Batch(
            s,
            ipc::decode_batch(f.payload).map_err(|e| e.to_string())?,
        )),
        FrameKind::SchemaChange => Ok(Out::SchemaChange(s, f.payload)),
        FrameKind::Barrier => {
            let e = Barrier::decode(f.payload.as_slice())
                .map_err(|e| e.to_string())?
                .epoch
                .unwrap_or_default();
            if e.generation != generation {
                return Err(format!(
                    "a barrier of generation {}, not {generation}",
                    e.generation
                ));
            }
            Ok(Out::Barrier(s, e))
        }
        FrameKind::Watermark => Ok(Out::Watermark(
            s,
            Watermark::decode(f.payload.as_slice())
                .map_err(|e| e.to_string())?
                .micros,
        )),
        FrameKind::EndOfStream => Ok(Out::End(s)),
        k => Err(format!("a {} frame", k.as_str_name())),
    }
}

/// Where a frame goes in another engine, by its index in the attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Target {
    Writer(usize),
    /// The keyed lane of the attempt's lane.
    Keyed(usize),
}

/// Where a lane sends what goes to other engines' writer lanes and keyed
/// lanes.
pub struct Links {
    first_writer: usize,
    writers: usize,
    /// By the attempt's writer lane, and by its lane, the engine that has
    /// it.
    engine_of: Vec<usize>,
    engine_of_lane: Vec<usize>,
    /// By engine; none for this one.
    links: Vec<Option<Link>>,
}

struct Link {
    queue: mpsc::UnboundedSender<Item>,
    room: Arc<Semaphore>,
    cap: u32,
    queued: Arc<AtomicU64>,
    /// The lanes waiting for room on it now.
    waiting: Arc<AtomicU64>,
    /// Whether the other engine took the connection.
    #[cfg(test)]
    connected: Arc<AtomicBool>,
}

/// A frame queued on a link, holding its bytes of the link's room.
struct Item {
    lane: u32,
    to: Target,
    kind: FrameKind,
    stream: u32,
    payload: Payload,
    _room: OwnedSemaphorePermit,
}

/// What a lane waits for to queue a frame on a link.
pub enum Room {
    Ready(Reserved),
    Wait {
        room: Arc<Semaphore>,
        n: u32,
        waiting: Arc<AtomicU64>,
    },
}

pub struct Reserved(OwnedSemaphorePermit);

/// Counts a lane among those waiting for room while it lives.
struct Waiting(Arc<AtomicU64>);

impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

impl Room {
    pub async fn wait(self) -> Option<Reserved> {
        match self {
            Room::Ready(r) => Some(r),
            Room::Wait { room, n, waiting } => {
                waiting.fetch_add(1, Ordering::Relaxed);
                let _waiting = Waiting(waiting);
                room.acquire_many_owned(n).await.ok().map(Reserved)
            }
        }
    }
}

impl Links {
    /// Starts a link to each other engine of the plan's mesh, each a task in
    /// `tasks`; none where the plan has no mesh of several engines.
    pub fn start(
        plan: &Arc<Plan>,
        tasks: &mut JoinSet<Result<(), JobError>>,
    ) -> Result<Option<Arc<Links>>, JobError> {
        let Some(m) = &plan.mesh else {
            return Ok(None);
        };
        let Some(id) = &m.identity else {
            return Ok(None);
        };
        let connector = connector(id)?;
        let cap = u32::try_from(m.buffer).unwrap_or(u32::MAX);
        let mut links = Vec::with_capacity(m.engines.len());
        let (mut engine_of, mut engine_of_lane) = (Vec::new(), Vec::new());
        for (e, peer) in m.engines.iter().enumerate() {
            engine_of.extend(std::iter::repeat_n(e, peer.writers));
            engine_of_lane.extend(std::iter::repeat_n(e, peer.lanes));
            if e == m.engine {
                links.push(None);
                continue;
            }
            let (queue, rx) = mpsc::unbounded_channel();
            let room = Arc::new(Semaphore::new(cap as usize));
            let queued = Arc::new(AtomicU64::new(0));
            let waiting = Arc::new(AtomicU64::new(0));
            let connected = Arc::new(AtomicBool::new(false));
            tasks.spawn(link(
                plan.clone(),
                connector.clone(),
                e,
                rx,
                room.clone(),
                queued.clone(),
                connected.clone(),
            ));
            links.push(Some(Link {
                queue,
                room,
                cap,
                queued,
                waiting,
                #[cfg(test)]
                connected,
            }));
        }
        let me = m.me();
        let links = Arc::new(Links {
            first_writer: me.first_writer,
            writers: me.writers,
            engine_of,
            engine_of_lane,
            links,
        });
        #[cfg(test)]
        let _ = m.links.set(Arc::downgrade(&links));
        Ok(Some(links))
    }

    /// The engine's own index of the attempt's writer lane `w`, if it's
    /// this engine's.
    pub fn local(&self, w: usize) -> Option<usize> {
        (self.first_writer..self.first_writer + self.writers)
            .contains(&w)
            .then(|| w - self.first_writer)
    }

    fn link(&self, to: Target) -> Result<&Link, JobError> {
        let engine = match to {
            Target::Writer(w) => self.engine_of.get(w),
            Target::Keyed(k) => self.engine_of_lane.get(k),
        };
        engine
            .and_then(|&e| self.links[e].as_ref())
            .ok_or_else(|| JobError::internal(format!("no other engine has {to:?}")))
    }

    /// The room `bytes` take on the link to `to`'s engine.
    pub fn room(&self, to: Target, bytes: usize) -> Result<Room, JobError> {
        let link = self.link(to)?;
        let n = u32::try_from(bytes).unwrap_or(u32::MAX).clamp(1, link.cap);
        match link.room.clone().try_acquire_many_owned(n) {
            Ok(p) => Ok(Room::Ready(Reserved(p))),
            Err(TryAcquireError::NoPermits) => Ok(Room::Wait {
                room: link.room.clone(),
                n,
                waiting: link.waiting.clone(),
            }),
            Err(TryAcquireError::Closed) => Err(JobError::stopped("the link stopped")),
        }
    }

    /// Queues a frame from the attempt's lane `lane` for `to`, in the room
    /// reserved for it.
    pub fn push(
        &self,
        lane: usize,
        to: Target,
        (kind, stream, payload): (FrameKind, u32, impl Into<Payload>),
        room: Reserved,
    ) -> Result<(), JobError> {
        let payload = payload.into();
        let link = self.link(to)?;
        let index = |i: usize| u32::try_from(i).map_err(|_| JobError::internal("a lane past u32"));
        let (Target::Writer(i) | Target::Keyed(i)) = to;
        index(i)?;
        link.queued
            .fetch_add(payload.len() as u64, Ordering::Relaxed);
        link.queue
            .send(Item {
                lane: index(lane)?,
                to,
                kind,
                stream,
                payload,
                _room: room.0,
            })
            .map_err(|_| JobError::stopped("the link stopped"))
    }

    /// The bytes each link holds now, by engine.
    #[cfg(test)]
    pub fn queued(&self) -> Vec<u64> {
        self.links
            .iter()
            .map(|l| l.as_ref().map_or(0, |l| l.queued.load(Ordering::Relaxed)))
            .collect()
    }

    /// Whether each link's connection is up, by engine; this engine's is.
    #[cfg(test)]
    pub fn connected(&self) -> Vec<bool> {
        self.links
            .iter()
            .map(|l| {
                l.as_ref()
                    .is_none_or(|l| l.connected.load(Ordering::Relaxed))
            })
            .collect()
    }

    /// The lanes waiting for room on each link now, by engine.
    #[cfg(test)]
    pub fn waiting(&self) -> Vec<u64> {
        self.links
            .iter()
            .map(|l| l.as_ref().map_or(0, |l| l.waiting.load(Ordering::Relaxed)))
            .collect()
    }
}

/// Binds where the plan's mesh listens; none without one.
pub async fn bind(plan: &Plan) -> Result<Option<TcpListener>, JobError> {
    let Some(id) = plan.mesh.as_ref().and_then(|m| m.identity.as_ref()) else {
        return Ok(None);
    };
    // Go's ":7720" is every address.
    let every = match id.listen.strip_prefix(':') {
        Some(port) => Some(port.parse::<u16>().map_err(|_| {
            JobError::config(format!(
                "the mesh listens at {}, whose port isn't one",
                id.listen
            ))
        })?),
        None => None,
    };
    let deadline = Instant::now() + BIND_TIMEOUT;
    loop {
        let bound = match every {
            Some(port) => bind_every(port),
            None => TcpListener::bind(&id.listen).await,
        };
        match bound {
            Ok(l) => return Ok(Some(l)),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse && Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            Err(e) => {
                return Err(transient(format!(
                    "listening for the attempt's other engines at {}: {e}",
                    id.listen
                )));
            }
        }
    }
}

/// Listens at `port` on every address: IPv6's with IPv4's mapped onto it,
/// whatever the host's default (net.ipv6.bindv6only), or IPv4's alone on a
/// host without IPv6.
fn bind_every(port: u16) -> std::io::Result<TcpListener> {
    use socket2::{Domain, Protocol, Socket, Type};
    use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
    let dual = || -> std::io::Result<std::net::TcpListener> {
        let s = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
        s.set_only_v6(false)?;
        s.set_reuse_address(true)?;
        s.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
        s.listen(1024)?;
        Ok(s.into())
    };
    let l = match dual() {
        Ok(l) => l,
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => return Err(e),
        Err(e) => {
            debug!(error = %e, "listening on IPv6 for the other engines failed; IPv4 alone");
            std::net::TcpListener::bind((Ipv4Addr::UNSPECIFIED, port))?
        }
    };
    l.set_nonblocking(true)?;
    TcpListener::from_std(l)
}

fn tune(tcp: &TcpStream) {
    let sock = socket2::SockRef::from(tcp);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE)
        .with_interval(KEEPALIVE_INTERVAL)
        .with_retries(KEEPALIVE_RETRIES);
    let tuned = tcp
        .set_nodelay(true)
        .and_then(|()| sock.set_tcp_keepalive(&keepalive));
    #[cfg(target_os = "linux")]
    let tuned = tuned.and_then(|()| sock.set_tcp_user_timeout(Some(USER_TIMEOUT)));
    if let Err(e) = tuned {
        warn!(error = %e, "tuning a connection between engines failed");
    }
}

/// TLS 1.3 with AES-128-GCM alone, the cheapest of its suites per byte
/// where the CPU has AES instructions.
fn provider() -> Arc<CryptoProvider> {
    Arc::new(CryptoProvider {
        cipher_suites: vec![rustls::crypto::aws_lc_rs::cipher_suite::TLS13_AES_128_GCM_SHA256],
        ..rustls::crypto::aws_lc_rs::default_provider()
    })
}

fn tls_error(e: rustls::Error) -> JobError {
    JobError::config(format!("the mesh's certificate or key: {e}"))
}

/// Trusts only the attempt's certificate.
#[derive(Debug)]
struct Pinned {
    certificate: CertificateDer<'static>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for Pinned {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        if intermediates.is_empty() && end_entity.as_ref() == self.certificate.as_ref() {
            return Ok(ServerCertVerified::assertion());
        }
        Err(rustls::Error::InvalidCertificate(
            rustls::CertificateError::ApplicationVerificationFailure,
        ))
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

fn connector(id: &Identity) -> Result<TlsConnector, JobError> {
    let provider = provider();
    let algorithms = provider.signature_verification_algorithms;
    let cfg = ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(tls_error)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(Pinned {
            certificate: CertificateDer::from(id.certificate.clone()),
            algorithms,
        }))
        .with_no_client_auth();
    Ok(TlsConnector::from(Arc::new(cfg)))
}

fn acceptor(id: &Identity) -> Result<TlsAcceptor, JobError> {
    let cfg = ServerConfig::builder_with_provider(provider())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(tls_error)?
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from(id.certificate.clone())],
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(id.key.clone())),
        )
        .map_err(tls_error)?;
    Ok(TlsAcceptor::from(Arc::new(cfg)))
}

/// Compares secrets in time that doesn't depend on where they differ.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

fn hello(plan: &Plan, m: &MeshPlan, secret: &[u8], refused: &str) -> Vec<u8> {
    MeshHello {
        attempt: plan.job.clone(),
        generation: plan.generation,
        engine: m.engine as u32,
        secret: secret.to_vec(),
        refused: refused.to_owned(),
    }
    .encode_to_vec()
}

/// Reads a hello of at most MAX_HELLO bytes, before its sender is known.
async fn read_hello<S: AsyncRead + Unpin>(c: &mut S) -> Result<MeshHello, String> {
    let f = timeout(HANDSHAKE_TIMEOUT, frame::read_limited(c, MAX_HELLO))
        .await
        .map_err(|_| "no hello in time".to_owned())?
        .map_err(|e| e.to_string())?
        .ok_or("closed before its hello")?;
    if f.kind != FrameKind::MeshHello {
        return Err(format!("its first frame is {}", f.kind.as_str_name()));
    }
    MeshHello::decode(f.payload.as_slice()).map_err(|e| e.to_string())
}

enum Dialed {
    /// Worth trying again until the deadline: the engine may not listen
    /// yet, or an engine of an earlier generation may still hold its
    /// address.
    Failed(String),
    Refused(JobError),
}

type Outgoing = tokio_rustls::client::TlsStream<TcpStream>;

async fn try_dial(
    plan: &Plan,
    m: &MeshPlan,
    connector: &TlsConnector,
    to: usize,
) -> Result<Outgoing, Dialed> {
    let peer = &m.engines[to];
    let failed = |e: String| Dialed::Failed(e);
    let tcp = timeout(CONNECT_TIMEOUT, TcpStream::connect(&peer.address))
        .await
        .map_err(|_| failed("no connection in time".into()))?
        .map_err(|e| failed(e.to_string()))?;
    tune(&tcp);
    let name = ServerName::try_from(SERVER_NAME).map_err(|e| failed(e.to_string()))?;
    let mut c = timeout(HANDSHAKE_TIMEOUT, connector.connect(name, tcp))
        .await
        .map_err(|_| failed("no TLS handshake in time".into()))?
        .map_err(|e| failed(e.to_string()))?;
    let secret = m.identity.as_ref().map_or(&[][..], |i| &i.secret[..]);
    frame::write(&mut c, FrameKind::MeshHello, 0, &hello(plan, m, secret, ""))
        .await
        .map_err(|e| failed(e.to_string()))?;
    c.flush().await.map_err(|e| failed(e.to_string()))?;
    let answer = read_hello(&mut c).await.map_err(failed)?;
    let refused = |why: String| {
        Dialed::Refused(transient(format!(
            "engine {} at {} refused the connection: {why}",
            to + 1,
            peer.address
        )))
    };
    if !answer.refused.is_empty() {
        return Err(refused(answer.refused));
    }
    if answer.attempt != plan.job
        || answer.generation != plan.generation
        || answer.engine as usize != to
    {
        return Err(refused(format!(
            "it answered as engine {} of attempt {:?}, generation {}",
            u64::from(answer.engine) + 1,
            answer.attempt,
            answer.generation
        )));
    }
    Ok(c)
}

async fn dial(
    plan: &Plan,
    m: &MeshPlan,
    connector: &TlsConnector,
    to: usize,
) -> Result<Outgoing, JobError> {
    let deadline = Instant::now() + m.timeout;
    let mut wait = Duration::from_millis(20);
    loop {
        let why = match try_dial(plan, m, connector, to).await {
            Ok(c) => return Ok(c),
            Err(Dialed::Refused(e)) => return Err(e),
            Err(Dialed::Failed(why)) => why,
        };
        if Instant::now() + wait > deadline {
            return Err(JobError::peer_lost(
                to,
                format!(
                    "engine {} at {} wasn't reached within {:?}: {why}",
                    to + 1,
                    m.engines[to].address,
                    m.timeout
                ),
            ));
        }
        debug!(engine = to + 1, error = %why, "reaching another engine; trying again");
        tokio::time::sleep(wait).await;
        wait = (wait * 2).min(Duration::from_secs(1));
    }
}

/// Connects to engine `to` and writes what the lanes queue for it, until
/// every lane is done with it.
async fn link(
    plan: Arc<Plan>,
    connector: TlsConnector,
    to: usize,
    rx: mpsc::UnboundedReceiver<Item>,
    room: Arc<Semaphore>,
    queued: Arc<AtomicU64>,
    connected: Arc<AtomicBool>,
) -> Result<(), JobError> {
    let res = async {
        let m = plan
            .mesh
            .as_ref()
            .ok_or_else(|| JobError::internal("a link without a mesh"))?;
        let c = dial(&plan, m, &connector, to).await?;
        connected.store(true, Ordering::Relaxed);
        send(c, rx, &queued).await.map_err(|e| {
            JobError::peer_lost(
                to,
                format!(
                    "the connection to engine {} at {} broke: {e}",
                    to + 1,
                    m.engines[to].address
                ),
            )
        })
    }
    .await;
    // Lanes waiting for room stop, and this task's error says why.
    room.close();
    res
}

async fn send<W: AsyncWrite + Unpin>(
    c: W,
    mut rx: mpsc::UnboundedReceiver<Item>,
    queued: &AtomicU64,
) -> std::io::Result<()> {
    let mut w = BufWriter::with_capacity(1 << 16, c);
    let mut route = None;
    loop {
        let item = match rx.try_recv() {
            Ok(item) => item,
            Err(TryRecvError::Empty) => {
                w.flush().await?;
                match rx.recv().await {
                    Some(item) => item,
                    None => break,
                }
            }
            Err(TryRecvError::Disconnected) => break,
        };
        // A route goes out with the frame after it, in the same writes.
        let mut before = Vec::new();
        if route != Some((item.lane, item.to)) {
            // push checked both fit.
            let r = match item.to {
                Target::Writer(w) => Route {
                    lane: item.lane,
                    writer_lane: w as u32,
                    keyed_lane: None,
                },
                Target::Keyed(k) => Route {
                    lane: item.lane,
                    writer_lane: 0,
                    keyed_lane: Some(k as u32),
                },
            };
            frame::write(&mut before, FrameKind::Route, 0, &r.encode_to_vec()).await?;
            route = Some((item.lane, item.to));
        }
        frame::write_parts(
            &mut w,
            &before,
            item.kind,
            item.stream,
            &item.payload.slices(),
        )
        .await?;
        queued.fetch_sub(item.payload.len() as u64, Ordering::Relaxed);
    }
    w.flush().await?;
    w.shutdown().await
}

type Incoming = tokio_rustls::server::TlsStream<TcpStream>;

/// Why a connection's hello isn't one of the attempt's other engines.
fn refusal(plan: &Plan, m: &MeshPlan, h: &MeshHello) -> Option<String> {
    let secret = m.identity.as_ref().map_or(&[][..], |i| &i.secret[..]);
    if h.attempt != plan.job {
        return Some(format!("it's of attempt {:?}", h.attempt));
    }
    if h.generation != plan.generation {
        return Some(format!(
            "it's of generation {}, and this engine of {}",
            h.generation, plan.generation
        ));
    }
    if !same(&h.secret, secret) {
        return Some("it hasn't the attempt's secret".into());
    }
    let e = h.engine as usize;
    if e >= m.engines.len() || e == m.engine {
        return Some(format!("engine {} isn't another of the attempt's", e + 1));
    }
    None
}

async fn handshake(
    plan: Arc<Plan>,
    acceptor: TlsAcceptor,
    tcp: TcpStream,
) -> Result<(usize, Incoming), String> {
    let m = plan
        .mesh
        .as_ref()
        .ok_or("a mesh's connection without one")?;
    let mut c = timeout(HANDSHAKE_TIMEOUT, acceptor.accept(tcp))
        .await
        .map_err(|_| "no TLS handshake in time".to_owned())?
        .map_err(|e| format!("TLS: {e}"))?;
    let h = read_hello(&mut c).await?;
    if let Some(why) = refusal(&plan, m, &h) {
        let _ = answer(&mut c, &plan, m, &why).await;
        return Err(why);
    }
    Ok((h.engine as usize, c))
}

async fn answer<W: AsyncWrite + Unpin>(
    c: &mut W,
    plan: &Plan,
    m: &MeshPlan,
    refused: &str,
) -> std::io::Result<()> {
    frame::write(c, FrameKind::MeshHello, 0, &hello(plan, m, &[], refused)).await?;
    c.flush().await?;
    if !refused.is_empty() {
        c.shutdown().await?;
    }
    Ok(())
}

/// Takes a connection from each of the mesh's other engines, and gives
/// what each sends to the writer lanes, `tx`, and the keyed lanes, `keyed`.
/// Done once each has ended every stream; fails if one doesn't connect in
/// time, or its connection breaks before. Connections that aren't of the
/// attempt's other engines, or that repeat one, are refused, and none of
/// their frames read.
pub async fn inbound(
    plan: Arc<Plan>,
    listener: TcpListener,
    tx: Vec<mpsc::Sender<(usize, Out)>>,
    keyed: Vec<mpsc::Sender<(usize, keyed::In)>>,
) -> Result<(), JobError> {
    let Some((m, id)) = plan
        .mesh
        .as_ref()
        .and_then(|m| m.identity.as_ref().map(|id| (m, id)))
    else {
        return Err(JobError::internal("a mesh's listener without one"));
    };
    let acceptor = acceptor(id)?;
    let mut waiting: BTreeSet<usize> = (0..m.engines.len()).filter(|&e| e != m.engine).collect();
    let deadline = tokio::time::sleep(m.timeout);
    tokio::pin!(deadline);
    let mut handshakes = JoinSet::new();
    let mut receivers = JoinSet::new();
    loop {
        if waiting.is_empty() && receivers.is_empty() {
            return Ok(());
        }
        tokio::select! {
            accepted = listener.accept(), if !waiting.is_empty() => match accepted {
                // Connections that aren't the attempt's can't hold more
                // than this many handshakes' memory.
                Ok((tcp, from)) if handshakes.len() >= MAX_HANDSHAKES => {
                    warn!(%from, "refused a connection: too many handshakes in progress");
                    drop(tcp);
                }
                Ok((tcp, _)) => {
                    tune(&tcp);
                    handshakes.spawn(handshake(plan.clone(), acceptor.clone(), tcp));
                }
                Err(e) => {
                    warn!(error = %e, "accepting a connection from another engine failed");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            },
            Some(h) = handshakes.join_next() => match h {
                Ok(Ok((e, c))) if waiting.remove(&e) => {
                    receivers.spawn(receive(plan.clone(), e, c, tx.clone(), keyed.clone()));
                }
                Ok(Ok((e, mut c))) => {
                    let plan = plan.clone();
                    tokio::spawn(async move {
                        if let Some(m) = &plan.mesh {
                            let _ = answer(&mut c, &plan, m, &format!("engine {} is connected already", e + 1)).await;
                        }
                    });
                    warn!(engine = e + 1, "refused a second connection from an engine");
                }
                Ok(Err(why)) => warn!(reason = %why, "refused a connection from another engine"),
                Err(e) => warn!(error = %e, "a connection from another engine failed"),
            },
            Some(r) = receivers.join_next() => match r {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(JobError::internal(format!("a connection from another engine failed: {e}"))),
            },
            () = &mut deadline, if !waiting.is_empty() => {
                let first = waiting.first().copied().unwrap_or_default();
                let missing: Vec<usize> = waiting.iter().map(|e| e + 1).collect();
                return Err(JobError::peer_lost(first, format!(
                    "engines {missing:?} of the attempt didn't connect within {:?}",
                    m.timeout
                )));
            }
        }
    }
}

/// Gives what engine `from` sends on `c` to the writer lanes and keyed
/// lanes, until it has ended every exchanged stream from each of its lanes
/// on each writer lane, and every window's statement on each keyed lane.
async fn receive(
    plan: Arc<Plan>,
    from: usize,
    mut c: Incoming,
    tx: Vec<mpsc::Sender<(usize, Out)>>,
    keyed: Vec<mpsc::Sender<(usize, keyed::In)>>,
) -> Result<(), JobError> {
    let m = plan
        .mesh
        .as_ref()
        .ok_or_else(|| JobError::internal("a mesh's connection without one"))?;
    let peer = &m.engines[from];
    let lost = |why: String| {
        JobError::peer_lost(
            from,
            format!("engine {} at {} was lost: {why}", from + 1, peer.address),
        )
    };
    let protocol = |why: String| JobError::internal(format!("engine {} sent {why}", from + 1));
    answer(&mut c, &plan, m, "")
        .await
        .map_err(|e| lost(e.to_string()))?;
    let me = m.me();
    let exchanged: Vec<bool> = plan.exchanges.iter().map(Option::is_some).collect();
    let windows = keyed::statements(&plan);
    let want = peer.lanes * me.writers * exchanged.iter().filter(|e| **e).count()
        + peer.lanes * keyed.len() * windows.len();
    let mut ended = HashSet::new();
    let mut route: Option<(usize, Target)> = None;
    loop {
        let f = match frame::read(&mut c).await {
            Ok(Some(f)) => f,
            Ok(None) if ended.len() == want => return Ok(()),
            Ok(None) => {
                return Err(lost(
                    "its connection closed before its streams ended".into(),
                ));
            }
            Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                return Err(protocol(e.to_string()));
            }
            Err(e) => return Err(lost(e.to_string())),
        };
        if f.kind == FrameKind::Route {
            let rt = Route::decode(f.payload.as_slice()).map_err(|e| protocol(e.to_string()))?;
            let lane = rt.lane as usize;
            let to = match rt.keyed_lane {
                Some(k) => Target::Keyed(k as usize),
                None => Target::Writer(rt.writer_lane as usize),
            };
            let local = match to {
                Target::Writer(w) => (me.first_writer..me.first_writer + me.writers)
                    .contains(&w)
                    .then(|| Target::Writer(w - me.first_writer)),
                Target::Keyed(k) => (me.first_lane..me.first_lane + keyed.len())
                    .contains(&k)
                    .then(|| Target::Keyed(k - me.first_lane)),
            };
            let Some(local) =
                local.filter(|_| (peer.first_lane..peer.first_lane + peer.lanes).contains(&lane))
            else {
                return Err(protocol(format!("a route from lane {lane} to {to:?}")));
            };
            route = Some((lane, local));
            continue;
        }
        let Some((lane, to)) = route else {
            return Err(protocol("a frame before its route".into()));
        };
        let s = f.stream as usize;
        match to {
            Target::Writer(w) => {
                if !exchanged.get(s).copied().unwrap_or(false) {
                    return Err(protocol(format!("stream {s}, which isn't exchanged")));
                }
                let out = unwire(f, plan.generation).map_err(protocol)?;
                if matches!(out, Out::End(_)) {
                    ended.insert((lane, to, s));
                }
                tx[w]
                    .send((lane, out))
                    .await
                    .map_err(|_| JobError::stopped("the writer lane stopped"))?;
            }
            Target::Keyed(k) => {
                if !windows.contains(&s) {
                    return Err(protocol(format!(
                        "statement {s} to a keyed lane, which isn't a group window's"
                    )));
                }
                let item = keyed::unwire(f, plan.generation).map_err(protocol)?;
                if matches!(item, keyed::In::End(_)) {
                    ended.insert((lane, to, s));
                }
                keyed[k]
                    .send((lane, item))
                    .await
                    .map_err(|_| JobError::stopped("the keyed lane stopped"))?;
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
pub(crate) mod tests;
