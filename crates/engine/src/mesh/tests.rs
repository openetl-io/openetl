//! Engines of one attempt in one process, over loopback TLS.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::cast::AsArray;
use base64::Engine as _;
use openetl_proto::connector::v1::{Epoch, ErrorCode};
use openetl_proto::engine::v1::{
    Barrier, EpochReady, EventsResponse, FrameKind, Mesh, MeshEngine, Rows, StartJobRequest,
    events_response,
};
use prost::Message;
use tokio::io::{AsyncWriteExt, BufReader, DuplexStream, duplex};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tonic::Status;

use super::*;
use crate::dataflow::run_with;
use crate::error::JobError;
use crate::frame::{self, Frame};
use crate::plan::Plan;
use crate::plan::tests::{exchange_by, lanes_request};

// Two self-signed certificates for openetl-exchange, with their PKCS #8 keys.
const CERT: &str = "MIIBbDCCARGgAwIBAgIBATAKBggqhkjOPQQDAjAbMRkwFwYDVQQDExBvcGVuZXRsLWV4Y2hhbmdlMCAXDTI2MDEwMTAwMDAwMFoYDzIxMjYwMTAxMDAwMDAwWjAbMRkwFwYDVQQDExBvcGVuZXRsLWV4Y2hhbmdlMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEbyoWmVp7ol1uoXer4/eRybDweEGkHsYrPhaVEJxRlPPG4sUcN4g/gZ4go3ylXxHLLNM7DC8IHao+kCNPgX8SH6NEMEIwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBsGA1UdEQQUMBKCEG9wZW5ldGwtZXhjaGFuZ2UwCgYIKoZIzj0EAwIDSQAwRgIhAP4ooRZoqNDPvspqSsVtFT+rP0ZyUNOMy8haNGZwoZ2jAiEAt7Q3+98r1R8poRW3TqN3FplOZPfjULk9hPi4NxR6GQQ=";
const KEY: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgdptva9iXh9mWCh/Uoo/UJaa2gpUZ8EX9W7GRw+nzfxWhRANCAARvKhaZWnuiXW6hd6vj95HJsPB4QaQexis+FpUQnFGU88bixRw3iD+BniCjfKVfEcss0zsMLwgdqj6QI0+BfxIf";
const OTHER_CERT: &str = "MIIBbDCCARGgAwIBAgIBAjAKBggqhkjOPQQDAjAbMRkwFwYDVQQDExBvcGVuZXRsLWV4Y2hhbmdlMCAXDTI2MDEwMTAwMDAwMFoYDzIxMjYwMTAxMDAwMDAwWjAbMRkwFwYDVQQDExBvcGVuZXRsLWV4Y2hhbmdlMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE3GKokVf0io7wWbGx764OaTJwu+fNl4uyDuic06DithGshDJ5B1PruIAraclaLyyw9AFKlxoIfvP3b+XIeoo7rqNEMEIwDgYDVR0PAQH/BAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMBMBsGA1UdEQQUMBKCEG9wZW5ldGwtZXhjaGFuZ2UwCgYIKoZIzj0EAwIDSQAwRgIhAIYcftvq3ZGOoIOoRSyRNwr/oxHpsA5B2ij9Qro/MQ51AiEA/0RW5bB+H7O3V9XAdiPyKREbg7H0OvTyxqvmeZi8S/4=";
const OTHER_KEY: &str = "MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgMxPdhmSHKYFKM2hpjCMXb+SfUqXjRNqjCseDNxB06aqhRANCAATcYqiRV/SKjvBZsbHvrg5pMnC7582Xi7IO6JzToOK2EayEMnkHU+u4gCtpyVovLLD0AUqXGgh+8/dv5ch6ijuu";

fn der(b64: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(b64)
        .unwrap()
}

struct Engine {
    ins: Vec<DuplexStream>,
    outs: Vec<BufReader<DuplexStream>>,
    events: mpsc::Receiver<Result<EventsResponse, Status>>,
    job: JoinHandle<Result<(), JobError>>,
}

/// The request of engine `me` of a mesh of `parts`, each its lanes and
/// writer lanes, for one keyed stream "a" exchanged by id, listening at
/// `addrs[me]`.
pub(crate) fn request(parts: &[(u32, u32)], me: usize, addrs: &[String]) -> StartJobRequest {
    let (lanes, writers) = parts[me];
    let mut req = lanes_request(&["a"], lanes, writers);
    req.streams[0].primary_key = vec!["id".into()];
    req.streams[0].exchange = exchange_by(&["id"]);
    req.first_lane = parts[..me].iter().map(|p| p.0).sum();
    req.first_writer_lane = parts[..me].iter().map(|p| p.1).sum();
    req.attempt_writer_lanes = parts.iter().map(|p| p.1).sum();
    req.mesh = Some(Mesh {
        engine: me as u32,
        engines: parts
            .iter()
            .zip(addrs)
            .map(|(&(lanes, writer_lanes), address)| MeshEngine {
                address: address.clone(),
                lanes,
                writer_lanes,
            })
            .collect(),
        listen: addrs[me].clone(),
        certificate: der(CERT),
        key: der(KEY),
        secret: b"the attempt's secret".to_vec(),
    });
    req
}

pub(crate) async fn listeners(n: usize) -> (Vec<TcpListener>, Vec<String>) {
    let mut ls = Vec::new();
    let mut addrs = Vec::new();
    for _ in 0..n {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap().to_string());
        ls.push(l);
    }
    (ls, addrs)
}

fn start(req: StartJobRequest, listener: TcpListener, timeout: Duration) -> Engine {
    let mut plan = Plan::from_request(req).unwrap();
    if let Some(m) = plan.mesh.as_mut() {
        m.timeout = timeout;
    }
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
    let job = tokio::spawn(run_with(plan, readers, writers, tx, Some(listener)));
    Engine {
        ins,
        outs,
        events,
        job,
    }
}

/// Starts a mesh of `parts`, each request as `change` leaves it.
async fn mesh(parts: &[(u32, u32)], change: impl Fn(usize, &mut StartJobRequest)) -> Vec<Engine> {
    let (ls, addrs) = listeners(parts.len()).await;
    ls.into_iter()
        .enumerate()
        .map(|(me, l)| {
            let mut req = request(parts, me, &addrs);
            change(me, &mut req);
            start(req, l, Duration::from_secs(10))
        })
        .collect()
}

async fn put(w: &mut DuplexStream, kind: FrameKind, payload: Vec<u8>) {
    frame::write(w, kind, 0, &payload).await.unwrap();
}

fn rows(ids: impl IntoIterator<Item = i64>) -> Vec<u8> {
    Rows {
        rows: ids
            .into_iter()
            .map(|i| format!(r#"{{"id": {i}}}"#).into_bytes())
            .collect(),
    }
    .encode_to_vec()
}

fn barrier(generation: u64, n: u64) -> Vec<u8> {
    Barrier {
        epoch: Some(Epoch { generation, n }),
    }
    .encode_to_vec()
}

async fn end(w: &mut DuplexStream) {
    put(w, FrameKind::EndOfStream, vec![]).await;
    w.shutdown().await.unwrap();
}

/// Reads each writer edge to its end, apart.
fn drain(outs: Vec<BufReader<DuplexStream>>) -> Vec<JoinHandle<Vec<Frame>>> {
    outs.into_iter()
        .map(|mut r| {
            tokio::spawn(async move {
                let mut out = Vec::new();
                while let Some(f) = frame::read(&mut r).await.unwrap() {
                    out.push(f);
                }
                out
            })
        })
        .collect()
}

fn ids(f: &Frame) -> Vec<i64> {
    let b = crate::ipc::decode_batch(f.payload.clone()).unwrap();
    let c = b.column(b.schema().index_of("id").unwrap());
    c.as_primitive::<arrow_array::types::Int64Type>()
        .iter()
        .flatten()
        .collect()
}

/// The ids before and after a writer edge's first barrier.
fn around_barrier(frames: &[Frame]) -> (BTreeSet<i64>, BTreeSet<i64>) {
    let at = frames
        .iter()
        .position(|f| f.kind == FrameKind::Barrier)
        .unwrap();
    let batches = |fs: &[Frame]| {
        fs.iter()
            .filter(|f| f.kind == FrameKind::Batch)
            .flat_map(ids)
            .collect::<BTreeSet<_>>()
    };
    (batches(&frames[..at]), batches(&frames[at + 1..]))
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

/// Rows go to the writer lanes owning their key groups, in whichever
/// engine, and a writer lane passes a barrier only once every lane of the
/// attempt has: its own engine's and the other's.
#[tokio::test]
async fn rows_cross_engines_and_barriers_align_on_every_lane() {
    let mut es = mesh(&[(1, 1), (2, 2)], |_, _| {}).await;
    let drains: Vec<_> = es
        .iter_mut()
        .map(|e| drain(std::mem::take(&mut e.outs)))
        .collect();
    put(&mut es[0].ins[0], FrameKind::Rows, rows(0..200)).await;
    put(&mut es[0].ins[0], FrameKind::Barrier, barrier(1, 1)).await;
    put(&mut es[0].ins[0], FrameKind::Rows, rows(1000..1100)).await;
    put(&mut es[1].ins[0], FrameKind::Rows, rows(200..300)).await;
    put(&mut es[1].ins[0], FrameKind::Barrier, barrier(1, 1)).await;
    put(&mut es[1].ins[1], FrameKind::Rows, rows(300..400)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    for e in &mut es {
        assert!(
            e.events.try_recv().is_err(),
            "an epoch was ready before every lane passed its barrier"
        );
    }
    put(&mut es[1].ins[1], FrameKind::Barrier, barrier(1, 1)).await;
    for e in &mut es {
        for w in &mut e.ins {
            end(w).await;
        }
    }
    let mut before = BTreeSet::new();
    let mut after = BTreeSet::new();
    let mut got = Vec::new();
    for (e, drains) in es.into_iter().zip(drains) {
        for d in drains {
            let frames = d.await.unwrap();
            let (b, a) = around_barrier(&frames);
            assert!(
                b.intersection(&before).next().is_none(),
                "a row reached two writer lanes"
            );
            got.push(b.clone());
            before.extend(b);
            after.extend(a);
        }
        e.job.await.unwrap().unwrap();
        let mut events = e.events;
        let ns: Vec<u64> = ready(&mut events)
            .await
            .iter()
            .map(|r| r.epoch.unwrap().n)
            .collect();
        assert_eq!(ns, [1]);
    }
    assert_eq!(before, (0..400).collect());
    assert_eq!(after, (1000..1100).collect());
    // Each writer lane got rows of lanes of both engines.
    for b in &got {
        assert!(
            b.iter().any(|id| *id < 200) && b.iter().any(|id| *id >= 200),
            "{b:?}"
        );
    }
}

/// A writer lane that isn't read holds its senders up: the link to it
/// queues no more than its bound, the sending lane stops reading its edge,
/// and once the writer lane is read again every row lands, once.
#[tokio::test]
async fn a_slow_writer_lane_slows_its_senders_within_the_links_bound() {
    let mut es = mesh(&[(1, 1), (1, 1)], |_, req| {
        req.limits = Some(openetl_proto::engine::v1::Limits {
            memory_bytes: 16 << 20,
        });
    })
    .await;
    let d0 = drain(std::mem::take(&mut es[0].outs));
    let mut stalled = std::mem::take(&mut es[1].outs);
    let mut w = std::mem::take(&mut es[0].ins).pop().unwrap();
    let (sent_tx, mut sent) = mpsc::unbounded_channel();
    const BATCHES: i64 = 1000;
    let feeder = tokio::spawn(async move {
        for i in 0..BATCHES {
            put(&mut w, FrameKind::Rows, rows(i * 500..(i + 1) * 500)).await;
            let _ = sent_tx.send(i);
        }
        put(&mut w, FrameKind::Barrier, barrier(1, 1)).await;
        end(&mut w).await;
    });
    let mut last = 0;
    loop {
        match tokio::time::timeout(Duration::from_secs(1), sent.recv()).await {
            Ok(Some(i)) => last = i,
            Ok(None) => panic!("every batch went in with a writer lane not read"),
            Err(_) => break,
        }
    }
    assert!(last < BATCHES - 1, "{last}");
    put(&mut es[1].ins[0], FrameKind::Barrier, barrier(1, 1)).await;
    end(&mut es[1].ins[0]).await;
    let d1 = drain(std::mem::take(&mut stalled));
    feeder.await.unwrap();
    let mut all = BTreeSet::new();
    for d in d0.into_iter().chain(d1) {
        for f in d.await.unwrap() {
            if f.kind == FrameKind::Batch {
                for id in ids(&f) {
                    assert!(all.insert(id), "{id} twice");
                }
            }
        }
    }
    assert_eq!(all.len(), 500 * BATCHES as usize);
    for e in es {
        e.job.await.unwrap().unwrap();
    }
}

/// What a link queues waits for room, which its bound holds.
#[tokio::test]
async fn a_link_queues_within_its_bound() {
    // A listener that never answers: nothing the link queues is sent.
    let (_ls, addrs) = listeners(2).await;
    let mut req = request(&[(1, 1), (1, 1)], 0, &addrs);
    req.limits = Some(openetl_proto::engine::v1::Limits {
        memory_bytes: 16 << 20,
    });
    let plan = Arc::new(Plan::from_request(req).unwrap());
    // An eighth of the job's memory, for its one link.
    let bound = plan.mesh.as_ref().unwrap().buffer;
    assert_eq!(bound, 2 << 20);
    let mut tasks = JoinSet::new();
    let links = Links::start(&plan, &mut tasks).unwrap().unwrap();
    let mut pushed = 0u64;
    loop {
        let frame = (FrameKind::Batch, 0, vec![0u8; 100 << 10]);
        match links.room(Target::Writer(1), frame.2.len()).unwrap() {
            Room::Ready(r) => {
                pushed += frame.2.len() as u64;
                links.push(0, Target::Writer(1), frame, r).unwrap();
            }
            Room::Wait(..) => break,
        }
        assert!(pushed <= bound);
    }
    assert_eq!(links.queued(), [0, pushed]);
    assert!(pushed + (100 << 10) > bound);
    // A frame larger than the bound takes all of it.
    assert!(
        matches!(links.room(Target::Writer(1), 64 << 20).unwrap(), Room::Wait(_, n) if u64::from(n) == bound)
    );
    tasks.abort_all();
}

async fn failure(e: Engine) -> JobError {
    let mut events = e.events;
    let res = tokio::time::timeout(Duration::from_secs(20), e.job)
        .await
        .expect("the job didn't end")
        .unwrap();
    let err = res.expect_err("the job didn't fail");
    let mut reported = false;
    while let Ok(Some(ev)) = tokio::time::timeout(Duration::from_secs(1), events.recv()).await {
        reported |= ev.is_err();
    }
    assert!(reported, "the failure wasn't reported");
    err
}

/// Engine 0 is of generation 1 and engine 1 of 2, with the same
/// certificate and secret: each refuses the other, and takes no frame of
/// it.
#[tokio::test]
async fn an_engine_of_another_generation_is_refused() {
    let (ls, addrs) = listeners(2).await;
    let parts = [(1, 1), (1, 1)];
    let mut es: Vec<Engine> = ls
        .into_iter()
        .enumerate()
        .map(|(me, l)| {
            let mut req = request(&parts, me, &addrs);
            req.generation = me as u64 + 1;
            start(req, l, Duration::from_secs(2))
        })
        .collect();
    let mut out = std::mem::take(&mut es[1].outs);
    put(&mut es[0].ins[0], FrameKind::Rows, rows(0..100)).await;
    let e1 = es.pop().unwrap();
    let e0 = es.pop().unwrap();
    let err = failure(e0).await;
    assert_eq!(err.code, ErrorCode::Transient);
    assert!(
        err.message.contains("refused") && err.message.contains("generation 1"),
        "{err}"
    );
    let err = failure(e1).await;
    assert_eq!(err.code, ErrorCode::Transient);
    assert!(
        err.message.contains("refused") && err.message.contains("generation 2"),
        "{err}"
    );
    while let Ok(Some(f)) = frame::read(&mut out[0]).await {
        assert_ne!(f.kind, FrameKind::Batch);
    }
}

/// An engine without the attempt's secret is refused; one that trusts
/// another certificate doesn't connect.
#[tokio::test]
async fn an_engine_without_the_attempts_identity_is_refused() {
    for wrong in ["secret", "certificate"] {
        let (ls, addrs) = listeners(2).await;
        let parts = [(1, 1), (1, 1)];
        let mut es: Vec<Engine> = ls
            .into_iter()
            .enumerate()
            .map(|(me, l)| {
                let mut req = request(&parts, me, &addrs);
                let m = req.mesh.as_mut().unwrap();
                if me == 0 {
                    match wrong {
                        "secret" => m.secret = b"another".to_vec(),
                        _ => (m.certificate, m.key) = (der(OTHER_CERT), der(OTHER_KEY)),
                    }
                }
                start(req, l, Duration::from_secs(2))
            })
            .collect();
        let e1 = es.pop().unwrap();
        let e0 = es.pop().unwrap();
        for (i, e) in [e0, e1].into_iter().enumerate() {
            let err = failure(e).await;
            assert_eq!(err.code, ErrorCode::Transient, "{wrong}, engine {i}: {err}");
            let m = &err.message;
            match wrong {
                "secret" => assert!(m.contains("refused") && m.contains("secret"), "{err}"),
                _ => assert!(
                    m.contains("wasn't reached") || m.contains("didn't connect"),
                    "{err}"
                ),
            }
        }
    }
}

/// An engine lost mid-epoch fails the others' jobs as transient, and no
/// writer lane passes the epoch's barrier.
#[tokio::test]
async fn an_engine_lost_mid_epoch_fails_the_job_as_transient() {
    let mut es = mesh(&[(1, 1), (1, 1)], |_, _| {}).await;
    let mut out = std::mem::take(&mut es[1].outs);
    put(&mut es[0].ins[0], FrameKind::Rows, rows(0..200)).await;
    put(&mut es[1].ins[0], FrameKind::Rows, rows(200..400)).await;
    put(&mut es[1].ins[0], FrameKind::Barrier, barrier(1, 1)).await;
    // Engine 1's writer lane has rows from engine 0 before it's lost.
    let mut from_0 = false;
    while !from_0 {
        let f = frame::read(&mut out[0]).await.unwrap().unwrap();
        assert_ne!(f.kind, FrameKind::Barrier);
        if f.kind == FrameKind::Batch {
            from_0 = ids(&f).iter().any(|id| *id < 200);
        }
    }
    let e1 = es.pop().unwrap();
    let e0 = es.pop().unwrap();
    e0.job.abort();
    let reader = tokio::spawn(async move {
        let mut kinds = Vec::new();
        while let Ok(Some(f)) = frame::read(&mut out[0]).await {
            kinds.push(f.kind);
        }
        kinds
    });
    let err = failure(e1).await;
    assert_eq!(err.code, ErrorCode::Transient);
    assert!(
        err.message.contains("engine 1 at") && err.message.contains("lost"),
        "{err}"
    );
    assert!(!reader.await.unwrap().contains(&FrameKind::Barrier));
}

/// Rows a second through two lanes and two writer lanes of a keyed
/// stream's exchange: in one engine, and in two over loopback TLS, a lane
/// and a writer lane each, on as many threads. `cargo test --release --
/// --ignored --nocapture exchange_throughput`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "a benchmark"]
async fn exchange_throughput() {
    use openetl_proto::engine::v1::{DiscardSink, EdgeDirection, SinkPlan, sink_plan};
    const ROWS: usize = 8192;
    const BATCHES: usize = 250;
    let rows: Vec<String> = (0..ROWS)
        .map(|i| {
            format!(
                r#"{{"id": {i}, "a": "name-{i}", "b": {}, "c": {}.5, "d": "2026-11-15", "e": true, "f": "{}", "g": {}, "h": "x", "j": {}}}"#,
                i * 7,
                i,
                "z".repeat(i % 24),
                i % 1000,
                i * 3
            )
        })
        .collect();
    let mut n = openetl_normalize::Normalizer::default();
    let batch = n.push(rows.iter().map(|r| r.as_bytes())).unwrap().batch;
    let payload = crate::ipc::encode_batch(&batch).unwrap();

    async fn run(parts: &[(u32, u32)], payload: &[u8]) -> Duration {
        let (ls, addrs) = listeners(parts.len()).await;
        let (mut jobs, mut ins) = (Vec::new(), Vec::new());
        for (me, l) in ls.into_iter().enumerate() {
            let mut req = request(parts, me, &addrs);
            if parts.len() == 1 {
                req.mesh = None;
            }
            req.edges
                .retain(|e| e.direction() == EdgeDirection::ToEngine);
            req.sink = Some(SinkPlan {
                sink: Some(sink_plan::Sink::Discard(DiscardSink {})),
            });
            let plan = Arc::new(Plan::from_request(req).unwrap());
            let mut readers = Vec::new();
            for _ in 0..plan.inbound.len() {
                let (w, r) = duplex(1 << 20);
                ins.push(w);
                readers.push(BufReader::new(r));
            }
            let (tx, mut events) = mpsc::channel(64);
            tokio::spawn(async move { while events.recv().await.is_some() {} });
            let listener = plan
                .mesh
                .as_ref()
                .and_then(|m| m.identity.as_ref())
                .map(|_| l);
            jobs.push(tokio::spawn(run_with::<_, DuplexStream>(
                plan,
                readers,
                Vec::new(),
                tx,
                listener,
            )));
        }
        let start = std::time::Instant::now();
        let feeders: Vec<_> = ins
            .into_iter()
            .map(|mut w| {
                let p = payload.to_vec();
                tokio::spawn(async move {
                    for _ in 0..BATCHES {
                        frame::write(&mut w, FrameKind::Batch, 0, &p).await.unwrap();
                    }
                    put(&mut w, FrameKind::Barrier, barrier(1, 1)).await;
                    end(&mut w).await;
                })
            })
            .collect();
        for f in feeders {
            f.await.unwrap();
        }
        for j in jobs {
            j.await.unwrap().unwrap();
        }
        start.elapsed()
    }

    let total = (2 * ROWS * BATCHES) as f64;
    let (mut one, mut two) = (Duration::MAX, Duration::MAX);
    for _ in 0..3 {
        one = one.min(run(&[(2, 2)], &payload).await);
        two = two.min(run(&[(1, 1), (1, 1)], &payload).await);
    }
    let rate = |d: Duration| total / d.as_secs_f64();
    println!(
        "one engine: {:.0} rows/s; two engines: {:.0} rows/s; {:.0}% of one engine's, on 2 threads, {} bytes a batch of {ROWS} rows",
        rate(one),
        rate(two),
        100.0 * one.as_secs_f64() / two.as_secs_f64(),
        payload.len()
    );
}

/// Trusts any certificate, as a client of no attempt can.
#[derive(Debug)]
struct AnyCertificate(rustls::crypto::WebPkiSupportedAlgorithms);

impl ServerCertVerifier for AnyCertificate {
    fn verify_server_cert(
        &self,
        _: &CertificateDer<'_>,
        _: &[CertificateDer<'_>],
        _: &ServerName<'_>,
        _: &[u8],
        _: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.0)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.0)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_schemes()
    }
}

/// A client of no attempt whose hello claims half a gigabyte is cut off as
/// its header arrives, before any of it is read or held, and the attempt's
/// engines still connect and finish.
#[tokio::test]
async fn a_long_hello_is_refused_before_it_is_read() {
    use tokio::io::AsyncReadExt;
    let parts = [(1, 1), (1, 1)];
    let (ls, addrs) = listeners(2).await;
    let mut es: Vec<Engine> = ls
        .into_iter()
        .enumerate()
        .map(|(me, l)| start(request(&parts, me, &addrs), l, Duration::from_secs(10)))
        .collect();
    let p = provider();
    let cfg = ClientConfig::builder_with_provider(p.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(AnyCertificate(
            p.signature_verification_algorithms,
        )))
        .with_no_client_auth();
    let tcp = TcpStream::connect(&addrs[1]).await.unwrap();
    let name = ServerName::try_from(SERVER_NAME).unwrap();
    let mut c = TlsConnector::from(Arc::new(cfg))
        .connect(name, tcp)
        .await
        .unwrap();
    c.write_all(&frame::header(FrameKind::MeshHello, 0, 512 << 20))
        .await
        .unwrap();
    c.flush().await.unwrap();
    let mut buf = [0u8; 16];
    let read = tokio::time::timeout(Duration::from_secs(5), c.read(&mut buf))
        .await
        .expect("the listener kept the connection open");
    assert!(matches!(read, Ok(0) | Err(_)), "{read:?}");

    let drains: Vec<_> = es
        .iter_mut()
        .map(|e| drain(std::mem::take(&mut e.outs)))
        .collect();
    for (i, e) in es.iter_mut().enumerate() {
        let from = i as i64 * 100;
        put(&mut e.ins[0], FrameKind::Rows, rows(from..from + 100)).await;
        put(&mut e.ins[0], FrameKind::Barrier, barrier(1, 1)).await;
        end(&mut e.ins[0]).await;
    }
    let mut all = BTreeSet::new();
    for (e, drains) in es.into_iter().zip(drains) {
        for d in drains {
            all.extend(around_barrier(&d.await.unwrap()).0);
        }
        e.job.await.unwrap().unwrap();
    }
    assert_eq!(all, (0..200).collect());
}

/// A listen address without a host, as Go writes it, is every address.
#[tokio::test]
async fn a_listen_address_without_a_host_binds_every_address() {
    let mut req = request(&[(1, 1), (1, 1)], 0, &[":0".into(), "127.0.0.1:1".into()]);
    req.mesh.as_mut().unwrap().listen = ":0".into();
    let plan = Plan::from_request(req).unwrap();
    let l = bind(&plan).await.unwrap().unwrap();
    let at = l.local_addr().unwrap();
    assert!(at.ip().is_unspecified(), "{at}");
    // IPv4 peers reach it, whatever the host's net.ipv6.bindv6only.
    let (dialed, accepted) = tokio::join!(
        TcpStream::connect(("127.0.0.1", at.port())),
        tokio::time::timeout(Duration::from_secs(5), l.accept())
    );
    dialed.expect("an IPv4 connection");
    accepted
        .expect("no IPv4 connection accepted")
        .expect("an IPv4 connection");
    // And IPv6 peers, where the listener has IPv6.
    if at.is_ipv6() {
        let (dialed, accepted) = tokio::join!(
            TcpStream::connect(("::1", at.port())),
            tokio::time::timeout(Duration::from_secs(5), l.accept())
        );
        dialed.expect("an IPv6 connection");
        accepted
            .expect("no IPv6 connection accepted")
            .expect("an IPv6 connection");
    }
}

/// A watermark crosses engines as it goes, and back as it came.
#[test]
fn a_watermark_crosses_as_its_frame() {
    let frame = wire(Out::Watermark(1, 1234)).unwrap();
    assert_eq!((frame.0, frame.1), (FrameKind::Watermark, 1));
    let f = Frame {
        kind: frame.0,
        stream: frame.1,
        payload: frame.2,
    };
    assert!(matches!(unwire(f, 1), Ok(Out::Watermark(1, 1234))));
}

/// Engine `me` of a mesh of two, a lane and a writer lane each, of stream
/// e with event time from `floor`, whose statement writes c, exchanged by
/// id.
fn timed_mesh(me: usize, addrs: &[String], floor: Option<i64>) -> StartJobRequest {
    let mut req = crate::dataflow::tests::timed_request(1, 1, false, floor);
    let mesh = request(&[(1, 1), (1, 1)], me, addrs);
    req.outputs[0].exchange = exchange_by(&["id"]);
    req.first_lane = mesh.first_lane;
    req.first_writer_lane = mesh.first_writer_lane;
    req.attempt_writer_lanes = mesh.attempt_writer_lanes;
    req.mesh = mesh.mesh;
    req
}

/// The writer lane's watermark of `stream` in each Metrics event, with the
/// epoch last ready before it.
fn writer_marks(
    events: &mut mpsc::Receiver<Result<EventsResponse, Status>>,
    stream: &str,
) -> Vec<(u64, Option<i64>)> {
    let (mut n, mut out) = (0, Vec::new());
    while let Ok(Ok(ev)) = events.try_recv() {
        match ev.event {
            Some(events_response::Event::EpochReady(r)) => n = r.epoch.unwrap().n,
            Some(events_response::Event::Metrics(m)) => out.push((
                n,
                m.streams
                    .iter()
                    .find(|s| s.stream == stream)
                    .and_then(|s| s.watermark),
            )),
            None => {}
        }
    }
    out
}

/// Runs a mesh of two timed engines, each lane's frames as `feed` gives
/// them, and returns each engine's writer lane's watermarks of c.
async fn timed_run(
    floor: Option<i64>,
    feed: [Vec<(FrameKind, Vec<u8>)>; 2],
) -> Vec<Vec<(u64, Option<i64>)>> {
    let (ls, addrs) = listeners(2).await;
    let mut es: Vec<Engine> = ls
        .into_iter()
        .enumerate()
        .map(|(me, l)| start(timed_mesh(me, &addrs, floor), l, Duration::from_secs(10)))
        .collect();
    let drains: Vec<_> = es
        .iter_mut()
        .map(|e| drain(std::mem::take(&mut e.outs)))
        .collect();
    for (e, frames) in es.iter_mut().zip(feed) {
        for (kind, payload) in frames {
            put(&mut e.ins[0], kind, payload).await;
        }
        end(&mut e.ins[0]).await;
    }
    let mut out = Vec::new();
    for (e, drains) in es.into_iter().zip(drains) {
        for d in drains {
            d.await.unwrap();
        }
        e.job.await.unwrap().unwrap();
        let mut events = e.events;
        out.push(writer_marks(&mut events, "c"));
    }
    out
}

/// A writer lane's watermark is the least of every lane of the attempt's,
/// local and remote, aligned with their barriers: engine 0's lane says 200
/// only past its barrier, which waits for engine 1's lane at 250, so at
/// epoch 1 each writer lane is at 60, and past it at 200, never back.
#[tokio::test]
async fn a_watermark_crossing_engines_is_the_least_aligned_with_barriers() {
    use crate::dataflow::tests::split;
    use FrameKind as K;
    let marks = timed_run(
        Some(20),
        [
            vec![
                (K::Split, split("a", Some(60))),
                (K::Barrier, barrier(1, 1)),
                (K::Split, split("a", Some(200))),
                (K::Barrier, barrier(1, 2)),
            ],
            vec![
                (K::Split, split("b", Some(250))),
                (K::Barrier, barrier(1, 1)),
                (K::Barrier, barrier(1, 2)),
            ],
        ],
    )
    .await;
    for (engine, m) in marks.iter().enumerate() {
        let at_1 = m.iter().find(|(n, _)| *n == 1).map(|(_, w)| *w);
        assert_eq!(at_1, Some(Some(60)), "engine {engine}: {m:?}");
        assert_eq!(
            m.last().map(|(_, w)| *w),
            Some(Some(200)),
            "engine {engine}: {m:?}"
        );
        let ws: Vec<i64> = m.iter().filter_map(|(_, w)| *w).collect();
        assert!(
            ws.windows(2).all(|p| p[0] <= p[1]),
            "engine {engine} went back: {m:?}"
        );
    }
}

/// A restored attempt's writer lanes start at the floor, and a lane of
/// either engine below it holds none of them under it.
#[tokio::test]
async fn a_restore_keeps_the_floor_across_engines() {
    use crate::dataflow::tests::split;
    use FrameKind as K;
    let marks = timed_run(
        Some(100),
        [
            vec![
                (K::Split, split("a", Some(60))),
                (K::Barrier, barrier(1, 1)),
            ],
            vec![
                (K::Split, split("b", Some(250))),
                (K::Barrier, barrier(1, 1)),
            ],
        ],
    )
    .await;
    for (engine, m) in marks.iter().enumerate() {
        assert!(!m.is_empty(), "engine {engine} reported nothing");
        for (_, w) in m {
            assert_eq!(*w, Some(100), "engine {engine}: {m:?}");
        }
    }
}

/// An engine lost by another fails it with that engine's index in a
/// PeerLost detail.
#[tokio::test]
async fn an_engine_lost_is_marked_as_lost() {
    let mut es = mesh(&[(1, 1), (1, 1)], |_, _| {}).await;
    put(&mut es[1].ins[0], FrameKind::Rows, rows(0..10)).await;
    let e1 = es.pop().unwrap();
    let e0 = es.pop().unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    e0.job.abort();
    let err = failure(e1).await;
    assert_eq!(err.peer_lost, Some(0), "{err}");
}

/// A group window's rows and their keyed lanes, across engines.
mod windows {
    use std::collections::{BTreeMap, BTreeSet};
    use std::path::Path;

    use openetl_proto::engine::v1::{EdgeDirection, StateSnapshot};

    use super::*;
    use crate::dataflow::tests::{
        MINUTES, SEC, WindowRow, barrier_of, minute_row, row, rows as json_rows, split,
        window_request, window_rows,
    };
    use crate::watermark::END;

    /// Engine `me` of a mesh of `parts`, each its lanes and writer lanes, of
    /// a job whose window counts stream a's rows a minute by email, and
    /// keeps its state below `state`. Stream b passes through.
    fn window_mesh(
        state: &Path,
        parts: &[(u32, u32)],
        me: usize,
        addrs: &[String],
        generation: u64,
        first_epoch: u64,
    ) -> StartJobRequest {
        window_mesh_of(state, MINUTES, parts, me, addrs, generation, first_epoch)
    }

    /// As window_mesh, of the statement `sql`, which writes minutes.
    fn window_mesh_of(
        state: &Path,
        sql: &str,
        parts: &[(u32, u32)],
        me: usize,
        addrs: &[String],
        generation: u64,
        first_epoch: u64,
    ) -> StartJobRequest {
        let (lanes, writers) = parts[me];
        let mut req = window_request(state, sql, lanes, generation, first_epoch);
        req.writer_lanes = writers;
        req.edges
            .retain(|e| e.direction() == EdgeDirection::ToEngine || e.lane < writers);
        let mesh = request(parts, me, addrs);
        req.first_lane = mesh.first_lane;
        req.first_writer_lane = mesh.first_writer_lane;
        req.attempt_writer_lanes = mesh.attempt_writer_lanes;
        req.mesh = mesh.mesh;
        req
    }

    async fn start_mesh(
        reqs: impl Fn(usize, &[String]) -> StartJobRequest,
        n: usize,
    ) -> Vec<Engine> {
        let (ls, addrs) = listeners(n).await;
        ls.into_iter()
            .enumerate()
            .map(|(me, l)| start(reqs(me, &addrs), l, Duration::from_secs(10)))
            .collect()
    }

    async fn put_on(w: &mut DuplexStream, kind: FrameKind, stream: u32, payload: Vec<u8>) {
        frame::write(w, kind, stream, &payload).await.unwrap();
    }

    /// Barrier n of both streams.
    async fn barriers(w: &mut DuplexStream, generation: u64, n: u64) {
        put_on(w, FrameKind::Barrier, 0, barrier_of(generation, n)).await;
        put_on(w, FrameKind::Barrier, 1, barrier_of(generation, n)).await;
    }

    /// The end of both streams.
    async fn end_both(w: &mut DuplexStream) {
        put_on(w, FrameKind::EndOfStream, 0, vec![]).await;
        put_on(w, FrameKind::EndOfStream, 1, vec![]).await;
        w.shutdown().await.unwrap();
    }

    /// Split `id`'s rows at `seconds`, keyed in turn by x, y, z and w.
    async fn timed_rows(w: &mut DuplexStream, id: &str, first: i64, seconds: &[i64]) {
        put_on(w, FrameKind::Split, 0, split(id, None)).await;
        let rs: Vec<String> = seconds
            .iter()
            .enumerate()
            .map(|(i, s)| minute_row(first + i as i64, ["x", "y", "z", "w"][i % 4], *s))
            .collect();
        let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
        put_on(w, FrameKind::Rows, 0, json_rows(&refs)).await;
    }

    /// Each engine's writer lanes' frames, in the attempt's order, and each
    /// engine's EpochReady events.
    async fn finish(
        es: Vec<Engine>,
        drains: Vec<Vec<JoinHandle<Vec<Frame>>>>,
    ) -> (Vec<Vec<Frame>>, Vec<Vec<EpochReady>>) {
        let (mut outs, mut ready) = (Vec::new(), Vec::new());
        for (e, drains) in es.into_iter().zip(drains) {
            for d in drains {
                outs.push(d.await.unwrap());
            }
            e.job.await.unwrap().unwrap();
            let mut events = e.events;
            ready.push(super::ready(&mut events).await);
        }
        (outs, ready)
    }

    fn sorted(rows: Vec<Vec<WindowRow>>) -> Vec<WindowRow> {
        let mut all: Vec<WindowRow> = rows.into_iter().flatten().collect();
        all.sort();
        all
    }

    /// Each snapshot's key groups, which together must be every key group
    /// once.
    fn assert_every_key_group_once(snapshots: &[StateSnapshot]) {
        let mut ranges: Vec<(u32, u32)> = snapshots
            .iter()
            .map(|s| {
                let r = s.key_groups.unwrap();
                (r.first, r.last)
            })
            .collect();
        ranges.sort();
        let mut next = 0;
        for (first, last) in &ranges {
            assert_eq!(*first, next, "{ranges:?}");
            next = last + 1;
        }
        assert_eq!(next, 256, "{ranges:?}");
    }

    /// A keyed lane's rows cross engines with the cells the stream holds,
    /// and the columns' metadata, which their key groups go by; its
    /// watermarks, barriers and end as they are.
    #[test]
    fn a_keyed_lanes_frames_cross_as_they_are() {
        use crate::dataflow::keyed::{In, Origin, unwire, wire};
        use arrow_array::{Int64Array, RecordBatch, StringArray};
        use arrow_schema::{DataType, Field, Schema};
        let md: std::collections::HashMap<String, String> =
            [("openetl.json".to_string(), "true".to_string())].into();
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true).with_metadata(md.clone()),
            Field::new("v", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec![Some("{}"), None])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap();
        let held = vec![None, Some(("v".to_string(), "too wide".to_string()))];
        let cross = |item: In| {
            let (kind, stream, payload) = wire(item).unwrap();
            unwire(
                Frame {
                    kind,
                    stream,
                    payload,
                },
                3,
            )
        };
        let Ok(In::Rows(2, got, got_held, None)) =
            cross(In::Rows(2, batch.clone(), held.clone(), None))
        else {
            panic!("rows didn't cross as rows");
        };
        assert_eq!(got, batch);
        assert_eq!(got.schema().field(0).metadata(), &md);
        assert_eq!(got_held, held);
        // With the split and place they were read at, where the epoch's
        // steps are recorded; a redo's never cross.
        let at = Some(Origin::At("orders-3".into(), 42));
        let Ok(In::Rows(2, got, _, got_at)) =
            cross(In::Rows(2, batch.clone(), held.clone(), at.clone()))
        else {
            panic!("rows didn't cross as rows");
        };
        assert_eq!((got, got_at), (batch.clone(), at));
        let met = Some(Origin::Met(vec![None, Some(5)]));
        assert!(wire(In::Rows(2, batch.clone(), held.clone(), met)).is_err());
        assert!(matches!(
            cross(In::Watermark(1, 77)),
            Ok(In::Watermark(1, 77))
        ));
        assert!(matches!(cross(In::End(4)), Ok(In::End(4))));
        let e = Epoch {
            generation: 3,
            n: 9,
        };
        assert!(matches!(cross(In::Barrier(0, e)), Ok(In::Barrier(0, b)) if b == e));
        let stale = Epoch {
            generation: 2,
            n: 9,
        };
        assert!(cross(In::Barrier(0, stale)).is_err());
    }

    /// Each lane of the attempt reads rows of x, y, z and w over the first
    /// two minutes, and the windows they fall in, as `timed_rows` gives them.
    const SECONDS: [i64; 8] = [0, 13, 26, 39, 52, 65, 78, 91];

    /// A window's rows cross engines to the keyed lanes that own their keys'
    /// groups: each key's minutes come once, from one writer lane of the
    /// attempt, with every engine's rows in them, in the epoch every lane of
    /// the attempt passed their end. Each engine's keyed lanes keep the state
    /// of their own key groups, which together are every key group once, and
    /// all report the same watermark at each barrier.
    #[tokio::test]
    async fn a_window_takes_its_keys_rows_from_every_engine() {
        let per_lane = window_across_engines().await;
        for key in ["x", "y", "z", "w"] {
            let lanes = per_lane
                .iter()
                .filter(|r| r.iter().any(|w| w.email == key))
                .count();
            assert_eq!(lanes, 1, "{key} on {lanes} writer lanes");
        }
    }

    /// A window's keyed lanes give its output to their own writer lanes, so
    /// a writer lane's part of an epoch is of the key groups of the window's
    /// keys, which a redo reads it again by: a job that exchanges the
    /// output again before its writer, by its own key, is refused, on one
    /// engine or several.
    #[test]
    fn a_windows_output_is_never_exchanged_again() {
        let state = tempfile::tempdir().unwrap();
        let addrs = ["127.0.0.1:7000".to_string(), "127.0.0.1:7001".to_string()];
        let mut alone = window_request(state.path(), MINUTES, 2, 1, 1);
        alone.outputs[0].exchange = exchange_by(&["window_start", "email"]);
        let mut meshed = window_mesh(state.path(), &[(1, 1), (1, 1)], 0, &addrs, 1, 1);
        meshed.outputs[0].exchange = exchange_by(&["window_start", "email"]);
        for req in [alone, meshed] {
            let Err(e) = Plan::from_request(req) else {
                panic!("planned");
            };
            assert_eq!(e.code, ErrorCode::Internal, "{e}");
            assert!(
                e.message.contains("exchanges it again before its writer"),
                "{e}"
            );
        }
        // Its own key's groups are a lane's part, unexchanged.
        assert!(Plan::from_request(window_request(state.path(), MINUTES, 2, 1, 1)).is_ok());
    }

    /// Runs `a_window_takes_its_keys_rows_from_every_engine`'s job, and
    /// returns each writer lane's rows.
    async fn window_across_engines() -> Vec<Vec<WindowRow>> {
        let state = tempfile::tempdir().unwrap();
        let parts = [(2, 1), (1, 1)];
        let mut es = start_mesh(
            |me, addrs| window_mesh(state.path(), &parts, me, addrs, 1, 1),
            2,
        )
        .await;
        let drains: Vec<_> = es
            .iter_mut()
            .map(|e| drain(std::mem::take(&mut e.outs)))
            .collect();
        let mut lane = 0;
        for e in &mut es {
            for w in &mut e.ins {
                let id = format!("s{lane}");
                timed_rows(w, &id, lane * 100, &SECONDS).await;
                barriers(w, 1, 1).await;
                put_on(w, FrameKind::Split, 0, split(&id, Some(END))).await;
                barriers(w, 1, 2).await;
                end_both(w).await;
                lane += 1;
            }
        }
        let (outs, ready) = finish(es, drains).await;
        let per_lane = window_rows(&outs, 1);
        // Each lane's x at 0 and 52, y at 13, z at 26 and w at 39 in the
        // first minute, which closes once every lane is at 91; the rest at
        // the end.
        assert_eq!(
            sorted(per_lane.clone()),
            [
                row(0, "w", 3, 1),
                row(0, "x", 6, 1),
                row(0, "y", 3, 1),
                row(0, "z", 3, 1),
                row(60, "w", 3, 2),
                row(60, "y", 3, 2),
                row(60, "z", 3, 2),
            ]
        );
        let mut snapshots = Vec::new();
        for (engine, r) in ready.iter().enumerate() {
            let marks: Vec<i64> = r.iter().map(|r| r.window_watermarks["a"]).collect();
            assert_eq!(marks, [91 * SEC, END], "engine {engine}");
            snapshots.extend(r[0].snapshots.clone());
        }
        assert_eq!(snapshots.len(), 3, "a snapshot a keyed lane");
        assert_every_key_group_once(&snapshots);
        // Each engine reports its keyed lanes' part of every epoch, and the
        // parts add up to every row in and every window's row.
        assert!(ready.iter().flatten().all(|r| r.operators.len() == 1));
        let ops: Vec<_> = ready.iter().flatten().flat_map(|r| &r.operators).collect();
        assert_eq!(
            ops.iter().map(|o| o.rows_in).sum::<u64>(),
            3 * SECONDS.len() as u64
        );
        assert_eq!(ops.iter().map(|o| o.inserts).sum::<u64>(), 7);
        per_lane
    }

    /// What one epoch of a window gave: its window rows, the rows it
    /// quarantined, its window watermark at the barrier, and the steps of
    /// that watermark its rows met.
    #[derive(Debug)]
    struct OneEpoch {
        rows: Vec<WindowRow>,
        late: u64,
        closed: i64,
        steps: Vec<openetl_proto::engine::v1::LaneSteps>,
    }

    /// One epoch of `sql` over stream a on one lane: the rows of key x at
    /// `seconds`, in that order, each a batch of its own, then the barrier.
    /// Live where `redo` is None, recording the steps of the window's
    /// watermark; else a redo of `redo`'s epoch, which replays its steps
    /// and closes the windows where it did.
    async fn one_epoch(sql: &str, seconds: &[i64], redo: Option<&OneEpoch>) -> OneEpoch {
        let dir = tempfile::tempdir().unwrap();
        let mut req = window_request(&dir.path().join("state"), sql, 1, 1, 1);
        req.quarantine_url = format!("file://{}", dir.path().display());
        req.record_window_steps = true;
        if let Some(live) = redo {
            req.state_read_only = true;
            req.streams[0].event_time.as_mut().unwrap().closes_at = Some(live.closed);
            req.window_steps = vec![openetl_proto::engine::v1::WindowSteps {
                operator: req.transform.as_ref().unwrap().operators[0].clone(),
                lanes: live.steps.clone(),
            }];
        }
        let mut h = crate::dataflow::tests::start_lanes(req);
        // Written while the writer lanes are read.
        let mut w = h.ins.remove(0);
        let seconds = seconds.to_vec();
        let writes = tokio::spawn(async move {
            put_on(&mut w, FrameKind::Split, 0, split("s", None)).await;
            for (i, s) in seconds.iter().enumerate() {
                let row = minute_row(i as i64, "x", *s);
                put_on(&mut w, FrameKind::Rows, 0, json_rows(&[&row])).await;
            }
            barriers(&mut w, 1, 1).await;
            for s in 0..2 {
                put_on(&mut w, FrameKind::EndOfStream, s, vec![]).await;
            }
            w.shutdown().await.unwrap();
        });
        let (outs, ready) = h.finish().await;
        writes.await.unwrap();
        let late = ready
            .iter()
            .flat_map(|r| &r.quarantine)
            .filter(|q| q.stream == "minutes")
            .map(|q| q.rows)
            .sum();
        let mut rows: Vec<WindowRow> = window_rows(&outs, 1).into_iter().flatten().collect();
        rows.sort();
        OneEpoch {
            rows,
            late,
            closed: ready[0].window_watermarks["a"],
            steps: ready[0]
                .operators
                .iter()
                .flat_map(|o| o.steps.clone())
                .collect(),
        }
    }

    /// The live epoch of `seconds`, and a redo of it, which gives the same
    /// window rows and quarantines the same rows: the live epoch's.
    async fn redone(sql: &str, seconds: &[i64]) -> OneEpoch {
        let live = one_epoch(sql, seconds, None).await;
        let redo = one_epoch(sql, seconds, Some(&live)).await;
        assert_eq!(
            (&redo.rows, redo.late),
            (&live.rows, live.late),
            "{seconds:?}: the redo, of {:?}",
            live.steps
        );
        assert!(redo.steps.is_empty(), "a redo records no steps");
        live
    }

    /// A redo replays the steps of the window's watermark that the epoch's
    /// rows met, so a row that came behind its window's close inside the
    /// epoch is late there too, and the window's row is the epoch's.
    #[tokio::test]
    async fn a_redo_takes_a_row_late_inside_its_epoch_as_late() {
        // 10 s, then 70 s, which closes the first minute, then 20 s.
        let live = redone(MINUTES, &[10, 70, 20]).await;
        assert_eq!(
            (live.rows, live.late, live.closed),
            (vec![row(0, "x", 1, 1)], 1, 70 * SEC)
        );
        // The minute's end, which the 20 s row met, is a step; the
        // watermark's moves inside a minute are none.
        let steps: Vec<_> = live
            .steps
            .iter()
            .flat_map(|l| &l.splits)
            .map(|s| (s.split.as_str(), s.places.clone(), s.watermarks.clone()))
            .collect();
        assert_eq!(steps, [("s", vec![1, 2], vec![0, 60 * SEC])]);
        // Without the late row, the same.
        let live = redone(MINUTES, &[10, 20, 70]).await;
        assert_eq!((live.rows, live.late), (vec![row(0, "x", 2, 1)], 0));
    }

    /// A hopping window: a row behind the watermark goes to the windows it
    /// falls in that are still open, and the redo's does too.
    #[tokio::test]
    async fn a_redo_takes_a_row_into_the_hops_still_open() {
        const HOPS: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
                            FROM a GROUP BY hop(at, INTERVAL '30 seconds', INTERVAL '1 minute'), email";
        // 10 s, then 70 s, which closes [-30, 30) and [0, 60), then 40 s,
        // which [30, 90) takes.
        let live = redone(HOPS, &[10, 70, 40]).await;
        assert_eq!(
            (live.rows, live.late),
            (vec![row(-30, "x", 1, 1), row(0, "x", 1, 1)], 0)
        );
    }

    /// Sessions, and rows behind the watermark inside the epoch. A row
    /// late there that meets two closed sessions: the epoch gives both
    /// sessions' rows and quarantines it, and so does the redo. A row
    /// behind the watermark that isn't late, as it meets an open session:
    /// the epoch gave the closed session's row before the row came, and
    /// the redo closes it before the row too, and gives its row.
    #[tokio::test]
    async fn a_redo_closes_the_sessions_a_row_behind_the_watermark_meets() {
        const SESSIONS: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
                                FROM a GROUP BY session(at, INTERVAL '10 seconds'), email";
        // 0 s and 20 s, closed by 45 s, then 10 s, which meets both.
        let live = redone(SESSIONS, &[0, 20, 45, 10]).await;
        assert_eq!(
            (live.rows, live.late),
            (vec![row(0, "x", 1, 1), row(20, "x", 1, 1)], 1)
        );
        // 0 s, closed by 19 s, then 9 s, which meets the open one.
        let live = redone(SESSIONS, &[0, 19, 9]).await;
        assert_eq!((live.rows, live.late), (vec![row(0, "x", 1, 1)], 0));
    }

    /// After 1,024 steps of the window's watermark in an epoch, a keyed lane
    /// holds the watermark where it is until the barrier: a row behind the
    /// senders' watermark then, but not behind the held one, is on time,
    /// and the redo takes it so too. One split's rows, which met the held
    /// watermark already, take no step past the 1,024th.
    #[tokio::test]
    async fn past_its_steps_a_keyed_lane_holds_the_watermark_until_the_barrier() {
        const SESSIONS: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
                                FROM a GROUP BY session(at, INTERVAL '10 seconds'), email";
        // A session every 20 s, each row a step, then a row 55 s behind
        // the last, which meets the session at 21,920 s.
        let mut seconds: Vec<i64> = (0..1100).map(|k| k * 20).collect();
        seconds.push(21_925);
        let live = redone(SESSIONS, &seconds).await;
        let steps: usize = live
            .steps
            .iter()
            .flat_map(|l| &l.splits)
            .map(|s| s.places.len())
            .sum();
        assert_eq!((steps, live.late), (crate::watermark::STEPS, 0));
        assert!(
            live.rows.contains(&row(21_920, "x", 2, 1)),
            "{:?}",
            live.rows.last()
        );
        assert_eq!(live.closed, 21_980 * SEC, "it caught up at the barrier");
    }

    /// Two engines of three lanes each send a window's rows to each other's
    /// keyed lanes at once, over links of the least room, while nothing
    /// reads their writer lanes for a while. The windows are a second long,
    /// so the keyed lanes write a row for each key every second, and every
    /// channel, link and socket between the engines fills: the lanes stop
    /// reading their edges. Each lane passes its barriers at another point of
    /// its rows, so keyed lanes wait at barriers with rows of senders past
    /// them, and of others not there yet, behind each other on one
    /// connection. A keyed lane keeps reading while it aligns, and sends only
    /// to its own engine's writer lane, so once the writer lanes are read
    /// every window comes out once, with every row: no wait goes in a circle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn windows_flow_both_ways_under_backpressure_with_barriers_in_flight() {
        const BATCHES: i64 = 120;
        const ROWS: i64 = 100;
        const KEYS: i64 = 64;
        const SECONDS: &str = "INSERT INTO minutes SELECT window_start, email, count(*) AS n \
                               FROM a GROUP BY tumble(at, INTERVAL '1 second'), email";
        let state = tempfile::tempdir().unwrap();
        let parts = [(3, 1), (3, 1)];
        let mut es = start_mesh(
            |me, addrs| {
                let mut req = window_mesh_of(state.path(), SECONDS, &parts, me, addrs, 1, 1);
                // An eighth of it for the one link: its least, MIN_BUFFER.
                req.limits = Some(openetl_proto::engine::v1::Limits {
                    memory_bytes: 2 << 20,
                });
                req
            },
            2,
        )
        .await;
        // Nothing reads the writer lanes until the lanes have stalled.
        let (go, read) = tokio::sync::watch::channel(false);
        let drains: Vec<JoinHandle<Vec<Frame>>> = es
            .iter_mut()
            .flat_map(|e| std::mem::take(&mut e.outs))
            .map(|mut r| {
                let mut read = read.clone();
                tokio::spawn(async move {
                    read.wait_for(|go| *go).await.unwrap();
                    let mut out = Vec::new();
                    while let Some(f) = frame::read(&mut r).await.unwrap() {
                        out.push(f);
                    }
                    out
                })
            })
            .collect();
        let mut feeders = Vec::new();
        let mut lane = 0i64;
        let mut want: BTreeMap<(i64, String), i64> = BTreeMap::new();
        let sent = Arc::new(std::sync::atomic::AtomicI64::new(0));
        for e in &mut es {
            for mut w in std::mem::take(&mut e.ins) {
                let l = lane;
                lane += 1;
                let sent = sent.clone();
                // Lane l passes barrier 1 after batch l * 17, and barrier 2
                // after batch 60 + l * 9.
                let (b1, b2) = (l * 17, 60 + l * 9);
                for b in 0..BATCHES {
                    for r in 0..ROWS {
                        *want
                            .entry((b, format!("k{}", (b * ROWS + r) % KEYS)))
                            .or_default() += 1;
                    }
                }
                feeders.push(tokio::spawn(async move {
                    let id = format!("s{l}");
                    put_on(&mut w, FrameKind::Split, 0, split(&id, None)).await;
                    for b in 0..BATCHES {
                        let rs: Vec<String> = (0..ROWS)
                            .map(|r| {
                                minute_row(
                                    l * 1_000_000 + b * ROWS + r,
                                    &format!("k{}", (b * ROWS + r) % KEYS),
                                    b,
                                )
                            })
                            .collect();
                        let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                        put_on(&mut w, FrameKind::Rows, 0, json_rows(&refs)).await;
                        sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if b == b1 {
                            barriers(&mut w, 1, 1).await;
                        }
                        if b == b2 {
                            barriers(&mut w, 1, 2).await;
                        }
                    }
                    put_on(&mut w, FrameKind::Split, 0, split(&id, Some(END))).await;
                    barriers(&mut w, 1, 3).await;
                    end_both(&mut w).await;
                }));
            }
        }
        // The engines hold their lanes back while the writer lanes aren't
        // read: no batch goes in for half a second, short of the last.
        let mut last = -1;
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let now = sent.load(std::sync::atomic::Ordering::Relaxed);
            assert!(
                now < 6 * BATCHES,
                "every lane's rows went in with nothing reading the writer lanes"
            );
            if now == last {
                break;
            }
            last = now;
        }
        assert!(last > 0, "no batch went in");
        go.send(true).unwrap();
        let run = async {
            for f in feeders {
                f.await.unwrap();
            }
            let mut outs = Vec::new();
            for d in drains {
                outs.push(d.await.unwrap());
            }
            for e in es {
                e.job.await.unwrap().unwrap();
            }
            outs
        };
        let outs = tokio::time::timeout(Duration::from_secs(120), run)
            .await
            .expect("the engines stopped moving");
        let mut got: BTreeMap<(i64, String), i64> = BTreeMap::new();
        for r in window_rows(&outs, 1).into_iter().flatten() {
            assert!(
                got.insert((r.start, r.email.clone()), r.n).is_none(),
                "window {} of {} came twice",
                r.start,
                r.email
            );
        }
        assert_eq!(got.len(), want.len());
        assert_eq!(got, want);
    }

    /// The first part of the job of `a_window_restores_onto_another_mesh`
    /// and `a_redo_closes_its_lanes_windows_where_the_epoch_did`: on a mesh
    /// of two engines of a lane and a writer lane each, epoch 1 reads each
    /// lane's rows of the first two minutes, and epoch 2 three more rows a
    /// lane at 100 to 110 seconds, then moves each split to 130 seconds, as
    /// when it's idle. Returns each writer lane's frames and each engine's
    /// EpochReady events.
    async fn two_epochs(state: &Path) -> (Vec<Vec<Frame>>, Vec<Vec<EpochReady>>) {
        let parts = [(1, 1), (1, 1)];
        let mut es = start_mesh(|me, addrs| window_mesh(state, &parts, me, addrs, 1, 1), 2).await;
        let drains: Vec<_> = es
            .iter_mut()
            .map(|e| drain(std::mem::take(&mut e.outs)))
            .collect();
        for (lane, e) in es.iter_mut().enumerate() {
            let w = &mut e.ins[0];
            let id = format!("s{lane}");
            let first = lane as i64 * 100;
            timed_rows(w, &id, first, &SECONDS).await;
            barriers(w, 1, 1).await;
            timed_rows(w, &id, first + 50, &[100, 105, 110]).await;
            put_on(w, FrameKind::Split, 0, split(&id, Some(130 * SEC))).await;
            barriers(w, 1, 2).await;
            end_both(w).await;
        }
        finish(es, drains).await
    }

    /// State written on a mesh of two engines restores on a mesh of three of
    /// other lanes, each keyed lane opening what the record names of its own
    /// key groups, from the floor: the second minute's windows go on with
    /// the rows read before, and a row of the first, which closed, is late.
    #[tokio::test]
    async fn a_window_restores_onto_another_mesh() {
        restore_onto_another_mesh(true).await;
    }

    /// Without a floor, each key group's horizon, which the state restored
    /// from the other engines' lanes keeps, closes what closed before.
    #[tokio::test]
    async fn a_restored_key_groups_horizon_keeps_its_windows_closed() {
        restore_onto_another_mesh(false).await;
    }

    async fn restore_onto_another_mesh(floored: bool) {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (_, first) = two_epochs(&state).await;
        let snapshots: Vec<StateSnapshot> =
            first.iter().flat_map(|r| r[0].snapshots.clone()).collect();
        let marked = first[0][0].window_watermarks["a"];
        assert_eq!(marked, 91 * SEC);
        let floor = floored.then_some(marked);

        let parts = [(2, 1), (1, 1), (1, 1)];
        let mut es = start_mesh(
            |me, addrs| {
                let mut req = window_mesh(&state, &parts, me, addrs, 2, 2);
                req.restored_snapshots = snapshots.clone();
                req.quarantine_url = format!("file://{}", dir.path().display());
                req.streams[0].event_time.as_mut().unwrap().floor = floor;
                req
            },
            3,
        )
        .await;
        let drains: Vec<_> = es
            .iter_mut()
            .map(|e| drain(std::mem::take(&mut e.outs)))
            .collect();
        let mut lane = 0;
        for e in &mut es {
            for w in &mut e.ins {
                let id = format!("r{lane}");
                put_on(w, FrameKind::Split, 0, split(&id, floor)).await;
                // One row of the second minute, and on the first lane one of
                // the first.
                let mut rs = vec![minute_row(1000 + lane, "y", 70)];
                if lane == 0 {
                    rs.push(minute_row(2000, "x", 30));
                }
                let refs: Vec<&str> = rs.iter().map(String::as_str).collect();
                put_on(w, FrameKind::Rows, 0, json_rows(&refs)).await;
                barriers(w, 2, 2).await;
                put_on(w, FrameKind::Split, 0, split(&id, Some(END))).await;
                barriers(w, 2, 3).await;
                end_both(w).await;
                lane += 1;
            }
        }
        let (outs, ready) = finish(es, drains).await;
        // y had one row a lane of two in the second minute, and gets one a
        // lane of four.
        assert_eq!(
            sorted(window_rows(&outs, 2)),
            [row(60, "w", 2, 3), row(60, "y", 6, 3), row(60, "z", 2, 3)]
        );
        // At the end of input every window has gone, and no lane names
        // any of the window's state.
        assert!(ready.iter().all(|r| r[1].snapshots.is_empty()));
        let late: u64 = ready
            .iter()
            .flatten()
            .flat_map(|r| &r.quarantine)
            .filter(|q| q.stream == "minutes")
            .map(|q| q.rows)
            .sum();
        assert_eq!(late, 1, "x at 30 is late");
        let restored: Vec<StateSnapshot> =
            ready.iter().flat_map(|r| r[0].snapshots.clone()).collect();
        assert_eq!(restored.len(), 4);
        assert_every_key_group_once(&restored);
    }

    /// A redo of one writer lane's part of an epoch, on one engine of one
    /// lane, from the state before the epoch: it reads every engine's splits
    /// again, keeps the rows of the lane's key groups, holds the windows at
    /// the floor while it reads, and closes them where the epoch's barrier
    /// did, though the splits' moves, which an idle split makes, aren't read
    /// again: what it writes is what the lane wrote.
    #[tokio::test]
    async fn a_redo_closes_its_lanes_windows_where_the_epoch_did() {
        let dir = tempfile::tempdir().unwrap();
        let state = dir.path().join("state");
        let (outs, ready) = two_epochs(&state).await;
        let before: Vec<StateSnapshot> =
            ready.iter().flat_map(|r| r[0].snapshots.clone()).collect();
        let (floor, closed) = (
            ready[0][0].window_watermarks["a"],
            ready[0][1].window_watermarks["a"],
        );
        assert_eq!((floor, closed), (91 * SEC, 130 * SEC));
        let wrote = window_rows(&outs, 1);
        let mut want = BTreeMap::new();
        for (w, rows) in wrote.iter().enumerate() {
            let epoch_2: Vec<WindowRow> = rows.iter().filter(|r| r.epoch == 2).cloned().collect();
            want.insert(w, epoch_2);
        }
        assert!(
            want.values().filter(|r| !r.is_empty()).count() == 2,
            "both writer lanes closed windows in epoch 2: {want:?}"
        );

        for (w, range) in [(0usize, (0u32, 127u32)), (1, (128, 255))] {
            let mut req = window_request(&state, MINUTES, 1, 3, 2);
            req.first_lane = u32::try_from(w).unwrap();
            req.state_read_only = true;
            req.restored_snapshots = before.clone();
            req.outputs[0].key_groups = Some(openetl_proto::engine::v1::KeyGroupRange {
                first: range.0,
                last: range.1,
            });
            let et = req.streams[0].event_time.as_mut().unwrap();
            (et.floor, et.closes_at) = (Some(floor), Some(closed));
            let mut h = crate::dataflow::tests::start_lanes(req);
            for s in 0..2 {
                timed_rows(
                    &mut h.ins[0],
                    &format!("s{s}"),
                    s * 100 + 50,
                    &[100, 105, 110],
                )
                .await;
            }
            barriers(&mut h.ins[0], 3, 2).await;
            h.end_all(2).await;
            let (redone, ready) = h.finish().await;
            let mut got: Vec<WindowRow> = window_rows(&redone, 2).into_iter().flatten().collect();
            got.sort();
            let mut wanted = want[&w].clone();
            wanted.sort();
            assert_eq!(got, wanted, "writer lane {w}");
            assert_eq!(ready.len(), 1);
            assert!(ready[0].snapshots.is_empty(), "a redo writes no state");
            assert!(ready[0].operators.is_empty(), "a redo reports no numbers");
        }
        let keys: BTreeSet<String> = want.values().flatten().map(|r| r.email.clone()).collect();
        assert!(keys.len() > 1, "{keys:?}");
    }
}
