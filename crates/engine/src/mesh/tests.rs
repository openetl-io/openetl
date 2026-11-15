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
fn request(parts: &[(u32, u32)], me: usize, addrs: &[String]) -> StartJobRequest {
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

async fn listeners(n: usize) -> (Vec<TcpListener>, Vec<String>) {
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
        match links.room(1, frame.2.len()).unwrap() {
            Room::Ready(r) => {
                pushed += frame.2.len() as u64;
                links.push(0, 1, frame, r).unwrap();
            }
            Room::Wait(..) => break,
        }
        assert!(pushed <= bound);
    }
    assert_eq!(links.queued(), [0, pushed]);
    assert!(pushed + (100 << 10) > bound);
    // A frame larger than the bound takes all of it.
    assert!(matches!(links.room(1, 64 << 20).unwrap(), Room::Wait(_, n) if u64::from(n) == bound));
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
        err.message.contains("engine 0") && err.message.contains("lost"),
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
