//! `openetl-engine`: the Rust data engine. The runner starts one process per
//! job and drives it over a control socket and framed Arrow IPC edges.
//!
//! `serve` binds both sockets, then prints `ready` on stdout. It exits after
//! Stop, when the runner closes an edge early, or, with `--watch-stdin`, when
//! stdin closes. `--threads` sets how many threads run its tasks: one per core
//! by default. Logs are JSON lines on stderr; `OPENETL_ENGINE_LOG` filters
//! them. Traces go out over OTLP as the OTEL_* variables say.
//!
//! `plan` plans a transform's SQL for the control plane, and for a runner
//! before a job starts: a request as JSON on stdin, or in `--file`, and the
//! answer as JSON on stdout (see [`sqlplan`]).

mod dataflow;
mod error;
mod frame;
mod ipc;
mod memory;
mod plan;
mod quarantine;
mod remote;
mod replay;
mod service;
mod sqlplan;
mod stage;
mod staging;
mod store;
mod telemetry;
mod transform;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use openetl_proto::engine::v1::engine_service_server::EngineServiceServer;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::layer::SubscriberExt as _;
use tracing_subscriber::util::SubscriberInitExt as _;
use tracing_subscriber::{EnvFilter, Layer as _};

#[global_allocator]
static ALLOCATOR: memory::Counting = memory::Counting;

/// The version a release build was made for, or the crate's.
const VERSION: &str = match option_env!("OPENETL_VERSION") {
    Some(v) => v,
    None => env!("CARGO_PKG_VERSION"),
};
const USAGE: &str = "usage: openetl-engine --version
       openetl-engine serve --control PATH --data PATH [--watch-stdin] [--threads N]
       openetl-engine plan [--file PATH]";
/// How long in-flight RPCs get to finish once shutdown starts.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

struct ServeArgs {
    control: PathBuf,
    data: PathBuf,
    watch_stdin: bool,
    threads: Option<usize>,
}

impl ServeArgs {
    fn parse(args: &[String]) -> Option<Self> {
        let (mut control, mut data, mut watch_stdin, mut threads) = (None, None, false, None);
        let mut it = args.iter();
        while let Some(a) = it.next() {
            match a.as_str() {
                "--control" => control = Some(PathBuf::from(it.next()?)),
                "--data" => data = Some(PathBuf::from(it.next()?)),
                "--watch-stdin" => watch_stdin = true,
                "--threads" => threads = Some(it.next()?.parse().ok().filter(|n| *n > 0)?),
                _ => return None,
            }
        }
        Some(Self {
            control: control?,
            data: data?,
            watch_stdin,
            threads,
        })
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("--version") | Some("version") => {
            println!("openetl-engine {VERSION}");
            ExitCode::SUCCESS
        }
        Some("serve") => match ServeArgs::parse(&args[1..]) {
            Some(a) => serve(a),
            None => usage(),
        },
        Some("plan") => match &args[1..] {
            [] => plan(None),
            [flag, path] if flag == "--file" => plan(Some(Path::new(path))),
            _ => usage(),
        },
        _ => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

/// Plans a request's SQL; see [`sqlplan`] for the exit codes.
fn plan(file: Option<&Path>) -> ExitCode {
    let read = match file {
        Some(p) => std::fs::read(p),
        None => {
            let mut b = Vec::new();
            std::io::stdin().read_to_end(&mut b).map(|_| b)
        }
    };
    let answer = read
        .map_err(|e| format!("reading the request: {e}"))
        .and_then(|text| {
            serde_json::from_slice::<sqlplan::Request>(&text)
                .map_err(|e| format!("the request isn't one: {e}"))
        })
        .and_then(|req| sqlplan::plan(&req))
        .and_then(|a| {
            serde_json::to_vec(&a)
                .map(|b| (a.errors.is_empty(), b))
                .map_err(|e| e.to_string())
        });
    let (ok, body) = match answer {
        Ok(a) => a,
        Err(e) => {
            eprintln!("openetl-engine plan: {e}");
            return ExitCode::from(2);
        }
    };
    let mut out = std::io::stdout().lock();
    if let Err(e) = out.write_all(&body).and_then(|()| out.flush()) {
        eprintln!("openetl-engine plan: writing the answer: {e}");
        return ExitCode::from(2);
    }
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::from(1)
    }
}

fn serve(args: ServeArgs) -> ExitCode {
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    if let Some(n) = args.threads {
        rt.worker_threads(n);
    }
    let rt = rt.enable_all().build();
    // The gRPC exporter needs the runtime.
    let traces = match &rt {
        Ok(rt) => {
            let _enter = rt.enter();
            telemetry::provider(VERSION)
        }
        Err(_) => Ok(None),
    };
    let (provider, traces_err) = match traces {
        Ok(p) => (p, None),
        Err(e) => (None, Some(e)),
    };
    let logs = tracing_subscriber::fmt::layer()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(std::io::stderr)
        // Reporting a failed write panics once the runner has stopped reading.
        .log_internal_errors(false)
        .with_filter(
            EnvFilter::try_from_env("OPENETL_ENGINE_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        );
    tracing_subscriber::registry()
        .with(logs)
        .with(
            provider
                .as_ref()
                .map(|p| telemetry::layer(p).with_filter(EnvFilter::new(telemetry::FILTER))),
        )
        .init();
    if let Some(e) = traces_err {
        warn!(error = %e, "traces aren't exported");
    }
    let rt = match rt {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "starting the runtime failed");
            return ExitCode::FAILURE;
        }
    };
    let res = rt.block_on(run(&args));
    if let Some(p) = &provider
        && let Err(e) = p.shutdown()
    {
        warn!(error = %e, "flushing traces failed");
    }
    rt.shutdown_timeout(Duration::from_millis(100));
    for p in [&args.control, &args.data] {
        let _ = std::fs::remove_file(p);
    }
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            error!(error = %e, "engine failed");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: &ServeArgs) -> Result<(), Box<dyn std::error::Error>> {
    let control = bind(&args.control)?;
    let data = bind(&args.data)?;
    let shutdown = CancellationToken::new();
    if args.watch_stdin {
        watch_stdin(shutdown.clone());
    }
    let engine = service::Engine::new(shutdown.clone());
    let edges = tokio::spawn(service::accept_edges(data, engine.clone()));

    let mut out = std::io::stdout();
    writeln!(out, "ready")?;
    out.flush()?;
    info!(version = VERSION, "engine ready");

    let server = tonic::transport::Server::builder()
        .add_service(EngineServiceServer::new(service::Control(engine)))
        .serve_with_incoming_shutdown(
            UnixListenerStream::new(control),
            shutdown.clone().cancelled_owned(),
        );
    let deadline = async {
        shutdown.cancelled().await;
        tokio::time::sleep(SHUTDOWN_GRACE).await;
    };
    let res = tokio::select! {
        r = server => r.map_err(Into::into),
        _ = deadline => {
            warn!("in-flight requests didn't finish before shutdown");
            Ok(())
        }
    };
    edges.abort();
    res
}

fn bind(path: &Path) -> std::io::Result<UnixListener> {
    UnixListener::bind(path)
        .map_err(|e| std::io::Error::new(e.kind(), format!("bind {}: {e}", path.display())))
}

/// The runner holds the other end of stdin, so EOF means it exited.
fn watch_stdin(shutdown: CancellationToken) {
    std::thread::spawn(move || {
        let mut stdin = std::io::stdin().lock();
        let mut buf = [0u8; 256];
        while let Ok(n) = stdin.read(&mut buf) {
            if n == 0 {
                break;
            }
        }
        shutdown.cancel();
        info!("stdin closed; exiting");
    });
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Option<ServeArgs> {
        let args: Vec<String> = ["--control", "c", "--data", "d"]
            .iter()
            .chain(args)
            .map(|a| a.to_string())
            .collect();
        ServeArgs::parse(&args)
    }

    #[test]
    fn threads() {
        assert_eq!(parse(&[]).unwrap().threads, None);
        assert_eq!(parse(&["--threads", "2"]).unwrap().threads, Some(2));
        assert!(parse(&["--threads", "0"]).is_none());
        assert!(parse(&["--threads", "many"]).is_none());
        assert!(parse(&["--threads"]).is_none());
    }
}
