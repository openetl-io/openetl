//! `openetl-engine`: the Rust data engine. The runner starts one process per
//! job and drives it over a control socket and framed Arrow IPC edges.
//!
//! `serve` binds both sockets, then prints `ready` on stdout. It exits after
//! Stop, when the runner closes an edge early, or, with `--watch-stdin`, when
//! stdin closes. Logs are JSON lines on stderr; `OPENETL_ENGINE_LOG` filters
//! them.

mod dataflow;
mod error;
mod frame;
mod ipc;
mod plan;
mod service;

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use openetl_proto::engine::v1::engine_service_server::EngineServiceServer;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const USAGE: &str = "usage: openetl-engine --version
       openetl-engine serve --control PATH --data PATH [--watch-stdin]";
/// How long in-flight RPCs get to finish once shutdown starts.
const SHUTDOWN_GRACE: Duration = Duration::from_secs(2);

struct ServeArgs {
    control: PathBuf,
    data: PathBuf,
    watch_stdin: bool,
}

impl ServeArgs {
    fn parse(args: &[String]) -> Option<Self> {
        let (mut control, mut data, mut watch_stdin) = (None, None, false);
        let mut it = args.iter();
        while let Some(a) = it.next() {
            match a.as_str() {
                "--control" => control = Some(PathBuf::from(it.next()?)),
                "--data" => data = Some(PathBuf::from(it.next()?)),
                "--watch-stdin" => watch_stdin = true,
                _ => return None,
            }
        }
        Some(Self {
            control: control?,
            data: data?,
            watch_stdin,
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
        _ => usage(),
    }
}

fn usage() -> ExitCode {
    eprintln!("{USAGE}");
    ExitCode::from(2)
}

fn serve(args: ServeArgs) -> ExitCode {
    tracing_subscriber::fmt()
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_writer(std::io::stderr)
        // Reporting a failed write panics once the runner has stopped reading.
        .log_internal_errors(false)
        .with_env_filter(
            EnvFilter::try_from_env("OPENETL_ENGINE_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            error!(error = %e, "starting the runtime failed");
            return ExitCode::FAILURE;
        }
    };
    let res = rt.block_on(run(&args));
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
