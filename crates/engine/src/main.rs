//! `openetl-engine`: the Rust data engine. The runner starts one process per job
//! and drives it over a control socket and framed Arrow IPC edges.
//!
//! This is the P0 skeleton: it only reports its version.

use std::process::ExitCode;

const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> ExitCode {
    match std::env::args().nth(1).as_deref() {
        Some("--version") | Some("version") => {
            println!("openetl-engine {VERSION}");
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("usage: openetl-engine --version");
            ExitCode::from(2)
        }
    }
}
