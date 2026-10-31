//! Compiles the connector and engine protocols with protox, a protobuf
//! compiler in Rust, so building the engine needs neither protoc nor buf.

use std::path::Path;

const PROTOS: &[&str] = &[
    "openetl/connector/v1/connector.proto",
    "openetl/connector/v1/destination.proto",
    "openetl/connector/v1/errors.proto",
    "openetl/connector/v1/host.proto",
    "openetl/connector/v1/source.proto",
    "openetl/engine/v1/engine.proto",
    "openetl/engine/v1/frames.proto",
];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    println!("cargo:rerun-if-changed={}", root.display());
    let fds = protox::compile(PROTOS, [&root])?;
    tonic_prost_build::configure().compile_fds(fds)?;
    Ok(())
}
