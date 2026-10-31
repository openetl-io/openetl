# OpenETL

OpenETL moves data from where it is produced to where it is used: databases, SaaS APIs, event streams and webhooks into warehouses, lakes, streams and HTTP endpoints. The same pipeline runs as a scheduled batch or continuously as a stream, with exactly-once delivery wherever the destination allows it.

OpenETL is MIT-licensed and built to be self-hosted.

**Status: pre-alpha.** Nothing here is usable yet. The first release, 0.1 alpha, will run scheduled batch syncs from Postgres and a few SaaS APIs into Postgres, a warehouse and Parquet on S3.

## Repository layout

| Path | What it holds |
|---|---|
| `proto/openetl` | The protocols: `connector/v1` (what connectors speak, including host services), `engine/v1` (runner to engine) and `api/v1` (the control-plane API) |
| `gen/`, `sdk/go/protocol/v1` | Go generated from `proto/` by `make generate`, committed |
| `internal/checkpoint` | The checkpoint log: one record per checkpoint plus a `HEAD` pointer in object storage, updated by compare-and-swap, which also fences out replaced jobs |
| `internal/runner` | The data-plane job driver and checkpoint coordinator |
| `internal/delivery` | Each stream's delivery guarantee, computed from the sink, the source's replay behaviour, the write mode and dedupe |
| `internal/platform/errs` | The error taxonomy every boundary carries |
| `sdk/go` | The Go connector SDK (a separate module) |
| `crates/` | `openetl-engine`, the Rust data engine, and `openetl-proto`, its generated protocol types |
| `test/sim` | A seeded simulation of the commit protocol under crashes, timeouts and zombie jobs |
| `deploy/compose` | Local dependencies: Postgres, Temporal, an S3-compatible store |

## Development

You need Go 1.26 and Rust (the toolchain is pinned in `rust-toolchain.toml`; rustup installs it). Developer tools are pinned in `tools/go.mod` and built into `bin/` by `make tools`.

```sh
make test          # Go and Rust unit tests
make sim           # commit-protocol simulation; SEEDS=10000 for more seeds
make lint          # buf lint, golangci-lint, rustfmt, clippy
make generate      # regenerate Go from proto/ after changing a .proto
make deps-up       # Postgres, Temporal and S3 in Docker
```

## License

[MIT](LICENSE)
