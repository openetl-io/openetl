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
| `internal/delivery` | Each stream's delivery guarantee, computed from the sink, the source's replay behaviour, the write mode, the primary key and the required level |
| `internal/platform/errs` | The error taxonomy every boundary carries |
| `sdk/go` | The Go connector SDK (a separate module), with `sdk.Serve` for external connectors |
| `internal/connectorhost` | Connectors the runner hosts: the few built in, declarative sources, and external connectors, which it starts as subprocesses |
| `crates/` | `openetl-engine`, the Rust data engine, and `openetl-proto`, its generated protocol types |
| `test/sim` | A seeded simulation of the commit protocol under crashes, timeouts and zombie jobs |
| `web/` | The UI: React and TypeScript, a client of the public API only. `web/src/gen` is generated from `proto/` and committed |
| `deploy/compose` | Local dependencies: Postgres, Temporal, an S3-compatible store |

## Development

You need Go 1.26, Rust (the toolchain is pinned in `rust-toolchain.toml`; rustup installs it), and Node 22 with pnpm for the UI. Developer tools are pinned in `tools/go.mod` and built into `bin/` by `make tools`; the UI's, protoc-gen-es among them, in `web/package.json`.

```sh
make test          # Go, Rust and UI unit tests
make sim           # commit-protocol simulation; SEEDS=10000 for more seeds
make lint          # buf lint, golangci-lint, rustfmt, clippy, tsc
make generate      # regenerate Go and TypeScript from proto/ after changing a .proto
make deps-up       # Postgres, Temporal, S3, Kafka and Kinesis in Docker
make connectors    # build the official connectors from ../openetl-connectors into bin/connectors
make e2e           # whole-stack scenarios; needs deps-up and the connectors
make dev           # openetld with every role, and the UI at http://localhost:8080
make web-dev       # the UI's dev server, proxying the API on localhost:8080
```

The UI signs in with an API key. `openetld` writes the owner's key to `.openetl/admin.key` when it creates the org, and `openetl key create` makes more. `openetld --web DIR` serves a built UI (`make web` builds `web/dist`) next to the API, on the same origin.

The official connectors, Postgres's included, live in [openetl-connectors](https://github.com/unsuitable001/openetl-connectors). Clone it next to this repo, or set `CONNECTORS_SRC`.

## License

[MIT](LICENSE)
