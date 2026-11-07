# OpenETL

OpenETL moves data from where it is produced to where it is used: databases, SaaS APIs, event streams and webhooks into warehouses, lakes, streams and HTTP endpoints. The same pipeline runs as a scheduled batch or continuously as a stream, with exactly-once delivery wherever the destination allows it.

OpenETL is licensed under Apache 2.0 and built to be self-hosted.

**Status: pre-alpha.** Nothing has been released, and the API, the pipeline spec and the protocols still change without notice. The first release, 0.1 alpha, will run scheduled and continuous pipelines:

- from Postgres (full refresh, incremental by a cursor, and CDC), Kafka and Kinesis,
- into Postgres, Iceberg (REST and Glue catalogs), Parquet on S3, Kafka and Kinesis,
- with transforms on your own Spark and Flink clusters, dbt after a sync, and metrics and traces over OpenTelemetry.

## How it works

- **The control plane** is `openetld`: the API, which the `openetl` CLI and the UI use, and orchestrators, which schedule syncs and place each attempt on a runner. Workflows run on DBOS, which needs nothing but Postgres, or on Temporal.
- **Runners** make up a data plane. They take their work from the API and run each attempt: connectors as subprocesses speaking the connector protocol, with `openetl-engine`, the Rust data engine, between source and destination. On Kubernetes, an attempt can run in a pod of its own, sized for its job.
- **Checkpoints** are a log in object storage, one record per checkpoint, updated by compare-and-swap, which also fences out attempts that were replaced. The destination connector commits each epoch behind that fence, so an attempt that crashes resumes from the last checkpoint, and a destination that allows it never applies an epoch twice.

## Repository layout

| Path | What it holds |
|---|---|
| `cmd/openetld`, `cmd/openetl` | The daemon (roles `server`, `orchestrator` and `runner`, or `dev` for all three) and the CLI |
| `proto/openetl` | The protocols: `connector/v1` (what connectors speak, including host services), `engine/v1` (runner to engine), `dataplane/v1` (runners to the API) and `api/v1` (the API) |
| `gen/`, `sdk/go/protocol/v1`, `web/src/gen`, `internal/api/openapi.yaml` | Generated from `proto/` by `make generate`, and committed |
| `internal/api` | The API server |
| `internal/orchestration` | Workflows, written against one interface with a DBOS and a Temporal backend, and placement and dispatch to runners |
| `internal/runner` | Runners: attempts, the checkpoint coordinator, and pod launchers |
| `internal/checkpoint` | The checkpoint log |
| `internal/delivery` | Each stream's delivery guarantee, worked out from the destination, the source's replay behaviour, the write mode, the primary key and the level the pipeline requires |
| `internal/store` | The metadata store on Postgres: migrations, and queries, from which sqlc generates `internal/store/db` |
| `internal/connectorhost`, `connectors/native` | Connectors the runner hosts: the two built in (`faker` and `stdout`), declarative sources, and external connectors |
| `sdk/go` | The Go connector SDK, a module of its own, with `sdk.Serve` for external connectors and a conformance suite |
| `crates/` | `openetl-engine`, the Rust data engine, and its libraries |
| `libs/` | `openetl-spark` and `openetl-flink`: what runs on your Spark and Flink clusters |
| `web/` | The UI: React and TypeScript, a client of the API only |
| `deploy/` | Local dependencies in Docker Compose, `openetld`'s image, and manifests for Kubernetes |
| `test/` | `sim`, a seeded simulation of the commit protocol under crashes, timeouts and zombie attempts; `e2e`, whole-stack scenarios and the chaos suite; `bench` |
| `examples/` | Pipelines to try |

## Development

You need Go 1.26, Rust (the toolchain is pinned in `rust-toolchain.toml`, and rustup installs it), Node 22 with pnpm for the UI, and Docker. Developer tools are pinned in `tools/go.mod` and built into `bin/` by `make tools`; the UI's are in `web/package.json`.

```sh
make hooks             # check formatting and lint before each commit
make test              # Go, Rust and UI unit tests
make lint              # buf, golangci-lint, rustfmt, clippy, cargo-deny and tsc
make generate          # regenerate code after changing a .proto or a query
make sim               # the commit-protocol simulation; SEEDS=10000 for more seeds
make deps-up           # Postgres, S3, Kafka, Kinesis, Spark, Flink and more in Docker; TEMPORAL=1 adds Temporal
make test-integration  # integration tests against those
make connectors        # build the official connectors from ../openetl-connectors into bin/connectors
make e2e               # whole-stack scenarios; needs deps-up and the connectors
make dev               # openetld dev, with the UI at http://localhost:8080
make web-dev           # the UI's dev server, against the API on localhost:8080
```

`make help` lists the rest.

`openetld dev` runs the server, an orchestrator and a runner in one process, and asks no one to sign in while it listens only on a loopback address. `openetld server` writes the owner's API key to `.openetl/admin.key` when it creates the org, and `openetl principal password-link` gives a user a link to set a password for the UI.

To try a pipeline, run `make deps-up` and `make dev`, then follow the comment at the top of [`examples/faker-to-postgres.yaml`](examples/faker-to-postgres.yaml).

The official connectors, Postgres's included, live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Clone it next to this repository, or set `CONNECTORS_SRC`.

## License

[Apache 2.0](LICENSE)
