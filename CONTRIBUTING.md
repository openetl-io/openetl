# Contributing to OpenETL

Thanks for helping. Bug reports, fixes, docs and connectors are all welcome. For a large change, open an issue first, so we can agree on the approach before you spend time on it.

[AGENTS.md](AGENTS.md) has the repository's rules, for people and coding agents alike, and [docs/AGENTS.md](docs/AGENTS.md) the docs'. The official connectors are in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), which has a contributing guide of its own.

## Set up

You need Go 1.26, Rust (the toolchain is pinned in `rust-toolchain.toml`, and rustup installs it), Node 22 with pnpm for the UI, and Docker. Developer tools are pinned in `tools/go.mod` and built into `bin/` by `make tools`; the UI's are in `web/package.json`.

Clone [openetl-connectors](https://github.com/openetl-io/openetl-connectors) next to this repository, or set `CONNECTORS_SRC` to where it is: `make connectors`, `make dev` and the end-to-end tests build the connectors from it.

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
make deps-down         # stop what deps-up started
```

`make help` lists the rest. `make deps-up` and `make dev` start every dependency unless `DEPS` names some, such as `DEPS="postgres s3"`.

`openetld dev` runs the server, an orchestrator and a runner in one process, and asks no one to sign in while it listens only on a loopback address. `openetld server` writes the owner's API key to `.openetl/admin.key` when it creates the org, and `openetl principal password-link` gives a user a link to set a password for the UI.

## Repository layout

| Path | What it holds |
|---|---|
| `cmd/openetld`, `cmd/openetl` | The daemon (roles `server`, `orchestrator` and `runner`, or `dev` for all three) and the CLI |
| `proto/openetl` | The protocols: `connector/v1` (what connectors speak, including host services), `engine/v1` (runner to engine), `dataplane/v1` (runners to the API) and `api/v1` (the API) |
| `gen/`, `sdk/go/protocol/v1`, `web/src/gen`, `internal/api/openapi.yaml` | Generated from `proto/` by `make generate`, and committed |
| `docs/reference/openetld.md`, `connector-file.md`, `connector-descriptor.md`, `manifest.md`, `events.md` | Generated from `internal/platform/config` and the four JSON Schemas by `make generate` (`go run ./internal/refdocs`), and committed |
| `internal/api` | The API server |
| `internal/orchestration` | Workflows, written against one interface with a DBOS and a Temporal backend, and placement and dispatch to runners |
| `internal/runner` | Runners: attempts, the checkpoint coordinator, and pod launchers |
| `internal/checkpoint` | The checkpoint log |
| `internal/delivery` | Each stream's delivery guarantee, worked out from the destination, the source's replay behavior, the write mode, the primary key and the level the pipeline requires |
| `internal/store` | The metadata store on Postgres: migrations, and queries, from which sqlc generates `internal/store/db` |
| `internal/connectorhost`, `connectors/native` | Connectors the runner hosts: the ones built in (`faker`, `stdout` and `parquet`), declarative sources, and external connectors |
| `sdk/go` | The Go connector SDK, a module of its own, with `sdk.Serve` for external connectors and a conformance suite |
| `crates/` | `openetl-engine`, the Rust data engine, and its libraries |
| `libs/` | `openetl-spark` and `openetl-flink`: what runs on your Spark and Flink clusters |
| `web/` | The UI: React and TypeScript, a client of the API only |
| `deploy/` | Local dependencies in Docker Compose, `openetld`'s image, manifests for Kubernetes, and the release candidate script |
| `docs/` | The documentation, which the docs site is built from |
| `test/` | `sim`, a seeded simulation of the commit protocol under crashes, timeouts and zombie attempts; `e2e`, whole-stack scenarios and the chaos suite; `bench` |
| `examples/` | Pipelines to try |

## Releasing

A release is built once, tested as built, and then promoted, so what's tested is what ships.

**Build a candidate.** `gh workflow run release-candidate.yml -f version=0.1.0-alpha -f commit=SHA -f connectors=SHA` builds that commit with the version in it: archives for Linux on amd64 and arm64, the multi-arch image, the connectors, and the Spark and Flink libraries, with checksums, SBOMs and third-party notices. The files go on a draft GitHub release, which makes no tag until it's published, and the image and connector packages go to the private `ghcr.io/openetl-io/candidates/VERSION/` namespace. `candidate.json` on the draft records the commits and digests. A version has one candidate at a time: `-f replace=true` discards the last one first.

**Test those files.** `gh workflow run e2e.yml -f candidate=VERSION`, and `chaos.yml` the same way, run the suites on the candidate's binaries, at the commit it was built from. On another machine:

```sh
deploy/release/candidate.sh fetch VERSION bin/candidate
deploy/release/candidate.sh unpack bin/candidate
make chaos CANDIDATE=bin/candidate
```

**Discard a candidate** with `gh workflow run release-discard.yml -f version=VERSION`, which deletes its packages and its draft and refuses a version that's been published.

## Pull requests

Work on a branch and open a pull request against `main`. Before you do, run `make lint` and `make test`; CI runs them too, along with the integration and end-to-end tests when a change needs them. A change to what someone using, running or integrating OpenETL sees updates the docs in the same pull request.
