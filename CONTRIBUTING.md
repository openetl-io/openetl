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

`make dev` keeps its store in the folder `.openetl/checkpoints`, which compose's Spark and Flink containers can't read. To run Spark steps and Flink transforms on them, use `make dev STORE=s3`: the store goes in the bucket `openetl-dev` on compose's S3, which both can read, Spark gets Hadoop's S3A jars (a one-time download of about 280 MB, into `.openetl/spark/s3a`), and the runner gets `deploy/compose/spark-submit` and a bridge for Flink. Your clusters are `spark://spark-master:7077`, with the UI at `http://127.0.0.1:8090`, and `http://localhost:8091`. A pipeline's checkpoints stay in the store it ran on. To carry them over when you change store, stop `openetld` and copy the folder with `aws s3 sync`, leaving out `*.lock`, `*.attrs` and `.tmp-*`, which only the folder store has.

`openetld dev` runs the server, an orchestrator and a runner in one process, and asks no one to sign in while it listens only on a loopback address. `openetld server` writes the owner's API key to `.openetl/admin.key` when it creates the org, and `openetl principal password-link` gives a user a link to set a password for the UI.

## Run it from source

The [quickstart](docs/get-started/quickstart.md) runs a release. To run this checkout instead, with the connectors built from `../openetl-connectors`:

```sh
make dev DEPS=postgres
```

When the log shows `API serving`, the console is at `http://localhost:8080`, with sign-in off. In a second terminal in this directory, build the CLI and run the quickstart's pipeline. The CLI finds the key that `make dev` wrote in `.openetl/admin.key`:

```sh
go build -o bin/ ./cmd/openetl && export PATH="$PWD/bin:$PATH"
echo postgres://openetl:openetl@localhost:5432/openetl | openetl secret set local-postgres-dsn
openetl apply -f examples/faker-to-postgres.yaml
```

The connectors `make dev` builds are local executables in `bin/connectors`, which a runner uses for a connector name that has no package applied. `docker compose -f deploy/compose/dev.yml exec postgres psql -U openetl` opens the database. `make deps-down` stops Postgres, and keeps its volume.

## Build the image

`make engine-linux image` builds `openetld`'s image from this checkout as `openetl:dev`: the Linux engine in a Rust container, then `openetld`, the CLI, the console and the official connectors built from `../openetl-connectors`. Unlike a release's image, it holds those connectors. Its build caches use about 1 GB in the Docker volumes `openetl-engine-target` and `openetl-cargo-registry`. On Linux, `cargo build -p openetl-engine` can replace `make engine-linux`.

- **Compose:** set `OPENETL_IMAGE=openetl:dev` in `.env` beside `deploy/compose/compose.yml`. To use the image on another machine, `docker save openetl:dev | ssh HOST docker load`.
- **Kubernetes:** `make engine-linux image IMAGE=REGISTRY/openetl:TAG`, push it, and set it in your kustomization's `images`.

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
| `install.sh` | The CLI's installer, which the docs fetch from `main` and pipe to `sh`; `test/install` runs it against a release it serves |
| `docs/` | The documentation, which the docs site is built from |
| `test/` | `sim`, a seeded simulation of the commit protocol under crashes, timeouts and zombie attempts; `e2e`, whole-stack scenarios and the chaos suite; `bench` |
| `examples/` | Pipelines to try |

## Releasing

OpenETL and its official connectors are released separately, from their own repositories, and in the alpha they share a version. A release is a tag, `vVERSION` such as `v0.1.0-alpha`, on a commit in `main`, which the `release` workflow then builds and publishes. A candidate tests a commit before it's tagged.

**Set the version** in a pull request before the tag. The docs, `install.sh`'s default, `deploy/compose/compose.yml`'s image and `deploy/kubernetes/kustomization.yaml`'s `newTag` name the release that users install. `go test ./test/docs -run TestReleaseVersion` fails until they all name the same one:

```sh
grep -rln 0.1.0-alpha README.md docs install.sh deploy
```

**Release.** Push the tag, then run the workflow with it:

```sh
git tag v0.1.0-alpha && git push origin v0.1.0-alpha
gh workflow run release.yml -f tag=v0.1.0-alpha -f publish=public
```

It builds the tag's commit with the version in it: the bundle's archives for Linux on amd64 and arm64 (`openetld`, the CLI, the engine and the console), the CLI's own archives for Linux and macOS, the multi-arch image, and the Spark and Flink jars and the Python wheel, with checksums, SBOMs and third-party notices. The image holds no connectors, only the default that points at their registry. `publish` says what happens to them:

- `public` pushes the image to `ghcr.io/openetl-io/openetl:VERSION` and publishes a release of the tag with the files. A job in the `release` environment signs the image and `checksums.txt` with keyless cosign first, and tags the Go SDK `sdk/go/vVERSION` on the same commit. GHCR creates a package private: after the first public release, make `ghcr.io/openetl-io/openetl` public in its settings, once. Then run the [quickstart](docs/get-started/quickstart.md) as written, which installs the release from GitHub and GHCR.
- `private` puts the files on a draft release of the tag and the image in the private `ghcr.io/openetl-io/candidates/VERSION/` namespace. Nothing is signed, as keyless signatures go to a public log. `gh workflow run release-discard.yml -f version=VERSION` removes both.
- `skip` builds and checks everything, and pushes nothing.

Running it again replaces the tag's draft. It refuses a tag that's already released.

**The connectors** are tagged and released the same way from [openetl-connectors](https://github.com/openetl-io/openetl-connectors), with its own `release` workflow, which builds them with this repository's CLI and SDK at the same tag.

**Build a candidate** of a commit that isn't tagged yet. `gh workflow run release-candidate.yml -f version=0.1.0-alpha -f commit=SHA` builds the same files, puts them on a draft GitHub release, which makes no tag until it's published, and puts the image in `ghcr.io/openetl-io/candidates/VERSION/`. `candidate.json` on the draft records the commit and digest. A version has one candidate at a time: `-f replace=true` discards the last one first.

**Test those files, as a user runs them.** On a machine with the compose stack, signed in with `gh` to an account that can see the drafts:

```sh
deploy/release/candidate.sh fetch VERSION bin/candidate
deploy/release/candidate.sh unpack bin/candidate
make chaos CANDIDATE=bin/candidate CONNECTOR_VERSION=VERSION CONNECTOR_REGISTRY_USER=USER CONNECTOR_REGISTRY_TOKEN=TOKEN
```

The suites run the candidate's binaries, connect the registry as the official source, apply each connector from it and let runners download them. `gh workflow run e2e.yml -f candidate=VERSION`, and `chaos.yml` the same way, run on the candidate's binaries at the commit it was built from, with connectors built from source, as a workflow's token can't read another repository's private packages.

**Discard a candidate** with `gh workflow run release-discard.yml -f version=VERSION`, which deletes its packages and its draft and refuses a version that's been published.

## Pull requests

Work on a branch and open a pull request against `main`. Before you do, run `make lint` and `make test`; CI runs them too, along with the integration and end-to-end tests when a change needs them. A change to what someone using, running or integrating OpenETL sees updates the docs in the same pull request.
