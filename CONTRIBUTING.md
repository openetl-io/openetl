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
| `deploy/` | Local dependencies in Docker Compose, the Compose file for installations, `openetld`'s image, and manifests for Kubernetes |
| `install.sh` | The CLI's installer, which the docs fetch from `main` and pipe to `sh`; `test/install` runs it against a release it serves |
| `docs/` | The documentation, which the docs site is built from |
| `test/` | `sim`, a seeded simulation of the commit protocol under crashes, timeouts and zombie attempts; `e2e`, whole-stack scenarios and the chaos suite; `bench` |
| `examples/` | Pipelines to try |

## Releasing

OpenETL and its official connectors are released separately, from their own repositories, and each official connector has a version of its own. OpenETL's versions are semantic. A release is a tag on a commit in `main`: `vX.Y.Z-alpha.N`, `vX.Y.Z-beta.N` or `vX.Y.Z-rc.N` for a prerelease, and `vX.Y.Z` for a stable release. A patch to a stable line, `vX.Y.Z` with `Z` above 0, is the one exception: it's tagged on the line's branch (see [Release branches](#release-branches)).

**Nothing names the version before the tag.** The tag is the only place a version lives: the binaries take it when they're built, and no committed file names the release that users install. `install.sh` installs the newest release, the first in the release list, and `OPENETL_VERSION` picks another. `deploy/compose/compose.yml` and `deploy/kubernetes` run `edge`, the image built from `main`, and the release attaches copies of them that run its own image. The docs write `{{version}}`, which the [docs site](https://github.com/openetl-io/docs-web) fills in ([docs/AGENTS.md](docs/AGENTS.md)). `go test ./test/docs` fails on a pinned release in the docs, the README, `install.sh`, `deploy/` or `examples/`.

**Release.** Push the tag:

```sh
git tag v0.1.0-alpha.1 && git push origin v0.1.0-alpha.1
```

The `release` workflow runs on it. First it checks three things: the tag's commit is in `main`, or in the line's branch `release-X.Y` for a patch; `nightly` ran every suite on that commit and passed; and the tag isn't released already. Then it builds the commit with the version in it: the bundle's archives for Linux on amd64 and arm64 (`openetld`, the CLI, the engine and the console), the CLI's own archives for Linux and macOS, the multi-arch image, the Spark and Flink jars and the Python wheel, and `compose.yml` and `kubernetes.yaml` with the release's image, with checksums, SBOMs and third-party notices. The image holds no connectors, only the default that points at their registry. It pushes the image to `ghcr.io/openetl-io/openetl:VERSION` and puts the files on a draft release, whose notes list the files and then the pull requests since the last release, or since the last stable release for a stable one. A job in the `release` environment signs the image and `checksums.txt` with keyless cosign, and publishes the release, as a prerelease when the tag has a suffix.

`nightly` runs each night on `main`'s head. To release a commit it hasn't run on, run every suite on the tag, and once that passes, run the release's failed jobs again:

```sh
gh workflow run nightly.yml --ref v0.1.0-alpha.1 -f suites=all
gh run rerun RUN_ID --failed
```

A release that failed later runs again the same way, and replaces the tag's draft. **A dry run,** `gh workflow run release.yml --ref REF` on any branch or tag, builds and checks everything for the ref's head, reports what would stop a release of it, and pushes and publishes nothing.

**A prerelease is the release candidate.** Test it as users run it, from its image and files. A stable release, `vX.Y.Z`, is a tag on the commit of a release candidate, `vX.Y.Z-rc.N`, that passed the gates: the chaos suite's 1,000 runs (`chaos.yml`, or `make chaos` on a machine of your own) and the tests on AWS. They gate only that step.

**The Go SDK** has versions of its own, which follow the connector protocol rather than OpenETL's releases. Tag it on a commit in `main` when it changes, `sdk/go/vX.Y.Z`, and connectors require it by that version:

```sh
git tag sdk/go/v0.2.0 && git push origin sdk/go/v0.2.0
```

**The connectors** are released from [openetl-connectors](https://github.com/openetl-io/openetl-connectors), each package with tags of its own, `PACKAGE/vX.Y.Z`. A change across both repositories lands here first, without breaking what the connectors use, and then the connectors move to the new SDK.

### Release branches

All work is on `main`, and every prerelease and stable release is a tag on it. Only the newest stable line gets fixes, as patch releases on its branch, `release-X.Y`. The branch is made the first time the line needs a fix, from the line's first release:

```sh
git push origin v0.2.0:refs/heads/release-0.2
```

The fix goes to `main` first. Then a pull request against `release-0.2` cherry-picks it (`git cherry-pick -x COMMIT`). Once that's merged, tag the patch, `v0.2.1`, on `release-0.2`.

### Backports

A fix goes to `main` first. When it also fixes a release line, label its pull request `backport release-X.Y`, before it's merged or after; make the label, once, when you make the branch. The `backport` workflow then opens a pull request of it against `release-X.Y`. Where it doesn't apply cleanly, the workflow comments with the commands to backport it by hand.

Backport a change to `docs/` that corrects or completes what a release does: a wrong command, output or default, a missing step, a limit the release has. Don't backport docs for what only `main` does, or a change that describes code the release doesn't have. A fix to text that `main` no longer has is a pull request to the release branch alone.

The workflow pushes a backport with the deploy key in the secret `BACKPORT_DEPLOY_KEY`, which has write access, so CI runs on it: GitHub starts no workflow for what a workflow's own token pushes. It opens the pull request with its own token, which **Allow GitHub Actions to create and approve pull requests**, in the repository's Actions settings, permits. Without the key, CI runs on a backport once it's merged; without the setting, the workflow comments with a link to open the pull request.

The docs site checks this repository every 15 minutes, and builds when what it reads has changed: `docs/`, `internal/api/openapi.yaml` or a JSON Schema on `main` or a release branch, or a published release.

## Pull requests

Work on a branch and open a pull request against `main`. Before you do, run `make lint` and `make test`; CI runs them too, along with the integration and end-to-end tests when a change needs them. A change to what someone using, running or integrating OpenETL sees updates the docs in the same pull request.
