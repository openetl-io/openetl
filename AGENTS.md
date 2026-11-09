# Working on openetl

For people and coding agents. [README.md](README.md) has what OpenETL is, [CONTRIBUTING.md](CONTRIBUTING.md) the repository's layout and the make targets; this file has the rules that aren't obvious from the code. [web/AGENTS.md](web/AGENTS.md) adds the UI's, and [docs/AGENTS.md](docs/AGENTS.md) the docs'.

## Before a commit

- `make hooks` once: the pre-commit hook runs gofmt and golangci-lint on the Go packages a commit touches, rustfmt and clippy when Rust changes, `tsc -b` when `web/` does, and buf on protos.
- `make lint` and `make test` before a pull request; CI runs them too.
- Work on a branch from `main` and open a pull request; nothing is pushed to `main` directly.

## Code

- **Comments** only where they're needed: contracts, behaviour that isn't obvious, units, and what nil or zero means. Don't restate names, and don't point to documents outside this repository.
- **No `//nolint`.** Fix what the linter found. For gosec's integer conversions, bound the value first (`if n >= 0 && n <= math.MaxUint32`) or make the types match.
- **Generated code is committed and never edited by hand:** `gen/`, `sdk/go/protocol/`, `web/src/gen/`, `internal/api/openapi.yaml`, `internal/store/db/`, and the reference pages `docs/reference/openetld.md`, `connector-file.md`, `connector-descriptor.md`, `manifest.md` and `events.md`. Change the `.proto`, the query in `internal/store/queries`, the JSON Schema or `internal/platform/config`, then `make generate`.
- **Migrations** are goose files in `internal/store/migrations`. Add a new one; don't change the statements of one that's merged.
- `sdk/go` is a module of its own that external connectors import. Changing it changes the connector protocol's Go side, so keep it compatible or change `openetl-connectors` in step.

## Boundaries

- Workflows are written against `internal/orchestration`'s interface, never against DBOS or Temporal directly, and runners reach the control plane only through the API. golangci-lint's depguard rules enforce both.
- Outbound HTTP goes through `internal/netguard`, which forbidigo enforces.
- A destination's commit (staging, applying, markers, fencing, catalogs) is its connector's job, not the runner's or the engine's: the runner's coordinator only decides when to commit. A new sink or format is a connector speaking the protocol.
- The official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), which holds connector code and tests only. Tooling for connectors (packaging, validating `openetl-connector.yaml`, release names) is `openetl connector …` here, so a third-party connector needs nothing from that repository.
- **No connector changes a column's type by itself:** not on targets, staging or anything that becomes the user's table, topic schema or files. It refuses, holding the column where the stream holds, and gives the statement or step that makes the change, unless the pipeline's spec allows the change. New columns still land.
- `openetld` and the CLI don't call language models. A service that does is a client of the API and platform events.

## Docs

- `docs/` is the documentation the docs site is built from, at `main` and at each release tag, so what it says ships with the code it describes. [docs/AGENTS.md](docs/AGENTS.md) has who each section is for, the format and the style.
- **A change and its docs land together.** A pull request that changes what someone using, running or integrating OpenETL sees (a command, a flag, a setting, a field of a spec, an event, a metric, the API, the console, a default, an error people act on) updates `docs/` in the same pull request. So does one that changes the connector protocol or the SDK, in `docs/build-connectors`.
- **No page about one connector here.** An official connector's page is its package's `README.md` in openetl-connectors; this repository documents only the connectors built into `openetld`.
- **The README is the front door.** It stays short and welcoming: what OpenETL does, how to try it, and where the docs for each kind of reader start. Layout, make targets and anything else for contributors go in CONTRIBUTING.md.
- `go test ./test/docs` checks what a machine can: frontmatter, headings, raw HTML, links and anchors, and that each folder's `meta.json` lists its pages. `make test` runs it.

## Tests

- `make test` needs no Docker. `-tags integration` tests (`make test-integration`) need `make deps-up`; `-tags e2e` tests (`make e2e`) also need the connectors (`make connectors`) and build the engine. `make deps-down` when you're done.
- `test/e2e` runs one test at a time: no `t.Parallel()` outside the chaos suite, and no shards or matrix. Its heartbeats are short, so don't build or run anything heavy while it runs locally: a stall ends a lease and adds an attempt, which looks like a regression.
- `test/sim` is a seeded simulation of the commit protocol. `make sim SEEDS=n` runs more seeds, and `go test ./test/sim -run 'TestSimulation/kafka' -seed=42 -v` one, with its trace.
- A second `openetld` against the compose stack takes work from any other in the same Temporal namespace. On Temporal, give a scratch run its own namespace and database; on DBOS, the default, its own `--database-url` is enough.
- Recorded histories in `internal/orchestration/workflows/testdata` replay against the current workflows, so a change that would break running workflows on Temporal fails there. Such a change needs `Context.Version`.
