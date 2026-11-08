---
title: Reference
description: Where to look up every resource, command, setting, API, event and schema, and the source in the code each one comes from.
---

This section is for looking things up. Each entry below says where its full reference is at this commit: a page here, a program's own help, or a file in the repository. Where a file is named, it's the source of truth, and the help and the docs follow it.

## Resource documents

Everything you apply with `openetl apply` is a YAML or JSON document with an `apiVersion`, a `kind`, `metadata` and a `spec`:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: faker
spec:
  connector: faker
  config:
    seed: 42
```

- A file can hold several documents, separated by `---`.
- `metadata.name` is lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit.
- `metadata.workspace` is optional. A document without one goes to the workspace `openetl apply --workspace` names, `default` unless it says.
- Fields a kind doesn't have are refused.
- In `spec`, enums are lowercase (`merge`), durations are Go durations (`15m`), and sizes have units (`256MB`).

| Kind | apiVersion | Belongs to | What it defines |
|---|---|---|---|
| `Source` | `openetl.io/v1alpha1` | A workspace | A system data comes from: its connector, and that connector's config |
| `Destination` | `openetl.io/v1alpha1` | A workspace | A system data goes to: its connector, and that connector's config |
| `Pipeline` | `openetl.io/v1alpha1` | A workspace | What to copy from a source to a destination, when, and how |
| `DbtProject` | `openetl.io/v1alpha1` | A workspace | A dbt project's files, for steps that run after a sync |
| `Webhook` | `openetl.io/v1alpha1` | A workspace | Where a workspace's platform events go |
| `SparkCluster` | `openetl.io/v1alpha1` | A workspace | Where Spark applications run |
| `FlinkCluster` | `openetl.io/v1alpha1` | A workspace | Where Flink jobs run |
| `Connector` | `openetl.io/v1alpha2` | The org | A version of an external connector package: its descriptor |
| `DeclarativeSource` | `openetl.io/v1alpha1` | The org | A version of a declarative source: its manifest |
| `ConnectorSource` | `openetl.io/v1alpha1` | The org | An OCI registry namespace or a GitHub repository whose connector packages the catalog lists |

`Connector` and `DeclarativeSource` documents also need `metadata.version`, a semantic version such as `1.2.0`, and their names may have an owner, as in `acme/billing`. A `Connector` descriptor at `openetl.io/v1alpha1` is still read, as a package with one role.

The fields of each `spec` are defined by the messages in [`proto/openetl/api/v1`](../../proto/openetl/api/v1), whose comments describe them, and read from YAML by [`internal/spec`](../../internal/spec). The [examples](../../examples) are working documents of each common kind. A `Connector` descriptor's and a `DeclarativeSource` manifest's fields are in their JSON Schemas, under [JSON Schemas](#json-schemas).

## The openetl CLI

`openetl` is the command-line client of the API. Run `openetl` for its commands, and `openetl COMMAND -h` for a command's flags.

| Command | What it does |
|---|---|
| `apply -f FILE` | Creates or updates the resources in `FILE`, of any kind in the table above |
| `plan -f FILE` | Shows what `apply` would change, and how each pipeline would sync |
| `pause NAME` | Stops a pipeline's scheduled syncs |
| `resume NAME` | Starts a pipeline's scheduled syncs again |
| `sync NAME` | Starts a sync of a pipeline now |
| `rescale --size SIZE NAME` | Sizes a pipeline's jobs until its next version, which restarts a running job at a checkpoint; `auto` gives the spec back its say |
| `syncs NAME` | Lists a pipeline's syncs, newest first |
| `versions NAME` | Lists a pipeline's versions, newest first |
| `logs NAME` | Prints a sync's log; `-f` follows a running one |
| `check source NAME`, `check destination NAME` | Checks that a source or destination is reachable |
| `discover NAME` | Lists a source's streams and columns |
| `connector …` | Packages, publishes, validates, applies, runs and tests connectors; see below |
| `secret set`, `list`, `delete` | Stores the secrets that configs name |
| `principal create`, `list`, `delete`, `disable`, `enable`, `grant`, `revoke`, `password-link`, `sign-out` | Manages users and service accounts, their roles, and users' passwords and sessions |
| `whoami` | Shows who the API key is, and what it may do |
| `key create`, `list`, `delete` | Manages principals' API keys |
| `dataplane list`, `token` | Lists data planes, and manages their runner tokens |
| `audit list` | Lists who changed what, and who read secrets |
| `webhook list`, `deliveries`, `test`, `disable`, `enable`, `delete` | Manages the webhooks platform events go to |
| `incident list`, `get`, `resolve` | Shows what's wrong with pipelines, and what OpenETL did about it |
| `holds NAME` | Lists the columns and cells a pipeline holds back |
| `holds release NAME STREAM COLUMN`, `holds discard NAME STREAM COLUMN` | Patches a hold's values in, or drops them; `release --new-version` patches them into `COLUMN__v2` |

`openetl connector` has commands of its own:

| Command | What it does |
|---|---|
| `package` | Builds each directory's `openetl-connector.yaml` for every platform, and writes its executables and descriptor, for a release |
| `publish oci://REGISTRY/NAMESPACE` | Pushes what `package` wrote to an OCI registry, and lists it in the namespace's index |
| `validate` | Checks each directory's `openetl-connector.yaml` without building, or with `--artifact`, a package version in a registry |
| `apply` | Applies connector versions: descriptors and manifests in a file, or `oci://` and `github.com/` references |
| `catalog` | Lists the packages of the connected connector sources, with the reference to apply each by |
| `spec TARGET` | Prints a connector's spec |
| `check --config FILE TARGET` | Checks that a connector reaches its upstream with a config |
| `discover --config FILE TARGET` | Lists a source's streams and columns |
| `read --config FILE TARGET` | Reads a source's records into a table, JSON lines or Parquet |
| `test --config FILE TARGET` | Runs the conformance kit's checks against a source or destination |
| `schema file`, `descriptor`, `manifest` | Prints the JSON Schema of `openetl-connector.yaml`, a descriptor or a manifest |

`TARGET` is an executable, a descriptor, a manifest, or a package version by reference: `oci://REGISTRY/REPOSITORY:VERSION` or `github.com/OWNER/REPO[/PACKAGE]@VERSION`.

Every command that calls the API sends the API key in `OPENETL_API_KEY`, or else the one in the file `--api-key-file` names (`OPENETL_API_KEY_FILE`, default `.openetl/admin.key`), to the API at `--api-url` (`OPENETL_API_URL`, default `http://localhost:8080`).

The commands and their help are in [`cmd/openetl`](../../cmd/openetl).

## openetld

`openetld` runs OpenETL's server side, in one of these roles:

| Role | What it runs |
|---|---|
| `server` | The API, and the console when `--web` names its files |
| `orchestrator` | Workflows, and the placement of jobs on runners |
| `runner` | A data plane's attempts, which it takes from the API |
| `dev` | The server, an orchestrator and a runner in one process; logs are text and sign-in is off unless flags say otherwise |

`openetld bootstrap-key` gives the admin service account a new owner key, in `--bootstrap-key-file`, for when every owner key is lost.

Every setting is a flag and an environment variable: `--database-url` is also `OPENETL_DATABASE_URL`. A flag overrides the variable, and the variable overrides the default. Every role takes every setting, and uses the ones that concern it. `openetld ROLE -h` lists them all with their variables and defaults. A runner's token can also come from `OPENETL_RUNNER_TOKEN`, which has no flag.

The settings are defined in [`internal/platform/config/config.go`](../../internal/platform/config/config.go). Traces and metrics are set with the standard `OTEL_*` variables instead: a signal is exported only once its exporter (`OTEL_TRACES_EXPORTER`, `OTEL_METRICS_EXPORTER`) or an OTLP endpoint is set, and `OTEL_METRICS_EXPORTER=prometheus` serves a Prometheus endpoint.

## The API

`openetld server` serves the API on its listen address over Connect, gRPC, gRPC-Web and REST. Calls carry an API key as `Authorization: Bearer KEY`; the console signs in with a session instead.

| Path | What it serves |
|---|---|
| `GET /openapi.yaml` | The REST API's OpenAPI 3 description |
| `/v1/…` | The REST API |
| `GET /healthz` | `ok` while the server is up |

The services, their methods and their messages are defined in [`proto/openetl/api/v1`](../../proto/openetl/api/v1). The OpenAPI description is generated from them into [`internal/api/openapi.yaml`](../../internal/api/openapi.yaml), and the server serves that file.

## Roles

A principal's roles are bound on the org, which covers every workspace, or on one workspace. Each role may do what the roles above it may, and more:

| Role | Bound on | Adds |
|---|---|---|
| `viewer` | The org or a workspace | Reads every resource but the audit log, syncs' logs and incidents included. No role reads secrets' values. |
| `operator` | The org or a workspace | Pauses, resumes, syncs and rescales pipelines; checks and discovers sources and destinations; disables, enables and tests webhooks; resolves incidents; refreshes the connector catalog |
| `editor` | The org or a workspace | Applies sources, destinations, pipelines, dbt projects, webhooks, and Spark and Flink clusters, and deletes them; releases and discards holds; sets and deletes secrets |
| `admin` | The org only | Applies connector versions and connector sources; manages principals, their keys and their roles below owner; manages runner tokens; reads the audit log; signs users out and resets their passwords |
| `owner` | The org only | Grants and revokes owner, and manages owners and their keys |

Each API method's role is in [`internal/api/authz.go`](../../internal/api/authz.go).

## JSON Schemas

`openetl connector schema` prints three JSON Schemas, for editors and checks:

| Command | Schema of | File |
|---|---|---|
| `openetl connector schema file` | A package's `openetl-connector.yaml` | [`internal/connectorpkg/openetl-connector.schema.json`](../../internal/connectorpkg/openetl-connector.schema.json) |
| `openetl connector schema descriptor` | A `Connector` descriptor | [`internal/spec/connector.schema.json`](../../internal/spec/connector.schema.json) |
| `openetl connector schema manifest` | A `DeclarativeSource` manifest | [`internal/declarative/manifest.schema.json`](../../internal/declarative/manifest.schema.json) |

## Platform events

OpenETL records these events, and [webhooks](../../examples/webhook.yaml) deliver them:

| Type | |
|---|---|
| `sync.started`, `sync.succeeded`, `sync.failed`, `sync.stopped` | A sync's progress |
| `pipeline.created`, `pipeline.updated`, `pipeline.deleted` | A pipeline's changes |
| `pipeline.paused`, `pipeline.resumed`, `pipeline.rescaled` | A pipeline paused, resumed or rescaled |
| `pipeline.lagging`, `pipeline.recovered` | A continuous pipeline fell behind its source, or caught up |
| `schema.changed`, `schema.blocked` | A stream's schema changed, or a change was stopped |
| `incident.opened`, `incident.escalated`, `incident.resolved` | An incident's progress |
| `webhook.test` | Sent to one webhook when it's tested, such as by `openetl webhook test` |

Each type's `data` is defined by the Go types in [`internal/events/events.go`](../../internal/events/events.go). No JSON Schema is published for them at this commit.

## The connector protocol

External connectors serve the connector protocol, defined in [`proto/openetl/connector/v1`](../../proto/openetl/connector/v1): what a source and a destination serve, the services the host offers them, and the errors they return. The Go SDK in [`sdk/go`](../../sdk/go) implements it. [Build a connector](../build-connectors/index.md) explains it.

## Built-in connectors

[Built-in connectors](./built-in-connectors.md) has every config field of `faker`, `stdout` and `parquet`. The official connectors' references are in [openetl-connectors](https://github.com/openetl-io/openetl-connectors).
