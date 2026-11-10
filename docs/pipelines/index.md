---
title: Pipelines as code
description: How pipelines are written as YAML, planned and applied with the CLI, versioned, and run day to day.
---

A pipeline is a YAML document that you keep in git like any other code. You write it, run `openetl plan` to see what applying it would change and how each stream would be delivered, and run `openetl apply` to make the change. This page covers that loop, the documents involved, and the pipeline spec's fields at a glance.

If you haven't yet, run the [quickstart](../get-started/quickstart.md) first: this page builds on its `faker` source and `local-postgres` destination. [Concepts](../get-started/concepts.md) explains the words used here.

[Your first pipeline](./first-pipeline.md) then copies tables from your own Postgres database into another, step by step.

## The documents

Everything you apply is a document with the same four parts:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-postgres
spec:
  connector: postgres
  config:
    dsn: { secret: local-postgres-dsn }
```

- `apiVersion` is `openetl.io/v1alpha1`, except for a `Connector`'s, which is `openetl.io/v1alpha2`.
- `kind` says what the document defines.
- `metadata.name` is lowercase letters, digits and hyphens, at most 63 characters. `metadata.workspace` is optional: an installation has one workspace, `default`.
- `spec` is the definition itself.

A file can hold several documents, separated by `---`, and `-f` can be given more than once. Documents refer to each other by name, so a pipeline's `source.ref` is the name of a `Source`.

| Kind | What it defines | Covered in |
|---|---|---|
| `Source` | A system to read from: a connector and its `config` | This page |
| `Destination` | A system to write to: a connector and its `config` | This page |
| `Pipeline` | What to copy from a source to a destination, and how | This page |
| `DbtProject` | A dbt project's files, to run after a pipeline's syncs | [Integrate](../integrate/index.md) |
| `SparkCluster`, `FlinkCluster` | Your own Spark or Flink cluster, for transforms and steps | [Integrate](../integrate/index.md) |
| `Webhook` | Where platform events, such as a failed sync, are sent | [Integrate](../integrate/index.md) |
| `Connector`, `DeclarativeSource` | A version of a connector, or of a source written in YAML | [Build a connector](../build-connectors/index.md) |
| `ConnectorSource` | A registry or repository whose connector packages the console lists | [Build a connector](../build-connectors/index.md) |

A `Source` or `Destination` names a connector, such as `postgres`, and holds that connector's `config`. What the config takes is up to the connector: its own documentation lists it, and `openetl connector spec TARGET` prints any connector's spec, which includes its config's JSON Schema. A connector name without a version, such as `postgres`, may pin a newer version each time you apply it, and the plan warns about that; `postgres@1` keeps to major version 1.

[Choose and manage connectors](./connectors.md) covers installing official, community and private packages, selecting versions, and upgrading them independently of OpenETL.

## Secrets

A config never holds a password. It names a secret instead, as `{ secret: NAME }`:

```yaml
config:
  dsn: { secret: local-postgres-dsn }
```

Store secrets with the CLI. `set` reads the value from stdin or from `--from-file`, or with `--ref` points the secret at a cloud secret store, which runners read with their own cloud identity:

```sh
echo postgres://openetl:openetl@localhost:5432/openetl | openetl secret set local-postgres-dsn
openetl secret list
openetl secret delete local-postgres-dsn
```

`list` shows current resources that use each secret. `delete` refuses a secret used by a current source, destination, webhook, Spark cluster, Flink cluster or pipeline. Check private connector sources separately. [Manage connection secrets](../self-host/secrets.md) covers cloud references, credential replacement and deletion.

## Plan, then apply

`openetl plan -f FILE` sends the documents to the API, which checks them and works out what applying them would do, without changing anything. Here's a second pipeline from the quickstart's `faker` source, into Parquet files on local disk with the built-in `parquet` connector, at 06:00 every day:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-parquet
spec:
  connector: parquet
  config:
    url: file:///tmp/openetl-parquet
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-parquet
spec:
  schedule: { cron: "0 6 * * *" }
  source: { ref: faker }
  destination: { ref: local-parquet }
```

```sh
openetl plan -f faker-to-parquet.yaml
```

```text
would be created workspaces/default/destinations/local-parquet (version d10c3e1925e9)
would be created workspaces/default/pipelines/faker-to-parquet (version known after apply)

workspaces/default/pipelines/faker-to-parquet (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    users      append  exactly_once  exact replay
    purchases  append  exactly_once  exact replay
```

The plan has two parts:

- **A line per document**: whether it would be created, updated or unchanged, and its version. A pipeline the file doesn't define, but that uses a source or destination the file changes, gets a line too, ending "which uses what changed": it gets a new version as well.
- **A section per pipeline**, which can have:
  - `changes`: each field that changes, as `field: before -> after`.
  - `job size`: how much CPU and memory its jobs get, what set that, and the pool of runners it runs in.
  - `streams`: each stream's write mode, its [delivery guarantee](../get-started/concepts.md#delivery-guarantees), and the reason for that guarantee.
  - `problems`: what stops the pipeline from running. `plan` exits with an error, and `apply` refuses the whole file.
  - `warnings`: what's allowed but worth a second look.
  - `notes`: what the plan couldn't check, and why.

The streams section needs two things the control plane may not have. It reads the source's streams from the source's latest catalog, which `openetl discover NAME` refreshes; until a source has been discovered, the plan checks only the delivery settings. And it needs to know what the destination can do, which it learns from the connector's package; for a connector it has no package for, such as the quickstart's local build of Postgres, a note says the runner checks the guarantees before each sync instead.

A plan with problems looks like this one, for the same pipeline made continuous:

```text
workspaces/default/pipelines/faker-to-parquet (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    users      append  exactly_once  exact replay
    purchases  append  exactly_once  exact replay
  problems:
    stream "users" is read with full_refresh, whose reads end, and a continuous pipeline needs streams read with cdc or stream
    stream "purchases" is read with full_refresh, whose reads end, and a continuous pipeline needs streams read with cdc or stream
openetl: the plan has problems, so apply would refuse it
```

When the plan is what you want, apply the same files:

```sh
openetl apply -f faker-to-parquet.yaml
```

Either every document in an apply is applied or none is. Apply prints the same lines as the plan, with "created", "updated" or "unchanged", followed by any warnings. A new scheduled pipeline starts its first sync straight away; a changed one waits for its schedule or for `openetl sync`. `openetl apply --dry-run` checks the documents the way `apply` does without changing anything, and without printing the plan.

## Versions

Every document you apply is stored as an immutable version, named by a hash of its content. A pipeline's version pins the versions of its source and destination, and of its dbt projects, so changing a source gives every pipeline that uses it a new version. Each sync runs exactly one version.

```sh
openetl versions faker-to-postgres
```

```text
VERSION                                                           CURRENT  CREATED              PROVENANCE
edab40da6d76c1aba235c7cd81c5618db8af2206c759539c5baea80a584560b3           2026-11-08 12:30:15  cli:examples/faker-to-postgres.yaml#3
9e052e7e6881672dbdcfcc4356f04f592bdfbd9777622e2d2a877eb1530db134  *        2026-11-08 12:28:23  cli:examples/faker-to-postgres.yaml#3
```

The provenance says where a version came from: here, the third document of a file applied with the CLI. Because versions are named by their content, applying a pipeline as it was before makes that earlier version current again; reverting the commit in git and applying is a rollback.

## The pipeline spec at a glance

Here is a pipeline that sets more than the defaults:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-to-warehouse
spec:
  schedule: { cron: "CRON_TZ=Europe/Berlin 0 6 * * *", jitter: 5m }
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: incremental
        cursor: updated_at
      - name: customers
        sync: cdc
        columns:
          balance: { type: string }
  destination:
    ref: warehouse
    namespace: shop
    write: merge
    deletes: hard
  schemaPolicy: allow_columns
  delivery:
    require: effectively_once
    commit: { interval: 2m, maxBytes: 512MB, scope: stream }
  resources: { size: m }
  heal: { mode: suggest }
```

Enum values are lowercase (`merge`), durations are Go durations (`90s`, `15m`), and sizes in bytes take units (`512MB`, `64MiB`). The tables below give each field in a line; [Pipeline](../reference/specs/pipeline.md) in the reference has every field.

### Running

| Field | What it does | Unset |
|---|---|---|
| `mode` | `scheduled` or `continuous` | `scheduled` |
| `schedule.every` | Start a sync this often, such as `1h` | One of `every` and `cron` is required in a scheduled pipeline |
| `schedule.cron` | Start a sync at the times a cron expression gives, in UTC unless it starts with `CRON_TZ=`, such as `CRON_TZ=Europe/Berlin 0 6 * * *` | |
| `schedule.jitter` | Start each sync up to this much later, at random, so pipelines on the same schedule don't all start at once | No delay |
| `resources.size` | How much CPU and memory each job gets: `auto`, `xs` (0.5 vCPU, 1 GiB), `s` (1 vCPU, 2 GiB), `m` (2 vCPUs, 4 GiB), `l` (4 vCPUs, 8 GiB), `xl` (8 vCPUs, 16 GiB), or `{ cpu: 3, memory: 6Gi }` | `auto`: from the connectors' hints, then the job's measured peaks |
| `resources.min`, `resources.max` | The smallest and largest size `auto` may choose; `max` is also what a retry after running out of memory may grow to | `max`: `xl` for `auto`, otherwise the size itself |
| `resources.finishWithin` | Scheduled, with an automatic size: run as many lanes as the last sync's volume needs to finish within this long | |
| `resources.pool` | Which pool of runners runs the jobs | `default` |
| `dataPlane` | Which data plane's runners run the jobs | The server's, `default` |
| `heal.mode` | What OpenETL does about the pipeline's incidents: `off` opens them and does nothing else; `suggest` also proposes actions, such as pausing the pipeline or a stream, or syncing less often; `auto_safe` takes them; `auto` does the same as `auto_safe` | `auto_safe` |

[Schedules](./schedules.md) covers when syncs start: intervals, cron time zones, jitter, a sync that runs long, pausing, and time OpenETL wasn't running.

### Source and streams

| Field | What it does | Unset |
|---|---|---|
| `source.ref` | The `Source` to read | Required |
| `source.streams` | The streams to copy, each by `name` as `openetl discover` lists it, with the settings below | Every stream the source offers |
| `sync` | How the stream is read: `full_refresh`, `incremental`, `cdc` or `stream` | The first way the source says it reads the stream; in a continuous pipeline, the first whose reads don't end |
| `cursor` | The column an incremental read goes by, such as `updated_at` | The source's cursor for the stream, if it names one |
| `columns.NAME.type` | For a column the source declares as a decimal, deliver it as `string` (the source's text) or `float64` (a double, which can lose digits) instead | The decimal |
| `timestamps` | `microsecond`, or `nanosecond` for a destination that keeps them | `microsecond`; finer values are truncated |
| `require` | This stream's lowest acceptable delivery guarantee, overriding `delivery.require` | The pipeline's |
| `collapse` | For a stream without a primary key: rows with the same content become one row | On for a stream that may be read twice and requires more than at least once |
| `quarantine` | What a value that doesn't fit its type holds back: its `row`, or only its `cell` (a keyed stream into a destination that patches) | `row` |
| `held`, `heldValue` | What a held cell shows: `null`, a `value` given in `heldValue`, or `last`, the destination's value (merge only); also under `columns.NAME` for one column | `null` |
| `groups` | Lists of columns held together: holding one holds the rest | |
| `typeChanges` | `refuse` or `allow`, overriding `destination.typeChanges` for this stream | The destination's |

### Destination

| Field | What it does | Unset |
|---|---|---|
| `destination.ref` | The `Destination` to write to | Required |
| `destination.namespace` | The schema or dataset to write into | |
| `destination.write` | `append`, `overwrite` or `merge` | Where the destination can: `overwrite` for a full refresh, `merge` for a keyed stream read with CDC, `append` otherwise |
| `destination.deletes` | How merge applies a delete: `soft` keeps the row with `_openetl_deleted` set to true, `hard` removes it | `soft` |
| `destination.naming` | `normalized`: lowercase, with anything outside `a-z`, `0-9` and `_` replaced; `source`: names as the source has them, quoted | `normalized` |
| `destination.typeChanges` | Whether the destination may change a column's type itself when the stream's widens: `refuse` or `allow` | `refuse` |

### Schema and delivery

| Field | What it does | Unset |
|---|---|---|
| `schemaPolicy` | What a change to a stream's schema does. `allow_all` adds columns and widens types, and sends a type change that isn't a widening to a new column; `allow_columns` adds columns and widens types, but holds other type changes or fails when they cannot be held; `block` holds changes or fails the stream; `ignore_new` drops new columns and quarantines values that don't fit | `allow_all` |
| `delivery.require` | The lowest guarantee any stream may have: `at_least_once`, `effectively_once` or `exactly_once` | `exactly_once` |
| `delivery.commit.interval` | How often to commit what's been read | 5 minutes in a scheduled pipeline; in a continuous one, the destination's own interval, or 10 seconds |
| `delivery.commit.maxBytes` | Commit once this much has been read since the last commit | `256MB`, which grows when commits can't keep up |
| `delivery.commit.scope` | Commit every stream together (`pipeline`) or each on its own (`stream`) | `pipeline` where the destination can, otherwise `stream` |
| `delivery.resumePointTimeout` | How long a commit waits for a point the source can resume from exactly, before committing without one where the stream keeps its guarantee anyway; `none` waits forever | `1m` |
| `types.infinity` | What becomes of infinite dates and timestamps: `clamp` to the type's largest or smallest value, or `quarantine` | `clamp` |

### After and between

| Field | What it does |
|---|---|
| `postSync` | Steps that run, in order, after each sync's data is committed: `dbt` (a `DbtProject`, with `command` `build`, `run` or `test`, `select`, `exclude` and `schema`) or `spark` (your own Spark application). A step that fails fails the sync. Scheduled pipelines only; see [`examples/faker-to-postgres-dbt.yaml`](../../examples/faker-to-postgres-dbt.yaml) |
| `transform` | Your own code between the source and the destination: `spark` in a scheduled pipeline, `flink` in a continuous one |
| `sparkAccess`, `flinkAccess` | How the pipeline's Spark or Flink code reaches its part of the data plane's storage; unset, the cluster's own identity |

[Integrate](../integrate/index.md) covers dbt, Spark and Flink.

[Handle schema changes and holds](./schema-changes.md) walks through a new column, a refused type change, and repairing and releasing the hold.

## Continuous pipelines

A continuous pipeline has `mode: continuous` and no `schedule`. It runs one job that keeps reading and commits as it goes, every 10 seconds unless the destination or `delivery.commit.interval` sets another interval:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: orders-live
spec:
  mode: continuous
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: cdc
  destination: { ref: warehouse, namespace: shop }
```

Its streams must be read in a way that never ends: `cdc` from a database, or `stream` from an event stream such as a topic. The plan fails a stream read any other way. A continuous pipeline has no syncs, so it has no `postSync` steps, and its transform, if any, is Flink. Pausing it stops its job; resuming starts it again from its last checkpoint.

## Delivery guarantees in practice

Every pipeline requires `exactly_once` unless `delivery.require`, or a stream's `require`, says otherwise, and a pipeline doesn't run while any stream's guarantee is below what it requires. Each stream's guarantee comes from what the source and the destination can each promise; the plan's streams section gives it and the reason.

Some destinations can't promise more than at least once: a stream that only delivers records and waits for them to be acknowledged, can deliver one twice after a retry. A pipeline into one says so:

```yaml
delivery:
  require: at_least_once
```

Under `at_least_once`, rows can land again, and nothing removes the repeats. For example, an incremental read starts at the highest cursor value the last read saw, so rows with that value are read again on every sync; under a higher guarantee a destination that can drops them when it applies the sync, and under `at_least_once` they land again. A retry after a crash can land rows a second time too.

So when you read a stream delivered at least once, keep one row per `_openetl_id`. Every row has one, made from the stream, the row's primary key (or its whole content when it has none) and its `_openetl_seq`, so a repeat of the same version of a row has the same ID. In Postgres:

```sql
select distinct on (_openetl_id) *
from faker.users
order by _openetl_id;
```

For the latest version of each row of an appended stream, keep the highest `_openetl_seq` for each primary key instead:

```sql
select distinct on (id) *
from faker.users
order by id, _openetl_seq desc;
```

[Delivery guarantees](./delivery.md) covers what decides each stream's guarantee, every reason the plan gives, streams without a primary key, and reading a stream delivered at least once.

## Day to day

| Command | What it does |
|---|---|
| `openetl plan -f FILE` | Show what applying the documents would change, and each pipeline's plan |
| `openetl apply -f FILE` | Create or update the documents' resources |
| `openetl sync NAME` | Start a sync of a scheduled pipeline now; it fails if the pipeline is paused or a sync is running |
| `openetl pause NAME` | Stop a pipeline's scheduled syncs; a sync that's running carries on. A continuous pipeline's job stops. `--stream STREAM` stops only that stream, which syncs skip ([Pause one stream](./schedules.md#pause-one-stream)) |
| `openetl resume NAME` | Start its scheduled syncs, or its job, again; `--stream STREAM` resumes a stream paused on its own |
| `openetl reset NAME` | Read its streams again from the start on the next sync or run; `--stream` picks streams. See [Reading from the start again](./positions.md) |
| `openetl rewind --yes NAME` | Let it restart from the checkpoint before its latest, if the latest can't be read, delivering that epoch again |
| `openetl overrides NAME` | List its paused streams, and heal's overrides of its spec ([When heal slows syncs down](./schedules.md#when-heal-slows-syncs-down)) |
| `openetl overrides remove NAME FIELD` | Give the spec back its say in a field heal overrides: `schedule.every` or `source.rate_limit_scale` |
| `openetl syncs NAME` | List its syncs, newest first, with how long each took, the rows it copied, its version and any error |
| `openetl logs NAME` | Print the latest sync's log; `-f` follows a running one, and `--sync ID` picks another sync |
| `openetl versions NAME` | List its versions, newest first |
| `openetl rescale --size SIZE NAME` | Size its jobs until its next version; a running job restarts at a checkpoint. `--size auto` gives the spec back its say |
| `openetl check source NAME`, `openetl check destination NAME` | Check that a runner can reach the source or destination with its config |
| `openetl discover NAME` | List a source's streams and their columns, and refresh the catalog plans read |
| `openetl holds NAME` | List the columns and cells the pipeline holds back, why, and the ways to clear each |
| `openetl holds release NAME STREAM COLUMN` | Patch a hold's values in on the pipeline's next run; `--new-version`, before the names, sends a column held whole to a new column, `COLUMN__v2`, instead |
| `openetl holds discard NAME STREAM COLUMN` | Drop a hold's values; the column stays as the destination has it |

Flags go before the names, and every command takes `--workspace`. `openetl COMMAND -h` lists a command's flags.

The console shows the same pipelines, syncs, versions and holds; [Use the console](../use/index.md) walks through it.

If a pipeline waits, fails repeatedly or falls behind, start with [Troubleshoot a pipeline](./troubleshooting.md).
