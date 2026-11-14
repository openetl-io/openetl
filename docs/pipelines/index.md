---
title: Pipelines as code
description: How pipelines are written as YAML, planned and applied with the CLI, versioned, and run day to day.
---

A pipeline is a YAML document that you can version in git. Use `openetl plan` to review changes and stream delivery guarantees. Use `openetl apply` to create or update the resources. This guide explains the workflow, resource documents and common pipeline settings.

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

A `Source` or `Destination` specifies a connector and its `config`. The connector's documentation defines the configuration fields. `openetl connector spec TARGET` prints the connector's specification, including its configuration JSON Schema.

An unversioned name such as `postgres` can select a newer installed version each time you apply the connection. The plan warns about this. `postgres@1` restricts selection to major version 1. Pin an exact version or digest when the deployment requires a specific release.

[Choose and manage connectors](./connectors.md) covers installing official, community and private packages, selecting versions, and upgrading them independently of OpenETL.

## Secrets

A config never holds a password. It names a secret instead, as `{ secret: NAME }`:

```yaml
config:
  dsn: { secret: local-postgres-dsn }
```

Store secrets with the CLI. `set` reads a value from standard input or `--from-file`. With `--ref`, it stores a reference to a cloud secret store. Runners resolve that reference with their cloud identity:

```sh
echo postgres://postgres:quickstart@quickstart-postgres:5432/postgres | openetl secret set local-postgres-dsn
openetl secret list
openetl secret delete local-postgres-dsn
```

`list` shows current resources that use each secret. `delete` refuses a secret used by a current source, destination, webhook, Spark cluster, Flink cluster or pipeline. Check private connector sources separately. [Manage connection secrets](../self-host/secrets.md) covers cloud references, credential replacement and deletion.

`set` drops one line ending from the end of a value it reads, and runners do the same with a value in a cloud store that's text, so a file saved by an editor or written by `echo` works as is. A field of a JSON value is used exactly as it is.

## Plan, then apply

`openetl plan -f FILE` sends the documents to the API, which checks them and works out what applying them would do, without changing anything. Here's a second pipeline from the quickstart's `faker` source, into Parquet files on the runner's disk with the built-in `parquet` connector, at 06:00 every day:

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

- **A line per document** shows its planned state and version. Pipelines affected by a source or destination change also get new versions, even if the file does not define them. Their lines end with "which uses what changed".
- **A section per pipeline**, which can have:
  - `changes`: each field that changes, as `field: before -> after`.
  - `job size`: how much CPU and memory its jobs get, what set that, and the pool of runners it runs in.
  - `transform.sql`: for a pipeline with a SQL transform, each statement and its output. [A SQL transform in the plan](#a-sql-transform-in-the-plan) explains it.
  - `streams`: each stream's write mode, its [delivery guarantee](../get-started/concepts.md#delivery-guarantees), and the reason for that guarantee.
  - `problems`: what stops the pipeline from running. `plan` exits with an error, and `apply` refuses the whole file.
  - `warnings`: what's allowed but worth a second look.
  - `notes`: what the plan couldn't check, and why.

The streams section requires two sources of information:

- **The source catalog.** `openetl apply` and `openetl discover NAME` refresh it. Until discovery completes, the plan checks only delivery settings.
- **Destination capabilities.** The connector package declares them. For a local executable without a package, the plan explains that the runner checks guarantees before each sync.

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

### A SQL transform in the plan

The control plane does not plan SQL itself. For a pipeline with `transform.sql`, it sends the statements and the schemas of the streams they name to a runner of the pipeline's data plane. The runner's `openetl-engine` plans them and returns the result. The schemas come from the source's catalog. If the catalog declares no columns for a stream, they come from the schemas that the pipeline's earlier syncs recorded. Each sync plans the statements again when it starts, in the same way, against the columns the source declares and those the pipeline's last checkpoint recorded.

This pipeline reads a source named `shop` whose catalog declares the columns of `customers` and `orders`:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-to-parquet
spec:
  schedule: { every: 1h }
  source: { ref: shop }
  destination:
    ref: local-parquet
    tables: { contacts: customer_contacts }
  transform:
    sql: |
      INSERT INTO customers
      SELECT id, lower(email) AS email, created_at
      FROM customers;

      INSERT INTO contacts
      SELECT id AS customer_id, email AS contact_email
      FROM customers
      WHERE email IS NOT NULL;
```

Its plan, without the job size:

```text
workspaces/default/pipelines/shop-to-parquet (version known after apply)
  transform.sql:
    1:1  INSERT INTO customers  row_wise  key id           no exchange  reads customers; replaces stream customers
    5:1  INSERT INTO contacts   row_wise  key customer_id  no exchange  reads customers
    output customers: id int64, email utf8, created_at timestamp[us, tz=UTC]
    output contacts: customer_id int64, contact_email utf8
    passed through: orders
  streams:
    orders                         append  exactly_once  exact replay
    customers                      append  exactly_once  a transform's output over exact inputs
    contacts -> customer_contacts  append  exactly_once  a transform's output over exact inputs
```

The `transform.sql` section has:

- **A line per statement:** where its `INSERT` is in the SQL, as line and column; the output it writes; its class; its output's primary key; whether its rows are exchanged between lanes; and the streams and views it reads. Only `row_wise` statements plan; a statement of another class is a problem. An output without a primary key cannot be written with `merge`.
- **The columns of each output,** with their types.
- **The streams that pass through:** the source's streams that no statement writes. They reach the destination as they would without a transform.
- **The streams that are not delivered,** when `transform.passthrough` is `none`.

[Write a SQL transform](./sql-transforms.md) walks through a transform from its statements to what the destination gets.

A statement that writes a source stream's name replaces that stream: the output is written in its place. The output takes the stream's entry in `destination.tables` and its `require`. The `streams` section lists what the destination gets: the outputs, and the streams that pass through.

Errors in the SQL are problems, with the line and column in the SQL text. A statement of a class that only a later tier runs, such as a group window, is a problem that names the tier:

```text
workspaces/default/pipelines/shop-to-parquet (version known after apply)
  problems:
    transform.sql:8:10: a group window: transforms don't run group windows yet (tier 2)
    transform.sql:11:27: there's no column mail; there are id, email, name, created_at
openetl: the plan has problems, so apply would refuse it
```

If the statements cannot be planned, a note that starts with `transform.sql wasn't planned` says why, and the `streams` section is empty. This happens when:

- No runner of the pipeline's data plane takes the request within 30 seconds.
- The source has not been discovered.
- The catalog declares no columns for a stream that the SQL names, and no sync has recorded its schema yet.

When the plan is what you want, apply the same files:

```sh
openetl apply -f faker-to-parquet.yaml
```

An apply stores all documents together or stores none. Its output identifies each resource as "created", "updated" or "unchanged", followed by warnings.

After the apply, a runner checks each source and destination that changed or that a changed pipeline uses. The runner also discovers those sources. The output shows `checked` with their names. A failed check only gives a warning: the applied documents stay, and a new pipeline still starts. To skip the checks, use `--no-check`. To run the checks for a pipeline at any time, use `openetl check pipeline NAME`.

A new scheduled pipeline starts its first sync immediately. An updated pipeline waits for its schedule or a manual `openetl sync`.

`openetl apply --dry-run` performs apply validation without storing changes. It does not print the plan; use `openetl plan` to review the planned changes.

## Versions

Each applied document is stored as an immutable version identified by its content hash. A pipeline version pins its source, destination and dbt project versions. Changing a source therefore creates a new version of every pipeline that uses it. Each sync runs one version.

```sh
openetl versions faker-to-postgres
```

```text
VERSION                                                           CURRENT  CREATED              PROVENANCE
edab40da6d76c1aba235c7cd81c5618db8af2206c759539c5baea80a584560b3           2026-11-08 12:30:15  cli:examples/faker-to-postgres.yaml#3
9e052e7e6881672dbdcfcc4356f04f592bdfbd9777622e2d2a877eb1530db134  *        2026-11-08 12:28:23  cli:examples/faker-to-postgres.yaml#3
```

A version of a pipeline with `transform.sql` also keeps what planning its statements gave when the version was made: each output's columns and primary key. The API returns it as the version's `sql_plan`.

Provenance identifies the origin of a version. Here, it is the third document in a file applied with the CLI. Versions are identified by content. To restore an earlier pipeline definition, revert its git commit and apply the file. This does not undo changes already written to the destination.

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
| `destination.tables` | Streams' names in the destination, by stream, such as `orders: fct_orders`, exactly as written: a table, or what the destination has instead, such as a topic | Each stream's own name, through `destination.naming` |
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
| `transform` | What changes the data between the source and the destination: your own code, `spark` in a scheduled pipeline or `flink` in a continuous one; or `sql` statements, which the engine runs in each lane ([reference](../reference/specs/pipeline.md#transforms), [guide](./sql-transforms.md)) |
| `sparkAccess`, `flinkAccess` | How the pipeline's Spark or Flink code reaches its part of the data plane's storage; unset, the cluster's own identity |

[Integrate](../integrate/index.md) covers dbt, Spark and Flink.

[Handle schema changes and holds](./schema-changes.md) walks through a new column, a refused type change, and repairing and releasing the hold.

## Continuous pipelines

A continuous pipeline has `mode: continuous` and no `schedule`. Its job keeps reading and commits at intervals. The default is 10 seconds unless the destination or `delivery.commit.interval` specifies another value:

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

Its streams must be read in a way that never ends: `cdc` from a database, or `stream` from an event stream such as a topic. The plan fails a stream read any other way. A continuous pipeline has no syncs, so it has no `postSync` steps and no Spark transform. Pausing it stops its job; resuming starts it again from its last checkpoint.

## Delivery guarantees in practice

The default requirement is `exactly_once`. Set `delivery.require` to change it for the pipeline, or a stream's `require` to change it for that stream. OpenETL refuses to run a pipeline if any stream falls below its requirement. The plan lists each stream's guarantee and reason.

Deliver-and-acknowledge destinations can repeat a record after a retry. They provide at-least-once delivery. A pipeline using such a destination must accept that guarantee:

```yaml
delivery:
  require: at_least_once
```

Under `at_least_once`, repeated changes remain as duplicate rows. For example, an incremental read includes rows at the previous highest cursor value again. With a higher guarantee, a capable destination removes duplicates during apply. With `at_least_once`, it writes them again. Recovery after a failure can also produce duplicates.

When reading a stream delivered at least once, keep one row per `_openetl_id`. This ID combines the stream, primary key and `_openetl_seq`. Without a primary key, it uses the row's whole content instead. Repeated reads of the same version produce the same ID. In Postgres:

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

[Delivery guarantees](./delivery.md) explains guarantee selection, plan reasons and streams without primary keys. It also shows how to read data delivered at least once.

## Day to day

| Command | What it does |
|---|---|
| `openetl plan -f FILE` | Show what applying the documents would change, and each pipeline's plan |
| `openetl apply -f FILE` | Create or update the documents' resources, then check what changed on a runner; `--no-check` skips that |
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
| `openetl check pipeline NAME` | Check its source and destination, refresh the source's catalog and print its plan; fails if any of that finds a problem |
| `openetl discover NAME` | List a source's streams and their columns, and refresh the catalog plans read |
| `openetl holds NAME` | List the columns and cells the pipeline holds back, why, and the ways to clear each |
| `openetl holds release NAME STREAM COLUMN` | Patch a hold's values in on the pipeline's next run; `--new-version`, before the names, sends a column held whole to a new column, `COLUMN__v2`, instead |
| `openetl holds discard NAME STREAM COLUMN` | Drop a hold's values; the column stays as the destination has it |

Flags go before the names, and every command takes `--workspace`. `openetl COMMAND -h` lists a command's flags.

The console shows the same pipelines, syncs, versions and holds; [Use the console](../use/index.md) walks through it.

If a pipeline waits, fails repeatedly or falls behind, start with [Troubleshoot a pipeline](./troubleshooting.md).
