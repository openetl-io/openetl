---
title: Pipeline
description: Every field of a Pipeline document, with its type, required or optional status, default and meaning.
---

A `Pipeline` copies streams from a `Source` to a `Destination`, in syncs on a schedule or continuously. This page lists every field its document takes. [Pipelines as code](../../pipelines/index.md) shows how to write one, plan it and apply it.

The **Required** column applies within the containing object or list entry. Optional objects may be omitted; when you include one, its required children must be set. Conditional requirements are stated in the table; “one of” means exactly one of the alternatives. **Default** describes what omission means.

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
  resources: { size: m, pool: batch }
  postSync:
    - dbt: { project: shop-models, command: build, select: "tag:daily" }
  heal: { mode: suggest }
```

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `Pipeline` |
| `metadata.name` | name | Yes | — | The pipeline's name: lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | No | The workspace `openetl apply --workspace` names, `default` unless it says | The workspace it belongs to |
| `spec` | object | Yes | — | The fields in the sections below |

In `spec`:

- A field the spec doesn't have is refused, and so is a value of the wrong type.
- Enum values are lowercase, as in `merge`.
- A duration is a Go duration, such as `90s`, `15m` or `1h30m`.
- A byte count is a whole number of bytes, or a number with a unit: `KB`, `MB`, `GB` and `TB` are powers of 1,000, and `KiB`, `MiB`, `GiB` and `TiB` powers of 1,024, as in `256MB` or `64MiB`.
- A delivery guarantee is `at_least_once`, `effectively_once` or `exactly_once`. [Concepts](../../get-started/concepts.md#delivery-guarantees) explains each.
- A secret reference is `{ secret: NAME }`, naming a secret in the same workspace.
- Some defaults depend on the source's streams and the destination's connector. `openetl plan` shows what they come to: each stream's write mode and guarantee, and the job size.

## Running

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `mode` | `scheduled` or `continuous` | No | `scheduled` | A scheduled pipeline copies in syncs, which start on its `schedule`. A continuous one runs one job that keeps reading, and commits as it goes. A continuous pipeline has no `schedule`, `postSync`, `transform.spark` or `resources.finishWithin`. |
| `schedule` | object | Yes, when `mode: scheduled` | — | When syncs start: one of `every` and `cron`. A new scheduled pipeline starts its first sync when it's applied. A start that comes while a sync is still running is skipped. |
| `schedule.every` | duration, positive | One of `every` or `cron` | None | Start a sync this often, such as `1h`, counting from midnight UTC on 1 January 1970: `1h` starts one on the hour. |
| `schedule.cron` | cron expression | One of `every` or `cron` | None | Start a sync at the times a cron expression gives, with five fields: minute, hour, day of the month, month and day of the week, as in `0 6 * * *`. It's in UTC unless it starts with `CRON_TZ=` and a time zone, as in `CRON_TZ=Europe/Berlin 0 6 * * *`. |
| `schedule.jitter` | duration | No | None | A random delay of up to this much before each start, so that pipelines on the same schedule don't all start at once |
| `dataPlane` | string | No | The server's data plane: its `--data-plane` setting, `default` unless set | The data plane whose runners run the pipeline's jobs |
| `heal` | object | No | `mode: auto_safe` | What OpenETL does about the pipeline's [incidents](../../get-started/concepts.md#incidents) |
| `heal.mode` | `off`, `suggest`, `auto_safe` or `auto` | No | `auto_safe` | `off` opens incidents and notifies, and takes no action. `suggest` proposes the actions that hold data back, such as pausing the pipeline, rather than taking them. `auto_safe` applies every playbook's actions. `auto` is the same as `auto_safe`. |

## Resources

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `resources` | object | No | Sized automatically, in the `default` pool | How much CPU and memory the pipeline's jobs get, and which runners run them |
| `resources.size` | job size, or `auto` | No | `auto` | Each job's size. `auto` takes it from the connectors' hints, and then from the job's measured peaks; with neither, it's `s`. |
| `resources.min` | job size | No | None | The smallest size `auto` chooses |
| `resources.max` | job size | No | `xl` with `auto`, and otherwise `resources.size` itself | The largest size a job grows to: `auto`'s choice, and a retry after the job runs out of memory |
| `resources.finishWithin` | duration, positive | No | None | Scheduled only, with an automatic size: run as many lanes as the last sync's bytes need, at each lane's measured rate, to finish within this long. |
| `resources.pool` | pool name | No | `default` | The pool of runners that runs the jobs: at most 64 letters, digits, dots, hyphens and underscores, starting with a letter or digit |

When a job size is an object rather than a preset, both `cpu` and `memory` are required; neither has a default.

`resources.min` can't be larger than `resources.size` or `resources.max`, nor `resources.size` larger than `resources.max`. A job size is a preset or amounts:

| Size | CPU | Memory |
|---|---|---|
| `xs` | 0.5 vCPU | 1 GiB |
| `s` | 1 vCPU | 2 GiB |
| `m` | 2 vCPUs | 4 GiB |
| `l` | 4 vCPUs | 8 GiB |
| `xl` | 8 vCPUs | 16 GiB |
| `{ cpu: 3, memory: 6Gi }` | `cpu` vCPUs, a positive number | `memory`, positive: bytes, or a whole number with `Ki`, `Mi`, `Gi` or `Ti` |

## Source and streams

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `source` | object | Yes | — | What to read |
| `source.ref` | name | Yes | — | The `Source` to read, by its name in the same workspace |
| `source.streams` | list | No | Every stream the source offers, each with the defaults below | The streams to copy. Naming a stream the source doesn't offer is a problem in the plan. |
| `source.streams[].name` | string | Yes | — | The stream's name, as `openetl discover` lists it. Each stream appears once. |
| `source.streams[].sync` | `full_refresh`, `incremental`, `cdc` or `stream` | No | The first way the source says it reads the stream; in a continuous pipeline, the first of those that's `cdc` or `stream` | How the stream is read. A continuous pipeline's streams are read with `cdc` or `stream`. |
| `source.streams[].cursor` | column name | When the read mode needs a cursor and the source supplies none | The stream's cursor, if the source names one | The column an `incremental` read goes by, such as `updated_at`. An incremental stream without a cursor is a problem in the plan, and so is a stream with a primary key that's read with `stream`, whose cursor tells one event of a key from the next. |
| `source.streams[].timestamps` | `microsecond` or `nanosecond` | No | `microsecond` | The unit timestamps arrive in. Finer values are truncated. Nanoseconds need a destination that keeps them, and hold only 1677-09-21 to 2262-04-11. A Spark transform's inputs can't keep nanoseconds. |
| `source.streams[].require` | delivery guarantee | No | `delivery.require` | The lowest guarantee this stream may have, in place of `delivery.require` |
| `source.streams[].collapse` | boolean | No | `true` for a stream whose reads may repeat rows (the plan's reason says `superset replay`) and that requires more than `at_least_once`; otherwise `false` | For a stream without a primary key: rows with the same content become one row. A stream with a primary key ignores it, and the plan warns. |
| `source.streams[].quarantine` | `row` or `cell` | No | `row` | What a value that doesn't fit its type holds back: its whole row, or only its cell |
| `source.streams[].held` | `null`, `value` or `last` | No | `null`, or `value` when `heldValue` is set | What a held cell shows, in the columns that `columns` doesn't set it for: `null`; `value`, the value in `heldValue`; or `last`, the value the destination already has, which needs merge. |
| `source.streams[].heldValue` | any value | Yes with `held: value` | None | With `held: value`, what held cells show; with `held: last`, what they show in rows the destination doesn't have yet. A value of the column's delivered type. Set alone, it means `held: value`. `held: null` takes none. |
| `source.streams[].groups` | list of lists of column names | No | None | Columns held together, by their source names: holding one holds the rest. Each group has two columns or more. |
| `source.streams[].typeChanges` | `refuse` or `allow` | No | `destination.typeChanges` | `destination.typeChanges` for this stream alone |
| `source.streams[].columns` | map of column names to objects | No | None | Settings for single columns, by their source names |
| `source.streams[].columns.NAME` | object | No | None | One column's settings. It sets a `type`, a `held` or a `heldValue`, or more than one. |
| `source.streams[].columns.NAME.type` | `string` or `float64` | No | The decimal | For a column the source declares as a decimal: `string` delivers the source's text for the value, NaN included; `float64` delivers a double, which can lose digits, and keeps NaN and infinities. Setting it on any other column is a problem in the plan. |
| `source.streams[].columns.NAME.held` | `null`, `value` or `last` | No | The stream's `held` | What this column's held cells show, as the stream's `held` |
| `source.streams[].columns.NAME.heldValue` | any value | Yes with this column's `held: value` | The stream's `heldValue` | As the stream's `heldValue`, for this column |

How a stream holds is the plan's to check. `quarantine: cell`, `held`, `heldValue` and `groups`, on the stream or on a column, need a stream with a primary key and a destination that patches the stream's write mode; `last` needs merge and a destination that keeps the last value. A transform's inputs set none of these, nor `quarantine`, `collapse` or `typeChanges`.

## Destination

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `destination` | object | Yes | — | Where and how to write |
| `destination.ref` | name | Yes | — | The `Destination` to write to, by its name in the same workspace |
| `destination.namespace` | string | No | The destination's default | The schema or dataset to write into |
| `destination.tables` | map of stream names to names | No | Each stream's own name, through `destination.naming` | Streams' names in the destination, exactly as written: a table, or what the destination has instead, such as a topic. A stream left out goes by its own name. Two streams can't share one, and it can't be used with a transform, whose outputs name their tables. Changing one on a running pipeline leaves what was delivered in the old table, and the new one starts with what's read next; `openetl reset --stream` reads it all again. |
| `destination.write` | `append`, `overwrite` or `merge` | No | For each stream, where the destination has the mode: `overwrite` for a stream read with `full_refresh`, and `merge` for a stream with a primary key read with `cdc`; `append` otherwise | How every stream is written. `merge` needs streams with a primary key, and `overwrite` streams read with `full_refresh`. |
| `destination.deletes` | `soft` or `hard` | No | `soft` | How merge applies a delete. `soft` keeps the row, with `_openetl_deleted` true and its other columns as they were; `hard` removes it. Other write modes don't apply deletes, and the plan warns. |
| `destination.naming` | `normalized` or `source` | No | `normalized` | How tables and columns are named. `normalized` names are lowercase, with anything outside `a-z`, `0-9` and `_` replaced; `source` names are the source's, quoted. |
| `destination.typeChanges` | `refuse` or `allow` | No | `refuse` | Whether the destination changes a column's type itself when the stream's type widens, for every stream that doesn't say. With `refuse`, the destination refuses, and the column is held, or the stream stops, until a person changes the column. With `allow`, the destination changes the type in place, which on a database can rewrite the table under a lock. |

## Schema

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `schemaPolicy` | `allow_all`, `allow_columns`, `block` or `ignore_new` | No | `allow_all` | What a change to a stream's schema does, once the destination has the stream: see the table below. |
| `types` | object | No | `infinity: clamp` | How values map to types where the pipeline has a choice |
| `types.infinity` | `clamp` or `quarantine` | No | `clamp` | What becomes of infinite dates and timestamps: `clamp` turns them into the type's largest or smallest value, and `quarantine` quarantines them. |

| Policy | A new column | A type that widens | A type that changes otherwise |
|---|---|---|---|
| `allow_all` | Added | Widened | Delivered to a new column, `COLUMN__v2` |
| `allow_columns` | Added | Widened | Held, or where the column can't be held, fails the sync with `SCHEMA_DRIFT` |
| `block` | Held, or fails the sync | Held, or fails the sync | Held, or fails the sync |
| `ignore_new` | Dropped | Values cast to the column's type | Values cast to the column's type |

A type widens when every old value fits the new type: an int16 to an int32, an int64, a float32 or a float64; an int32 to an int64 or a float64; an int64 or a float32 to a float64; an integer or a decimal to a decimal that holds it; and anything to a string. Where a column's type widens, the destination changes it only if `typeChanges` allows; otherwise the destination refuses, and the column is held or the stream stops. Under `ignore_new`, a value that doesn't fit the column's type is quarantined, as `quarantine` says. In a continuous pipeline, what fails a sync fails its job.

## Delivery

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `delivery` | object | No | The defaults below | How the pipeline commits, and the guarantee it requires |
| `delivery.require` | delivery guarantee | No | `exactly_once` | The lowest guarantee any stream may have. A stream whose guarantee is lower is a problem in the plan. |
| `delivery.commit` | object | No | The defaults below | When the pipeline commits what it has read |
| `delivery.commit.interval` | duration, positive | No | 5 minutes in a scheduled pipeline; in a continuous one, the destination's own interval, or else 10 seconds | How often to commit. Outside the destination's minimum and maximum, if it has them, it's a problem in the plan. |
| `delivery.commit.maxBytes` | byte count | No | `256MB`, which grows when commits can't keep up | Commit once this much has been read since the last commit. Once set, it doesn't grow. |
| `delivery.commit.scope` | `pipeline` or `stream` | No | `pipeline` where the destination can, otherwise `stream` | Commit every stream together (`pipeline`), or each on its own (`stream`). A scope the destination can't do is a problem in the plan. |
| `delivery.resumePointTimeout` | duration, positive, or `none` | No | `1m` | How long a commit waits for a point the source can resume from exactly, before it commits without one. It commits without one only where the stream keeps its guarantee anyway. `none` waits forever. |

## Transforms

A transform is your own code between the source and the destination: Spark in a scheduled pipeline, Flink in a continuous one. The source's streams are its inputs, and its outputs are what the destination gets. [Integrate](../../integrate/index.md) covers writing one.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `transform` | object | No | None | One of `spark` and `flink` |
| `transform.spark` | object | One of `spark` or `flink`, when `transform` is set | None | Runs your openetl-spark `Transform` on each epoch of a sync, on a Spark cluster. Scheduled pipelines only. |
| `transform.spark.cluster` | name | Yes | — | A `SparkCluster` in the same workspace |
| `transform.spark.code` | object | Yes | — | The code: one of `jar` and `python`, and its `sha256`. The runner checks the file and gives the cluster its own copy. |
| `transform.spark.code.jar` | URI | One of `jar` or `python` | None | A jar, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.spark.code.python` | URI | One of `jar` or `python` | None | A Python file that calls `openetl_spark.run` with its transform, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.spark.code.sha256` | 64 lowercase hex digits | Yes | — | The file's SHA-256 |
| `transform.spark.code.class` | class name | Yes with `jar` | — | The class in the jar that implements openetl-spark's `Transform`. A Python file takes none. |
| `transform.spark.conf` | map of strings | No | None | Spark properties for the application, over the cluster's. Each name starts with `spark.`, and isn't one the runner sets: `spark.master`, `spark.submit.deployMode`, `spark.app.name`, `spark.yarn.tags`, `spark.kubernetes.driver.pod.name`, or any that starts with `spark.openetl.`. |
| `transform.spark.outputs` | list | No | Each input stream, under its name and with its settings | The transform's outputs, which are the destination's streams |
| `transform.spark.outputs[].name` | string | Yes | — | The output's name. Each output appears once. |
| `transform.spark.outputs[].primaryKey` | list of column names | No | The primary key of the input stream with the output's name, if there is one | The output's primary key |
| `transform.spark.outputs[].write` | `append`, `overwrite` or `merge` | No | `destination.write`; without it, `overwrite` when every input is read with `full_refresh`, and `merge` when the input stream with the output's name is read with `cdc` and the output has a primary key, where the destination has the mode; `append` otherwise | How the output is written. `overwrite` needs every input read with `full_refresh`, and `merge` a primary key. |
| `transform.spark.outputs[].require` | delivery guarantee | No | The `require` of the input stream with the output's name, then `delivery.require` | The lowest guarantee the output may have |
| `transform.flink` | object | One of `spark` or `flink`, when `transform` is set | None | Runs your Flink job, built with openetl-flink, between a continuous pipeline's source and destination, on a Flink cluster. The job's checkpoints are the pipeline's. Continuous pipelines only. |
| `transform.flink.cluster` | name | Yes | — | A `FlinkCluster` in the same workspace |
| `transform.flink.code` | object | Yes | — | The job's jar and its `sha256`. The runner checks the file and gives the cluster its own copy. |
| `transform.flink.code.jar` | URI | Yes | — | A jar, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.flink.code.sha256` | 64 lowercase hex digits | Yes | — | The file's SHA-256 |
| `transform.flink.code.class` | class name | No | The class the jar's manifest names | The job's main class |
| `transform.flink.parallelism` | integer, 0 or more | No | `0`: the cluster's default | The job's parallelism |
| `transform.flink.conf` | map of strings | No | None | Flink configuration for the job, over the cluster's. Each name has a dot in it, and isn't one the runner sets, listed below. |
| `transform.flink.outputs` | list | No | Each input stream, under its name and with its settings | As `transform.spark.outputs` |
| `transform.flink.outputs[].name` | string | Yes | — | As `transform.spark.outputs[].name` |
| `transform.flink.outputs[].primaryKey` | list of column names | No | As `transform.spark.outputs[].primaryKey` | As `transform.spark.outputs[].primaryKey` |
| `transform.flink.outputs[].write` | `append`, `overwrite` or `merge` | No | As `transform.spark.outputs[].write` | As `transform.spark.outputs[].write` |
| `transform.flink.outputs[].require` | delivery guarantee | No | As `transform.spark.outputs[].require` | As `transform.spark.outputs[].require` |

The runner sets these Flink options itself, so `transform.flink.conf` can't: `parallelism.default`, `pipeline.name`, `execution.runtime-mode`, `execution.target`, `jobmanager.execution.failover-strategy`, `kubernetes.cluster-id`, `kubernetes.namespace`, `yarn.application.name` and `yarn.tags`, and any whose name starts with `execution.checkpointing.`, `state.checkpoints.`, `state.savepoints.`, `execution.state-recovery.`, `execution.savepoint`, `restart-strategy`, `high-availability`, `kubernetes.operator.`, `openetl.` or `$internal.`.

A Flink transform's inputs must each replay exactly from a resume point, since its checkpoints restore them; the plan says which don't.

## Steps after a sync

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `postSync` | list | No | None | Steps that run in order after a sync's data is committed. Each has one of `dbt` and `spark`. A step that fails fails the sync, and the steps after it don't run. Scheduled pipelines only. |
| `postSync[].dbt` | object | One of `dbt` or `spark` per step | None | Runs a dbt project against the pipeline's destination |
| `postSync[].dbt.project` | name | Yes | — | A `DbtProject` in the same workspace. A sync runs the project's files as they were when it started. |
| `postSync[].dbt.command` | `build`, `run` or `test` | No | `build` | The dbt command to run |
| `postSync[].dbt.select` | string | No | None | dbt's `--select` |
| `postSync[].dbt.exclude` | string | No | None | dbt's `--exclude` |
| `postSync[].dbt.schema` | string | No | The schema the pipeline writes to | The target schema |
| `postSync[].spark` | object | One of `dbt` or `spark` per step | None | Runs your own Spark application on a Spark cluster |
| `postSync[].spark.cluster` | name | Yes | — | A `SparkCluster` in the same workspace. A sync runs the cluster's settings as they were when it started. |
| `postSync[].spark.code` | object | Yes | — | The application: one of `jar` and `python`, and its `sha256`. The runner checks the file and gives the cluster its own copy. |
| `postSync[].spark.code.jar` | URI | One of `jar` or `python` | None | A jar, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `postSync[].spark.code.python` | URI | One of `jar` or `python` | None | A Python file, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `postSync[].spark.code.sha256` | 64 lowercase hex digits | Yes | — | The file's SHA-256 |
| `postSync[].spark.code.class` | class name | No | The class the jar's manifest names | A jar's main class. A Python file takes none. |
| `postSync[].spark.args` | list of strings | No | None | The application's arguments |
| `postSync[].spark.conf` | map of strings | No | None | Spark properties for the application, over the cluster's, named as for `transform.spark.conf` |

## Access to the data plane's storage

A transform or step reaches the pipeline's own part of the data plane's object storage with the cluster's own identity, which reaches every pipeline's, unless the pipeline gives it one of its own.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `sparkAccess` | object | No | The cluster's own identity | How the pipeline's Spark applications reach its `_spark/` prefix of the data plane's store. It sets `emrRole`, `keys`, or both, and needs a Spark transform or step. |
| `sparkAccess.emrRole` | IAM role ARN | One or both of `emrRole` and `keys`, when `sparkAccess` is set | None | On EMR: the runtime role each step runs as (`ExecutionRoleArn`, EMR 6.7 and later). It's all the step's AWS access, to your own data too. |
| `sparkAccess.keys` | object | One or both of `emrRole` and `keys`, when `sparkAccess` is set | None | Elsewhere: S3A keys for the data plane's bucket alone |
| `sparkAccess.keys.accessKeyId` | string, or secret reference | Yes in `keys` | — | The access key's ID |
| `sparkAccess.keys.secretAccessKey` | secret reference | Yes in `keys` | — | The secret access key, as a secret reference, so the key isn't kept in the spec |
| `flinkAccess` | object | No | The cluster's own identity | How the pipeline's Flink job reaches its `_flink/` prefix of the data plane's store, where its code, checkpoints and savepoints are. It sets `emrRole`, `keys`, or both, and needs a Flink transform. |
| `flinkAccess.emrRole` | IAM role ARN | One or both of `emrRole` and `keys`, when `flinkAccess` is set | None | On EMR: the runtime role each step runs as (`ExecutionRoleArn`) |
| `flinkAccess.keys` | object | One or both of `emrRole` and `keys`, when `flinkAccess` is set | None | Elsewhere: S3 keys for the data plane's bucket alone, for Flink's Hadoop S3 filesystem. A session cluster reads its filesystems' settings when it starts, so it can't take them. |
| `flinkAccess.keys.accessKeyId` | string, or secret reference | Yes in `keys` | — | The access key's ID |
| `flinkAccess.keys.secretAccessKey` | secret reference | Yes in `keys` | — | The secret access key, as a secret reference |

## A continuous pipeline

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
  destination: { ref: lake, namespace: shop }
  delivery:
    commit: { interval: 30s }
  transform:
    flink:
      cluster: flink
      code:
        jar: s3://acme-jobs/enrich-orders-1.4.0.jar
        sha256: 9f2c4a8e0b6d1f3a5c7e9b2d4f6a8c0e1b3d5f7a9c2e4b6d8f0a1c3e5b7d9f2a
      parallelism: 4
      outputs:
        - name: orders_enriched
          primaryKey: [order_id]
          write: merge
  flinkAccess:
    keys:
      accessKeyId: { secret: lake-flink-key-id }
      secretAccessKey: { secret: lake-flink-key }
```
