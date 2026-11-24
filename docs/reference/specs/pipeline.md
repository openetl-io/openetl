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

- A field the spec does not have is refused, and so is a value of the wrong type.
- An integer field takes a whole number in its range, such as `4`, `4.0` or `0x10`. A number with a fraction, such as `1.9`, is refused, and so is one outside the range. The error names the field, the value and the range: `resources.workers: "1.9" isn't a whole number from 1 to 64`.
- Enum values are lowercase, as in `merge`.
- A duration is a Go duration, such as `90s`, `15m` or `1h30m`.
- A byte count is an integer number of bytes or a number with a unit, such as `256MB` or `64MiB`. `KB`, `MB`, `GB` and `TB` use powers of 1,000. `KiB`, `MiB`, `GiB` and `TiB` use powers of 1,024.
- A delivery guarantee is `at_least_once`, `exactly_once_within`, `effectively_once` or `exactly_once`, from the lowest to the highest. A `require` accepts its own guarantee and the higher ones. [Concepts](../../get-started/concepts.md#delivery-guarantees) explains each, and [How the guarantees rank](../../pipelines/delivery.md#how-the-guarantees-rank) their order.
- A secret reference is `{ secret: NAME }`, naming a secret in the same workspace.
- Some defaults depend on the source's streams and the destination's connector. `openetl plan` shows what they come to: each stream's write mode and guarantee, and the job size.

## Running

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `mode` | `scheduled` or `continuous` | No | `scheduled` | A scheduled pipeline copies in syncs, which start on its `schedule`. A continuous one runs one job that keeps reading, and commits as it goes. A continuous pipeline has no `schedule`, `postSync`, `transform.spark` or `resources.finishWithin`. |
| `schedule` | object | Yes, when `mode: scheduled` | — | When syncs start: one of `every` and `cron`. A new scheduled pipeline starts its first sync when it is applied. A start that comes while a sync is still running is skipped. |
| `schedule.every` | duration, positive | One of `every` or `cron` | None | Start a sync this often, such as `1h`, counting from midnight UTC on 1 January 1970: `1h` starts one on the hour. |
| `schedule.cron` | cron expression | One of `every` or `cron` | None | Start a sync at the times a cron expression gives, with five fields: minute, hour, day of the month, month and day of the week, as in `0 6 * * *`. It is in UTC unless it starts with `CRON_TZ=` and a time zone, as in `CRON_TZ=Europe/Berlin 0 6 * * *`. |
| `schedule.jitter` | duration | No | None | A random delay of up to this much before each start, so that pipelines on the same schedule do not all start at once |
| `dataPlane` | string | No | The server's data plane: its `--data-plane` setting, `default` unless set | The data plane whose runners run the pipeline's jobs |
| `heal` | object | No | `mode: auto_safe` | What OpenETL does about the pipeline's [incidents](../../get-started/concepts.md#incidents) |
| `heal.mode` | `off`, `suggest`, `auto_safe` or `auto` | No | `auto_safe` | `off` opens incidents and notifies, and takes no action. `suggest` proposes the actions that hold data back, such as pausing the pipeline, rather than taking them. `auto_safe` applies every playbook's actions. `auto` is the same as `auto_safe`. |

## Resources

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `resources` | object | No | Sized automatically, in the `default` pool | How much CPU and memory the pipeline's jobs get, and which runners run them |
| `resources.size` | job size, or `auto` | No | `auto` | Each job's size. `auto` takes it from the connectors' hints, and then from the job's measured peaks; with neither, it is `s`. |
| `resources.min` | job size | No | None | The smallest size `auto` chooses |
| `resources.max` | job size | No | `xl` with `auto`, and otherwise `resources.size` itself | The largest size a job grows to: `auto`'s choice, and a retry after the job runs out of memory |
| `resources.finishWithin` | duration, positive | No | None | Scheduled only, with an automatic size: run as many lanes as the last sync's bytes need, at each lane's measured rate, to finish within this long. |
| `resources.pool` | pool name | No | `default` | The pool of runners that runs the jobs: at most 64 letters, digits, dots, hyphens and underscores, starting with a letter or digit |
| `resources.workers` | integer, 1 to 64 | No | `1` | How many workers each job runs on, each of the job's size. Above 1, a coordinator of size `xs` runs too, so a job takes the workers plus one pod, which the pool's pod launcher makes all or none. A job that cannot run on workers runs on one pod. |

When a job size is an object rather than a preset, both `cpu` and `memory` are required; neither has a default.

### Workers

With `resources.workers` above 1, `openetl plan` shows `workers:` and the count after the job size. A job runs on one pod, and the plan's warnings say why, when:

- the pipeline has a Spark or Flink `transform`;
- the destination declares fewer writer lanes than workers (`maxWriterLanes`, one if unset), is a transactional stream without lane commits (`laneCommits`), loads staged epochs, or is the built-in `parquet`;
- the destination has lane commits, and a stream can hold columns;
- no pod launcher of the pipeline's pool has reported in the last ten minutes, while other runners of its data plane have.

A pipeline with `transform.sql` runs on workers, also into a destination with lane commits. A [group window](../sql-windows.md#group-windows)'s statement sends each row of the stream it reads to the worker that keeps the state of the row's key group, and that worker writes the window's rows: the plan's `partitioning` shows the one exchange, before the statement. Each checkpoint holds every worker's state, and a later attempt restores it on any number of workers and lanes. Streams that hold columns or cells run on workers too, but not into a destination with lane commits. The coordinator decides each stream's schema and what it holds, so every worker writes the same columns and holds the same ones. Only the first worker patches in released holds. A transactional destination with lane commits runs on workers, and on one pod gets one writer lane. [When a pod fails](../../pipelines/workers.md#when-a-pod-fails) says how it recovers an epoch.

Each job's workers read the source's splits between them. A stream with an exchange in the plan's `partitioning` sends each row to the writer lane that owns its key group, on whichever worker that writer lane is. Each of these streams is read on one worker, and the plan warns about each one:

- a stream read with `cdc`, on the same worker as every other stream read with `cdc`, since the source reads their changes from one log;
- a stream written with `overwrite`.

With `transform.sql`, these rules apply to each statement's output and each stream passed through: the stream a statement reads is read on one worker when it is read with `cdc`, or an output from it is written with `overwrite`.

When the control plane cannot check the destination's capabilities or the source's catalog, the plan says so in a note. Then the job's coordinator checks the capabilities before each attempt, and runs the attempt on one pod if it must. The sync's attempts show how many workers each one ran on, and why one ran on one pod. [Run a job on several pods](../../pipelines/workers.md) explains the plan's messages and what happens when a pod fails.

`resources.min` cannot be larger than `resources.size` or `resources.max`, nor `resources.size` larger than `resources.max`. A job size is a preset or amounts:

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
| `source.streams` | list | No | Every stream the source offers, each with the defaults below | The streams to copy. Naming a stream the source does not offer is a problem in the plan. |
| `source.streams[].name` | string | Yes | — | The stream's name, as `openetl discover` lists it. Each stream appears once. |
| `source.streams[].sync` | `full_refresh`, `incremental`, `cdc` or `stream` | No | The first way the source says it reads the stream; in a continuous pipeline, the first of those that is `cdc` or `stream` | How the stream is read. A continuous pipeline's streams are read with `cdc` or `stream`. Each new read with `full_refresh` starts with no state, also the first one after a change from `incremental`. After a change to `incremental`, the next read starts from the state that the last read saved when it completed, if it saved one. If the stream's read has not completed when `sync` changes to or from `full_refresh`, the next sync does not resume that read: it starts a new one, with no state. Rows that the unfinished read wrote with `append` are then written again. |
| `source.streams[].cursor` | column name | When the read mode needs a cursor and the source supplies none | The stream's cursor, if the source names one | The column an `incremental` read goes by, such as `updated_at`. An incremental stream without a cursor is a problem in the plan, and so is a stream with a primary key that is read with `stream`, whose cursor tells one event of a key from the next. In a stream deduped at apply, any two changes of a row must give it different cursor values. A date, or a timestamp stored in whole seconds, does not. The plan warns where the catalog shows such a type. See [Choose a cursor that changes on every update](../../pipelines/delivery.md#choose-a-cursor-that-changes-on-every-update). |
| `source.streams[].timestamps` | `microsecond` or `nanosecond` | No | `microsecond` | The unit timestamps arrive in. Finer values are truncated. Nanoseconds need a destination that keeps them, and hold only 1677-09-21 to 2262-04-11. A Spark transform's inputs cannot keep nanoseconds. |
| `source.streams[].require` | delivery guarantee | No | `delivery.require` | The lowest guarantee this stream may have, in place of `delivery.require` |
| `source.streams[].collapse` | boolean | No | `true` for a stream whose reads may repeat rows (the plan's reason says `superset replay`) and that requires more than `at_least_once`; otherwise `false` | For a stream without a primary key: rows with the same content become one row. A stream with a primary key ignores it, and the plan warns. |
| `source.streams[].unfitValues` | `quarantine` or `hold` | No | `quarantine` | What a value that does not fit its column's type does. `quarantine`: OpenETL does not write the row, and quarantines it with the reason. `hold`: OpenETL writes the row and holds the value's cell, where it can hold the cell. Where it cannot, it quarantines the row ([When hold still quarantines a row](../../pipelines/holds-and-quarantine.md#when-hold-still-quarantines-a-row)). |
| `source.streams[].held` | `null`, `value` or `last` | No | `null`, or `value` when `heldValue` is set | What a held cell shows, in the columns that `columns` does not set it for: `null`; `value`, the value in `heldValue`; or `last`, the value the destination already has, which needs merge. |
| `source.streams[].heldValue` | any value | Yes with `held: value` | None | With `held: value`, what held cells show; with `held: last`, what they show in rows the destination does not have yet. A value of the column's delivered type. Set alone, it means `held: value`. `held: null` takes none. |
| `source.streams[].groups` | list of lists of column names | No | None | Columns held together, by their source names: holding one holds the rest. Each group has two columns or more. |
| `source.streams[].typeChanges` | `refuse` or `allow` | No | `destination.typeChanges` | `destination.typeChanges` for this stream alone. With a SQL transform, for the stream as the destination gets it ([Settings and holds of outputs](#settings-and-holds-of-outputs)). |
| `source.streams[].eventTime` | column name | No | None: a row's time is when the engine reads it, and the stream has no watermark | The column that holds a row's event time, for a SQL transform's statements, and that the stream's watermark follows ([Event time and watermarks](#event-time-and-watermarks)). A [group window](../sql-windows.md#group-windows) or [`OVER`](../sql-over.md#over) over the stream goes by it. Its values are timestamps or dates. It needs `transform.sql`. A column that the source's latest catalog does not list for the stream is a problem in the plan. |
| `source.streams[].watermark` | object | No | `delay: 0s` | How far behind the latest event time the stream's watermark stays. It needs `eventTime`. |
| `source.streams[].watermark.delay` | duration, 0 or more | No | `0s` | Each of the stream's splits has a watermark that trails the latest event time the split has read by this much. |
| `source.streams[].watermark.lateness` | duration, 0 or more | No | `0s` | How long after a group window over the stream closes a row may still come. Such a row writes the window's row again, as an update. A later row is quarantined. A stream with lateness needs `destination.write: merge` for each group window's output over it. A changed value applies to windows that are open or within their lateness, and to new windows. A window whose lateness had passed stays gone, and its late rows are quarantined. `OVER` and a dedupe take no lateness: a stream that `OVER` or a dedupe reads, and no group window does, sets none. |
| `source.streams[].dedupe` | object | No | None | How a SQL transform's [dedupe](../sql-over.md#dedupe) over the stream that keeps each key's first row, `ROW_NUMBER() … = 1` in ascending order of the event time, remembers a key. It needs `eventTime` and `transform.sql`. Such a dedupe over a stream without it is a problem in the plan, and a stream that sets it and has no such dedupe over it is a problem too. |
| `source.streams[].dedupe.retention` | duration more than zero, or `unbounded` | Yes, in `dedupe` | None | How long, in event time, the dedupe remembers a key after the key's first row. A key is forgotten once the stream's watermark passes the first row's time plus the retention, and the key's next row is written again. A retention under 1µs, the unit that event time counts in, is a problem in the plan. Into a destination that delivers and acknowledges, the plan reports the dedupe's output as `at_least_once`. Into a destination that stages and applies, or that writes each commit in a transaction, it reports: for a stream read with `full_refresh`, whatever the retention and the replay, `exactly_once` overwritten, `effectively_once` merged into a destination that stages and applies, and otherwise `at_least_once`; for another read of a stream that can repeat rows, with a duration, `exactly_once_within`, which the default `delivery.require` refuses, or `effectively_once` if it is merged into a destination that stages and applies. `unbounded` remembers every key, and the dedupe's state grows with the number of keys. A changed value applies to every key kept, and a key that was forgotten stays forgotten. Keys kept while it was `unbounded` stay kept. |
| `source.streams[].columns` | map of column names to objects | No | None | Settings for single columns, by their source names |
| `source.streams[].columns.NAME` | object | No | None | One column's settings. It sets a `type`, a `held` or a `heldValue`, or more than one. |
| `source.streams[].columns.NAME.type` | `string` or `float64` | No | The decimal | For a column the source declares as a decimal: `string` delivers the source's text for the value, NaN included; `float64` delivers a double, which can lose digits, and keeps NaN and infinities. Setting it on any other column is a problem in the plan. |
| `source.streams[].columns.NAME.held` | `null`, `value` or `last` | No | The stream's `held` | What this column's held cells show, as the stream's `held` |
| `source.streams[].columns.NAME.heldValue` | any value | Yes with this column's `held: value` | The stream's `heldValue` | As the stream's `heldValue`, for this column |

The plan checks hold requirements:

- `unfitValues: hold` and the `held`, `heldValue` and `groups` settings require a primary key. The destination must support patching for the selected write mode. These requirements also apply to column-level settings.
- `held: last` additionally requires merge writes and destination support for preserving the last value.
- The streams of a pipeline with a Spark or Flink transform cannot set `unfitValues: hold`, `held`, `heldValue`, `groups`, `collapse` or `typeChanges`. Their columns cannot set `held` or `heldValue`.
- With a SQL transform, these settings apply to the stream as the destination gets it. [Settings and holds of outputs](#settings-and-holds-of-outputs) explains what that is, and what the plan refuses.

## Destination

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `destination` | object | Yes | — | Where and how to write |
| `destination.ref` | name | Yes | — | The `Destination` to write to, by its name in the same workspace |
| `destination.namespace` | string | No | The destination's default | The schema or dataset to write into |
| `destination.tables` | map of stream names to names | No | Each stream's own name, through `destination.naming` | Streams' names in the destination, exactly as written: a table, or what the destination has instead, such as a topic. A stream left out goes by its own name. Two streams cannot share one. It cannot be used with a Spark or Flink transform, whose outputs name their tables. With a SQL transform, it names the outputs that statements write and the streams that pass through. A name that is neither is a problem in the plan, once the statements are planned, and fails a sync with `CONFIG_INVALID`; until the statements are planned, the plan checks only each table's length. Changing one on a running pipeline leaves what was delivered in the old table, and the new one starts with what is read next; `openetl reset --stream` reads it all again. |
| `destination.write` | `append`, `overwrite` or `merge` | No | For each stream, where the destination has the mode: `overwrite` for a stream read with `full_refresh`, and `merge` for a stream with a primary key read with `cdc`; `append` otherwise | How every stream is written. `merge` needs streams with a primary key, and `overwrite` streams read with `full_refresh`. With `overwrite`, each read replaces the table. Each new read starts with no state, so the source reads the whole stream, whatever position its last read saved. If a stream's read has not completed when its write mode changes to or from `overwrite`, the next sync does not resume that read: it starts a new one, with no state. A read with no rows empties the table, or creates it empty, if the source declares the stream's column types, its cursor's among them, or the stream has had rows before. Otherwise, as for a transform's output that has never had rows, the destination is left as it is. |
| `destination.deletes` | `soft` or `hard` | No | `soft` | How merge applies a delete. `soft` keeps the row, with `_openetl_deleted` true and its other columns as they were; `hard` removes it. Other write modes do not apply deletes, and the plan warns. |
| `destination.naming` | `normalized` or `source` | No | `normalized` | How tables and columns are named. `normalized` names are lowercase, with anything outside `a-z`, `0-9` and `_` replaced; `source` names are the source's, quoted. |
| `destination.typeChanges` | `refuse` or `allow` | No | `refuse` | Whether the destination changes a column's type itself when the stream's type widens, for every stream that does not say. With `refuse`, the destination refuses, and the column is held, or the stream stops, until a person changes the column. With `allow`, the destination changes the type in place, which on a database can rewrite the table under a lock. |

## Schema

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `schemaPolicy` | `allow_all`, `allow_columns`, `block` or `ignore_new` | No | `allow_all` | What a change to a stream's schema does, once the destination has the stream: see the table below. |
| `types` | object | No | `infinity: clamp` | How values map to types where the pipeline has a choice |
| `types.infinity` | `clamp` or `unfit` | No | `clamp` | What becomes of infinite dates and timestamps: `clamp` turns them into the type's largest or smallest value. `unfit` makes them values that do not fit, with the reason `range`. Each stream's `unfitValues` then quarantines or holds them. |

| Policy | A new column | A type that widens | A type that changes otherwise |
|---|---|---|---|
| `allow_all` | Added | Widened | Delivered to a new column, `COLUMN__v2` |
| `allow_columns` | Added | Widened | Held, or where the column cannot be held, fails the sync with `SCHEMA_DRIFT` |
| `block` | Held, or fails the sync | Held, or fails the sync | Held, or fails the sync |
| `ignore_new` | Dropped | Values cast to the column's type | Values cast to the column's type |

Some columns cannot go to a new column, because their rows would have a null key or a null `_openetl_seq` from then on:

- a column of the primary key;
- the cursor of a stream with a primary key, unless the stream is read with `cdc`, which takes `_openetl_seq` from the source's change log.

A change to such a column's type fails the sync with `SCHEMA_DRIFT`, under `allow_all` too, unless it is to a wider integer, to a decimal with more digits at the same scale, or from JSON to a string. [When a key or cursor changes type](../../pipelines/schema-changes.md#when-a-key-or-cursor-changes-type) lists what passes and why. Under `ignore_new`, once the policy applies, the values are cast to the column's type instead. A reset does not clear the error, because the stream keeps its schema. Change the column back at the source. To keep the new type, delete the pipeline and apply it again with a new destination table. A stream whose key or cursor went to a new column in an earlier sync fails in the same way. For a transform's output, the error also names the statement that writes the output, as for [its other schema changes](#schema-changes).

A policy applies to a stream's rows after the first checkpoint that records a schema with the stream's columns, on every lane and every worker. That schema is the stream's first schema. If a sync restores a checkpoint that has the stream's columns, the policy applies from the start of the sync. A restored checkpoint without the stream's columns does not start the policy.

On workers, the policy starts in two ways:

- When a worker's lanes pass a checkpoint's barrier with the stream's columns, the worker applies the policy to its own rows from then on, by its own schema.
- The coordinator records the first schema as it merges the workers' parts of the first checkpoint whose schema has the stream's columns. If the coordinator accepts a change before that merge, the change is part of the first schema, also a change from rows after that checkpoint. The coordinator then sends the first schema to every worker, and does not wait for the workers to receive it. A worker applies the policy by the first schema once it receives it, also a worker that has read no rows of the stream.

Until a worker that applies the policy by its own schema receives the first schema, its schema can lack another worker's accepted change. Under every policy except `ignore_new`, the worker sends each change that its rows make to the coordinator. If the coordinator has a newer schema, the worker shapes the rows again by it. Under `ignore_new`, the worker drops a column that its own schema does not have, and casts values to its own schema's types. So the rows that it shapes in that time can lose a column or a type that the first schema has. The rows that it shapes after it receives the first schema keep them.

Widening lets a column represent a broader range of values. Numeric widening includes these conversions:

| Source type | Wider types |
|---|---|
| `int16` | `int32`, `int64`, `float32`, `float64` |
| `int32` | `int64`, `float64` |
| `int64`, `float32` | `float64` |
| Integer or decimal | A decimal with sufficient precision and scale |

Any type can widen to a string. To prevent integer rounding, a column that has contained values beyond ±2^53 widens to text instead of floating point.

The destination changes an existing column's type only when `typeChanges` permits it. Otherwise, it refuses the change, and OpenETL holds the column or stops the stream. Under `ignore_new`, values that do not fit the existing type follow the stream's `unfitValues`: their rows are quarantined, or their cells held. In a continuous pipeline, an error that would fail a scheduled sync fails the running job.

## Delivery

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `delivery` | object | No | The defaults below | How the pipeline commits, and the guarantee it requires |
| `delivery.require` | delivery guarantee | No | `exactly_once` | The lowest guarantee any stream may have. A stream whose guarantee is lower is a problem in the plan. |
| `delivery.commit` | object | No | The defaults below | When the pipeline commits what it has read |
| `delivery.commit.interval` | duration, positive | No | 5 minutes in a scheduled pipeline; in a continuous one, the destination's own interval, or else 10 seconds | How often to commit. Outside the destination's minimum and maximum, if it has them, it is a problem in the plan. |
| `delivery.commit.maxBytes` | byte count | No | `256MB`, which grows when commits cannot keep up | Commit once this much has been read since the last commit. Once set, it does not grow. |
| `delivery.commit.scope` | `pipeline` or `stream` | No | `pipeline` where the destination can, otherwise `stream` | Commit every stream together (`pipeline`), or each on its own (`stream`). A scope the destination cannot do is a problem in the plan. |
| `delivery.resumePointTimeout` | duration, positive, or `none` | No | `1m` | How long a commit waits for a point the source can resume from exactly, before it commits without one. It commits without one only where the stream keeps its guarantee anyway. `none` waits forever. |

## Quarantined rows

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `quarantine` | object | No | OpenETL keeps the files | What OpenETL does with the files of the pipeline's quarantined rows |
| `quarantine.retention` | duration, positive | No | None: OpenETL never deletes the files | How long OpenETL keeps the files of committed epochs. A run deletes older files when it starts, and every hour while it runs. The control plane stops listing them. |

[Quarantine files](../quarantine-files.md) describes the files, their format, and an object store rule that deletes them instead.

## Transforms

A transform sits between the source and the destination. The source's streams are its inputs, and its outputs are what the destination gets. A pipeline has at most one transform: SQL statements, or your own code, which is Spark in a scheduled pipeline and Flink in a continuous one. The [Spark](../../integrate/spark.md) and [Flink](../../integrate/flink.md) guides cover writing and running your own code.

[A SQL transform](#a-sql-transform) says which statements run, and what they do to keys, changelogs and errors. `openetl plan` and `openetl apply` plan the statements on a runner of the pipeline's data plane. Errors in the SQL are problems in the plan, with their line and column, and `apply` refuses them. When no runner takes the statements, or a stream's columns are not known yet, the plan does not plan them and cannot report their errors: a note says why, and a sync reports them when it starts. [A SQL transform in the plan](../../pipelines/plan-and-apply.md#a-sql-transform-in-the-plan) shows the output and those cases.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `transform` | object | No | None | One of `sql`, `spark` and `flink` |
| `transform.sql` | string, or `{ file: PATH }` | One of `sql`, `spark` or `flink`, when `transform` is set | None | The transform's SQL statements, as text. Each `INSERT INTO OUTPUT SELECT …` statement writes an output, and the statements read the source's streams as tables. With `{ file: PATH }`, `openetl apply` and `openetl plan` read the file, at a path relative to the file that names it, and send its text in place of the path. Other clients, such as the API, send the text. A pipeline version holds the text. Scheduled and continuous pipelines. |
| `transform.passthrough` | `all` or `none` | No | `all` | Which of the streams that no statement writes reach the destination: `all` of them, as without a transform, or `none`. With `none`, the rows of a stream that no statement reads are dropped unchecked, and no sync records its columns. A statement that writes a stream's name replaces that stream, and its output takes the stream's entry in `destination.tables` and its `require`. Only with `sql`. |
| `transform.spark` | object | One of `sql`, `spark` or `flink`, when `transform` is set | None | Runs your openetl-spark `Transform` on each epoch of a sync, on a Spark cluster. Scheduled pipelines only. |
| `transform.spark.cluster` | name | Yes | — | A `SparkCluster` in the same workspace |
| `transform.spark.code` | object | Yes | — | The code: one of `jar` and `python`, and its `sha256`. The runner checks the file and gives the cluster its own copy. |
| `transform.spark.code.jar` | URI | One of `jar` or `python` | None | A jar, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.spark.code.python` | URI | One of `jar` or `python` | None | A Python file that calls `openetl_spark.run` with its transform, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.spark.code.sha256` | 64 lowercase hex digits | Yes | — | The file's SHA-256 |
| `transform.spark.code.class` | class name | Yes with `jar` | — | The class in the jar that implements openetl-spark's `Transform`. A Python file takes none. |
| `transform.spark.conf` | map of strings | No | None | Spark properties for the application, over the cluster's. Each name starts with `spark.`, and is not one the runner sets: `spark.master`, `spark.submit.deployMode`, `spark.app.name`, `spark.yarn.tags`, `spark.kubernetes.driver.pod.name`, or any that starts with `spark.openetl.`. |
| `transform.spark.outputs` | list | No | Each input stream, under its name and with its settings | The transform's outputs, which are the destination's streams |
| `transform.spark.outputs[].name` | string | Yes | — | The output's name. Each output appears once. |
| `transform.spark.outputs[].primaryKey` | list of column names | No | The primary key of the input stream with the output's name, if there is one | The output's primary key |
| `transform.spark.outputs[].write` | `append`, `overwrite` or `merge` | No | `destination.write`; without it, `overwrite` when every input is read with `full_refresh`, and `merge` when the input stream with the output's name is read with `cdc` and the output has a primary key, where the destination has the mode; `append` otherwise | How the output is written. `overwrite` needs every input read with `full_refresh`, and `merge` a primary key. |
| `transform.spark.outputs[].require` | delivery guarantee | No | The `require` of the input stream with the output's name, then `delivery.require` | The lowest guarantee the output may have |
| `transform.flink` | object | One of `sql`, `spark` or `flink`, when `transform` is set | None | Runs your Flink job, built with openetl-flink, between a continuous pipeline's source and destination, on a Flink cluster. The job's checkpoints are the pipeline's. Continuous pipelines only. |
| `transform.flink.cluster` | name | Yes | — | A `FlinkCluster` in the same workspace |
| `transform.flink.code` | object | Yes | — | The job's jar and its `sha256`. The runner checks the file and gives the cluster its own copy. |
| `transform.flink.code.jar` | URI | Yes | — | A jar, at an `s3://`, `s3a://` or `https://` URI the runner can read |
| `transform.flink.code.sha256` | 64 lowercase hex digits | Yes | — | The file's SHA-256 |
| `transform.flink.code.class` | class name | No | The class the jar's manifest names | The job's main class |
| `transform.flink.parallelism` | integer, 0 or more | No | `0`: the cluster's default | The job's parallelism |
| `transform.flink.conf` | map of strings | No | None | Flink configuration for the job, over the cluster's. Each name has a dot in it, and is not one the runner sets, listed below. |
| `transform.flink.outputs` | list | No | Each input stream, under its name and with its settings | As `transform.spark.outputs` |
| `transform.flink.outputs[].name` | string | Yes | — | As `transform.spark.outputs[].name` |
| `transform.flink.outputs[].primaryKey` | list of column names | No | As `transform.spark.outputs[].primaryKey` | As `transform.spark.outputs[].primaryKey` |
| `transform.flink.outputs[].write` | `append`, `overwrite` or `merge` | No | As `transform.spark.outputs[].write` | As `transform.spark.outputs[].write` |
| `transform.flink.outputs[].require` | delivery guarantee | No | As `transform.spark.outputs[].require` | As `transform.spark.outputs[].require` |

The runner controls these Flink options. `transform.flink.conf` cannot override them:

- `parallelism.default`
- `pipeline.name`
- `execution.runtime-mode`
- `execution.target`
- `jobmanager.execution.failover-strategy`
- `kubernetes.cluster-id`
- `kubernetes.namespace`
- `yarn.application.name`
- `yarn.tags`

It also rejects names starting with these prefixes:

- `execution.checkpointing.`
- `state.checkpoints.`
- `state.savepoints.`
- `execution.state-recovery.`
- `execution.savepoint`
- `restart-strategy`
- `high-availability`
- `kubernetes.operator.`
- `openetl.`
- `$internal.`

A Flink transform's inputs must each replay exactly from a resume point, since its checkpoints restore them; the plan says which do not.

## Steps after a sync

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `postSync` | list | No | None | Steps that run in order after a sync's data is committed. Each has one of `dbt` and `spark`. A step that fails fails the sync, and the steps after it do not run. Scheduled pipelines only. |
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

A transform or step uses the cluster's identity to access pipeline storage by default. That identity can access every pipeline's storage prefix. `sparkAccess` and `flinkAccess` provide credentials or a runtime role for the pipeline's applications instead.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `sparkAccess` | object | No | The cluster's own identity | How the pipeline's Spark applications reach its `_spark/` prefix of the data plane's store. It sets `emrRole`, `keys`, or both, and needs a Spark transform or step. |
| `sparkAccess.emrRole` | IAM role ARN | One or both of `emrRole` and `keys`, when `sparkAccess` is set | None | On EMR: the runtime role each step runs as (`ExecutionRoleArn`, EMR 6.7 and later). It is all the step's AWS access, to your own data too. |
| `sparkAccess.keys` | object | One or both of `emrRole` and `keys`, when `sparkAccess` is set | None | Elsewhere: S3A keys for the data plane's bucket alone |
| `sparkAccess.keys.accessKeyId` | string, or secret reference | Yes in `keys` | — | The access key's ID |
| `sparkAccess.keys.secretAccessKey` | secret reference | Yes in `keys` | — | The secret access key, as a secret reference, so the key is not kept in the spec |
| `flinkAccess` | object | No | The cluster's own identity | How the pipeline's Flink job reaches its `_flink/` prefix of the data plane's store, where its code, checkpoints and savepoints are. It sets `emrRole`, `keys`, or both, and needs a Flink transform. |
| `flinkAccess.emrRole` | IAM role ARN | One or both of `emrRole` and `keys`, when `flinkAccess` is set | None | On EMR: the runtime role each step runs as (`ExecutionRoleArn`) |
| `flinkAccess.keys` | object | One or both of `emrRole` and `keys`, when `flinkAccess` is set | None | Elsewhere: S3 keys for the data plane's bucket alone, for Flink's Hadoop S3 filesystem. A session cluster reads its filesystems' settings when it starts, so it cannot take them. |
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

## A SQL transform

A SQL transform runs in the engine, after each stream's rows are typed and before they are written. Its statements read each stream under its source column names, with logical types. Each output gets destination names and metadata columns, as a stream does without a transform. The fields of a SQL transform go in `transform` and in each stream's settings. [`examples/postgres-to-parquet-sql.yaml`](../../../examples/postgres-to-parquet-sql.yaml) is a complete pipeline. [SQL transforms](../sql.md) lists the statements, operators and functions the SQL can use, and [Write a SQL transform](../../pipelines/sql-transforms.md) shows how to write one.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop
spec:
  mode: continuous
  source:
    ref: shop-db
    streams:
      - name: orders
        eventTime: created_at
        watermark: { delay: 5s }
      - name: customers
  destination: { ref: lake }
  transform:
    sql: |
      INSERT INTO customers
      SELECT id, lower(email) AS email, created_at
      FROM customers;
```

To keep the statements in a file of their own, write `sql: { file: transforms/shop.sql }`, with a path relative to the pipeline's file.

### Statements that run

The engine plans the statements each time a sync or a continuous pipeline's job starts.

- It runs row-wise statements: projections and renames, literals, casts, `CASE`, scalar functions, `WHERE` and `UNNEST`, and `CREATE VIEW` over them. Each statement runs on every lane of the job.
- It refuses other statements, such as aggregates, `GROUP BY`, window functions, joins, `DISTINCT`, `ORDER BY` and `LIMIT`. The sync, or the continuous pipeline's job, fails with `CONFIG_INVALID`. The message gives the position of each statement it refuses, as `LINE:COLUMN`.
- A statement reads only streams whose columns are known when the sync starts: the source declares them, as a database source does, or an earlier sync of the pipeline recorded them. If a statement reads a stream whose columns the source infers from its rows, such as `faker`'s, and no sync has recorded them, the sync fails with `CONFIG_INVALID`. Such a stream can still pass through. A column that an earlier sync recorded and the source no longer sends is null.
- `openetl plan` and `openetl apply` plan the statements on a runner, against the columns the source's catalog declares, or where it declares none, those an earlier sync recorded. A sync plans them again when it starts, against both.

An output's primary key is its stream's, if each key column passes through unchanged, renamed or not, and the statement does not use `UNNEST`, which can give several rows for one. Otherwise the output has no primary key, and is appended or overwritten. If `destination.write` is `merge`, each statement must keep its stream's primary key.

A stream that passes through is delivered as it would be without the transform. A statement's output is not deduped when it is applied, so where its stream's reads may repeat rows, it is effectively once if written with `merge`, and at least once otherwise. Both are below the default `delivery.require`, and the plan has a problem, and the sync fails with `CONFIG_INVALID`, unless `delivery.require` allows them, or, for an output that replaces a stream, that stream's `source.streams[].require`.

On a changelog stream, such as one read with `cdc`:

- A statement must keep the stream's primary key, and cannot use `UNNEST`.
- If the stream has no primary key, a statement cannot have a `WHERE` that filters it. The plan has a problem, and the sync fails with `CONFIG_INVALID`. A `WHERE` that planning simplifies to true, such as `WHERE TRUE`, leaves no filter, and is accepted.
- A delete passes without the `WHERE`. The statement's `SELECT` list runs on the values that the source sent with the delete. A column that the source did not send is null before the `SELECT` list runs, so a constant such as `1 AS version` still gives its value.
- If the statement cannot evaluate a delete, or the values it gives do not fit the output's column types, the delete passes with its key alone and its other columns null. A delete is not quarantined. With cell holds, a value that does not fit is held, as it is on a stream that passes through, and the delete passes.
- An insert or update that fails the `WHERE` becomes a delete of its key alone, with its other columns null, which a merge applies whether the table has the row or not. With `destination.deletes: soft`, the default, a row that leaves the filter stays in the table with `_openetl_deleted` true; with `hard`, it leaves the table.
- If an update does not send a column, as Postgres does not send a large value that did not change, an output column that depends only on columns it did not send keeps the value it has, and the statement does not evaluate it.
- The statement cannot evaluate such an update if an output column depends on a column it did not send and on one it sent, or if the `WHERE` reads any column it did not send.
- With a `WHERE`, an update that does not send a column is written in the same way where it passes the `WHERE`. If the row failed the `WHERE` before, the table does not have the current value of an output column that depends only on columns the update did not send, and the row comes back with what the table has: no value if the row never passed the `WHERE` or was deleted with `destination.deletes: hard`, or with `soft`, the value it had when it left the filter. The plan has a note for each statement with a `WHERE` that reads a changelog. To avoid such rows, have the source send whole rows on update, which its connector's page says how to do; or read such tables in a scheduled pipeline with `full_refresh` or `incremental`, which read whole rows; or accept them.

Some rows fail when a statement evaluates them. Examples are a value that a cast cannot convert, and arithmetic whose result does not fit its type. The engine quarantines such a row as one of the output's records, as it quarantines a row whose value does not fit its type. The files are under `_quarantine/_outputs/`, next to the pipeline's checkpoints. The engine writes the rest of the batch. The row's line has the code `statement`. [Quarantine files](../quarantine-files.md) gives the format.

Each output row keeps the `_openetl_id` of the row it comes from. Each row that `UNNEST` gives gets an `_openetl_id` of its own.

### Event time and watermarks

A stream with `eventTime` has a watermark: a time before which no more of the stream's rows are expected, except late ones. The engine keeps a watermark for each of the stream's splits, such as a partition or a key range.

- A split's watermark is the latest event time that the split has read, less `watermark.delay`. It never goes back. A row with a null event time does not move it.
- The engine moves the watermark this way only if the stream is read in the order of its event time: with `cdc` or `stream`, or with `incremental` and `cursor` set to the `eventTime` column. A stream read in another order, such as with `full_refresh`, holds each split's watermark until the split's read ends. The watermark is then the end of input. The next read of such a stream starts with no watermark.
- In a continuous pipeline, a split that has read no rows for 1 minute is idle. Its watermark moves to the current time, less the delay, and keeps moving while the split is idle. If the split waits for a checkpoint's barrier, the move comes after the barrier. A move counts as something the source emitted, so while a split is idle the pipeline commits about one epoch each `delivery.commit.interval`, or each 15 seconds if the interval is shorter, even with no rows.
- When a split of a stream read in event-time order completes its read, its watermark moves to the current time, less the delay, not to the end of input. So a sync of such a stream closes only the windows that end before then, and the next sync goes on with the rest. A run that is not followed by another sync leaves those windows open.
- The stream's watermark in each of the job's lanes is the least of the watermarks of the splits that the lane reads. A lane without splits of the stream does not hold it back. The watermark of a statement's output is the least of the watermarks of the streams the statement reads.
- Each checkpoint records each split's watermark. A job that restores a checkpoint starts each split at the recorded watermark. The next sync's read starts each split at the watermark its read ended with, and a split the source adds at the least of those watermarks.
- If the `eventTime` column holds values other than timestamps or dates, the sync fails with `CONFIG_INVALID`.
- A [group window](../sql-windows.md#group-windows) closes when the watermark reaches the window's end. Rows that come after the watermark has passed their time are late, and `watermark.lateness` says how long a closed window still takes them.

`openetl plan` has a note for each stream with event time. The note names the column and says how the stream's watermark moves. The `openetl.job.watermark` metric gives each stream's watermark, and `openetl.operator.watermark` each group window's ([Monitoring](../../self-host/monitoring.md#stateful-operators)). [Aggregate by windows of time](../../pipelines/windows.md) explains watermarks with examples.

### Settings and holds of outputs

A stream's `unfitValues`, `held`, `heldValue`, `groups`, `collapse`, `typeChanges` and its columns' `held` and `heldValue` apply to the stream as the destination gets it:

- A stream that passes through takes them as it would without a transform.
- The output of a statement that writes a stream's name takes that stream's settings. `groups` and the columns' settings name the output's columns.
- An output of another name takes the defaults and `destination.typeChanges`.

The plan has a problem, and a sync fails with `CONFIG_INVALID`, for each of these cases:

- A stream sets one of these settings, no statement writes its name, and `transform.passthrough` is `none`. The destination does not get the stream.
- A stream sets `collapse`, and a statement writes its name. The engine does not collapse a statement's output.
- A stream sets `unfitValues: hold`, `held`, `heldValue` or `groups`, and the statement that writes its name gives its output no primary key. The hold requirements above apply to the output, with its write mode.

An output holds as a stream without a transform does. It needs a primary key and a destination that patches its write mode. If a column's type changes and `schemaPolicy` or the destination does not take the change, the engine holds the column. The output's other columns are still written. A new column follows `schemaPolicy`, as in [the table above](#schema): `allow_all` and `allow_columns` add it, `block` holds it, and `ignore_new` drops it. The engine also holds a new column that the destination refuses, or that a destination with no room for more columns cannot add. `openetl holds` lists the hold under the output's name, with the ways to clear it, and `openetl holds release NAME OUTPUT COLUMN` releases it.

With `unfitValues: hold`, a value that does not fit its type holds its cell. This applies where the stream is typed, and where `schemaPolicy: ignore_new` casts an output's values to its columns' types. A statement cannot evaluate a row whose stream holds a cell of a column that the statement reads. The engine quarantines that row as one of the statement output's records.

### Schema changes

If a stream's schema changes during a sync, the engine plans the statements that read it again. A new column reaches an output only through `*`. An output's schema changes under `schemaPolicy`, as a stream's does.

These changes fail the sync with `SCHEMA_DRIFT`:

- A change that a statement cannot take, such as a column whose new type a function does not accept. The message names each column the statement reads whose type changed, and the cast that reads it as before, such as `CAST(email AS VARCHAR)`.
- A change that would change an output's primary key.
- A change to the type of an output's primary key column that a stream's key does not take, [as for a stream](#schema).
- A change to an output column's type that `schemaPolicy` does not take, where the output cannot hold the column. The message names the statement that writes the output, and the cast that keeps the column's type, such as `CAST(… AS BIGINT) AS amount`.
- A change that the destination refuses, where the output cannot hold the column. The message gives the statement to run on the destination, as for a stream without a transform.
