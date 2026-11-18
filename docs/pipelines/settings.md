---
title: Pipeline settings by task
description: Look up common pipeline settings for scheduling, stream selection, writing and delivery.
---

Use these tables to find a setting by task. The [Pipeline reference](../reference/specs/pipeline.md) lists every field and requirement.

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
| `schedule.jitter` | Start each sync up to this much later, at random, so pipelines on the same schedule do not all start at once | No delay |
| `resources.size` | How much CPU and memory each job gets: `auto`, `xs` (0.5 vCPU, 1 GiB), `s` (1 vCPU, 2 GiB), `m` (2 vCPUs, 4 GiB), `l` (4 vCPUs, 8 GiB), `xl` (8 vCPUs, 16 GiB), or `{ cpu: 3, memory: 6Gi }` | `auto`: from the connectors' hints, then the job's measured peaks |
| `resources.min`, `resources.max` | The smallest and largest size `auto` may choose; `max` is also what a retry after running out of memory may grow to | `max`: `xl` for `auto`, otherwise the size itself |
| `resources.finishWithin` | Scheduled, with an automatic size: run as many lanes as the last sync's volume needs to finish within this long | |
| `resources.pool` | Which pool of runners runs the jobs | `default` |
| `resources.workers` | How many workers each job runs on, from 1 to 64, each of the job's size. Above 1, a coordinator of size `xs` runs too, so a job takes one pod more than its workers; the pool needs a pod launcher. [Run a job on several pods](./workers.md) explains when they help | `1` |
| `dataPlane` | Which data plane's runners run the jobs | The server's, `default` |
| `heal.mode` | What OpenETL does about the pipeline's incidents: `off` opens them and does nothing else; `suggest` also proposes actions, such as pausing the pipeline or a stream, or syncing less often; `auto_safe` takes them; `auto` does the same as `auto_safe` | `auto_safe` |

[Schedules](./schedules.md) covers when syncs start: intervals, cron time zones, jitter, a sync that runs long, pausing, and time OpenETL was not running.

### Source and streams

| Field | What it does | Unset |
|---|---|---|
| `source.ref` | The `Source` to read | Required |
| `source.streams` | The streams to copy, each by `name` as `openetl discover` lists it, with the settings below | Every stream the source offers |
| `sync` | How the stream is read: `full_refresh`, `incremental`, `cdc` or `stream` | The first way the source says it reads the stream; in a continuous pipeline, the first whose reads do not end |
| `cursor` | The column an incremental read goes by, such as `updated_at` | The source's cursor for the stream, if it names one |
| `columns.NAME.type` | For a column the source declares as a decimal, deliver it as `string` (the source's text) or `float64` (a double, which can lose digits) instead | The decimal |
| `timestamps` | `microsecond`, or `nanosecond` for a destination that keeps them | `microsecond`; finer values are truncated |
| `require` | This stream's lowest acceptable delivery guarantee, overriding `delivery.require` | The pipeline's |
| `collapse` | For a stream without a primary key: rows with the same content become one row | On for a stream that may be read twice and requires more than at least once |
| `unfitValues` | What a value that does not fit its column's type does: `quarantine` its row, which OpenETL does not write, or `hold` its cell, for a keyed stream into a destination that patches. With `hold`, OpenETL still quarantines a row whose value it cannot hold ([Holds and quarantine](./holds-and-quarantine.md)) | `quarantine` |
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
| `schemaPolicy` | What a change to a stream's schema does. `allow_all` adds columns and widens types, and sends a type change that is not a widening to a new column; `allow_columns` adds columns and widens types, but holds other type changes or fails when they cannot be held; `block` holds changes or fails the stream; `ignore_new` drops new columns, and values that do not fit follow `unfitValues` | `allow_all` |
| `delivery.require` | The lowest guarantee a stream may have, unless its own `require` sets another: `at_least_once`, `exactly_once_within`, `effectively_once` or `exactly_once` | `exactly_once` |
| `delivery.commit.interval` | How often to commit what is been read | 5 minutes in a scheduled pipeline; in a continuous one, the destination's own interval, or 10 seconds |
| `delivery.commit.maxBytes` | Commit once this much has been read since the last commit | `256MB`, which grows when commits cannot keep up |
| `delivery.commit.scope` | Commit every stream together (`pipeline`) or each on its own (`stream`) | `pipeline` where the destination can, otherwise `stream` |
| `delivery.resumePointTimeout` | How long a commit waits for a point the source can resume from exactly, before committing without one where the stream keeps its guarantee anyway; `none` waits forever | `1m` |
| `types.infinity` | What becomes of infinite dates and timestamps: `clamp` to the type's largest or smallest value, or `unfit`, which makes them values that do not fit, for `unfitValues` | `clamp` |
| `quarantine.retention` | How long OpenETL keeps the files of quarantined rows, such as `720h`. A run deletes older files ([Quarantine files](../reference/quarantine-files.md)) | Kept |

### After and between

| Field | What it does |
|---|---|
| `postSync` | Steps that run, in order, after each sync's data is committed: `dbt` (a `DbtProject`, with `command` `build`, `run` or `test`, `select`, `exclude` and `schema`) or `spark` (your own Spark application). A step that fails fails the sync. Scheduled pipelines only; see [`examples/faker-to-postgres-dbt.yaml`](../../examples/faker-to-postgres-dbt.yaml) |
| `transform` | What changes the data between the source and the destination: your own code, `spark` in a scheduled pipeline or `flink` in a continuous one; or `sql` statements, which the engine runs in each lane ([reference](../reference/specs/pipeline.md#transforms), [guide](./sql-transforms.md)) |
| `sparkAccess`, `flinkAccess` | How the pipeline's Spark or Flink code reaches its part of the data plane's storage; unset, the cluster's own identity |

[Integrate](../integrate/index.md) covers dbt, Spark and Flink.

[Handle schema changes and holds](./schema-changes.md) walks through a new column, a refused type change, and repairing and releasing the hold.
