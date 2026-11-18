---
title: SQL OVER
description: Look up the functions, frames, ordering, state and query changes of OVER, and of dedupe by ROW_NUMBER() … = 1.
---

This reference describes what OVER and dedupe do. For a procedure, see [Compute running totals with OVER](../pipelines/over.md) and [Deduplicate rows with ROW_NUMBER](../pipelines/dedupe.md).

## OVER

`OVER` gives each row of a stream the values of functions over the rows before it in its partition, in event-time order. [Compute running totals with OVER](../pipelines/over.md) is the guide to it. Each input row gives one output row, with its own columns and the functions' values:

```sql
INSERT INTO running_revenue
SELECT id, region, amount, created_at,
  sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS revenue_so_far,
  lag(amount) OVER (PARTITION BY region ORDER BY created_at) AS previous_amount
FROM orders;
```

- The stream has an event time: its `eventTime` in the [Pipeline](./specs/pipeline.md#source-and-streams).
- `ORDER BY` is the event-time column alone, as the statement reads it, in ascending order. A [dedupe](#dedupe) also takes descending order.
- `PARTITION BY` names columns of the stream, renamed or not. A computed column, such as `lower(region)`, and a list or a struct are refused. Without `PARTITION BY`, every row is in one partition.
- Every `OVER` of a statement has the same `PARTITION BY` and `ORDER BY`, in any order of the columns. One operator keeps their state.
- `OVER` is in the `SELECT` list or in `QUALIFY` of the statement's own `SELECT`, or of a subquery in its `FROM`. It reads the stream or a view of it, and the view is row-wise. `OVER` in a view, `WINDOW` and `OVER` over another `OVER`'s values are refused.
- `WHERE` runs before `OVER`, so it cannot read `OVER`'s values. Compare them in `QUALIFY`, or in the `WHERE` of a `SELECT` that reads the statement's.
- The stream is read whole, with `full_refresh`, or in the order of its event time: with `incremental` and its `eventTime` as the cursor, or with `stream`. [Reads](#over-reads) says why.
- A changelog stream, such as one read with `cdc`, is refused: `OVER` reads appended rows.

### Functions

| Function | Value |
|---|---|
| `count(*)`, `count(X)` | The frame's rows, or those where `X` is not null, as `BIGINT`. `X` is of any type, a list too. |
| `sum(X)`, `avg(X)` | As the [aggregates](./sql-windows.md#aggregates) of a group window give them, over the frame's rows |
| `min(X)`, `max(X)` | The frame's least or greatest value of a number, text, binary, a boolean, a date, a time, a timestamp or a UUID |
| `first_value(X)` | `X` on the frame's first row. It takes the types that `min` takes. |
| `lag(X)`, `lag(X, N)`, `lag(X, N, DEFAULT)` | `X` on the row `N` rows before the current one, 1 by default, or `DEFAULT` where there is none, null by default. `N` is a constant whole number, 1 or more, and `DEFAULT` a constant. `lag` takes no frame: a frame written on it is ignored, and keeps the operator's ID. |

- `mean` is another name for `avg`.
- `FILTER (WHERE …)` after an aggregate counts only the frame's rows where its condition is true, and evaluates the aggregate's argument only on those rows. A row it leaves out still takes its place in a `ROWS` frame.
- `DISTINCT` in `count`, `sum` or `avg`, `IGNORE NULLS`, and `FILTER` on `lag` or `first_value` are refused.
- If a function's value does not fit its type, such as a sum past the range of `BIGINT`, the engine quarantines the row instead of writing it, with the code `aggregate`. The row still counts in its partition's frames.
- A row that a function cannot evaluate is quarantined, with the code `statement`, and has no place in any frame.

### Frames

A frame ends at the current row:

| Frame | Rows |
|---|---|
| `ORDER BY` alone, or `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | Every row of the partition up to the current row's time, its peers included |
| `ROWS UNBOUNDED PRECEDING` | Every row of the partition up to the current row |
| `ROWS BETWEEN N PRECEDING AND CURRENT ROW`, or `ROWS N PRECEDING` | The `N` rows before the current row, and the current row. `ROWS BETWEEN CURRENT ROW AND CURRENT ROW` is the current row. |
| `RANGE BETWEEN CURRENT ROW AND CURRENT ROW` | The current row and its peers: the partition's rows at the current row's time |
| `RANGE BETWEEN INTERVAL PRECEDING AND CURRENT ROW` | The rows whose time is at most `INTERVAL` before the current row's, up to the current row's time, its peers included. `INTERVAL` is a constant of days, hours, minutes, seconds or smaller, in whole microseconds, such as `INTERVAL '1 hour'`. Months are refused. |

`GROUPS` frames, frames that end before or after the current row, and frames that start after it are refused.

### Order and peers

A partition's rows are in the order of their event time. Rows of one partition with the same event time are peers:

- A `RANGE` frame holds every peer of the current row, so peers get the same values.
- A `ROWS` frame, `lag` and `first_value` take peers in the order of their record IDs (`_openetl_id`), then of their values. The order depends only on the rows, so a retry or a redo gives each row the values it had.
- A row whose event time is null is quarantined, with the code `event_time`.

### When a row is written

A row waits until the stream's watermark passes its event time, so every row of its time has come. The engine then writes the row, in the epoch of the next checkpoint. [Event time and watermarks](./specs/pipeline.md#event-time-and-watermarks) says how the watermark moves.

- The watermark of `OVER` is the least watermark of the lanes that read the stream, as a group window's is.
- A row is late if it comes after the watermark has passed its event time. The engine quarantines it, with the code `late`.
- `OVER` takes no `watermark.lateness`. Lateness on a stream that `OVER` reads is a problem in the plan, unless a group window reads the stream too.
- At the end of input, every row that waits is written.
- After a restore, the watermark starts where the checkpoint's watermarks are. A row behind it is late.

### OVER reads

| Read | Pipeline | When rows are written |
|---|---|---|
| `full_refresh` | Scheduled | The watermark holds until the sync's read ends, and every row is written then. The next sync starts each partition's frames again, from the whole stream. |
| `incremental` by its `eventTime`, or `stream` | Scheduled | The watermark follows the rows as they come. At the end of the sync's read, it moves to the current time, less `watermark.delay`. Frames and waiting rows go on in the next sync, which restores them. |
| `stream` | Continuous | The watermark follows the rows as they come. Each row is written while the pipeline runs. |

`OVER` over a stream read with `incremental` and a cursor other than its `eventTime` is a problem in the plan: each sync's rows would come in their own order, after rows of an earlier sync that come later in time.

### The output

- The output keeps the stream's primary key, by the rule of [Primary keys](./sql.md#primary-keys).
- Each row is an insert, written once. Its `_openetl_id` is its input row's, and its `_openetl_seq` is its event time.
- Without `destination.write`, the output is appended to, or overwritten when its stream is read with `full_refresh`.
- The engine writes each row with the writer lane that owns the key group of its `PARTITION BY` columns. With `destination.write: merge`, the output's primary key holds the `PARTITION BY` columns, or the destination writes on one writer lane. Otherwise the plan has a problem.

### OVER state

`OVER` keeps state by key group: the rows that wait for the watermark, and each partition's frame. `openetl plan` shows the statement's operator, which keeps the state:

```text
    1:1  INSERT INTO running_revenue  over  key id  exchange by region  reads orders; keeps state as operator over-81fa38d0e7ba7bb2
```

- The engine exchanges the stream's rows by the key groups of the `PARTITION BY` columns, so each partition's rows reach one lane. `OVER` without `PARTITION BY` runs on one lane.
- An unbounded frame keeps an accumulator, and `first_value` the first row's value. A `ROWS N PRECEDING` frame keeps `N` rows' values, a `RANGE` frame the values within its interval of the latest time, and `lag(X, N)` `N` rows' values.
- A row waits in state until it is written, with all of its columns.
- The state is kept as a group window's is, in the same files, checkpoints and retention ([State](./sql-windows.md#state)). The next attempt restores it on any number of lanes and workers.
- Its lanes write their changes to files early, keep what they read in the disk cache, and count against the job's memory as a group window's do. If the state does not fit, the run fails with `RESOURCE_EXHAUSTED`: `the OVER statement writing OUTPUT keeps more state in memory than the job's size allows`. Run the pipeline with a larger `resources.size`.
- With `resources.workers` above 1, the exchange sends each row to the worker whose lane owns its key group, and that worker writes the row ([Group windows on workers](../pipelines/workers.md#group-windows-on-workers)).
- To build the state again from the stream, reset the streams that the statement reads. A reset reads the stream again for every statement that reads it, so an appended output gets those rows twice. Read [Build the state again](../pipelines/over.md#build-the-state-again) first, then run `openetl reset --stream STREAM PIPELINE`.

### Changing an OVER statement

The operator's ID is a hash of what it computes and keeps: the output's name, the stream, the event-time column and its type, the `PARTITION BY` columns and their types, each function with its argument, `FILTER`, frame and types (`lag` has no frame), the filters and `UNNEST` before it, and its state's layout. These edits keep the ID:

- Formatting, comments, other names for columns, aliases and views that rename columns.
- The order of the `PARTITION BY` columns and of the functions.
- Columns that pass through, added or removed, also when the source adds a column that `SELECT *` takes. A row that waited in state before then is written with a null in the new column.
- `QUALIFY`, and filters and projections after `OVER`.
- Other statements, and the order of the statements.

Another output name, `PARTITION BY`, function, argument, `FILTER` or frame (but a frame written on `lag`, which `lag` ignores), a filter or `UNNEST` before `OVER`, and a change of type of the event-time column, a `PARTITION BY` column or a function's argument give a new ID. The new operator's state starts empty, and the plan shows it as [Changing a query](./sql-windows.md#changing-a-query) describes:

```text
    1:1  running_revenue  over-81fa38d0e7ba7bb2  new  over by created_at on region: lag(amount, 1), sum(amount) range unbounded; keeps each key's frame, and its rows until the watermark passes them; starts empty
```

## Dedupe

`ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) = 1` keeps one row for each key of `PARTITION BY`: the first row in ascending order of the event time, or the latest in descending order. [Deduplicate rows with ROW_NUMBER](../pipelines/dedupe.md) is the guide to it.

```sql
INSERT INTO events_once
SELECT event_id, kind, amount, created_at
FROM events
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY created_at) = 1;
```

- The stream has an event time, as for `OVER`. `ORDER BY` is the event-time column alone, ascending or with `DESC`.
- `PARTITION BY` is required, and names columns of the stream, renamed or not. The statement selects every `PARTITION BY` column, renamed or not, and they are the output's primary key.
- The number is compared `= 1` in `QUALIFY`, or in the `WHERE` of a `SELECT` that reads it, as a number or through a cast that keeps numbers apart, such as `CAST(… AS VARCHAR) = '1'`. Other comparisons, as in a top-N, are refused as a later tier's.
- `ROW_NUMBER()` takes no arguments, `FILTER` or `DISTINCT`, and no other function `OVER` in the same statement.
- `WHERE` runs before the dedupe, on the stream's rows. A filter beside `= 1`, or in a `SELECT` that reads the dedupe, runs on the rows kept.
- A changelog stream, such as one read with `cdc`, is refused.

### Which row a key keeps

| Order | Keeps | When it is written |
|---|---|---|
| Ascending | The key's first row by event time | Once the watermark passes its event time. A row of a key that the dedupe keeps, less than the retention after the key's first row, is a repeat, and is dropped as it comes. |
| Descending | The key's latest row by event time | As it comes, if it is later than the key's latest row. An earlier row, or one with the same time and a lesser record ID, is dropped, whatever the watermark. |

- A key's rows with the same event time go by their record IDs (`_openetl_id`). A first-row dedupe keeps the least, and of rows with one ID, the least by the bytes that the engine encodes their values to. A latest-row dedupe keeps the greatest, and of rows with one ID, the greatest by a 64-bit hash of their values. The order depends only on the rows, so a retry or a redo keeps the same row.
- A row whose event time is null is quarantined, with the code `event_time`.
- A `PARTITION BY` value that is null is a key, as in a group window.
- Equal JSON values are one key, whatever their spaces or the order of their objects' keys.

### Retention

A dedupe that keeps each key's first row needs its stream's `source.streams[].dedupe.retention` in the [Pipeline](./specs/pipeline.md#source-and-streams), which has no default:

| `dedupe.retention` | A kept key is forgotten | The plan reports |
|---|---|---|
| A duration of 1µs or more, such as `24h` | Once the watermark passes the time of the key's first row plus the retention. The key's next row is written again, and the key is kept from it. | `exactly_once_within`, which the default `delivery.require` refuses, over a stream that can repeat rows, read with other than `full_refresh`, into a destination that stages and applies or writes each commit in a transaction, unless merged into one that stages and applies. [The dedupe's output](#the-dedupes-output) lists every case. |
| `unbounded` | Never. The dedupe's state grows with the number of keys. | `exactly_once`, and that the state grows with the number of keys, except where [The dedupe's output](#the-dedupes-output) says otherwise |

- A first-row dedupe over a stream without `dedupe.retention` is a problem in the plan, and a job refuses it with `CONFIG_INVALID`. So is a retention under 1µs, the unit that event time counts in.
- A row of a kept key is a repeat when its time is less than the retention after the key's first row. A row at or after that time waits for the watermark, as a row of a key not kept does, and is written once the watermark passes its time, when the key is forgotten.
- A row that comes after the watermark has passed its time is late. A late row of a kept key is a repeat. A late row of a key not kept, or forgotten before the row came, is quarantined, with the code `late`.
- A dedupe takes no `watermark.lateness`. Lateness on a stream that a dedupe reads, and no group window does, is a problem in the plan.
- A dedupe that keeps each key's latest row takes no retention, and keeps every key. `dedupe.retention` on a stream that no first-row dedupe reads is a problem in the plan.

### Dedupe reads

| Read | Pipeline | A first-row dedupe | A latest-row dedupe |
|---|---|---|---|
| `full_refresh` | Scheduled | The watermark holds until the sync's read ends, and each key's first row is written then. The next sync starts with no keys kept, so it writes every key's first row again. | Each key's latest row, as rows come. The next sync starts with no keys kept, so it writes an upsert of every key's latest row that passes the filters after the dedupe, and a delete of every key whose latest row they drop. |
| `incremental` by its `eventTime`, or `stream` | Scheduled | Kept keys and waiting rows go on in the next sync. | Each key's latest time goes on in the next sync. |
| `incremental` by another cursor | Scheduled | A problem in the plan: each sync's read would start its keys again. | As for `incremental` by its `eventTime`. |
| `stream` | Continuous | Each first row is written while the pipeline runs. | Each latest row is written as it comes. |

### The dedupe's output

- A first-row dedupe's rows are inserts, written once for each key, or again after the retention. Without `destination.write`, the output is appended to, or overwritten when its stream is read with `full_refresh`. `merge` takes it too.
- A latest-row dedupe's rows are upserts keyed by the `PARTITION BY` columns: an insert, then updates. They need `merge`: without `destination.write`, the output is merged where the destination merges, and another write mode is a problem in the plan.
- Where a filter after a latest-row dedupe drops a key's latest row, the dedupe writes a delete of the key, which `destination.deletes` applies, unless it has seen the key and the output has no row of it. A key that it has not seen gets the delete, as the output can have the key's row from before its state began, such as from an earlier `full_refresh` read, a reset or an edit that gave the dedupe a new ID. With `destination.deletes: soft`, a delete of a key that the table does not have can add a row for the key with `_openetl_deleted` true, as the official Postgres and Iceberg destinations do. A key's writes come in the order of their `_openetl_seq`, the event time, so a merge ends with the key's latest row. The delete has the key's columns, and nulls in the others, or their type's zero where a column cannot be null: the steps after the filter do not run on the row. A row that the filter cannot evaluate is quarantined, with the code `statement`, and the output keeps the key's row.
- Each row's `_openetl_id` is its input row's, and its `_openetl_seq` is its event time.
- A dedupe drops what a source repeats after a failure. Into a destination that stages and applies, or that writes each commit in a transaction, its output over a stream read with `full_refresh` is, for every dedupe, whatever its retention and whatever the stream's replay, as each full read starts with no keys kept: `exactly_once` overwritten, `effectively_once` merged into a destination that stages and applies, and otherwise `at_least_once`, appended included.
- Into the same destinations, its output over a stream that can repeat rows, read another way, is:
  - `exactly_once_within` for a first-row dedupe with a retention, with the reason `a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, and writes a repeat after that again`. The default `delivery.require` refuses it: set `require: exactly_once_within`, or `dedupe.retention: unbounded`, which remembers every key from then on but not a key already forgotten ([Accept a dedupe's retention](../pipelines/delivery.md#accept-a-dedupes-retention)).
  - `effectively_once` for the same dedupe written with `merge` into a destination that stages and applies.
  - `exactly_once` for a first-row dedupe with `unbounded`, and for a latest-row dedupe.
- Over streams that all replay exactly, read another way than with `full_refresh`, the output is `exactly_once`, as any transform's output is. Into a destination that delivers and acknowledges, it is `at_least_once` ([The plan's reasons](../pipelines/delivery.md#the-plans-reasons)).

### Dedupe state

The engine exchanges the stream's rows by the key groups of the `PARTITION BY` columns, as for `OVER`. `openetl plan` shows the dedupe in the `state` section, with what it keeps and guarantees:

```text
    1:1  events_once      dedupe-811f1c1cde3a7c1a  new  dedupe first by created_at on event_id; remembers a key 24h0m0s of event time from its first row: exactly_once_within 24h0m0s of event time; starts empty
    6:1  latest_profiles  dedupe-efcfcbf11e9e099f  new  dedupe latest by updated_at on customer_id; keeps each key's latest row's time, and writes it as an upsert: exactly_once, and its state grows with the number of keys; starts empty
```

- A first-row dedupe keeps each kept key with its first row's time, and each row of a key not kept until the watermark passes it, with all of its columns. With a retention, the state of a key is dropped once the key is forgotten.
- A latest-row dedupe keeps each key's latest time, record ID and hash of values, and whether the output has the key's row.
- The state is kept as a group window's is, in the same files, checkpoints and retention ([State](./sql-windows.md#state)). The next attempt restores it on any number of lanes and workers.
- Its lanes write their changes to files early, the kept keys and the waiting rows among them, keep what they read in the disk cache, and count against the job's memory as a group window's do. If the state does not fit, the run fails with `RESOURCE_EXHAUSTED`: `the dedupe writing OUTPUT keeps more state in memory than the job's size allows`. Run the pipeline with a larger `resources.size`.

### Changing a dedupe

The dedupe's ID is a hash of the output's name, the stream, the event-time column and its type, the `PARTITION BY` columns and their types, which row it keeps, the filters and `UNNEST` before it, and its state's layout. These edits keep the ID and the state:

- Formatting, comments, other names for columns, aliases and views that rename columns.
- Columns that pass through, added or removed.
- `QUALIFY` or a subquery, and filters and projections after the dedupe.
- The stream's `dedupe.retention`.
- Other statements, and the order of the statements.

Another `PARTITION BY`, order, output name, filter or `UNNEST` before the dedupe, or a change of type of the event-time column or a `PARTITION BY` column, gives a new ID, whose state starts empty.

A changed `dedupe.retention` applies to every key that the dedupe keeps: a longer one keeps them longer, and a shorter one forgets those it has passed. A key that a retention forgot stays forgotten. Keys kept with `unbounded` stay kept after a change to a duration. To start the state again, reset the streams that the statement reads.

> [!WARNING]
> A reset reads the stream again for every statement that reads it, so an appended output gets those rows twice. Read [Build the state again](../pipelines/over.md#build-the-state-again) first.

```sh
openetl reset --stream STREAM PIPELINE
```
