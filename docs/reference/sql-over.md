---
title: SQL OVER
description: Look up the functions, frames, ordering, state and query changes of OVER.
---

This reference describes what OVER does. For a procedure, see [Compute running totals with OVER](../pipelines/over.md).

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
- `ORDER BY` is the event-time column alone, as the statement reads it, in ascending order.
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
| `lag(X)`, `lag(X, N)`, `lag(X, N, DEFAULT)` | `X` on the row `N` rows before the current one, 1 by default, or `DEFAULT` where there is none, null by default. `N` is a constant whole number, 1 or more, and `DEFAULT` a constant. `lag` takes no frame. |

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

The operator's ID is a hash of what it computes and keeps: the output's name, the stream, the event-time column and its type, the `PARTITION BY` columns and their types, each function with its argument, `FILTER`, frame and types, the filters and `UNNEST` before it, and its state's layout. These edits keep the ID:

- Formatting, comments, other names for columns, aliases and views that rename columns.
- The order of the `PARTITION BY` columns and of the functions.
- Columns that pass through, added or removed, also when the source adds a column that `SELECT *` takes. A row that waited in state before then is written with a null in the new column.
- `QUALIFY`, and filters and projections after `OVER`.
- Other statements, and the order of the statements.

Another output name, `PARTITION BY`, function, argument, `FILTER` or frame, a filter or `UNNEST` before `OVER`, and a change of type of the event-time column, a `PARTITION BY` column or a function's argument give a new ID. The new operator's state starts empty, and the plan shows it as [Changing a query](./sql-windows.md#changing-a-query) describes:

```text
    1:1  running_revenue  over-81fa38d0e7ba7bb2  new  over by created_at on region: lag(amount, 1), sum(amount) range unbounded; keeps each key's frame, and its rows until the watermark passes them; starts empty
```
