---
title: Compute running totals with OVER
description: Give each row a running total, a moving average or the previous row's value with SQL's OVER, and know when each row is written, what happens to late rows, and where the state is kept.
---

An `OVER` statement gives each row of a stream values that come from the rows before it, such as a running total of each customer's orders, or the amount of the customer's previous order. It writes each row once, with its own columns and those values. This page explains how to write one, when each row is written, what happens to rows that come late, and how to change the statement. [Write a SQL transform](./sql-transforms.md) explains statements in general. [OVER](../reference/sql-over.md#over) in the SQL reference lists every function, frame and rule.

## Before you start

- You can write a SQL transform, as [Write a SQL transform](./sql-transforms.md) shows.
- The stream has a column that holds the time of each row, such as `created_at`, and you set it as the stream's `eventTime`. [Event time and watermarks](./windows.md#event-time-and-watermarks) explains event time and the watermark, which `OVER` uses as group windows do.
- The stream is read with `full_refresh`, with `stream`, or with `incremental` and the `eventTime` column as the cursor.

## Write an OVER statement

Give each order its region's revenue so far, and the amount of the region's order before it:

```sql
INSERT INTO running_revenue
SELECT id, region, amount, created_at,
  sum(amount) OVER (PARTITION BY region ORDER BY created_at) AS revenue_so_far,
  lag(amount) OVER (PARTITION BY region ORDER BY created_at) AS previous_amount
FROM orders;
```

- `PARTITION BY` names the columns whose rows go together, here `region`. Without it, every row of the stream goes together.
- `ORDER BY` is the stream's `eventTime` column, and nothing else. The order is ascending.
- Every `OVER` of a statement has the same `PARTITION BY` and `ORDER BY`. Compute others in a statement of their own.
- The output keeps the stream's primary key, here `id`.

The functions are `sum`, `count`, `min`, `max`, `avg`, `lag` and `first_value`. Each function except `lag` reads a frame: the rows of its partition from a start up to the current row. `ORDER BY` alone gives a frame of every row up to the current row's time:

| Frame | Rows it holds |
|---|---|
| `ORDER BY created_at`, or `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` | Every row of the partition before the current row, and every row with the current row's time |
| `ROWS UNBOUNDED PRECEDING` | Every row of the partition before the current row, and the current row |
| `ROWS BETWEEN 2 PRECEDING AND CURRENT ROW` | The 2 rows before the current row, and the current row |
| `RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW` | The rows of the last hour of event time, up to the current row's time |

`lag(amount, 2, 0)` gives the amount of the second row before the current one, or 0 if there is no such row. Frames that look past the current row, `lead` and `rank` are refused. [What's refused](../reference/sql-errors.md#window-functions) lists them.

### Rows with the same time

Rows of one partition with the same event time are **peers**. A `RANGE` frame holds every peer of the current row, so peers get the same value: two orders at 12:00:00 both get the revenue of both. A `ROWS` frame, `lag` and `first_value` take peers one at a time, in the order of their record IDs (`_openetl_id`). That order depends only on the rows, not on the order in which they arrive. So a retry, or a read of the rows again, gives each row the same values.

If the order of peers matters to you, use a `RANGE` frame, or give the rows event times that differ.

## When a row is written

A row waits until the stream's watermark passes its event time. Then every row of that time has come, and the engine writes the row, with its values, in the next epoch that it commits. Each partition's rows are written in event-time order.

Take the statement above with `watermark.delay: 30s`, over a stream with one split that reads orders at 12:00:10, 12:00:50 and 12:01:20. At 12:01:20 the split's watermark is 12:00:50, which passes the order at 12:00:10. So the engine writes that order. The order at 12:00:50 waits until the watermark passes 12:00:50, which happens when the split reads an order after 12:01:20.

At the end of a `full_refresh` read, the watermark moves to the end of input, and every row that waits is written. [How the read moves the watermark](./windows.md#how-the-read-moves-the-watermark) describes each read.

## Late rows

A row is late when the watermark has passed its event time before the row comes. Rows before it in its partition have already been written with their values, so the late row has no place in the order. The engine quarantines it with the code `late`. A row whose event time is null has no place in the order either, and is quarantined with the code `event_time`.

`OVER` takes no `watermark.lateness`. The plan refuses lateness on a stream that `OVER` reads, unless a group window reads the stream too, which takes it. To take rows that come out of order, set a longer `watermark.delay`. A longer delay keeps more rows waiting, and writes each row later.

To list the quarantined rows, run:

```sh
openetl quarantine --stream running_revenue PIPELINE
```

## The output

`OVER` writes each row once, as an insert, so its output is appended to, or overwritten with `full_refresh`. Each row keeps its record ID. Its sequence (`_openetl_seq`) is its event time.

The engine writes each row with the writer lane that owns the key group of its `PARTITION BY` columns. With `destination.write: merge`, a destination with several writer lanes could get two rows of one primary key on two writer lanes. So the plan refuses `merge` for an `OVER` output whose primary key does not hold its `PARTITION BY` columns, unless the destination writes on one writer lane.

## Across syncs

A scheduled pipeline keeps the state of `OVER` in its checkpoint between syncs, as it does a group window's:

- **`full_refresh`.** Each sync reads the whole stream, and writes every row at the end of the read. The next sync starts again from the whole stream.
- **`incremental` by the event time, or `stream`.** A partition's frame goes on in the next sync, so a running total counts the rows of every sync. Rows that wait at the end of a sync are written in a later sync, once the watermark passes their time.

The plan refuses `OVER` over a stream read with `incremental` by another cursor. Each sync's rows would come in their own order, after rows of an earlier sync that come later in time.

## Where the state is kept

`OVER` keeps the rows that wait for the watermark, and each partition's frame: the accumulator of an unbounded frame, and the rows that a bounded frame or `lag` can still reach. The state is kept where a group window's is ([Where the state is kept](./windows.md#where-the-state-is-kept)), with the same checkpoints, files and retention. A `ROWS BETWEEN 1000 PRECEDING AND CURRENT ROW` frame keeps up to 1,000 rows' values for each partition. A stream read in another order than its event time, such as with `full_refresh`, keeps every row that it reads until the read ends.

On [workers](./workers.md#group-windows-on-workers), `OVER` sends each row to the worker that owns its partition's key group, as a group window does. That worker keeps the partition's state and writes its rows.

### Build the state again

To build the state again from the stream, reset the stream that the statement reads:

> [!WARNING]
> A reset reads the stream again for every statement that reads it, and each of their outputs gets the stream's rows again. An appended output then has those rows twice. [Read streams again from the start](./positions.md#read-streams-again-from-the-start) has what the destination gets.

```sh
openetl reset --stream orders PIPELINE
```

## Change the statement

`OVER` keeps its state under an ID that the plan shows, in the `state` section of `transform.sql`:

```text
    1:1  running_revenue  over-81fa38d0e7ba7bb2  new  over by created_at on region: lag(amount, 1), sum(amount) range unbounded; keeps each key's frame, and its rows until the watermark passes them; starts empty
```

Other names for columns, columns that pass through, `QUALIFY` and the order of the functions keep the ID. Another `PARTITION BY`, function, argument or frame gives a new ID, and the new operator's state starts empty: running totals start again from the rows that the pipeline reads after the change. [Changing a query](../reference/sql-over.md#changing-an-over-statement) lists the edits that keep the ID. To build the new state from the stream's start, apply the change, then reset the stream as [Build the state again](#build-the-state-again) describes.

A column that the source adds to the stream keeps the ID too. Rows that waited in state before the change are written with a null in the new column.

## Watch it

The console's **SQL** tab shows, under **At the last checkpoint**, what the statement did in the last epoch: its watermark, the rows it took in and wrote, its late rows and its state's size. The same numbers are the `openetl.operator.*` metrics ([Monitoring](../self-host/monitoring.md#stateful-operators)). If `openetl.operator.rows.late` grows, set a longer `watermark.delay`.
