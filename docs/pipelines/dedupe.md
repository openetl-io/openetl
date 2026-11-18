---
title: Deduplicate rows with ROW_NUMBER
description: Keep each key's first row or its latest row with ROW_NUMBER() … = 1, set how long a key is remembered, and know when each row is written and what the plan guarantees.
---

A dedupe keeps one row for each key of a stream. It is a SQL statement with `ROW_NUMBER() OVER (PARTITION BY … ORDER BY …) = 1`. In ascending order of the event time, it keeps each key's **first** row, such as the first delivery of each event that a queue sends more than once. In descending order, it keeps each key's **latest** row, such as each customer's current profile. This page explains how to write both, when each row is written, how long a key is remembered, and what the plan reports. [Write a SQL transform](./sql-transforms.md) explains statements in general. [Dedupe](../reference/sql-over.md#dedupe) in the SQL reference lists every rule.

## Before you start

- You can write a SQL transform, as [Write a SQL transform](./sql-transforms.md) shows.
- The stream has a column that holds the time of each row, such as `created_at`, and you set it as the stream's `eventTime`. [Event time and watermarks](./windows.md#event-time-and-watermarks) explains event time and the watermark.
- The columns in `PARTITION BY` identify a row, such as an event ID or a customer ID.

## Keep each key's first row

A queue can deliver one event more than once. To write each event once, keep the first row of each `event_id`:

```yaml
source:
  ref: events-queue
  streams:
    - name: events
      eventTime: created_at
      watermark: { delay: 30s }
      dedupe: { retention: 24h }
```

```sql
INSERT INTO events_once
SELECT event_id, kind, amount, created_at
FROM events
QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY created_at) = 1;
```

- `PARTITION BY` names the key, here `event_id`. The statement selects every `PARTITION BY` column, which is the output's primary key.
- `ORDER BY` is the stream's `eventTime` column, and nothing else. Ascending order keeps the first row.
- Compare the number `= 1` in `QUALIFY`, or in the `WHERE` of a `SELECT` that reads it.
- `dedupe.retention` is how long, in event time, the dedupe remembers a key after its first row. It is required, and has no default.

### Set the retention

Set `source.streams[].dedupe.retention` to one of these:

| Retention | What the dedupe does | What the plan reports |
|---|---|---|
| A duration, such as `24h` | It forgets a key once the watermark passes the key's first row's time plus the retention. The key's next row is then written again, as a new first row. | Exactly once within the retention, which it names |
| `unbounded` | It remembers every key. Its state grows with the number of keys. | `exactly_once`, and that the state grows with the number of keys |

Choose a duration that is longer than the time over which the source can repeat a row. A queue that redelivers a message within an hour needs at least `1h`. If the plan finds a first-row dedupe over a stream that sets no retention, it has a problem, and `openetl apply` refuses the pipeline:

```text
  problems:
    transform.sql:1:1: the dedupe writing "events_once", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, and stream "events" sets no dedupe.retention, how long it remembers a key: set source.streams[].dedupe.retention to a duration of event time, such as 24h, after which a key's next row is written again, or to unbounded, to remember every key
```

### When the first row is written

A row of a key that the dedupe has not kept waits until the watermark passes its event time. Until then, an earlier row of the key can still come. When the watermark passes it, the earliest row of the key is written, in the next epoch that the pipeline commits. A row that comes out of order, within the watermark's delay, never makes a later row win.

A row of a key that the dedupe keeps, with a time less than the retention after the key's first row, is a repeat. The dedupe drops it as it comes, and does not keep it in state.

A row with a time at or after the key's first row's time plus the retention is not a repeat, even while the key is still kept. It waits for the watermark as a row of a new key does. It is written once the watermark passes its time, as the key's next first row. By then the watermark has passed the end of the retention, so the key is forgotten. For example, with `retention: 10s`, a key whose first row is at 12:00:25 and the watermark at 12:00:30:

| A row of the key at | What the dedupe does |
|---|---|
| 12:00:31 | Drops it as a repeat, as 12:00:31 is less than 10 seconds after 12:00:25 |
| 12:00:35 | Keeps it in state, and writes it once the watermark passes 12:00:35 |

### Late rows

A row is late when the watermark has passed its event time before the row comes:

- If its key is kept, the late row is a repeat, and the dedupe drops it.
- If its key is not kept, the late row may have been the key's first row, which the output cannot get now. The engine quarantines it with the code `late`. This applies also to a row of a key that the dedupe forgot before the row came. In the example above, without the row at 12:00:35, a row at 12:00:34 that comes after the watermark passes 12:00:35 is quarantined.
- A row whose event time is null is quarantined with the code `event_time`.

A dedupe takes no `watermark.lateness`: the plan refuses lateness on a stream that only a dedupe or `OVER` reads. To take rows that come out of order, set a longer `watermark.delay`.

## Keep each key's latest row

To keep each customer's current profile, keep the latest row of each `customer_id`:

```sql
INSERT INTO latest_profiles
SELECT customer_id, email, plan, updated_at FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM profile_changes
) AS t
WHERE rn = 1;
```

- Descending order keeps the latest row.
- The output is written as upserts keyed by the `PARTITION BY` columns, so it needs `destination.write: merge`. Without `destination.write`, the output is merged where the destination can merge. On another write mode, or a destination that cannot merge, the plan has a problem.
- A row is written as it comes, if it is later than the key's latest row. An older row that comes after a newer one never replaces it.
- A latest-row dedupe takes no retention. It keeps each key's latest time for good, so its state grows with the number of keys. The plan reports `exactly_once`, and that cost. A stream that sets `dedupe.retention` and has no first-row dedupe over it is a problem in the plan.

### A filter after the latest row

A filter beside `rn = 1` runs on each key's latest row. The output holds a key's row only where its latest row passes the filter:

```sql
INSERT INTO paying_profiles
SELECT customer_id, email, plan, updated_at FROM (
  SELECT *, row_number() OVER (PARTITION BY customer_id ORDER BY updated_at DESC) AS rn
  FROM profile_changes
) AS t
WHERE rn = 1 AND plan <> 'free';
```

If a customer's latest row has `plan = 'free'`, and the output has the customer's row, the dedupe writes a delete of the customer's key. The delete has the key's columns, and nulls in the others. `destination.deletes` says how the destination applies it. If the filter cannot evaluate a row, the engine quarantines it with the code `statement`, and the output keeps the row it had.

## Rows with the same time

Rows of one key with the same event time go by their record IDs (`_openetl_id`), and rows with one ID by their values. A first-row dedupe keeps the least, and a latest-row dedupe the greatest. The order depends only on the rows, not on the order in which they arrive. So a retry, or a read of the rows again, keeps the same row. If the order of such rows matters to you, give them event times that differ.

## The output

| Dedupe | Rows | Write modes | `_openetl_id` | `_openetl_seq` |
|---|---|---|---|---|
| First row | Each key's first row, once for each retention, as an insert | Append, merge, or overwrite with `full_refresh` | The input row's | Its event time |
| Latest row | Each key's latest row, as an insert, then updates, and a delete where a filter drops it | Merge | The input row's | Its event time |

The dedupe drops a row that the source repeats after a failure, as it drops any repeat. So the plan reports its output as written exactly once, also over a source that repeats rows, and names the retention. This applies to a destination that stages and applies, or that writes each commit in a transaction:

```text
  streams:
    events_once  append  exactly_once  exactly once within 24h0m0s of event time: a dedupe's output over superset inputs, which remembers a key 24h0m0s from its first row and writes a repeat after that again
```

[The plan's reasons](./delivery.md#the-plans-reasons) lists the guarantees and their reasons.

## Across syncs

A scheduled pipeline keeps the dedupe's state in its checkpoint between syncs:

- **`full_refresh`.** Each sync reads the whole stream, and a first-row dedupe writes every key's first row at the end of the read. The next sync starts again from the whole stream, with no keys kept.
- **`incremental` by the event time, or `stream`.** The kept keys go on in the next sync, so a key written in one sync is a repeat in the next.

The plan refuses a first-row dedupe over a stream read with `incremental` by another cursor: each sync would start its keys again, and write each key's first row again. A latest-row dedupe takes any read, as it compares each row with its key's latest. A stream read with `cdc` is refused, as it is for `OVER`.

## Where the state is kept

A first-row dedupe keeps each kept key with its first row's time, and the rows that wait for the watermark. A latest-row dedupe keeps each key's latest time and record ID. The state is kept where a group window's is ([Where the state is kept](./windows.md#where-the-state-is-kept)), with the same checkpoints, files and retention.

On [workers](./workers.md#group-windows-on-workers), a dedupe sends each row to the worker that owns its key's key group, as `OVER` does. That worker keeps the key's state and writes its rows.

## Change the statement

A dedupe keeps its state under an ID that the plan shows, in the `state` section of `transform.sql`:

```text
    1:1  events_once  dedupe-811f1c1cde3a7c1a  new  dedupe first by created_at on event_id; remembers a key 24h0m0s of event time from its first row: exactly once within 24h0m0s of event time; starts empty
```

Other names for columns, columns that pass through, filters after the dedupe and the retention keep the ID. Another `PARTITION BY`, order, event-time column or filter before the dedupe gives a new ID, and the new dedupe's state starts empty: a first-row dedupe writes each key's next row again. [Changing a dedupe](../reference/sql-over.md#changing-a-dedupe) lists the edits.

A changed retention applies to every key that the dedupe keeps, once the new version runs. A key that the old retention forgot stays forgotten. Keys kept while the retention was `unbounded` stay kept for good after a change to a duration. To forget them, reset the stream that the statement reads:

> [!WARNING]
> A reset reads the stream again for every statement that reads it. An appended output then gets the stream's rows again. [Read streams again from the start](./positions.md#read-streams-again-from-the-start) has what the destination gets.

```sh
openetl reset --stream events PIPELINE
```

## Watch it

The console's **SQL** tab shows, under **At the last checkpoint**, what the statement did in the last epoch: its watermark, the keys it keeps, the rows it took in and wrote, the repeats it dropped and its late rows. The same numbers are the `openetl.operator.*` metrics ([Monitoring](../self-host/monitoring.md#stateful-operators)): `openetl.operator.rows.repeats` counts the rows a dedupe dropped, and `openetl.operator.windows` the keys it keeps. If `openetl.operator.rows.late` grows, set a longer `watermark.delay`.
