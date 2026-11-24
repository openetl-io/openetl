---
title: SQL group windows
description: Look up window bounds, aggregates, state and query changes.
---

This reference describes group window semantics. For a procedure, see [Aggregate by windows of time](../pipelines/windows.md).

## Group windows

A group window aggregates a stream's rows by windows of their event time, and by the columns its `GROUP BY` names. [Aggregate by windows of time](../pipelines/windows.md) is the guide to them. It writes one row for each window and key:

```sql
INSERT INTO revenue_by_minute
SELECT window_start, region, sum(amount) AS revenue, count(*) AS orders
FROM orders
GROUP BY tumble(created_at, INTERVAL '1 minute'), region;
```

- The stream has an event time: its `eventTime` in the [Pipeline](./specs/pipeline.md#source-and-streams). The window's first argument is that column, as the statement reads it.
- `GROUP BY` has one window, and any number of the stream's columns, renamed or not. A computed key, such as `lower(region)`, is refused.
- The stream is read whole, with `full_refresh`, or in the order of its event time: with `incremental` and its `eventTime` as the cursor, or with `stream`. A group window over a stream read another way is a problem in the plan: [Reads](#reads) says why.
- A group window is in the statement's own `SELECT`. It reads the stream or a view of it, and the view is row-wise.

### Windows

| Window | Rows it groups |
|---|---|
| `tumble(TIME, SIZE)` | Windows of `SIZE`, one after another from the Unix epoch, 1970-01-01 00:00 UTC. Each row is in one window. |
| `hop(TIME, SLIDE, SIZE)` | Windows of `SIZE` that start every `SLIDE`, from the Unix epoch. Each row is in each window that holds its time: at most `SIZE / SLIDE` of them, rounded up. |
| `session(TIME, GAP)` | A key's rows with no more than `GAP` between one and the next. A session starts at its first row's time and ends `GAP` after its last row's time. |

- A window holds the times from its start up to, but not including, its end. A session holds its end too.
- Sizes are constant intervals of days, hours, minutes, seconds or smaller, in whole microseconds, such as `INTERVAL '5 minutes'`. Months are refused, as their length varies.
- A hop's slide is at most its size, and a row is in at most 1,000 of its windows.
- A row whose event time is null is in no window. The engine quarantines it, with the code `event_time`.

### Bounds and keys

- `window_start` and `window_end` in the `SELECT` list and in `HAVING` are the window's bounds, of the event time's type. Write the names unquoted. A quoted `"window_start"` is a stream's column of that name.
- `WHERE` runs before rows are grouped, so it cannot read `window_start` or `window_end`. Compare them in `HAVING`.
- The output's primary key is `window_start` and the group keys, as the output names them. The `SELECT` list has both: a statement that leaves either out is refused.
- An output can be merged on that key. The rows a window writes are inserts, and updates when its row is written again.

### Aggregates

| Aggregate | Argument | Value |
|---|---|---|
| `count(*)` | None | The window's rows, as `BIGINT` |
| `count(X)` | Any | The rows where `X` is not null, as `BIGINT` |
| `sum(X)` | A number | The sum. Integers give `BIGINT`. `DECIMAL(P, S)` gives `DECIMAL(P + 10, S)`, at most 38 digits. Floats give `DOUBLE`, rounded once from the exact sum. |
| `avg(X)` | A number | The mean. Integers and floats give `DOUBLE`. `DECIMAL(P, S)` gives `DECIMAL(P + 4, S + 4)`, rounded half away from zero. |
| `min(X)`, `max(X)` | A number, text, binary, a boolean, a date, a time, a timestamp or a UUID | The least or the greatest value. NaN is greater than every other float, and -0 equals 0. |
| `approx_distinct(X)` | As `min` | How many distinct values `X` has, as `BIGINT`: exact up to 256, then an estimate within about 2% |
| `median(X)` | A number | The middle value, or the two middle values' mean. Integers give `DOUBLE`. A `DECIMAL` keeps its type, rounded down. |
| `percentile_cont(F) WITHIN GROUP (ORDER BY X)` | A number, and `F` from 0 to 1 | The value at fraction `F` of the sorted values, interpolated between the nearest two, as `DOUBLE`. `ORDER BY X DESC` reads `1 - F`. `quantile_cont` is another name. |

- `mean` is another name for `avg`.
- Each aggregate ignores nulls, except `count(*)`. An aggregate of no values is null, except `count` and `approx_distinct`, which give 0.
- `FILTER (WHERE …)` after an aggregate counts only the rows where its condition is true: `count(*) FILTER (WHERE qty > 1)`.
- `HAVING` keeps the windows where its condition is true. It can read the aggregates, the bounds and the group keys.
- An aggregate gives the same value whatever order rows come in.
- If an aggregate's value does not fit its type, the engine quarantines the window's row instead of writing it, with the code `aggregate`. An example is a sum past the range of `BIGINT`.

### When a window's row is written

A window closes when the stream's watermark reaches the window's end: the engine expects no more rows before that time, except late ones. A session closes when the watermark passes its end. The engine then writes the window's row, in the epoch of the next checkpoint. A window that fails `HAVING` writes no row. [Event time and watermarks](./specs/pipeline.md#event-time-and-watermarks) says how the watermark moves.

- The watermark of a group window is the least watermark of the lanes that read the stream. Each lane's watermark is the least of its splits' watermarks.
- A split that reads no rows holds windows open. In a continuous pipeline, a split that reads no rows for 1 minute is idle, and its watermark moves on with the clock.
- A row is late if it comes after the watermark has passed its time. A late row of a window that has not closed yet goes into the window.
- `watermark.lateness` on the stream keeps a closed window's state that long. Within the lateness, a late row writes the window's row again, as an update. A window that no longer passes `HAVING` writes a delete of its row.
- A delete has the output's key, and the values of the row it deletes. The engine does not evaluate a column that can change between runs, such as one that reads `now()` or `random()`, again for a delete. That column is null in the delete, or its type's zero if the column cannot be null.
- A late row moves a session's start when it comes before the session's first row. The session's old row is then deleted, and the session's row is written under its new start. A row that would move a session's start is late when its own time plus the gap is past the lateness, as a row that joins no session is: a session that had gone could have had that start.
- The engine quarantines a row that comes later than its window's end and the lateness, with the code `late`. On a hop, a row is quarantined only when every window that holds it is past its lateness. A window that is past its lateness stays past it when `watermark.lateness` changes ([Changing a query](#changing-a-query)).
- `watermark.lateness` needs `destination.write: merge` for the output.
- After a restore, the watermark starts where the checkpoint's watermarks are. A window that closed before the checkpoint stays closed, and a row of it is late.

### Reads

How the stream is read decides when windows close:

| Read | Pipeline | When windows close |
|---|---|---|
| `full_refresh` | Scheduled | The watermark holds until the sync's read ends. Every window closes at the end of the read. The next sync starts its windows again, from the whole stream. |
| `incremental` by its `eventTime`, or `stream` | Scheduled | The watermark follows the rows as they come. At the end of the sync's read, it moves to the current time, less `watermark.delay`. It does not move to the end of input. A window that is still open goes on in the next sync, which restores its state. Without a later sync, it stays open. |
| `stream` | Continuous | The watermark follows the rows as they come. Each window closes while the pipeline runs. |

A group window over a stream read another way is a problem in the plan:

- With `cdc`, the stream is a changelog, and a group window groups appended rows.
- With `incremental` and a cursor other than the stream's `eventTime`, each sync's windows would hold only the rows that the sync reads.

### State

A group window keeps each window's aggregates as state, by key group, until its row is written and its lateness has passed. `openetl plan` shows the statement's operator, which keeps the state:

```text
    1:1  INSERT INTO revenue_by_minute  group_window  key window_start, region  exchange by region  reads orders; keeps state as operator window-29faae33810aefa5
```

- The engine exchanges the stream's rows by the key groups of the group keys, so each key's rows reach one lane. The output reaches its writer with no second exchange.
- A window over no group keys runs on one lane.
- Each lane keeps what its state changed since it last wrote a file of state. At each checkpoint, while those changes fit in the lane's share, the checkpoint record holds them, and the lane writes no file. Each record holds them again until the lane writes them to a file.
- The shares add up to 1 MiB of each record, or to a 64th of the engine's memory if that is less. Each statement that keeps state gets an equal part. Each of its lanes, on every worker, gets the fraction that the lane's key groups are of the pipeline's.
- At the checkpoint where a lane's changes become larger than its share, the lane writes them as a file under `_state/`, next to the pipeline's checkpoints, and starts again with no changes. The checkpoint record names the files that hold the state. A file that no checkpoint names, such as one from an epoch that did not commit, is never read.
- The record holds each window's newest change only. When the engine stops keeping a window's state, the record stops holding it. If a file of state may hold the window, the record holds a delete of it instead, until the lane writes a file again.
- A checkpoint names only the parts of files that the state still uses. When every window that a part holds has closed and its lateness has passed, or newer files hold all of it, the next checkpoints stop naming that part. When every window has closed and its lateness has passed, a checkpoint names no file of the statement's state.
- Each lane merges some of its files of state in the background, so that it reads fewer of them. It merges a window's values only when it holds more than 8 files of them. A checkpoint names the merged file in place of the files it replaces, which retention then deletes.
- The next attempt restores the state that its checkpoint names, on any number of lanes and workers. Each lane takes the changes that the record holds for its own key groups. A continuous pipeline's attempt that restarts, or a sync's attempt that fails, goes on with its open windows. Each window's row has every row of the window, each one time, as the next item says.
- A window drops repeats over a stream with [superset replay](../pipelines/delivery.md#what-decides-a-streams-guarantee), such as one read with `incremental`, that has a primary key or collapses ([Streams without a primary key](../pipelines/delivery.md#streams-without-a-primary-key)). Its state also keeps each row's record ID (`_openetl_id`) and a hash of its values, while a window that the row went to is kept. A row with the ID and the values of a row that the window took is a repeat. The window drops it, and **Written** counts it as a repeat. A row with the same ID and other values, such as a second version of a row at one cursor value, is a row of its own. The hash leaves out null values, so a column that the stream gained since does not change it.
- For those streams, the state grows with the rows of the windows it keeps, and not only with the windows. A session keeps the ID and hash of each of its rows until it closes and its lateness has passed. So while rows keep a session open, its state grows with each row it takes.
- Over a stream with exact replay, the state keeps no rows' IDs. An attempt that restores a checkpoint taken at a forced barrier reads some rows again, and counts them again in each window that is still open. The same goes for a stream read with `full_refresh`, and for one without a primary key that does not collapse. State restored from a checkpoint of an earlier version of OpenETL holds no rows' IDs, so a row that a read gives again into one of its windows counts again, once.
- A row that a read gives again after its window has gone is late, as any row is. A stream read with `incremental` reads again the rows at its last cursor value. Some streams read further back: an SDK or declarative source's stream that is not read in cursor order, or one with a `lookback`. A window that such a read reaches again can be gone, and its rows are quarantined with the code `late`. To keep them, set `watermark.delay` and `watermark.lateness` so that together they cover about one sync interval plus the window's size.
- The engine reads a file of state when a lane first needs it, not at the start of the attempt. At its first checkpoint that writes state, a lane reads whole the windows' values in the files it restored, to find which parts the state still uses.
- Each lane keeps its changes in memory, those since the last checkpoint and those that the last checkpoint record holds, up to its part of the job's memory: an eighth of the engine's memory limit, divided among the job's stateful operators and the engine's lanes, and at least 64 KiB. Past that part, or when the engine holds more than seven eighths of its limit, the lane writes them before the checkpoint as files of their own, `….earlyN.run`: first the changes that the record holds, then those since the checkpoint. The lane reads them from those files, and the next checkpoint names them. The record then holds only what the lane changed after it wrote them. If the epoch does not commit, a restore never reads those files, since it reads only what its checkpoint names, and retention deletes them.
- The engine keeps the parts of the state's files that it reads, and the files that it writes, in a disk cache: in a folder of the engine's own in the `scratch` folder of the runner's `--cache`, within half of the runner's `--scratch`. While a part stays in the cache, the engine reads it from the disk, not from the checkpoint store. Past the limit, the cache removes its oldest parts, but keeps those it read recently. A part that the cache removed, or whose checksum fails on disk, is read from the checkpoint store again. The engine deletes the folder when the job ends, and a runner deletes the folders of engines whose runner stopped without doing so. With `--scratch 0` there is no disk cache.
- If the engine nears its memory limit and the state of all the job's statements, with their block caches, still holds more than half of the job's memory, the run fails with `RESOURCE_EXHAUSTED`: `the group window writing OUTPUT keeps more state in memory than the job's size allows`. Run the pipeline with a larger `resources.size`. In memory, the state holds the changes since the last checkpoint, the indexes and filters of the files it reads, the hashes of the windows' keys in them, and a block cache of up to a sixteenth of the job's memory.
- A run keeps the last 10 checkpoint records, and deletes each file of state that none of them names. It does so when it starts, and after every 10 checkpoints. So the number of files of state follows how long windows stay open, and does not grow with the number of checkpoints.
- Each checkpoint records what the operator did in its epoch, on every worker: its rows, its late rows, its watermark, the windows it keeps, its state's size, its requests to object storage, and its reads that the disk cache answered. The console's **SQL** tab shows the last checkpoint's, and the `openetl.operator.*` metrics give them ([Monitoring](../self-host/monitoring.md#stateful-operators)).
- To build a window's state again from the stream, reset the streams that the group window reads: `openetl reset --stream STREAM NAME`. The reset drops the window's state, and the next run builds the state again from a new read of the stream ([Read streams again from the start](../pipelines/positions.md#read-streams-again-from-the-start)). A run deletes the files of the dropped state once none of the records it keeps names them.
- After a reset, each window's row comes only from the rows that the new read returns. If the read starts partway through a window, `merge` replaces that window's row with a row of part of its rows. A window that the new read does not reach keeps its row in the destination. Start the read where it includes every row of the windows it changes, or correct those rows in the destination.
- With `resources.workers` above 1, the exchange sends each row to the worker whose lane owns the row's key group, and that worker keeps the key group's state and writes its windows' rows ([Group windows on workers](../pipelines/workers.md#group-windows-on-workers)).

### Changing a query

The operator's ID is a hash of what the operator computes and keeps. The engine writes it over the stream's own columns, so the names of columns, aliases and views, and the shape of the SQL, do not change it. The output's name is part of the ID: an output of another name is another operator, which does not keep the state. Each checkpoint names the state by the ID.

These edits keep the ID, and with it the state:

- Formatting, comments and the case of keywords, and an equal interval, such as `INTERVAL '60 seconds'` for `INTERVAL '1 minute'`.
- The `FROM` alias, other names for the output's columns, and views that rename columns or pass more columns.
- The order of the `GROUP BY` keys, of the `SELECT` list and of the aggregates. The output's primary key follows the order of `GROUP BY`. Aggregates that call a function that gives another value each time, such as `random()`, keep the ID only in the same order.
- `HAVING`, `window_end`, and expressions over the aggregates, when they use only aggregates that the statement already computes. An aggregate that only `HAVING` or an expression uses is still an aggregate that the operator keeps: adding one gives a new ID.
- The same filters written in a view or in the statement, and `count(1)` for `count(*)`.
- Other statements, and the order of the statements.

These edits give the operator a new ID:

- The output's name, or the stream that the statement reads.
- The window's kind, size, or a hop's slide.
- A key, an aggregate or an aggregate's argument, added, removed or changed, and an aggregate's `FILTER`.
- A filter or an `UNNEST` before the window.
- A change of type of the event-time column, a key or an aggregate's argument, also when the source changes the column's type.

`watermark.lateness` is not part of the ID. A changed lateness keeps the state, and applies from the next run:

- A window that is open at the change, or closed and within its lateness, takes the new lateness. Its state expires when the watermark passes its end plus the lateness in force when it closed, or later if the lateness rises before then.
- A window whose lateness had passed before the change stays gone. A row of it is late, also within a higher lateness, and the window's row stays as it is.
- The state keeps, in each key group, the latest point that the group's watermark less the lateness has reached. A tumbling or hopping window that ends at or before that point is gone. A session ends at its last row's time plus the gap. A session's row is late when the session that it joins or starts ends before that point. It is also late when it would move a session's start and its own time plus the gap is before that point.
- A lower lateness can leave the state of a window that closed before the change until the old lateness passes. Once the new lateness has passed the window, it takes no more rows.

[Change the lateness](../pipelines/windows.md#change-the-lateness) has what each window does.

`openetl plan` and `openetl apply` show each operator in a `state` section of `transform.sql`. Each line has the statement's place, the output, the ID, what the operator is against the pipeline's current version, and what it keeps. Where the pipeline's last checkpoint says which operators' state it names, the line ends with whether it holds the operator's state. In this example, the current version has the statement of [Group windows](./sql-windows.md#group-windows) and a second statement, `orders_by_hour`, with `tumble(created_at, INTERVAL '1 hour')`. The new version gives the first statement a `FROM` alias and other column names, and changes the second one's window to 30 minutes. The stream has `watermark.lateness: 1m`:

```text
  state:
    1:1  revenue_by_minute  window-29faae33810aefa5  kept     tumble 1m0s by created_at on region: count(*), sum(amount); keeps a window's state 2m0s of event time (lateness 1m0s)
    6:1  orders_by_hour     window-d110a4b5535848da  changed  tumble 30m0s by created_at: count(*); keeps a window's state 31m0s of event time (lateness 1m0s); starts empty, in place of window-c627132d9872ae49; a backfill reads again orders
```

The plan's warnings say the same for `orders_by_hour`, with the command for the backfill.

| What | Means | Its state |
|---|---|---|
| `kept` | The current version has the operator. | The next sync restores it. |
| `new` | The current version has no operator that writes the output. | Starts empty. If an earlier version had the operator, the current version dropped it, and the last checkpoint still holds its state, as when no sync has run since, the line says so, and the next sync restores that state. |
| `changed` | The current version's operator for the output computes or keeps something else. | Starts empty, or, if an earlier version had the operator, as for `new`. The next sync drops the old operator's state. |
| `dropped` | No statement keeps the current version's operator. | The next sync drops it. |
| `unknown` | The current version's statements were not planned when it was applied. | As for `kept` if the ID is the same, otherwise the next sync fails as below. |

A new or changed operator starts from now: its windows hold only the rows that the pipeline reads after the change. A window that is open at the change, or that a late row opens again, loses the rows read before. The plan has a warning for each changed operator, and for each new one over a stream that the pipeline has read, unless each sync reads the stream whole.

To build the state from the stream's start, read the stream again after you apply the change (a backfill).

> [!CAUTION]
> A reset clears state for every statement that reads the stream. Each output receives the stream’s rows again.

[Read streams again from the start](../pipelines/positions.md#read-streams-again-from-the-start) explains the destination effects.

```sh
openetl reset --stream orders NAME
```

A version without `transform.sql` drops every operator of the version before it. The next attempt drops the state of each operator that the version dropped or replaced, so the records after it stop naming that state, and retention deletes its files once no record it keeps names them. If an operator's ID changes without a new version, because the source changed the type of a column that it keeps, the version did not drop the old operator. The next attempt then fails with `CONFIG_INVALID`, and its message names the operator and the command to run. Reset the streams that the window reads to build the state again.

An operator has no name of its own in the SQL. Its ID is the only name.
