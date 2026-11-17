---
title: Aggregate by windows of time
description: Add up a stream's rows by minutes, hours or sessions of their own time with a SQL group window, and know when each window's row is written, what happens to late rows, and where the state is kept.
---

A group window is a SQL statement that adds up a stream's rows by windows of time, such as the revenue of each region in each minute. It writes one row for each window and key. To give each row a running total, or the value of the row before it, use `OVER` instead ([Compute running totals with OVER](./over.md)). This page explains the time that windows use, when each window's row is written, what happens to rows that come late, and how to change a window's statement. [Write a SQL transform](./sql-transforms.md) explains statements in general. [Group windows](../reference/sql.md#group-windows) in the SQL reference lists every window, aggregate and rule.

## Before you start

- You can write a SQL transform, as [Write a SQL transform](./sql-transforms.md) shows.
- The stream has a column that holds the time of each row, such as `created_at`. Its values are timestamps or dates.
- The output's destination can merge, if rows can come late. [Late rows](#late-rows) says why.

## Event time and watermarks

A row has two times. Its **event time** is the time in the row, such as when an order was placed. Its read time is when OpenETL reads it. Rows often come out of event-time order: a phone sends its clicks an hour late, or one partition of a topic is behind another. A group window groups rows by their event time, so a late click still counts in the minute it happened.

So OpenETL must decide when a window has all of its rows. It uses a **watermark**: a time before which it expects no more of the stream's rows. When the watermark reaches the end of a window, the window closes and OpenETL writes its row. A row whose event time is before the watermark when it comes is **late**.

OpenETL keeps a watermark for each split of the stream, such as a partition of a topic or a range of a table. A split's watermark is the latest event time that the split has read, less a delay that you set. The stream's watermark is the least watermark of its splits. So one split that is behind holds every window open until it catches up.

### Set the event time

Set `eventTime` on the stream, and set how far the watermark stays behind the latest event time:

```yaml
  source:
    ref: clicks
    streams:
      - name: page_views
        sync: stream
        eventTime: viewed_at
        watermark:
          delay: 30s
          lateness: 5m
```

- `eventTime` names the column. It needs `transform.sql`.
- `watermark.delay` is how long the watermark waits for rows that come out of order. With `30s`, a row up to 30 seconds older than the newest row that its split has read is not late. A longer delay means fewer late rows, and each window's row is written later.
- `watermark.lateness` is how long a window takes late rows after it closes. [Late rows](#late-rows) explains it.

Both are 0 by default. The [Pipeline reference](../reference/specs/pipeline.md#event-time-and-watermarks) has the exact rules.

`openetl plan` has a note for each stream with an event time. For the stream above, in a continuous pipeline, the note is:

```text
  notes:
    stream page_views: event time viewed_at; each split's watermark trails the latest it has read by 30s, and moves on with the clock after 1m0s with nothing to read
```

### How the read moves the watermark

How the source reads the stream decides how the watermark moves:

| Read | Rows come | The watermark |
|---|---|---|
| `stream` or `cdc` | In the order of their event time, about | Follows the rows as they come, less the delay. |
| `incremental`, with `cursor` set to the `eventTime` column | In the order of their event time | Follows the rows as they come, less the delay. |
| `full_refresh`, or `incremental` by another cursor | In another order, such as by primary key | Holds until the read ends. Then it moves to the end of input, which closes every window. |

A group window takes a stream read with `full_refresh`, `stream`, or `incremental` by its `eventTime`. The plan refuses a group window over a stream read another way.

### Quiet splits

A split that reads no rows holds the stream's watermark back. So one quiet partition keeps every window open. In a continuous pipeline, a split that reads no rows for 1 minute is idle. Its watermark moves to the current time, less the delay, and keeps moving while the split is idle. When the split reads a row again, its watermark follows its rows again. It never goes back.

An idle split's move counts as something the source emitted. So a quiet continuous pipeline with an event time still commits about one epoch each commit interval (`delivery.commit.interval`), or each 15 seconds if the interval is shorter. Each such epoch writes a checkpoint, and the rows of the windows that the move closed.

A scheduled pipeline has no idle splits. A split that completes its read moves its watermark as [Windows across syncs](#windows-across-syncs) describes.

## Write a group window

Group by a window of the event time and by columns:

```sql
INSERT INTO views_by_minute
SELECT window_start, page, count(*) AS views
FROM page_views
GROUP BY tumble(viewed_at, INTERVAL '1 minute'), page;
```

- `tumble` makes windows of one size, one after another. `hop` makes windows that overlap, and `session` groups a key's rows with gaps no longer than a size. [Windows](../reference/sql.md#windows) in the reference lists them.
- The output's primary key is `window_start` and the group keys, here `page`.
- `HAVING` keeps only the windows where its condition is true.

## When a window's row is written

A window closes when the watermark reaches the window's end. A session closes when the watermark passes its last row's time and the gap. The engine then writes the window's row, in the next epoch that it commits. A window that fails `HAVING` writes no row.

Take the statement above with `delay: 30s`, over a stream with one split that reads views at 12:00:10, 12:00:50 and 12:01:20. At 12:01:20 the split's watermark is 12:00:50, so the window from 12:00 to 12:01 is still open. When the split reads a view at 12:01:35, its watermark is 12:01:05, and the window closes. Its row has every view from 12:00 to 12:01 that came by then.

## Late rows

A row is late when the watermark has passed its event time. A late row of a window that is still open goes into the window as usual. A late row of a window that has closed goes into the window only within `watermark.lateness`:

- **Within the lateness**, the window's state is kept, so the late row is added to it. The engine writes the window's row again, as an update of the row with the same key. If the window no longer passes `HAVING`, the engine writes a delete of its row. If a late row moves a session's start, the engine deletes the session's old row and writes the row under its new start. A row that would move a session's start is late when its own time plus the gap is past the lateness, as a row with no session to join is. Another session that had gone could have had that start, and its row stays as it is.
- **After the lateness**, the window's state is gone. The engine quarantines the late row with the code `late`. A row whose event time is null is in no window, and is quarantined with the code `event_time`. A window that is gone stays gone when you change the lateness later ([Change the lateness](#change-the-lateness)).

Updates and deletes need a destination that merges, so a stream with `watermark.lateness` needs `destination.write: merge` for each group window's output. The plan refuses lateness otherwise.

A longer lateness keeps each window's state longer, so the job keeps more state. For each window, the engine keeps the state from its first row until the watermark passes its end plus the lateness.

### See late rows

A late row that comes after its window's lateness is quarantined as a row of the window's output, not of the stream. Late rows that go into their windows are not counted apart. To list the quarantined ones, run:

```sh
openetl quarantine --stream views_by_minute PIPELINE
```

The list adds up rows by cause, so late rows show as the code `late`. In the console, the output's **Quarantined** number opens the same files ([Quarantined rows](../use/index.md#quarantined-rows)). The **SQL** tab's **At the last checkpoint** table shows how many the last epoch quarantined, and the `openetl.operator.rows.late` metric counts them ([Watch a window](#watch-a-window)).

## Windows across syncs

A scheduled pipeline keeps each window's state in its checkpoint between syncs. What a sync closes depends on the read:

- **`full_refresh`.** Each sync reads the whole stream, and the watermark holds until the read ends. Then every window closes, each with all of its rows. The next sync starts its windows again from the whole stream, and its rows replace the last sync's under `merge`.
- **`incremental` by the event time, or `stream`.** When a split's read in the sync ends, its watermark moves to the current time, less the delay. It does not move to the end of input, since the stream goes on. So the windows that end before that time close, and later windows stay open. The next sync restores their state and goes on with them, so a window that spans two syncs closes in the second.

So a sync of a stream read in event-time order leaves its newest windows open until a later sync. If your rows' event times are close to the current time, each window that ends later than the sync's end less the delay waits for the next sync. If you run such a pipeline once, those windows are never written. Run the pipeline on a schedule, or read the stream with `full_refresh` if every sync must close every window.

## Where the state is kept

A group window keeps state for each window that is open, or closed and within its lateness: the window's aggregates, and the values of the row it wrote. OpenETL keeps it in the runners' checkpoint store (`--checkpoints`), in a `_state/` folder next to the pipeline's checkpoints. At each checkpoint, the engine writes what the state changed since the last one, and the checkpoint names the files that hold the state.

- A file counts only once a checkpoint that names it commits. A file of an epoch that failed is never read.
- After a failure, or between syncs, the next run opens the state that its checkpoint names, and reads each file when it first needs it.
- The engine stops keeping a window's state when the watermark passes the window's end plus the lateness.
- Each lane merges some of its files in the background, once it holds more than 8 files of windows' values. A checkpoint names the merged file in place of the files it replaces.
- A checkpoint names only the files that hold state still in use. When every window that a file holds has closed and its lateness has passed, or newer files hold all of it, the next checkpoints stop naming it. When every window has closed and its lateness has passed, a checkpoint names no file of the window's state.
- A run keeps the last 10 checkpoints. When it starts, and every 10 checkpoints after, it deletes each file of state that none of those checkpoints names. So the files of state follow how long windows stay open, and do not grow with the number of checkpoints.

[State](../reference/sql.md#state) in the reference has the rest.

### Build the state again

To build a window's state again from the stream, reset the stream that the window reads. The reset drops the stream's position, its watermark and the state of every statement that reads it. The next run reads the stream again from where the source starts a new read, and builds the state from those rows. The files of the dropped state are deleted once no kept checkpoint names them.

> [!WARNING]
> A reset reads the stream again for every statement that reads it, and each of their outputs gets the stream's rows again. If the new read starts partway through a window, that window's row is written again from only part of its rows, and under `merge` it replaces the row that had all of them. [Read streams again from the start](./positions.md#read-streams-again-from-the-start) has what the destination gets.

```sh
openetl reset --stream page_views PIPELINE
```

## Change a window's statement

A group window keeps its state under an ID that the plan shows. Some edits keep the ID, and with it the state: other names for columns, a new column through a view, and a `HAVING` over the aggregates that the statement computes. Another output name, window size, key or aggregate gives the window a new ID, and its state starts empty. [Changing a query](../reference/sql.md#changing-a-query) lists the edits that keep the ID.

1. Edit the statement.
2. Run `openetl plan -f FILE`, and read the `state` section of `transform.sql`. Each window shows `kept`, `new`, `changed` or `dropped`. The plan has a warning for each changed window, and for each new one over a stream that the pipeline has read, unless each sync reads the stream whole.
3. Run `openetl apply -f FILE`. The next run drops the state of each changed or dropped window.
4. If the stream is not read with `full_refresh`, and a changed or new window must include the rows that the pipeline read before, read the stream again. Run `openetl reset --stream STREAM PIPELINE`, as [Build the state again](#build-the-state-again) describes.

Without step 4, a changed or new window starts from now: its windows hold only the rows that the pipeline reads after the change. A window that is open at the change loses the rows read before. A stream read with `full_refresh` is read whole at each sync, so a changed or new window over it holds every row from the next sync on.

A changed `watermark.delay` or `watermark.lateness` keeps the state.

### Change the lateness

When you apply a new `watermark.lateness`, the next run uses it for the windows that are still kept, and for new windows. It does not bring back a window that is gone:

- **A window whose lateness had passed before the change** stays gone. A row for it is late, also when the row comes within a higher new lateness. The engine quarantines the row with the code `late`, and the window's row in the destination stays as it is.
- **A window that is open at the change, or closed and within its lateness,** takes the new lateness. With a higher lateness, the engine keeps the window's state longer, and a late row within the new lateness updates the window's row. With a lower lateness, a row that comes after the new lateness is late, and the window's row stays as it is.
- **A window that opens after the change** uses the new lateness.

The engine keeps a record of how far windows are gone in the window's state, and the checkpoint holds it. So the rule holds after a failure, a restart and a change in the number of workers. After a higher lateness, the late rows of windows that were gone before the change go to quarantine. This continues until the watermark less the new lateness passes the point where those windows had gone.

For a `session` window, the engine judges a row by the session that the row joins or starts. A session ends at its last row's time plus the gap. If that end is before the point where windows had gone, the row is late. A row that would move a session's start is also late if its own time plus the gap is before that point. Other rows start or join a session, as they do with an unchanged lateness. Such a session can overlap a session that had gone, and each one has its own row.

With a lower lateness, the engine can keep the state of a window that had closed before the change until the old lateness passes. Once the new lateness has passed the window, the window takes no more rows.

A change of the lateness never deletes or rewrites a row that a window wrote. To build windows that are gone again, with the rows that came late, read the stream again as [Build the state again](#build-the-state-again) describes. The reset drops the state, and the new read builds each window again from the rows that it returns.

## Watch a window

The console's **SQL** tab shows, under **At the last checkpoint**, what each window's statement did in the last epoch, on every worker: its watermark, how far it was behind the commit, the windows it keeps, the rows it took in and wrote, its late rows, its state's size and its requests to object storage. [SQL](../use/index.md#sql) describes the table.

The same numbers are metrics, with the operator's ID as `openetl.operator` ([Monitoring](../self-host/monitoring.md#stateful-operators)). Watch these:

- `openetl.operator.watermark.lag` grows when a split is behind the others. A window's row waits until the watermark reaches the window's end, so a growing lag delays the rows.
- `openetl.operator.rows.late` counts late rows that came after their window's lateness, which OpenETL quarantined. If it grows, raise `watermark.delay` or `watermark.lateness`. A raised lateness applies to the windows that are still kept and to new windows. The late rows of windows that were gone before the change still go to quarantine, as [Change the lateness](#change-the-lateness) describes.
- `openetl.operator.windows` and `openetl.operator.state.memory` grow with the keys and the lateness.
