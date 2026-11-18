---
title: Inspect SQL transforms
description: Read the SQL plan, output schemas and window statistics in the console.
---

Use this guide if your pipeline has a SQL transform. It assumes that you know SQL.

For pipeline health, start with [Check data freshness](./freshness.md).

## SQL

A pipeline with a SQL transform has a **SQL** tab. A SQL transform is SQL statements that change rows on their way to the destination. Each `INSERT INTO` statement writes an **output**, which the destination gets as a stream.

- **Messages** at the top are the plan's problems and notes about the SQL that do not belong to one statement. A problem is something a sync fails on. Each message starts with its place in the SQL, as line and column, such as `9:10`.
- **Statements** shows the SQL as the pipeline's current version has it, with line numbers. Lines with a problem are marked in red, and lines with a note in blue. A line under it says what happens to streams that no statement writes: they pass through to the destination unchanged, or they are not delivered.
- **Plan** shows how the statements plan against the source's latest list of streams and columns. There is a card for each statement and each view, in the order they are written.

Each statement's card shows:

| Part | What it shows |
|---|---|
| **INSERT INTO** | The output it writes, and the destination table it goes to if the pipeline names one |
| Its place | The line and column where the statement starts |
| **Row-wise**, **Group window** or **Over** | The kind of statement. A row-wise statement changes one row at a time. A group window adds up rows by windows of time. An over statement gives each row values from the rows before it, such as a running total. |
| **Replaces stream** | The output takes the place of the source's stream of the same name. |
| **Primary key** | The output's key. **None, so append only** means rows are only added, never merged. |
| **Reads** | The source's streams it reads, and the views it reads them through |
| **Exchange** | Whether rows move between lanes before the statement. **None** means they do not. |
| Notes and problems | The plan's messages about this statement |
| **Column**, **Type**, **Nulls** | The output's columns |

A **CREATE VIEW** card shows a view: a named part of the SQL that statements read. It writes nothing itself.

Below the cards, **State** lists each statement that keeps state between rows, such as a statement that adds up rows by windows of time. Each row shows:

- **Operator**: the name that the pipeline's checkpoints give the statement's state, with the output and the statement's place.
- **Keeps**: what the statement computes, and how long it keeps each window, in the time of the rows.
- **State**: **Kept**, **New**, **Changed**, **Dropped** or **Unknown**, against the pipeline's current version. **Changed** means that an edit changed what the statement computes, so its state starts empty. When the pipeline has already read the stream, the row also shows the command that reads the stream again to build the state. The row also says whether the pipeline's last checkpoint holds the state, where that is known.

**Passed through** lists the source's streams that no statement writes, and **Not delivered** the ones left out.

If the statements have errors, the **Plan** side says so, and the errors are at the top, each with its place. If the statements could not be planned, for example because no runner answered, a note at the top says why.

If a statement keeps state, **At the last checkpoint** shows what it did in the pipeline's newest epoch. An epoch is the rows between two checkpoints. The numbers add up every worker of the pipeline. Each row shows:

- **Operator**: the statement's operator, and its output.
- **Watermark**: the time in the rows before which the statement expects no more rows, and how far it was behind the commit. A window of time closes when the watermark reaches its end. **None** means that a part of the stream has had no rows, or that the watermark is outside the years 1 to 9999. **End of input** means that the read ended and every window closed.
- **Windows**: the windows the statement keeps: open ones, and closed ones that can still take late rows.
- **Rows in**: the rows of the stream that reached the statement in the epoch.
- **Written**: the rows it wrote to its output in the epoch, as new rows, updates and deletes.
- **Late**: the late rows that came after their window had closed and its allowed lateness had passed, which OpenETL quarantined with the code `late`. Late rows that still went into their window are not in this count. [Quarantined rows](./held-data.md#quarantined-rows) shows them under the output.
- **State** shows memory usage and the state kept by the checkpoint. **stored** counts files that hold state still in use. This count reaches zero after every window closes and its allowed lateness passes. **in the checkpoint** counts recent changes held in the checkpoint record before they move to a file. The pipeline can keep up to 1 MiB of these changes in the record.
- **Requests**: its requests to the data plane's storage in the epoch, with their bytes.
  - **Writes** write its state at the checkpoint. The statement runs in parallel lanes, and each lane writes nothing while the checkpoint holds that lane's changes. So one statement can show both **Writes** and state **in the checkpoint**.
  - **Reads** read the state that it needed.
  - **Restores** open the state that a checkpoint names after a restart. They also read that state once, whole, to find what is still in use.
  - **Merges** count the files of state that the checkpoint wrote to merge other files, and the reads of the files that they merge.

  A read, a restore or a merge's read made near the checkpoint can count in the next epoch instead.

[Aggregate by windows of time](../pipelines/windows.md) explains watermarks, windows and late rows.
