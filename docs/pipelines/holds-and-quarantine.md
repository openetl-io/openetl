---
title: Holds and quarantine
description: What OpenETL does with a value it cannot write, how a hold differs from quarantine, and how to choose between them with unfitValues.
---

Sometimes OpenETL cannot write a value as the source sent it. The value does not fit its column's type, or the destination refuses a change to a column. OpenETL does not drop such a value without a record, and it does not stop the pipeline if it can continue. It keeps the value back in one of two ways: it **holds** the value, or it **quarantines** the value's row.

## The difference

| | Hold | Quarantine |
|---|---|---|
| The row | OpenETL writes the row. The held cell shows null, a value that you choose, or the value that the destination already has. | OpenETL does not write the row. |
| What OpenETL keeps | The held value, with the row's key and version | The whole record as the source sent it, with the reason |
| The way back | **Release** patches the value into the same row. **Discard** drops it. | There is no row to patch. Read the record again from the source, or load it yourself. |
| What it needs | A primary key, and a destination that patches the stream's write mode | Nothing |
| Where you see it | **Held** in the console, and `openetl holds` | **Quarantined** in the console, `openetl quarantine`, and the **Quarantine** incident |
| Where OpenETL keeps it | Parquet files under `_held/`, next to the pipeline's checkpoints | JSON lines under `_quarantine/`, next to the pipeline's checkpoints |

A hold keeps a value back from a row that OpenETL writes. Quarantine keeps a row back from the destination, as a dead-letter queue does. A hold has a way back into the destination. A quarantined row has none.

The files are different because their data is different. A held value has a type and a key, and OpenETL reads it back as a patch. So OpenETL keeps held values in Parquet. A quarantined record did not fit its types, and the records of one commit can have different shapes. So OpenETL keeps each record as the source sent it, in JSON lines that you can read with `jq`.

## Why there are two

OpenETL applies two rules to data it cannot write:

- **No data goes away without a record.** OpenETL keeps every value that it does not write, with the reason, until a person decides what to do with it.
- **The rest of the data continues.** One bad value does not stop a stream, and one stream does not stop the pipeline, where OpenETL can prevent it.

Quarantine and holds apply these rules at two sizes:

- **Quarantine treats the row as the unit.** The destination gets only rows whose values fit their types. A row with a bad value is missing until you load it again. On a `merge` stream, the key's other columns keep their old values until OpenETL writes a later version of the row. If that version also has a value that does not fit, OpenETL quarantines it too. If the row was an insert, rows that refer to it point at nothing.
- **A hold treats the value as the unit.** OpenETL writes the row, and its other columns are current. Only the held value is missing. The row's `_openetl_held` column names the held columns. A reader that does not check `_openetl_held` takes a partial row for a complete one.

A partial row changes what a row means. So OpenETL holds single values only if you tell it to. By default, a value that does not fit quarantines its row. A change to a whole column is different, because a stop for that change would stop every row of the stream. So OpenETL holds the column by default where it can ([Column holds](#column-holds)).

## Choose what a value that does not fit does

A value does not fit when its column's type cannot keep it. Each held cell and each quarantined row has one of these codes:

| Code | The value |
|---|---|
| `type` | Is not of the column's type, such as text in a number column |
| `range` | Is outside the type's range, such as an infinite date with `types.infinity: unfit` |
| `precision` | Has more digits or decimal places than the column keeps |
| `nan` | Is NaN, in a type that has no NaN |

For these codes, the reason names the column. It never contains the value. A quarantined row can also have a code that is not about one value. Examples are `object` for a record that is not a JSON object, and `statement` for a row that a SQL statement cannot evaluate. [Quarantine files](../reference/quarantine-files.md#codes) lists every code.

The stream setting `unfitValues` chooses what such a value does:

```yaml
source:
  ref: shop
  streams:
    - name: orders
      unfitValues: hold
      held: null
```

| `unfitValues` | What it does |
|---|---|
| `quarantine` | OpenETL quarantines every row that has a value that does not fit. It writes none of those rows. This is the default. |
| `hold` | If OpenETL can hold the value's cell, it writes the row and holds the cell. If it cannot, it quarantines the row. |

So `quarantine` quarantines every such row, but `hold` does not hold every such value. [When hold still quarantines a row](#when-hold-still-quarantines-a-row) lists the cases.

`hold` needs a primary key and a destination that patches the stream's write mode. If the stream does not have both, the plan has a problem. The destination connector's documentation says which write modes it patches. `held` and `heldValue` choose what a held cell shows, and `groups` names columns that OpenETL holds together. The [Pipeline reference](../reference/specs/pipeline.md#source-and-streams) lists each setting.

`schemaPolicy: ignore_new` casts each value to the type that the destination's column already has. A value that does not fit that type follows `unfitValues` too.

## When hold still quarantines a row

With `unfitValues: hold`, if a row has no safe place to hold its value, OpenETL quarantines the row. These are the cases:

- **The record is broken, not one of its values.** A record that is not JSON, or is not a JSON object, has no cell to hold.
- **The value is in a column that OpenETL cannot hold:**
  - a column of the primary key, because a patch finds its row by the key;
  - the stream's cursor, because it orders the versions of a row;
  - a column that the destination reports it cannot hold, such as a partition column;
  - a column that cannot be null, unless `heldValue` gives a value to write in its place;
  - a column whose `heldValue` does not fit the column's type.
- **A SQL transform reads the held value.** A statement cannot compute from a value that OpenETL holds. So OpenETL quarantines a row whose input holds a cell in a column that the statement reads, as one of the statement output's records. A delete is the exception: it passes with its key. OpenETL also quarantines a row that a statement cannot evaluate, whatever `unfitValues` says. Examples are a cast that fails, a division by zero and a row that comes too late for its window ([SQL reference](../reference/sql.md)).

In each of these cases, the row's line in the quarantine file has a `notHeld` field. It says why OpenETL did not hold the value: `record`, `key`, `cursor`, `destination`, `not_null`, `held_value` or `transform`. [Quarantine files](../reference/quarantine-files.md#why-a-value-was-not-held) explains each value.

A stream without a primary key cannot set `hold`. The streams of a pipeline with a Spark or Flink transform cannot set it either. If they do, the plan has a problem.

## Column holds

`unfitValues` decides only what single values do. In these cases, OpenETL holds a whole column, whatever `unfitValues` says:

- the destination refuses a change to the column's type, which it does unless `typeChanges` is `allow`;
- the schema policy does not take the change: `allow_columns` for a type change that is not a widening, and `block` for any change;
- the destination cannot add more columns.

The schema policy applies to a stream's rows after the first checkpoint that records a schema with the stream's columns. That schema is the stream's first schema. If a sync restores a checkpoint that has the stream's columns, the policy applies from the start of the sync. The schema policy holds none of the changes in the first schema. Holds that the destination causes still apply to them: a type change that the destination refuses, or a new column that the destination has no room for.

On [workers](./workers.md#schema-changes-and-holds), the policy applies on every worker after that checkpoint, also on a worker that has not read any of the stream's rows yet. The first schema can have a change from rows after that checkpoint. [Schema changes and holds](./workers.md#schema-changes-and-holds) says when, and what a worker does until it receives the first schema.

A column hold needs what a cell hold needs: a primary key and a destination that patches the stream's write mode. If OpenETL cannot hold the column, the sync fails with `SCHEMA_DRIFT`. A change to a column does not quarantine rows. [Handle schema changes and holds](./schema-changes.md) walks through a column hold and its release.

OpenETL cannot hold these columns whole:

- a column of the primary key;
- the stream's cursor;
- a column that the destination reports it cannot hold, such as a partition column or the key of the destination's table;
- a column that cannot be null, unless `heldValue` gives a value to write in its place.

If the destination refuses a change to one of these columns, the sync fails with `SCHEMA_DRIFT`. The error includes the destination's reason and its repair statement. Run the repair, or change the column back at the source. Then start a sync.

A hold can be in force on such a column, such as a hold that a sync recorded before OpenETL checked these cases. Each sync then fails with `SCHEMA_DRIFT` until you release or discard the hold. Either way, the destination then gets the column as the stream has it. To end the hold:

1. Run the destination's repair from the error, or change the column back at the source.
2. Release the hold with `openetl holds release PIPELINE STREAM COLUMN`, which patches its held values in. Or discard it with `openetl holds discard PIPELINE STREAM COLUMN`, which drops them.
3. Start a sync.

If the destination still refuses the column, the sync fails with `SCHEMA_DRIFT` and the repair statement.

## A change that stops a stream holding

A stream holds values only while it meets every requirement for holds. These changes can stop a stream holding:

- a version that changes the stream's write mode to one that the destination does not patch, or removes its primary key;
- a version that removes the stream from `source.streams`, or a source catalog that leaves the stream out;
- a version that renames or removes a SQL transform's output, or adds a Spark or Flink transform;
- a version that points to a destination without holds;
- a runner without storage for held values.

If the stream has held values, the sync fails with `SCHEMA_DRIFT`. OpenETL keeps the holds and their held values. The error names the stream, what it holds and why it does not hold now. Choose one of these ways on:

- Undo the change, release the holds, and make the change again after the release.
- Discard the holds with `openetl holds discard PIPELINE STREAM COLUMN`. The next sync then makes the change, and the held values are dropped. A discard also replaces a release that waits for the next sync.

A release that is being patched in (**Releasing**) can finish only while the stream holds. If you cannot undo the change, discard the release with `openetl holds discard PIPELINE STREAM COLUMN`. If the stream does not hold in the next sync, that sync drops the values that the release has not patched in. While the stream holds, the release goes on, and the discard applies only if the stream stops holding before the release ends. If the release ends first, the discard drops nothing, and values that the release leaves held stay held.

A paused stream keeps its holds. No destination patches `overwrite`, so a change to `overwrite` stops the stream holding. Unless the stream sets how it holds, which is then a problem in the plan, the change does not fail the sync. Each sync reads the whole stream again and replaces the table, so the stream's holds end, and their held values are not patched in.

## An example

A stream `orders` has the primary key `id`, and the pipeline writes it with `merge`. Its `price` column is a decimal with two decimal places. The source sends three rows, and row 2's price is `NaN`, which a decimal cannot keep:

| `id` | `price` | `note` |
|---|---|---|
| 1 | 10.00 | first |
| 2 | NaN | second |
| 3 | 30.00 | third |

With `unfitValues: quarantine`, the default, the destination gets rows 1 and 3. Row 2 is in the commit's quarantine file for `orders`, with a reason that starts with `nan`. If an earlier sync wrote row 2, the destination keeps that version of it, its `note` included:

| `id` | `price` | `note` |
|---|---|---|
| 1 | 10.00 | first |
| 3 | 30.00 | third |

To write row 2:

1. Fix the cause. Change the value at the source. Or give `price` a type that keeps `NaN`, as steps 1 to 3 below do.
2. Read the row again. On an incremental stream, update the row at the source. Or run `openetl reset --stream orders PIPELINE` to read the whole stream again.

The next sync writes row 2.

With `unfitValues: hold` and `held: null`, the destination gets all three rows. Row 2's `price` is null, and its `_openetl_held` names `price`:

| `id` | `price` | `note` |
|---|---|---|
| 1 | 10.00 | first |
| 2 | | second |
| 3 | 30.00 | third |

`openetl holds PIPELINE` shows a hold on cells of `price`. To clear the hold:

1. Set `columns.price.type: string` on the stream, so that `price` keeps the source's text.
2. Apply the pipeline.
3. Change the destination's `price` column to a text type.
4. Release the hold with `openetl holds release PIPELINE orders price`.
5. Start a sync.

The sync patches `NaN` into row 2, and the hold clears. If a cell still does not fit, it stays held.

## Choose between them

Keep the default, `quarantine`, when:

- readers must see only complete rows, such as amounts that a report adds up;
- the stream has no primary key, or the destination does not patch its write mode;
- you plan to load bad rows again yourself.

`quarantine` does not stop [column holds](#column-holds). A row that OpenETL writes can still have a held column. So a reader that needs complete rows checks `_openetl_held` and the pipeline's holds, whatever `unfitValues` says.

Choose `hold` when:

- a row with one missing value is better than a missing or old row, such as on a `merge` stream that other tables join to;
- one column sometimes has bad values, and the other columns must stay current;
- readers check `_openetl_held`, or can work with a null where OpenETL holds a value.

## Find what OpenETL kept back

Held values:

- On a pipeline's **Overview**, the **Held** column of the **Streams** table shows the held columns. The **Holds** list shows each hold and the ways to clear it ([Use the console](../use/freshness.md#overview)).
- `openetl holds PIPELINE` lists the holds. [Handle schema changes and holds](./schema-changes.md) shows how to release or discard one.
- The `openetl.job.cells.held` and `openetl.job.hold.age` metrics count held cells and give each hold's age ([Monitoring](../self-host/monitoring.md#pipeline-signals)).

Quarantined rows:

- The **Quarantined** counts on a pipeline's **Overview**, its **Syncs** and **Checkpoints** tabs, and a sync's page count the rows that OpenETL did not write. Select a count to see the stream's files, with their rows by cause. If you are an editor or above, **Show records** shows a file's first records ([Use the console](../use/held-data.md#quarantined-rows)).
- `openetl quarantine PIPELINE` lists the files of quarantined rows, newest first. It also adds up their rows by stream and cause, such as `312  type on amount`. Add `--stream STREAM` or `--since 7d` to list fewer files. The API method is `ListQuarantine`.
- `openetl quarantine show PIPELINE PATH` prints a file's first lines, so that you can pipe them to `jq`. It needs the editor role.
- The rows are in the data plane's object storage, under `_quarantine/` next to the pipeline's checkpoints, in files of JSON lines. A job reads in parallel parts, called lanes. For each stream, a commit has one file from each lane that quarantined rows: `STREAM.jsonl` from the first lane, and `STREAM.N.jsonl` from lane `N`. Read all of a commit's files for the stream.
- Each line has the stream, a code, the column, the reason and the record. On a stream with `unfitValues: hold`, it also has `notHeld`. [Quarantine files](../reference/quarantine-files.md) describes the format, which you can use to load the rows again yourself.
- OpenETL keeps the files of every commit, and deletes the files of commits that failed. To delete old files, set the pipeline's `quarantine.retention`, such as `720h`. Without it, OpenETL keeps the files.
- After each commit that quarantined rows, OpenETL sends a `stream.quarantined` event, with the commit's files and their rows by cause. A [webhook](../integrate/webhooks.md#events-and-webhooks) can deliver it to your own system.
- If a stream quarantines a large share of its rows, the **Quarantine** incident opens. Its **Next steps** names the newest file and the command that lists the others.
- The `openetl.job.rows.quarantined` metric counts quarantined rows.
