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
| Where you see it | **Held** in the console, and `openetl holds` | **Quarantined** in the console, and the **Quarantine** incident |
| Where OpenETL keeps it | Parquet files under `_held/`, next to the pipeline's checkpoints | JSON lines under `_quarantine/`, next to the pipeline's checkpoints |

A hold keeps a value back from a row that OpenETL writes. Quarantine keeps a row back from the destination, as a dead-letter queue does. A hold has a way back into the destination. A quarantined row has none.

The files are different because their data is different. A held value has a type and a key, and OpenETL reads it back as a patch. So OpenETL keeps held values in Parquet. A quarantined record did not fit its types, and the records of one commit can have different shapes. So OpenETL keeps each record as the source sent it, in JSON lines that you can read with `jq`.

## Why there are two

OpenETL applies two rules to data it cannot write:

- **No data goes away without a record.** OpenETL keeps every value that it does not write, with the reason, until a person decides what to do with it.
- **The rest of the data continues.** One bad value does not stop a stream, and one stream does not stop the pipeline, where OpenETL can prevent it.

Quarantine and holds apply these rules at two sizes:

- **Quarantine treats the row as the unit.** The destination gets only complete rows whose values fit their types. A row with a bad value is missing until you load it again. On a `merge` stream, the key's other columns keep their old values until the source changes the row again. If the row was an insert, rows that refer to it point at nothing.
- **A hold treats the value as the unit.** OpenETL writes the row, and its other columns are current. Only the held value is missing. The row's `_openetl_held` column names the held columns. A reader that does not check `_openetl_held` takes a partial row for a complete one.

A partial row changes what a row means. So OpenETL holds single values only if you tell it to. By default, a value that does not fit quarantines its row. A change to a whole column is different, because a stop for that change would stop every row of the stream. So OpenETL holds the column by default where it can ([Column holds](#column-holds)).

## Choose what a value that does not fit does

A value does not fit when its column's type cannot keep it. The reason for each quarantined row and each held cell starts with one of these codes:

| Code | The value |
|---|---|
| `type` | Is not of the column's type, such as text in a number column |
| `range` | Is outside the type's range, such as an infinite date with `types.infinity: unfit` |
| `precision` | Has more digits or decimal places than the column keeps |
| `nan` | Is NaN, in a type that has no NaN |

The reason names the column. It never contains the value.

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
- **A SQL transform reads the row.** A statement cannot compute from a value that OpenETL holds. So OpenETL quarantines a row whose input holds a cell, as one of the statement output's records. A delete is the exception: it passes with its key. OpenETL also quarantines a row that a statement cannot evaluate, whatever `unfitValues` says. Examples are a cast that fails, a division by zero and a row that comes too late for its window ([SQL reference](../reference/sql.md)).

A stream without a primary key cannot set `hold`. The streams of a pipeline with a Spark or Flink transform cannot set it either. If they do, the plan has a problem.

## Column holds

`unfitValues` decides only what single values do. In these cases, OpenETL holds a whole column, whatever `unfitValues` says:

- the destination refuses a change to the column's type, which it does unless `typeChanges` is `allow`;
- the schema policy does not take the change: `allow_columns` for a type change that is not a widening, and `block` for any change;
- the destination cannot add more columns.

A column hold needs what a cell hold needs: a primary key and a destination that patches the stream's write mode. If OpenETL cannot hold the column, the sync fails with `SCHEMA_DRIFT`. A change to a column does not quarantine rows. [Handle schema changes and holds](./schema-changes.md) walks through a column hold and its release.

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

Choose `hold` when:

- a row with one missing value is better than a missing or old row, such as on a `merge` stream that other tables join to;
- one column sometimes has bad values, and the other columns must stay current;
- readers check `_openetl_held`, or can work with a null where OpenETL holds a value.

## Find what OpenETL kept back

Held values:

- On a pipeline's **Overview**, the **Held** column of the **Streams** table shows the held columns. The **Holds** list shows each hold and the ways to clear it ([Use the console](../use/index.md#overview)).
- `openetl holds PIPELINE` lists the holds. [Handle schema changes and holds](./schema-changes.md) shows how to release or discard one.
- The `openetl.job.cells.held` and `openetl.job.hold.age` metrics count held cells and give each hold's age ([Monitoring](../self-host/monitoring.md#pipeline-signals)).

Quarantined rows:

- The **Quarantined** counts on a pipeline's **Overview**, its **Syncs** and **Checkpoints** tabs, and a sync's page count the rows that OpenETL did not write.
- The rows are in the data plane's object storage, under `_quarantine/` next to the pipeline's checkpoints, in files of JSON lines. A job reads in parallel parts, called lanes. For each stream, a commit has one file from each lane that quarantined rows: `STREAM.jsonl` from the first lane, and `STREAM.N.jsonl` from lane `N`. Read all of a commit's files for the stream. Each line has the stream, the reason and the record. OpenETL keeps the files of every commit and deletes the files of commits that failed.
- If a stream quarantines a large share of its rows, the **Quarantine** incident opens, and its **Next steps** names the newest file.
- The `openetl.job.rows.quarantined` metric counts quarantined rows.
