---
title: Handle schema changes and holds
description: Inspect a schema change, repair and release a hold, or choose a new column or discard its held values.
---

A stream's schema and a destination's existing columns are separate. OpenETL can widen the stream's type without permission to change the destination's column. By default, the destination refuses that type change and OpenETL holds the column where it can, or stops the stream. [Holds and quarantine](./holds-and-quarantine.md) explains holds; this guide walks through inspecting and clearing one.

## Choose the policy

Set `schemaPolicy` on the pipeline and `destination.typeChanges` on the write side. A stream can override `typeChanges` individually. These choices answer different questions:

| Setting | Decision |
|---|---|
| `schemaPolicy` | Whether new columns and changes to the stream's schema are accepted, held, versioned or ignored. |
| `destination.typeChanges` | Whether a destination connector may change an existing column's type. The default is `refuse`. |
| `source.streams[].unfitValues` | Whether a value that does not fit its type quarantines its row, which is not written, or holds its cell. Cell holds require a primary key and a destination that can patch values. [Holds and quarantine](./holds-and-quarantine.md) explains the difference. |
| `held`, `heldValue`, `groups` | What replaces held cells in delivered rows, and which columns must be held together. |

Use the [pipeline reference](../reference/specs/pipeline.md#schema) for each policy and its requirements. Allowing a type change can rewrite or lock a destination table; keep the default when a person should review that change first. A destination without hold support fails the stream rather than continuing with held values.

## Work through an example

This example uses the [official Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md), with a source and destination on a disposable database. Its SQL and column types are specific to that connector. Other connectors provide their own type mappings, requirements and repair steps.

Before starting, prepare these requirements:

- A running OpenETL installation and a configured CLI.
- The Postgres connector available to the runner.
- A disposable Postgres database reachable by the runner.
- A database account that can create schemas and alter their tables.
- An API key with the `editor` role or higher for hold operations.

The example creates `docs_source` and `docs_target`. Do not use a database that already contains schemas with those names.

### Create the source table and pipeline

In `psql`, connected to the disposable database:

```sql
create schema docs_source;
create table docs_source.orders (
  id bigint primary key,
  amount integer not null,
  updated_at timestamptz not null default now()
);
insert into docs_source.orders (id, amount) values (1, 100);
```

Store the database's DSN as a secret, replacing the connection details:

```sh
echo 'postgres://USER:PASSWORD@HOST:5432/DATABASE' | openetl secret set schema-demo-dsn
openetl plan -f examples/schema-holds.yaml
openetl apply -f examples/schema-holds.yaml
openetl syncs schema-demo
```

Run from the repository root. [`examples/schema-holds.yaml`](../../examples/schema-holds.yaml) defines both connections and the pipeline. It reads `docs_source.orders` incrementally by `updated_at`, merges by `id` into `docs_target.orders`, requires effectively-once delivery, and holds cells whose values do not fit (`unfitValues: hold`). It leaves `typeChanges` at `refuse`.

Wait until the first sync succeeds. Query the destination to confirm the row with `amount = 100` landed.

### Add a column and widen a type

On the source, add a column and write an amount that no longer fits a 32-bit integer:

```sql
alter table docs_source.orders add column note text;
alter table docs_source.orders alter column amount type bigint;
update docs_source.orders
set amount = 2147483648, note = 'new column', updated_at = clock_timestamp()
where id = 1;
```

Start another sync, then inspect its hold after the sync completes:

```sh
openetl sync schema-demo
openetl syncs schema-demo
openetl holds schema-demo
```

The hold explains that `amount` changed from `int32` to `int64` and that the destination refused to change its column. It lists a destination repair statement and commands for the available ways out. In the console, open the pipeline's **Overview**, then **Holds**, or inspect **Schemas** to compare the versions.

The new column has landed, and the held amount is null with this pipeline's defaults:

```sql
select id, amount, note from docs_target.orders order by id;
```

```text
 id | amount |    note
----+--------+------------
  1 |        | new column
(1 row)
```

A successful sync can therefore contain held values. Check holds as well as sync status when validating delivered data.

### Repair the destination and release

Review the statement in the hold's **To clear it** section. For this example, run it on the destination:

```sql
ALTER TABLE docs_target.orders ALTER COLUMN amount TYPE bigint;
```

Then approve the release and start the pipeline's next sync:

```sh
openetl holds release schema-demo orders amount
openetl sync schema-demo
openetl syncs schema-demo
openetl holds schema-demo
```

A release first becomes **Pending**. The pipeline applies it on its next run; it does not patch the destination during the CLI request. A continuous pipeline restarts its run to take the release. If a pipeline or stream is paused, resume it so the release can run.

After the sync succeeds, query the destination again. `amount` is now `2147483648`, and the hold is cleared.

If the release is refused, inspect its reason. Complete the remaining repair. Request release again. A refused release does not retry automatically.

If a sync fails with `CHECKPOINT_CORRUPT` while it applies a release, a held file that the release reads is missing or damaged. The error names the file, below `_held` in the data plane's storage, and the incident pauses the pipeline. Restore the file, then resume the pipeline.

## Choose another way out

The hold lists only the actions it can offer. Instead of changing the existing destination column, a column held whole can be released into a new version:

```sh
openetl holds release --new-version PIPELINE STREAM COLUMN
```

That sends the column's held and future values to `COLUMN__v2`, leaving the old column's type in place. It is unavailable for a hold on individual cells alone. Update downstream queries to use the new column when choosing this path.

To drop the held values, choose discard:

```sh
openetl holds discard PIPELINE STREAM COLUMN
```

The next run drops those saved values and leaves the destination's column as it is. Discard is not a repair to the source or schema policy. Do not run release and discard as consecutive recovery steps: they are different decisions about the same saved data.

A discarded hold on a column held whole stays in force, with the state **Discarded**. Each later row is written with the column's held cell, null or `heldValue`, and OpenETL keeps none of the column's later values. To deliver the column again:

1. Make the destination take the change, as for a release.
2. Release the discarded hold with `openetl holds release PIPELINE STREAM COLUMN`.
3. Start a sync.

The next run writes the column as the stream has it. The values that the discard dropped stay dropped. If the destination still refuses the column, OpenETL holds it again and keeps its values from that run on. A discarded hold cannot go to a new version.

## When a key or cursor changes type

A column of the primary key cannot go to a new column, such as `id__v2`. Neither can the cursor of a stream with a primary key, unless the stream is read with `cdc`.

Until the first checkpoint that records the stream's schema, such a column takes any widening. The stream's first schema has the type that its first rows give. For example, if a source sends `id` as a number in some of the first rows and as text in others, `id` is a `string`. A sync that fails before that checkpoint starts again with no schema.

After that checkpoint, such a column takes only these changes of type, which keep the order of its values and the record IDs (`_openetl_id`) that they give:

- An integer to a wider integer: `int16` to `int32` or `int64`, or `int32` to `int64`.
- A decimal to more digits at the same scale, such as `decimal(10, 2)` to `decimal(12, 2)`.
- A `json` column to a `string`, with the same text.
- For the cursor, an integer to a float, such as `int64` to `float64`. Each integer keeps its exact value: a column that has held an integer beyond ±2^53 becomes a `string` instead, which fails.

A timestamp that the source sends in another unit, such as milliseconds, does not change the column's type. OpenETL converts it to the stream's unit. In the same way, a string or binary value in another Arrow representation, such as `large_utf8` for `utf8`, keeps the column's type, for any column.

When the cursor changes from an integer to a float, `_openetl_seq` changes to the float too. The destination changes the type of the cursor's column and of `_openetl_seq` only if `destination.typeChanges` allows it. Otherwise it refuses, and the sync fails with `SCHEMA_DRIFT`, because the cursor cannot be held. The error gives the statement that changes the column on the destination. Run it, then start a sync. Do the same for each such error that follows, such as one for `_openetl_seq`. If the destination cannot make the change, the error gives no statement. Then use a new destination table, as described below.

Any other change to such a column's type fails the sync with `SCHEMA_DRIFT`. This includes the other widenings: to a string, which sorts `10` before `9`, and a key from an integer to a float. It also includes a `float32` to a `float64`, an integer to a decimal, and a decimal to another scale, because they change the record IDs. Changes that are not widenings fail too, before the first checkpoint as well, such as `bigint` to `uuid` or `timestamptz` to `timestamp`. The error names the column, its old and new types, and the way out. This happens under `allow_all` too. Under `ignore_new`, once the policy applies, the values are cast to the column's type instead.

A reset does not clear the error, because the stream keeps its schema. Choose one of these ways out:

- Change the column back to its old type at the source. Then start a sync.
- To keep the new type, delete the pipeline through the API. Then apply it again with a new destination table in `destination.tables`. The new pipeline has no checkpoints, so it reads the stream from the start. Its first schema has the new type if the rows before its first checkpoint have it, as all the rows of a database table do after a change to the column's type. If the source sends values of both types in one column, such as numbers in its first rows and text in later rows, the new pipeline can fail in the same way.

A sync can fail because the key or cursor went to a new column, such as `id__v2`, in an earlier sync. The rows written since then have a null key or a null `_openetl_seq`. Delete the pipeline and apply it again with a new destination table.

## Clean up the example

Pause the example so it no longer reads or writes:

```sh
openetl pause schema-demo
```

When you are done inspecting the result, remove only the schemas created for this example:

```sql
drop schema docs_source cascade;
drop schema docs_target cascade;
```

Keep the example pipeline paused until you delete its resource through the API. Dropping a destination's tables while its pipeline is active can make the next run recreate them or fail.
