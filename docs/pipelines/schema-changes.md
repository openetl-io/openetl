---
title: Handle schema changes and holds
description: Inspect a schema change, repair and release a hold, or choose a new column or discard its held values.
---

A stream's schema and a destination's existing columns are separate. OpenETL can widen the stream's type without permission to change the destination's column. By default, the destination refuses that type change and OpenETL holds the column where it can, or stops the stream. [Concepts](../get-started/concepts.md#schema-changes-and-holds) explains holds; this guide walks through inspecting and clearing one.

## Choose the policy

Set `schemaPolicy` on the pipeline and `destination.typeChanges` on the write side. A stream can override `typeChanges` individually. These choices answer different questions:

| Setting | Decision |
|---|---|
| `schemaPolicy` | Whether new columns and changes to the stream's schema are accepted, blocked, versioned or quarantined. |
| `destination.typeChanges` | Whether a destination connector may change an existing column's type. The default is `refuse`. |
| `source.streams[].quarantine` | Whether a value that does not fit holds back its row or its cell. Cell holds require a primary key and a destination that can patch values. |
| `held`, `heldValue`, `groups` | What replaces held cells in delivered rows, and which columns must be held together. |

Use the [pipeline reference](../reference/specs/pipeline.md#schema) for each policy and its requirements. Allowing a type change can rewrite or lock a destination table; keep the default when a person should review that change first. A destination without hold support fails the stream rather than continuing with held values.

## Work through an example

This example uses the [official Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md), with a source and destination on a disposable database. Its SQL and column types are specific to that connector. Other connectors provide their own type mappings, requirements and repair steps.

You need OpenETL and the CLI running, the connector available to the runner, a Postgres database reachable by that runner, and an account that can create the two example schemas and alter their tables. Use an editor or owner API key for the hold operations. Do not use existing schemas named `docs_source` or `docs_target` for this example.

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

Run from the repository root. [`examples/schema-holds.yaml`](../../examples/schema-holds.yaml) defines both connections and the pipeline. It reads `docs_source.orders` incrementally by `updated_at`, merges by `id` into `docs_target.orders`, requires effectively-once delivery, and uses cell quarantine. It leaves `typeChanges` at `refuse`.

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

After the sync succeeds, query the destination again. `amount` is now `2147483648`, and the hold is cleared. If the release is refused, inspect the new reason, make the remaining repair, and release it again; a refused release is not retried until you request another one.

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

The next run drops those saved values and leaves the destination's column as it is. Discard is not a repair to the source or schema policy; understand how later values will be handled before choosing it. Do not run release and discard as consecutive recovery steps: they are different decisions about the same saved data.

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
