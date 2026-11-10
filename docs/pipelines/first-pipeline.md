---
title: Your first pipeline
description: Copy tables from your own Postgres database into another, from checking the connection to the first sync.
---

This tutorial copies two tables between Postgres databases once an hour, using the official Postgres connector. You configure database access, store connection secrets, check the connections and discover the source tables. You then plan and apply the pipeline. The complete definition is in [`examples/postgres-to-postgres.yaml`](../../examples/postgres-to-postgres.yaml).

The database permissions, configuration and behavior in this tutorial are specific to the [Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md). For another connector or release, follow its documentation. [Choose and manage connectors](./connectors.md) explains installation and version selection. The connection checks, discovery, plan and apply workflow also apply to other connectors.

## Before you start

You need:

- OpenETL, the official connectors and the `openetl` CLI, configured as in the [quickstart](../get-started/quickstart.md). Run CLI commands from that repository directory so the CLI can find its API key.
- A source database named `app` and a destination database named `warehouse`. They can use the same server. Both must be reachable from the runner. With `openetld dev`, the runner runs on your computer.
- `psql`, as a user who may create roles and grant privileges on both databases.

Start with an empty `app` database. Connect to it with `psql`. Create these three example tables:

```sql
create table customers (
  id bigint primary key,
  name text not null,
  email text not null,
  updated_at timestamptz not null default now()
);
create table orders (
  id bigint primary key,
  customer_id bigint not null references customers (id),
  amount numeric(10, 2) not null,
  status text not null,
  updated_at timestamptz not null default now()
);
create table countries (
  code char(2) primary key,
  name text not null
);
insert into customers (id, name, email, updated_at) values
  (1, 'Ada Lovelace', 'ada@example.com', '2026-11-01 09:00:00+00'),
  (2, 'Alan Turing', 'alan@example.com', '2026-11-02 10:30:00+00'),
  (3, 'Grace Hopper', 'grace@example.com', '2026-11-03 14:15:00+00');
insert into orders (id, customer_id, amount, status, updated_at) values
  (101, 1, 25.00, 'paid', '2026-11-04 08:00:00+00'),
  (102, 2, 140.50, 'paid', '2026-11-04 09:10:00+00'),
  (103, 1, 9.99, 'pending', '2026-11-05 11:20:00+00'),
  (104, 3, 72.25, 'shipped', '2026-11-06 16:45:00+00');
insert into countries (code, name) values
  ('DE', 'Germany'), ('IN', 'India'), ('US', 'United States');
```

## 1. Give OpenETL a user on each database

For this example, OpenETL needs read access to `app` and permission to create a schema in `warehouse`. Connect to `app` with `psql`. Create a user with read access to the `public` tables:

```sql
create role openetl_reader login password 'PASSWORD';
grant usage on schema public to openetl_reader;
grant select on all tables in schema public to openetl_reader;
alter default privileges in schema public grant select on tables to openetl_reader;
```

Run `alter default privileges` as the role that creates your tables. It grants access to future tables created by that role.

Connect to `warehouse`. Create a user with permission to create a schema:

```sql
create role openetl_writer login password 'PASSWORD';
grant create on database warehouse to openetl_writer;
```

On its first sync, the pipeline creates a schema named `app` in `warehouse`, and `openetl_writer` owns it and its tables.

## 2. Store the connections as secrets

Connection configuration references a secret instead of including its password. Replace `HOST` and `PASSWORD` with your connection details. Store each database's connection URL:

```sh
echo 'postgres://openetl_reader:PASSWORD@HOST:5432/app' | openetl secret set app-db-dsn
echo 'postgres://openetl_writer:PASSWORD@HOST:5432/warehouse' | openetl secret set warehouse-dsn
```

```text
created workspaces/default/secrets/app-db-dsn
created workspaces/default/secrets/warehouse-dsn
```

For a server that requires TLS, add `?sslmode=require` to the URL.

## 3. Write the source and the destination

Make a file `app-db.yaml` with a `Source` and a `Destination`, both using the `postgres` connector:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: app-db
spec:
  connector: postgres
  config:
    dsn: { secret: app-db-dsn }
    schema: public
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: warehouse
spec:
  connector: postgres
  config:
    dsn: { secret: warehouse-dsn }
    schema: app
```

- The source `app-db` offers each table of `app`'s `public` schema as a stream.
- The destination `warehouse` writes each stream into a table of the same name in its `app` schema, which it creates if it's missing.

The [Postgres connector's page](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) lists all configuration fields. Apply the file:

```sh
openetl apply -f app-db.yaml
```

```text
created workspaces/default/sources/app-db (version 5e1d0feeafb7)
created workspaces/default/destinations/warehouse (version f9aec669685a)
```

This stores the source and destination. It does not start a sync because no pipeline exists yet.

## 4. Check both connections

Ask a runner to connect to each database with its config:

```sh
openetl check source app-db
openetl check destination warehouse
```

```text
workspaces/default/sources/app-db passed its check (version 5e1d0feeafb7)
workspaces/default/destinations/warehouse passed its check (version f9aec669685a)
```

A failed check identifies the missing requirement. For example, the default destination schema is `public`. On a default Postgres 15 or later installation, `openetl_writer` lacks permission to create tables there:

```text
openetl: workspaces/default/destinations/warehouse failed its check: PERMISSION_DENIED: postgres: no CREATE on schema "public"
```

Correct the database privileges or connection configuration. If you changed `app-db.yaml`, apply it again. Repeat both connection checks before continuing.

## 5. Look at the source's streams

List what the source offers:

```sh
openetl discover app-db
```

```text
countries (exact; key code)
  code  utf8
  name  utf8  nullable
customers (exact; key id)
  id          int64
  name        utf8                   nullable
  email       utf8                   nullable
  updated_at  timestamp[us, tz=UTC]  nullable
orders (exact; key id)
  id           int64
  customer_id  int64                  nullable
  amount       decimal(10, 2)         nullable
  status       utf8                   nullable
  updated_at   timestamp[us, tz=UTC]  nullable
```

Each table appears as a stream with its columns and delivery types:

- `key` identifies the primary key used for merge writes.
- `exact` means a read resumed from a checkpoint returns exactly the subsequent rows, without repeats. Incremental reads override this replay behavior.
- No stream declares a cursor column. The pipeline must select one for an incremental read.
- `nullable` means the connector can return null for that column. CDC deletes contain only the key, so other columns can be nullable even if the source declares `not null`.

`discover` stores this catalog for later plans.

## 6. Write the pipeline

Add the pipeline to the end of `app-db.yaml`, after another `---`:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: app-db-to-warehouse
spec:
  schedule: { every: 1h }
  source:
    ref: app-db
    streams:
      - name: customers
        sync: full_refresh
      - name: orders
        sync: incremental
        cursor: updated_at
  destination:
    ref: warehouse
    write: merge
  delivery:
    require: effectively_once
```

- `schedule` starts a sync every hour. [Schedules](./schedules.md) covers cron expressions and time zones.
- `streams` lists the tables to copy, so `countries` isn't copied. Without `streams`, the pipeline copies every stream the source offers.
- `customers` is read whole on every sync (`full_refresh`).
- `orders` uses `incremental` reads with the `updated_at` cursor. Each sync reads values at or above the highest cursor value from the previous sync. Your application must set `updated_at` on every change. Null cursor values and deleted rows are not read. To capture deletes, use `cdc` as described in the Postgres connector documentation.
- `write: merge` keeps one row per primary key in the destination, the latest version of the source's row.
- `delivery.require` sets the minimum [delivery guarantee](./delivery.md). Incremental reads repeat rows at the previous cursor value. Merge writes update the same rows, so `orders` receives effectively-once delivery. The default requirement is exactly once. Without `require: effectively_once`, `openetl apply` refuses this pipeline:

  ```text
  openetl: failed_precondition: the plan fails, so nothing was applied:
  workspaces/default/pipelines/app-db-to-warehouse: delivery.require: stream "orders" gets effectively_once (superset replay, written with merge), below exactly_once
  ```

The [pipeline reference](../reference/specs/pipeline.md) has every field.

## 7. Plan

See what applying the file would do:

```sh
openetl plan -f app-db.yaml
```

```text
would be unchanged workspaces/default/sources/app-db (version 5e1d0feeafb7)
would be unchanged workspaces/default/destinations/warehouse (version f9aec669685a)
would be created workspaces/default/pipelines/app-db-to-warehouse (version 1e56f1b4d3be)

workspaces/default/pipelines/app-db-to-warehouse (version 1e56f1b4d3be)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    customers  merge  exactly_once      exact replay
    orders     merge  effectively_once  superset replay, written with merge
```

The plan leaves the source and destination unchanged and creates the pipeline. It lists each stream's write mode, [delivery guarantee](./delivery.md) and reason. A guarantee below `delivery.require` appears under `problems`. Such a problem prevents `openetl apply` from applying the file.

## 8. Apply

```sh
openetl apply -f app-db.yaml
```

```text
unchanged workspaces/default/sources/app-db (version 5e1d0feeafb7)
unchanged workspaces/default/destinations/warehouse (version f9aec669685a)
created workspaces/default/pipelines/app-db-to-warehouse (version 1e56f1b4d3be)
```

A new pipeline starts its first sync immediately. List its syncs:

```sh
openetl syncs app-db-to-warehouse
```

```text
SYNC                                  STATE      STARTED              TOOK  ROWS  VERSION                                                           LAST COMMIT          LAG  ERROR
01a1bafe-9b3b-7003-a556-ed9689a1d1db  succeeded  2026-11-08 17:20:10  1s    7     1e56f1b4d3be6980b640291057e775781df8344060d443736c26b0b69f1c045d  2026-11-08 17:20:10  -
```

`ROWS` counts both streams: three customers and four orders. `openetl logs app-db-to-warehouse` prints the sync's log, which ends with the attempt's result:

```text
2026-11-08 17:20:11.010 INFO  attempt 1 sync attempt finished committed=1 generation=1 recovery=applied restored=0
```

A sync that fails has its error in the last column of `openetl syncs`, cut short, and in full in its log. The console shows the same under **Pipelines**, then **app-db-to-warehouse**.

## 9. Look at the data

In `psql`, connected to `warehouse`:

```sql
select table_name from information_schema.tables where table_schema = 'app' order by table_name;
```

```text
   table_name
----------------
 _openetl_fence
 _openetl_loads
 customers
 orders
(4 rows)
```

`customers` and `orders` contain the copied data. The connector uses `_openetl_fence` and `_openetl_loads` for exactly-once commits. Do not modify these tables.

```sql
select id, customer_id, amount, status, updated_at, _openetl_op, _openetl_deleted from app.orders order by id;
```

```text
 id  | customer_id | amount | status  |       updated_at       | _openetl_op | _openetl_deleted
-----+-------------+--------+---------+------------------------+-------------+------------------
 101 |           1 |  25.00 | paid    | 2026-11-04 08:00:00+00 | insert      | f
 102 |           2 | 140.50 | paid    | 2026-11-04 09:10:00+00 | insert      | f
 103 |           1 |   9.99 | pending | 2026-11-05 11:20:00+00 | insert      | f
 104 |           3 |  72.25 | shipped | 2026-11-06 16:45:00+00 | insert      | f
(4 rows)
```

Besides the source's columns, each row has the columns OpenETL adds, which the [quickstart](../get-started/quickstart.md#8-look-at-the-data) describes. A merged table also has `_openetl_deleted`, which is true for a row deleted in the source; only a `cdc` read sees deletes.

## 10. Sync the changes

Change an order and add one in `app`:

```sql
update orders set status = 'shipped', updated_at = '2026-11-07 10:00:00+00' where id = 103;
insert into orders (id, customer_id, amount, status, updated_at) values
  (105, 2, 18.00, 'paid', '2026-11-07 12:30:00+00');
```

Start a sync now, rather than waiting for the hour:

```sh
openetl sync app-db-to-warehouse
```

```text
started a sync of workspaces/default/pipelines/app-db-to-warehouse
```

Once it's done, list the syncs again:

```sh
openetl syncs app-db-to-warehouse
```

```text
SYNC                                  STATE      STARTED              TOOK  ROWS  VERSION                                                           LAST COMMIT          LAG  ERROR
01a1bafe-ce6b-7a29-801f-5f7a80824a09  succeeded  2026-11-08 17:20:23  1s    6     1e56f1b4d3be6980b640291057e775781df8344060d443736c26b0b69f1c045d  2026-11-08 17:20:24  -
01a1bafe-9b3b-7003-a556-ed9689a1d1db  succeeded  2026-11-08 17:20:10  1s    7     1e56f1b4d3be6980b640291057e775781df8344060d443736c26b0b69f1c045d  2026-11-08 17:20:10  -
```

This sync copied six rows:

- Three customers, because `customers` uses full refresh.
- Order 103, which changed.
- Order 105, which is new.
- Order 104, because its `updated_at` equals the previous sync's highest cursor value.

The destination keeps one row per order, with the latest values:

```sql
select id, status, updated_at from app.orders order by id;
```

```text
 id  | status  |       updated_at
-----+---------+------------------------
 101 | paid    | 2026-11-04 08:00:00+00
 102 | paid    | 2026-11-04 09:10:00+00
 103 | shipped | 2026-11-07 10:00:00+00
 104 | shipped | 2026-11-06 16:45:00+00
 105 | paid    | 2026-11-07 12:30:00+00
(5 rows)
```

## Next

- [Delivery guarantees](./delivery.md) explains what each guarantee promises, and how to read data delivered at least once.
- [Schedules](./schedules.md) covers cron expressions, time zones and jitter.
- [Pipelines as code](./index.md) has the commands for every day: pausing, resuming, versions and holds.
- The [pipeline reference](../reference/specs/pipeline.md) lists every field of a pipeline.
- The [Postgres connector's page](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) covers reading by `cdc`, which sees deletes, and the types each column arrives as.
