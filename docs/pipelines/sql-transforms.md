---
title: Write a SQL transform
description: Rename, filter and reshape a pipeline's streams with SQL statements, check them with openetl plan, and know what the destination gets.
---

A SQL transform changes rows between the source and the destination. You write `INSERT INTO … SELECT` statements in the pipeline's `transform.sql`. Each statement reads a source's stream and writes an **output**, which the destination gets as a stream. The engine runs the statements as it copies, in scheduled and continuous pipelines, with no other system to run.

The statements are row-wise: they rename, compute, filter and unnest columns, one input row at a time. [SQL transforms](../reference/sql.md) in the reference lists every statement, operator and function, and what is refused.

## Before you start

- The pipeline's source and destination are applied, as in [Pipelines as code](./index.md).
- The source's catalog declares the columns of the streams that the statements read. Run `openetl discover SOURCE` to see them. If the source infers columns from its rows, such as `faker`, sync the pipeline once without the transform first. The statements can then read the columns that the sync recorded.
- A runner of the pipeline's data plane is running. `openetl plan` sends the statements to it to plan them.

## A worked example

This example uses a database source named `shop-db`. Its catalog declares two tables, with these columns:

| Stream | Columns |
|---|---|
| `orders` | `id` (the primary key), `customer_id`, `status`, `amount` (a decimal), `details` (JSON), `tags` (a list of strings), `created_at`, `updated_at` |
| `customers` | `id` (the primary key), `email`, `name`, `country`, `updated_at` |

The destination is the built-in [`parquet`](../reference/built-in-connectors.md#parquet) destination, which writes Parquet files:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: lake
spec:
  connector: parquet
  config:
    url: file:///tmp/openetl-lake
```

### Write the statements

Save this pipeline as `shop-to-lake.yaml`:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-to-lake
spec:
  schedule: { every: 1h }
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: incremental
        cursor: updated_at
      - name: customers
        sync: incremental
        cursor: updated_at
  destination: { ref: lake }
  delivery:
    require: at_least_once
  transform:
    sql: |
      CREATE VIEW paid_orders AS
      SELECT id, customer_id, amount, details, tags, created_at
      FROM orders
      WHERE status = 'paid';

      INSERT INTO order_facts
      SELECT id AS order_id, customer_id, amount,
        details->>'coupon' AS coupon, created_at
      FROM paid_orders;

      INSERT INTO order_tags
      SELECT id AS order_id, unnest(tags) AS tag
      FROM paid_orders;

      INSERT INTO customers
      SELECT id, lower(email) AS email, country, updated_at
      FROM customers;
```

- `paid_orders` is a view: a named `SELECT` that the next two statements read. It filters `orders` to paid orders. A view writes nothing itself.
- `order_facts` is a new output. It renames `id` to `order_id`, and reads the `coupon` key of the JSON column `details` as text.
- `order_tags` is a new output with one row for each tag of each paid order.
- The last statement writes `customers`, which is a source's stream, so its output replaces that stream. It leaves out `name`, and writes `email` in lower case.
- `orders` is not written by any statement, so it passes through to the destination unchanged.
- `delivery.require` is lowered to `at_least_once`. [Delivery guarantees](#delivery-guarantees) explains why.

### Plan it

```sh
openetl plan -f shop-to-lake.yaml
```

The plan, without the job size:

```text
workspaces/default/pipelines/shop-to-lake (version known after apply)
  transform.sql:
    6:1   INSERT INTO order_facts  row_wise  key order_id            no exchange  reads orders through view paid_orders
    11:1  INSERT INTO order_tags   row_wise  no key, so append only  no exchange  reads orders through view paid_orders
    15:1  INSERT INTO customers    row_wise  key id                  no exchange  reads customers; replaces stream customers
    view paid_orders, at 1:13, reads orders
    output order_facts: order_id int64, customer_id int64, amount decimal(12, 2), coupon utf8, created_at timestamp[us, tz=UTC]
    output order_tags: order_id int64, tag utf8
    output customers: id int64, email utf8, country utf8, updated_at timestamp[us, tz=UTC]
    passed through: orders
  streams:
    orders       append  at_least_once  superset replay, and the destination can't dedupe
    order_facts  append  at_least_once  a transform's output over superset inputs, which isn't deduped unless written with merge
    order_tags   append  at_least_once  a transform's output over superset inputs, which isn't deduped unless written with merge
    customers    append  at_least_once  a transform's output over superset inputs, which isn't deduped unless written with merge
```

Read the `transform.sql` section:

- Each statement's line starts with its position in the SQL, as line and column.
- `order_facts` keeps the primary key of `orders`, renamed to `order_id`, because `id` passes through unchanged.
- `order_tags` has no primary key, because `unnest` gives several rows for one order. It can only be appended.
- The `output` lines give each output's columns and types.
- `passed through` lists the streams that no statement writes.

The `streams` section lists what the destination gets: the outputs, and `orders`.

### Apply and sync

```sh
openetl apply -f shop-to-lake.yaml
openetl sync shop-to-lake
```

The runner plans the statements again when the sync starts, against the columns the source declares then.

### What the destination gets

Take a paid order with two tags, and an unpaid one:

| `orders.id` | `status` | `amount` | `details` | `tags` |
|---|---|---|---|---|
| 1001 | `paid` | 42.50 | `{"coupon": "SPRING"}` | `["gift", "express"]` |
| 1002 | `pending` | 18.00 | `{}` | `["gift"]` |

After the sync, the destination has:

- `orders`: both rows, as without a transform.
- `order_facts`: order 1001, with `coupon` `SPRING`. Order 1002 fails the view's `WHERE`, so it has no row.
- `order_tags`: two rows for order 1001, with `tag` `gift` and `express`.
- `customers`: each customer, without `name`, and with `email` in lower case.

Each output gets the metadata columns that a stream gets, such as `_openetl_id`, and is named through `destination.naming` and `destination.tables`, as a stream is.

## Common tasks

### Rename, drop and compute columns

List the columns you want, and name each computed one with `AS`:

```sql
INSERT INTO contacts
SELECT id AS customer_id, lower(email) AS email, upper(country) AS country
FROM customers;
```

A column you do not list is not written. `SELECT *` writes every column, and a column the stream gains later. Unquoted column names match without regard to case.

### Filter rows

```sql
INSERT INTO big_orders
SELECT * FROM orders
WHERE amount >= 100 AND status IN ('paid', 'shipped');
```

### One row per list element

`unnest` gives one row for each element of a list:

```sql
INSERT INTO order_tags
SELECT id AS order_id, unnest(tags) AS tag
FROM orders;
```

An order with an empty or null list gives no row. Use `unnest_outer` to keep one row, with a null tag. The output has no primary key.

### Chain statements with views

A statement never reads another statement's output. To build on a result, name it with `CREATE VIEW`, and read the view in later statements. Define a view before what reads it.

```sql
CREATE VIEW eu_customers AS
SELECT id, lower(email) AS email FROM customers WHERE country = 'EU';

INSERT INTO eu_contacts SELECT id, email FROM eu_customers;
INSERT INTO eu_domains SELECT id, split_part(email, '@', 2) AS domain FROM eu_customers;
```

### Replace a stream, or add one

`INSERT INTO` a source's stream name replaces that stream: the destination gets the output in its place, in its table. Any other name adds an output of its own, which `destination.tables` can map to a table:

```yaml
  destination:
    ref: lake
    tables: { eu_contacts: contacts_eu }
```

### Choose which streams pass through

By default, every stream that no statement writes passes through to the destination. To deliver only what statements write, set `passthrough: none`:

```yaml
  transform:
    passthrough: none
    sql: |
      INSERT INTO order_facts
      SELECT id AS order_id, amount FROM orders;
```

With `none`, the plan lists the other streams under `not delivered, as passthrough is none`. A stream that no statement reads is read and dropped, and no sync records its columns.

### Keep the SQL in a file

Write `sql: { file: shop.sql }`, with a path relative to the pipeline's file. `openetl plan` and `openetl apply` read the file and send its text. The pipeline's version keeps the text.

### Read JSON

The operators `->`, `->>`, `#>`, `#>>` and `?`, and the `json_` functions, read JSON columns and strings:

```sql
INSERT INTO signups
SELECT id,
  details->>'plan' AS plan,
  CAST(details->'seats' AS BIGINT) AS seats,
  details->'address' AS address
FROM accounts
WHERE details ? 'plan';
```

`->>` gives text. `->` gives a JSON value: written to an output column, it is JSON, as the document writes it. Invalid JSON gives null. [JSON](../reference/sql.md#json) in the reference has the exact rules.

## Changelog streams

A stream read with `cdc` is a changelog: its rows are inserts, updates and deletes of the stream's primary key. This continuous pipeline keeps a table of open orders, in a destination that merges:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: open-orders
spec:
  mode: continuous
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: cdc
  destination: { ref: warehouse, write: merge }
  transform:
    passthrough: none
    sql: |
      INSERT INTO open_orders
      SELECT id, customer_id, status, amount, details
      FROM orders
      WHERE status <> 'closed';
```

Its plan, trimmed to the `transform.sql` section and the notes:

```text
  transform.sql:
    1:1  INSERT INTO open_orders  row_wise  key id  no exchange  reads orders
    output open_orders: id int64, customer_id int64, status utf8, amount decimal(12, 2), details extension<arrow.json>
    not delivered, as passthrough is none: orders
  notes:
    transform.sql:1:1: orders is a changelog: a row that an update brings into the WHERE without sending a column an output column needs, as Postgres leaves out a large value that didn't change, lands without that column's current value
```

On a changelog, the engine does this with each row:

- **Deletes pass.** A delete is written as a delete of its key.
- **A row that leaves the `WHERE` is deleted.** An insert or update that fails the `WHERE` becomes a delete of its key. When order 1001 is closed, `open_orders` loses it. With `destination.deletes: soft`, the default, the row stays with `_openetl_deleted` true; with `hard`, the destination removes it.
- **The key is kept.** A statement must pass the primary key through, renamed or not. A statement that drops or changes it is refused, and so is `unnest`.
- **An update that does not send a column keeps the destination's value.** Some sources leave a column out of an update when its value did not change, as Postgres does with large values. An output column that depends only on such columns is written unchanged.
- **Such an update can enter the `WHERE` without the value.** If the row failed the `WHERE` before, the destination does not have the column's current value. The note above says so. To avoid it, have the source send whole rows on update, as its connector's documentation describes. You can also read such tables in a scheduled pipeline with `full_refresh` or `incremental`, which read whole rows, or accept it.
- **Some updates cannot be evaluated.** If the `WHERE` reads a column that the update did not send, or an output column depends on a column it sent and one it did not, the engine quarantines the update.

The [Pipeline reference](../reference/specs/pipeline.md#statements-that-run) has the details.

## Delivery guarantees

A stream that passes through is delivered as it would be without the transform. A statement's output is not deduped when the destination applies it. So:

- Over streams that replay exactly from a resume point, which the plan's reasons call `exact replay`, an output can be delivered exactly once.
- Over a stream whose reads may repeat rows, such as an incremental read, an output is delivered at least once. Written with `merge`, it is effectively once.

The default `delivery.require` is `exactly_once`. Without `require: at_least_once`, the plan of the example has problems, and `openetl apply` refuses it:

```text
  problems:
    delivery.require: stream "orders" gets at_least_once (superset replay, and the destination can't dedupe), below exactly_once
    delivery.require: stream "order_facts" gets at_least_once (a transform's output over superset inputs, which isn't deduped unless written with merge), below exactly_once
    delivery.require: stream "order_tags" gets at_least_once (a transform's output over superset inputs, which isn't deduped unless written with merge), below exactly_once
    delivery.require: stream "customers" gets at_least_once (a transform's output over superset inputs, which isn't deduped unless written with merge), below exactly_once
openetl: the plan has problems, so apply would refuse it
```

To accept what the outputs get, do one of these:

- Lower `delivery.require` for the whole pipeline, as the example does.
- For an output that replaces a stream, lower that stream's `source.streams[].require`. The output takes the `require` of the stream it replaces, not of the stream it reads.

[Delivery guarantees](./delivery.md) explains the levels, and how to read an output delivered at least once.

## Rows a statement cannot evaluate

A row can fail when a statement runs, such as a cast of a value that does not convert. The engine quarantines that row, as one of the output's records, and writes the rest of the batch. Quarantined output rows go under `_quarantine/_outputs/`, next to the pipeline's checkpoints.

To keep such rows, use `TRY_CAST`, which gives null instead of failing:

```sql
INSERT INTO readings
SELECT id, TRY_CAST(raw_value AS DOUBLE) AS value
FROM sensor_events;
```

## When the source's schema changes

When a stream's schema changes during a sync, the engine plans the statements that read it again:

- A new column reaches an output only through `*`.
- An output's schema changes under the pipeline's `schemaPolicy`, as a stream's does.
- If a statement no longer plans, or an output's primary key would change, the sync fails with `SCHEMA_DRIFT`. Fix the statement, and apply the pipeline again.

The [Pipeline reference](../reference/specs/pipeline.md#statements-that-run) has the rest, including the stream settings a transform allows.

## When the SQL has errors

Errors are problems in the plan, with their line and column in the SQL. If the last statement of the example read `lower(mail)`, the plan would say:

```text
workspaces/default/pipelines/shop-to-lake (version known after apply)
  problems:
    transform.sql:16:18: there's no column mail; there are id, email, name, country, updated_at
openetl: the plan has problems, so apply would refuse it
```

Statements that the engine does not run, such as `GROUP BY`, joins and window functions, are problems too. [What's refused](../reference/sql.md#whats-refused) lists them with their messages.

If no runner takes the statements within 30 seconds, or a stream's columns are not known yet, the plan has a note that starts with `transform.sql wasn't planned`. The sync plans the statements when it starts, and fails with `CONFIG_INVALID` if they have errors.

## In the console

A pipeline with a SQL transform has a **SQL** tab on its page. It shows the statements, each output's columns and key, and the plan's notes and errors at their place in the SQL. [Use the console](../use/index.md#sql) describes it.
