---
title: SQL transforms
description: The statements, names, types, operators and functions a pipeline's transform.sql can use, and what the engine refuses.
---

| Look up | Reference |
|---|---|
| Statement syntax and names | [Statements](#statements) |
| Time windows and aggregates | [SQL group windows](./sql-windows.md) |
| Operators, data types and casts | [SQL expressions and types](./sql-expressions.md) |
| Built-in functions | [SQL functions](./sql-functions.md) |
| Unsupported operations and error messages | [SQL restrictions and errors](./sql-errors.md) |

A SQL transform is the `transform.sql` field of a [Pipeline](./specs/pipeline.md#a-sql-transform). Its statements read the source's streams and write outputs, which the destination gets as streams. The engine runs them on every lane of a sync or a continuous pipeline's job, between typing a stream's rows and writing them. [Write a SQL transform](../pipelines/sql-transforms.md) shows how to write one and check it.

The engine runs row-wise statements. Each output row comes from one input row or one list element expanded with `UNNEST`.

The engine also runs [group windows](./sql-windows.md#group-windows). These aggregate a stream's rows by windows of event time. Unsupported statements fail with their position in the SQL text. [SQL restrictions and errors](./sql-errors.md#whats-refused) lists the messages.

## Statements

A transform is one or more statements, each ended by `;`. The last `;` is optional. Comments start with `--` or are written as `/* … */`.

| Statement | What it does |
|---|---|
| `INSERT INTO OUTPUT SELECT …` | Writes the `SELECT`'s rows to the output `OUTPUT`. |
| `CREATE VIEW NAME AS SELECT …` | Names a `SELECT` that later statements and views read as if it were a stream. A view writes nothing. |

- A transform has at least one `INSERT INTO`.
- One statement writes each output.
- `INSERT INTO` takes no list of columns: the `SELECT` names the output's columns. It takes no `ON CONFLICT`.
- `CREATE VIEW` takes a name and `AS SELECT`, and nothing else: not `OR REPLACE`, `TEMPORARY`, `MATERIALIZED` or a list of columns.
- A view is defined before the statements and views that read it.
- Views go at most 32 deep. A view that reads no view is 1 deep, and a view that reads views is one deeper than the deepest of them. A deeper view is a problem in the plan, at its name.
- Statements read the source's streams, never an output. To read what a statement writes, define its `SELECT` as a view, and read the view.

## What FROM reads

A `SELECT` reads one relation, which its `FROM` names:

- A source's stream, by its whole name. See [Stream names](#stream-names).
- A view.
- A subquery in parentheses with an alias, such as `FROM (SELECT …) AS t`, which is read as a view is.

A `SELECT` without a `FROM`, one that reads `VALUES`, and one that reads two relations are refused. A statement reads a stream's rows as the source sends them, under the source's column names and with the columns' logical types.

## Names

### Stream names

A stream's name is its whole name in the source's catalog, such as `public.orders` or `HR.EMPLOYEES`.

- Write it as a dotted path, `public.orders`, or quoted whole, `"public.orders"`.
- Unquoted parts match without regard to case: `PUBLIC.Orders` reads `public.orders`. Quoted parts match exactly.
- If two streams differ only by case, quote the name to pick one.
- `source.` before a stream's name is optional: `FROM source.public.orders` reads `public.orders`. If the source has a stream whose name starts with `source.`, then `source.` is the qualifier, and `FROM "source.orders"` reads that stream. The plan has a note when both readings exist.

### Output names

`INSERT INTO` names the output.

- If the name is a source stream's, the output replaces that stream. The destination gets the output in its place, under the stream's entry in `destination.tables`, and with its `require`. Name the stream as `FROM` does, at any depth: `INSERT INTO public.orders`.
- Otherwise, the output is new, and its name is one identifier. Quote a name that has dots: `INSERT INTO "eu.orders"`. An unquoted name is folded to lower case.
- `destination.` before an output's name is optional: `INSERT INTO destination.customers` writes `customers`. `source.` is refused there.

A stream that no statement replaces passes through to the destination as it would without a transform, unless `transform.passthrough` is `none`.

### View names

A view's name is one identifier, never qualified. Quote a name that has dots. An unquoted name is folded to lower case. A view cannot have the name of a source's stream, of an output or of another view. Views take no `source.` qualifier.

### Column names

- An unquoted column name matches a column without regard to case, when only one column fits. A quoted name matches exactly.
- If two columns differ only by case, quote the name to pick one.
- A column that passes through keeps the source's spelling. A name given with `AS` is folded to lower case, unless it is quoted.
- A column can be qualified by the alias that `FROM` gives, or, without one, by the name that `FROM` reads: `orders.id` or `public.orders.id`.
- An output's column names are unique without regard to case.
- A column that is not a plain column needs a name: write `lower(email) AS email`, not `lower(email)`.

## What a statement can do

| What | How | Notes |
|---|---|---|
| Choose and rename columns | `SELECT id AS order_id, status` | |
| Every column | `SELECT *`, or `SELECT o.*` | A column that the stream gains later reaches the output only through `*`. |
| Compute a column | `SELECT amount * 100 AS cents` | Expressions, operators and [functions](./sql-functions.md#functions) |
| Literals | `'eu' AS region`, `1 AS version` | A `NULL` literal needs a cast: `CAST(NULL AS VARCHAR) AS note` |
| Filter rows | `WHERE status = 'paid'` | |
| One row for each element of a list | `SELECT id, unnest(tags) AS tag` | See [UNNEST](#unnest) |
| Read a view or a subquery | `FROM paid_orders`, `FROM (SELECT …) AS t` | |

### UNNEST

`unnest(LIST) AS NAME` in the `SELECT` list gives one output row for each element of the list, with the row's other columns repeated.

- A null or empty list gives no row. `unnest_outer(LIST)` gives one row with a null for it.
- Several `unnest` calls in one `SELECT` give rows side by side: the first elements together, then the second ones.
- `unnest` flattens one level. Use `flatten()` first for a list of lists.
- `unnest` takes a list, not a struct. It goes in the `SELECT` list, not in `FROM`.
- Each row that `unnest` gives has an `_openetl_id` of its own, derived from its input row's.

### Primary keys

An output's primary key is the primary key of the stream it reads when both of these are true:

- Each key column passes through unchanged, renamed or not.
- The statement does not use `unnest`, which can give several rows for one input row.

Otherwise the output has no primary key, and is appended or overwritten, never merged. An expression over a key column, such as `id + 1`, drops the key. A view passes a key on by the same rule.

### Changelog streams

A changelog stream is one whose rows are inserts, updates and deletes of their key, such as a stream read with `cdc`. A statement that reads one:

- Keeps the stream's primary key, unchanged and renamed or not. A statement that drops or changes a key column is refused.
- Does not use `unnest`.
- Has no `WHERE` if the stream has no primary key. Such a statement is refused, unless planning simplifies its `WHERE` to true, as with `WHERE TRUE`, which leaves no filter.
- Passes each delete without its `WHERE`. The statement's `SELECT` list runs on the values the source sent with the delete, and gives the delete's columns. A column that the source did not send is null before the `SELECT` list runs, so an expression such as `1 AS version` still gives its value.
- Passes a delete with its key alone if the statement cannot evaluate the delete, or if the values it gives do not fit the output's column types. The delete's other columns are then null. A delete is not quarantined. If the output holds cells, a value that does not fit is held instead, as it is on a stream that passes through, and the delete passes with its other values.
- Turns an insert or update that fails its `WHERE` into a delete of its key alone. The delete's other columns are null.

The [Pipeline reference](./specs/pipeline.md#statements-that-run) says how the destination applies these deletes, and what happens to an update that does not send every column.

### Changelog streams

See [Changelog streams](./sql-errors.md#changelog-streams).

## Group windows

See [Group windows](./sql-windows.md#group-windows).

### Windows

See [Windows](./sql-windows.md#windows).

### Bounds and keys

See [Bounds and keys](./sql-windows.md#bounds-and-keys).

### Aggregates

See [Aggregates](./sql-windows.md#aggregates).

### When a window's row is written

See [When a window's row is written](./sql-windows.md#when-a-windows-row-is-written).

### Reads

See [Reads](./sql-windows.md#reads).

### State

See [State](./sql-windows.md#state).

### Changing a query

See [Changing a query](./sql-windows.md#changing-a-query).

## Expressions and operators

See [Expressions and operators](./sql-expressions.md#expressions-and-operators).

### Time zones

See [Time zones](./sql-expressions.md#time-zones).

## Types

See [Types](./sql-expressions.md#types).

### Casts

See [Casts](./sql-expressions.md#casts).

## Functions

See [Functions](./sql-functions.md#functions).

### Conditionals

See [Conditionals](./sql-functions.md#conditionals).

### Strings

See [Strings](./sql-functions.md#strings).

### Regular expressions

See [Regular expressions](./sql-functions.md#regular-expressions).

### Numbers

See [Numbers](./sql-functions.md#numbers).

### Dates and times

See [Dates and times](./sql-functions.md#dates-and-times).

### Hashes and encodings

See [Hashes and encodings](./sql-functions.md#hashes-and-encodings).

### Lists

See [Lists](./sql-functions.md#lists).

### Structs

See [Structs](./sql-functions.md#structs).

### JSON

See [JSON](./sql-functions.md#json).

## What's refused

See [What is refused](./sql-errors.md#whats-refused).

### Statements of other kinds

See [Statements of other kinds](./sql-errors.md#statements-of-other-kinds).

### Clauses and statements

See [Clauses and statements](./sql-errors.md#clauses-and-statements).

### Names and columns

See [Names and columns](./sql-errors.md#names-and-columns).

### Group windows

See [Group windows](./sql-errors.md#group-windows).

### Changelog streams

See [Changelog streams](./sql-errors.md#changelog-streams).

### When rows run

See [When rows run](./sql-errors.md#when-rows-run).
