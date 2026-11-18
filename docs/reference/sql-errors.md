---
title: SQL restrictions and errors
description: Look up refused SQL statements, clauses, names and stream operations.
---

Use this reference when a SQL transform fails to plan or evaluate.

## What's refused

The engine plans the statements when `openetl plan` or `openetl apply` checks a pipeline, and again when each sync or continuous job starts. A statement it refuses is a problem in the plan, written as `transform.sql:LINE:COLUMN: MESSAGE`, and `openetl apply` refuses the pipeline:

```text
  problems:
    transform.sql:16:18: there's no column mail; there are id, email, name, country, updated_at
```

If a sync or a continuous pipeline's job starts with statements that the engine refuses, it fails with `CONFIG_INVALID`, and its error has the same messages.

### Statements of other kinds

These are refused, each at the place it is written. The message says what was found.

| What | Message |
|---|---|
| A window function without `OVER` | `rank is a window function, which takes OVER: rank(…) OVER (PARTITION BY … ORDER BY …)` |
| `JOIN` | `a join: transforms don't run joins yet (tier 4)` |
| Two relations in `FROM` | `reading two relations: transforms don't run joins yet (tier 4)` |
| A subquery in an expression, such as `IN (SELECT …)` or `EXISTS (…)` | `a subquery: transforms don't run joins yet (tier 4)` |
| `GROUP BY` without a window | `GROUP BY without a window: transforms don't run updating statements yet (tier 5)` |
| An aggregate, such as `count(*)` or `sum(x)`, without a window | `an aggregate without a window: transforms don't run updating statements yet (tier 5)` |
| `HAVING` without a window | `HAVING without a window: transforms don't run updating statements yet (tier 5)` |
| `DISTINCT`, `DISTINCT ON` | `DISTINCT: transforms don't run updating statements yet (tier 5)` |
| A function that is not in [the list](./sql-functions.md#functions), such as a user-defined function | `my_udf isn't a function transforms can call` |

A view of one of these kinds is refused too, where the view is defined.

### Clauses and statements

| What | Message |
|---|---|
| `ORDER BY` | `a stream has no order, so ORDER BY is refused` |
| `LIMIT`, `OFFSET` | `a stream has no end, so LIMIT and OFFSET are refused` |
| `FETCH` | `a stream has no end, so FETCH is refused` |
| `UNION`, `INTERSECT`, `EXCEPT` | `UNION, INTERSECT and EXCEPT aren't supported` |
| `WITH` | `WITH isn't supported: name the query with CREATE VIEW, before the statements that read it` |
| `SELECT` without `FROM` | `a SELECT reads a stream or a view: it needs a FROM` |
| `VALUES` | `a statement reads a stream or a view, not VALUES` |
| `UNNEST` in `FROM` | `UNNEST goes in the SELECT list: SELECT unnest(list) AS item FROM …` |
| `INSERT INTO x (a, b) …` | `INSERT INTO takes an output's name and a SELECT, without a list of columns: the SELECT names the output's columns` |
| A computed column without `AS` | `name this column: lower(region) AS name` |
| A `SELECT` on its own | `a SELECT on its own writes nothing: write INSERT INTO output SELECT …, or name it with CREATE VIEW` |
| Other statements, such as `UPDATE`, `DELETE` or `CREATE TABLE` | `a transform's statements are INSERT INTO … SELECT and CREATE VIEW … AS SELECT, not DELETE FROM` |
| No `INSERT INTO` | `a transform writes at least one output, with INSERT INTO … SELECT` |
| A view more than 32 views deep | `view v32 is 33 views deep, counting each view it reads through: views go at most 32 deep; read a view nearer the stream, or merge views into one` |

### Names and columns

| What | Message |
|---|---|
| A stream or view that is not there | `there's no stream or view nope` |
| `FROM` an output | `cleaned is an output, and FROM reads a source's stream or a view: name the SELECT that writes it with CREATE VIEW, and read that` |
| Two statements that write one output | `x is written by an earlier statement: one statement writes each output` |
| A view read before it is defined | `view later is defined after this statement: define a view before the statements that read it` |
| A column that is not there | `there's no column nope; there are id, region, amount, created_at` |
| A quoted column in another case | `there's no column "employee_id": a quoted name matches exactly, so write "EMPLOYEE_ID", or the name unquoted` |
| A name that fits two columns or streams | `id fits the columns "id" and "ID", which differ only by case: quote the name to pick one` |
| A column name used twice | `there's already a column id: give this one another name with AS` |
| Types that do not combine | `expressions have incompatible types (has type Utf8; has type Int64)` |
| A type an output cannot hold | `column nothing is Null, which an output can't hold: CAST it to a type it can` |

### Group windows

| What | Message |
|---|---|
| A stream without an event time | `orders has no event time, which a group window goes by: set eventTime on the stream in source.streams` |
| A window over another column | `tumble goes by the event time of logins, at: write tumble("at", …)` |
| A window outside `GROUP BY` | `tumble is a group window, which goes in GROUP BY: select window_start and window_end for its bounds` |
| `window_start` or `window_end` in `WHERE` | `window_start is a window's bound, and WHERE runs before rows are grouped: compare it in HAVING` |
| Two windows | `a group window groups by one window: tumble, hop or session` |
| `ROLLUP`, `CUBE`, `GROUPING SETS` | `a group window groups by its window and columns, without ROLLUP, CUBE or GROUPING SETS` |
| A computed group key | `a group window groups by columns of orders, renamed or not, beside its window: lower(orders.region) is computed` |
| A group key of a list or a struct | `a group window can't group by tags, of type List(Utf8)` |
| A size in months | `tumble's sizes are in days, hours, minutes or seconds, as a month's length varies` |
| A size of zero or less | `tumble's sizes are more than zero, and under 290,000 years` |
| A hop's slide larger than its size | `hop's slide is at most its size, so every row is in a window: hop(time, slide, size)` |
| A hop with more than 1,000 windows a row | `hop puts each row in at most 1000 windows: its size over its slide is more` |
| An aggregate it does not run, such as `stddev` | `stddev isn't an aggregate transforms run: a group window takes count, sum, min, max, avg, approx_distinct, median and percentile_cont` |
| `DISTINCT` in an aggregate | `count(DISTINCT …) isn't supported: approx_distinct(x) estimates how many distinct values x has` |
| An output without `window_start` | `a group window's output keeps window_start, which with its group keys is its primary key: select window_start` |
| An output without a group key | `a group window's output keeps its group keys, which with window_start are its primary key: select region` |
| The window itself in the `SELECT` list | `select a window's bounds as window_start and window_end, not the window` |
| A group window in a view | `a view is row-wise, so it can't group by a window: group in the INSERT INTO … SELECT that reads the view` |
| A group window in a subquery | `a group window is in the statement's own SELECT, not in a subquery's` |
| A group window over a changelog stream | `changes is a changelog, and a group window groups appended rows: transforms don't run updating statements yet (tier 5)` |

The plan also has a problem where a group window's stream is read in a way that the window cannot take ([Reads](./sql-windows.md#reads)), and where a stream sets `watermark.lateness` and the window's output is not merged.

### Window functions

| What | Message |
|---|---|
| A stream without an event time | `orders has no event time, which OVER goes by: set eventTime on the stream in source.streams` |
| `ORDER BY` another column, or no `ORDER BY` | `OVER goes by the event time of orders, created_at: write ORDER BY created_at` |
| `ORDER BY` the event time and other columns | `OVER orders by the event time alone, which ties don't change: write ORDER BY created_at` |
| `ORDER BY … DESC`, other than in a dedupe | `OVER's frames end at the current row in event-time order: write ORDER BY created_at without DESC` |
| `ROW_NUMBER()` selected, not compared | `ROW_NUMBER() deduplicates, keeping the rows where it's 1: compare it = 1 in QUALIFY, or in the WHERE of a SELECT that reads it; count(*) OVER (… ROWS UNBOUNDED PRECEDING) numbers every row` |
| `ROW_NUMBER()` without `PARTITION BY` | `ROW_NUMBER() … = 1 keeps a row for each key of its PARTITION BY: partition by the columns that identify a row` |
| A dedupe that doesn't select a `PARTITION BY` column | `a deduplicated output keeps its PARTITION BY columns, which are its primary key: select customer` |
| `ROW_NUMBER()` and another function `OVER` in a statement | `ROW_NUMBER() … = 1 deduplicates on its own: compute other functions OVER in a statement of their own` |
| `ROW_NUMBER()` with an argument, `FILTER` or `DISTINCT` | `ROW_NUMBER() takes nothing but OVER (…)` |
| A frame that does not end at the current row | `OVER's frames end at the current row: write ROWS or RANGE BETWEEN … PRECEDING AND CURRENT ROW` |
| A frame that starts after the current row | `a frame starts before the current row, or at it` |
| `GROUPS` | `GROUPS frames aren't supported: write ROWS or RANGE` |
| A `RANGE` offset that is a number, such as `RANGE BETWEEN 5 PRECEDING AND CURRENT ROW` | `a RANGE frame's offset is an INTERVAL, such as INTERVAL '1 hour'` |
| A `RANGE` offset in months | `a RANGE frame's offset is in days, hours, minutes or seconds, as a month's length varies` |
| A `RANGE` offset in parts of a microsecond | `a RANGE frame's offset is whole microseconds` |
| A negative `RANGE` offset, or one that does not fit a 64-bit count of the event time's unit: about 292,000 years of microseconds, or about 292 years of nanoseconds | `a RANGE frame's offset is zero or more, and under 290,000 years` |
| An event time that is not a timestamp, such as a number | `OVER goes by a timestamp, not Int64`, with the column's type |
| `lead` | `lead reads rows after the current one, and OVER's frames end at the current row: lag reads the rows before it` |
| Another function, such as `last_value` | `last_value isn't a function OVER takes: OVER takes sum, count, min, max, avg, lag and first_value, and ROW_NUMBER() … = 1 to deduplicate` |
| `rank` | `rank(): transforms don't run updating statements yet (tier 5)` |
| `dense_rank` | `dense_rank(): transforms don't run updating statements yet (tier 5)` |
| `ROW_NUMBER()` compared otherwise than `= 1` | `ROW_NUMBER() compared other than = 1, as in a top-N: transforms don't run updating statements yet (tier 5)` |
| Two `PARTITION BY` or `ORDER BY` in a statement | `a statement's OVER clauses have one PARTITION BY and ORDER BY, by which one operator keeps their state: compute the others in a statement of their own` |
| A computed `PARTITION BY` column | `OVER partitions by columns of orders, renamed or not: lower(orders.region) is computed` |
| A `PARTITION BY` that is not deterministic | `random() isn't deterministic, so it can't feed a window's partition` |
| `PARTITION BY` a list or a struct | `OVER can't partition by tags, of type List(Utf8)` |
| `DISTINCT` in `count`, `sum` or `avg` | `count(DISTINCT …) isn't supported over a frame` |
| `IGNORE NULLS` | `first_value(…) IGNORE NULLS isn't supported` |
| `FILTER` on `lag` or `first_value` | `lag takes no FILTER, which is for aggregates` |
| `lag`'s offset other than a constant of 1 or more | `lag's offset is a constant whole number, 1 or more` |
| `lag`'s default other than a constant | `lag's default is a constant, of the value's type` |
| `sum` or `avg` of a value that is not a number | `there's no sum(Utf8): CAST the argument to a type it takes`, with the function and the value's type |
| `min`, `max`, `first_value` or `lag` of a list, a struct or a map | `first_value takes a number, text, binary, a boolean, a date, a time, a timestamp or a UUID, not List(Utf8)`, with the function and the value's type |
| `OVER` over a changelog stream | `cdc is a changelog, and OVER reads appended rows: transforms don't run updating statements yet (tier 5)` |
| `OVER` and a group window in one statement | `a statement groups by a window or computes OVER, not both: compute OVER in a statement of its own` |
| `OVER` in a view | `a view is row-wise, so it can't compute OVER: compute it in the INSERT INTO … SELECT that reads the view` |
| `OVER` in `WHERE` | `WHERE runs before OVER, so it can't read OVER's values: compare them in QUALIFY, or in the WHERE of a SELECT that reads this one` |
| `WINDOW` | `WINDOW isn't supported: write each OVER (…) in full` |
| `OVER` over another `OVER`'s values | `OVER over the values of another OVER isn't supported: compute a statement's OVER over the stream's rows` |
| `unnest` of `OVER`'s rows | `UNNEST of OVER's rows isn't supported: unnest the stream's rows before OVER, in a view` |

The plan also has a problem where the stream of `OVER` is read in a way that it cannot take ([OVER reads](./sql-over.md#over-reads)), where the stream sets `watermark.lateness` and no group window reads it, and where the output is merged and its primary key does not hold the `PARTITION BY` columns, on a destination with several writer lanes ([The output](./sql-over.md#the-output)).

A [dedupe](./sql-over.md#dedupe) has these problems in the plan, each about the spec, not the SQL. A job that starts with a first-row dedupe over a stream without `dedupe.retention` fails with `CONFIG_INVALID` and the same message.

| What | Message |
|---|---|
| A first-row dedupe over a stream without `dedupe.retention` | `transform.sql:1:1: the dedupe writing "events_once", ROW_NUMBER() … = 1 in ascending order of the event time, keeps each key's first row, and stream "events" sets no dedupe.retention, how long it remembers a key: set source.streams[].dedupe.retention to a duration of event time, such as 24h, after which a key's next row is written again, or to unbounded, to remember every key` |
| A latest-row dedupe whose output isn't merged | `transform.sql:1:1: the dedupe writing "latest_profiles", ROW_NUMBER() … = 1 in descending order of the event time, keeps each key's latest row, as upserts and deletes that only a merge applies, and the output is written with append: set destination.write: merge, on a destination that merges` |
| A first-row dedupe over a stream read by another cursor than its event time | `the dedupe writing "events_once" reads stream "events" with incremental by cursor "id", not by its eventTime "created_at", so each sync's read would start its keys again, and write each key's first row again: read it with full_refresh` |
| `watermark.lateness` on a stream that a latest-row dedupe reads | `stream "profile_changes" sets watermark.lateness, which the dedupe writing "latest_profiles" doesn't take: it writes each key's latest row as it comes, whatever the watermark. Set no lateness` |
| `dedupe.retention` on a stream that no first-row dedupe reads | `stream "profile_changes" sets dedupe.retention, which only a dedupe that keeps each key's first row takes, ROW_NUMBER() … = 1 in ascending order of its eventTime, and no statement of transform.sql dedupes it so: set no dedupe` |
| A `dedupe.retention` under 1µs | `stream "events" sets dedupe.retention 1ns, under the microsecond that event time counts in: set 1µs or more, or unbounded` |

### Changelog streams

| What | Message |
|---|---|
| A statement that drops a key column | `cdc is a changelog, so what reads it keeps its key (id) unchanged, renamed or not; this drops it` |
| A statement that changes a key column, such as `id + 1` or a cast | `… keeps its key (id) unchanged, renamed or not; this changes it` |
| `unnest` | `… keeps its key (id) unchanged, renamed or not; this multiplies the rows of it` |
| `WHERE` on a changelog stream without a primary key | `log is a changelog without a primary key, so WHERE is refused, as a row that fails it has no key to delete it by: give the stream a primary key at its source, or read it with full_refresh or incremental in a scheduled pipeline` |

### When rows run

A statement that plans can still fail on a row. Examples are a cast of a value that does not convert, and an integer divided by zero. The engine quarantines such a row, and writes the rest of the batch. The [Pipeline reference](./specs/pipeline.md#statements-that-run) says where quarantined rows go, and what a change to a stream's schema does to the statements that read it. [Quarantine files](./quarantine-files.md) gives the format of their lines, and the code of each failure.

The settings that streams can and cannot have beside `transform.sql` are in the [Pipeline reference](./specs/pipeline.md#transforms).
