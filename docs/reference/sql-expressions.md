---
title: SQL expressions and types
description: Look up SQL operators, time zones, data types and casts.
---

Use this reference when you write a [SQL transform](../pipelines/sql-transforms.md).

## Expressions and operators

| Operator or syntax | Meaning |
|---|---|
| `+`, `-`, `*`, `/`, `%` | Arithmetic. A timestamp plus or minus an `INTERVAL` is a timestamp. |
| `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=` | Comparison |
| `AND`, `OR`, `NOT` | Logic |
| `IS NULL`, `IS NOT NULL`, `IS TRUE`, `IS NOT TRUE` and the like | Tests that are never null |
| `IS DISTINCT FROM`, `IS NOT DISTINCT FROM` | Comparison that treats two nulls as equal |
| `IN (…)`, `NOT IN (…)` | Membership in a list of values. A list of values only: a subquery is refused. |
| `BETWEEN … AND …` | A range, both ends included |
| `LIKE`, `ILIKE`, `SIMILAR TO` | Pattern matching. `ILIKE` ignores case. |
| `~`, `~*`, `!~`, `!~*` | Regular expression matching. `*` ignores case, and `!` negates. |
| `\|\|` | String concatenation |
| `CASE WHEN … THEN … ELSE … END`, `CASE x WHEN … END` | Conditional value |
| `CAST(x AS TYPE)`, `x::TYPE` | Conversion. A value that cannot be converted fails its row. See [Types](#types). |
| `TRY_CAST(x AS TYPE)` | Conversion that gives null for a value that cannot be converted |
| `list[i]` | A list's element, counting from 1 |
| `s['field']` | A struct's field |
| `EXTRACT(PART FROM ts)` | `date_part('PART', ts)` |
| `POSITION(sub IN s)` | `strpos(s, sub)` |
| `SUBSTRING(s FROM start FOR length)` | `substr(s, start, length)` |
| `TRIM([BOTH \| LEADING \| TRAILING] chars FROM s)` | `btrim`, `ltrim` or `rtrim` |
| `OVERLAY(s PLACING r FROM start [FOR length])` | `overlay` |
| `DATE '2026-01-01'`, `TIMESTAMP '2026-01-01 00:00:00'`, `INTERVAL '1 day'` | Literals of those types |
| `ts AT TIME ZONE zone` | A timestamp in a time zone: see [Time zones](#time-zones) |
| `->`, `->>`, `#>`, `#>>`, `?` | JSON paths: see [JSON](./sql-functions.md#json) |

### Time zones

`ts AT TIME ZONE zone`, and the function `timezone(zone, ts)`, convert between a timestamp with a time zone and the wall-clock time in a zone:

| `ts` is | The result is |
|---|---|
| A timestamp without a time zone | The instant at which a clock in `zone` shows `ts`, as a timestamp with a time zone |
| A timestamp with a time zone | The time that a clock in `zone` shows at the instant `ts`, as a timestamp without a time zone |

A timestamp with a time zone and a timestamp without one are different types of output column. A destination writes each as its own column type, as it does for `CAST(ts AS TIMESTAMP)`.

```sql
INSERT INTO orders_local
SELECT id,
       created_at AT TIME ZONE 'America/New_York' AS created_local,
       CAST(created_at AT TIME ZONE 'Asia/Kolkata' AS DATE) AS india_day,
       local_time AT TIME ZONE store_zone AS happened_at
FROM orders
```

- `zone` is the IANA name of a time zone, such as `'Europe/Berlin'`, in any case, or `'UTC'`. It can come from a column, so it can differ by row.
- An offset such as `'+05:30'` or `'UTC-3'` is not a zone. Write a zone's name, such as `'Asia/Kolkata'`.
- An abbreviation such as `'CET'` or `'PST'` is not a zone. Write a zone's name, such as `'Europe/Berlin'`.
- A zone written as a literal, or as an expression of literals such as `'Europe/' || 'Berlin'`, is checked at plan time. If it is not a zone, the statement is refused.
- A zone from a column is checked for each row. If it is not a zone, the row fails.
- Daylight saving can skip a wall-clock time or repeat it. A skipped time is read with the offset before the change, and a repeated time with the offset after it. Each gives the later of the two instants. In Berlin, `TIMESTAMP '2024-03-31 02:30:00'` is `2024-03-31 01:30:00+00`, and `TIMESTAMP '2024-10-27 02:30:00'` is `2024-10-27 01:30:00+00`.
- A null `ts` or `zone` gives null.
- `AT TIME ZONE` binds more tightly than arithmetic: `ts AT TIME ZONE 'UTC' + INTERVAL '1 hour'` adds the hour to the result.

These forms are refused at plan time. A zone from a column that is an offset, an abbreviation or an unknown name is not refused: each row with such a zone fails instead.

| What | Message |
|---|---|
| A literal offset as the zone | `'+05:30' is an offset, not a time zone's name: write an IANA name, such as 'Asia/Kolkata'` |
| A literal abbreviation as the zone | `'CET' is an abbreviation, not a time zone's name: write an IANA name, such as 'Europe/Berlin', or 'UTC'` |
| A literal name that is not a time zone | `'Nowhere/Land' isn't a time zone transforms know: write an IANA name such as 'Europe/Berlin', or 'UTC'` |
| A date | `AT TIME ZONE takes a timestamp, not a date: CAST the date to TIMESTAMP for midnight in the zone, or to TIMESTAMPTZ for midnight UTC` |
| A string, or a text column | `AT TIME ZONE takes a timestamp, not text: write TIMESTAMP '…' for a wall-clock time or TIMESTAMPTZ '…' for an instant, or CAST the text to one` |
| An untyped `NULL` | `AT TIME ZONE takes a timestamp, and NULL has no type: write CAST(NULL AS TIMESTAMP) or CAST(NULL AS TIMESTAMPTZ)` |
| A time of day | `AT TIME ZONE takes a timestamp, and a time of day has no date to place it in a zone: add it to a date first` |
| A zone that is not text, such as an `INTERVAL` | `a time zone is text, such as 'Europe/Berlin', not Interval(MonthDayNano)` |

## Types

A statement reads each column with the logical type that the stream's schema gives it, such as `int64`, `utf8`, `decimal(12, 2)` or `timestamp[us, tz=UTC]`. Statements see every column as nullable.

An output's columns have the engine's logical types. A value of another type is converted to one that holds it:

| A statement gives | The output column has |
|---|---|
| `bool`, `int16`, `int32`, `int64`, `float32`, `float64`, `utf8`, `binary`, `date32`, `time64[us]`, `month_day_nano_interval`, `decimal(P, S)` with P from 1 to 38 and S from 0 to P | The same type |
| `int8`, `uint8` | `int16` |
| `uint16` | `int32` |
| `uint32`, `uint64` | `int64` |
| `float16` | `float32` |
| A large or view string or binary type | `utf8` or `binary` |
| A timestamp in seconds, milliseconds or microseconds | `timestamp[us]` |
| A timestamp in nanoseconds | `timestamp[ns]` |
| A timestamp with a time zone | The same instant, with time zone `UTC` |
| A list | A list of the converted element type |
| A JSON column, or a value from `->`, `#>` or `json_get` | JSON, written as the document's own text |
| A UUID column, passed through | A UUID |

Any other type is refused: a struct, a map, a duration, a time in a unit other than microseconds, and an untyped `NULL`. The message names the type and asks for a cast:

```text
column s is Struct("a": Int64), which an output can't hold: CAST it to a type it can
```

### Casts

`CAST` and `::` take SQL type names:

| SQL type | Type |
|---|---|
| `BOOLEAN` | `bool` |
| `TINYINT` | `int8`, written as `int16` |
| `SMALLINT` | `int16` |
| `INT`, `INTEGER` | `int32` |
| `BIGINT` | `int64` |
| `INT UNSIGNED` and the other unsigned types | Unsigned integers, written as wider signed ones |
| `REAL` | `float32` |
| `DOUBLE` | `float64` |
| `DECIMAL(P, S)` | `decimal(P, S)` |
| `VARCHAR`, `TEXT` | `utf8` |
| `BYTEA` | `binary` |
| `DATE` | `date32` |
| `TIMESTAMP`, `TIMESTAMPTZ` | `timestamp[ns]` |
| `INTERVAL` | `month_day_nano_interval` |

`JSON` and `UUID` are not SQL types that casts take, and a UUID cannot be cast to a string. A cast of a JSON value reads it as the type it names: see [JSON](./sql-functions.md#json).
