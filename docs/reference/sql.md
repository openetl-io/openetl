---
title: SQL transforms
description: The statements, names, types, operators and functions a pipeline's transform.sql can use, and what the engine refuses.
---

A SQL transform is the `transform.sql` field of a [Pipeline](./specs/pipeline.md#a-sql-transform). Its statements read the source's streams and write outputs, which the destination gets as streams. The engine runs them on every lane of a sync or a continuous pipeline's job, between typing a stream's rows and writing them. [Write a SQL transform](../pipelines/sql-transforms.md) shows how to write one and check it.

The engine runs row-wise statements: each output row comes from one input row, or from one element of a list with `UNNEST`. Everything else is refused, with the position in the SQL text where it is written. [What's refused](#whats-refused) lists the refusals and their messages.

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
| Compute a column | `SELECT amount * 100 AS cents` | Expressions, operators and [functions](#functions) |
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
- Passes each delete as a delete of its key.
- Turns an insert or update that fails its `WHERE` into a delete of its key.

The [Pipeline reference](./specs/pipeline.md#statements-that-run) says how the destination applies these deletes, and what happens to an update that does not send every column.

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
| `->`, `->>`, `#>`, `#>>`, `?` | JSON paths: see [JSON](#json) |

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

`JSON` and `UUID` are not SQL types that casts take, and a UUID cannot be cast to a string. A cast of a JSON value reads it as the type it names: see [JSON](#json).

## Functions

These are the functions a statement can call, with their other names. Functions that are not deterministic give a new value each time they run, so a row read again gets another value. A function not listed here is refused:

```text
my_udf isn't a function transforms can call
```

[`crates/sql/functions.txt`](../../crates/sql/functions.txt) is the list the engine has, and `go test ./test/docs` fails when this page differs from it.

### Conditionals

| Function | Other names | Meaning |
|---|---|---|
| `coalesce` | | Its first argument that is not null |
| `greatest` | | The largest of its arguments, ignoring nulls |
| `least` | | The smallest of its arguments, ignoring nulls |
| `nullif` | | `nullif(a, b)`: null if `a` equals `b`, else `a` |
| `nvl` | `ifnull` | `nvl(a, b)`: `b` if `a` is null, else `a` |
| `nvl2` | | `nvl2(a, b, c)`: `b` if `a` is not null, else `c` |

### Strings

| Function | Other names | Meaning |
|---|---|---|
| `ascii` | | The code of the string's first character |
| `bit_length` | | The string's length in bits |
| `btrim` | `trim` | The string without the given characters, spaces by default, at both ends |
| `character_length` | `char_length`, `length` | The string's length in characters |
| `chr` | | The character with the given code |
| `concat` | | Its arguments joined, ignoring nulls |
| `concat_ws` | | `concat_ws(separator, …)`: its other arguments joined by the separator, ignoring nulls |
| `contains` | | Whether the string contains another |
| `ends_with` | | Whether the string ends with another |
| `find_in_set` | | `find_in_set(s, list)`: the position of `s` in a comma-separated list, from 1, or 0 |
| `initcap` | | The string with the first letter of each word in upper case |
| `left` | | The first `n` characters |
| `levenshtein` | | The edit distance between two strings |
| `lower` | | The string in lower case |
| `lpad` | | The string padded on the left to `n` characters, with spaces or the given string |
| `ltrim` | | The string without the given characters, spaces by default, at its start |
| `octet_length` | | The string's length in bytes |
| `overlay` | | `overlay(s PLACING r FROM start FOR n)`: `s` with `n` characters from `start` replaced by `r` |
| `repeat` | | The string repeated `n` times |
| `replace` | | `replace(s, from, to)`: `s` with every `from` replaced by `to` |
| `reverse` | | The string's characters in reverse order |
| `right` | | The last `n` characters |
| `rpad` | | The string padded on the right to `n` characters, with spaces or the given string |
| `rtrim` | | The string without the given characters, spaces by default, at its end |
| `split_part` | | `split_part(s, delimiter, n)`: the `n`th part of `s`, from 1 |
| `starts_with` | | Whether the string starts with another |
| `strpos` | `instr`, `position` | `strpos(s, sub)`: the position of `sub` in `s`, from 1, or 0 |
| `substr` | `substring` | `substr(s, start, length)`: part of `s`, from `start`, counting from 1 |
| `substr_index` | `substring_index` | `substr_index(s, delimiter, n)`: `s` before the `n`th delimiter, or with a negative `n`, after it, counting from the end |
| `to_hex` | | An integer in hexadecimal |
| `translate` | | `translate(s, from, to)`: `s` with each character of `from` replaced by the one at the same place in `to` |
| `upper` | | The string in upper case |
| `uuid` | | Not deterministic. A new random UUID, as a string. |

### Regular expressions

| Function | Other names | Meaning |
|---|---|---|
| `regexp_count` | | The number of matches in the string |
| `regexp_instr` | | The position of a match in the string, from 1, or 0 |
| `regexp_like` | | Whether the string matches |
| `regexp_match` | | The capture groups of the first match, as a list |
| `regexp_replace` | | The string with matches replaced; the flag `g` replaces every match |

### Numbers

| Function | Other names | Meaning |
|---|---|---|
| `abs` | | The absolute value |
| `acos` | | The arc cosine |
| `acosh` | | The inverse hyperbolic cosine |
| `asin` | | The arc sine |
| `asinh` | | The inverse hyperbolic sine |
| `atan` | | The arc tangent |
| `atan2` | | `atan2(y, x)`: the arc tangent of `y / x` |
| `atanh` | | The inverse hyperbolic tangent |
| `cbrt` | | The cube root |
| `ceil` | | The smallest integer not less than the number |
| `cos` | | The cosine |
| `cosh` | | The hyperbolic cosine |
| `cot` | | The cotangent |
| `degrees` | | Radians as degrees |
| `exp` | | `e` to the power of the number |
| `factorial` | | The factorial |
| `floor` | | The largest integer not greater than the number |
| `gcd` | | The greatest common divisor of two integers |
| `isnan` | | Whether the number is NaN |
| `iszero` | | Whether the number is zero |
| `lcm` | | The least common multiple of two integers |
| `ln` | | The natural logarithm |
| `log` | | `log(base, x)`, or `log(x)` in base 10 |
| `log10` | | The base-10 logarithm |
| `log2` | | The base-2 logarithm |
| `nanvl` | | `nanvl(x, y)`: `y` if `x` is NaN, else `x` |
| `pi` | | π |
| `power` | `pow` | `power(x, y)`: `x` to the power of `y` |
| `radians` | | Degrees as radians |
| `random` | `rand` | Not deterministic. A random number from 0, inclusive, to 1. |
| `round` | | The number rounded to the given number of decimal places, 0 by default |
| `signum` | | -1, 0 or 1, by the number's sign |
| `sin` | | The sine |
| `sinh` | | The hyperbolic sine |
| `sqrt` | | The square root |
| `tan` | | The tangent |
| `tanh` | | The hyperbolic tangent |
| `trunc` | | The number truncated to the given number of decimal places, 0 by default |

### Dates and times

| Function | Other names | Meaning |
|---|---|---|
| `current_date` | | Not deterministic. The date in UTC when the engine evaluates the batch. |
| `date_bin` | | `date_bin(interval, ts)`: the start of the interval-long bin that holds `ts` |
| `date_part` | `datepart` | `date_part(part, ts)`: a part of the time, such as `'year'`, `'hour'` or `'dow'` |
| `date_trunc` | `datetrunc` | `date_trunc(part, ts)`: the time truncated to the part, such as `'day'` |
| `from_unixtime` | | Seconds since 1970-01-01 as a timestamp |
| `make_date` | | `make_date(year, month, day)`: a date |
| `now` | `current_timestamp` | Not deterministic. The time in UTC when the engine evaluates the batch. |
| `to_char` | `date_format` | `to_char(value, format)`: a date or time as a string, in a `strftime`-style format such as `'%Y-%m-%d'` |
| `to_date` | | A string or number of days as a date, in the given formats or ISO 8601 |
| `to_timestamp` | | A string or number of seconds as a timestamp in nanoseconds, in the given formats or RFC 3339 |
| `to_timestamp_micros` | | As `to_timestamp`, in microseconds, and a number counts microseconds |
| `to_timestamp_millis` | | As `to_timestamp`, in milliseconds, written as microseconds, and a number counts milliseconds |
| `to_timestamp_nanos` | | As `to_timestamp`, and a number counts nanoseconds |
| `to_timestamp_seconds` | | As `to_timestamp`, in seconds, written as microseconds |
| `to_unixtime` | | A timestamp, or a string in the given formats, as seconds since 1970-01-01 |

### Hashes and encodings

| Function | Other names | Meaning |
|---|---|---|
| `decode` | | `decode(s, format)`: `base64` or `hex` text as binary |
| `digest` | | `digest(value, algorithm)`: a hash as binary, by an algorithm such as `'sha256'` |
| `encode` | | `encode(value, format)`: binary as `base64` or `hex` text |
| `md5` | | The MD5 hash, in hexadecimal |
| `sha224` | | The SHA-224 hash, as binary |
| `sha256` | | The SHA-256 hash, as binary |
| `sha384` | | The SHA-384 hash, as binary |
| `sha512` | | The SHA-512 hash, as binary |

### Lists

| Function | Other names | Meaning |
|---|---|---|
| `array_append` | `array_push_back`, `list_append`, `list_push_back` | The list with a value added at its end |
| `array_concat` | `array_cat`, `list_cat`, `list_concat` | The lists joined end to end |
| `array_distinct` | `list_distinct` | The list without repeated values |
| `array_element` | `array_extract`, `list_element`, `list_extract` | `array_element(list, i)`: the `i`th element, from 1, as `list[i]` |
| `array_has` | `array_contains`, `list_contains`, `list_has` | Whether the list has the value |
| `array_has_all` | `list_has_all` | Whether the list has every value of another list |
| `array_has_any` | `arrays_overlap`, `list_has_any` | Whether the list has any value of another list |
| `array_length` | `list_length` | The list's length |
| `array_position` | `array_indexof`, `list_indexof`, `list_position` | The position of the value in the list, from 1, or null |
| `array_prepend` | `array_push_front`, `list_prepend`, `list_push_front` | `array_prepend(value, list)`: the list with the value added at its start |
| `array_remove` | `list_remove` | The list without the first element equal to the value |
| `array_reverse` | `list_reverse` | The list in reverse order |
| `array_slice` | `list_slice` | `array_slice(list, from, to)`: the elements from `from` to `to`, both included, from 1 |
| `array_sort` | `list_sort` | The list sorted, ascending by default |
| `array_to_string` | `array_join`, `list_join`, `list_to_string` | `array_to_string(list, separator)`: the elements as one string |
| `cardinality` | | The number of elements in the list |
| `flatten` | | A list of lists as one list |
| `make_array` | `make_list` | Its arguments as a list |
| `string_to_array` | `string_to_list` | `string_to_array(s, delimiter)`: the string's parts as a list |

### Structs

| Function | Other names | Meaning |
|---|---|---|
| `get_field` | | `get_field(struct, 'name')`: a field's value, as `s['name']` |
| `named_struct` | | `named_struct('a', x, 'b', y)`: a struct with those fields |
| `struct` | `row` | A struct of its arguments, with fields `c0`, `c1` and so on |

An output cannot hold a struct, so a struct is useful only inside an expression, such as `named_struct('a', id)['a']`.

### JSON

The JSON functions read a document from a JSON column or from any string.

| Function | Other names | Meaning |
|---|---|---|
| `json_as_text` | | The value as text: a string's own value, another value's JSON text, and null for a JSON null. As `->>` and `#>>`. |
| `json_contains` | | Whether the path reaches a value, a JSON null included. As `?`. Never null: false for a null document. |
| `json_from_scalar` | `scalar_to_json` | A boolean, number or string as a JSON value |
| `json_get` | | The value, of any JSON type. As `->` and `#>`. |
| `json_get_array` | | An array's elements as a list, each as its JSON text: a string element keeps its quotes |
| `json_get_bool` | | A boolean, or a string that spells one |
| `json_get_float` | | A number, or a string that spells one, as `float64` |
| `json_get_int` | | An integer, a number with no fraction that `int64` holds, or a string that spells an integer |
| `json_get_json` | | The value's JSON text, as the document writes it; a JSON null is the text `null` |
| `json_get_str` | | A string's value |
| `json_length` | `json_len` | The number of an array's elements or an object's keys |
| `json_object_keys` | `json_keys` | An object's keys, as a list |
| `json_union_to_text` | | A value of `json_get` as JSON text, and null for a JSON null |

Each function but `json_from_scalar` takes the document, then a path: keys and array indexes, as further arguments or as one list. `json_from_scalar` takes one boolean, number or string, and no path. A value of another type than the function gives is null: `json_get_str` of a number is null.

- A key is a string, and matches exactly. An index is an integer, from 0. A negative index counts from the end: `-1` is the last element.
- A key that reaches an array is an index if it spells one: `'1'` is the second element.
- A path can come from columns, so it can differ by row: `json_get_str(doc, field_name)`.
- A path that the document does not have gives null. So does a null step and a null document.
- Text that is not valid JSON throughout gives null, and `json_contains` gives false. A truncated document is not valid.
- If an object has a key twice, the last value counts.

The operators are the functions:

| Operator | Function | Example |
|---|---|---|
| `doc -> 'a'` | `json_get(doc, 'a')` | `doc->'user'->'emails'->0`, a chain, is one call with the whole path |
| `doc ->> 'a'` | `json_as_text(doc, 'a')` | `doc->'user'->>'email'` |
| `doc #> ARRAY['a', 'b']` | `json_get(doc, ARRAY['a', 'b'])` | The path as one list |
| `doc #>> ARRAY['a', 'b']` | `json_as_text(doc, ARRAY['a', 'b'])` | |
| `doc ? 'a'` | `json_contains(doc, 'a')` | Whether the path exists. It takes a path, not only a top-level key: `'[1, 2]' ? 1` is true. |

A value that `->`, `#>` or `json_get` gives can be of any JSON type:

- Selected into an output column, it is JSON, written as the document writes it: `{"a": 1}`, `"text"`, `3.50`.
- Compared with a value, it is read as that value's type: `doc->'status' = 'active'` compares the string, and `doc->'n' > 3` the number. A JSON null and a missing path are null.
- Cast, it is read with the function of that type: `CAST(doc->'n' AS BIGINT)` is `json_get_int(doc, 'n')`, and a cast to `VARCHAR` or `TEXT` is `->>`'s text.

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
| Group windows: `GROUP BY tumble(…)`, `hop(…)`, `session(…)` | `a group window: transforms don't run group windows yet (tier 2)` |
| `OVER`, window functions such as `row_number()`, `rank()` and `lag()`, `QUALIFY` | `OVER: transforms don't run window functions yet (tier 3)`, or with `a window function:` or `QUALIFY:` first |
| `JOIN` | `a join: transforms don't run joins yet (tier 4)` |
| Two relations in `FROM` | `reading two relations: transforms don't run joins yet (tier 4)` |
| A subquery in an expression, such as `IN (SELECT …)` or `EXISTS (…)` | `a subquery: transforms don't run joins yet (tier 4)` |
| `GROUP BY` without a window | `GROUP BY without a window: transforms don't run updating statements yet (tier 5)` |
| An aggregate, such as `count(*)` or `sum(x)`, without a window | `an aggregate without a window: transforms don't run updating statements yet (tier 5)` |
| `HAVING` | `HAVING without a window: transforms don't run updating statements yet (tier 5)` |
| `DISTINCT`, `DISTINCT ON` | `DISTINCT: transforms don't run updating statements yet (tier 5)` |
| A function that is not in [the list](#functions), such as a user-defined function | `my_udf isn't a function transforms can call` |

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

### Changelog streams

| What | Message |
|---|---|
| A statement that drops a key column | `cdc is a changelog, so what reads it keeps its key (id) unchanged, renamed or not; this drops it` |
| A statement that changes a key column, such as `id + 1` or a cast | `… keeps its key (id) unchanged, renamed or not; this changes it` |
| `unnest` | `… keeps its key (id) unchanged, renamed or not; this multiplies the rows of it` |

### When rows run

A statement that plans can still fail on a row, such as a cast of a value that does not convert, or an integer divided by zero. The engine quarantines such a row, and writes the rest of the batch. The [Pipeline reference](./specs/pipeline.md#statements-that-run) says where quarantined rows go, and what a change to a stream's schema does to the statements that read it.

The settings that streams can and cannot have beside `transform.sql` are in the [Pipeline reference](./specs/pipeline.md#transforms).
