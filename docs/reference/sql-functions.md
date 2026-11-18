---
title: SQL functions
description: Look up conditional, string, numeric, time, hash, list, struct and JSON functions.
---

Use these function tables with the [SQL expression and type reference](./sql-expressions.md).

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
| `timezone` | | `timezone(zone, ts)`: `ts AT TIME ZONE zone`. See [Time zones](./sql-expressions.md#time-zones). |
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
