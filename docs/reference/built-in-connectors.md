---
title: Built-in connectors
description: The connectors built into OpenETL, the faker source and the stdout and parquet destinations, with every config field and what each one supports.
---

OpenETL has three connectors built in, for trying pipelines out and for tests. They need no executable or package of their own.

| Connector | Role | What it does |
|---|---|---|
| [`faker`](#faker) | Source | Generates the same users and purchases every time |
| [`stdout`](#stdout) | Destination | Prints each row as a line of JSON |
| [`parquet`](#parquet) | Destination | Writes Parquet files into a directory |

A source or destination names one in `spec.connector`, as `faker` or as `openetl/faker`. Either name always means the built-in. Each one refuses config fields it doesn't know.

The official connectors, such as Postgres, Kafka and Iceberg, aren't built in: they're in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), with their own pages.

## faker

A source of generated rows. It offers two streams, `users` and `purchases`, of `count` rows each. Row `i` of a stream depends only on `seed` and `i`, so every read with the same config yields the same rows, and a resumed read yields exactly the rows after its resume point.

### Config

| Field | Type | Default | Meaning |
|---|---|---|---|
| `seed` | integer, 0 or more | `0` | Seeds the generator. The same seed gives the same rows. |
| `count` | integer, 0 or more | `1000` | Rows per stream. With `endless`, it only sets the range of `purchases.user_id`. |
| `batchSize` | integer, 1 or more | `100` | Rows per batch. Each batch ends at a resume point. |
| `delay` | duration, such as `50ms` | none | A pause after each batch. With `endless`, after each round. |
| `overlap` | integer, 0 or more | `0` | Rows before its resume point that a resumed read emits again, like a cursor's lookback window. Above 0, the streams replay a superset of what was read rather than exactly the same rows. |
| `keyless` | boolean | `false` | The streams declare no primary key, and `id` as their cursor, like tables without a primary key. |
| `twice` | integer, 0 or more | `0` | Makes every `twice`-th row a copy of the row before it, as a table without a primary key can hold a row twice. Needs `keyless`. |
| `endless` | boolean | `false` | The streams never end, for a continuous pipeline. Each round emits a batch of every stream, then waits `delay`; with no `delay`, the next round starts at once. |

### Streams

`users`:

| Column | Value |
|---|---|
| `id` | The row's number, from 1 |
| `name` | A first and a last name |
| `email` | `First.Last.ID@example.com` |
| `age` | 18 to 87 |
| `score` | 0 to 100, with three decimal places |
| `active` | `true` or `false` |
| `address` | An object with `city` and `zip` |
| `created_at` | An RFC 3339 time: `2026-01-01T00:00:00Z`, plus 37 seconds for each row before it |

`purchases`:

| Column | Value |
|---|---|
| `id` | The row's number, from 1 |
| `user_id` | 1 to `count` |
| `amount` | 0.00 to 999.99 |
| `currency` | `EUR`, `GBP`, `INR`, `JPY` or `USD` |
| `created_at` | An RFC 3339 time: `2026-01-01T00:00:00Z`, plus 53 seconds for each row before it |

Each column's type is inferred from its values.

### Reads

| | Without `endless` | With `endless` |
|---|---|---|
| Sync mode | `full_refresh` | `stream` |
| Primary key | `id`, or none with `keyless` | `id`, or none with `keyless` |
| Replay | Exact, or a superset when `overlap` is above 0 | Exact, or a superset when `overlap` is above 0 |

### Example

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: faker
spec:
  connector: faker
  config:
    seed: 42
    count: 1000
    batchSize: 100
```

[`examples/faker-to-postgres.yaml`](../../examples/faker-to-postgres.yaml) uses it in a pipeline.

## stdout

A destination that prints each row to the standard output of the process that runs the sync, as a line of JSON holding the stream's name and the row. Trimmed to two of the row's columns, a line looks like this:

```json
{"stream":"users","record":{"id":1,"name":"Margaret Turing"}}
```

`openetld` logs to standard error unless `--log-output` says otherwise, so standard output carries only the rows. Rows are flushed at each commit.

### Config

`stdout` takes no config: leave out `config`, or give `{}`.

### Writes

| | |
|---|---|
| Write modes | `append` only |
| Delivery | At least once: the destination delivers and acknowledges, so it can't keep a row from being printed again after a retry |
| Timestamps | Keep nanoseconds |

A pipeline asks for exactly once unless it says otherwise, so a pipeline into `stdout` sets `delivery.require` to `at_least_once`, or its plan fails.

### Example

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: print
spec:
  connector: stdout
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-stdout
spec:
  schedule: { every: 1h }
  source: { ref: faker }
  destination: { ref: print }
  delivery: { require: at_least_once }
```

## parquet

A destination that writes each stream as Parquet files into a directory on the machine that runs the sync's job. The data engine writes the files, compressed with Snappy, in row groups of up to 128 MiB, and starts a new file when a stream's schema changes.

### Config

| Field | Type | Default | Meaning |
|---|---|---|---|
| `url` | string | required | `file://` followed by an absolute path, such as `file:///tmp/openetl-parquet`. The host is empty or `localhost`, with no query or fragment. |

`openetl check destination NAME` creates the directory if it's missing, and checks that new files can be made in it.

### Files

| Path under the directory | What it holds |
|---|---|
| `TABLE/EPOCH-gGENERATION-PART-PIPELINE.parquet` | A committed file. `TABLE` is the stream's name as the pipeline's `destination.naming` makes it. `EPOCH` and `GENERATION` are the commit's, in 20 digits each, `PART` is the file's number in it, in 5, and `PIPELINE` is the pipeline's ID. |
| `_openetl/staging/PIPELINE/` | Files of the pipeline's commits that haven't finished |
| `_openetl/loads/PIPELINE/TABLE/EPOCH-gGENERATION` | An empty file marking each stream's committed epochs |

Read only the `TABLE` directories, and leave `_openetl` alone. No stream may be named `_openetl`. The pipeline's `destination.namespace` doesn't change where files go. Pipelines can share a destination, and a table: each writes files of its own into `TABLE`. Files that OpenETL wrote before it put the pipeline's ID in their names keep their names.

A file gets its name under `TABLE` only when its commit applies, and applying a commit a second time changes nothing.

### Writes

| | |
|---|---|
| Write modes | `append` only |
| Delivery | Exactly once for streams the source replays exactly; at least once for the rest, since it can't drop repeated rows |
| Commit interval | In a continuous pipeline, 1 minute unless `delivery.commit.interval` sets another; never less than 1 second |
| Timestamps | Keep nanoseconds |
| Holds | Takes none |

### Example

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-parquet
spec:
  connector: parquet
  config:
    url: file:///tmp/openetl-parquet
```

[`examples/postgres-to-parquet.yaml`](../../examples/postgres-to-parquet.yaml) uses it in a pipeline.
