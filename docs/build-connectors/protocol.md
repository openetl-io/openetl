---
title: Connector contracts
description: Look up connector roles, lifecycle operations and source and destination contracts.
---

A source implements reads. A destination implements writes and commits. OpenETL decides when to commit.

## Three kinds of connector

| Kind | What it is | Who writes it |
|---|---|---|
| Built in | Compiled into `openetld`: the `faker` source, and the `stdout` and `parquet` destinations | OpenETL's own code, in [`internal/connectorhost/native.go`](../../internal/connectorhost/native.go) |
| Declarative source | A YAML document of `kind: DeclarativeSource` that describes an HTTP API as data, with CEL expressions. The runner runs it: every request goes through OpenETL's host services, and a resume point follows every page. Sources only. | Anyone, without compiling anything |
| External | An executable that serves the connector protocol, `openetl.connector.v1` over gRPC on a Unix socket. The runner starts it as a process of its own for each attempt, and for each check or discovery. | Anyone, in any language; Go has an SDK |

A source or destination selects its connector by name, such as `connector: acme/greetings`. OpenETL checks for a built-in connector first. It then checks for an applied declarative source or package version. Finally, it checks the runner's local connectors directory for an executable.

`openetl connector schema manifest` prints the JSON Schema of a declarative source's document, for your editor; the schema is [`internal/declarative/manifest.schema.json`](../../internal/declarative/manifest.schema.json).

The rest of this page is about external connectors.

## The contract

The protocol is defined in [`proto/openetl/connector/v1`](../../proto/openetl/connector/v1). Every connector serves `ConnectorService`, including `Spec` and `Check`. Connectors that need lifecycle operations also implement `Setup` or `Teardown`. Sources additionally serve `SourceService`; destinations serve `DestinationService`.

The runner provides `HostService` to each connector it starts. This service supplies HTTP requests with retries, rate limits and authentication, plus secrets, logs and a key-value store.

The runner sets these environment variables before starting the connector. It then waits up to 10 seconds for an exact `ready` line on standard output. Earlier output goes to the connector's log:

| Variable | What it holds |
|---|---|
| `OPENETL_CONNECTOR_ADDR` | Where to serve: a Unix socket, `unix:///PATH` |
| `OPENETL_HOST_ADDR` | Where host services are |
| `OPENETL_CONNECTOR_ROLE` | `source` or `destination`, for an executable that has both |
| `OPENETL_WATCH_STDIN` | `1`: exit when stdin closes |

The runner also sets `GOMAXPROCS` and `GOMEMLIMIT` from the job's size. From its own environment, the runner passes only `PATH`, `HOME`, `TMPDIR`, `TZ`, `LANG`, `SSL_CERT_FILE` and `SSL_CERT_DIR`. A connector does not get the runner's cloud credentials, such as AWS keys or the token of an EKS pod's IAM role. If your connector calls a cloud service, take its credentials in the connector's config. Mark them `"x-secret": true` in the config's JSON Schema.

If your connector calls AWS, also use the AWS SDK's default credential chain when its config has no keys. The runner sets the chain's variables:

- **By default**, `AWS_EC2_METADATA_DISABLED`, `AWS_CONFIG_FILE` and `AWS_SHARED_CREDENTIALS_FILE`, so that the chain finds nothing of the runner's.
- **If the source or destination names a role** (`aws.role`), also `AWS_CONTAINER_CREDENTIALS_FULL_URI` and `AWS_CONTAINER_AUTHORIZATION_TOKEN`. The chain then finds the role's credentials, which the runner serves on `127.0.0.1` and renews.
- **If the runner's `OPENETL_CONNECTOR_AWS_CREDENTIALS` is `inherit`**, the runner's own `AWS_` variables instead.

[Run on AWS](../self-host/aws.md#credentials-for-connectors) explains these from the operator's side.

The Go SDK, in [`sdk/go`](../../sdk/go), does all of that in `sdk.Serve`, and its packages `source` and `destination` describe the contract in Go.

### A source

A source discovers its streams and reads them.

`Discover` returns a catalog. For each stream, declare its name, primary key, cursor field, supported sync modes and replay behavior:

- **Exact** replay returns exactly the records after a resume point.
- **Superset** replay can repeat records, such as rows at an incremental cursor's previous value.

Replay behavior and destination capabilities determine the stream's [delivery guarantee](../get-started/concepts.md#delivery-guarantees).

For `stream` mode with a primary key, declare a cursor that distinguishes events for the same key, such as an offset. The pipeline can also select this cursor. Without it, those events would share an `_openetl_id`, so OpenETL rejects the plan.

`Read` emits JSON objects or Arrow batches. It marks **resume points**, each with state that lets a later read resume at that position. A bounded stream ends with `Complete`; its state becomes the next sync's starting state. A stream read with `full_refresh` or written with `overwrite` is the exception. Each new read of it starts with no state, and `Splits` gets no ended splits for it. A retry of a read that has not completed still resumes from its last committed resume point, unless the stream's read changed to or from `full_refresh`, or its write to or from `overwrite`, since the read started. A new read then starts, with no state. Commits normally align to resume points. A [forced barrier](../pipelines/delivery.md#reading-a-stream-delivered-at-least-once) is permitted only when replay preserves the required delivery guarantee.

Each read request lists the declared `columns`. If the source sends text values for OpenETL to parse, those columns need declared types. Declare timestamps in microseconds, inside lists too: OpenETL refuses a stream that declares another unit. If a new column appears, fail the read with a transient error. The next attempt's discovery can declare its type before reading again.

Each read request also gives the declared types of each stream's columns in `arrow_schema`. They are the types that the source's `Discover` returned for that run, without control columns. An empty `arrow_schema` means that the request does not give types. A column's type can change after that discovery and before the read starts. OpenETL then parses values with the old type: for example, it decodes text in a column declared as binary as base64.

If a column's current type differs from its declared type, fail the read with a transient error. The next attempt discovers the stream again. Compare each column by name, with its Arrow storage type and its extension's name and metadata, such as `arrow.uuid` or `arrow.json`. OpenETL can send a canonical extension's metadata in a different form that Arrow defines as equal: for example, `arrow.json` with empty metadata instead of `{}`. In Go, `source.ConfiguredStream` has the types in `Schema`, and `source.ChangedTypes` compares them.

Sources can also implement these operations:

| Operation | Contract |
|---|---|
| `Check` | Check the connection alone, without reading: for example, sign in. See [Checks and preflights](./checks.md). |
| `PreflightSource` | Check a pipeline's streams as a sync would read them, without reading records. See [Checks and preflights](./checks.md). |
| `Committed` | Acknowledge upstream offsets or replication positions. Notifications are one checkpoint behind. |
| `Splits`, `Resplit` | Divide streams into concurrent reads. |
| `Retention` | Report retention usage for saved positions while the pipeline is stopped. Set `Retention: true` in `sdk.Connector` to enable checks every 15 minutes. Reports can open a **Retention at risk** incident. |
| `Setup`, `Teardown` | Create or remove source resources for a pipeline, as described below. |

#### Splits and lanes

A read request's `lanes` is how many of its splits the source can read at a time. A stream read whole counts as one split. Splits that share a connection can count as fewer. When a split completes, its lane goes to a split that waits.

In a continuous read, the splits of streams read with `cdc` or `stream` do not complete. So they do not count against `lanes`: the source reads all of them at once, and also the splits that they add. Records of several splits can come in any order on the read, each marked with its split.

In Go, `source.ReadSplits` reads the splits for you, up to `Lanes` at a time. In a continuous read, it starts each split of a stream that follows its source at once, without a lane. A stream follows its source unless its `SyncMode` is `FullRefresh` or `Incremental`, because `ReadSplits` cannot tell how a source reads a stream without one. If such a stream also has splits that complete and must take turns, such as a snapshot's chunks, read them in a separate `ReadSplits` call without `Continuous`.

### A destination

A destination declares its **capabilities**, and opens a writer for each writer lane of an attempt. Its capabilities say how it makes an epoch, the data between two checkpoints, visible:

- **Stage and apply:** stage in `PreCommit`, apply in `Commit`, with a marker per stream behind a fence.
- **Transactional stream:** flush into a transaction in `PreCommit`, commit it in `Commit`.
- **Deliver and acknowledge:** deliver during the epoch, and return from `PreCommit` once everything is acknowledged.

A writer's calls are the commit protocol:

1. **Write** adds batches to an epoch.
2. **PreCommit** makes the lane's epoch data durable and returns a **committable**, a small description of the staged data. OpenETL stores each lane's committable in the checkpoint record. For stage-and-apply and transactional destinations, the data remains invisible until commit. Deliver-and-acknowledge destinations have already delivered their records.
3. **Commit** runs after the checkpoint record is written. For stage-and-apply destinations, it makes the epoch visible with stream markers protected by the destination's fence. It must be idempotent.
4. **Recover** runs when an attempt starts. It raises the fence to the attempt's generation, preventing older attempts from committing. It reports whether the recorded epoch was already applied, was applied during recovery, or is lost.

A stream written with `overwrite` gets **EndRead** in the epoch where its read ends, before that epoch's **PreCommit**. That epoch's **Commit** replaces the stream's table with what the read wrote. By then, lane 0's writer, which commits, has the stream's schema, from its batches or from **ApplySchema**, which comes before **Commit**. A read with no rows gets the schema from **ApplySchema** alone: replace the table with an empty one, or create it empty. Some streams get no schema from a read with no rows, and their tables stay as they are: a stream that has never had rows and whose source does not declare its column types, or its cursor's type, and a transform's output that has never had rows.

A reply to **PreCommit** or **Commit** names the epoch of the request it answers, and each request gets one reply. OpenETL fails the attempt on a reply for another epoch, or one it did not ask for. `sdk.Serve` does this for you.

The commit is the destination's own: its staging, markers, fence and catalog live in its writer, not in OpenETL.

A destination can also implement `Check` and `PreflightDestination`, as [Checks and preflights](./checks.md) describes.

### Setup and teardown

Connectors that maintain pipeline-specific resources, such as replication slots or staging tables, declare `setup` in their spec. In Go, set `Setup: true` in `sdk.Connector`. Implement `Setup`, `Teardown` or both.

OpenETL calls `Setup` before each attempt and `Teardown` after pipeline deletion. Both receive the connection configuration and pipeline ID. Both must be idempotent: existing resources or already removed resources must not cause an error. Without `setup`, OpenETL calls neither operation.

Failed teardown operations retry for up to 7 days. After that, OpenETL records `pipeline.teardown_failed` and stops retrying. In Go, implement the applicable `source.Setupper`, `source.Teardowner`, `destination.Setupper` or `destination.Teardowner` interfaces.

A destination must not change an existing column's type unless the pipeline permits it. This applies to target columns, staging columns and other columns that become user data. Refuse an unpermitted type change with `RefusedColumn`. Include the column, reason, types and a `Fix` statement or repair step, such as `ALTER TABLE`.

OpenETL holds the affected column where supported, or stops the stream. It shows the repair to the user. New columns can still be added.

A destination that takes holds keeps, for each held cell, the version that it holds the cell for: the `_openetl_id` of the row that listed the column in `_openetl_held`. A patch fills a cell only where the target still holds the cell for the patch's `_openetl_id`. A merge update that lists a column in `_openetl_unchanged` did not send that column. Keep the target's value of the column and the version that the target holds it for, as a soft delete does, so the held version's patch still fills it. The conformance kit's `holds/unchanged` check tests this.

A destination that takes holds also answers with each stream's `TableLimits`. The limits name columns by their destination names:

- `Unholdable`: columns that the destination cannot hold, such as a partition column or the key of its table.
- `NotNull`: columns that cannot be null.
- `Fixed`: columns whose type cannot change.

OpenETL cannot hold some refused columns:

- a column that the destination lists in `Unholdable`;
- a column of the stream's primary key, or its cursor;
- a column in `NotNull`, if the pipeline gives it no `heldValue`.

If the destination refuses one of these columns, the attempt fails with `SCHEMA_DRIFT`. The error includes the refusal's reason and its `Fix`.

### Checks and preflights

A connector can tell people whether its configuration works before any data moves, and whether a pipeline would run:

- **Check:** does this configuration reach the system? A check is about one source or destination, with no pipeline. Keep it light: connect, sign in, find what the config names.
- **Preflight:** would this pipeline work? A preflight gets what a sync of the pipeline would read or write, and examines each stream. OpenETL calls it only after the connector's check passed.

Both answer with items, one for each thing the connector looked at: passed, warning, failed or skipped, with a fix for what failed. A connector can have neither, either or both. In Go, a connector declares each as a list of named checks that the SDK runs. The SDK's toolkits check networks, HTTP APIs and databases, and a connector adds checks of its own. [Checks and preflights](./checks.md) explains what belongs in each, how to declare and test them, and the protocol for other languages.

### The config schema

The config schema is a JSON Schema, draft 2020-12. OpenETL validates configs against it, and builds the console's view of a role's config from it: fields in the order the schema declares them, under groups, with their values, defaults and conditions. Give every field a `description`, and a `title` where its name does not read well. These keywords shape the form:

| Keyword | Where | What it does |
|---|---|---|
| `x-groups` | The root | The form's sections, in order: a list of `{"id", "title", "description"}`. Fields without a group go before every section. |
| `x-group` | A field | The `id` of the section it is in. A nested field is in its parent's unless it sets one. |
| `x-secret` | A field | Its value is a secret's name, as `{ secret: NAME }`, never the secret. |
| `x-advanced` | A field | Shown after the others in its section, and marked advanced. |
| `x-when` | A field | It applies only when other fields have these values: `{"consumer.type": "efo"}`, or a list of values, `{"mode": ["a", "b"]}`. Paths are dotted from the config's root, and a field that is not set has its default. It hides the field in a form; validate the config in the connector too. |
| `x-format` | A field | What a text field holds, beyond JSON Schema's `format`: `duration` (Go's syntax, `500ms`, `1m30s`), `date-time` (RFC 3339), `uri`, `email`, `hostname`, `regex` or `multiline`. |
| `oneOf` of `const` | A field | Values with labels: `"oneOf": [{"const": "efo", "title": "Enhanced fan-out", "description": "…"}]`. A plain `enum` works too, shown as its values. A `oneOf` or `anyOf` gives values only when each of its branches has a `const`. |
| `default`, `examples`, `minimum`, `maximum`, `pattern`, `deprecated` | A field | As in JSON Schema; the form shows each. |

`openetl connector package` refuses a schema that would break a form: an `x-group` the root does not declare, an `x-when` on a field that does not exist or on a value it cannot take, a `default` that is not one of a field's values, or an `x-format` it does not know. The conformance kit's `spec` check finds the same, and flags fields without a description.

### Jobs on several workers

A pipeline can run each job on several workers, each a runner in a pod of its own, as [Run a job on several pods](../pipelines/workers.md) explains. Each worker starts its own source and destination. What your connector declares decides how much of a job the workers share:

- **Setup.** A coordinator starts the connectors first, calls `Setup` on each one that declares `setup`, discovers the streams, then stops them. The workers call `Discover`, and never `Setup`.
- **Splits.** Workers share a stream only through its splits, so implement `Splits`. For a keyed stream, set `SplitsByKey` in its `source.CatalogStream` when every record of a key is in one split. Then each record goes from its split's lane to a writer lane of the same worker. Otherwise the workers' engines exchange the stream's records, so that one writer lane gets each key's records. One worker reads all of a stream written with `overwrite`, and one worker reads every stream in `cdc` mode, in one `Read` call, since a source reads their changes from one log, such as a replication slot.
- **Positions.** Each worker's source gets `Committed` only for the positions of its own splits.
- **Writer lanes.** Each worker opens at least one writer lane, so a job runs on workers only if `maxWriterLanes` is at least the number of workers. Writer lane 0 is on the first worker.
- **Commits.** The first worker calls `Recover`, and commits each checkpoint with every worker's committables. `Commit` must work from committables that writers in other processes returned from `PreCommit`. A destination with [lane commits](#lane-commits) is different: each worker commits its own writer lanes.
- **One pod.** A job runs on one pod when its destination is a `transactional_stream` without `laneCommits`, or has `delivery: staged`. It also runs on one pod when its destination declares `laneCommits` and takes holds for a stream's write mode, so that the stream can hold columns.
- **Schemas and holds.** Every worker's writers get the same schemas, and the first worker applies them. A destination that takes holds gets the same held columns from every worker, and patches only from the first worker.

#### Lane commits

A transactional destination can declare **lane commits**: `laneCommits` in its package, `LaneCommits` in Go. Each lane's part of an epoch is then a transaction that the lane's writer holds open, such as one in a producer with a transactional ID of the lane. With lane commits:

- **Commit** runs on each lane's writer, with that lane's committable alone. It writes a marker of the epoch and the lane.
- An attempt on one pod opens one writer lane. An attempt on several workers opens a writer lane or more on each worker.
- To settle a recorded epoch, OpenETL opens a writer for each committable, as the lane that staged it, before it opens the attempt's lanes. It calls **ApplySchema** there with the recorded epoch's schemas, as it does on lane 0's writer, then calls **Recover** with that committable alone. Recover fences what the lane holds, for example by initializing the lane's transactional IDs. It reports applied if it finds the lane's marker, redone if it committed a copy the lane kept, or lost. It must not drop what other lanes staged, since other writers settle them at the same time.
- After lost, OpenETL reads the lane's part of the epoch again from the source. It sends the rows to the same writer as the recorded epoch: **Write**, then **PreCommit**, then **Commit** with the new committable. The writer checks its fence by its own generation, not by the epoch's.

Keeping a copy of each lane's part, so that Recover can commit it, is the destination's choice. Without one, recovery reads the source again, which is exact only for a source that returns the same records from the same position.

That read gives each split, or the whole stream, the state it starts from. If the epoch's part ended at a resume point, the read also gives an `until`: the state of that resume point. A source can assume that `until` is the state of a resume point it sent before. It is never the state of a completion. A source that can stop at `until` sends no record past that position. It sends a resume point with the `until` state, and completes the split with it. A source that ignores `until` makes the read exact only if it sends the same resume points as the first read did. A source that batches records by what it fetched, such as one reading a log, can rarely do that.

If the epoch's part ended at the split's completion, the read gives a `complete_at` instead: the state that the first read completed the split at, unless that state was empty. A completion's state is where the next sync starts. For a source that reads a log, such as offsets or a WAL position, it is also where the read ended. A source that can tell from the state where the first read ended sends no record past that position. It completes the split with the `complete_at` state, and can send a resume point with that state first. A source whose completion state is a cursor floor, rather than where the read ended, ignores `complete_at`. A source that can stop there for some reads only ignores it for the others, and its documentation says which. A source that ignores it reads to where it catches up. The read is then exact only if the source has not moved past where the first read completed. If the read completes at another state, the attempt fails with `POSITION_LOST`. If it completes at the same state with more records, OpenETL commits them, as it does with more records before an `until`. A source whose completion state does not move with what it reads can do that.

In Go, the SDK gives both as `Until` and `CompleteAt` on `source.Split`, and on `source.ConfiguredStream` for a stream read whole. A read never has both.

OpenETL reads the lane's part again on one read for each split, or for each stream read whole. It runs several of these reads at once. It reads the streams read with `cdc` one after another instead, on one worker, because only one reader can hold their log. OpenETL ends each read when it reaches its position, and does not wait for the source to close it. So the next read can start while the source still closes the read before it. A source that holds its log in a session, such as a replication slot's connection, waits a bounded time for the log to be free.
