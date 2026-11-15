---
title: Build a connector
description: What a connector is to OpenETL, the kinds you can write, and how to build, test, package and publish one.
---

A connector reads from or writes to a system. OpenETL starts the connector, supplies its configuration and moves records between it and the data engine. OpenETL decides when to commit; the destination connector implements the commit. Write a connector to support another system or behavior.

You can maintain it in your own repository, release it on your own schedule, and distribute it publicly or privately. The official connectors in [openetl-connectors](https://github.com/openetl-io/openetl-connectors) use the same protocol and tooling; your connector needs nothing from that repository.

This guide defines source and destination contracts, then builds an example source in Go. It also explains local testing, conformance checks, packaging and publication with the `openetl` CLI.

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

`Read` emits JSON objects or Arrow batches. It marks **resume points**, each with state that lets a later read resume at that position. A bounded stream ends with `Complete`; its state becomes the next sync's starting state. Commits normally align to resume points. A [forced barrier](../pipelines/delivery.md#reading-a-stream-delivered-at-least-once) is permitted only when replay preserves the required delivery guarantee.

Each read request lists the declared `columns`. If the source sends text values for OpenETL to parse, those columns need declared types. If a new column appears, fail the read with a transient error. The next attempt's discovery can declare its type before reading again.

Sources can also implement these operations:

| Operation | Contract |
|---|---|
| `Check` | Check the connection alone, without reading: for example, sign in. See [Checks and preflights](#checks-and-preflights). |
| `PreflightSource` | Check a pipeline's streams as a sync would read them, without reading records. See [Checks and preflights](#checks-and-preflights). |
| `Committed` | Acknowledge upstream offsets or replication positions. Notifications are one checkpoint behind. |
| `Splits`, `Resplit` | Divide streams into concurrent reads. |
| `Retention` | Report retention usage for saved positions while the pipeline is stopped. Set `Retention: true` in `sdk.Connector` to enable checks every 15 minutes. Reports can open a **Retention at risk** incident. |
| `Setup`, `Teardown` | Create or remove source resources for a pipeline, as described below. |

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

A reply to **PreCommit** or **Commit** names the epoch of the request it answers, and each request gets one reply. OpenETL fails the attempt on a reply for another epoch, or one it didn't ask for. `sdk.Serve` does this for you.

The commit is the destination's own: its staging, markers, fence and catalog live in its writer, not in OpenETL.

A destination can also implement `Check` and `PreflightDestination`, as [Checks and preflights](#checks-and-preflights) describes.

### Setup and teardown

Connectors that maintain pipeline-specific resources, such as replication slots or staging tables, declare `setup` in their spec. In Go, set `Setup: true` in `sdk.Connector`. Implement `Setup`, `Teardown` or both.

OpenETL calls `Setup` before each attempt and `Teardown` after pipeline deletion. Both receive the connection configuration and pipeline ID. Both must be idempotent: existing resources or already removed resources must not cause an error. Without `setup`, OpenETL calls neither operation.

Failed teardown operations retry for up to 7 days. After that, OpenETL records `pipeline.teardown_failed` and stops retrying. In Go, implement the applicable `source.Setupper`, `source.Teardowner`, `destination.Setupper` or `destination.Teardowner` interfaces.

A destination must not change an existing column's type unless the pipeline permits it. This applies to target columns, staging columns and other columns that become user data. Refuse an unpermitted type change with `RefusedColumn`. Include the column, reason, types and a `Fix` statement or repair step, such as `ALTER TABLE`.

OpenETL holds the affected column where supported, or stops the stream. It shows the repair to the user. New columns can still be added.

### Checks and preflights

A connector answers two questions before any data moves:

- **Check:** does this configuration reach the system? A check is about one source or destination, with no pipeline. Keep it light: connect, sign in, reach a schema registry. Users run it from a source's or destination's page and with `openetl check`.
- **Preflight:** would this pipeline work? A preflight gets what a sync of the pipeline would read or write, and examines each stream. Users run it from a pipeline's page, with `openetl preflight`, and after each `apply` that changes a pipeline.

OpenETL runs a pipeline's preflight in parts: the source's check, then the destination's check. Then it calls `PreflightSource` if the source's check passed, and `PreflightDestination` if the destination's check passed. Last, it plans the sync.

| | Check | Preflight |
|---|---|---|
| RPC | `ConnectorService.Check` | `SourceService.PreflightSource`, `DestinationService.PreflightDestination` |
| Gets | The config | The config, and the pipeline's ID |
| A source also gets | Nothing | The streams, with their sync modes and cursors; whether the pipeline is continuous; the longest interval between its scheduled syncs |
| A destination also gets | Nothing | The namespace; the streams, with their write modes, keys and delete modes; each stream's declared columns and its table, in the `openetl.table` schema metadata |
| Belongs here | Reach the service, credentials, the account's own permissions | Each stream: exists, readable or writable, a mode the connector has; retention against the schedule; resources the pipeline will create |

Which items a connector reports depends on its configuration. For example, a source that reads through a consumer checks that consumer only in the mode that uses one.

Both answer with items, in the order they ran. Each item has these fields:

- `name`: unique and the same from one run to the next, such as `stream.clicks.read`.
- `title`: for people, such as "Read clicks".
- A status: `passed`, `warning`, `failed` or `skipped`.
- `message`: what it found.
- `error`: the classified error of a failed item.
- `fix`: what to do about a failure or a warning.
- `field`: the config field it concerns, as a dotted path.

The response's `error` is the first failed item's error. A warning does not fail it: the pipeline can still run, but not as configured or not as well as it could. Skip an item that an earlier failure makes pointless. Also skip an item that cannot be checked without a change, such as writing a record, and name the permission it needs in its `fix`. Item text is shown and kept, so it never holds secrets or records. A connector without a preflight answers with no items, and `sdk.Serve` does that for you.

In Go, implement `source.CheckReporter` or `destination.CheckReporter` in place of `Checker` for an itemised check. Implement `source.Preflighter` or `destination.Preflighter` for a preflight. Record failures in the report rather than stopping at the first, so one run shows everything that is wrong:

```go
func (s *Source) CheckReport(ctx context.Context, r *check.Report) {
	r.Run("connect", "Reach the API", func() error { return s.ping(ctx) }, check.Field("url"))
}

func (s *Source) Preflight(ctx context.Context, req source.PreflightRequest, r *check.Report) {
	for _, st := range req.Streams {
		r.Run("stream."+st.Name+".read", "Read "+st.Name, func() error {
			return check.Fix(s.peek(ctx, st.Name), "Grant the API key the "+st.Name+":read scope.")
		}, check.Field("streams"))
	}
}
```

`check.Fix` attaches a fix to an error, so the code that classifies errors can say what to do about each. `Warn`, `Pass` and `Skip` record the other statuses.

### The config schema

The config schema is a JSON Schema, draft 2020-12. OpenETL validates configs against it, and builds the console's view of a role's config from it: fields in the order the schema declares them, under groups, with their values, defaults and conditions. Give every field a `description`, and a `title` where its name doesn't read well. These keywords shape the form:

| Keyword | Where | What it does |
|---|---|---|
| `x-groups` | The root | The form's sections, in order: a list of `{"id", "title", "description"}`. Fields without a group go before every section. |
| `x-group` | A field | The `id` of the section it's in. A nested field is in its parent's unless it sets one. |
| `x-secret` | A field | Its value is a secret's name, as `{ secret: NAME }`, never the secret. |
| `x-advanced` | A field | Shown after the others in its section, and marked advanced. |
| `x-when` | A field | It applies only when other fields have these values: `{"consumer.type": "efo"}`, or a list of values, `{"mode": ["a", "b"]}`. Paths are dotted from the config's root, and a field that isn't set has its default. It hides the field in a form; validate the config in the connector too. |
| `x-format` | A field | What a text field holds, beyond JSON Schema's `format`: `duration` (Go's syntax, `500ms`, `1m30s`), `date-time` (RFC 3339), `uri`, `email`, `hostname`, `regex` or `multiline`. |
| `oneOf` of `const` | A field | Values with labels: `"oneOf": [{"const": "efo", "title": "Enhanced fan-out", "description": "…"}]`. A plain `enum` works too, shown as its values. |
| `default`, `examples`, `minimum`, `maximum`, `pattern`, `deprecated` | A field | As in JSON Schema; the form shows each. |

`openetl connector package` refuses a schema that would break a form: an `x-group` the root doesn't declare, an `x-when` on a field that doesn't exist or on a value it can't take, a `default` that isn't one of a field's values, or an `x-format` it doesn't know. The conformance kit's `spec` check finds the same, and flags fields without a description.

### Jobs on several workers

A pipeline can run each job on several workers, each a runner in a pod of its own, as [Run a job on several pods](../pipelines/workers.md) explains. Each worker starts its own source and destination. What your connector declares decides how much of a job the workers share:

- **Setup.** A coordinator starts the connectors first, calls `Setup` on each one that declares `setup`, discovers the streams, then stops them. The workers call `Discover`, and never `Setup`.
- **Splits.** Workers share a stream only through its splits, so implement `Splits`. For a keyed stream, set `SplitsByKey` in its `source.CatalogStream` when every record of a key is in one split. Then each record goes from its split's lane to a writer lane of the same worker. Otherwise the workers' engines exchange the stream's records, so that one writer lane gets each key's records. One worker reads all of a stream written with `overwrite`, and one worker reads every stream in `cdc` mode, in one `Read` call, since a source reads their changes from one log, such as a replication slot.
- **Positions.** Each worker's source gets `Committed` only for the positions of its own splits.
- **Writer lanes.** Each worker opens at least one writer lane, so a job runs on workers only if `maxWriterLanes` is at least the number of workers. Writer lane 0 is on the first worker.
- **Commits.** The first worker calls `Recover`, and commits each checkpoint with every worker's committables. `Commit` must work from committables that writers in other processes returned from `PreCommit`. A destination with [lane commits](#lane-commits) is different: each worker commits its own writer lanes.
- **One pod.** A job runs on one pod when its destination is a `transactional_stream` without `laneCommits`, or has `delivery: staged`.
- **Schemas and holds.** Every worker's writers get the same schemas, and the first worker applies them. A destination that takes holds gets the same held columns from every worker, and patches only from the first worker.

#### Lane commits

A transactional destination can declare **lane commits**: `laneCommits` in its package, `LaneCommits` in Go. Each lane's part of an epoch is then a transaction that the lane's writer holds open, such as one in a producer with a transactional ID of the lane. With lane commits:

- **Commit** runs on each lane's writer, with that lane's committable alone. It writes a marker of the epoch and the lane.
- An attempt on one pod opens one writer lane. An attempt on several workers opens a writer lane or more on each worker.
- To settle a recorded epoch, OpenETL opens a writer for each committable, as the lane that staged it, before it opens the attempt's lanes. It calls **ApplySchema** there with the recorded epoch's schemas, as it does on lane 0's writer, then calls **Recover** with that committable alone. Recover fences what the lane holds, for example by initializing the lane's transactional IDs. It reports applied if it finds the lane's marker, redone if it committed a copy the lane kept, or lost. It must not drop what other lanes staged, since other writers settle them at the same time.
- After lost, OpenETL reads the lane's part of the epoch again from the source. It sends the rows to the same writer as the recorded epoch: **Write**, then **PreCommit**, then **Commit** with the new committable. The writer checks its fence by its own generation, not by the epoch's.

Keeping a copy of each lane's part, so that Recover can commit it, is the destination's choice. Without one, recovery reads the source again, which is exact only for a source that returns the same records from the same position.

That read gives each split, or the whole stream, the state it starts from. If the epoch's part ended at a resume point, the read also gives an `until`: the state of that resume point. A source can assume that `until` is the state of a resume point it sent before. It is never the state of a completion, which can be a cursor floor rather than where the read ended. A source that can stop at `until` sends no record past that position. It sends a resume point with the `until` state, and completes the split with it. A source that ignores `until` makes the read exact only if it sends the same resume points as the first read did. A source that batches records by what it fetched, such as one reading a log, can rarely do that. A part that ended at the split's completion has no `until`: OpenETL reads it to its completion, which must be at the state the first read completed at.

## Write a source in Go

The SDK's `sdk.Source` declares a source from streams and the functions that read them. Here is a source with one stream, `greetings`, that makes up as many rows as its config asks for.

Make a module, and add the SDK. The SDK has versions of its own, tagged `sdk/go/vVERSION`, which follow the connector protocol rather than OpenETL's releases. `@latest` resolves the newest:

```sh
go mod init example.com/greetings
go get github.com/openetl-io/openetl/sdk/go@latest
```

Write `cmd/source-greetings/main.go`:

```go
// Command source-greetings serves a source with one stream, greetings.
package main

import (
	"log/slog"
	"os"

	sdk "github.com/openetl-io/openetl/sdk/go"
)

type config struct {
	Count int `json:"count"`
}

var configSchema = []byte(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "additionalProperties": false,
  "required": ["count"],
  "properties": {
    "count": {"type": "integer", "minimum": 1, "description": "How many greetings to read."}
  }
}`)

var greetings = sdk.Source[config]{
	Name: "source-greetings",
	Streams: []sdk.Stream[config]{
		sdk.Full("greetings", func(c *sdk.Ctx[config], out *sdk.Out) error {
			for i := 1; i <= c.Config.Count; i++ {
				if err := out.Emit(map[string]any{"id": i, "text": "hello"}); err != nil {
					return err
				}
			}
			return nil
		}, sdk.PrimaryKey("id")),
	},
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	sdk.Serve(sdk.Connector{
		Name:         "source-greetings",
		ConfigSchema: configSchema,
		Egress:       sdk.Egress{Tier: sdk.EgressHost},
		Source:       greetings.Bind,
	})
}
```

Then build it:

```sh
go mod tidy
go build -o bin/ ./cmd/source-greetings
```

`sdk.Full` declares a full-refresh stream. `sdk.Incremental` declares a stream read from a cursor, and `sdk.Resumable` declares one with its own saved state. `sdk.Paginate` marks a resume point after each API page.

These stream helpers declare superset replay. Retries can repeat records. The destination determines whether repeated records produce duplicate writes, using `_openetl_id` where supported.

`sdk.Ordered` declares that the API returns records in ascending cursor order. It corresponds to `ordered: true` in a declarative source's `incremental` settings. With this declaration, the saved cursor floor advances after every page.

A cursor below a previous value fails the read with `CONTRACT_CHANGED`. The floor might already have advanced past unread records. Equal cursor values are permitted. If the API does not guarantee this ordering, remove the option. The floor then advances only when the stream completes.

For Arrow batches, exact replay, splits or CDC, implement `source.Source` and `destination.Destination` yourself. The official connectors in [openetl-connectors](https://github.com/openetl-io/openetl-connectors) do: each `cmd/<executable>/main.go` there hands one to `sdk.Serve`.

Emitter methods can return before transmission completes. A transmission failure can therefore appear in a later call or when `Read` ends. Emitters do not retain caller memory, except for batches passed to `Records`. After a successful `Records` call, the emitter owns and releases the batch. If `Records` returns an error, the caller must release it.

## Try it with the CLI

`openetl connector` runs a connector the way a runner does, with nothing else running: no running OpenETL installation. [Install the CLI](../get-started/install.md) if you do not have it.

Print its spec, as the runner sees it:

```sh
openetl connector spec ./bin/source-greetings
```

```text
role: source
requires:
    protocol: v1
egress:
    tier: host
config:
    $schema: https://json-schema.org/draft/2020-12/schema
    additionalProperties: false
    properties:
        count:
            description: How many greetings to read.
            minimum: 1
            type: integer
    required:
        - count
    type: object
```

With a config in `config.json`, such as `{"count": 3}`, list its streams and read them:

```sh
openetl connector discover --config config.json ./bin/source-greetings
openetl connector read --config config.json ./bin/source-greetings
```

```text
greetings (superset; full_refresh; key id)
  columns are inferred from the records
```

```text
greetings (3 records)
id  text
1   hello
2   hello
3   hello
greetings: 3 records, complete
```

Connectors run as trusted code: `egress` says what yours means to reach, and OpenETL doesn't enforce it yet. [What that means for the people who run them](../self-host/index.md#connectors-are-trusted-code).

`openetl connector check` checks a config reaches the upstream, and prints the check's items. Each command takes an executable, a descriptor, a declarative source's document, or a package version by reference, as `oci://REGISTRY/REPOSITORY:VERSION` or `github.com/OWNER/REPO[/PACKAGE]@VERSION`. A config names its secrets as `{"secret": NAME}`, whose values come from `--secret NAME=VALUE` or the variable `OPENETL_SECRET_NAME`. `--attach ADDR` waits for a connector you start yourself, under a debugger, say. `openetl connector read -h` has the rest.

## Test it with the conformance kit

The [conformance kit](../../sdk/go/conformance) tests connectors through the protocol, regardless of implementation language. Source checks cover specifications and their config schemas, connection checks and their items, a preflight of every discovered stream, discovery, reads, resumption, and reads that stop at a redo's `until`. Destination checks cover a preflight of the suite's streams, commits, recovery at each commit step, fencing, write modes, schema changes, types, type-change refusal, holds and writer lanes. For a destination with lane commits, the writer-lane checks commit each lane on its own lane, settle each committable on a writer of its lane, and write a lost lane's part again.

Run it from the CLI:

```sh
openetl connector test --config config.json ./bin/source-greetings
```

```text
PASS spec
PASS check
SKIP errors
    the suite has neither BadCredentials nor Unreachable
SKIP setup
    the connector has no setup
PASS discover
PASS read/full
SKIP read/resume
    no stream sent a resume point
SKIP read/until
    the suite doesn't say the source stops at until
SKIP read/incremental
    the suite reads no stream incrementally
SKIP read/committed
    the source doesn't acknowledge, so the platform never calls Committed
SKIP read/continuous
    the suite reads no stream with cdc or stream
source-greetings: 11 checks, 4 passed, 0 failed, 7 skipped
```

`--bad-credentials` and `--unreachable` add configs for the errors check, and `--add-records-command` lets the incremental check add records upstream. A destination's checks that write need `--read-command`, which prints what the destination made visible; they're skipped without it. `openetl connector test -h` has the details.

The until check runs only for a source that stops at `until`: pass `--stops-at-until`, or set `StopsAtUntil` in Go. The check reads from a resume point, with the next resume point's state as `until`. It expects the records between the two and nothing past them, then a resume point and a completion at `until`. It also reads from the last resume point with no `until`, and expects a completion at the state the first read completed at.

In Go, a connector's own tests run the same checks with `conformance.SourceSuite` or `conformance.DestinationSuite` and `Test(t)`, as every official connector does in its `conformance_test.go`.

## Use it in OpenETL

While you develop, put the executable in the runner's connectors directory (`openetld`'s `--connectors`; `bin/connectors` under `make dev`), named `ROLE-NAME`, such as `source-greetings`. A source then names it as `connector: greetings`. For a package with an owner, such as `acme/greetings`, the path is `acme/source-greetings`.

Everywhere else, publish it as a package, and apply a version of it.

## Package and publish

A package is a directory with an `openetl-connector.yaml`: its name, its metadata, and how to build each role.

```yaml
name: acme/greetings
description: Greetings, made up
license: Apache-2.0
repository: https://github.com/acme/openetl-greetings
docs: https://github.com/acme/openetl-greetings#readme
roles:
  source:
    build: { go: ./cmd/source-greetings }
```

`openetl connector schema file` prints the file's JSON Schema. Check the file, then build a version:

```sh
openetl connector validate
openetl connector package --version 0.1.0
```

```text
.: acme/greetings
acme/greetings 0.1.0: dist/greetings.connector.yaml
```

`package` builds each role into `dist/`, with names such as `greetings-source-linux-amd64`. The default targets are Linux and macOS on amd64 and arm64; the package file can override them. It runs the executable for the local platform to read its spec. It then writes the **descriptor**, `dist/greetings.connector.yaml`.

The descriptor lists each platform's executable URL and digest. By default, URLs point to GitHub release assets for the version's tag, here `v0.1.0`. `--tag` accepts a release tag instead of `--version`.

Upload the contents of `dist/` as GitHub release assets. Alternatively, publish them to an OCI registry with `openetl connector publish oci://REGISTRY/NAMESPACE`.

An org then applies a version, by reference or from the descriptor:

```sh
openetl connector apply github.com/acme/openetl-greetings@0.1.0
openetl connector apply oci://REGISTRY/NAMESPACE/greetings:0.1.0
openetl connector apply -f dist/greetings.connector.yaml
```

A `ConnectorSource` document connects a registry namespace or a GitHub repository to the org as a source of its catalog. `openetl connector catalog`, and **Connectors** in the console, list the packages its sources offer, with the command that applies each. Under a connected source, a reference without a version, such as `github.com/acme/openetl-greetings`, applies the newest version: the highest stable version, or the highest prerelease if there is no stable one. OpenETL connects the official connectors' source, `oci://ghcr.io/openetl-io/openetl-connectors`, when it creates the org.

## Release and compatibility

Choose a package name under your own owner, such as `acme/greetings`. A name without an owner belongs to `openetl`. Use your own name for a fork too, so an installation can distinguish it from the original package. Package names stay associated with their source repository, and applied versions are immutable: changed code or metadata needs a new version.

Your connector's version is independent of OpenETL's version and of other connectors' versions. For a repository with one package, tags can be `v0.1.0`; for several packages, `greetings/v0.1.0` lets each package release separately. `openetl connector package --tag TAG` selects the package and version from that tag.

Compatibility comes from the role's contract, not matching version numbers:

- The descriptor records the connector protocol and required features reported by the executable's spec. A runner must support them.
- `roles.ROLE.requires.platform` in `openetl-connector.yaml` declares the oldest OpenETL release the role runs on, when it needs a minimum release.
- `roles.ROLE.state.version` and `roles.ROLE.state.reads` describe the checkpoint state it writes and the oldest state it reads. Document state changes and whether an older connector can resume after an upgrade.
- Build executables for the operating systems and architectures your users' runners use. An image-only role cannot run on a runner without a container launcher.

Test the released artifact with the conformance kit against the OpenETL versions you support. Publish release notes for config, behavior and state changes, and keep setup instructions and limits with the connector version. The [package file](../reference/connector-file.md) and [descriptor](../reference/connector-descriptor.md) references define these fields.

### Private distribution

The same package commands work for a private connector. Publish to a registry namespace your organization controls, or upload the descriptor and binaries to a private GitHub release. Your organization supplies its own repository, docs URL and release process; no entry in the official connector repository is required.

The CLI reads Docker's registry credentials when publishing or running a package locally. An OpenETL installation uses the credentials in its own `ConnectorSource` instead. Give the people installing your package its reference, the required role and platform versions, and the [private connector setup](../pipelines/connectors.md#private-connectors).

## Document it

Keep connector documentation in the connector's repository. Set `docs` in `openetl-connector.yaml` to its URL. The descriptor includes this URL, and the console catalog shows it as **Docs**.

Document the supported systems and versions, account permissions, required settings, configuration fields, type mappings, delivery guarantees and limits. Include verification and recovery steps where the connector requires them.

The config's JSON Schema is the reference for its fields, so give every property a `description`. The official connectors' pages live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), next to their code.
