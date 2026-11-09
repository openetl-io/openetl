---
title: Build a connector
description: What a connector is to OpenETL, the kinds you can write, and how to build, test, package and publish one.
---

A connector is the program that reads from or writes to one kind of system. OpenETL starts it, hands it its config, moves records between it and the data engine, and decides when to commit. Write one for a system you need to connect, such as an internal API, or for behavior an existing connector doesn't provide.

You can maintain it in your own repository, release it on your own schedule, and distribute it publicly or privately. The official connectors in [openetl-connectors](https://github.com/openetl-io/openetl-connectors) use the same protocol and tooling; your connector needs nothing from that repository.

This page covers the kinds of connector, the contract a source and a destination each keep, and the loop of building, testing, packaging and publishing one with the `openetl` CLI.

## Three kinds of connector

| Kind | What it is | Who writes it |
|---|---|---|
| Built in | Compiled into `openetld`: the `faker` source, and the `stdout` and `parquet` destinations | OpenETL's own code, in [`internal/connectorhost/native.go`](../../internal/connectorhost/native.go) |
| Declarative source | A YAML document of `kind: DeclarativeSource` that describes an HTTP API as data, with CEL expressions. The runner runs it: every request goes through OpenETL's host services, and a resume point follows every page. Sources only. | Anyone, without compiling anything |
| External | An executable that serves the connector protocol, `openetl.connector.v1` over gRPC on a Unix socket. The runner starts it as a process of its own for each attempt, and for each check or discovery. | Anyone, in any language; Go has an SDK |

A source or destination names its connector, such as `connector: acme/greetings`. OpenETL looks for a built-in connector of that name first, then for a declarative source or a package version applied to the org, then for an executable in the runner's connectors directory.

`openetl connector schema manifest` prints the JSON Schema of a declarative source's document, for your editor; the schema is [`internal/declarative/manifest.schema.json`](../../internal/declarative/manifest.schema.json).

The rest of this page is about external connectors.

## The contract

The protocol is in [`proto/openetl/connector/v1`](../../proto/openetl/connector/v1). Every connector serves `ConnectorService` (`Spec`, `Check`, and `Setup` and `Teardown` for those that need them), and a source serves `SourceService` or a destination `DestinationService` beside it. The runner serves `HostService` to every connector it starts: HTTP requests with retries, rate limits and auth, secrets, logs and a key-value store.

The runner starts a connector with these variables set, and waits up to 10 seconds for a line on stdout that is exactly `ready`. Anything printed before it, as a library might at startup, goes to the connector's log:

| Variable | What it holds |
|---|---|
| `OPENETL_CONNECTOR_ADDR` | Where to serve: a Unix socket, `unix:///PATH` |
| `OPENETL_HOST_ADDR` | Where host services are |
| `OPENETL_CONNECTOR_ROLE` | `source` or `destination`, for an executable that has both |
| `OPENETL_WATCH_STDIN` | `1`: exit when stdin closes |

The Go SDK, in [`sdk/go`](../../sdk/go), does all of that in `sdk.Serve`, and its packages `source` and `destination` describe the contract in Go.

### A source

A source discovers its streams and reads them.

- **Discover** returns a catalog: each stream's name, primary key, cursor field, how it can be read (full refresh, incremental, CDC or stream), and its **replay**. A stream is exact when reading again from a resume point yields exactly the records after it, as a change log or an ordered event partition can, and superset when records can repeat, as they do when reading from a cursor's last value. Replay is half of what sets a stream's [delivery guarantee](../get-started/concepts.md#delivery-guarantees); the destination is the other. A stream read with `stream` that has a primary key needs a cursor field that differs between a key's events, such as an offset, or the pipeline to name one: without one each event of a key would get the same `_openetl_id` and be dropped as a repeat, so OpenETL refuses the plan.
- **Read** emits records, as JSON objects or Arrow batches, and marks **resume points**: places in a stream it can restart from, each with a state it gets back when it does. OpenETL commits only at the resume points a source emits. A bounded stream ends with **Complete**, whose state is where the next sync starts.
- A source can also check its config (`Check`), acknowledge upstream what's committed (`Committed`, for event offsets or a replication position, a checkpoint behind), divide streams into splits that are read concurrently (`Splits`, `Resplit`), say how much of what it keeps for a pipeline a checkpoint's positions have used while the pipeline isn't running (`Retention`, set with `Retention: true` in `sdk.Connector`; OpenETL asks every 15 minutes and opens a **Retention at risk** incident from the answer), and prepare and remove what it keeps upstream for a pipeline (`Setup` and `Teardown`, below).

### A destination

A destination declares its **capabilities**, and opens a writer for each writer lane of an attempt. Its capabilities say how it makes an epoch, the data between two checkpoints, visible:

- **Stage and apply:** stage in `PreCommit`, apply in `Commit`, with a marker per stream behind a fence.
- **Transactional stream:** flush into a transaction in `PreCommit`, commit it in `Commit`.
- **Deliver and acknowledge:** deliver during the epoch, and return from `PreCommit` once everything is acknowledged.

A writer's calls are the commit protocol:

1. **Write** adds batches to an epoch.
2. **PreCommit** makes the lane's part of the epoch durable but not visible, and returns a committable: a few kilobytes that say what it staged. OpenETL stores every lane's committable in the epoch's checkpoint record.
3. **Commit** runs once that record is written. It makes the epoch visible with a marker per stream, behind the destination's fence, and must be idempotent.
4. **Recover** runs when an attempt starts. It raises the fence to the attempt's generation, so older attempts can no longer commit, and says whether the epoch the checkpoint log points to was applied, applied it now, or is lost.

A reply to **PreCommit** or **Commit** names the epoch of the request it answers, and each request gets one reply. OpenETL fails the attempt on a reply for another epoch, or one it didn't ask for. `sdk.Serve` does this for you.

The commit is the destination's own: its staging, markers, fence and catalog live in its writer, not in OpenETL.

### Setup and teardown

A source or destination that keeps something for each pipeline, such as a replication slot or staging tables, sets `setup` in its spec (`Setup: true` in `sdk.Connector`) and serves `Setup`, `Teardown` or both. OpenETL calls `Setup` before each attempt that reads or writes for a pipeline, and `Teardown` once the pipeline is deleted, each with the config and the pipeline's ID. A `Teardown` that fails is tried again for up to 7 days, and after that OpenETL records a `pipeline.teardown_failed` event and stops. Both must be idempotent: preparing what's there, or removing what's gone, isn't an error. Without `setup`, OpenETL calls neither. In Go, implement `source.Setupper` or `source.Teardowner`, and `destination.Setupper` or `destination.Teardowner`.

A destination never changes the type of a column it already has unless the pipeline allows it. When a stream's column widens and the pipeline doesn't allow the change, the destination refuses it with a `RefusedColumn`: the column, the reason, the types, and a `Fix`, a statement a person can run to make the change, such as an `ALTER TABLE`. OpenETL holds the column, or stops the stream, and shows the fix. New columns still land.

## Write a source in Go

The SDK's `sdk.Source` declares a source from streams and the functions that read them. Here is a source with one stream, `greetings`, that makes up as many rows as its config asks for.

Make a module, and take the SDK from `main`, since it has no tagged version:

```sh
go mod init example.com/greetings
go get github.com/openetl-io/openetl/sdk/go@main
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

`sdk.Full` declares a stream read whole on every sync. `sdk.Incremental` declares one read from a cursor onwards, and `sdk.Resumable` one that keeps its own state; `sdk.Paginate` marks a resume point after each page of an API. Streams declared this way are superset: a retry can repeat records, and whether they land twice depends on the destination, which can drop repeats by their `_openetl_id`.

`sdk.Ordered`, like `ordered: true` in a declarative source's `incremental`, says the API returns a stream's records sorted by its cursor, oldest first, so the cursor's floor moves after every page. A cursor lower than one before it fails the read with `CONTRACT_CHANGED`, because the floor may already be past records the read hasn't reached. Equal cursors are fine. If you see that error, the API doesn't sort by that field: remove the option, and the floor moves only when the stream completes.

For Arrow batches, exact replay, splits or CDC, implement `source.Source` and `destination.Destination` yourself. The official connectors in [openetl-connectors](https://github.com/openetl-io/openetl-connectors) do: each `cmd/<executable>/main.go` there hands one to `sdk.Serve`.

An emitter's methods can return before what they accepted has been sent, so a failure to send can come back from a later call or when `Read` ends. They keep none of your memory, except the batch `Records` takes. `Records` releases that batch for you, and if it returns an error, the batch is still yours to release.

## Try it with the CLI

`openetl connector` runs a connector the way a runner does, with nothing else running: no running OpenETL installation. Build the CLI from a checkout of OpenETL, as in the [quickstart](../get-started/quickstart.md#3-build-the-cli).

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

`openetl connector check` checks a config reaches the upstream. Each command takes an executable, a descriptor, a declarative source's document, or a package version by reference, as `oci://REGISTRY/REPOSITORY:VERSION` or `github.com/OWNER/REPO[/PACKAGE]@VERSION`. A config names its secrets as `{"secret": NAME}`, whose values come from `--secret NAME=VALUE` or the variable `OPENETL_SECRET_NAME`. `--attach ADDR` waits for a connector you start yourself, under a debugger, say. `openetl connector read -h` has the rest.

## Test it with the conformance kit

The conformance kit, in [`sdk/go/conformance`](../../sdk/go/conformance), drives a connector over the protocol the way OpenETL does, and checks what the protocol promises: the spec, check, discovery, reads and resuming from resume points for a source; commits, recovery after a crash at each step of the commit, fencing, write modes, schema changes, types, refusing type changes, holds and writer lanes for a destination. It knows nothing of the connector's language.

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
SKIP read/incremental
    the suite reads no stream incrementally
SKIP read/committed
    the source doesn't acknowledge, so the platform never calls Committed
SKIP read/continuous
    the suite reads no stream with cdc or stream
source-greetings: 10 checks, 4 passed, 0 failed, 6 skipped
```

`--bad-credentials` and `--unreachable` add configs for the errors check, and `--add-records-command` lets the incremental check add records upstream. A destination's checks that write need `--read-command`, which prints what the destination made visible; they're skipped without it. `openetl connector test -h` has the details.

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

`package` builds each role for every platform (Linux and macOS, on amd64 and arm64, unless the file says otherwise) into `dist/`, as `greetings-source-linux-amd64` and the like, runs the one for this machine to read its spec, and writes the **descriptor**, `dist/greetings.connector.yaml`. The descriptor names each platform's executable by URL and digest; by default the URLs are the assets of the GitHub release of the version's tag, here `v0.1.0`, and `--tag` takes the tag instead of `--version`. Upload what `dist/` holds as that release's assets, or push it to an OCI registry with `openetl connector publish oci://REGISTRY/NAMESPACE`.

An org then applies a version, by reference or from the descriptor:

```sh
openetl connector apply github.com/acme/openetl-greetings@0.1.0
openetl connector apply oci://REGISTRY/NAMESPACE/greetings:0.1.0
openetl connector apply -f dist/greetings.connector.yaml
```

A `ConnectorSource` document connects a registry namespace or a GitHub repository to the org as a source of its catalog. `openetl connector catalog`, and **Connectors** in the console, list the packages its sources offer, with the command that applies each. OpenETL connects the official connectors' source, `oci://ghcr.io/openetl-io/connectors`, when it creates the org.

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

Write your connector's page where its code lives, and put its address in `docs` in `openetl-connector.yaml`. The descriptor carries it, and the console's **Connectors** catalog links to it as **Docs**. A good page says what the connector reads or writes, what it needs upstream (an account, permissions, settings), its config, how its types map, the delivery guarantee its streams get, and its limits.

The config's JSON Schema is the reference for its fields, so give every property a `description`. The official connectors' pages live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), next to their code.
