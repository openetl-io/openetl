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
| `Check` | Validate the connection configuration. |
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

### Setup and teardown

Connectors that maintain pipeline-specific resources, such as replication slots or staging tables, declare `setup` in their spec. In Go, set `Setup: true` in `sdk.Connector`. Implement `Setup`, `Teardown` or both.

OpenETL calls `Setup` before each attempt and `Teardown` after pipeline deletion. Both receive the connection configuration and pipeline ID. Both must be idempotent: existing resources or already removed resources must not cause an error. Without `setup`, OpenETL calls neither operation.

Failed teardown operations retry for up to 7 days. After that, OpenETL records `pipeline.teardown_failed` and stops retrying. In Go, implement the applicable `source.Setupper`, `source.Teardowner`, `destination.Setupper` or `destination.Teardowner` interfaces.

A destination must not change an existing column's type unless the pipeline permits it. This applies to target columns, staging columns and other columns that become user data. Refuse an unpermitted type change with `RefusedColumn`. Include the column, reason, types and a `Fix` statement or repair step, such as `ALTER TABLE`.

OpenETL holds the affected column where supported, or stops the stream. It shows the repair to the user. New columns can still be added.

## Write a source in Go

The SDK's `sdk.Source` declares a source from streams and the functions that read them. Here is a source with one stream, `greetings`, that makes up as many rows as its config asks for.

Make a module, and add the SDK of the OpenETL release that you build for. Each release tags the SDK as `sdk/go/vVERSION`, so Go resolves the release's version:

```sh
go mod init example.com/greetings
go get github.com/openetl-io/openetl/sdk/go@v0.1.0-alpha
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

`openetl connector check` checks a config reaches the upstream. Each command takes an executable, a descriptor, a declarative source's document, or a package version by reference, as `oci://REGISTRY/REPOSITORY:VERSION` or `github.com/OWNER/REPO[/PACKAGE]@VERSION`. A config names its secrets as `{"secret": NAME}`, whose values come from `--secret NAME=VALUE` or the variable `OPENETL_SECRET_NAME`. `--attach ADDR` waits for a connector you start yourself, under a debugger, say. `openetl connector read -h` has the rest.

## Test it with the conformance kit

The [conformance kit](../../sdk/go/conformance) tests connectors through the protocol, regardless of implementation language. Source checks cover specifications, connection checks, discovery, reads and resumption. Destination checks cover commits, recovery at each commit step, fencing, write modes, schema changes, types, type-change refusal, holds and writer lanes.

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

`package` builds each role into `dist/`, with names such as `greetings-source-linux-amd64`. The default targets are Linux and macOS on amd64 and arm64; the package file can override them. It runs the executable for the local platform to read its spec. It then writes the **descriptor**, `dist/greetings.connector.yaml`.

The descriptor lists each platform's executable URL and digest. By default, URLs point to GitHub release assets for the version's tag, here `v0.1.0`. `--tag` accepts a release tag instead of `--version`.

Upload the contents of `dist/` as GitHub release assets. Alternatively, publish them to an OCI registry with `openetl connector publish oci://REGISTRY/NAMESPACE`.

An org then applies a version, by reference or from the descriptor:

```sh
openetl connector apply github.com/acme/openetl-greetings@0.1.0
openetl connector apply oci://REGISTRY/NAMESPACE/greetings:0.1.0
openetl connector apply -f dist/greetings.connector.yaml
```

A `ConnectorSource` document connects a registry namespace or a GitHub repository to the org as a source of its catalog. `openetl connector catalog`, and **Connectors** in the console, list the packages its sources offer, with the command that applies each. OpenETL connects the official connectors' source, `oci://ghcr.io/openetl-io/openetl-connectors`, when it creates the org.

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
