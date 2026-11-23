---
title: Write a source in Go
description: Build an example source and inspect its records with the CLI.
---

This tutorial builds an external source with the Go SDK. First, read the [connector contract](./protocol.md).

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

A pipeline can read an `sdk.Incremental` or `sdk.Resumable` stream with `full_refresh`, or write it with `overwrite`. Each new read of it then starts with no state: `Incremental` gets a zero floor, and `Resumable` gets nil.

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

Connectors run as trusted code: `egress` says what yours means to reach, and OpenETL does not enforce it yet. [What that means for the people who run them](../self-host/index.md#connectors-are-trusted-code).

`openetl connector check` checks a config reaches the upstream, and prints the check's items, as [Checks and preflights](./test-checks.md#with-openetl-connector-check) shows. Each command takes an executable, a descriptor, a declarative source's document, or a package version by reference, as `oci://REGISTRY/REPOSITORY:VERSION` or `github.com/OWNER/REPO[/PACKAGE]@VERSION`. A config names its secrets as `{"secret": NAME}`, whose values come from `--secret NAME=VALUE` or the variable `OPENETL_SECRET_NAME`. `--attach ADDR` waits for a connector you start yourself, under a debugger, say. `openetl connector read -h` has the rest.

Next, [test conformance](./test.md) before you [package and publish the connector](./publish.md).
