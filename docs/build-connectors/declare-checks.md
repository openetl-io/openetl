---
title: Declare connector checks
description: Define named connection checks and preflights with the Go SDK.
---

Use this guide to add checks to a source or destination. It assumes that you know the [connector contract](./protocol.md).

## Check and preflight

A connector answers two questions before any data moves:

- **Check:** does this configuration reach the system? A check is about one source or destination, with no pipeline. Keep it light: connect, sign in, find what the config names, and read the account's own permissions.
- **Preflight:** would this pipeline work? A preflight gets what a sync of the pipeline would read or write, and examines each stream.

OpenETL runs a check when someone checks a source or a destination, and for each source and destination that an `apply` changes. It runs a preflight when someone preflights a pipeline, and after each `apply` that creates or changes a pipeline. A preflight runs on a runner, in this order:

1. Start the source connector, and run its check.
2. Start the destination connector, and run its check.
3. Settle what a sync reads and writes, as a sync does: discover the source's streams, and plan the sync.
4. Call the source connector's preflight.
5. Call the destination connector's preflight.

OpenETL calls a connector's preflight only if that connector started, its check passed, and the sync could be planned. Otherwise it skips the preflight, and says why. [Checks and preflights](../get-started/concepts.md#checks-and-preflights) describes both from the user's side.

| | Check | Preflight |
|---|---|---|
| RPC | `ConnectorService.Check` | `SourceService.PreflightSource`, `DestinationService.PreflightDestination` |
| Gets | The config | The config, and the pipeline's ID |
| A source also gets | Nothing | The streams, with their sync modes and cursors; whether the pipeline is continuous; the longest interval between its scheduled syncs, as `Interval` describes |
| A destination also gets | Nothing | The namespace; the streams, with their write modes, keys and delete modes; each stream's declared columns and its table, in the `openetl.table` schema metadata |
| Belongs here | Reach the service, credentials, the account's own permissions, what the config names | Each stream: exists, readable or writable, in a mode the connector has; retention against the schedule; resources the pipeline will create |

Which items a connector reports depends on its configuration. For example, a source that reads through a consumer checks that consumer only in the mode that uses one.

A warning does not fail the check or the preflight. The pipeline can still run, but not as configured or not as well as it could. Skip an item that an earlier failure makes pointless. Also skip an item that you cannot check without a change, such as writing a record, and name the permission it needs. Item text is shown and kept, so it never holds secrets or records.

A connector can have neither, either or both:

- **Without a check**, the check passes with no items.
- **Without a preflight**, the connector's part of a preflight passes with no items.
- **With a check that returns one error** (`Check(ctx) error`), people see that error and no items. Prefer a list of checks: one run then shows everything that is wrong.

## Declare checks

In Go, a connector declares its checks as a list of `check.Check` values, and the SDK runs them in order. Building the list does no I/O. Only each check's `Run` does.

### On a source or destination you write

Implement these interfaces on the value that your `Source` or `Destination` function returns to `sdk.Serve`. Each is in both `source` and `destination`:

| Interface | Method | What it lists |
|---|---|---|
| `CheckLister` | `Checks() []check.Check` | The connection's checks |
| `Preflighter` | `Preflight(req PreflightRequest) []check.Check` | A pipeline's checks, for what `req` would read or write |
| `Checker` | `Check(ctx context.Context) error` | No items: one error, or none |

If `Checks` returns a list that is not empty, the SDK does not call `Check`. The SDK calls `Checks` and `Preflight` for each request, so a method can build its list from the config and the request each time.

A source's `source.PreflightRequest` has the pipeline's ID in `Pipeline`. Its `Streams` are as a sync would read them: names, sync modes and cursors, but no states or splits. `Continuous` says whether the pipeline reads continuously. `Interval` is the longest time between its scheduled syncs, jitter included:

- For a schedule with `every`, it is `every` plus the jitter.
- For a cron schedule, it is the longest gap between firings over about one year, plus the jitter. OpenETL reads the cron in UTC, unless the expression starts with `CRON_TZ=`.
- It is 0 for a continuous pipeline, a pipeline without a regular schedule, and a cron that never fires.

A destination's `destination.PreflightRequest` has `Pipeline`, the `Namespace`, and the `Streams` as a sync would write them. `Schemas` has each stream's Arrow schema by stream name, with its table in the `openetl.table` metadata key.

[A complete example](./check-example.md#a-complete-example) implements both methods on a source.

### Through sdk.Source

`sdk.Source[C]` has two fields for the same lists. Each gets the bound config and the host services:

```go
var orders = sdk.Source[config]{
	Name: "source-orders",
	Checks: func(cfg *config, h host.Host) []check.Check {
		me := host.Request{URL: cfg.BaseURL + "/v1/me", Auth: host.Bearer{TokenSecret: cfg.Token}}
		return []check.Check{
			httpcheck.Reach(h, me),        // api.reach
			httpcheck.Authenticate(h, me), // api.auth
		}
	},
	Preflight: func(cfg *config, h host.Host, req source.PreflightRequest) []check.Check {
		var checks []check.Check
		for _, st := range req.Streams {
			list := host.Request{URL: cfg.BaseURL + "/v1/" + url.PathEscape(st.Name) + "?limit=1", Auth: host.Bearer{TokenSecret: cfg.Token}}
			c := httpcheck.Access(check.Name("stream", st.Name, "read"), "Read {stream}", h, list)
			c.Params["stream"] = st.Name
			checks = append(checks, c)
		}
		return checks
	},
	Streams: []sdk.Stream[config]{ /* ... */ },
}
```

`Checks` replaces the older `Check` field when it returns any checks. `h` is nil when the source is bound with `New`, which binds it without host services.

### The fields of a check

| Field | What it is for |
|---|---|
| `Name` | Identifies the item. Unique in the list and the same from one release to the next. See [Names](#names). |
| `Kind` | What the check verifies. See [Kinds](#kinds). Zero is `check.Custom`. |
| `Title` | What is checked, for people: `Read {stream}`. A [template](./write-checks.md#placeholders). Required. |
| `Description` | What the check verifies, and why the connector needs it. A template. Optional. |
| `Field` | The config field the check concerns, as a dotted path such as `consumer.name`. Clients show the item with that field. Optional. |
| `Params` | Values for the templates' placeholders. Never secrets. |
| `Needs` | Names of earlier checks that must pass or warn before this one runs. See [Order and needs](./write-checks.md#order-and-needs). |
| `Advisory` | A failure is a warning. |
| `Timeout` | How long `Run` can take. Zero: only the run's own deadline. |
| `Run` | Does the check. Returns nil to pass, `check.Skip`'s error to skip, `check.Warn`'s to warn, and any other error to fail. |

`check.Validate` refuses a list with a malformed or repeated name, or a check without a title or a `Run`. It also refuses a need that is not an earlier check, and a malformed placeholder. The SDK runs nothing from a list it refuses. The answer then has no items and an `INTERNAL` error that starts "the connector declares its checks wrongly".

### Names

A name is lowercase letters and digits, in words joined by `.`, `_` or `-`: `^[a-z0-9]+([._-][a-z0-9]+)*$`. Clients follow an item by its name from one run and one release to the next. Do not rename a check in a new release.

Begin a name with what it concerns, joined by dots: `stream.orders.read`, `replication.slot`. The toolkits' fixed names begin with `net.`, `api.` or `db.`, so they do not collide with yours or with each other.

For a name built from data, such as a stream's or a table's, use `check.Name`. It lowercases each part and replaces each run of other characters with `-`. It then trims `-` from the part's ends, and uses `x` for a part left empty. Last, it joins the parts with `.`, so the result is always a valid name:

| Call | Name |
|---|---|
| `check.Name("stream", "My Orders", "read")` | `stream.my-orders.read` |
| `check.Name("table", "public.orders", "read")` | `table.public-orders.read` |

Different parts can give the same name: `My Orders` and `my_orders` both give `my-orders`. `check.Validate` refuses the repeated name. If your data can have such names, add something to each name that tells them apart.

### Kinds

| Kind | What the check verifies |
|---|---|
| `check.Configuration` | The config's values make sense together, beyond what its schema says. |
| `check.Connection` | The network path: the name resolves, the port answers, TLS agrees. |
| `check.Authentication` | The upstream accepts the credentials. |
| `check.Permission` | The identity can do what the connector needs: read a table, write to a bucket, create a replication slot. |
| `check.Resource` | Something the config names exists: a database, a schema, a bucket, a topic. |
| `check.Compatibility` | The upstream's version or settings are ones the connector works with. |
| `check.Quota` | Limits leave room for the connector: rate limits, quotas, free slots. |
| `check.Custom` | The connector's own kind of check. |
