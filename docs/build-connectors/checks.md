---
title: Checks and preflights
description: Declare the checks a connector runs on its connection and on a pipeline, build them from the SDK's toolkits, and test them.
---

A check tells people whether a source's or a destination's configuration works. A preflight tells them whether a pipeline would run. Both answer with items: one for each thing the connector looked at, each passed, warning, failed or skipped, with a fix for what failed.

This page explains what belongs in each, and how to declare them in Go with the SDK's `check` package and its toolkits. It then gives a complete example, the protocol for connectors in other languages, and how to test your checks. [Build a connector](./index.md) has the rest of the connector contract.

## Check and preflight

A connector answers two questions before any data moves:

- **Check:** does this configuration reach the system? A check is about one source or destination, with no pipeline. Keep it light: connect, sign in, find what the config names, and read the account's own permissions.
- **Preflight:** would this pipeline work? A preflight gets what a sync of the pipeline would read or write, and examines each stream.

OpenETL runs a check when someone checks a source or a destination, and for each source and destination that an `apply` changes. It runs a preflight when someone preflights a pipeline, and after each `apply` that creates or changes a pipeline. A preflight starts with the checks of both connections. OpenETL calls a connector's preflight only if that connector's check passed. [Checks and preflights](../get-started/concepts.md#checks-and-preflights) describes both from the user's side.

| | Check | Preflight |
|---|---|---|
| RPC | `ConnectorService.Check` | `SourceService.PreflightSource`, `DestinationService.PreflightDestination` |
| Gets | The config | The config, and the pipeline's ID |
| A source also gets | Nothing | The streams, with their sync modes and cursors; whether the pipeline is continuous; the longest interval between its scheduled syncs |
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

A source's `source.PreflightRequest` has the pipeline's ID in `Pipeline`. Its `Streams` are as a sync would read them: names, sync modes and cursors, but no states or splits. `Continuous` says whether the pipeline reads continuously. `Interval` is the longest time between its scheduled syncs, or 0.

A destination's `destination.PreflightRequest` has `Pipeline`, the `Namespace`, and the `Streams` as a sync would write them. `Schemas` has each stream's Arrow schema by stream name, with its table in the `openetl.table` metadata key.

[A complete example](#a-complete-example) implements both methods on a source.

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
| `Title` | What is checked, for people: `Read {stream}`. A [template](#placeholders). Required. |
| `Description` | What the check verifies, and why the connector needs it. A template. Optional. |
| `Field` | The config field the check concerns, as a dotted path such as `consumer.name`. Clients show the item with that field. Optional. |
| `Params` | Values for the templates' placeholders. Never secrets. |
| `Needs` | Names of earlier checks that must pass or warn before this one runs. See [Order and needs](#order-and-needs). |
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

## Start from a toolkit

The SDK has four toolkits that make common checks. Each function returns a `check.Check`, or a list of them. Change its `Title`, `Description`, `Field`, `Needs`, `Timeout` or `Advisory` before you put it in your list.

Some toolkit checks have params already. Add to them with `c.Params["name"] = value`. Set `c.Params` only on a check that has none, such as the checks `sqlcheck` makes with `Check`, `Query`, `True` and `Value`.

### netcheck

`github.com/openetl-io/openetl/sdk/go/check/netcheck` checks the network path to a host. Its checks have no state.

| Function | Name | Kind | Needs | Params | Passes when |
|---|---|---|---|---|---|
| `Resolve(host)` | `net.dns` | Connection | Nothing | `host`; `ips` when it passes | The name, or a `host:port`'s host, resolves. An IP address passes without a lookup. Fails as `dns_failed`. |
| `Dial(address)` | `net.connect` | Connection | Nothing | `address` | The `host:port` accepts a TCP connection within `netcheck.DialTimeout`, 10 seconds. Fails as `check.Classify` classifies the dial's error, such as `connection_refused` or `timed_out`. |
| `TLS(address, cfg)` | `net.tls` | Connection | Nothing | `address`; `tls_version` and `expires` when it passes | The handshake completes and `cfg` verifies it. Fails as `tls_failed`, or `timed_out`. Warns as `certificate_expiring` when the certificate expires within 14 days. |
| `Reach(address)` | `net.dns`, `net.connect` | Connection | `net.connect` needs `net.dns` | As above | Both pass. |
| `ReachTLS(address, cfg)` | `net.dns`, `net.connect`, `net.tls` | Connection | Each needs the one before | As above | All three pass. |

The TLS check is for a port that starts with TLS, such as an HTTPS or LDAPS port. A database that starts TLS inside its own protocol, such as PostgreSQL, MySQL or SQL Server, fails it. For such a database, use `Reach`, and let the driver's connect check cover TLS.

A nil `cfg` means TLS 1.2 or later, verified against the system's roots. An empty `ServerName` means the address's host. If your connector trusts its own certificate authority, pass the `tls.Config` that the connector uses.

### httpcheck

`github.com/openetl-io/openetl/sdk/go/check/httpcheck` checks an HTTP API through the host's HTTP service. The first argument, `h`, is the `host.Host` the connector gets.

| Function | Name | Kind | Needs | Passes when |
|---|---|---|---|---|
| `Reach(h, req)` | `api.reach` | Connection | Nothing | `req` gets an answer with a status below 500. |
| `Authenticate(h, req)` | `api.auth` | Authentication | `api.reach` | `req` succeeds with its credentials, with a status below 400. |
| `Access(name, title, h, req)` | Yours | Permission | Nothing | `req` succeeds, with a status below 400. |
| `JSON(name, title, h, req, verify)` | Yours | Permission | Nothing | As `Access`, then the body, of at most 1 MiB, decodes into the type that `verify` takes, and `verify` returns nil. |

Each check sets the param `url`: the request's URL without its user info, query and fragment, which can hold credentials. A request whose `Budget` is zero gets 10 seconds, not the host's default, which allows minutes of retries.

`Access` and `JSON` need nothing, so they run even when the API is down. If a check only makes sense once the API answers, set its `Needs` to `api.reach` or `api.auth`. Then an API that is down does not cost each check its whole budget:

```go
me := host.Request{URL: cfg.BaseURL + "/v1/me", Auth: host.Bearer{TokenSecret: cfg.Token}}
account := host.Request{URL: cfg.BaseURL + "/v1/account", Auth: me.Auth}
plan := httpcheck.JSON("account.plan", "Read the account's plan", h, account,
	func(ctx context.Context, p *check.Probe, body *struct{ Plan string }) error {
		p.Param("plan", body.Plan)
		if body.Plan == "free" {
			return check.Warn(planTooSmall.Fail())
		}
		p.Found("The account is on the {plan} plan.")
		return nil
	})
plan.Needs = []string{"api.auth"}
checks := []check.Check{httpcheck.Reach(h, me), httpcheck.Authenticate(h, me), plan}
```

The checks fail as `httpcheck.Classify` classifies the error. The host can give a status a code of its own, for example from a declarative source's manifest. If that code is not the status's default, it decides the reason. Otherwise the status decides:

| Status | Reason |
|---|---|
| 400 | `config_invalid` |
| 401 | `credentials_rejected` |
| 403 | `permission_denied` |
| 404, 410 | `not_found`, with `resource` set to the URL |
| 429 | `rate_limited` |
| 500 and above, except 501 and 505 | `unavailable` |

A failure with a status sets the param `status`, and the detail has the start of a 4xx response's body. A request that got no response fails as one of the network's reasons, such as `unreachable`, with `address` set to the URL's `host:port`. If the host did not send the request, it fails as `config_invalid`. The host does not send it when the name does not resolve, its egress rules refuse the address, or it does not trust the certificate.

In `JSON`, a body that is not JSON fails as `config_invalid`. JSON that does not decode into the type fails as `unknown`, with the code `CONTRACT_CHANGED`.

### sqlcheck

`github.com/openetl-io/openetl/sdk/go/check/sqlcheck` checks a database through `database/sql`. `sqlcheck.New(open)` makes a `Kit`, whose checks share one `*sql.DB`. The first check that runs opens it with `open`, and it closes once every run that used it has ended.

| Method | Name | Kind | Params | Passes when |
|---|---|---|---|---|
| `Connect()` | `db.connect` | Connection | None | The database answers a ping. |
| `Query(name, title, query, args...)` | Yours | Permission | None | The query runs and its first row, if any, reads. Some drivers report an error, such as a missing privilege, only on the first row. |
| `True(name, title, denied, missing, query, args...)` | Yours | Permission | None | The query's first value is true: a boolean, its text such as `t`, or the integer 1. |
| `Readable(name, relation)` | Yours | Permission | `relation`, the relation; `resource`, "The table " and the relation | `SELECT * FROM relation WHERE 1=0` runs. Its title is `Read {relation}`. |
| `Value(name, title, verify, query, args...)` | Yours | Compatibility | `found`, set to the value | `verify` returns nil for the query's first value, as text. NULL is the empty string. |
| `Check(name, kind, title, run)` | Yours | Yours | None | Your `run` returns nil. See [Reuse a toolkit's connection](#reuse-a-toolkits-connection). |

Every check but `Connect` needs `db.connect`, so put `Connect` first in the list.

In `True`, another value fails with `denied`, and no row or NULL fails with `missing`. A nil `denied` is `check.PermissionDenied`'s failure. A nil `missing` is `check.NotFound`'s, with the message "The query found no row.".

In `Value`, `verify` returns the check's error. Return `check.UnsupportedVersion` or `check.SettingMismatch` failures there: the kit sets their `{found}` to the value. No row fails as `not_found`.

> [!WARNING]
> `Readable` uses `relation` in its query as written. Quote it for the database's dialect, and never take it from input that the config's owner does not control.

The kit classifies a driver's error with `Kit.Failure`, as [Classify driver errors](#classify-driver-errors) explains.

### adbccheck

`github.com/openetl-io/openetl/sdk/go/check/adbccheck` checks a database through ADBC. It is a module of its own, so that the SDK does not require ADBC. Its versions are its own, tagged `sdk/go/check/adbccheck/vX.Y.Z`, apart from the SDK's `sdk/go/vX.Y.Z`. Require it by one of those versions.

Its `Kit` shares one ADBC database and connection. It has the same methods as `sqlcheck`'s, except `Readable`, and adds `Table`:

```go
k := adbccheck.New(func(ctx context.Context) (adbc.Database, error) { return drv.NewDatabase(opts) })
checks := []check.Check{k.Connect()}
for _, t := range tables {
	checks = append(checks, k.Table(check.Name("table", t, "find"), nil, &schema, t))
}
```

- `Connect` opens a connection and asks for the vendor's name, so a driver that connects lazily makes a round trip.
- `Query`, `True` and `Value` take a query without arguments.
- `Table(name, catalog, schema, table)` is a Resource check that the table exists and its schema reads. A nil catalog or schema is the connection's default. It sets `table` and `resource`. A driver that cannot read a table's schema skips it.

ADBC does not make a connection safe for concurrent use, so the kit's checks use the connection one at a time, also when runs overlap. A check that needs the connection while another check uses it waits. If its deadline passes first, it fails as `timed_out` with the message "The connection to the database was still in use by a check that didn't finish." and no fix. A check left running in the driver keeps the connection until its call returns. In a check of your own, close the statements and readers you open before `run` returns, not in a cleanup.

`Kit.Failure` classifies an `adbc.Error` by its status. If the status is `Unknown` or `Internal`, it classifies by the SQLSTATE, as `sqlcheck` does. It sets the params `sqlstate` and `vendor_code` where the error has them.

### Combine toolkits

The toolkits' checks go into one list. Put the checks that others need first, and add needs between kits where one makes sense only after the other:

```go
k := sqlcheck.New(func(ctx context.Context) (*sql.DB, error) { return sql.Open("pgx", cfg.DSN) })
connect := k.Connect()
connect.Needs = []string{"net.connect"}
checks := netcheck.Reach(net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))) // net.dns, net.connect
checks = append(checks, connect)                                             // db.connect
```

If the host does not resolve, `db.connect` is skipped, and people see one failure, `net.dns`, rather than three.

The toolkits' names are fixed, so a list holds one check of each such name. `sqlcheck` and `adbccheck` both make `db.connect`, so a list holds one database kit.

## Write your own check

Write a check of your own for what the toolkits do not cover. A check is a `check.Check` with a `Run`. Its failures say what went wrong in words people can act on.

### Reasons and failures

A `check.Reason` is a kind of failure: a name, an error code, a message and a fix. The message and the fix are [templates](#placeholders). Use one of the [standard reasons](#the-standard-reasons), or declare your own:

```go
var bucketMissing = check.Reason{
	Name:    "bucket_missing",
	Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
	Message: "The bucket {bucket} doesn't exist.",
	Fix:     "Create the bucket {bucket}, or correct bucket in the config.",
}

func (d *Destination) bucketCheck() check.Check {
	return check.Check{
		Name:   "bucket.exists",
		Kind:   check.Resource,
		Title:  "Find the bucket {bucket}",
		Field:  "bucket",
		Params: check.Params{"bucket": d.cfg.Bucket},
		Run: func(ctx context.Context, p *check.Probe) error {
			region, err := d.client.BucketRegion(ctx, d.cfg.Bucket)
			switch {
			case errors.Is(err, errNoSuchBucket):
				return bucketMissing.Fail()
			case err != nil:
				return err
			}
			p.Param("region", region)
			p.Found("{bucket} is in {region}.")
			return nil
		},
	}
}
```

A reason's name is lowercase words joined by `_`. A reason with an unspecified code has the code `INTERNAL`.

`Reason.Fail` makes a `*check.Failure`, which is the error a check fails with. It takes params as name and value pairs: `bucketMissing.Fail("bucket", name)`. Here it needs none, because the check's own `bucket` param fills `{bucket}`. A name without a value gets the value `!MISSING`.

`Failure.With` returns a copy of a failure with more params, and leaves the original unchanged. So one failure value can serve many checks:

```go
notFound := check.NotFound.Fail()
return notFound.With("resource", "The topic "+name)
```

A `Failure` that you build by hand works too. An empty `Reason` reads as `unknown`, an unspecified `Code` as `INTERNAL`, and an empty `Message` as `unknown`'s:

```go
return &check.Failure{Reason: "plan_too_small", Code: protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, Message: "The account's plan allows {limit} requests a day."}
```

### Attach the cause

`Reason.Wrap` is `Fail` with the error that caused the failure. The cause's text becomes the failure's `Detail`, and a wait before retrying in the cause becomes its `After`. The code stays the reason's:

```go
if errors.Is(err, errNoSuchBucket) {
	return bucketMissing.Wrap(err)
}
```

The detail is shown under the message, as the driver's or the API's own words. It is not a template. `Wrap` removes the user info, query and fragment of each `*url.Error`'s URL in the cause from the detail, and so does `check.Classify`.

A `Run` can also return any other error. The SDK then classifies it with `check.Classify`:

```go
if err != nil {
	return fmt.Errorf("look up the bucket's region: %w", err)
}
```

`check.Classify` uses, in order:

1. The first non-nil `*check.Failure` in the error's chain. The text of the errors that wrap it is kept in `Detail`.
2. The error's shape. An address or URL that does not parse is `config_invalid`. DNS, TLS, a refused connection, a timeout and other network errors get the network's reasons.
3. The protocol error code in its chain, such as one from `sdk.WithCode`. `AUTH_INVALID` is `credentials_rejected`, `PERMISSION_DENIED` is `permission_denied`, and so on.
4. Otherwise `unknown`.

A code in the chain replaces the reason's code in steps 2 to 4.

A nil `*check.Failure` returned as the error itself is a pass. So `return k.Failure(err)` passes when `err` is nil. A nil `*check.Failure` inside another error, such as in `errors.Join`, does not hide the other errors.

### Choose a code

The code tells OpenETL and people what kind of failure it is. These are the codes a check uses. [`errors.proto`](../../proto/openetl/connector/v1/errors.proto) has the full list.

| Code | Use it when |
|---|---|
| `CONFIG_INVALID` | The config, or the upstream's setup, must change: a wrong name, a missing resource, a wrong setting. Trying again does not help. |
| `AUTH_INVALID` | The upstream rejects the credentials. |
| `AUTH_EXPIRED` | The credentials have expired. |
| `PERMISSION_DENIED` | The identity is known but lacks a permission. |
| `TRANSIENT` | Trying again later can work: the network, a timeout, an upstream that cannot serve now. |
| `RATE_LIMITED` | The upstream limits requests. Set `After` when it says how long to wait. |
| `RESOURCE_EXHAUSTED` | A quota or a limit is used up. |
| `CONTRACT_CHANGED` | The upstream answers in a way the connector does not expect. |
| `INTERNAL` | The connector cannot say. `unknown` has this code. |

### Params found at run time

A check's `Params` hold what you know when you build the list. `Probe.Param` sets a value that the check finds as it runs, as `fmt.Sprint` prints it. Use it for both a failure's templates and a passing check's message.

`Probe.Found` sets a passing check's message, a template: what it found. `p.Found("{bucket} is in {region}.")` tells people more than a bare pass. A check that passes without `Found` has no message.

### Reuse a toolkit's connection

`sqlcheck`'s and `adbccheck`'s `Kit.Check` make a check of your own that runs with the kit's handle or connection. It needs `db.connect`. Return `k.Failure(err)` for a driver's error, so the kit classifies it. [A complete example](#a-complete-example) has one, `replication.slot`.

`k.Failure` returns nil for a nil error, and `With` keeps it nil, so this passes when `err` is nil:

```go
return k.Failure(err).With("table", table)
```

### Classify driver errors

`sqlcheck`'s `Kit.Failure` classifies an error in three steps:

1. The kit's `Classify` hook, if you set one and it returns a failure.
2. The SQLSTATE of an error in the chain with a `SQLState() string` method, as pgx's and lib/pq's errors have. Each failure sets the param `sqlstate`.
3. `check.Classify`.

| SQLSTATE | Reason |
|---|---|
| Class `28` | `credentials_rejected` |
| `42501` | `privilege_missing`, with the message "The database user lacks a privilege this needs." |
| `42P01`, `42S02` | `not_found`, with `resource` "The table" |
| `3D000` | `not_found`, with `resource` "The database" |
| `3F000` | `not_found`, with `resource` "The schema" |
| Class `08` | `unreachable`, with the message "Can't reach the database." |

A check's own params win over a failure's. So `Readable`'s `resource` names the table that does not exist: "The table X doesn't exist.", as `adbccheck`'s `Table` says it.

Set `Classify` to give a vendor's errors reasons of your own. Return nil to leave an error to the kit. Set it before any check runs:

```go
k.Classify = func(err error) *check.Failure {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok && pgErr.Code == "55006" {
		return slotInUse.Wrap(err)
	}
	return nil
}
```

`adbccheck`'s `Kit` has the same hook. In a check of your own that calls the host's HTTP service, classify its errors as `httpcheck`'s checks do:

```go
resp, err := h.HTTP(ctx, &req)
if err != nil {
	return httpcheck.Classify(err, req.URL)
}
defer func() { _ = resp.Body.Close() }()
```

## Placeholders

A check's `Title` and `Description`, a failure's message and fix, and a passing check's message are templates. `{name}` in a template stands for the param of that name. A name is a lowercase letter, then lowercase letters, digits and `_`: `[a-z][a-z0-9_]*`.

The connector sends each template and its params apart, and the client that shows the item fills them in. A client can then show a value apart from the text, or in another language.

An item's params come from three places. If two of them set a name, the first in this list wins:

1. `Probe.Param`, as the check runs.
2. The check's `Params`.
3. The failure's params, which only add names that the first two do not set.

So `bucketMissing.Fail("bucket", "other")` does not change a `{bucket}` that the check's params set. To show another value in a failure, set it with `Probe.Param` first.

A placeholder without a param stays as written, such as `{schema}`. The conformance kit flags it. Text without placeholders is plain text, and a value is not filled in again: a value with braces shows as written.

`check.Validate` refuses a param name outside the grammar. It also refuses what looks like a placeholder in a title or a description but is not one, such as `{Host}` or `{db-host}`. The conformance kit also fails such text in a message or a fix.

Params are shown to people and kept with results. Never put a secret in a param, a template or a detail.

## Order, skips, warnings and time

### Order and needs

The SDK runs the checks one at a time, in the list's order, and answers with the items in that order. A check with `Needs` runs only if each check it names has passed or warned. Otherwise it is skipped, with a message such as "needs db.connect, which failed". A need must name an earlier check in the list.

Put the cheap checks that others depend on first: the network, then the connection, then credentials, then permissions and resources.

### Skip

Return `check.Skip` from `Run` when a check does not apply. Its text is the item's message:

```go
if readOnly {
	return check.Skip("the config is read-only, so the connector never writes")
}
```

The text is a template like any message, so a `{name}` in it is filled from the params. Put a value that can hold braces in a param, not in the format's arguments. Prefer to leave a check out of the list when the config alone shows that it does not apply.

### Warnings

A warning does not fail the check or the preflight. There are two ways to warn:

- **`Advisory: true`** makes any failure of the check a warning. Use it for a check of something that makes the pipeline work less well, but still work.
- **`check.Warn(err)`** makes one error a warning, and the check's other errors still fail it. `check.Warn(nil)` is nil.

A warning keeps its failure's reason, code, fix and detail.

### Time limits

A check's `Timeout` limits its `Run`, and the run's context limits every check. The context that `Run` gets ends at the earlier of the two. Stop when it ends:

- A check that returns nil after its deadline is not a pass.
- A check that has not returned 100 milliseconds after its deadline is left running, and the run moves on.

Either check fails as `timed_out`, with the message "The check didn't finish in time." and no fix, since the check, not the network, was slow. A timeout error that the check returns itself keeps `timed_out`'s fix.
- An `Advisory` check that runs out of time warns rather than fails.

A check left running cannot change its item: once its item is done, `Param` and `Found` do nothing.

### Cleanups

`Probe.Cleanup` registers a function that runs after every check of the run has ended, the last registered first. Use it for what a check opens and must close, even if the check is left running:

```go
func (d *Destination) sessionCheck() check.Check {
	return check.Check{
		Name:    "session.open",
		Kind:    check.Connection,
		Title:   "Open a session with {host}",
		Params:  check.Params{"host": d.cfg.Host},
		Needs:   []string{"net.connect"},
		Timeout: 20 * time.Second,
		Run: func(ctx context.Context, p *check.Probe) error {
			s, err := d.client.OpenSession(ctx)
			if err != nil {
				return err
			}
			p.Cleanup(func(ctx context.Context) { _ = s.Close(ctx) })
			return s.Ping(ctx)
		},
	}
}
```

A cleanup's context is not cancelled with the run, and ends 10 seconds after the cleanups start. `check.Run` waits for the cleanups only until shortly after its own context is done. Cleanups still running then finish in the background, so a slow cleanup does not delay the answer past its deadline. The SDK recovers a panic in each cleanup, and the others still run. A check left running that registers a cleanup after its item is done runs it at once. The toolkits' kits use cleanups to close their shared handle. `adbccheck`'s kit does not close its connection while a check left running still uses it.

### Cancellation and panics

If the run's context is cancelled, the check that is running and the checks after it are skipped. If its deadline passes, the checks after the running one are skipped. Either way, the run fails: its error is the first failed item's, or else one that says the context ended.

The SDK recovers a panic in `Run` as the check's failure, and a panic in `Checks` or `Preflight` as the answer's error.

## A complete example

This source reads a PostgreSQL database's changes through a replication slot and a publication. The example is not an official connector: it shows how a connector's checks fit together. The rest of the source, its `Discover` and `Read`, is not shown.

Its check combines `sqlcheck` checks with one of its own:

- `db.connect` connects.
- `role.replication` checks a role attribute with `True`.
- `setting.wal-level` checks a setting with `Value`, and `setting.max-slot-wal-keep-size` warns about another, as an advisory check.
- One `Readable` check for each configured table, named with `check.Name`.
- `replication.slot` is its own check, with its own reasons and the kit's connection.

Its preflight makes one check for each stream that the pipeline reads: that the stream's table is in the publication.

```go
package pgsource

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/sqlcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

type Config struct {
	Host        string   `json:"host"`
	Port        int      `json:"port"`
	Database    string   `json:"database"`
	User        string   `json:"user"`
	Password    string   `json:"password"`
	Slot        string   `json:"slot"`
	Publication string   `json:"publication"`
	Tables      []string `json:"tables"`
}

type Source struct {
	cfg Config
}

var (
	_ source.CheckLister = (*Source)(nil)
	_ source.Preflighter = (*Source)(nil)
)

var (
	slotMissing = check.Reason{
		Name:    "replication_slot_missing",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The replication slot {slot} doesn't exist on {host}.",
		Fix:     "Create it: SELECT pg_create_logical_replication_slot('{slot}', 'pgoutput');",
	}
	slotPlugin = check.Reason{
		Name:    "replication_slot_plugin",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "The replication slot {slot} decodes with {plugin}, and the connector needs pgoutput.",
		Fix:     "Drop the slot and create it again with the plugin pgoutput.",
	}
	notPublished = check.Reason{
		Name:    "table_not_published",
		Code:    protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID,
		Message: "{stream} isn't in the publication {publication}.",
		Fix:     "Run ALTER PUBLICATION {publication} ADD TABLE {stream};",
	}
)

// kit is new for each list, so each run opens one handle and closes it.
func (s *Source) kit() *sqlcheck.Kit {
	dsn := url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(s.cfg.User, s.cfg.Password),
		Host:   net.JoinHostPort(s.cfg.Host, strconv.Itoa(s.cfg.Port)),
		Path:   s.cfg.Database,
	}
	return sqlcheck.New(func(ctx context.Context) (*sql.DB, error) {
		return sql.Open("pgx", dsn.String())
	})
}

func (s *Source) Checks() []check.Check {
	k := s.kit()

	noReplication := check.PrivilegeMissing.Fail("principal", s.cfg.User, "privilege", "REPLICATION", "object", "the server")
	noReplication.Fix = "Run ALTER ROLE {principal} WITH REPLICATION;"
	replication := k.True("role.replication", "Stream changes as {user}", noReplication, nil,
		`SELECT rolreplication OR rolsuper FROM pg_roles WHERE rolname = current_user`)
	replication.Params = check.Params{"user": s.cfg.User}
	replication.Field = "user"

	walLevel := k.Value("setting.wal-level", "Decode the write-ahead log", func(v string) error {
		if v != "logical" {
			return check.SettingMismatch.Fail("setting", "wal_level", "required", "logical")
		}
		return nil
	}, `SHOW wal_level`)

	walKeep := k.Value("setting.max-slot-wal-keep-size", "Bound the WAL a slot keeps", func(v string) error {
		if v == "-1" {
			return check.SettingMismatch.Fail("setting", "max_slot_wal_keep_size", "required", "a limit, such as 100GB")
		}
		return nil
	}, `SHOW max_slot_wal_keep_size`)
	walKeep.Advisory = true
	walKeep.Description = "A slot that falls behind keeps WAL on the server's disk until this limit."

	checks := []check.Check{k.Connect(), replication, walLevel, walKeep}
	for _, t := range s.cfg.Tables {
		read := k.Readable(check.Name("table", t, "read"), pgx.Identifier(strings.Split(t, ".")).Sanitize())
		read.Field = "tables"
		checks = append(checks, read)
	}
	return append(checks, s.slotCheck(k))
}

func (s *Source) slotCheck(k *sqlcheck.Kit) check.Check {
	c := k.Check("replication.slot", check.Resource, "Find the replication slot {slot}",
		func(ctx context.Context, p *check.Probe, db *sql.DB) error {
			var plugin string
			err := db.QueryRowContext(ctx, `SELECT plugin FROM pg_replication_slots WHERE slot_name = $1`, s.cfg.Slot).Scan(&plugin)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				return slotMissing.Fail()
			case err != nil:
				return k.Failure(err)
			}
			p.Param("plugin", plugin)
			if plugin != "pgoutput" {
				return slotPlugin.Fail()
			}
			p.Found("{slot} decodes with {plugin}.")
			return nil
		})
	c.Params = check.Params{"slot": s.cfg.Slot, "host": s.cfg.Host}
	c.Field = "slot"
	return c
}

func (s *Source) Preflight(req source.PreflightRequest) []check.Check {
	k := s.kit()
	checks := []check.Check{k.Connect()}
	for _, st := range req.Streams {
		c := k.True(check.Name("stream", st.Name, "published"), "Publish {stream} in {publication}", notPublished.Fail(), nil,
			`SELECT EXISTS (SELECT 1 FROM pg_publication_tables WHERE pubname = $1 AND schemaname || '.' || tablename = $2)`,
			s.cfg.Publication, st.Name)
		c.Params = check.Params{"stream": st.Name, "publication": s.cfg.Publication}
		c.Field = "publication"
		checks = append(checks, c)
	}
	return checks
}
```

Some details of the example:

- `slotMissing.Fail()` and `notPublished.Fail()` take no params. The check's params fill their placeholders.
- `slotPlugin`'s `{plugin}` comes from `p.Param`, which the check calls before it fails.
- The password is in the DSN, but not in a param or a template. Mark `password` with `"x-secret": true` in the config schema, and serve the connector on the `EgressRaw` tier. The SDK then puts the secret's value in the config, and removes it from the answers.

To see the items, run each list with `check.Run`, and print each item with its templates filled in:

```go
func show(r check.Report) {
	for _, it := range r.Items {
		it = it.Rendered()
		fmt.Printf("%-8s %s: %s\n", it.Status, it.Name, it.Title)
		if it.Message != "" {
			fmt.Printf("         %s\n", it.Message)
		}
		if f := it.Failure; f != nil {
			if f.Detail != "" {
				fmt.Printf("         detail: %s\n", f.Detail)
			}
			if f.Fix != "" {
				fmt.Printf("         fix: %s\n", f.Fix)
			}
		}
	}
	fmt.Println("error:", r.Err())
}
```

The config here has the host `db.example.com`, the user `openetl`, the slot `openetl_orders`, the publication `openetl` and the tables `public.orders` and `public.refunds`. The database has `wal_level` set to `logical` and `max_slot_wal_keep_size` set to `-1`. The user can read `public.orders` but not `public.refunds`. The slot does not exist, and the publication has `public.orders` only. The check, `show(check.Run(ctx, src.Checks()))`, prints:

```text
passed   db.connect: Connect to the database
passed   role.replication: Stream changes as openetl
passed   setting.wal-level: Decode the write-ahead log
warning  setting.max-slot-wal-keep-size: Bound the WAL a slot keeps
         max_slot_wal_keep_size is -1, and the connector needs a limit, such as 100GB.
         fix: Set max_slot_wal_keep_size to a limit, such as 100GB.
passed   table.public-orders.read: Read "public"."orders"
failed   table.public-refunds.read: Read "public"."refunds"
         The database user lacks a privilege this needs.
         detail: ERROR: permission denied for table refunds (SQLSTATE 42501)
         fix: Grant the connector's database user the privilege the detail names.
failed   replication.slot: Find the replication slot openetl_orders
         The replication slot openetl_orders doesn't exist on db.example.com.
         fix: Create it: SELECT pg_create_logical_replication_slot('openetl_orders', 'pgoutput');
error: Read "public"."refunds": The database user lacks a privilege this needs: ERROR: permission denied for table refunds (SQLSTATE 42501)
```

A preflight of the streams `public.orders` and `public.refunds` prints:

```text
passed   db.connect: Connect to the database
passed   stream.public-orders.published: Publish public.orders in openetl
failed   stream.public-refunds.published: Publish public.refunds in openetl
         public.refunds isn't in the publication openetl.
         fix: Run ALTER PUBLICATION openetl ADD TABLE public.refunds;
error: Publish public.refunds in openetl: public.refunds isn't in the publication openetl.
```

The `error` line is the report's `Err`: the first failed item, as a host from before items shows it.

## The standard reasons

The `check` package has 17 standard reasons, and `netcheck` adds one. The toolkits and `check.Classify` fail with them, and your checks can too. Each expects the params that its message and fix use. A reason without a fix has none of its own: give the failure one where you can.

| Reason | In Go | Code | Message | Fix | Placeholders |
|---|---|---|---|---|---|
| `dns_failed` | `check.DNSFailed` | `TRANSIENT` | `The host name {host} doesn't resolve.` | `Check the host name in the config, and that the runner's network resolves it.` | `host` |
| `connection_refused` | `check.ConnectionRefused` | `TRANSIENT` | `{address} refused the connection.` | `Check the host and port, and that the service listens there.` | `address` |
| `timed_out` | `check.TimedOut` | `TRANSIENT` | `{address} didn't answer in time.` | `Check that firewalls, security groups and allowlists let the runner reach it.` | `address` |
| `tls_failed` | `check.TLSFailed` | `TRANSIENT` | `The TLS handshake with {address} failed.` | `Check the certificate the upstream presents, and the TLS settings in the config.` | `address` |
| `unreachable` | `check.Unreachable` | `TRANSIENT` | `Can't reach {address}.` | `Check the address in the config, and the network path from the runner.` | `address` |
| `credentials_rejected` | `check.CredentialsRejected` | `AUTH_INVALID` | `The upstream rejected the credentials.` | `Check the credentials in the config and the secrets they name.` | None |
| `credentials_expired` | `check.CredentialsExpired` | `AUTH_EXPIRED` | `The credentials have expired.` | `Renew the credentials and update their secret.` | None |
| `permission_denied` | `check.PermissionDenied` | `PERMISSION_DENIED` | `The upstream denied access.` | `Grant the connector's identity the access this check names.` | None |
| `privilege_missing` | `check.PrivilegeMissing` | `PERMISSION_DENIED` | `{principal} has no {privilege} on {object}.` | `Grant {privilege} on {object} to {principal}.` | `principal`, `privilege`, `object` |
| `not_found` | `check.NotFound` | `CONFIG_INVALID` | `{resource} doesn't exist.` | `Check the name in the config, or create it.` | `resource` |
| `config_invalid` | `check.ConfigInvalid` | `CONFIG_INVALID` | `The config doesn't work with the upstream.` | `Correct the config.` | None |
| `unsupported_version` | `check.UnsupportedVersion` | `CONFIG_INVALID` | `The upstream's version is {found}, and the connector needs {required}.` | None | `found`, `required` |
| `setting_mismatch` | `check.SettingMismatch` | `CONFIG_INVALID` | `{setting} is {found}, and the connector needs {required}.` | `Set {setting} to {required}.` | `setting`, `found`, `required` |
| `rate_limited` | `check.RateLimited` | `RATE_LIMITED` | `The upstream is limiting requests.` | `Try again later, or raise the limit.` | None |
| `quota_exceeded` | `check.QuotaExceeded` | `RESOURCE_EXHAUSTED` | `A quota of the upstream is used up.` | `Free capacity, or raise the quota.` | None |
| `unavailable` | `check.Unavailable` | `TRANSIENT` | `The upstream can't serve requests now.` | `Try again later.` | None |
| `unknown` | `check.Unknown` | `INTERNAL` | `The check failed.` | None | None |
| `certificate_expiring` | `netcheck.CertificateExpiring` | `TRANSIENT` | `The certificate of {address} expires on {expires}.` | `Renew the certificate.` | `address`, `expires` |

`check.Classify` can find a network error without an address. It then gives the reason a message without a placeholder, such as "The upstream refused the connection.".

## What the platform does with the answer

`sdk.Serve` turns the report into the RPC's answer. These rules apply to every connector, and the SDK follows them for you:

- **The answer's error.** If an item failed, the response's `error` is the first failed item's. Its code is the item's code, and its message is `title: message: detail`, with the placeholders filled in. A host from before items reads only this error.
- **An error without a failed item.** The response's `error` is also set when the checks could not finish, because they ran out of time or were cancelled. The items they did not reach or finish are then skipped. It is also set, with no items, when the connector could not run its checks at all, such as for a list that `check.Validate` refuses. A response with an `error` has failed either way.
- **Failing closed.** A host counts an item whose status is not passed, warning or skipped as failed. A failed item fails the answer even if the response has no `error`.
- **Failures in the answer.** A failed check is an answer, not an RPC error. A connector that answers a preflight RPC with `Unimplemented` has no preflight: no items and no failure.
- **The deadline.** If the RPC has a deadline, the SDK ends the run earlier: by a quarter of the time left, and by one second at most. It then answers in time with the items that finished, and the rest as [Time limits](#time-limits) describes.
- **Secrets.** On the `EgressProxy` and `EgressRaw` tiers, the SDK puts the values of the config's `x-secret` fields in the config. It then replaces each such value, and its URL-escaped forms, with `[redacted]` in every item's text, params, detail and error. On the `EgressHost` tier, the connector gets only the secrets' names.

The SDK removes only the values it resolved, as written. It cannot find a secret that your connector changed, such as one encoded or hashed, or one it got another way. Do not put a secret, or anything made from one, in a param, a template or a detail.

## Connectors in other languages

A connector in another language serves the same RPCs and answers with `CheckItem` messages, defined in [`check.proto`](../../proto/openetl/connector/v1/check.proto). `CheckResponse` is in [`connector.proto`](../../proto/openetl/connector/v1/connector.proto), and the preflight messages are in [`source.proto`](../../proto/openetl/connector/v1/source.proto) and [`destination.proto`](../../proto/openetl/connector/v1/destination.proto). Their comments are the contract.

| Field | What it holds |
|---|---|
| `name` | Unique in the response and the same between releases: `^[a-z0-9]+([._-][a-z0-9]+)*$`. |
| `title` | What was checked, for people. A template. |
| `status` | `PASSED`, `WARNING`, `FAILED` or `SKIPPED`. |
| `message` | What it found when passed, what failed when failed or warning, and why it was not checked when skipped. A template. |
| `error` | Required when failed, optional when warning, and unset otherwise. Its message is the item's message with the placeholders filled in, without the detail. |
| `fix` | For a failure or a warning: what to do about it. A template. |
| `field` | The config field it concerns, as a dotted path. Empty for none. |
| `kind` | What it verifies. `UNSPECIFIED` counts as `CUSTOM`. |
| `description` | What it verifies, and why the connector needs it. A template. Optional. |
| `reason` | For a failure or a warning: a standard reason's name or your own, lowercase words joined by `_`. Empty counts as `unknown`. |
| `params` | Values for the placeholders. |
| `detail` | For a failure or a warning: the cause's text. Not a template. |
| `duration` | How long the check of the item ran. Unset if it did not run. |

Follow these rules:

- A placeholder is `{name}`, with a name that matches `[a-z][a-z0-9_]*`, filled from `params`. A placeholder without a param stays as written. Text without placeholders is plain text.
- Send the items in the order they ran.
- When an item failed, set the response's `error` to the first failed item's code. Set its message to `title: message`, with the placeholders filled in, then `: ` and the detail if there is one.
- If the checks ran out of time or were cancelled, mark the items they did not reach or finish `SKIPPED`. Set the response's `error` even if no item failed.
- If the connector could not run its checks at all, send no items and set the response's `error`.
- Leave the response's `error` unset in every other case.
- Report a failed check in the response, not as an RPC error.
- Answer before the RPC's deadline, with the items that finished.
- No item text, param or detail holds a secret or record contents.

## Test your checks

### In a unit test

`check.Validate` needs no database: building a list does no I/O. Test that each list your connector builds is valid. Here `testConfig` gives the test's config:

```go
func TestListsAreValid(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	if err := check.Validate(src.Checks()); err != nil {
		t.Error(err)
	}
	req := source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "public.orders"}, {Name: "public.refunds"}}}
	if err := check.Validate(src.Preflight(req)); err != nil {
		t.Error(err)
	}
}
```

Such a test catches names built from data that collide. For example, with the tables `public.orders` and `Public.Orders` in the config, `Validate` returns `check 6: the name "table.public-orders.read" is repeated`.

`check.Run` runs a list as the SDK does. Run it against a test database, and check the report:

```go
func TestPreflight(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	req := source.PreflightRequest{Streams: []source.ConfiguredStream{{Name: "public.orders"}}}
	r := check.Run(t.Context(), src.Preflight(req))
	for _, it := range r.Items {
		t.Logf("%s %s", it.Status, it.Name)
	}
	if !r.OK() {
		t.Fatal(r.Err())
	}
}
```

To test one check's `Run`, give it a zero `check.Probe`, and classify what it returns:

```go
func TestSlotMissing(t *testing.T) {
	src := &Source{cfg: testConfig(t)}
	c := src.slotCheck(src.kit())
	err := c.Run(t.Context(), &check.Probe{})
	if f := check.Classify(err); f == nil || f.Reason != "replication_slot_missing" {
		t.Fatalf("got %v, want replication_slot_missing", err)
	}
}
```

A zero `Probe` never runs its cleanups, so a kit's handle stays open until the test ends.

### With the conformance kit

The [conformance kit](./index.md#test-it-with-the-conformance-kit) checks every answer to `Check` and to a preflight through the protocol, in any language. Its `check` and `preflight` checks fail if, with the suite's good config, an item fails or the response has an error. They log each warning. A connector that answers a preflight with `Unimplemented` skips the `preflight` check. A connector that answers with no items passes it.

The `errors` check runs `Check` with the suite's bad credentials, which must fail with `AUTH_INVALID`. It also gives `Check` 5 seconds with an upstream that does not answer, which must fail with a code that can be retried. `Check` fails the conformance check if it runs out of time as an RPC, or answers more than 1 second late. Give the suite these configs with `BadCredentials` and `Unreachable`, or with `openetl connector test --bad-credentials` and `--unreachable`.

In each of these answers, the kit fails:

- An item without a name, with a name outside the grammar, or with the name of another item.
- An item without a title.
- A kind or a status that the protocol does not have, or no status.
- A reason that is not lowercase words joined by `_`.
- A failed item without an error, or a passed or skipped item with one.
- An error without a code, or whose message is not the item's message with its placeholders filled in.
- A title, description, message or fix with what looks like a placeholder but is not one, such as `{Host}`.
- A field that is not a property of the config schema, by its dotted path.
- A response with an error, no failed item, and no skipped item, when it has items. A response with a failed item but no error.
- A response's error whose code is not the first failed item's, or whose message does not start with that item's title.
- Any text, param name or value, detail or error that holds the value of a secret in the suite's config. The kit looks at each template as written and filled in.

It flags, and does not fail, a placeholder without a param of its name.
