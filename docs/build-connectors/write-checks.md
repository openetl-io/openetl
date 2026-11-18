---
title: Write custom checks
description: Define failures, placeholders, dependencies, time limits and cleanup for connector checks.
---

Use a custom check when the [SDK toolkits](./check-toolkits.md) do not cover a connector requirement.

## Write your own check

Write a check of your own for what the toolkits do not cover. A check is a `check.Check` with a `Run`. Its failures say what went wrong in words people can act on.

### Reasons and failures

A `check.Reason` is a kind of failure: a name, an error code, a message and a fix. The message and the fix are [templates](#placeholders). Use one of the [standard reasons](./check-results.md#the-standard-reasons), or declare your own:

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

`sqlcheck`'s and `adbccheck`'s `Kit.Check` make a check of your own that runs with the kit's handle or connection. It needs `db.connect`. Return `k.Failure(err)` for a driver's error, so the kit classifies it. [A complete example](./check-example.md#a-complete-example) has one, `replication.slot`.

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

A warning keeps its failure's reason, code, fix and detail. An error that no reason fits, such as one from `errors.New`, gets the message "The check raised a warning.", and its text becomes the detail. To give the warning a message of its own, return a failure of a `check.Reason`. For example, with a reason `replicaLag` that you declare:

```go
return check.Warn(replicaLag.Wrap(err))
```

### Time limits

A check's `Timeout` limits its `Run`, and the run's context limits every check. The context that `Run` gets ends at the earlier of the two. Stop when it ends:

- A check that returns nil after its deadline is not a pass.
- A check that has not returned 100 milliseconds after its deadline is left running, and the run moves on.

Either check fails as `timed_out`, with the message "The check didn't finish in time." and no fix, since the check, not the network, was slow. A timeout error that the check returns itself keeps `timed_out`'s fix. An `Advisory` check that runs out of time warns and does not fail.

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
