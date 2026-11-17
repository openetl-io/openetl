---
title: Use check toolkits
description: Select network, HTTP, SQL and ADBC checks from the Go SDK.
---

The SDK provides checks for common connection tasks. First, [declare your checks](./declare-checks.md).

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

> [!CAUTION]
> `Readable` inserts `relation` into SQL as written. Use a trusted relation name and quote it for your database.

| Method | Name | Kind | Params | Passes when |
|---|---|---|---|---|
| `Connect()` | `db.connect` | Connection | None | The database answers a ping. |
| `Query(name, title, query, args...)` | Yours | Permission | None | The query runs and its first row, if any, reads. Some drivers report an error, such as a missing privilege, only on the first row. |
| `True(name, title, denied, missing, query, args...)` | Yours | Permission | None | The query's first value is true: a boolean, its text such as `t`, or the integer 1. |
| `Readable(name, relation)` | Yours | Permission | `relation`, the relation; `resource`, "The table " and the relation | `SELECT * FROM relation WHERE 1=0` runs. Its title is `Read {relation}`. |
| `Value(name, title, verify, query, args...)` | Yours | Compatibility | `found`, set to the value | `verify` returns nil for the query's first value, as text. NULL is the empty string. |
| `Check(name, kind, title, run)` | Yours | Yours | None | Your `run` returns nil. See [Reuse a toolkit's connection](./write-checks.md#reuse-a-toolkits-connection). |

Every check but `Connect` needs `db.connect`, so put `Connect` first in the list.

In `True`, another value fails with `denied`, and no row or NULL fails with `missing`. A nil `denied` is `check.PermissionDenied`'s failure. A nil `missing` is `check.NotFound`'s, with the message "The query found no row.".

In `Value`, `verify` returns the check's error. Return `check.UnsupportedVersion` or `check.SettingMismatch` failures there: the kit sets their `{found}` to the value. No row fails as `not_found`.

The kit classifies a driver's error with `Kit.Failure`, as [Classify driver errors](./write-checks.md#classify-driver-errors) explains.

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
