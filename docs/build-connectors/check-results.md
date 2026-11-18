---
title: Check reasons and protocol results
description: Look up standard failure reasons and check result behavior across hosts and languages.
---

Use this reference when you implement or interpret connector check results.

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

On a warning, `unknown`'s message is `The check raised a warning.` `check.Classify` can find a network error without an address. It then gives the reason a message without a placeholder, such as "The upstream refused the connection.".

## What the platform does with the answer

`sdk.Serve` turns the report into the RPC's answer. These rules apply to every connector, and the SDK follows them for you:

- **The answer's error.** If an item failed, the response's `error` is the first failed item's. Its code is the item's code, and its message is `title: message: detail`, with the placeholders filled in. A host from before items reads only this error.
- **An error without a failed item.** The response's `error` is also set when the checks could not finish, because they ran out of time or were cancelled. The items they did not reach or finish are then skipped. It is also set, with no items, when the connector could not run its checks at all, such as for a list that `check.Validate` refuses. A response with an `error` has failed either way.
- **Failing closed.** A host counts an item whose status is not passed, warning or skipped as failed. A failed item fails the answer even if the response has no `error`.
- **Failures in the answer.** A failed check is an answer, not an RPC error. A connector that answers a preflight RPC with `Unimplemented` has no preflight: no items and no failure.
- **The deadline.** If the RPC has a deadline, the SDK ends the run earlier: by a quarter of the time left, and by one second at most. It then answers in time with the items that finished, and the rest as [Time limits](./write-checks.md#time-limits) describes.
- **Secrets.** On the `EgressProxy` and `EgressRaw` tiers, the SDK puts the values of the config's `x-secret` fields in the config. It then replaces each such value with `[redacted]` in every item's name, text, reason, field, param names and values, detail and error. It finds the value as written, URL-escaped with uppercase or lowercase hex digits, and escaped as in a Go or JSON string, such as `%q` prints it. On the `EgressHost` tier, the connector gets only the secrets' names.

The SDK removes only the values it resolved, in those forms. It cannot find a secret that your connector changed in another way, such as one encoded in base64 or hashed, or one it got another way. Do not put a secret, or anything made from one, in a param, a template or a detail.

The runner's host then takes the answer, whatever language the connector is written in, and applies these rules:

- **Secrets.** The host replaces the values of the secrets that it gave the connector with `[redacted]`, in the same places and forms as the SDK. These are the values of the config's `x-secret` fields, the token of an AWS role that the runner leased for the connector, and the runner's own AWS secret key and session token when the connector inherits them.
- **Text.** The host makes all text valid UTF-8. It replaces control characters, and bidirectional and other invisible formatting characters, with spaces. A detail keeps its line breaks and tabs.
- **Time.** A connector's check has 1 minute, and its preflight has 2 minutes. The host passes the time as the RPC's deadline. In a preflight, a connector that does not answer in time fails its part with `TRANSIENT`, and a message such as `the source's preflight didn't finish within 2m0s`. A check of one source or destination that does not answer in time could not run: people get an error, not a result.
- **Limits.** The host cuts each text that is longer than its limit, and ends it with `…`. It keeps the first 50 params of an item, in the order of their names. The limits are in bytes:

  | What | Limit |
  |---|---|
  | Items in one answer | 2,000 |
  | `name` | 200 bytes |
  | `title`, `field` | 300 bytes each |
  | `reason` | 100 bytes |
  | `message`, `fix`, `description`, and an item's `error` message | 2,000 bytes each |
  | `detail`, and the response's `error` message | 4,000 bytes each |
  | Params of one item | 50, each name at most 64 bytes and each value at most 1,000 bytes |
  | The text of all items in one answer, params included | 1 MiB |

  An answer with more than 2,000 items, or more than 1 MiB of text, fails with `INTERNAL`, and the host keeps the items that fit. Its message is one of these, with the numbers filled in:

  - `the connector answered with N items, and an answer has at most 2000: the first 2000 are kept`
  - `the connector answered with N bytes of items, and an answer has at most 1048576: the first K items are kept`
  - `the connector answered with N items and more than 1048576 bytes of them, and an answer has at most 2000 items and 1048576 bytes: the first K are kept`

  The result of a whole operation has at most 1.5 MiB, as the runner encodes it. A larger result fails the operation, with a message such as `the result of the preflight is N bytes, and a result has at most 1572864: the connectors answered with too much`.

Design your checks for these limits. Report a bounded number of items, such as one item for each stream, not one for each column or file. `openetl connector check` does not apply the host's limits. It reads the answer as a host does, and fails closed: an item whose status is not passed, warning or skipped shows as failed. A failed item without an error code gets `INTERNAL`, and one without a reason gets `unknown`.

OpenETL keeps each item's templates and params apart, from the connector to the API. The CLI and the console fill the placeholders when they show an item. Under a failed or warning item, they show its message, detail, fix, field, code and reason. The console also shows each item's kind, and its description on request. [Pipeline CLI commands](../reference/pipeline-cli.md#text-output) and [Use the console](../use/checks.md#read-a-checks-items) show what people see.

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
