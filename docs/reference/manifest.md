---
title: Declarative source manifest
description: Every field of a DeclarativeSource manifest.
---

A source for an HTTP API, as data. Expressions are CEL. Templates are text with {expression} holes; {{ and }} are literal braces. A template that is one hole keeps the hole's value: null leaves a parameter or header out, and a list repeats a parameter.

A manifest is a whole source for an HTTP API, which OpenETL runs itself, with no executable. Apply one with `openetl connector apply -f FILE`, or name one as a role's `manifest` in [`openetl-connector.yaml`](./connector-file.md). `openetl connector schema manifest` prints its JSON Schema.

This page is generated from [`internal/declarative/manifest.schema.json`](../../internal/declarative/manifest.schema.json). A field that isn't listed is refused, unless its section says other fields are allowed.

## Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `apiVersion` | string | Yes | Is always `openetl.io/v1alpha1`. |
| `kind` | string | Yes | Is always `DeclarativeSource`. |
| `metadata` | [object](#metadata) | Yes | Which connector and version this is, and what the console shows about it. |
| `spec` | [object](#spec) | Yes | The source: its config, how it calls the API, and its streams. |

## `metadata`

Which connector and version this is, and what the console shows about it.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | The connector's package: owner/name, or name for the official owner, openetl. Each part is lowercase letters, digits and hyphens. Matches `^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?/)?[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`. |
| `version` | string | Yes | Semantic version, without build metadata (`+…`): versions that differ only by it compare as equal. Matches `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$`. |
| `description` | string | No | One line. Has at most 500 characters. |
| `license` | string | No | An SPDX expression, such as Apache-2.0. Has at most 100 characters. |
| `repository` | string | No | An https URL of the source repository. Matches `^https://`. |
| `docs` | string | No | An https URL. Matches `^https://`. |
| `tags` | array of string | No | Words the catalog shows with the connector, such as crm. Has at most 20 items. Each item matches `^[a-z0-9][a-z0-9-]{0,31}$`. |
| `author` | string | No | Who made the connector. Has at most 200 characters. |
| `logo` | string | No | An https URL of the connector's logo, which the UI shows. Matches `^https://`. |
| `release` | [object](#metadatarelease) | No | The git release this version comes from. |

## `metadata.release`

The git release this version comes from.

| Field | Type | Required | Description |
|---|---|---|---|
| `tag` | string | No | The git tag. Has at most 255 characters. |
| `url` | string | No | An https URL, such as the release's page. Matches `^https://`. |

## `spec`

The source: its config, how it calls the API, and its streams.

| Field | Type | Required | Description |
|---|---|---|---|
| `resources` | [object](#specresources) | No | What the source asks of its jobs. |
| `config` | [object](#specconfig) | Yes | The JSON Schema of the connector's config: an object. Top-level string properties with x-secret: true are secrets, which only auth and sign can reference, by name. Expressions see the other properties as config.NAME. |
| `http` | [object](#spechttp) | Yes | How the source calls the API: its base URL, headers, auth, signing, rate limit and error rules. |
| `check` | [object](#speccheck) | No | What Check does: fetch the first page of a stream. Default: the first stream without a parent. |
| `streams` | array of [object](#specstreams) | Yes | The streams the source reads. Has at least one item. |

## `spec.resources`

What the source asks of its jobs.

| Field | Type | Required | Description |
|---|---|---|---|
| `hint` | string | No | The job size a pipeline of this source starts at when it's sized automatically. Is one of `xs`, `s`, `m`, `l` or `xl`. |

## `spec.config`

The JSON Schema of the connector's config: an object. Top-level string properties with x-secret: true are secrets, which only auth and sign can reference, by name. Expressions see the other properties as config.NAME.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Is always `object`. |
| `properties` | object | No | The config's properties, by name, each a JSON Schema. Each name matches `^[A-Za-z_][A-Za-z0-9_]*$`. |

Other fields are allowed.

## `spec.http`

How the source calls the API: its base URL, headers, auth, signing, rate limit and error rules.

| Field | Type | Required | Description |
|---|---|---|---|
| `base_url` | [`template`](#template) | Yes | A template over config. Request paths are appended to it, and next-page URLs must have its scheme, host and port. |
| `headers` | [`templates`](#templates) | No | Headers for every request: templates, as in a stream's request. |
| `auth` | [object](#spechttpauth) | No | How requests authenticate. The host adds the credential when a request leaves, and only to the base URL's scheme, host and port. |
| `sign` | [object](#spechttpsign) | No | Signs every request with a key the host holds. hmac puts a MAC of message in a header or a query parameter; jwt sends a JWT as the Authorization header. |
| `rate_limit` | [object](#spechttprate_limit) | No | How fast the source sends requests. |
| `errors` | [`errors`](#errors) | No | Error rules for every stream, after the stream's own. |

## `spec.http.auth`

How requests authenticate. The host adds the credential when a request leaves, and only to the base URL's scheme, host and port.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | How requests authenticate. Is one of `bearer`, `basic`, `api_key`, `oauth2` or `aws_sigv4`. |

### `spec.http.auth` when `type` is `bearer`

| Field | Type | Required | Description |
|---|---|---|---|
| `token` | [`secret`](#secret) | Yes | The bearer token. |

### `spec.http.auth` when `type` is `basic`

| Field | Type | Required | Description |
|---|---|---|---|
| `username` | [`template`](#template) | Yes | A template over config. |
| `password` | [`secret`](#secret) | Yes | The password. |

### `spec.http.auth` when `type` is `api_key`

| Field | Type | Required | Description |
|---|---|---|---|
| `key` | [`secret`](#secret) | Yes | The API key. |
| `header` | string | No | The header the key goes in, such as X-API-Key. Isn't empty. |
| `query` | string | No | The query parameter the key goes in. Isn't empty. |
| `prefix` | string | No | Put before the key, such as "Token ". |

- Has exactly one of `header` or `query`.

### `spec.http.auth` when `type` is `oauth2`

| Field | Type | Required | Description |
|---|---|---|---|
| `token_url` | [`template`](#template) | Yes | A template over config. |
| `client_id` | [`templateOrSecret`](#templateorsecret) | Yes | The client's ID. |
| `client_secret` | [`secret`](#secret) | Yes | The client's secret. |
| `refresh_token` | [`secret`](#secret) | No | A refresh token, for the refresh token grant. Without one, the host uses the client credentials grant. |
| `scopes` | array of string | No | The scopes to ask for. |
| `auth_style` | string | No | Where the client credentials go in token requests. Default: the host tries both. Is one of `header` or `params`. |

### `spec.http.auth` when `type` is `aws_sigv4`

| Field | Type | Required | Description |
|---|---|---|---|
| `access_key_id` | [`templateOrSecret`](#templateorsecret) | Yes | The AWS access key ID. |
| `secret_access_key` | [`secret`](#secret) | Yes | The AWS secret access key. |
| `session_token` | [`secret`](#secret) | No | An AWS session token, for temporary credentials. |
| `region` | [`template`](#template) | Yes | A template over config. |
| `service` | string | Yes | The AWS service's signing name, such as execute-api. Isn't empty. |

## `spec.http.sign`

Signs every request with a key the host holds. hmac puts a MAC of message in a header or a query parameter; jwt sends a JWT as the Authorization header.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | How requests are signed. Is one of `hmac` or `jwt`. |
| `key` | [`secret`](#secret) | Yes | The signing key. |

### `spec.http.sign` when `type` is `hmac`

| Field | Type | Required | Description |
|---|---|---|---|
| `hash` | string | Yes | The HMAC's hash function. Is one of `sha1`, `sha256` or `sha512`. |
| `message` | [`expression`](#expression) | Yes | An expression over request (method, url, path, query, headers, body), config and now, returning a string or bytes. |
| `encoding` | string | No | Default: hex. Is one of `hex` or `base64`. |
| `header` | string | No | The header the MAC goes in. Isn't empty. |
| `query` | string | No | The query parameter the MAC goes in. Isn't empty. |
| `prefix` | string | No | Put before the MAC. |

- Has exactly one of `header` or `query`.

### `spec.http.sign` when `type` is `jwt`

| Field | Type | Required | Description |
|---|---|---|---|
| `algorithm` | string | Yes | The JWT's signing algorithm. Is one of `HS256`, `RS256` or `ES256`. |
| `claims` | [`expression`](#expression) | Yes | An expression over config and now, returning a map. |
| `jose_header` | [`expression`](#expression) | No | An expression returning extra header fields, such as kid. The host sets alg and typ. |
| `prefix` | string | No | Default: "Bearer ". |

## `spec.http.rate_limit`

How fast the source sends requests.

| Field | Type | Required | Description |
|---|---|---|---|
| `rps` | number | Yes | Requests per second at most. The rate adapts below it when the API throttles. Is more than 0. |

## `spec.check`

What Check does: fetch the first page of a stream. Default: the first stream without a parent.

| Field | Type | Required | Description |
|---|---|---|---|
| `stream` | string | Yes | The stream whose first page Check fetches. |

## `spec.streams[]`

A stream the source reads: how to request it, page through it, and find its records.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | The stream's name. Isn't empty. |
| `primary_key` | array of string | No | The top-level properties that make up a record's primary key. |
| `request` | [object](#specstreamsrequest) | Yes | The request for each page. Templates over config, state, page, parent, partition and now. |
| `records` | [`expression`](#expression) | No | An expression over response, returning the page's records, each an object. Default: response.body. |
| `paginator` | [object](#specstreamspaginator) | No | How to get the next page. A resume point follows every page. Without a paginator, a stream is one page. |
| `incremental` | [object](#specstreamsincremental) | No | Read from a cursor floor onwards. Requests see the floor, less the lookback, as state.cursor, which is absent on the first sync. The API must return every record whose cursor is at or after it. |
| `parent` | [object](#specstreamsparent) | No | Read the stream once per record of another stream, which the request sees as parent. The parent must have a primary key and no parent of its own. |
| `partitions` | [object](#specstreamspartitions) | No | Read the stream in splits, concurrently where the job has room, each with its own place and floor. Requests see each one as partition. Not for a stream with a parent. |
| `errors` | [`errors`](#errors) | No | Error rules for this stream, tried before the API's. |
| `schema` | [object](#specstreamsschema) | No | JSON Schema for some or all of the record's top-level properties, which declares their types. The others are inferred. |

## `spec.streams[].request`

The request for each page. Templates over config, state, page, parent, partition and now.

| Field | Type | Required | Description |
|---|---|---|---|
| `method` | string | No | Default: GET. Is one of `GET` or `POST`. |
| `path` | [`template`](#template) | Yes | Appended to the base URL. Holes are escaped as path segments. |
| `params` | [`templates`](#templates) | No | Query parameters. |
| `headers` | [`templates`](#templates) | No | Headers, besides the API's. |
| `body` | [`expression`](#expression) | No | An expression whose value is sent as JSON. |
| `idempotent` | boolean | No | Lets the host retry a POST, such as a search that changes nothing. |

## `spec.streams[].paginator`

How to get the next page. A resume point follows every page. Without a paginator, a stream is one page.

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | How the next page is found. Is one of `cursor`, `next_url`, `link`, `offset` or `page`. |
| `stop` | [`expression`](#expression) | No | An expression over response and records; true ends pagination. |

### `spec.streams[].paginator` when `type` is `cursor`

| Field | Type | Required | Description |
|---|---|---|---|
| `next` | [`expression`](#expression) | Yes | An expression over response and records: the next page's token, or null or an empty string at the end. Requests see it as page.token. |
| `param` | string | No | Also send the token as this query parameter. Isn't empty. |

### `spec.streams[].paginator` when `type` is `next_url`

| Field | Type | Required | Description |
|---|---|---|---|
| `next` | [`expression`](#expression) | Yes | An expression over response and records: the next page's URL, absolute or relative to the base URL, or null or an empty string at the end. It's fetched as it is, with no params added. |

### `spec.streams[].paginator` when `type` is `link`

The next page is the Link header's rel="next" URL, fetched as it is.

### `spec.streams[].paginator` when `type` is `offset`

| Field | Type | Required | Description |
|---|---|---|---|
| `param` | string | No | Default: offset. Isn't empty. |
| `start` | integer | No | Default: 0. Is at least 0. |
| `size` | integer | No | Records in a full page. A shorter page is the last. Is at least 1. |
| `size_param` | string | No | Send size as this query parameter. Isn't empty. |

### `spec.streams[].paginator` when `type` is `page`

| Field | Type | Required | Description |
|---|---|---|---|
| `param` | string | No | Default: page. Isn't empty. |
| `start` | integer | No | Default: 1. Is at least 0. |
| `size` | integer | No | Records in a full page. A shorter page is the last. Is at least 1. |
| `size_param` | string | No | Send size as this query parameter. Isn't empty. |

## `spec.streams[].incremental`

Read from a cursor floor onwards. Requests see the floor, less the lookback, as state.cursor, which is absent on the first sync. The API must return every record whose cursor is at or after it.

| Field | Type | Required | Description |
|---|---|---|---|
| `cursor_field` | string | Yes | A top-level property of the records. Isn't empty. |
| `type` | string | Yes | `datetime`: RFC 3339 text, and `state.cursor` is a timestamp. `unix`: seconds since 1970, and `state.cursor` is an int. `integer` and `string`: compared as they are. Is one of `datetime`, `unix`, `integer` or `string`. |
| `lookback` | string | No | Read again this far behind the floor, for late updates. datetime and unix only. A duration such as 90s, 5m or 1h30m. Matches `^([0-9]+(\.[0-9]+)?(ns\|us\|µs\|ms\|s\|m\|h))+$`. |
| `ordered` | boolean | No | The API returns records sorted by the cursor, oldest first, so the floor advances after every page. |

## `spec.streams[].parent`

Read the stream once per record of another stream, which the request sees as parent. The parent must have a primary key and no parent of its own.

| Field | Type | Required | Description |
|---|---|---|---|
| `stream` | string | Yes | The parent stream's name. |

## `spec.streams[].partitions`

Read the stream in splits, concurrently where the job has room, each with its own place and floor. Requests see each one as partition. Not for a stream with a parent.

| Field | Type | Required | Description |
|---|---|---|---|
| `values` | [`expression`](#expression) | No | An expression over config and now, returning a list: a partition per entry. |
| `range` | [object](#specstreamspartitionsrange) | No | Partitions of {start, end}, in RFC 3339, a step at a time. |

## `spec.streams[].partitions.range`

Partitions of {start, end}, in RFC 3339, a step at a time.

| Field | Type | Required | Description |
|---|---|---|---|
| `start` | [`expression`](#expression) | Yes | An expression over config and now, returning a timestamp. |
| `end` | [`expression`](#expression) | No | As start. Default: now. |
| `step` | string | Yes | A duration, such as 24h. |

## `spec.streams[].schema`

JSON Schema for some or all of the record's top-level properties, which declares their types. The others are inferred.

| Field | Type | Required | Description |
|---|---|---|---|
| `properties` | map of names to object | No | The properties' JSON Schemas, by name. |

Other fields are allowed.

## Types

Fields above refer to these types by name.

### `template`

Text with {expression} holes. A string.

### `templates`

Names, each with a template, such as a request's query parameters. A map of names to [`template`](#template).

### `secret`

A config property marked x-secret, by name.

| Field | Type | Required | Description |
|---|---|---|---|
| `secret` | string | Yes | The name of a config property marked x-secret. Isn't empty. |

### `templateOrSecret`

A template, or a secret. Either [`template`](#template) or [`secret`](#secret).

### `expression`

A CEL expression. A string. Isn't empty.

### `errors`

Rules tried in order on every response. The first whose when is true decides; an expression that fails to evaluate doesn't match. A list of objects with these fields.

| Field | Type | Required | Description |
|---|---|---|---|
| `when` | [`expression`](#expression) | Yes | An expression over response. |
| `action` | string | Yes | `fail`: fail with `code` and `message`. `ignore`: treat the response as an empty last page. `expired`: the page token a resumed read started from was rejected, so the stream starts over from its floor. Is one of `fail`, `ignore` or `expired`. |
| `code` | string | No | For fail. Default: how the host classified the response. Is one of `TRANSIENT`, `RATE_LIMITED`, `AUTH_EXPIRED`, `AUTH_INVALID`, `PERMISSION_DENIED`, `CONFIG_INVALID`, `CONTRACT_CHANGED` or `RESOURCE_EXHAUSTED`. |
| `message` | [`template`](#template) | No | For fail: a template over response. |
