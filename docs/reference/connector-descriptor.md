---
title: Connector descriptor
description: Every field of a Connector document, which describes one version of a connector package.
---

A connector package's version: its metadata, and each role's runtime and spec. `openetl connector package` writes it from openetl-connector.yaml; it isn't written by hand.

`openetl connector apply` applies a descriptor to the org, from a file or by a package version's reference, and runners fetch each role's executable or image from what it names. `openetl connector schema descriptor` prints its JSON Schema.

This page is generated from [`internal/spec/connector.schema.json`](../../internal/spec/connector.schema.json). A field that isn't listed is refused, unless its section says other fields are allowed.

## Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `apiVersion` | string | Yes | Is always `openetl.io/v1alpha2`. |
| `kind` | string | Yes | Is always `Connector`. |
| `metadata` | [object](#metadata) | Yes | Which package and version this is, and what the console shows about it. |
| `spec` | [object](#spec) | Yes | Each role's runtime and spec. |

## `metadata`

Which package and version this is, and what the console shows about it.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | The package: owner/name, or name for the official owner, openetl. Each part is lowercase letters, digits and hyphens, at most 63 characters. Matches `^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?/)?[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`. |
| `version` | string | Yes | Semantic version, without build metadata (`+…`): versions that differ only by it compare as equal. Matches `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?$`. |
| `title` | string | No | A display name, such as PostgreSQL. None: the name. Matches `^[^\r\n]*$`. Has at most 100 characters. |
| `description` | string | No | One line. Matches `^[^\r\n]*$`. Has at most 500 characters. |
| `license` | string | No | An SPDX expression, such as Apache-2.0. Matches `^[A-Za-z0-9.+:()\- ]{1,100}$`. |
| `repository` | [`https`](#https) | Yes | The source repository. The package's name stays with it. |
| `docs` | [`https`](#https) | No | The package's documentation, which the console's connector catalog links to. |
| `logo` | [`https`](#https) or [`ociBlob`](#ociblob) | No | An SVG or PNG image, which the UI shows. |
| `author` | string | No | Who makes the package, which the catalog shows. Has at most 200 characters. |
| `tags` | array of string | No | Words the catalog shows with the package, such as database. Has at most 20 items. Each item matches `^[a-z0-9][a-z0-9-]{0,31}$`. |
| `release` | [object](#metadatarelease) | No | The release the version was built from. |

## `metadata.release`

The release the version was built from.

| Field | Type | Required | Description |
|---|---|---|---|
| `tag` | string | No | The git tag the version was built from. Matches `^\S*$`. Has at most 255 characters. |
| `url` | [`https`](#https) | No | The release's page. |

## `spec`

Each role's runtime and spec.

| Field | Type | Required | Description |
|---|---|---|---|
| `roles` | [object](#specroles) | Yes | What the package provides: a source, a destination, or both. |

## `spec.roles`

What the package provides: a source, a destination, or both.

| Field | Type | Required | Description |
|---|---|---|---|
| `source` | [object](#specrolesrole) | No | The package's source. Can't have `capabilities`. |
| `destination` | [object](#specrolesrole) | No | The package's destination. Needs `capabilities`. Can't have `runtime.manifest`. Can't have `acknowledges`, `splits`, `resplit` or `retention`. |

- Has at least one of its fields.

## `spec.roles.ROLE`

`ROLE` is `source` or `destination`.

What a package provides as a source or a destination, and how it runs.

| Field | Type | Required | Description |
|---|---|---|---|
| `requires` | [object](#specrolesrolerequires) | No | What the role needs from the platform. |
| `runtime` | [object](#specrolesroleruntime) | Yes | At least one of binaries and an image, or a manifest alone. |
| `resources` | [object](#specrolesroleresources) | No | A container launcher's requests and limits, and the job size the role wants. |
| `state` | [object](#specrolesrolestate) | No | The version of the checkpoint state it writes, and the oldest it reads (at most version). |
| `egress` | [object](#specrolesroleegress) | No | What the connector means to reach on the network. OpenETL doesn't enforce it yet: connectors are trusted code. |
| `setup` | boolean | No | The connector serves `Setup` and `Teardown`. |
| `acknowledges` | boolean | No | A source's: it acknowledges upstream once a checkpoint commits. |
| `splits` | boolean | No | A source's: it divides streams into splits. |
| `resplit` | boolean | No | A source's: it splits a running split's remainder. Needs splits. |
| `retention` | boolean | No | A source's: it says, without reading, how much of what it keeps for a pipeline a checkpoint's positions have used. |
| `capabilities` | [object](#specrolesrolecapabilities) | No | A destination's. |
| `config` | object | No | The config's JSON Schema, as Spec returns it. |

- `resplit` needs `splits`.
- If it has `runtime.manifest`: A manifest's config and egress are in the manifest, and it speaks no protocol. Can't have `runtime.binaries` or `runtime.image`. Can't have `requires.protocol`. Can't have `egress`, `config` or `capabilities`.
- Otherwise: Needs `requires`, `egress` and `config`. Needs `requires.protocol`.

## `spec.roles.ROLE.requires`

What the role needs from the platform.

| Field | Type | Required | Description |
|---|---|---|---|
| `protocol` | string | No | The connector protocol it speaks; none for a manifest. Is always `v1`. |
| `features` | array of string | No | Additions to the protocol after v1 that it can't run without. Each item matches `^[a-z0-9][a-z0-9.-]{0,62}$`. |
| `platform` | string | No | The oldest openetl release it runs on. Matches `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$`. |

## `spec.roles.ROLE.runtime`

At least one of binaries and an image, or a manifest alone.

| Field | Type | Required | Description |
|---|---|---|---|
| `binaries` | map of [`platform`](#platform) to [object](#specrolesroleruntimebinariesplatform) | No | One executable per platform, downloaded over HTTP and pinned by digest. |
| `image` | [object](#specrolesroleruntimeimage) | No | An OCI image index that serves the launch contract under the image contract. |
| `manifest` | object | No | A DeclarativeSource's spec: the role's whole runtime, run in the platform. Sources only. |

- Has at least one of these: `binaries` with at least one entry; `image`; or `manifest`.

## `spec.roles.ROLE.runtime.binaries.PLATFORM`

One platform's executable.

| Field | Type | Required | Description |
|---|---|---|---|
| `url` | string or [`ociBlob`](#ociblob) | Yes | Where the executable is downloaded from: an http or https URL, or a blob in an OCI registry. Either a string that matches `^https?://[^/?#\s]+([/?#]\S*)?$` and has at most 2048 characters, or [`ociBlob`](#ociblob). |
| `digest` | [`digest`](#digest) | No | The executable's digest. The API fills in a missing one when the version is applied. |
| `attestation` | [`https`](#https) or [`ociBlob`](#ociblob) | No | The executable's provenance attestation. |

## `spec.roles.ROLE.runtime.image`

An OCI image index that serves the launch contract under the image contract.

| Field | Type | Required | Description |
|---|---|---|---|
| `ref` | string | Yes | A repository, without a tag or digest. Matches `^[a-z0-9]+([._-][a-z0-9]+)*(\.[a-z0-9-]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+$`. |
| `digest` | [`digest`](#digest) | Yes | The index's digest. |
| `platforms` | array of [`platform`](#platform) | Yes | The platforms the index has an image for. Has at least one item. |
| `command` | array of string | No | The connector's argv, where it isn't the entrypoint. Each item isn't empty. |
| `attestation` | [`https`](#https) or [`ociBlob`](#ociblob) | No | The image's provenance attestation. |

## `spec.roles.ROLE.resources`

A container launcher's requests and limits, and the job size the role wants.

| Field | Type | Required | Description |
|---|---|---|---|
| `cpu` | string | No | CPU, as a Kubernetes quantity, such as 500m or 2. Matches `^([0-9]+m\|[0-9]+(\.[0-9]{1,3})?)$`. |
| `memory` | [`bytes`](#bytes) | No | Memory. |
| `disk` | [`bytes`](#bytes) | No | The size of TMPDIR. |
| `hint` | string | No | The job size a pipeline of this role starts at when it's sized automatically. Is one of `xs`, `s`, `m`, `l` or `xl`. |

## `spec.roles.ROLE.state`

The version of the checkpoint state it writes, and the oldest it reads (at most version).

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | integer | Yes | The version of the state it writes. Is from 1 to 4294967295. |
| `reads` | integer | Yes | The oldest version of the state it can resume from. Is from 1 to 4294967295. |

## `spec.roles.ROLE.egress`

What the connector means to reach on the network. OpenETL doesn't enforce it yet: connectors are trusted code.

| Field | Type | Required | Description |
|---|---|---|---|
| `tier` | string | Yes | `host`: only through the host's services. `proxy`: HTTPS through a forward proxy, to the hosts in `allow`, for vendor SDKs. `raw`: sockets, to the host:port pairs in `allow`, for database drivers. Is one of `host`, `proxy` or `raw`. |
| `allow` | array of string | No | Hosts on the proxy tier, host:port pairs on raw. |

## `spec.roles.ROLE.capabilities`

A destination's.

| Field | Type | Required | Description |
|---|---|---|---|
| `class` | string | Yes | How the destination makes an epoch visible. `stage_and_apply`: it stages in `PreCommit` and applies in `Commit`. `transactional_stream`: it flushes into a transaction in `PreCommit` and commits it in `Commit`. `deliver_and_ack`: it delivers during the epoch, and `PreCommit` returns once everything is acknowledged. Is one of `stage_and_apply`, `transactional_stream` or `deliver_and_ack`. |
| `scopes` | array of string | No | The commit scopes a pipeline may choose in `delivery.commit.scope`: `pipeline`, where every stream's epoch becomes visible together, and `stream`, where each stream's does on its own. Each item is one of `pipeline` or `stream`. |
| `dedupeAtApply` | boolean | No | The destination can skip append and overwrite rows whose `_openetl_id` is already in the target. |
| `defaultCommitInterval` | [`duration`](#duration) | No | The commit interval of a pipeline whose `delivery.commit.interval` sets none. |
| `minCommitInterval` | [`duration`](#duration) | No | The shortest `delivery.commit.interval` a pipeline may set. |
| `maxCommitInterval` | [`duration`](#duration) | No | The longest `delivery.commit.interval` a pipeline may set. None: no maximum. |
| `nanosecondTimestamps` | boolean | No | Timestamps keep nanoseconds; otherwise only microseconds. |
| `maxIdentifierLength` | integer | No | The longest table or column name, in bytes; 0 for no limit. Is from 0 to 4294967295. |
| `writeModes` | array of string | No | None: append only. Each item is one of `append`, `overwrite` or `merge`. |
| `delivery` | string | No | How epochs reach it: streamed as batches, or staged as Parquet files the platform hands over. None: streamed. Is one of `streamed` or `staged`. |
| `maxWriterLanes` | integer | No | The most writer lanes an attempt may open, writing concurrently. None: one. Is from 0 to 4294967295. |
| `holds` | [object](#specrolesrolecapabilitiesholds) | No | What the destination does with held columns and cells. None: it takes no holds. |

## `spec.roles.ROLE.capabilities.holds`

What the destination does with held columns and cells. None: it takes no holds.

| Field | Type | Required | Description |
|---|---|---|---|
| `byKey` | boolean | No | Patches a merge stream's rows by their key. |
| `byId` | boolean | No | Patches an append stream's rows by their `_openetl_id`. |
| `last` | boolean | No | A held cell can keep the target's value, in a merge stream. |
| `guarded` | boolean | No | A patch fills a cell only where the target still holds the version it comes from. |

## Types

Fields above refer to these types by name.

### `https`

An https URL. A string. Matches `^https://[^/?#\s]+([/?#]\S*)?$`. Has at most 2048 characters.

### `ociBlob`

A blob in an OCI registry, by its digest. A string. Matches `^oci://[^/@\s]+/[a-z0-9]+([._/-][a-z0-9]+)*@sha256:[0-9a-f]{64}$`. Has at most 2048 characters.

### `platform`

os/arch, such as linux/amd64. A string. Matches `^[a-z0-9]+/[a-z0-9]+$`.

### `digest`

A SHA-256 digest: sha256: and 64 hex digits. A string. Matches `^sha256:[0-9a-f]{64}$`.

### `bytes`

A Kubernetes quantity of bytes, such as 512Mi. A string. Matches `^[0-9]+(Ki|Mi|Gi|Ti|k|M|G|T)?$`.

### `duration`

A Go duration, such as 90s or 15m. A string. Matches `^-?([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|ms|s|m|h)(([0-9]+(\.[0-9]*)?|\.[0-9]+)(ns|us|µs|ms|s|m|h))*$|^0$`.
