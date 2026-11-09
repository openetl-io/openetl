---
title: openetl-connector.yaml
description: Every field of the file a connector package is built from.
---

A connector package, as its author writes it. `openetl connector package` builds it and writes its descriptor.

The file is `openetl-connector.yaml` at the root of the package's directory. `openetl connector validate` checks it without building, and the descriptor `openetl connector package` writes for each version is a [Connector descriptor](./connector-descriptor.md). `openetl connector schema file` prints its JSON Schema, for an editor to check the file as you write it. [Build a connector](../build-connectors/index.md#package-and-publish) shows one.

This page is generated from [`internal/connectorpkg/openetl-connector.schema.json`](../../internal/connectorpkg/openetl-connector.schema.json). A field that isn't listed is refused, unless its section says other fields are allowed.

## Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | The package's name: owner/name, or name for the official owner, openetl. Each part is lowercase letters, digits and hyphens, at most 63 characters. Matches `^([a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?/)?[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`. |
| `title` | string | No | A display name, such as PostgreSQL. None: the name. Matches `^[^\n\r]*$`. Has at most 100 characters. |
| `description` | string | No | One line. Matches `^[^\n\r]*$`. Has at most 500 characters. |
| `license` | string | No | An SPDX expression, such as Apache-2.0. Matches `^[A-Za-z0-9.+:()\- ]{1,100}$`. |
| `repository` | [`https`](#https) | Yes | The package's source repository. Its name stays with it. |
| `docs` | [`https`](#https) | No | The package's documentation, which the console's connector catalog links to. |
| `logo` | [`https`](#https) or string | No | An SVG or PNG image of at most 256 KiB, which the UI shows: a file beside this one, such as ./logo.svg, which is packaged with each version, or an https URL, which openetl connector publish copies into the registry. Either [`https`](#https), or a string that matches `^[^:/][^:]*\.(svg\|png)$`. |
| `author` | string | No | Who makes the package, which the catalog shows. Has at most 200 characters. |
| `tags` | array of string | No | Words the catalog shows with the package, such as database. Has at most 20 items. Each item matches `^[a-z0-9][a-z0-9-]{0,31}$`. |
| `platforms` | array of string | No | The os/arch pairs to build. Default: linux/amd64, linux/arm64, darwin/amd64 and darwin/arm64. Has at least one item. Has no item twice. Each item matches `^[a-z0-9]+/[a-z0-9]+$`. |
| `roles` | [object](#roles) | Yes | What the package provides, a source, a destination or both, and how each is built or run. |

## `roles`

What the package provides, a source, a destination or both, and how each is built or run.

| Field | Type | Required | Description |
|---|---|---|---|
| `source` | [object](#rolesrole) | No | The package's source. |
| `destination` | [object](#rolesrole) | No | The package's destination. |

- Has at least one of its fields.

## `roles.ROLE`

`ROLE` is `source` or `destination`.

How one role is built, or run from an image or a manifest.

| Field | Type | Required | Description |
|---|---|---|---|
| `build` | [object](#rolesrolebuild) | No | How to build the role's executable for each platform. |
| `image` | string | No | An OCI image repository, without a tag: its tag is the release's version. Matches `^[a-z0-9]+([._-][a-z0-9]+)*(\.[a-z0-9-]+)*(:[0-9]+)?(/[a-z0-9]+([._-][a-z0-9]+)*)+$`. |
| `command` | array of string | No | The image's argv, where it isn't its entrypoint. Each item isn't empty. |
| `manifest` | string | No | A DeclarativeSource document, relative to this file. Sources only, and nothing else. Isn't empty. |
| `requires` | [object](#rolesrolerequires) | No | What the role needs from the platform. |
| `resources` | [object](#rolesroleresources) | No | A container launcher's requests and limits, and the job size the role wants. |
| `state` | [object](#rolesrolestate) | No | The version of the checkpoint state the role writes, and the oldest it can resume from. |

- Has at least one of `build`, `image` or `manifest`.

## `roles.ROLE.build`

How to build the role's executable for each platform.

| Field | Type | Required | Description |
|---|---|---|---|
| `go` | string | No | A Go main package, relative to this file, cross-compiled with `CGO_ENABLED=0`. Isn't empty. |
| `command` | string | No | Run once with sh in this file's directory, with `OPENETL_VERSION` and `OPENETL_PLATFORMS` set. Isn't empty. |
| `output` | string | No | Where `command` leaves each platform's executable, relative to this file, with `{os}` and `{arch}` in it. Isn't empty. |

- Has exactly one of these: `go`; or `command` and `output`.

## `roles.ROLE.requires`

What the role needs from the platform.

| Field | Type | Required | Description |
|---|---|---|---|
| `platform` | string | No | The oldest openetl release the role runs on. Matches `^(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)\.(0\|[1-9][0-9]*)$`. |

## `roles.ROLE.resources`

A container launcher's requests and limits, and the job size the role wants.

| Field | Type | Required | Description |
|---|---|---|---|
| `cpu` | string | No | CPU, as a Kubernetes quantity, such as 500m or 2. Matches `^([0-9]+m\|[0-9]+(\.[0-9]{1,3})?)$`. |
| `memory` | string | No | Memory, as a Kubernetes quantity, such as 512Mi. Matches `^[0-9]+(Ki\|Mi\|Gi\|Ti\|k\|M\|G\|T)?$`. |
| `disk` | string | No | The size of TMPDIR. Matches `^[0-9]+(Ki\|Mi\|Gi\|Ti\|k\|M\|G\|T)?$`. |
| `hint` | string | No | The job size a pipeline of this role starts at when it's sized automatically. Is one of `xs`, `s`, `m`, `l` or `xl`. |

## `roles.ROLE.state`

The version of the checkpoint state the role writes, and the oldest it can resume from.

| Field | Type | Required | Description |
|---|---|---|---|
| `version` | integer | Yes | The version of the state the role writes. Is at least 1. |
| `reads` | integer | Yes | The oldest version of the state the role can resume from. Is at least 1. |

## Types

Fields above refer to these types by name.

### `https`

An https URL. A string. Matches `^https://[^/?#\s]+`. Has at most 2048 characters.
