---
title: Source
description: Every field of a Source document, which names the connector a pipeline reads with and holds its config.
---

A `Source` is a system pipelines read from: a connector, and that connector's config. Pipelines name it in `source.ref`. [Pipelines as code](../../pipelines/index.md#the-documents) shows how the documents fit together.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: demo-data
spec:
  connector: faker
  config:
    seed: 42
    count: 1000
```

## The document

| Field | Type | Default | Meaning |
|---|---|---|---|
| `apiVersion` | string | Required | `openetl.io/v1alpha1` |
| `kind` | string | Required | `Source` |
| `metadata.name` | name | Required | The source's name: lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | The workspace `openetl apply --workspace` names, `default` unless it says | The workspace it belongs to |
| `spec` | object | Required | The fields below |

A field the document doesn't have is refused.

## spec

| Field | Type | Default | Meaning |
|---|---|---|---|
| `connector` | connector reference | Required | The connector to read with, and which of its versions. See [connector references](#connector-references). |
| `config` | object | None | The connector's config. Its fields are the connector's own: its spec's JSON Schema defines them, which `openetl connector spec TARGET` prints. A config never holds a secret's value: a field holds `{ secret: NAME }` instead, naming a secret in the same workspace, which must exist when you apply. Where the connector's spec marks a field as secret, the config must name a secret for it. |

## Connector references

`connector` is a connector's package name, with a version after `@` if you want one. `acme/billing` below is an example name, not a published connector:

| Reference | What it names |
|---|---|
| `acme/billing` | The highest stable version of `acme/billing` applied to the control plane, or the highest prerelease if there is no stable version. With no versions applied, the runner looks for `acme/source-billing` in its connectors directory. |
| `acme/billing@1`, `acme/billing@1.4` | The highest stable release with that major, or major and minor, version |
| `acme/billing@1.2.0` | That version |
| `acme/billing@sha256:DIGEST` | The version with that digest |
| `billing` | Shorthand for `openetl/billing`. A name without an owner is `openetl`'s. With no versions applied, the runner looks for `source-billing`. |

Each apply resolves the reference again, so a reference without an exact version or digest may pin a newer version than the last apply did. The built-in source, `faker` (also `openetl/faker`), has no versions, so its reference takes none. [Built-in connectors](../built-in-connectors.md) has its config.

Connector versions are independent of OpenETL versions. Resolution uses packages already applied to the org, not every release in a remote registry. [Choose and manage connectors](../../pipelines/connectors.md) covers installation and controlled upgrades.
