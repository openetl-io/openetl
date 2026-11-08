---
title: Destination
description: Every field of a Destination document, which names the connector a pipeline writes with and holds its config.
---

A `Destination` is a system pipelines write to: a connector, and that connector's config. Pipelines name it in `destination.ref`. [Pipelines as code](../../pipelines/index.md#the-documents) shows how the documents fit together.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-parquet
spec:
  connector: parquet
  config:
    url: file:///tmp/openetl-parquet
```

## The document

| Field | Type | Default | Meaning |
|---|---|---|---|
| `apiVersion` | string | Required | `openetl.io/v1alpha1` |
| `kind` | string | Required | `Destination` |
| `metadata.name` | name | Required | The destination's name: lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | The workspace `openetl apply --workspace` names, `default` unless it says | The workspace it belongs to |
| `spec` | object | Required | The fields below |

A field the document doesn't have is refused.

## spec

| Field | Type | Default | Meaning |
|---|---|---|---|
| `connector` | connector reference | Required | The connector to write with, and which of its versions. A reference is read as a [Source's](./source.md#connector-references) is, and with no versions applied, the runner looks for an executable named `destination-NAME`. |
| `config` | object | None | The connector's config. Its fields are the connector's own: its spec's JSON Schema defines them, which `openetl connector spec TARGET` prints. A config never holds a secret's value: a field holds `{ secret: NAME }` instead, naming a secret in the same workspace, which must exist when you apply. Where the connector's spec marks a field as secret, the config must name a secret for it. |

The built-in destinations, `stdout` and `parquet` (also `openetl/stdout` and `openetl/parquet`), have no versions, so their references take none. [Built-in connectors](../built-in-connectors.md) has their configs.

How a pipeline writes to the destination, such as the schema it writes into and its write mode, is the pipeline's: see [Pipeline](./pipeline.md#destination).
