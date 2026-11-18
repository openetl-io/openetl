---
title: Define pipeline resources
description: Define sources, destinations and pipelines in YAML, with references to stored secrets.
---

Use YAML files to define pipeline resources. This guide assumes that you have completed the [quickstart](../get-started/quickstart.md).

## The documents

Everything you apply is a document with the same four parts:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-postgres
spec:
  connector: postgres
  config:
    dsn: { secret: local-postgres-dsn }
```

- `apiVersion` is `openetl.io/v1alpha1`, except for a `Connector`'s, which is `openetl.io/v1alpha2`.
- `kind` says what the document defines.
- `metadata.name` is lowercase letters, digits and hyphens, at most 63 characters. `metadata.workspace` is optional: an installation has one workspace, `default`.
- `spec` is the definition itself.

A file can hold several documents, separated by `---`, and `-f` can be given more than once. Documents refer to each other by name, so a pipeline's `source.ref` is the name of a `Source`.

| Kind | What it defines | Covered in |
|---|---|---|
| `Source` | A system to read from: a connector and its `config` | This page |
| `Destination` | A system to write to: a connector and its `config` | This page |
| `Pipeline` | What to copy from a source to a destination, and how | This page |
| `DbtProject` | A dbt project's files, to run after a pipeline's syncs | [Integrate](../integrate/index.md) |
| `SparkCluster`, `FlinkCluster` | Your own Spark or Flink cluster, for transforms and steps | [Integrate](../integrate/index.md) |
| `Webhook` | Where platform events, such as a failed sync, are sent | [Integrate](../integrate/index.md) |
| `Connector`, `DeclarativeSource` | A version of a connector, or of a source written in YAML | [Build a connector](../build-connectors/index.md) |
| `ConnectorSource` | A registry or repository whose connector packages the console lists | [Build a connector](../build-connectors/index.md) |

A `Source` or `Destination` specifies a connector and its `config`. The connector's documentation defines the configuration fields. `openetl connector spec TARGET` prints the connector's specification, including its configuration JSON Schema.

An unversioned name such as `postgres` can select a newer installed version each time you apply the connection. The plan warns about this. `postgres@1` restricts selection to major version 1. Pin an exact version or digest when the deployment requires a specific release.

[Choose and manage connectors](./connectors.md) covers installing official, community and private packages, selecting versions, and upgrading them independently of OpenETL.

## Secrets

A config never holds a password. It names a secret instead, as `{ secret: NAME }`:

```yaml
config:
  dsn: { secret: local-postgres-dsn }
```

Store secrets with the CLI. `set` reads a value from standard input or `--from-file`. With `--ref`, it stores a reference to a cloud secret store. Runners resolve that reference with their cloud identity:

```sh
echo postgres://postgres:quickstart@quickstart-postgres:5432/postgres | openetl secret set local-postgres-dsn
openetl secret list
openetl secret delete local-postgres-dsn
```

`list` shows current resources that use each secret. `delete` refuses a secret used by a current source, destination, webhook, Spark cluster, Flink cluster or pipeline. Check private connector sources separately. [Manage connection secrets](../self-host/secrets.md) covers cloud references, credential replacement and deletion.

`set` removes one final line ending from its input. Runners do the same for text values read from a cloud store. This permits values written by an editor or `echo`. Fields inside JSON values remain unchanged.

Next, [plan and apply the files](./plan-and-apply.md).
