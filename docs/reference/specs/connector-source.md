---
title: Connector source
description: Every field of a ConnectorSource document, which connects a public or private package catalog to an org.
---

A `ConnectorSource` connects an OCI registry namespace or a GitHub repository to the org's connector catalog. It lists available packages and supplies credentials for fetching packages under its URL. It is separate from a `Source`, which configures where a pipeline reads data.

The **Required** column applies within the containing object or list entry. Optional objects may be omitted; when you include one, its required children must be set. Conditional requirements are stated in the table. **Default** describes what omission means.

```yaml
apiVersion: openetl.io/v1alpha1
kind: ConnectorSource
metadata:
  name: team-connectors
spec:
  url: oci://registry.example.com/team/connectors
  username: REGISTRY_USER
  secret: connector-registry-token
```

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `ConnectorSource` |
| `metadata.name` | name | Yes | — | The catalog source's name: lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `spec` | object | Yes | — | The fields below |

Connector sources belong to the org and are shared across workspaces. They have no package version, and `metadata.workspace` and `metadata.version` must be omitted. A field the document doesn't have is refused.

## spec

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `url` | string | Yes | — | `oci://REGISTRY/NAMESPACE`, without a tag or digest, or `https://github.com/OWNER/REPO` |
| `username` | string | No | None | The registry's user, at most 200 characters. Requires `secret`. Omit for GitHub. |
| `secret` | secret name | No | None | A registry password or token, or a GitHub token. A short name means a secret in the `default` workspace; `workspaces/WORKSPACE/secrets/NAME` names one explicitly. The secret must exist and hold its value in OpenETL, rather than a reference to an external store. |

`secret` is a string, not the `{ secret: NAME }` object a source or destination config uses. When several connector source URLs cover an artifact URL, the longest matching URL supplies its credential. The control plane uses the credential for catalog reads and package resolution; runners use it for artifact downloads.

## Catalog behavior

Apply a connector source with `openetl connector apply -f FILE`. Only an `admin` or `owner` can apply it. `openetl connector catalog` lists packages; `openetl connector catalog --refresh` refreshes the listing and requires `operator` or above.

Connecting or refreshing a catalog source does not install its packages or upgrade pipelines. Apply a package version separately, then select it in a `Source` or `Destination`. If a source fails to list, the catalog keeps its last successful listing and records the error, shown under **Sources** in the console's **Connectors** page.

OpenETL creates the source `official`, pointing at `oci://ghcr.io/openetl-io/openetl-connectors`, when it creates the org. Its URL cannot be changed; `openetl connector catalog --delete-source official` disconnects it to stop listing its packages.

[Choose and manage connectors](../../pipelines/connectors.md) shows installation and private registry setup. The fields are read by [`internal/spec`](../../../internal/spec/spec.go) and defined in [`proto/openetl/api/v1/connector.proto`](../../../proto/openetl/api/v1/connector.proto).
