---
title: Choose and manage connectors
description: Find, install, pin and upgrade official, community and private source and destination connectors.
---

Install the connectors your pipelines need, then name them in your `Source` and `Destination` documents. A connector package provides a source role, a destination role, or both. Its author defines its config, supported systems, read and write modes, type mappings and limits.

Official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Community members and organizations can maintain connectors in their own repositories, including private ones. Each package has its own versions and release cadence, independent of OpenETL. The [built-in connectors](../reference/built-in-connectors.md) ship with OpenETL and need no installation.

## Choose a connector

In the console, open **Connections**, then **Connectors**. The **Catalog** lists packages from the registries and repositories connected to your installation. Open a package and follow its **Docs** link for setup instructions and limits.

Check these requirements before selecting a version:

- The source or destination role your pipeline needs.
- Supported systems and system versions.
- Required sync modes, write modes and delivery capabilities.
- Compatible OpenETL versions.
- Executables for your runners' operating systems and architectures.

Connectors for the same system can have different capabilities.

The docs site's **Connectors** section lists official packages. For community and private connectors, use the documentation their authors provide. Connector docs follow the connector's release; selecting an OpenETL docs version does not select a connector version.

## List and install packages

Installing packages requires the `admin` or `owner` role. The CLI uses your installation's API URL and key, as described in the [CLI reference](../reference/index.md#the-openetl-cli).

List packages from your installation's connected catalog sources:

```sh
openetl connector catalog
```

An operator can refresh the listing:

```sh
openetl connector catalog --refresh
```

Refreshing lists what's available. It does not install a version or change a pipeline. A **connector source** is a registry namespace or repository used for this listing; it is separate from a pipeline's data source.

A package you've applied that no connected source lists, from a file or by a reference, is in the catalog too, marked **Manually installed**, at the highest version you've applied. It has no apply command there, and OpenETL can't say whether a newer version exists.

A package whose listed version is later than the highest you've applied is marked **Update available** in the console, and `(update available)` in the catalog's `APPLIED` column. Applying the new version doesn't change a pipeline already pinned to an older one.


Use the apply command shown by the catalog, or a reference from the connector's author. These are alternative forms; replace the capitals with your package's location and version:

```sh
openetl connector apply oci://REGISTRY/NAMESPACE/PACKAGE:VERSION
openetl connector apply github.com/OWNER/REPO/PACKAGE@VERSION
openetl connector apply -f PACKAGE.connector.yaml
```

For a GitHub repository with one package, omit `/PACKAGE`. A descriptor file is produced by the author's packaging command; it describes a release and where to fetch its artifacts.

Applying a connector makes that package version available to the org, across workspaces. It does not create a data source, a destination or a pipeline. Runners fetch the selected executable when they need it.

## Select a version in a connection

In a `Source` or `Destination`, set `spec.connector` to the installed package's name and version. For example, `acme/billing@1.2.0` selects version `1.2.0` of the package named `acme/billing`; this is an example name, not a published connector.

The package name comes from its descriptor, not from its registry path. An owner is part of that name. Omitting the owner means `openetl`, so use the full name for a community or private package.

| Reference | Selects among versions already applied to the org |
|---|---|
| `acme/billing@1.2.0` | That exact version |
| `acme/billing@1.2` | The highest stable release in the `1.2` line |
| `acme/billing@1` | The highest stable release in major version `1` |
| `acme/billing` | The highest stable release, or the highest prerelease if there is no stable one |
| `acme/billing@sha256:DIGEST` | The version with that descriptor digest |

Use an exact version or digest for a reviewed deployment. A major version, minor version or unversioned reference can select a newer installed version each time you apply the connection. These references do not fetch uninstalled releases from a registry. [Source](../reference/specs/source.md#connector-references) and [Destination](../reference/specs/destination.md) define the fields.

Use the selected connector's documentation for `config`; there is no common set of connection fields. Store credentials as [secrets](./index.md#secrets). Once you apply your connection documents, use `openetl check source NAME` or `openetl check destination NAME` to check them, and `openetl discover NAME` to list a source's streams. Then plan and apply the pipeline, as in [Pipelines as code](./index.md).

## Add a catalog source

An administrator can connect another registry namespace or GitHub repository with a `ConnectorSource` document. Save this example as `connector-source.yaml`, replacing its URL with your namespace:

```yaml
apiVersion: openetl.io/v1alpha1
kind: ConnectorSource
metadata:
  name: team-connectors
spec:
  url: oci://registry.example.com/team/connectors
```

Apply it, then refresh the catalog:

```sh
openetl connector apply -f connector-source.yaml
openetl connector catalog --refresh
```

For GitHub, use `https://github.com/OWNER/REPO` as the URL. Connecting a catalog source does not install its packages. You can also apply a public package directly by reference without adding its repository to the catalog.

OpenETL connects the official registry namespace when it creates the org. Your installation can also list community and private packages. The [Connector source reference](../reference/specs/connector-source.md) describes every field.

## Private connectors

An organization can build its own connector or maintain a fork under its own package name. The package uses the same protocol and installation commands as a public one. [Build a connector](../build-connectors/index.md) covers authoring and distribution.

For an authenticated registry or GitHub repository, store a read credential in OpenETL. For example, read a token from a local file:

```sh
openetl secret set --from-file TOKEN_FILE connector-registry-token
```

Add the credential to `connector-source.yaml`:

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

Here `secret` takes a secret name. Data connection configuration instead uses the `{ secret: NAME }` object. A short name selects the `default` workspace. The secret value must be stored in OpenETL because the control plane also needs it. External secret-store references cannot be used for catalog credentials.

Apply the file with `openetl connector apply -f connector-source.yaml`. The control plane uses this credential to list packages and resolve references, and runners use it to fetch artifacts under the source's URL. When several sources cover a URL, the most specific URL supplies the credential. For a private GitHub repository, change the URL to `https://github.com/OWNER/REPO`, keep `secret`, and omit `username`.

A local `docker login` supplies credentials to the CLI's local package commands. It does not configure credentials on the OpenETL installation. The control plane and runners must also be able to reach your registry; see [Self-host](../self-host/index.md#the-settings-every-installation-sets).

## Upgrade a connector

An OpenETL upgrade and a connector upgrade are separate choices. Matching version numbers do not establish compatibility. Check the connector release notes for its required protocol, features, minimum OpenETL release, config changes and checkpoint-state compatibility.

1. Apply the new package version with `openetl connector apply`.
2. Change `spec.connector` in the source or destination documents you intend to upgrade to the new exact version. Make any config changes the release requires.
3. Run `openetl plan -f FILE`. A changed connection gives every pipeline that uses it a new version, so review all affected pipelines and their delivery guarantees.
4. Run `openetl apply -f FILE`, check the connections, and watch the next sync or continuous run in the console.

Installing a version alone leaves existing pipelines on their resolved package. Reapplying a connection with an unversioned, major or minor reference can move it to a newer installed release even if the YAML did not change. Pin exact versions to make that choice explicit.

Before returning to an older connector, check its checkpoint-state compatibility. Restoring YAML does not undo destination changes or convert checkpoint state for an older connector. Restore the earlier connection configuration and version reference. Review the plan before applying it.

## Local development builds

When no package versions are applied for an unversioned external connector name, a runner falls back to an executable in its connectors directory. `make dev` uses this to run the connectors that it builds from source, as [CONTRIBUTING.md](../../CONTRIBUTING.md#run-it-from-source) describes. These executables have no package version pinned by OpenETL; replacing one changes what that runner executes.

Use applied packages for deployments where you need versioned artifacts. [Use it in OpenETL](../build-connectors/index.md#use-it-in-openetl) describes the local executable names, and [Connectors are trusted code](../self-host/index.md#connectors-are-trusted-code) explains how connectors run.
