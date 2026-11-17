---
title: Package and publish a connector
description: Package, validate, distribute and document a connector release.
---

Use this guide after you [test connector conformance](./test.md). Keep connector releases in your own repository.

## Use it in OpenETL

While you develop, put the executable in the runner's connectors directory (`openetld`'s `--connectors`; `bin/connectors` under `make dev`), named `ROLE-NAME`, such as `source-greetings`. A source then names it as `connector: greetings`. For a package with an owner, such as `acme/greetings`, the path is `acme/source-greetings`.

Everywhere else, publish it as a package, and apply a version of it.

## Package and publish

A package is a directory with an `openetl-connector.yaml`: its name, its metadata, and how to build each role.

```yaml
name: acme/greetings
description: Greetings, made up
license: Apache-2.0
repository: https://github.com/acme/openetl-greetings
docs: https://github.com/acme/openetl-greetings#readme
roles:
  source:
    build: { go: ./cmd/source-greetings }
```

`openetl connector schema file` prints the file's JSON Schema. Check the file, then build a version:

```sh
openetl connector validate
openetl connector package --version 0.1.0
```

```text
.: acme/greetings
acme/greetings 0.1.0: dist/greetings.connector.yaml
```

`package` builds each role into `dist/`, with names such as `greetings-source-linux-amd64`. The default targets are Linux and macOS on amd64 and arm64; the package file can override them. It runs the executable for the local platform to read its spec. It then writes the **descriptor**, `dist/greetings.connector.yaml`.

The descriptor lists each platform's executable URL and digest. By default, URLs point to GitHub release assets for the version's tag, here `v0.1.0`. `--tag` accepts a release tag instead of `--version`.

Upload the contents of `dist/` as GitHub release assets. Alternatively, publish them to an OCI registry with `openetl connector publish oci://REGISTRY/NAMESPACE`.

An org then applies a version, by reference or from the descriptor:

```sh
openetl connector apply github.com/acme/openetl-greetings@0.1.0
openetl connector apply oci://REGISTRY/NAMESPACE/greetings:0.1.0
openetl connector apply -f dist/greetings.connector.yaml
```

A `ConnectorSource` document connects a registry namespace or a GitHub repository to the org as a source of its catalog. `openetl connector catalog`, and **Connectors** in the console, list the packages its sources offer, with the command that applies each. Under a connected source, a reference without a version, such as `github.com/acme/openetl-greetings`, applies the newest version: the highest stable version, or the highest prerelease if there is no stable one. OpenETL connects the official connectors' source, `oci://ghcr.io/openetl-io/openetl-connectors`, when it creates the org.

## Release and compatibility

Choose a package name under your own owner, such as `acme/greetings`. A name without an owner belongs to `openetl`. Use your own name for a fork too, so an installation can distinguish it from the original package. Package names stay associated with their source repository, and applied versions are immutable: changed code or metadata needs a new version.

Your connector's version is independent of OpenETL's version and of other connectors' versions. For a repository with one package, tags can be `v0.1.0`; for several packages, `greetings/v0.1.0` lets each package release separately. `openetl connector package --tag TAG` selects the package and version from that tag.

Compatibility comes from the role's contract, not matching version numbers:

- The descriptor records the connector protocol and required features reported by the executable's spec. A runner must support them.
- `roles.ROLE.requires.platform` in `openetl-connector.yaml` declares the oldest OpenETL release the role runs on, when it needs a minimum release.
- `roles.ROLE.state.version` and `roles.ROLE.state.reads` describe the checkpoint state it writes and the oldest state it reads. Document state changes and whether an older connector can resume after an upgrade.
- Build executables for the operating systems and architectures your users' runners use. An image-only role cannot run on a runner without a container launcher.

Test the released artifact with the conformance kit against the OpenETL versions you support. Publish release notes for config, behavior and state changes, and keep setup instructions and limits with the connector version. The [package file](../reference/connector-file.md) and [descriptor](../reference/connector-descriptor.md) references define these fields.

### Private distribution

The same package commands work for a private connector. Publish to a registry namespace your organization controls, or upload the descriptor and binaries to a private GitHub release. Your organization supplies its own repository, docs URL and release process; no entry in the official connector repository is required.

The CLI reads Docker's registry credentials when publishing or running a package locally. An OpenETL installation uses the credentials in its own `ConnectorSource` instead. Give the people installing your package its reference, the required role and platform versions, and the [private connector setup](../pipelines/connectors.md#private-connectors).

## Document it

Keep connector documentation in the connector's repository. Set `docs` in `openetl-connector.yaml` to its URL. The descriptor includes this URL, and the console catalog shows it as **Docs**.

Document the supported systems and versions, account permissions, required settings, configuration fields, type mappings, delivery guarantees and limits. Include verification and recovery steps where the connector requires them.

The config's JSON Schema is the reference for its fields, so give every property a `description`. The official connectors' pages live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors), next to their code.
