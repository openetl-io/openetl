---
title: dbt project
description: The required project input and bundle format of a DbtProject document.
---

A `DbtProject` stores the files a scheduled pipeline runs after a sync. A pipeline names it in `postSync[].dbt.project`. [Run dbt after a sync](../../integrate/dbt.md) covers the adapter, generated connection profile and execution.

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `DbtProject` |
| `metadata.name` | name | Yes | — | Lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | No | The CLI's `--workspace`, otherwise `default` | The workspace the resource belongs to |
| `spec` | object | Yes | — | The fields below |

Fields that aren't listed are refused. **Required** applies within the containing object; an optional object may be omitted, but its required children must be set when you include it. **Default** describes what omission means.

## spec

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `path` | directory path | When no `bundle` is supplied to the CLI | — | CLI input only: the project directory, relative to the file containing this document, or an absolute path. The CLI packages it and replaces this field with `bundle` before calling the API. |
| `bundle` | base64 string | Yes for the API; supplied by the CLI when using `path` | — | The project's files as a base64-encoded gzipped tar archive. The compressed archive is at most 16 MiB. Supply `path` for a local project; API clients supply `bundle`. |

Use `path` for a local project. If both inputs are supplied, the CLI replaces `bundle` with the files from `path`. The API refuses `path`; it accepts only the bundle prepared by the CLI or your client. The CLI's packing leaves out hidden files, `target/`, `logs/` and `profiles.yml`. It includes `dbt_packages/`; runners don't run `dbt deps`, so vendor the packages your project needs.

The project requires a `dbt_project.yml` with both `name` and `profile`. The profile must be a plain name, without template expressions. Runners generate the profile for the pipeline's destination, and run the project version pinned by the sync. Updating a project gives its dependent pipelines new versions. A failed post-sync step fails the sync after its copied data has already committed.

The authored document in [`examples/faker-to-postgres-dbt.yaml`](../../../examples/faker-to-postgres-dbt.yaml) uses `path: dbt`, resolved to [`examples/dbt`](../../../examples/dbt). The pipeline's dbt command, selection and target schema are [Pipeline fields](./pipeline.md#steps-after-a-sync), rather than project fields.
