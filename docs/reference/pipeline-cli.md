---
title: Pipeline CLI commands
description: Look up the arguments, flags and defaults for applying, inspecting and operating pipelines with openetl.
---

These commands call the OpenETL API. The command definitions are in [`cmd/openetl`](../../cmd/openetl). The [CLI overview](./index.md#the-openetl-cli) lists the other command groups.

Put flags before positional arguments. For a command group, put flags after the subcommand. `NAME`, `PIPELINE`, `SOURCE` and `DESTINATION` are resource names within a workspace, not full API paths. `STREAM` is the stream name reported by discovery or the pipeline.

## Common flags and credentials

Every command on this page accepts these optional flags:

| Flag | Type | Default | Meaning |
|---|---|---|---|
| `--api-url` | string | `OPENETL_API_URL`, otherwise `http://localhost:8080` | The API server URL. |
| `--api-key-file` | path | `OPENETL_API_KEY_FILE`, otherwise `.openetl/admin.key` | The file that holds the API key. Relative paths start at the current directory. |
| `--workspace` | string | `default` | The resource's workspace. For `apply` and `plan`, this supplies the workspace only for documents without `metadata.workspace`. |
| `-h` | boolean | `false` | Print the command's flags without calling the API. |

`OPENETL_API_KEY` takes precedence over the key file, even if you supply `--api-key-file`. The API URL flag takes precedence over `OPENETL_API_URL`. The key-file flag takes precedence over `OPENETL_API_KEY_FILE`. There is no `--api-key` flag.

Help is written to standard error. Successful commands return exit status 0. Errors return exit status 1. Plan warnings alone do not cause an error.

## Apply and plan

| Syntax | Result |
|---|---|
| `openetl apply -f FILE` | Create or update the resources in the file. |
| `openetl plan -f FILE` | Validate the documents and show resource changes and pipeline plans without applying them. |

| Flag | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `-f` | repeatable path | Yes | None | A YAML or JSON file. Use `-` for standard input. Files can contain multiple documents separated by `---`. |
| `--dry-run` | boolean | No | `false` | Validate without applying. Available on `apply` only. |

Neither command accepts positional arguments. A document's `metadata.workspace` takes precedence over `--workspace`. Organization resources do not belong to a workspace.

`plan` returns an error if a pipeline that would be created or changed has plan problems. Its output can also show problems on unchanged pipelines. See [Plan, then apply](../pipelines/index.md#plan-then-apply) for the review procedure.

## Pause, resume and sync

| Syntax | Result |
|---|---|
| `openetl pause NAME` | Pause the pipeline. |
| `openetl resume NAME` | Resume the pipeline. |
| `openetl sync NAME` | Request a sync now. The command returns before the sync finishes. |

| Flag | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `--stream` | string | No | None | Pause or resume one stream. Available on `pause` and `resume` only. |

Stream pause is unavailable for CDC streams and pipelines with transforms. A paused stream and a paused pipeline have separate states. Resuming the pipeline does not resume streams paused individually. See [Pause and resume](../pipelines/schedules.md#pause-and-resume).

## Inspect syncs, versions and logs

| Syntax | Result |
|---|---|
| `openetl syncs PIPELINE` | List syncs, newest first. |
| `openetl versions PIPELINE` | List pipeline versions, newest first. `*` marks the current version. |
| `openetl logs PIPELINE` | Print the selected sync's log. |

| Flag | Commands | Type | Required | Default | Meaning |
|---|---|---|---|---|---|
| `--limit` | `syncs`, `versions` | integer | No | `20` | Maximum number of results. The CLI requests further API pages until it reaches the limit or the end. |
| `--sync` | `logs` | string | No | Latest sync | The sync ID from `syncs`. |
| `-f` | `logs` | boolean | No | `false` | Follow the log until the sync ends. Poll every 2 seconds when there are no new entries. |
| `--json` | `logs` | boolean | No | `false` | Print one log entry per line as protobuf JSON. |

`syncs` shows a shortened error. Use `logs` to read the full message. A running sync can show failed attempts while recovery continues. A missing lag value is printed as `-`; it does not mean zero lag. See [Troubleshoot a pipeline](../pipelines/troubleshooting.md).

## Reset and rewind

| Syntax | Result |
|---|---|
| `openetl reset NAME` | Read selected streams from the start on the next sync or run. |
| `openetl rewind --yes NAME` | Permit recovery from the previous checkpoint if the latest checkpoint cannot be read. |

| Flag | Command | Type | Required | Default | Meaning |
|---|---|---|---|---|---|
| `--stream` | `reset` | repeatable string | No | Every stream | Select the streams to read again. |
| `--yes` | `rewind` | boolean | Yes, set to `true` | `false` | Accept delivery of the latest epoch again, including possible duplicate rows. |

Neither operation resumes a paused pipeline. Both operations are unavailable for pipelines with Flink transforms. A reset uses the connector's starting-position behavior. A rewind does not select an arbitrary point in history. See [Reading from the start again](../pipelines/positions.md) for destination effects and recovery steps.

## Rescale

`openetl rescale --size SIZE NAME` changes the job size until the pipeline's next version. A running job restarts at a checkpoint.

| Flag | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `--size` | string | Yes | None | `xs`, `s`, `m`, `l`, `xl`, a CPU and memory pair such as `3,6Gi`, or `auto`. CPU is in vCPUs. `auto` removes the manual size override. |

The pipeline's [resource settings](./specs/pipeline.md#resources) define its normal size and limits. The data plane must have capacity for the requested size.

## Inspect and remove overrides

| Syntax | Result |
|---|---|
| `openetl overrides PIPELINE` | List individually paused streams and overrides set by heal. `overrides list` is an alias. |
| `openetl overrides remove PIPELINE FIELD` | Remove the named override so the pipeline specification controls that field again. |

These commands have no flags beyond the common flags. Use the `FIELD` value from the list. Removing an override does not change the pipeline specification. See [When heal slows syncs down](../pipelines/schedules.md#when-heal-slows-syncs-down).

## Inspect, release and discard holds

| Syntax | Result |
|---|---|
| `openetl holds PIPELINE` | List held columns and cells, their states and available actions. `holds list` is an alias. |
| `openetl holds release PIPELINE STREAM COLUMN` | Request release of the held values on the next run. |
| `openetl holds discard PIPELINE STREAM COLUMN` | Request deletion of the saved values on the next run. |

| Flag | Command | Type | Required | Default | Meaning |
|---|---|---|---|---|---|
| `--new-version` | `holds release` | boolean | No | `false` | Release a whole-column hold into `COLUMN__v2`. Keep the existing column's type. Unavailable for a hold on individual cells alone. |

Release and discard do not change destination data during the CLI request. See [Handle schema changes and holds](../pipelines/schema-changes.md) for repair and verification steps.

## Check connections and discover streams

| Syntax | Result |
|---|---|
| `openetl check source SOURCE` | Ask a runner to check a source connection. |
| `openetl check destination DESTINATION` | Ask a runner to check a destination connection. |
| `openetl discover SOURCE` | Ask a runner to list a source's streams and columns. |

| Flag | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `--data-plane` | string | No | The server's default data plane | The data plane whose runner performs the operation. |

Put flags before `source` or `destination` for `check`. These commands use connections already applied to the API. To test a connector locally with a config file, use the [connector CLI](./index.md#the-openetl-cli).
