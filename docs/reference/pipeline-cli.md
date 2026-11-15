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
| `--no-check` | boolean | No | `false` | Do not check what the apply changed. Available on `apply` only. |

Neither command accepts positional arguments. A document's `metadata.workspace` takes precedence over `--workspace`. Organization resources do not belong to a workspace.

After `apply` changes resources, it asks a runner to check them:

- Each source and destination that the apply created or updated.
- The source and destination of each pipeline that the apply created or updated, on the pipeline's data plane.

`apply` discovers each source that passes its check, which refreshes the catalog that plans read. Then it prints the problems in the changed pipelines' plans. A failed check or a plan problem prints a warning and does not change the exit status, because the applied resources stay. The checks stop after 2 minutes. `--dry-run` and `--no-check` skip them.

For a pipeline with `transform.sql`, `plan` and `apply` send the statements to a runner of the pipeline's data plane and wait up to 30 seconds for one to take them. Without a runner, the plan has a note instead of the statements' plan.

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

## List quarantined rows

| Syntax | Result |
|---|---|
| `openetl quarantine PIPELINE` | List the files of rows that the pipeline's committed epochs quarantined, newest first. For each file, show when it committed, its stream, generation and epoch, its rows and its path. Then add up the rows by stream and cause. `quarantine list` is an alias. |
| `openetl quarantine show PIPELINE PATH` | Print the first lines of the file at `PATH`, as `openetl quarantine` gives it. Each line is a JSON object with a record and its cause. The command prints them as the file has them, so you can pipe them to `jq`. |

| Flag | Command | Type | Required | Default | Meaning |
|---|---|---|---|---|---|
| `--stream` | `quarantine` | string | No | Every stream | List only this stream's files, or this transform output's. |
| `--since` | `quarantine` | duration | No | No limit | List only files committed in this period before now, such as `24h` or `7d`. |
| `--limit` | `quarantine` | integer | No | `100` | List at most this many files, the newest. |
| `--json` | `quarantine` | boolean | No | `false` | Print each file as a line of JSON, with its causes, and no sums. |
| `--limit` | `quarantine show` | integer | No | `20` | Print at most this many lines, from 1 to 100. The command also stops before 256 KiB of lines. |

A cause is a code, the column if one column caused it, and why OpenETL did not hold the value if the stream holds values. [Quarantine files](./quarantine-files.md) lists the codes and gives the format of the files' lines. `quarantine` needs the viewer role. It reads the control plane's list of files, not the files.

`quarantine show` needs the editor role, and the audit log records each read. A runner of the pipeline's data plane reads the file. The control plane passes the lines to the command, and does not keep them. If the file has more lines, the command says so on standard error. If the pipeline's `quarantine.retention` or a rule of the store deleted the file, the command fails with `not_found`.

## Check connections and discover streams

| Syntax | Result |
|---|---|
| `openetl check source SOURCE` | Ask a runner to check a source connection. |
| `openetl check destination DESTINATION` | Ask a runner to check a destination connection. |
| `openetl check pipeline PIPELINE` | Ask a runner to check the pipeline's source and destination, discover the source's streams, and then print the pipeline's plan. |
| `openetl discover SOURCE` | Ask a runner to list a source's streams and columns. |

| Flag | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `--data-plane` | string | No | The server's default data plane. For `check pipeline`, the pipeline's data plane. | The data plane whose runner performs the operation. |

Put flags before `source`, `destination` or `pipeline` for `check`. `check pipeline` returns an error if a check fails or the plan has problems. When the connector says what its check looked at, each command prints one line an item, its status (`passed`, `warning`, `failed` or `skipped`) and title, then its error code and message, and under an item that failed or warned, the config `field` it concerns and its `fix`. After an `apply`, a warning about a failed check lists the fixes of its failed items. These commands use connections already applied to the API. To test a connector locally with a config file, use the [connector CLI](./index.md#the-openetl-cli).
