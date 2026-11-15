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

Help is written to standard error. Successful commands return exit status 0. Errors return exit status 1. Plan warnings alone do not cause an error. `check`, `preflight` and `openetl connector check` have [exit statuses of their own](#exit-status-of-checks-and-preflights).

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

- A preflight of each pipeline that the apply created or updated, as `openetl preflight` runs it.
- A check of each other source and destination that the apply created or updated.

`apply` discovers each source that passes its check, which refreshes the catalog that plans read. Then it prints the problems in the plans of pipelines that depend on what changed. The checks stop after 2 minutes. `--dry-run` and `--no-check` skip them.

`apply` prints `checked` and the names of the pipelines, sources and destinations that passed, with or without warnings. Under `the apply stands, but its checks found:`, it lists each one that failed, each one that passed with warnings, and each plan problem. Under a target, it prints each failed or warning item with its message, and the item's fix on the next line. For example, an apply that creates a pipeline whose destination check fails, and updates another destination whose check warns, prints:

```text
unchanged workspaces/default/sources/app-db (version 5e1d0feeafb7)
unchanged workspaces/default/destinations/warehouse (version f9aec669685a)
updated workspaces/default/destinations/lake (version 98fc31616b56)
created workspaces/default/pipelines/app-db-to-warehouse (version 1e56f1b4d3be)
checked destination lake

the apply stands, but its checks found:
  pipeline app-db-to-warehouse: destination warehouse's check failed: PERMISSION_DENIED: Create tables in public: openetl_writer has no CREATE on schema public: ERROR: permission denied for schema public (SQLSTATE 42501)
    failed   Create tables in public: openetl_writer has no CREATE on schema public.
             fix: Grant CREATE on schema public to openetl_writer.
  destination lake: passed, with warnings
    warning  Let a commit run to its end: statement_timeout is 30s, and the connector needs 0, or at least 10min.
             fix: Set statement_timeout to 0, or at least 10min.
```

A failed check, a failed preflight or a plan problem does not change the exit status of `apply`, because the applied resources stay. To see every item of a target, run its check or its preflight.

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

## Check connections, preflight pipelines and discover streams

| Syntax | Result |
|---|---|
| `openetl check source SOURCE` | Ask a runner to check a source's connection, and print the check's items. |
| `openetl check destination DESTINATION` | Ask a runner to check a destination's connection, and print the check's items. |
| `openetl preflight PIPELINE` | Ask a runner of the pipeline's data plane to run its preflight, print each part and its items, then print the pipeline's plan. |
| `openetl check pipeline PIPELINE` | Deprecated. The same as `openetl preflight PIPELINE`. |
| `openetl discover SOURCE` | Ask a runner to list a source's streams and columns. |

| Flag | Commands | Type | Required | Default | Meaning |
|---|---|---|---|---|---|
| `--data-plane` | `check`, `discover` | string | No | The server's default data plane | The data plane whose runner performs the operation. |
| `--format` | `check`, `preflight` | `text` or `json` | No | `text` | `text` prints a checklist. `json` prints the API's result as one JSON document. See [JSON output](#json-output). |

Put flags before `source` or `destination` for `check`, and before the pipeline for `preflight`. These commands use connections and pipelines already applied to the API. To test a connector locally with a config file, use `openetl connector check`, in the [connector CLI](./index.md#the-openetl-cli).

`openetl check pipeline PIPELINE` takes the flags of `preflight`. It prints `openetl: check pipeline is deprecated; use openetl preflight NAME` on standard error, then runs the preflight.

A [check](../get-started/concepts.md#checks-and-preflights) tests one connection. A runner starts the connector and runs its check, which has 1 minute. If the connector does not start, or does not answer within 1 minute, the check could not run, and the command exits with `2`.

A preflight tests a pipeline on a runner of the pipeline's data plane. It runs in five parts, and its result lists them in this order:

1. **The source's check.** The runner starts the source connector, then runs its check.
2. **The destination's check.** The runner starts the destination connector, then runs its check.
3. **The sync's plan.** The runner settles what a sync reads and writes, as a sync does: it discovers the source's streams and plans the sync. If that passes, the API refreshes the source's catalog and plans the pipeline. A plan problem is a failed item, and a plan warning is a warning item. This part is skipped if the source did not start, the source's check failed, or the destination did not start.
4. **The source's preflight.** The source connector's checks of the streams the pipeline reads, with their sync modes and the pipeline's schedule. This part is skipped if the source did not start, its check failed, or a sync cannot be planned.
5. **The destination's preflight.** The destination connector's checks of the streams the pipeline writes, with their write modes, keys, columns and tables. This part is skipped if the destination did not start, its check failed, or a sync cannot be planned.

A skipped part gives its reason: `the source didn't start`, `the source's check failed`, `the destination didn't start`, `the destination's check failed` or `a sync can't be planned`. A skipped part fails the preflight.

A connector that does not start, or does not answer, fails its part with an error code and a message, and no items. Each part has a time limit: 1 minute for a check, and 2 minutes for a preflight. The plan has what is left of the preflight's 10 minutes, less the time kept for the two connectors' preflights. A part that runs out of time fails with the code `TRANSIENT` and a message such as `the source's preflight didn't finish within 2m0s`. The parts that do not need it still run.

### Text output

`check` prints one line for the source or destination, then one line for each item, in the order the connector checked them. A check that passes with a warning:

```sh
openetl check destination lake
```

```text
workspaces/default/destinations/lake passed its check (version f9aec669685a)
  passed   Resolve lake.internal
           lake.internal resolves to 10.0.4.12.
  passed   Reach lake.internal:5432
  passed   Connect to the database
  warning  Let a commit run to its end
           statement_timeout is 30s, and the connector needs 0, or at least 10min.
           fix: Set statement_timeout to 0, or at least 10min.
           code: CONFIG_INVALID (setting_mismatch)
```

A check that fails:

```sh
openetl check destination warehouse
```

```text
workspaces/default/destinations/warehouse failed its check (version f9aec669685a)
  passed   Resolve warehouse.internal
           warehouse.internal resolves to 10.0.4.12.
  passed   Reach warehouse.internal:5432
  passed   Connect to the database
  failed   Create tables in public
           openetl_writer has no CREATE on schema public.
           detail: ERROR: permission denied for schema public (SQLSTATE 42501)
           fix: Grant CREATE on schema public to openetl_writer.
           field: schema
           code: PERMISSION_DENIED (privilege_missing)
  skipped  Write to tables in public
           needs schema.create, which failed
openetl: workspaces/default/destinations/warehouse failed its check: PERMISSION_DENIED: Create tables in public: openetl_writer has no CREATE on schema public: ERROR: permission denied for schema public (SQLSTATE 42501)
```

The first line says whether the check passed, and the version of the source or destination that was checked. Each item's line has its status and its title. The lines under an item are:

| Line | Shown for | Meaning |
|---|---|---|
| The message | Every item that has one | What the connector found, for a passed item. What is wrong, for a failed or warning item. Why it was not checked, for a skipped item. |
| `detail:` | Failed and warning items | The cause in the words of the upstream system or its driver, such as a database's error. It can have more than one line. |
| `fix:` | Failed and warning items | What to do to correct it. |
| `field:` | Failed and warning items | The config field it concerns, as a dotted path such as `consumer.name`. |
| `code:` | Failed and warning items | The error code, such as `PERMISSION_DENIED`, then in parentheses the reason: the kind of failure, such as `privilege_missing`. |

A line is left out when the item has no value for it. A connector that does not say what it checked has no items. Then only the first line is printed.

If a check fails and no item failed, the first line ends with the error code and the message. This happens with a connector that reports one error and no items, and with checks that the connector stopped, such as when they ran out of time. For example:

```sh
openetl check destination archive
```

```text
workspaces/default/destinations/archive failed its check (version f9aec669685a): PERMISSION_DENIED: postgres: no CREATE on schema "public"
openetl: workspaces/default/destinations/archive failed its check: PERMISSION_DENIED: postgres: no CREATE on schema "public"
```

`preflight` prints one line for each part, in the order of the result: `passed`, `failed` with the error code and message, or `skipped` with the reason. The part's items follow, as `check` prints them. Then the command prints the pipeline's plan, as `openetl plan` prints it. A preflight with a failed part and a skipped part, with the plan cut to its first line:

```sh
openetl preflight app-db-to-warehouse
```

```text
source app-db's check: passed
  passed   Resolve app-db.internal
           app-db.internal resolves to 10.0.4.12.
  passed   Reach app-db.internal:5432
  passed   Connect to the database
destination warehouse's check: failed: PERMISSION_DENIED: Create tables in public: openetl_writer has no CREATE on schema public: ERROR: permission denied for schema public (SQLSTATE 42501)
  passed   Resolve warehouse.internal
           warehouse.internal resolves to 10.0.4.12.
  passed   Reach warehouse.internal:5432
  passed   Connect to the database
  failed   Create tables in public
           openetl_writer has no CREATE on schema public.
           detail: ERROR: permission denied for schema public (SQLSTATE 42501)
           fix: Grant CREATE on schema public to openetl_writer.
           field: schema
           code: PERMISSION_DENIED (privilege_missing)
  skipped  Write to tables in public
           needs schema.create, which failed
the sync's plan: passed
  passed   The plan
           2 streams, no problems
source app-db's preflight: passed
  passed   Publish public.customers in openetl
  passed   Publish public.orders in openetl
destination warehouse's preflight: skipped, the destination's check failed

workspaces/default/pipelines/app-db-to-warehouse (version 1e56f1b4d3be)
openetl: workspaces/default/pipelines/app-db-to-warehouse failed its preflight: destination warehouse's check failed: PERMISSION_DENIED: Create tables in public: openetl_writer has no CREATE on schema public: ERROR: permission denied for schema public (SQLSTATE 42501)
```

The last line, which starts with `openetl:`, is on standard error. For a failed preflight, it names the first failed part. If no part failed, it names the first skipped part, such as `source shop's preflight was skipped: the source's check failed`. If no part failed or was skipped, it says `it failed, though none of its parts did; run it again`.

The CLI fills each title, message and fix from the item's params. It removes control characters and bidirectional formatting characters from all text that a connector sent, so that the text cannot change the terminal. A detail keeps its line breaks. OpenETL cuts a connector's long texts, and ends each cut text with `…`. An answer with too many items fails with `INTERNAL`. [What OpenETL does with the answer](../build-connectors/checks.md#what-the-platform-does-with-the-answer) gives the limits.

### JSON output

With `--format json`, `check` and `preflight` print the API's result as one JSON document on standard output, and nothing else there. `check` prints a `CheckResult`, and `preflight` prints a `PreflightResult`. [Check and preflight results](../integrate/index.md#check-and-preflight-results) describes their fields. In JSON, `preflight` does not print the plan. Notes and errors go to standard error.

The document is as the API sends it:

- Titles, messages, fixes and descriptions are templates. Each `{name}` in them is a placeholder for the value of `name` in the item's `params`.
- Fields with an empty or false value are left out. For example, `ok` is not in the document when the check failed.

This command prints the message of each failed or warning item, with its placeholders filled:

```sh
openetl check --format json destination lake | jq -r '.items[] | select(.status == "warning" or .status == "failed") | (.params // {}) as $p | .message | gsub("\\{(?<n>[a-z][a-z0-9_]*)\\}"; $p[.n] // "{\(.n)}")'
```

```text
statement_timeout is 30s, and the connector needs 0, or at least 10min.
```

### Exit status of checks and preflights

`check`, `preflight`, `check pipeline` and `openetl connector check` return these exit statuses. Scripts and CI jobs can rely on them:

| Status | Meaning |
|---|---|
| `0` | It passed. Warnings do not change the status. |
| `1` | It ran and failed: an item failed, a part failed or was skipped, or the check or preflight failed with no failed item. In a preflight, a connector that does not start or answer fails its part, so the status is `1`. |
| `2` | It could not run: a usage error, an unknown `--format`, the API or a runner that cannot be reached, or an operation that ran out of time. For `check` and `openetl connector check`, also a connector that did not start, or did not answer within 1 minute. For `preflight`, also a source catalog that could not be refreshed after the plan part passed. |

A status of `2` says nothing about the connection or the pipeline. The error starts with `the check couldn't run:`, or `the preflight couldn't run:` for a preflight. For example, if no runner of the data plane takes the check within 1 minute, `check` prints this and exits with `2`:

```text
openetl: unavailable: the check couldn't run: no runner of data plane "default" took it within 1m0s
```

A destination whose connector does not answer its check within 1 minute gives:

```text
openetl: unavailable: the check couldn't run: TRANSIENT: the destination's check didn't answer within 1m0s
```
