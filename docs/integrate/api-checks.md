---
title: Check results in the API
description: Interpret connection checks, pipeline preflights and errors returned by the API.
---

First, [connect your application to the API](./api.md). Use this reference to interpret check and preflight responses.

### Check and preflight results

These calls ask a runner to run a [check or a preflight](../get-started/concepts.md#checks-and-preflights), and wait for its result. Each needs the `operator` role.

| Call | REST | Response field |
|---|---|---|
| `SourceService.CheckSource` | `POST /v1/workspaces/WORKSPACE/sources/SOURCE:check` | `result`, a `CheckResult` |
| `DestinationService.CheckDestination` | `POST /v1/workspaces/WORKSPACE/destinations/DESTINATION:check` | `result`, a `CheckResult` |
| `PipelineService.PreflightPipeline` | `POST /v1/workspaces/WORKSPACE/pipelines/PIPELINE:preflight` | `result`, a `PreflightResult` |

A check runs on the data plane in the request's `data_plane`, or on the default data plane if it is empty. A preflight runs on the pipeline's data plane. `openetl check --format json` and `openetl preflight --format json` print the `result` as the API sends it. [JSON output](../reference/pipeline-cli.md#json-output) shows an example.

A `CheckResult` has `ok`, `code`, `message`, the checked resource's `version`, and `items`.

A `PreflightResult` has `ok`, the pipeline `version`, and `parts`. The parts follow this execution order:

1. The source connection check.
2. The destination connection check.
3. The sync plan.
4. The source connector's preflight.
5. The destination connector's preflight.

Identify a part by its `role` and `stage`. [Pipeline CLI commands](../reference/pipeline-cli.md#check-connections-preflight-pipelines-and-discover-streams) describes skipping rules and time limits.

Each `PreflightPart` has these fields:

- `role`: `source`, `destination` or `sync`.
- `stage`: `check`, `preflight` or `plan`.
- `resource`: the resource that the part concerns.
- `version`: the responding connector's digest. Built-in connectors and the plan have an empty version.
- `ok`, `code`, `message`, `skipped` and `items`: the result fields described below.

Read `ok`, `code`, `message` and `skipped` as follows:

| Field | `CheckResult` | `PreflightPart` | `PreflightResult` |
|---|---|---|---|
| `ok` | True only if `code` is empty and no item failed | True only if `code` and `skipped` are empty and no item failed | True only if every part is ok. A skipped part makes it false. |
| `code` | An error code, such as `AUTH_INVALID`. If the connector's answer has no code and an item failed, it is the first failed item's code, or `INTERNAL` if that item has none. | As for `CheckResult` | Not present |
| `message` | What failed. If the code came from an item, it is `title: message: detail` of that item, with the placeholders filled. | As for `CheckResult` | Not present |
| `skipped` | Not present | Why the part did not run, because a part before it failed: `the source didn't start`, `the source's check failed`, `the destination didn't start`, `the destination's check failed` or `a sync can't be planned`. Empty if it ran. | Not present |

A result or a part can fail with no failed item. For example, a connector can report one error and no items, a part's connector can fail to start or answer, or a part can run out of time. Then `code` and `message` say why. A part that runs out of time has the code `TRANSIENT` and a message such as `the source's preflight didn't finish within 2m0s`.

Each item in `items` is a `CheckItem`:

| Field | What it holds |
|---|---|
| `name` | The item's identifier, unique in the result and the same from one run to the next, such as `net.connect`. |
| `title` | What was checked. A template. |
| `status` | `passed`, `warning`, `failed` or `skipped`. Count any other value as `failed`. |
| `message` | A template. What the connector found for a passed item, what is wrong for a failed or warning item, and why it was not checked for a skipped item. |
| `code` | For a failed or warning item: an error code, such as `PERMISSION_DENIED`. |
| `fix` | For a failed or warning item: what to do. A template. |
| `field` | The config field the item concerns, as a dotted path. Empty if none. |
| `kind` | What the item verifies: `configuration`, `connection`, `authentication`, `permission`, `resource`, `compatibility`, `quota` or `custom`. Empty if the connector did not say. |
| `description` | What the item verifies, and why the connector needs it. A template. |
| `reason` | For a failed or warning item: the kind of failure, in lowercase words joined by `_`, such as `connection_refused`. |
| `params` | The values of the templates' placeholders, by name. |
| `detail` | For a failed or warning item: the cause, in the words of the upstream system. It is not a template, and it can have more than one line. |
| `duration` | How long the item's check ran. Not present if it did not run. |

A warning item does not fail its result or its part. The API sends `title`, `message`, `fix` and `description` as the connector sent them, and does not fill them. To show them, fill each placeholder:

- A placeholder is `{name}`, where `name` is a lowercase letter followed by lowercase letters, digits and `_`: `[a-z][a-z0-9_]*`.
- Replace a placeholder with the value of `name` in `params`. Keep a placeholder without a param as written.
- Do not scan a value for placeholders again. Text without placeholders is plain text.

For example, the title `Create tables in {schema}` with the params `{"schema": "public"}` shows as `Create tables in public`. Item text comes from the connector. Before you show it in a terminal, remove its control characters.

A failed check or preflight is a result, not an error. A call that could not run returns an error, and no result. Its message starts with `the check couldn't run`, or `the preflight couldn't run` for a preflight:

| Code | When | Example message |
|---|---|---|
| `Unavailable` | No runner of the data plane took the operation within 1 minute. | `the check couldn't run: no runner of data plane "dp1" took it within 1m0s` |
| `Unavailable` | The operation failed with `TRANSIENT`, `RATE_LIMITED`, `DESTINATION_UNAVAILABLE` or `INTERNAL`, it was stopped, or it failed in another way. | `the check couldn't run: INTERNAL: the source didn't start: exit status 1` |
| `InvalidArgument` | The operation failed with `CONFIG_INVALID`. | `the check couldn't run: CONFIG_INVALID: no host` |
| `FailedPrecondition` | The operation failed with another code. | `the check couldn't run: PERMISSION_DENIED: denied` |

A check also could not run when its connector does not start, or does not answer within 1 minute, such as `the check couldn't run: TRANSIENT: the destination's check didn't answer within 1m0s`. In a preflight, such a connector fails its part instead. A preflight could not run when the plan part passed and the source's catalog then could not be refreshed: `the preflight couldn't run: the source's catalog couldn't be refreshed: CODE: MESSAGE`. An error says nothing about the connection or the pipeline. Run the call again when its cause is fixed.
