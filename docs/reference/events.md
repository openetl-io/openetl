---
title: Platform events
description: Every platform event's type, and every field of its data.
---

The body of a webhook's delivery of one platform event, laid out as Standard Webhooks says. Resources are named as the API names them, such as `workspaces/default/pipelines/shop`.

[Webhooks](../integrate/index.md#events-and-webhooks) deliver the events you choose, each as one `POST` with this JSON body. `type` says which section below applies to `data`.

This page is generated from [`internal/events/event.schema.json`](../../internal/events/event.schema.json). The schema allows fields it doesn't list, so ignore any you don't know.

## Top-level fields

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | What happened. Is one of `sync.started`, `sync.succeeded`, `sync.failed`, `sync.stopped`, `pipeline.created`, `pipeline.updated`, `pipeline.deleted`, `pipeline.teardown_failed`, `pipeline.paused`, `pipeline.resumed`, `pipeline.lagging`, `pipeline.recovered`, `pipeline.rescaled`, `pipeline.overridden`, `stream.paused`, `stream.resumed`, `pipeline.reset`, `schema.changed`, `schema.blocked`, `incident.opened`, `incident.escalated`, `incident.resolved` or `webhook.test`. |
| `timestamp` | string | Yes | When the event was recorded, in RFC 3339. |
| `data` | object | Yes | The event's details. What it holds depends on `type`, as the sections below say. |

### When `type` is `sync.started`

A sync, or a continuous pipeline's run, started.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`sync`](#sync) | Yes | The sync. |

### When `type` is `sync.succeeded`

A sync, or a continuous pipeline's run, ended without an error.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`sync`](#sync) | Yes | The sync. |

### When `type` is `sync.failed`

A sync, or a continuous pipeline's run, failed. `error` says why, and `code` is the error's code if it has one.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`sync`](#sync) | Yes | The sync. |

### When `type` is `sync.stopped`

A sync, or a continuous pipeline's run, was stopped before it ended.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`sync`](#sync) | Yes | The sync. |

### When `type` is `pipeline.created`

A pipeline was applied for the first time.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline's first version, with `provenance`. |

### When `type` is `pipeline.updated`

A new version of a pipeline was applied.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The new version, with `previousVersion` and `provenance`. |

### When `type` is `pipeline.deleted`

A pipeline was deleted.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, and its last version. |

### When `type` is `pipeline.teardown_failed`

What a deleted pipeline's connector or Flink cluster kept for it couldn't be removed after 7 days of trying, and is left for someone to remove.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, by its name alone, with `connector` and `error`. |

### When `type` is `pipeline.paused`

A pipeline was paused, by someone or by a playbook.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, with its `version` if someone paused it, or the `incident` if a playbook did. |

### When `type` is `pipeline.resumed`

A pipeline was resumed.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, and its version. |

### When `type` is `pipeline.lagging`

A continuous pipeline's read fell further behind its source than its threshold.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`lag`](#lag) | Yes | How far behind it is. |

### When `type` is `pipeline.recovered`

A continuous pipeline that was lagging caught up, to within half its threshold.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`lag`](#lag) | Yes | How far behind it is now, and since when it had been lagging. |

### When `type` is `pipeline.rescaled`

A pipeline's jobs were rescaled, through the API or by the job loop. A running job stops at a checkpoint and starts again at the new size.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`rescale`](#rescale) | Yes | The rescale. |

### When `type` is `pipeline.overridden`

Heal set an override of a field of the pipeline's spec, or someone removed one.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, with `field` and `value`, and the `incident` if a playbook set it. |

### When `type` is `stream.paused`

One of a pipeline's streams was paused on its own, by someone or by a playbook.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, with `stream`, and the `incident` if a playbook paused it. |

### When `type` is `stream.resumed`

One of a pipeline's streams was resumed.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline, with `stream`. |

### When `type` is `pipeline.reset`

A pipeline's streams are to be read again from the start.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`pipeline`](#pipeline) | Yes | The pipeline and its version, with `streams`, or none for every stream. |

### When `type` is `schema.changed`

A stream's schema has a new version.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`schemaChange`](#schemachange) | Yes | The new schema, and how it differs from the one before. |

### When `type` is `schema.blocked`

A sync or run failed because of a schema change it can't take. It comes with that sync's `sync.failed`.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`sync`](#sync) | Yes | The sync, with its `error`, and `code` `SCHEMA_DRIFT`. |

### When `type` is `incident.opened`

An incident opened.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`incident`](#incident) | Yes | The incident. |

### When `type` is `incident.escalated`

An open incident became critical.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`incident`](#incident) | Yes | The incident. |

### When `type` is `incident.resolved`

An incident was resolved.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`incident`](#incident) | Yes | The incident, with `resolveTime` and `resolution`. |

### When `type` is `webhook.test`

Sent to one webhook when it's tested, such as by `openetl webhook test`.

| Field | Type | Required | Description |
|---|---|---|---|
| `data` | [`webhookTest`](#webhooktest) | Yes | The webhook. |

## Types

Fields above refer to these types by name.

### `sync`

A sync, or a continuous pipeline's run.

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline` | string | Yes | The pipeline's resource name. |
| `sync` | string | Yes | The sync's resource name. |
| `version` | string | Yes | The version of the pipeline it ran. |
| `continuous` | boolean | Yes | Whether it's a continuous pipeline's run, not a scheduled sync. |
| `startTime` | string | Yes | When it started, in RFC 3339. |
| `endTime` | string | No | When it ended, in RFC 3339. Left out until it has. |
| `error` | string | No | What it failed with. Left out if it didn't. |
| `code` | string | No | The error's code, such as `SCHEMA_DRIFT`. Left out if it has none. |

### `pipeline`

A pipeline, and what changed about it.

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline` | string | Yes | The pipeline's resource name. In `pipeline.teardown_failed`, its name alone, as the pipeline no longer exists. |
| `version` | string | No | The pipeline's version. |
| `previousVersion` | string | No | The version this one replaces. |
| `provenance` | string | No | Where the version came from, such as `cli:shop.yaml#3`. |
| `incident` | string | No | The resource name of the incident a playbook paused the pipeline or stream, or overrode its spec, for. |
| `stream` | string | No | The stream paused or resumed. |
| `connector` | string | No | The source, destination or Flink cluster a failed teardown was for. |
| `error` | string | No | What the teardown last failed with. |
| `field` | string | No | The field of the spec that's overridden. |
| `value` | string | No | The override's value, as JSON. Left out once it's removed. |
| `streams` | array of string | No | The streams a reset reads again. Left out for every stream. |

### `lag`

How far a continuous pipeline's read is behind its source.

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline` | string | Yes | The pipeline's resource name. |
| `sync` | string | Yes | The run's resource name. |
| `lagSeconds` | number | Yes | How far behind the read is, in seconds. |
| `thresholdSeconds` | number | Yes | How far behind counts as lagging, in seconds. |
| `since` | string | Yes | When it started lagging, in RFC 3339. |
| `stream` | string | No | The stream furthest behind. Left out for the whole read. |

### `rescale`

A pipeline's jobs rescaled.

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline` | string | Yes | The pipeline's resource name. |
| `version` | string | Yes | The pipeline's version. |
| `by` | string | Yes | What rescaled it. Is one of `api` or `autoscaler`. |
| `size` | string | No | The size the API set, such as `l` or `3 vCPU, 6 GiB`. Left out when the job loop rescaled it, or the API gave the spec back its say. |
| `lanes` | integer | No | The lanes the job loop set. |
| `fromLanes` | integer | No | The lanes of the job it rescaled. |

### `schemaChange`

A stream's new schema.

| Field | Type | Required | Description |
|---|---|---|---|
| `pipeline` | string | Yes | The pipeline's resource name. |
| `stream` | string | Yes | The stream. |
| `output` | boolean | No | Whether the stream is one of a transform's outputs. |
| `version` | integer | Yes | The schema's version. |
| `table` | string | Yes | The table it's written to. |
| `columns` | array of [object](#schemachangecolumns) | Yes | Every column of the new schema. |
| `added` | array of string | No | The names of the columns added. |
| `removed` | array of string | No | The names of the columns removed. |
| `changed` | array of string | No | The names of the columns whose type or nullability changed. |

### `schemaChange.columns[]`

A column of a stream's schema.

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | The column's name. |
| `type` | string | Yes | The column's type. |
| `nullable` | boolean | Yes | Whether it can be null. |

### `incident`

An incident.

| Field | Type | Required | Description |
|---|---|---|---|
| `incident` | string | Yes | The incident's resource name. |
| `pipeline` | string | Yes | The pipeline's resource name. |
| `stream` | string | No | The stream it's about. Left out for the whole pipeline. |
| `class` | string | Yes | What kind of incident it is, such as `auth` or `crash_loop`. |
| `code` | string | No | The error's code, if it came from one. |
| `severity` | string | Yes | How serious it is. Is one of `warning` or `critical`. |
| `summary` | string | Yes | One line about it. |
| `message` | string | Yes | What happened. |
| `nextSteps` | string | No | What to do about it. |
| `fingerprint` | string | Yes | What identifies the fault across pipelines. |
| `occurrences` | integer | Yes | How many times it has happened. |
| `details` | map of names to string | No | More about it, by name. |
| `sync` | string | No | The resource name of the sync it came from. |
| `firstSeenTime` | string | Yes | When it first happened, in RFC 3339. |
| `lastSeenTime` | string | Yes | When it last happened, in RFC 3339. |
| `resolveTime` | string | No | When it was resolved, in RFC 3339. Left out until it is. |
| `resolution` | string | No | How it was resolved. |

### `webhookTest`

A test delivery.

| Field | Type | Required | Description |
|---|---|---|---|
| `webhook` | string | Yes | The webhook's resource name. |
