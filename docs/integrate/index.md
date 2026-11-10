---
title: Integrate
description: Connect OpenETL to dbt, Spark, Flink, your own code and your monitoring, and read what it writes.
---

This section is for people who know another system well and want it to work with OpenETL. Each part says what OpenETL sends or expects, and shows the least setup that works. The YAML documents here are applied like any other, with `openetl plan` and `openetl apply`; [Pipelines](../pipelines/index.md) covers that loop.

| Your task | Integration | When it runs |
|---|---|---|
| Build or test models against the loaded Postgres tables | [dbt](./dbt.md) | After a scheduled sync commits. |
| Transform batches before they reach the destination | [Spark transform](./spark.md) | Once per epoch of a scheduled sync. |
| Run an application against completed destination tables | [Spark post-sync step](./spark.md#run-a-spark-application-after-a-sync) | After a scheduled sync commits. |
| Apply a streaming job with checkpointed state | [Flink](./flink.md) | During a continuous pipeline's run. |

## dbt after a sync

Use a dbt step to build or test models after a scheduled sync commits its copied data. The runner executes the project against the pipeline's Postgres destination. A failed step fails the sync but leaves the copied data committed.

[Run dbt after a sync](./dbt.md) covers runner installation, project packaging, generated profiles, schema selection, verification and recovery. The guide uses the complete [dbt example](../../examples/faker-to-postgres-dbt.yaml).

## Transforms on Spark

Use Spark to transform each epoch of a scheduled sync before the destination receives it. Your application returns one DataFrame per output stream. Each call sees one epoch, so cross-stream joins and aggregates do not automatically cover the entire sync.

Use a Spark post-sync step when the application needs to read the completed destination tables. Both operations run on a cluster that you provide.

[Transform data with Spark](./spark.md) covers building a Java application, Python support, cluster submission, shared storage, output contracts and failure diagnosis. Start with the buildable [Spark example project](../../examples/spark-transform) and its [pipeline document](../../examples/faker-spark-to-parquet.yaml).

## Transforms on Flink

Use Flink to transform a continuous pipeline. The job exchanges rows with its runner through an authenticated bridge. OpenETL coordinates Flink checkpoints with destination commits. Inputs must replay exactly from a saved position.

[Transform a stream with Flink](./flink.md) covers building the job, session-cluster requirements, bridge routing, storage, row types and state recovery. Start with the buildable [Flink example project](../../examples/flink-transform) and its [pipeline document](../../examples/faker-flink-to-parquet.yaml).

## Events and webhooks

OpenETL records an event when something happens to a pipeline, and sends the events you choose to an HTTP endpoint of yours. [`examples/webhook.yaml`](../../examples/webhook.yaml):

```yaml
apiVersion: openetl.io/v1alpha1
kind: Webhook
metadata:
  name: ops-alerts
spec:
  url: https://hooks.example.com/openetl
  events: ["sync.failed", "schema.*", "pipeline.lagging", "pipeline.recovered"]
  pipelines: [faker-to-postgres]
  secrets: [ops-alerts-key]
```

Each secret the webhook names holds a signing key. Make one before applying the webhook, then send a test event:

```sh
echo "whsec_$(openssl rand -base64 32)" | openetl secret set ops-alerts-key
openetl apply -f examples/webhook.yaml
openetl webhook test ops-alerts
```

- **Which events.** `events` takes event types, or globs of them such as `sync.*` or `*`. `pipelines` limits them to those pipelines; left out, the webhook gets every pipeline's events and the workspace's own.

  | Events | When |
  |---|---|
  | `sync.started`, `sync.succeeded`, `sync.failed`, `sync.stopped` | A sync, or a continuous pipeline's run, starts or ends |
  | `pipeline.created`, `pipeline.updated`, `pipeline.deleted` | A pipeline is applied or deleted |
  | `pipeline.paused`, `pipeline.resumed`, `pipeline.rescaled` | Someone pauses, resumes or rescales a pipeline |
  | `stream.paused`, `stream.resumed` | Someone, or heal, pauses or resumes one of a pipeline's streams |
  | `pipeline.overridden` | Heal changes how a pipeline runs, such as syncing less often, or someone undoes it |
  | `pipeline.reset` | Someone asks for a pipeline's streams to be read again from the start; `data.streams` names them, and is left out for every stream |
  | `pipeline.lagging`, `pipeline.recovered` | A continuous pipeline falls behind its source, and catches up |
  | `pipeline.teardown_failed` | What a deleted pipeline's connector keeps for it, such as a replication slot, couldn't be removed after 7 days of trying, and is left for someone to remove |
  | `schema.changed`, `schema.blocked` | A stream's schema changes, or a change can't be taken |
  | `incident.opened`, `incident.escalated`, `incident.resolved` | An incident opens, gets worse, or is resolved |
  | `webhook.test` | `openetl webhook test` sends one |

- **What's sent.** A `POST` with a JSON body, laid out as [Standard Webhooks](https://www.standardwebhooks.com) says: `type`, `timestamp`, and the event's `data`, which names resources as the API does, such as `workspaces/default/pipelines/faker-to-postgres`. [Platform events](../reference/events.md) lists every field of each type's `data`.
- **Signatures.** Each delivery carries the headers `webhook-id`, the event's ID, `webhook-timestamp` and `webhook-signature`, an HMAC-SHA256 under each of the webhook's keys. Any Standard Webhooks library verifies them. Naming two secrets lets you add a new key before removing the old one.
- **Retries.** A `2xx` answer within 15 seconds is a delivery. No answer, a `408`, a `429` or a `5xx` is tried again for about a day, honoring `Retry-After`; another status fails the delivery. Redirects aren't followed. A delivery can arrive more than once, so drop repeats by `webhook-id`. An endpoint that answers `410 Gone`, or fails every delivery for 3 days with no day without a failure, has its webhook disabled; `openetl webhook enable` turns it back on.
- **Deliveries.** `openetl webhook deliveries ops-alerts` lists the last 7 days' deliveries, with each one's state, status and error.
- **Which addresses.** The server sends webhooks to public addresses only, unless its `--egress-allow` setting names the private networks it may reach as well.

The [Webhook reference](../reference/specs/webhook.md) lists required fields and filter defaults.

[Receive and verify webhooks](./webhooks.md) provides signature verification code, receiver checks, delivery recovery and signing-key rotation steps.

## The API

Everything the CLI and the console do goes through OpenETL's API, served by `openetld server`. The same API is served four ways:

- **REST**, with JSON, under `/v1`, such as `GET /v1/workspaces/default/pipelines`. The server serves its OpenAPI description at `/openapi.yaml`.
- **Connect, gRPC and gRPC-Web**, from the protos in [`proto/openetl/api/v1`](../../proto/openetl/api/v1), such as `openetl.api.v1.PipelineService/ListPipelines`.

Every call sends an API key as `Authorization: Bearer KEY`. Give each integration a service account of its own, with the least role it needs: `viewer` reads, `operator` also syncs, pauses and resumes pipelines, and `editor` also applies and deletes.

```sh
openetl principal create reporting
openetl principal grant reporting viewer
openetl key create --out reporting.key reporting
curl -H "Authorization: Bearer $(cat reporting.key)" http://localhost:8080/v1/workspaces/default/pipelines
```

Responses use the protos' JSON form, which isn't quite the YAML you apply: enums are spelled in full (`PIPELINE_MODE_SCHEDULED`) and durations in seconds (`3600s`). Sources, destinations, pipelines and the other kinds are created and changed through `ApplyService` (`POST /v1/workspaces/default:apply`), which takes the same documents `openetl apply` sends.

## Metrics and traces

`openetld` exports metrics and traces through OpenTelemetry, configured by the standard `OTEL_*` environment variables. Nothing is exported until a signal's exporter, or an OTLP endpoint, is set:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

To have Prometheus scrape each process instead, serve its metrics on a port:

```sh
export OTEL_METRICS_EXPORTER=prometheus
export OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0
export OTEL_EXPORTER_PROMETHEUS_PORT=9464
```

The [Kubernetes manifests](../self-host/kubernetes.md) set exactly that. Every process reports as the service `openetld`, with its role in `openetl.role` and its data plane in `openetl.data_plane`. Metrics are named `openetl.*`, such as `openetl.job.rows.committed`, `openetl.job.lag` and `openetl.pipeline.freshness`. Traces cover API calls, the runners' work, and each attempt's recovery and commits.

[Monitor an installation](../self-host/monitoring.md) explains the metrics, missing-series behavior and alerts.

## Reading what OpenETL writes

Every row OpenETL writes to a table or a file carries metadata columns besides the source's:

| Column | What it holds |
|---|---|
| `_openetl_id` | The ID of this version of the record: a hash of the stream, the record's primary key (or, without one, every column), and its `_openetl_seq`. The same version gets the same ID however often it's read. |
| `_openetl_op` | `insert`, `update` or `delete`. Only streams read with CDC have updates and deletes. |
| `_openetl_seq` | Where this version comes in the source's order: its position in the source's log, its cursor value, or when the read started. A later version of a record has a higher one. |
| `_openetl_synced_at` | When OpenETL wrote it, in UTC. |
| `_openetl_deleted` | `true` for a row deleted at the source. Only in streams written with merge and soft deletes, the default, which keep a deleted row rather than removing it. |

A stream whose [delivery guarantee](../get-started/concepts.md#delivery-guarantees) is at least once can hold the same version of a record twice, with the same `_openetl_id`. Keep one of each when you read it:

```sql
select *
from (
  select *, row_number() over (partition by _openetl_id order by _openetl_synced_at) as n
  from faker.purchases
) as t
where n = 1;
```

To read the latest version of each record from a stream written with append, partition by its primary key instead, and order by `_openetl_seq` descending.

The destination connector defines the table, file or event format and how readers see committed data. Follow that connector version's documentation for catalog setup, reader compatibility and any destination-specific metadata.
