---
title: Monitor an installation
description: Export metrics and traces, interpret pipeline and placement signals, and choose alerts for stalled data delivery.
---

Use pipeline freshness to track delivery, source lag to track backlog, and placement metrics to track jobs waiting for room. Logs and traces explain individual failures. The console's [Incidents](../use/index.md#incidents) records conditions that need attention; [Troubleshoot a pipeline](../pipelines/troubleshooting.md) connects symptoms to recovery steps.

## Export telemetry

`openetld` exports nothing until an exporter is configured. Send telemetry to your own OpenTelemetry collector by setting this on servers, orchestrators and runners:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

To serve metrics for a Prometheus scraper instead:

```sh
export OTEL_METRICS_EXPORTER=prometheus
export OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0
export OTEL_EXPORTER_PROMETHEUS_PORT=9464
```

The [Kubernetes manifests](./kubernetes.md) already configure this endpoint and annotate pods for scraping. Expose or scrape each component's metrics address; the API's `/healthz` is a process health check, not a promise that pipelines are fresh.

Each process reports service name `openetld`, its role as `openetl.role`, and its data plane as `openetl.data_plane`. Job metrics include pipeline and job identifiers; check the exported labels when joining metrics from different components.

## Pipeline signals

These are OpenTelemetry instrument names; Prometheus replaces dots with underscores and adds unit or counter suffixes. For example, the freshness gauge is `openetl_pipeline_freshness_seconds`.

| Instrument | Unit | Meaning |
|---|---|---|
| `openetl.pipeline.freshness` | seconds | Time since the pipeline's latest commit. The API reads its checkpoint mirror every 15 seconds. Pipelines with no commit in the last 30 days are absent. |
| `openetl.job.rows.committed` | rows, counter | Rows in committed epochs, including quarantined rows. It is not a count of usable destination rows. |
| `openetl.job.rows.quarantined` | rows, counter | Rows held back in committed epochs. Compare its rate with the committed-row rate. |
| `openetl.job.lag` | seconds | Time lag last reported by the source. |
| `openetl.job.lag.bytes`, `openetl.job.lag.records` | bytes or records | Unacknowledged backlog last reported by the source. |
| `openetl.job.retention_left` | seconds | Time left before the source discards unread data, when reported. |
| `openetl.job.retention_left.bytes` | bytes | Additional retained data available before unread data is discarded, when reported. |
| `openetl.job.commit.duration` | seconds, histogram | Time spent writing a checkpoint, applying schemas and committing at the destination. |
| `openetl.job.hold.age` | seconds | Age of an open hold, updated at commits. |

Source progress gauges are present only while a job reports them. Missing lag is not zero lag. Freshness is time since commit rather than the age of source records; use it alongside the expected schedule and source-reported lag. A pipeline that has never committed needs a separate check in the console or API, because the freshness series is absent.

## Placement signals

Orchestrators export the same placement view, refreshed every 10 seconds. Use `max`, rather than summing replicas, when aggregating that view.

| Instrument | Unit | Meaning |
|---|---|---|
| `openetl.placement.cpu.capacity` | vCPUs | CPU available from runners currently reporting. |
| `openetl.placement.memory.capacity` | bytes | Memory available from those runners. |
| `openetl.placement.cpu.reserved`, `openetl.placement.memory.reserved` | vCPUs or bytes | Resources of placed or running attempts. |
| `openetl.placement.cpu.pending`, `openetl.placement.memory.pending` | vCPUs or bytes | Resources requested by attempts waiting for room. |
| `openetl.placement.pending.age` | seconds | Age of the oldest attempt waiting for room. |

Capacity is labeled by `data_plane` and `pool`. Demand also has `size` and `runs_on` (`runner` or `pod`). A job waiting for a pod should not drive a shared-runner scaling decision. The [KEDA guide](./kubernetes.md#runners-with-keda) has queries that account for those labels and duplicate orchestrator views.

## Choose alerts

Choose thresholds from each pipeline's schedule, commit interval and delivery deadline. A two-hour freshness limit might fit an hourly pipeline, while a continuous pipeline needs a limit based on its own latency target. These are starting points to adapt, rather than built-in alert rules:

| Condition | Investigate |
|---|---|
| Freshness exceeds the delivery deadline | Paused pipelines, failed syncs, missing runners, or a stalled commit. |
| Pending age keeps growing | Pool mismatch, insufficient capacity, pod limits or Kubernetes scheduling. |
| Lag grows while retention left shrinks | Read throughput, source throttling and destination commit throughput. |
| Quarantine rate rises or hold age grows | Invalid values, schema policy and a pending schema repair. |
| Metrics disappear | Scrape/exporter health and component availability, then whether the pipeline is running. |

Freshness, backlog and capacity answer different questions. Keep separate alerts for telemetry availability and data delivery. Use [events and webhooks](../integrate/index.md#events-and-webhooks) for incident notifications; receivers should deduplicate deliveries and verify signatures.

## Follow a failure

Start with its pipeline and sync IDs in the console or `openetl syncs PIPELINE`, then read `openetl logs --sync SYNC_ID PIPELINE`. Look at the matching runner's JSON logs and traces for recovery and commits. For jobs that never reached a runner, inspect orchestrator placement and runner registration logs. [openetld settings](../reference/openetld.md#logs) lists log destinations and rotation settings.
