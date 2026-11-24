---
title: Monitor an installation
description: Export metrics and traces, interpret pipeline and placement signals, and choose alerts for stalled data delivery.
---

Use pipeline freshness to track delivery, source lag to track backlog, and placement metrics to track jobs waiting for room. Logs and traces explain individual failures. The console's [Incidents](../use/incidents.md#incidents) records conditions that need attention; [Troubleshoot a pipeline](../pipelines/troubleshooting.md) connects symptoms to recovery steps.

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
| `openetl.job.rows.quarantined` | rows, counter | Rows quarantined in committed epochs: not written, and kept aside with the reason. Compare its rate with the committed-row rate. |
| `openetl.job.cells.held` | cells, counter | Cells held back in committed epochs, by stream and column, from rows that were written. |
| `openetl.job.lag` | seconds | Time lag last reported by the source. |
| `openetl.job.lag.bytes`, `openetl.job.lag.records` | bytes or records | Unacknowledged backlog last reported by the source. |
| `openetl.job.retention_left` | seconds | Time left before the source discards unread data, when reported. |
| `openetl.job.retention_left.bytes` | bytes | Additional retained data available before unread data is discarded, when reported. |
| `openetl.job.commit.duration` | seconds, histogram | Time spent writing a checkpoint, applying schemas and committing at the destination. |
| `openetl.job.hold.age` | seconds | Age of an open hold, updated at commits. |
| `openetl.job.watermark` | seconds since the Unix epoch | For a stream with `eventTime` in a SQL transform: the least watermark of the lanes of a job's runner, or of each worker. It is absent until the stream has a watermark, and is not updated after the stream's input ends. |

Source progress gauges are present only while a job reports them. Missing lag is not zero lag. Freshness is time since commit rather than the age of source records; use it alongside the expected schedule and source-reported lag. A pipeline that has never committed needs a separate check in the console or API, because the freshness series is absent.

## Stateful operators

A SQL transform's group window, `OVER` or dedupe is a stateful operator: it keeps state between rows ([Aggregate by windows of time](../pipelines/windows.md), [Compute running totals with OVER](../pipelines/over.md), [Deduplicate rows with ROW_NUMBER](../pipelines/dedupe.md)). The runner that commits a job records these metrics at each commit, from every worker's lanes of the operator, so a job on several pods reports each number once. Each has the attributes `openetl.org`, `openetl.pipeline` and `openetl.operator`, the operator's ID as `openetl plan` shows it. Counters count committed epochs only, as `openetl.job.rows.committed` does. Gauges hold the values at the operator's last commit.

| Instrument | Unit | Meaning |
|---|---|---|
| `openetl.operator.rows.in` | rows, counter | Rows of the statement's stream that reached the operator. |
| `openetl.operator.rows.written` | rows, counter | Rows the operator wrote to its output, by `openetl.op`: `insert`, `update` or `delete`. Rows that the output quarantined are not counted. |
| `openetl.operator.rows.late` | rows, counter | Rows the operator quarantined with the code `late`, as they came after their windows' allowed lateness, or after their windows had gone under an earlier lateness. |
| `openetl.operator.rows.repeats` | rows, counter | Rows a dedupe dropped: repeats of a key it keeps, and for a dedupe that keeps each key's latest row, rows no later than the key's latest. For a group window or `OVER`, the rows that a read gave again, which it had taken: of one record ID and the same values. |
| `openetl.operator.windows` | windows | Windows, or sessions, whose state the operator keeps: open, or closed and within their lateness. For a dedupe, the keys it keeps. A first-row dedupe with a retention counts keys forgotten by buckets of their first rows' times, a sixteenth of the retention wide and at least a second, once the watermark passes a bucket's end plus the retention. A reset, or a version that changes the operator, starts it again from 0. |
| `openetl.operator.watermark` | seconds since the Unix epoch | The operator's watermark: the least of its lanes'. It is absent until every lane has one, and is not updated at the end of input. |
| `openetl.operator.watermark.lag` | seconds | How far the watermark was behind the time of the commit. |
| `openetl.operator.state.memory` | bytes | What the operator's state held in memory at the end of the epoch. It counts the epoch's changes, the changes that the checkpoint record holds, and the indexes and filters of the state files that the operator opened. It also counts the hashes of the windows' keys in the files it names, what its merges hold, and its block cache. |
| `openetl.operator.state.checkpoint` | bytes | The bytes of the operator's state that the checkpoint record holds: what each lane changed since it last wrote a state file. A lane writes its changes as a file when they become larger than its share of the record, which is at most 1 MiB for all of a pipeline's operators. |
| `openetl.operator.state.stored` | bytes | The bytes on object storage of the state files that the checkpoint names for the operator. A checkpoint names a file while any of the state in it is still in use, and this counts the whole file. A checkpoint names none after every window has closed and its lateness has passed. Retention deletes the rest. |
| `openetl.operator.state.requests` | requests, counter | The operator's requests to object storage, by `openetl.request.kind`. `barrier` writes its state at a checkpoint. It also counts the files that a lane writes before the checkpoint when its changes pass its part of the job's memory. It counts each epoch's own writes. It counts none in an epoch whose changes the checkpoint record holds, or whose changes left nothing in use. `lookup` reads blocks of the state. `restore` opens the files that a checkpoint names. After a restart, it also reads their windows' values whole at the first checkpoint that writes state. `merge_write` writes, at a checkpoint, the files that merge other files. `merge_read` reads the files that a merge merges. A `lookup`, `restore` or `merge_read` made near a checkpoint can count with the epoch next to it. Each request counts once. |
| `openetl.operator.state.request.bytes` | bytes, counter | Those requests' bytes, by `openetl.request.kind`. |
| `openetl.operator.state.disk.reads` | reads, counter | The operator's reads of state that the runner's disk cache answered, in place of a `lookup`, `restore` or `merge_read` request ([Scratch disk](./index.md#scratch-disk)). They are not requests to object storage. A read made near a checkpoint can count with the epoch next to it. |
| `openetl.operator.state.disk.read.bytes` | bytes, counter | Those reads' bytes. |

The console's **SQL** tab shows the same numbers for the last checkpoint ([SQL](../use/sql.md#sql)).

### OVER and dedupe

The same metrics describe `OVER` and dedupe, with these meanings:

| Instrument | `OVER` | First-row dedupe | Latest-row dedupe |
|---|---|---|---|
| `openetl.operator.rows.in` | Rows of the stream | Rows of the stream | Rows of the stream |
| `openetl.operator.rows.written` | One insert for each row, once the watermark passes its time, but rows that the output quarantined | One insert for each key's first row, and again after each retention, but rows that the output quarantined | An insert for each new key, an update for each later row of a key, and a delete where a filter after the dedupe drops a key's latest row, but rows that the output quarantined. Over a stream read with `full_refresh`, each sync writes again an upsert, an insert or an update, of every key's latest row that passes the filter, and a delete of every key whose latest row it drops. |
| `openetl.operator.rows.repeats` | Rows that a read gave again while their copies waited, which it dropped | Rows of a key that it keeps, which it dropped | Rows no later than their key's latest, which it dropped |
| `openetl.operator.rows.late` | Rows that came after the watermark had passed their time | Rows of a key that it does not keep, which came after the watermark had passed their time | 0 |
| `openetl.operator.windows` | 0 | The keys that it keeps | The keys that it keeps |
| `openetl.operator.watermark.lag` | How far the watermark was behind the time of the commit. Rows wait until the watermark passes their time. | As for `OVER`, for the rows of keys that it does not keep | As for `OVER`. Rows do not wait for the watermark. |
| `openetl.operator.state.memory` | The partitions' frames, and the rows that wait | The kept keys, and the rows that wait | The kept keys |

Alert on these:

- An increase of `openetl.operator.rows.late` over a recent interval, such as the last 15 minutes, for `OVER` or a first-row dedupe. The engine quarantined those rows with the code `late`. Set a longer `watermark.delay` on the stream. The counter does not go down, so an alert on its total never clears.
- `openetl.operator.watermark.lag` that keeps growing. A split is behind or holds the watermark back. Rows of `OVER` and of a first-row dedupe wait until the watermark passes their time, so they are written later.
- `openetl.operator.state.memory` that nears half of the job's memory, as a warning. The run fails with `RESOURCE_EXHAUSTED` when the state holds more than that and the engine is also near its memory limit. Set a shorter `dedupe.retention` for a first-row dedupe, or a larger `resources.size`.

A rise in `openetl.operator.rows.repeats` is not an error. It can mean that a producer sends rows again, or that a pipeline read rows again after a failure.

## Placement signals

Orchestrators export the same placement view, refreshed every 10 seconds. Use `max`, rather than summing replicas, when aggregating that view.

| Instrument | Unit | Meaning |
|---|---|---|
| `openetl.placement.cpu.capacity` | vCPUs | CPU available from runners currently reporting. |
| `openetl.placement.memory.capacity` | bytes | Memory available from those runners. |
| `openetl.placement.cpu.reserved`, `openetl.placement.memory.reserved` | vCPUs or bytes | Resources of placed or running attempts. |
| `openetl.placement.cpu.pending`, `openetl.placement.memory.pending` | vCPUs or bytes | Resources requested by attempts waiting for room. |
| `openetl.placement.pending.age` | seconds | Age of the oldest attempt waiting for room. |

Capacity is labeled by `data_plane` and `pool`. Demand also has `size` and `runs_on` (`runner` or `pod`). A job waiting for a pod should not drive a shared-runner scaling decision. A job on [several pods](./kubernetes.md#jobs-on-several-pods) counts under `runs_on="pod"` and its workers' `size`, with its workers' and its coordinator's sizes added together. The [KEDA guide](./kubernetes.md#runners-with-keda) has queries that account for those labels and duplicate orchestrator views.

## Choose alerts

Choose thresholds from each pipeline's schedule, commit interval and delivery deadline. A two-hour freshness limit might fit an hourly pipeline, while a continuous pipeline needs a limit based on its own latency target. These are starting points to adapt, rather than built-in alert rules:

| Condition | Investigate |
|---|---|
| Freshness exceeds the delivery deadline | Paused pipelines, failed syncs, missing runners, or a stalled commit. |
| Pending age keeps growing | Pool mismatch, insufficient capacity, pod limits or Kubernetes scheduling. |
| Lag grows while retention left shrinks | Read throughput, source throttling and destination commit throughput. |
| Quarantine rate rises or hold age grows | Invalid values, schema policy and a pending schema repair. |
| Metrics disappear | Scrape/exporter health and component availability, then whether the pipeline is running. |

Freshness, backlog and capacity answer different questions. Keep separate alerts for telemetry availability and data delivery. Use [events and webhooks](../integrate/webhooks.md#events-and-webhooks) for incident notifications; receivers should deduplicate deliveries and verify signatures.

If the quarantine rate rises, run `openetl quarantine --since 1h PIPELINE`. It lists the files of quarantined rows, and adds up their rows by stream, column and code. It lists the newest 100 files, and its sums count only those files. If it says that there are more, add `--limit` with a larger number.

A webhook for `stream.quarantined` gets one event for each stream that quarantined rows in a commit. The event has that stream's files and counts, not the commit's totals. The metric and the event never carry the values of a row.

The files stay in the runners' `--checkpoints` store. If the pipeline does not set `quarantine.retention`, OpenETL keeps them until you delete them. If it does, a run deletes the files of committed epochs that are older than that period. The run does this when it starts, and every hour while it runs. So a pipeline that does not run keeps its files until its next run. [Quarantine files](../reference/quarantine-files.md) gives their format, and an object store rule that deletes them.

## Follow a failure

Start with its pipeline and sync IDs in the console or `openetl syncs PIPELINE`, then read `openetl logs --sync SYNC_ID PIPELINE`. Look at the matching runner's JSON logs and traces for recovery and commits. For jobs that never reached a runner, inspect orchestrator placement and runner registration logs. [openetld settings](../reference/openetld.md#logs) lists log destinations and rotation settings.
