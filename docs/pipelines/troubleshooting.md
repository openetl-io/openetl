---
title: Troubleshoot a pipeline
description: Diagnose waiting jobs, failed syncs, stale data and connection errors, then verify recovery.
---

Start with the pipeline's page in the console. Check its state, latest sync, **Incidents**, and messages above **Streams**. A scheduled pipeline runs in syncs; a continuous pipeline keeps a job running. [Schedules](./schedules.md) explains when a scheduled sync should start.

For a smoke test using only built-in connectors, use [`examples/faker-to-parquet.yaml`](../../examples/faker-to-parquet.yaml). Its output path is on the runner's machine; in Compose, choose a path under `/var/lib/openetl` to keep it on the mounted runner volume.

## Collect the evidence

An operator can investigate and start syncs; a viewer can read logs and incidents. Replace `PIPELINE` with the name in the console:

```sh
openetl syncs PIPELINE
openetl logs PIPELINE
openetl incident list
openetl overrides PIPELINE
```

`logs` defaults to the latest sync. Use `openetl logs --sync SYNC_ID PIPELINE` for the failing sync, or `openetl logs -f PIPELINE` to follow the current one. A console sync's page shows the same log. An incident's **Next steps** and **Heal** explain what failed and what OpenETL changed.

Record the pipeline version, sync or job ID, attempt number, error code and time. Keep credentials out of reports. For failures before a job reaches a runner, check the orchestrator and runner process logs too; there may be no sync log yet.

## A sync waits or never starts

| Check | What to do |
|---|---|
| Is the pipeline paused? | Fix its open incident first, then resume it. A paused stream is separate from a paused pipeline. |
| Is a sync already running? | Wait for it or diagnose it. Scheduled starts are skipped while another sync runs. |
| Did heal change the schedule? | Read `openetl overrides PIPELINE` and the incident. [Schedules](./schedules.md#when-heal-slows-syncs-down) explains removing an override. |
| Are orchestrators running? | Check their database connection, selected backend and process logs. A runner alone does not schedule work. |
| Are runners connected to the right data plane and pool? | Compare the pipeline's `dataPlane` and `resources.pool` with runner settings. Verify the runner's API URL and token. |
| Does a job fit? | Compare its planned CPU and memory with runner capacity after the reserve. Check pod-launcher limits and Kubernetes pod events when using a launcher. |

Run `openetl plan -f FILE` on the pipeline's definition to inspect its job size, pool and plan problems. [Kubernetes job placement](../self-host/kubernetes.md#how-jobs-are-placed) explains why a job can wait despite idle machines. The placement metrics in [Monitoring](../self-host/monitoring.md) distinguish demand from available capacity.

## The source or destination refuses a connection

Ask a runner to check the configured connection:

```sh
openetl check source SOURCE
openetl check destination DESTINATION
```

The runner needs network access to your systems; the CLI's computer reaching them does not prove the runner can. In a container, `localhost` means that container. Check DNS, ports, TLS settings, the secret being used, and the permissions required by the selected connector version.

An `Auth` incident may have paused the pipeline. An editor can update its secret with `openetl secret set NAME`, or apply corrected connection config. Run the checks again before resuming. Platform administrators manage runner authentication separately through [runner tokens](../self-host/access.md#rotate-a-runner-token).

## A sync fails repeatedly

Read the complete error in the failing sync's log rather than only the shortened error in the sync list.

| Symptom | Next step |
|---|---|
| `CONFIG_INVALID` or plan problems | Correct the named setting, apply it, and check connector/runtime compatibility. Repeating the same sync will not fix invalid config. |
| Source rate limits | Check the source's limits and heal's overrides. Increase the interval or reduce request rate where supported. |
| Out of memory | Inspect job peaks and `resources.max`. Increase the size or maximum where the data plane has room; a larger request still needs capacity. |
| `POSITION_LOST` | Follow [Reading from the start again](./positions.md#after-a-lost-position). A reset can replay data and cannot recover history the source has discarded. |
| `CHECKPOINT_CORRUPT` | Follow [checkpoint recovery](./positions.md#when-the-latest-checkpoint-cant-be-read). Read the delivery implications before allowing a rewind. |
| Schema errors, held columns or quarantined values | Follow [Handle schema changes and holds](./schema-changes.md). |
| A dbt or Spark post-sync step fails | Fix that step. The sync can fail after its copied data has committed; inspect the step's log and [integration setup](../integrate/index.md). |

For a continuous pipeline, repeated failures without a commit can open a **Crash loop** incident. Investigate its attempts' errors and runner health before resuming. Closing an incident by hand does not repair the failure or resume a paused pipeline.

## The pipeline runs but data is stale

Separate three questions:

- **When did it last commit?** Freshness measures time since the latest commit, not when the source last changed. A scheduled pipeline's normal freshness includes its schedule interval.
- **Is it behind the source?** Source-reported lag and retention show whether unread data is accumulating. Not every source reports these metrics.
- **Did all values land?** Check the stream's **Quarantined** and **Held** counts, paused streams, and the destination's visibility rules.

Confirm which streams and columns the pipeline selects. An incremental read depends on its cursor changing on every relevant update; it does not see deletes. Check the connector's discovery result and documentation if a table or stream is missing.

For a continuous pipeline with growing lag, compare read throughput, commit duration and capacity. If retention is at risk, restore progress before the source drops unread history. Do not treat resetting positions as a throughput fix.

## Verify recovery

After correcting the cause, resume a paused pipeline or stream:

```sh
openetl resume PIPELINE
openetl resume --stream STREAM PIPELINE
```

For an unpaused scheduled pipeline, start a sync with `openetl sync PIPELINE`. Check that the next attempt commits, the affected stream makes progress, and the incident resolves. Inspect the delivered rows when the failure involved a hold, reset or rewind. Leave any remaining incident open until its cause is understood, or resolve it with a comment recording the repair.
