---
title: Troubleshoot a pipeline
description: Diagnose waiting jobs, failed syncs, stale data, connection errors and failed checks, then verify recovery.
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

To test the whole pipeline, including the permissions on each stream it reads and writes, run its preflight:

```sh
openetl preflight PIPELINE
```

An `Auth` incident may have paused the pipeline. An editor can update its secret with `openetl secret set NAME`, or apply corrected connection config. Run the checks again before resuming. Platform administrators manage runner authentication separately through [runner tokens](../self-host/access.md#rotate-a-runner-token).

## A check or preflight fails

`openetl check` and `openetl preflight` exit with status `1` when the check or the preflight ran and failed, and with `2` when it could not run. [Pipeline CLI commands](../reference/pipeline-cli.md#text-output) explains each line of their output. Find your case:

| What you see | What it means | What to do |
|---|---|---|
| An item marked `failed`, with `fix:` and `field:` under it | The connector found a problem with the config or the upstream system. `detail:` gives the upstream system's own error. | Do what `fix:` says. If `field:` names a config field, correct it and apply the source or destination again. Then run the check again. |
| An item marked `warning` | The check still passes. The pipeline can run, but not as configured or not as well as it could. | Do what `fix:` says when you can. |
| An item marked `skipped`, such as `needs schema.create, which failed` | The connector did not examine it, because an item it needs failed. | Correct the item it names, then run the check again. |
| A part marked `skipped`, such as `destination warehouse's preflight: skipped, the destination's check failed` | The preflight did not run that part, because a part before it failed. `a sync can't be planned` means that the sync's plan failed. A skipped part fails the preflight. | Correct the part that failed first, then run the preflight again. |
| `failed its check` with an error code and a message on the same line, and no item marked `failed` | The check failed for a reason that no item explains. Some connectors report one error and no items. | Read the code and the message, and correct the cause. Then run the check again. |
| A preflight part marked `failed` with a code and a message, and no items | The part's connector did not start or did not answer, or the part ran out of time. For example, `TRANSIENT: the source's preflight didn't finish within 2m0s`. A check has 1 minute, and a connector's preflight has 2 minutes. | Read the message. If the connector did not start, check its version and the runner's log. For a time-out, check the network path from the runner to the upstream system, and the upstream system's load. Then run the preflight again. |
| `the sync's plan: failed` | The plan has problems, which show as failed items. Or the runner could not settle what a sync reads and writes, for example because the source's discovery failed. Then the part has a code and a message, and both connectors' preflights are skipped with `a sync can't be planned`. | Read the code and the message, or the plan's failed items. Correct the pipeline or the source, then run the preflight again. |
| `INTERNAL: the connector answered with` and a number of items or bytes | The connector sent more items or more text than OpenETL keeps from one answer. OpenETL kept the first items, and failed the check. A text that ends with `…` was also cut. | Report it to the connector's maintainers, with the connector's version. [The limits](../build-connectors/check-results.md#what-the-platform-does-with-the-answer) are in Build connectors. |
| Exit status `2`, with `openetl: unavailable: the check couldn't run: no runner of data plane "DATA_PLANE" took it within 1m0s` | No runner of the data plane took the check. The result says nothing about the connection. | Check the runners, as in [A sync waits or never starts](#a-sync-waits-or-never-starts). For `openetl check`, `--data-plane` selects another data plane. |
| Exit status `2`, with `the check couldn't run: TRANSIENT: the destination's check didn't answer within 1m0s` | The connector did not answer its check within 1 minute. The result says nothing about the connection. | Check the network path from the runner to the upstream system, then run the check again. |
| Exit status `2`, with another message that starts `the check couldn't run` or `the preflight couldn't run` | The runner could not run the operation, for example because the connector did not start. | Read the code and the message. Correct the cause, then run the command again. |

The console's **Check** and **Preflight** show the same items. [Use the console](../use/connections.md#sources-destinations-and-connectors) explains how to read them.

## A sync fails repeatedly

Read the complete error in the failing sync's log rather than only the shortened error in the sync list.

| Symptom | Next step |
|---|---|
| `CONFIG_INVALID` or plan problems | Correct the named setting, apply it, and check connector/runtime compatibility. Repeating the same sync will not fix invalid config. |
| Source rate limits | Check the source's limits and heal's overrides. Increase the interval or reduce request rate where supported. |
| Out of memory | Inspect job peaks and `resources.max`. Increase the size or maximum where the data plane has room; a larger request still needs capacity. |
| `POSITION_LOST` | Follow [Reading from the start again](./positions.md#after-a-lost-position). A reset can replay data and cannot recover history the source has discarded. |
| `CHECKPOINT_CORRUPT` | If the error names a held file below `_held`, restore the file and resume the pipeline, as [a release that cannot read a held file](./schema-changes.md#repair-the-destination-and-release) explains. Otherwise, follow [checkpoint recovery](./positions.md#when-the-latest-checkpoint-cant-be-read). Read the delivery implications before allowing a rewind. |
| Schema errors, held columns or quarantined values | Read [Holds and quarantine](./holds-and-quarantine.md) to tell them apart. For a hold, follow [Handle schema changes and holds](./schema-changes.md). |
| `SCHEMA_DRIFT` with `can't go to a new column` or `went to a new version` | A column of the primary key, or the cursor, changed type in a way that is not a widening, or by a widening that changes how its values sort or the record IDs they give, such as an integer to a string. Follow [When a key or cursor changes type](./schema-changes.md#when-a-key-or-cursor-changes-type). |
| `SCHEMA_DRIFT` with `can't be held` | The destination refused a change to a column that OpenETL cannot hold, such as a column of the primary key. Or a hold is in force on such a column. Run the statement that the error gives on the destination, or change the column back at the source. If the error names a hold, release it, as [Column holds](./holds-and-quarantine.md#column-holds) explains. |
| `SCHEMA_DRIFT` with `so this run can't keep them` | A stream has held values, and the pipeline's version or the source's catalog stops it holding. Undo the change and release the holds, or discard them, as [A change that stops a stream holding](./holds-and-quarantine.md#a-change-that-stops-a-stream-holding) explains. |
| A dbt or Spark post-sync step fails | Fix that step. The sync can fail after its copied data has committed; inspect the step's log and [integration setup](../integrate/index.md). |

For a `SCHEMA_DRIFT` or `RESOURCE_EXHAUSTED` from a group window, `OVER` or a dedupe, see [A stateful SQL statement fails or quarantines rows](#a-stateful-sql-statement-fails-or-quarantines-rows).

For a continuous pipeline, repeated failures without a commit can open a **Crash loop** incident. Investigate its attempts' errors and runner health before resuming. Closing an incident by hand does not repair the failure or resume a paused pipeline.

### A continuous pipeline stops retrying

OpenETL retries a continuous pipeline's failed job. The wait before each retry doubles from 5 seconds up to 5 minutes. If the job keeps failing with the same error code, OpenETL stops retrying it and fails the run:

- A failure counts after the retries are 5 minutes apart. With the default limit of 5, the run fails after about 30 minutes of the same failure.
- OpenETL compares the error code only, not the message. A message can name something that changes on each attempt, such as the epoch.
- A failure with another code starts the count again. So does a job that commits or runs for 10 minutes.
- These codes never count, because an outage or a stop causes them and they pass when it ends: `TRANSIENT`, `RATE_LIMITED`, `AUTH_EXPIRED`, `DESTINATION_UNAVAILABLE`, `RESOURCE_EXHAUSTED`, `DRAINING` and `FENCED`. OpenETL retries them for as long as they last.

The run's error starts with `the job failed with CODE N times in a row, so OpenETL stopped retrying it`, and a **Sync failed** incident opens. The pipeline is not paused, but it reads nothing until it starts again. Correct the cause, then start it:

```sh
openetl resume PIPELINE
```

Applying the pipeline again also starts it. The orchestrators' setting [`--continuous-failure-limit`](../reference/openetld.md#orchestration) sets the limit. A change applies to runs that start after it.

`DATA_INVALID` is retried, under this limit, because some of its failures pass on a retry. Rows of a SQL transform can pass one at a time and fail together in a batch, and a staging file can fail to write on the runner's own disk. A fix outside OpenETL, such as a raised size limit or a replaced file, takes effect at the next retry, without a resume.

## A stateful SQL statement fails or quarantines rows

A group window, an `OVER` statement and a dedupe keep state between rows ([Aggregate by windows of time](./windows.md), [Compute running totals with OVER](./over.md), [Deduplicate rows with ROW_NUMBER](./dedupe.md)). Find your case:

| What you see | What it means | What to do |
|---|---|---|
| Rows quarantined with the code `late`, and `openetl.operator.rows.late` grows | For `OVER` and a first-row dedupe, a row came after the watermark had passed its event time. The engine writes rows in event-time order as the watermark passes them, so the row has no place in that order, also for a key with no rows written yet. For a group window, a row came after its window's lateness. | Set a longer `watermark.delay` on the stream, so rows wait longer for rows that come out of order. For a group window, a longer `watermark.lateness` also works. List the rows with `openetl quarantine --stream OUTPUT PIPELINE`. |
| Rows quarantined with the code `late` in many syncs of a stream read with `incremental`, though the output has the windows or `OVER` rows of their times | A sync read again rows that an earlier sync read, after their window had gone or their `OVER` row was written. A stream read with `incremental` reads again the rows at its last cursor value. Some streams read further back: an SDK or declarative source's stream that is not read in cursor order reads again what changed since the previous read started, and a stream with a `lookback` reads that far back. Most of these rows are copies of rows that an earlier sync counted, so they are not missing from the output. A row whose values changed since an earlier read is not a copy: the output counts only its earlier values. | For a group window, set `watermark.delay` and `watermark.lateness` on the stream so that together they cover about one sync interval plus the window's size. For `OVER`, which takes no lateness, set `watermark.delay` to cover about one sync interval. Before you load quarantined rows yourself, compare them with the output, so that you do not count a row twice. [State](../reference/sql-windows.md#state) has the details. |
| A plan problem that ends `sets no dedupe.retention, how long it remembers a key` | A first-row dedupe needs its stream's `dedupe.retention`, which has no default. | Set `source.streams[].dedupe.retention` to a duration longer than the time over which the source can repeat a row, such as `24h`, or to `unbounded`. [Set the retention](./dedupe.md#set-the-retention) explains the choice. |
| The sync fails with `SCHEMA_DRIFT` and `rows kept in state can't be read as the statement reads them now: column COLUMN changed from TYPE to TYPE` | A column of the stream changed its type, and rows that wait in the state of `OVER` or a dedupe cannot be cast to the new type, such as text that is not a number into an integer. | Change the column back in the source. Or start the statement's state again with a reset, which reads the stream again: an appended output then gets the stream's rows twice. Read [Build the state again](./over.md#build-the-state-again) first, then run `openetl reset --stream STREAM PIPELINE`. |
| The run fails with `RESOURCE_EXHAUSTED` and `keeps more state in memory than the job's size allows` | The state of the job's statements holds more than half of the job's memory while the engine nears its limit. | Run the pipeline with a larger `resources.size`. More workers spread the keys of a dedupe or the partitions of `OVER` over more pods, but one partition's state stays on one worker. For a first-row dedupe, set a shorter `dedupe.retention` than it has now; a latest-row dedupe keeps every key. For `OVER`, a shorter `ROWS` or `RANGE` frame keeps fewer rows. A stream read with `full_refresh` keeps each row that it reads until the read ends, so read it with `incremental` by its event time where the source can. |

## The pipeline runs but data is stale

Separate three questions:

- **When did it last commit?** Freshness measures time since the latest commit, not when the source last changed. A scheduled pipeline's normal freshness includes its schedule interval.
- **Is it behind the source?** Source-reported lag and retention show whether unread data is accumulating. Not every source reports these metrics.
- **Did all values land?** Check the stream's **Quarantined** and **Held** counts, paused streams, and the destination's visibility rules.

Confirm which streams and columns the pipeline selects. An incremental read depends on its cursor changing on every relevant update, to a value that differs from the one before. A cursor stored as a date or in whole seconds can give two changes of a row one cursor value. In a stream deduped at apply, OpenETL tells such versions apart by their values, except in the cases that [Choose a cursor that changes on every update](./delivery.md#choose-a-cursor-that-changes-on-every-update) lists, where the table keeps the first version. An incremental read does not see deletes. Check the connector's discovery result and documentation if a table or stream is missing.

For a continuous pipeline with growing lag, compare read throughput, commit duration and capacity. If retention is at risk, restore progress before the source drops unread history. Do not treat resetting positions as a throughput fix.

## A row lands again on every sync

If an appended table gets the same rows again on every sync, each time with a new `_openetl_id`, a column of the stream probably changes on every read while the row does not: a computed value, the time the source fetched the row, or JSON whose keys come in another order. An incremental read gives again the rows at the previous highest cursor value. In a stream deduped at apply, OpenETL tells the versions of a row apart by their values, so such a column makes each read of the row a new version, which lands. A group window or `OVER` over the stream counts such a row again in the same way.

Compare two copies of a row to find the column whose value differs. Then list the column in the stream's [`volatileColumns`](../reference/specs/pipeline.md#source-and-streams), by the name that the source's catalog gives it, and apply the pipeline. The plan warns of a column that the catalog does not have:

```yaml
source:
  streams:
    - name: products
      volatileColumns: [fetched_at]
```

OpenETL then leaves the column out when it compares versions, so the rows read again from the next sync on keep their IDs. A change to that column alone no longer lands, so list only columns whose changes you do not need.

## Verify recovery

After correcting the cause, resume a paused pipeline or stream:

```sh
openetl resume PIPELINE
openetl resume --stream STREAM PIPELINE
```

For an unpaused scheduled pipeline, start a sync with `openetl sync PIPELINE`. For an unpaused continuous pipeline whose run failed, `openetl resume PIPELINE` starts a new run. Check that the next attempt commits, the affected stream makes progress, and the incident resolves. Inspect the delivered rows when the failure involved a hold, reset or rewind. Leave any remaining incident open until its cause is understood, or resolve it with a comment recording the repair.
