---
title: Schedules
description: Configure intervals, cron times and jitter, and understand delayed, paused and missed syncs.
---

A scheduled pipeline copies data in syncs. Its `schedule` uses either an `every` interval or a `cron` expression. Optional `jitter` spreads start times across an interval. This guide explains those settings and the behavior of manual, paused, overlapping and missed syncs.

A continuous pipeline has no schedule, since it runs all the time: see [continuous pipelines](./index.md#continuous-pipelines). [The reference](../reference/specs/pipeline.md) has every field of a pipeline.

## Every interval

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-orders
spec:
  schedule: { every: 1h }
  source: { ref: shop-db }
  destination: { ref: warehouse }
```

`every` is a positive Go duration, such as `15m`, `1h` or `24h`. Intervals align to midnight UTC on January 1, 1970. They do not count from the previous sync's end time.

For example, `every: 1h` starts syncs on the hour. `every: 15m` starts at 00, 15, 30 and 45 minutes past each hour. If you apply this hourly pipeline at 09:50, its first sync starts immediately. The next syncs start at 10:00, 11:00 and subsequent hours.

## At cron times

```yaml
spec:
  schedule: { cron: "0 6 * * *" }
```

`cron` is a standard cron expression of five fields: minute, hour, day of the month, month and day of the week. This one syncs at 06:00 every day.

Cron expressions use UTC by default. To select a time zone, prefix the expression with `CRON_TZ=` and its IANA name. Examples include `Europe/Berlin` and `America/New_York`. This pipeline syncs at 06:00 Berlin time, equivalent to 05:00 UTC in winter and 04:00 UTC in summer:

```yaml
spec:
  schedule: { cron: "CRON_TZ=Europe/Berlin 0 6 * * *" }
```

`openetl plan` and `openetl apply` refuse a cron expression that isn't valid, before anything is stored:

```text
openetl: invalid_argument: shop-orders.yaml#1: spec: schedule.cron: "0 25 * * *" isn't a cron expression of five fields: end of range (25) above maximum (23): 25
```

## Jitter

`jitter` adds a random delay up to the configured duration. Use it to spread the start times of pipelines with the same schedule:

```yaml
spec:
  schedule: { cron: "0 6 * * *", jitter: 10m }
```

This pipeline's scheduled syncs start between 06:00 and 06:10. Jitter does not change the underlying schedule. The first sync of a new pipeline and manually started syncs have no jitter.

## The first sync, and changing a schedule

Applying a new scheduled pipeline starts its first sync immediately, without waiting for the schedule or jitter. The next sync uses the first scheduled time after the apply.

An apply that changes a pipeline starts no sync:

- A change to `every` or `cron` moves the next sync to the new schedule's next time after the apply. With `every: 10m` applied at 09:56, the next sync starts at 10:00.
- Any other change, `jitter` included, leaves the time of the next sync as it was.

The next sync runs the new version. A sync that's running when you apply carries on with the version it started with.

Each orchestrator rebuilds schedules from pipeline definitions at startup and every 10 minutes. This also applies stored changes that failed to reach the scheduler during an apply, pause or resume.

## When a sync is still running

A pipeline runs one sync at a time. If a sync is still running at the next scheduled time, that start is skipped. The skipped sync does not start when the active sync ends. With `every: 1m`, a sync lasting 1 minute 18 seconds skips one scheduled start.

If starts are skipped often, sync less often, or give each sync more room with `resources`, which the [pipeline spec at a glance](./index.md#running) covers.

## Start a sync now

`openetl sync`, or **Sync now** in the console, starts a sync straight away:

```sh
openetl sync shop-orders
```

```text
started a sync of workspaces/default/pipelines/shop-orders
```

A manual sync has priority over other pipelines' scheduled syncs waiting for runner capacity. It does not change the schedule. The command fails if the same pipeline already has a running sync:

```text
openetl: failed_precondition: a sync of workspaces/default/pipelines/shop-orders is running
```

A continuous pipeline has no syncs, so `openetl sync` fails for one.

## Pause and resume

`openetl pause`, or **Pause** in the console, stops a pipeline's scheduled syncs. A sync that's running carries on to its end.

```sh
openetl pause shop-orders
```

```text
paused workspaces/default/pipelines/shop-orders
```

Scheduled starts are skipped while a pipeline is paused. They are not retried later. `openetl resume`, or **Resume**, enables the schedule without starting a sync immediately. The next sync starts at the next scheduled time. While the pipeline is paused, `openetl sync` fails:

```text
openetl: failed_precondition: workspaces/default/pipelines/shop-orders is paused; resume it first
```

Pausing a continuous pipeline stops its job, and resuming starts it again from its last checkpoint.

### Pause one stream

`--stream` pauses one stream and leaves the others syncing. Syncs skip the stream and keep its position, so resuming it reads on from where it stopped:

```sh
openetl pause --stream public.customers shop-orders
openetl resume --stream public.customers shop-orders
```

```text
paused stream public.customers of workspaces/default/pipelines/shop-orders
resumed stream public.customers of workspaces/default/pipelines/shop-orders
```

A sync that's running carries on with the stream; a continuous pipeline's job restarts at a checkpoint without it. Some streams can't stop on their own, and pausing one fails with `failed_precondition`:

- a `cdc` stream, because skipping it while other streams advance the shared log would lose its changes;
- a stream a transform reads;
- the last stream that isn't paused: pause the pipeline instead.

Heal pauses a stream by itself when it quarantines too many of its rows ([Incidents](../use/index.md#incidents)). `openetl overrides NAME` lists a pipeline's paused streams, and why each was paused. Resuming the whole pipeline leaves its paused streams paused, and an apply that no longer reads a stream drops its pause.

## When heal slows syncs down

After 3 consecutive syncs fail from source rate limits, heal reduces the request rate or sync frequency. It repeats this adjustment after every 3 further failures. `heal.mode: off` disables this response; `suggest` only proposes it:

- A declarative source's rate limit is halved, down to an eighth of what its manifest declares.
- An `every` interval doubles, up to a day, if the rate limit cannot decrease further or the sync duration reached the interval. A `cron` schedule does not change.

Heal applies these **overrides** without creating a pipeline version. An override remains until you remove it or apply a version that changes the corresponding schedule or source connector version. `openetl overrides NAME` lists the overrides. To restore the interval from the pipeline definition, remove its override:

```sh
openetl overrides remove shop-orders schedule.every
```

```text
removed heal's override of schedule.every from workspaces/default/pipelines/shop-orders; the spec has its say again
```

`openetl plan` lists the overrides a pipeline keeps, and notes the ones an apply would drop.

## When OpenETL wasn't running

With DBOS, the first orchestrator to restart starts one sync for all missed scheduled times. The schedule then resumes at its next time. With Temporal, Temporal's schedules determine which missed sync to start. Neither backend starts overlapping syncs for the same pipeline.
