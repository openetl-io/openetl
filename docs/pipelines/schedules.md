---
title: Schedules
description: Run a pipeline every interval or at the times a cron expression gives, in a time zone you choose, and know what happens when a sync runs long, is paused or is missed.
---

A scheduled pipeline copies in syncs, and its `schedule` says when each one starts: `every` so long, or at the times a `cron` expression gives. A schedule has one of the two, and can add `jitter`. This page covers each, and what happens around them: the first sync, a sync that runs long, starting one by hand, pausing, and time when OpenETL wasn't running.

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

`every` is a Go duration greater than zero, such as `15m`, `1h` or `24h`. The intervals count from when you applied the pipeline, not from when each sync ends: applied at 09:50, this pipeline syncs at 09:50, 10:50, 11:50 and so on, however long each sync takes.

On an installation whose [orchestrator](../self-host/index.md#choose-an-orchestrator) is Temporal rather than the default, intervals count from midnight UTC on 1 January 1970 instead, so `every: 1h` syncs on the hour.

## At cron times

```yaml
spec:
  schedule: { cron: "0 6 * * *" }
```

`cron` is a standard cron expression of five fields: minute, hour, day of the month, month and day of the week. This one syncs at 06:00 every day.

A cron expression is in UTC unless it starts with `CRON_TZ=` and a time zone's name from the IANA database, such as `Europe/Berlin` or `America/New_York`. This pipeline syncs at 06:00 in Berlin, which is 05:00 UTC in winter and 04:00 UTC in summer:

```yaml
spec:
  schedule: { cron: "CRON_TZ=Europe/Berlin 0 6 * * *" }
```

> [!WARNING]
> `openetl plan` and `openetl apply --dry-run` don't check a cron expression. An invalid one fails `openetl apply` after the pipeline is stored, and the pipeline has no schedule until you apply it again with a valid one, which starts its first sync:
>
> ```text
> openetl: unavailable: the definitions were stored, but what runs a pipeline couldn't be updated; apply again: schedules: schedule schedule/a4d14205-4426-470d-bda9-e2e34c624c42: end of range (25) above maximum (23): 25
> ```

## Jitter

`jitter` starts each scheduled sync up to that much later, at random, so that pipelines on the same schedule don't all start at once:

```yaml
spec:
  schedule: { cron: "0 6 * * *", jitter: 10m }
```

This pipeline's syncs start between 06:00 and 06:10. Jitter doesn't move the schedule: the next sync is still worked out from 06:00. The first sync after an apply and a sync started by hand start without it.

## The first sync, and changing a schedule

Applying a new scheduled pipeline starts its first sync straight away, without waiting for its schedule or its jitter. The next sync starts at the schedule's next time after the apply.

An apply that changes a pipeline starts no sync:

- A change to `every` or `cron` starts the schedule over from the time of the apply. With `every: 10m` applied at 09:56, the next sync starts at 10:06.
- Any other change, `jitter` included, leaves the time of the next sync as it was.

The next sync runs the new version. A sync that's running when you apply carries on with the version it started with.

## When a sync is still running

A pipeline runs one sync at a time. A sync that's due while the last one is still running is skipped, and the schedule moves on to its next time: nothing starts when the long sync ends. With `every: 1m`, a sync that takes 1 minute 18 seconds makes the schedule skip one start.

If starts are skipped often, sync less often, or give each sync more room with `resources`, which the [pipeline spec at a glance](./index.md#running) covers.

## Start a sync now

`openetl sync`, or **Sync now** in the console, starts a sync straight away:

```sh
openetl sync shop-orders
```

```text
started a sync of workspaces/default/pipelines/shop-orders
```

It goes ahead of other pipelines' scheduled syncs that are waiting for room on the runners, and it doesn't move the schedule: the next scheduled sync starts when it would have. It fails while a sync of the pipeline is running:

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

Syncs that fall due while a pipeline is paused are skipped, not made up later. `openetl resume`, or **Resume**, starts no sync: the next one starts at the schedule's next time, as if the pipeline had never been paused. While it's paused, `openetl sync` fails:

```text
openetl: failed_precondition: workspaces/default/pipelines/shop-orders is paused; resume it first
```

Pausing a continuous pipeline stops its job, and resuming starts it again from its last checkpoint.

## When OpenETL wasn't running

On the default orchestrator, if no orchestrator was running when syncs fell due, the first to start again starts one sync for all of them, however many were missed, and the schedule then carries on from its next time. On Temporal, Temporal's schedules decide what to start, and still never start a sync while one is running.
