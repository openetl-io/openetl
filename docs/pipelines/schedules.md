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

`every` is a Go duration greater than zero, such as `15m`, `1h` or `24h`. The intervals count from midnight UTC on 1 January 1970, not from when each sync ends, so `every: 1h` syncs on the hour and `every: 15m` at 00, 15, 30 and 45 minutes past, however long each sync takes. Applied at 09:50, this pipeline syncs at 09:50, its first sync, then at 10:00, 11:00 and so on.

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

`openetl plan` and `openetl apply` refuse a cron expression that isn't valid, before anything is stored:

```text
openetl: invalid_argument: shop-orders.yaml#1: spec: schedule.cron: "0 25 * * *" isn't a cron expression of five fields: end of range (25) above maximum (23): 25
```

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

- A change to `every` or `cron` moves the next sync to the new schedule's next time after the apply. With `every: 10m` applied at 09:56, the next sync starts at 10:00.
- Any other change, `jitter` included, leaves the time of the next sync as it was.

The next sync runs the new version. A sync that's running when you apply carries on with the version it started with.

Each orchestrator sets every schedule again from its pipeline when it starts and every 10 minutes after. A change that was stored but didn't reach the schedule, because an apply, a pause or a resume failed partway, takes effect by then.

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

- a stream read with `cdc`, which shares the source's log of changes with the pipeline's other `cdc` streams, so skipping it would lose its changes;
- a stream a transform reads;
- the last stream that isn't paused: pause the pipeline instead.

Heal pauses a stream by itself when it quarantines too many of its rows ([Incidents](../use/index.md#incidents)). `openetl overrides NAME` lists a pipeline's paused streams, and why each was paused. Resuming the whole pipeline leaves its paused streams paused, and an apply that no longer reads a stream drops its pause.

## When heal slows syncs down

When 3 syncs in a row fail because the source kept limiting their requests, and every 3 after that, heal slows the pipeline down, unless `heal.mode` is `off` (`suggest` only proposes it):

- A declarative source's rate limit is halved, down to an eighth of what its manifest declares.
- An `every` schedule's interval is doubled, up to a day, when there's no rate limit left to lower or the sync ran at least as long as the interval. A `cron` schedule isn't changed.

These are heal's overrides: they apply in place of the spec's values, without a new version. They stay until you remove one, or apply a version that changes the field: a new `schedule`, or a new version of the source's connector. `openetl overrides NAME` lists them, and `openetl overrides remove NAME FIELD` gives the spec back its say:

```sh
openetl overrides remove shop-orders schedule.every
```

```text
removed heal's override of schedule.every from workspaces/default/pipelines/shop-orders; the spec has its say again
```

`openetl plan` lists the overrides a pipeline keeps, and notes the ones an apply would drop.

## When OpenETL wasn't running

On the default orchestrator, if no orchestrator was running when syncs fell due, the first to start again starts one sync for all of them, however many were missed, and the schedule then carries on from its next time. On Temporal, Temporal's schedules decide what to start, and still never start a sync while one is running.
