---
title: Run a job on several pods
description: Spread a large job over workers with resources.workers, read what openetl plan says about it, and know what happens when one of its pods fails.
---

A job usually runs in one place: on a shared runner, or in a pod of its own. With `resources.workers` above 1, each job runs on that many **workers**, each in a pod of its own, and a **coordinator** in one more pod. The workers read the source's splits between them and write to the destination at the same time. The coordinator records each checkpoint and tells the workers when to commit.

[The reference](../reference/specs/pipeline.md#workers) has the exact rules. [Install on Kubernetes](../self-host/kubernetes.md#jobs-on-several-pods) covers what the pods need from the cluster.

## When workers help

Workers give a job more CPU and memory than one pod can have. A job gains from them when all of these are true:

- The job already runs at the largest size that you can give it, and it is still too slow.
- The source divides its streams into many splits, which it can read at the same time.
- The destination takes at least as many writer lanes as there are workers. Its documentation says how many it takes.

Some work does not spread:

- A stream that one worker reads is read no faster. [Streams that one worker reads](#streams-that-one-worker-reads) lists them.
- A keyed stream that the source does not split by key sends rows between the workers. [The exchange between workers](#the-exchange-between-workers) says what that costs.
- A [group window](../reference/sql-windows.md#group-windows) sends the rows of the stream it reads between the workers too. [Group windows on workers](#group-windows-on-workers) says how.
- The first worker commits each checkpoint for all the workers, so a job whose time goes to commits gains little. A destination with lane commits is different: each worker commits its own part.
- Each attempt starts more pods. The coordinator also starts the source and destination connectors to set them up before the workers start.

## Before you start

- Your pipeline's pool has a pod launcher. The `default` pool of [the Kubernetes install](../self-host/kubernetes.md) has one. The Docker Compose install has none, so its jobs never run on workers. Ask your operator if you are not sure.
- The pod launcher can make all the pods at once: one for each worker, and one for the coordinator. Your operator sets how many pods and vCPUs it can have.

## Set the workers

Set `resources.workers` to a whole number from 1 to 64. `1`, the default, runs each job on one pod.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-orders
spec:
  schedule: { every: 1h }
  source: { ref: shop-db }
  destination: { ref: warehouse }
  resources:
    size: l
    workers: 4
```

Each job of this pipeline runs on four workers of size `l` (4 vCPUs, 8 GiB each), and a coordinator of size `xs`. That is five pods. `size` is each worker's size, not the job's total. With `size: auto`, OpenETL picks each worker's size from the connectors' hints and the largest worker's measured peaks.

OpenETL uses only the count that you set. It does not choose a count for you, and it does not change the count as the data grows.

You can change the count at any time. As with any change to the pipeline, syncs that start after you apply it use the new count. Each attempt restores from the last checkpoint, whatever count wrote it.

## Check the plan

Run `openetl plan` before you apply:

```sh
openetl plan -f shop-orders.yaml
```

When the job can run on workers, the plan shows a line after the job's size:

```text
  workers: 4, each of the job's size, and their coordinator
```

When the job runs on one pod instead, the plan has no `workers:` line. A warning says why, in this form:

```text
resources.workers asks for 4 workers, and the job runs on one pod: REASON
```

The pipeline still applies, and its jobs run on one pod. These are the reasons, and what you can do:

| Reason | What to do |
|---|---|
| `a Spark or Flink transform runs on one pod` | Nothing today: a pipeline with a Spark or Flink transform runs on one pod. |
| `the destination declares one writer lane` | Nothing: the destination takes one writer lane, so its jobs run on one pod. |
| `the destination takes N writer lanes, fewer than M workers` | Set `resources.workers` to `N` or fewer. |
| `a transactional destination runs on one pod unless its writer lanes commit on their own` | Nothing today: a transactional destination runs on workers only if its connector declares lane commits. Its documentation says whether it does. |
| `a destination that loads staged epochs runs on one pod` | Nothing today: a destination that loads files that OpenETL stages runs on one pod. |
| `a destination the engine writes runs on one pod` | Nothing today: the built-in `parquet` destination runs on one pod. |
| `no pod launcher of data plane "PLANE" and pool "POOL" has reported in the last ten minutes, and an attempt runs on workers only in pods` | Run the pipeline in a pool that has a pod launcher, or ask your operator to add one. |

A plan **problem**, not a warning, says when no pod launcher of the pool can ever take the job's pods. `apply` refuses the pipeline. Lower `resources.workers` or `resources.size`, or ask your operator to raise the launcher's limits.

The plan cannot always check everything. Then a note says so:

- If the control plane does not know the destination's capabilities, each attempt's coordinator checks them before it starts the workers.
- If the source has not been discovered, the plan cannot say which streams one worker reads. Run `openetl discover SOURCE`, then plan again.

When the coordinator finds that a job cannot run on workers, OpenETL places the same attempt again on one pod. The sync's attempt records why.

## Streams that one worker reads

The workers read the source's splits between them. Each attempt reads all the splits of these streams on one worker:

- a stream read with `cdc`;
- a stream written with `overwrite`.

The source reads the changes of every stream read with `cdc` from one log, such as a replication slot, and only one reader can hold the log. So one worker reads all of a job's `cdc` streams together. If such a stream has a primary key, the [exchange](#the-exchange-between-workers) still sends its rows to the writer lanes of every worker.

The plan warns about each one, in this form:

```text
STREAM: each attempt reads it on one of its 4 workers: REASON
```

Each such stream is read no faster than on one pod. Different streams written with `overwrite` can go to different workers, which read them at the same time. Every `cdc` stream goes to the same worker. A worker with no stream or split to read has no rows to read. For example, a job of four workers with only two streams, both written with `overwrite`, leaves two workers with nothing to read. If both streams are read with `cdc` instead, one worker reads both, and three workers have nothing to read. Workers that read nothing still write the rows that the exchange sends them.

With a [SQL transform](./sql-transforms.md), these rules apply to each statement's output, and to each stream that passes through. One worker reads all the splits of a stream that is read with `cdc`, or when an output written from it is written with `overwrite`. The plan's warning names the stream that the statement reads.

## The exchange between workers

A keyed stream that the source does not split by key, and a keyed stream read with `cdc`, has an **exchange** in the plan's [`partitioning` section](./plan-and-apply.md#partitioning-in-the-plan). On workers, the exchange sends each row to the writer lane that owns its key group, on whichever worker that writer lane is. Every worker writes the keys that it owns, whichever worker read them.

Where the rows are read does not depend on the exchange. The workers can share a stream's splits, unless the stream is read with `cdc` or written with `overwrite`; then one worker reads all of them, as [Streams that one worker reads](#streams-that-one-worker-reads) says. A worker gets no split when there are fewer splits than workers.

Each worker's engine connects to every other worker's engine once, on TCP port 7720 in Kubernetes. The connection uses TLS 1.3 with a certificate that the coordinator makes for each attempt. The engines trust only that certificate, and each connection proves a secret of the attempt. An engine refuses a connection of another attempt, or of an earlier try of the same attempt. [The route between pods](../self-host/kubernetes.md#the-route-between-pods) says what the network must allow.

The exchange costs a job on workers more than an exchange in one pod:

- Each row that goes to another worker is encoded, encrypted, sent, decrypted and decoded again. That takes CPU on both workers, and network bandwidth between them. With `N` workers, about `(N-1)/N` of such a stream's rows go to another worker.
- A writer lane that falls behind slows the workers that send it rows. Each worker's engine holds at most an eighth of the job's memory, and at most 8 MiB for each other worker, in rows that wait to be sent.
- A writer lane passes a checkpoint only after every lane of every worker has passed it. So the slowest worker sets the pace of every checkpoint.

If a connection between workers breaks, or a worker cannot reach another within a minute, the attempt fails with `TRANSIENT`, and OpenETL retries it from the last checkpoint. If the other worker failed with an error of its own, such as `SCHEMA_DRIFT`, within 5 seconds, the attempt fails with that error instead.

## Group windows on workers

A [group window](./sql-transforms.md#aggregate-rows-by-windows-of-time) needs every row of a key in one place. In the plan, its statement has an exchange on the columns that it groups by. On workers, that exchange sends each row to the worker that owns the key group of the row's keys, whichever worker read the row. That worker keeps the state of its key groups, closes their windows, and writes the windows' rows with its own writer lanes. The output's key holds the columns that the window groups by, so the window's rows need no second exchange before the writer:

```text
  partitioning:
    minutes  split by id -> exchange by key group on region -> statement 1:1, needs region -> writer, needs window_start, region
```

The exchange costs what [The exchange between workers](#the-exchange-between-workers) says. A window closes when the watermark of every lane of every worker has passed its end, so the slowest split of the stream holds every worker's windows open.

[`OVER`](./over.md) runs on workers in the same way, with its exchange on its `PARTITION BY` columns. The worker that owns a partition's key group keeps its frames and the rows that wait for the watermark, and writes each row with its own writer lanes, once. A [dedupe](./dedupe.md) does the same: the worker that owns a key's key group keeps the key, and writes its row.

At each checkpoint, each worker writes the state that changed in its key groups, and the checkpoint names the state of every worker. A later attempt restores that state on any number of workers and lanes. Each worker opens the state of the key groups that it owns now. So you can change `resources.workers` without a reset.

## Schema changes and holds

Each worker reads different rows, so one worker can see a schema change before the others. The coordinator decides each stream's schema for all the workers:

- A worker that sees a new column or a wider type sends the change to the coordinator. The coordinator takes the first change for each version of the schema, and sends it to every worker.
- A worker whose change came second writes its rows again by the schema that the coordinator took.
- The coordinator sets the stream's schema for every worker at the first checkpoint whose rows have the stream's columns. A change that reaches the coordinator before it has every worker's part of that checkpoint is part of the first schema, whichever worker's rows made it. After that, `schemaPolicy` applies on every worker, also on a worker that has read no rows of the stream yet. For example, under `block`, a worker whose first rows change a column's type stops that change, as a worker that wrote the column does. If the stream can hold columns, the worker holds the column. Otherwise, the sync fails with `SCHEMA_DRIFT`. [Column holds](./holds-and-quarantine.md#column-holds) lists what a column hold needs.
- With a SQL transform, the coordinator decides the schema of each stream that a statement reads, and of each statement's output.

[Holds](../get-started/concepts.md#holds-and-quarantine) work the same way. When a worker starts to hold a column, the coordinator sends the hold to every worker, and every worker then holds that column. Each checkpoint records the hold once. If the destination refuses a column, the coordinator starts the attempt again at once, and the same worker pods join it and hold the column. When you release a hold, only the first worker writes the held values to the destination.

## When a pod fails

A job on workers fails as a whole. OpenETL then retries it from the last checkpoint, as it does any job, and keeps each stream's [delivery guarantee](./delivery.md):

- If a worker fails, the attempt fails, and OpenETL retries it. The next attempt restores every worker from the last checkpoint, and reads again what came after it.
- If a worker's pod is stopped, such as on a node drain or a spot interruption, the worker does not wait for a checkpoint. The attempt fails with the code `TRANSIENT`, as it does when a worker's pod is lost, and OpenETL retries it.
- If the coordinator's pod is stopped, the coordinator stops the attempt at a checkpoint, as a job's pod does. OpenETL then places the job again.
- Every worker must join its coordinator within 5 minutes after the coordinator starts the attempt. A worker that does not join in time fails the attempt.
- An attempt can be stopped before its workers have joined, or before they have their work. For example, you pause the pipeline, apply a new version, or release a hold, which starts a continuous pipeline's job again. The coordinator does not wait for the workers. It stops the attempt at once, as it stops at a checkpoint, and the attempt does not count as failed.
- If a worker runs out of memory, the retry runs at a larger size only when the pod launcher reports the worker's pod before the coordinator reports the attempt. Otherwise the retry runs at the same size. If workers run out of memory, set a larger `resources.size`.

Some transactional destinations commit each writer lane in a transaction of its own. Their connector declares **lane commits**. With such a destination, a pod can fail after a checkpoint and before every lane commits. The next attempt finishes the epoch before it reads anything new. For each lane that did not commit, it does one of these:

- It commits the copy that the lane kept, if the destination keeps one.
- It reads that lane's part of the epoch again from the source, from the same positions, and commits it. With an exchange, the lane's part holds its keys' rows of every split that the epoch read, on any worker, so the next attempt reads all those splits again. One worker reads the streams read with `cdc` again, one stream at a time. Only one reader can hold the log that their changes come from.

If that attempt is stopped while it reads a lane's part again, the lane stays uncommitted, and the attempt after it reads the part again.

With a [SQL transform](./sql-transforms.md), the next attempt runs the statements again on what it reads. It commits each output's rows that the lane wrote, and no others:

- Of an output without an exchange, the rows of the splits that the lane's own worker read and wrote to that lane.
- Of an output with an exchange, the rows of the lane's keys, from every split that the statement read in the epoch, on any worker.
- Of a group window's output, the rows of the windows of the lane's keys that the epoch closed. The next attempt starts from the state of the checkpoint before the epoch, and reads every split that the epoch read again, on any worker. Each row meets the window's watermark that it met in the epoch, and the windows close where the epoch closed them. So the next attempt writes the window rows that the lane wrote in the epoch. A row that was `late` in the epoch is late again, and a session that closed before a row came stays closed.
- Of an output of `OVER`, the rows of the lane's partitions that the epoch's watermark passed, by the same steps. Each row meets the watermark that it met in the epoch, so a row that was `late` is late again, and every other row gets the values that it got in the epoch.
- Of a first-row dedupe's output, the first rows of the lane's keys that the epoch's watermark passed, by the same steps, and of a latest-row dedupe's, each of the lane's keys' latest row of the epoch.

For this, the checkpoint of each epoch keeps the steps of each group window's, `OVER`'s or first-row dedupe's watermark in the epoch, where the destination has lane commits. A step says from which row of a split the rows met a later watermark, and which one. The checkpoint keeps a step only where a row can tell it apart. A tumbling or hopping window's watermark takes a step only when it passes the end of a window. The watermark of `OVER` or a first-row dedupe takes a step at each move that a row meets, and a latest-row dedupe's takes none. An epoch in which the watermark did not move keeps no steps, and other pipelines' checkpoints keep none.

After 1,024 steps of a window's watermark in an epoch, a lane that keeps the window's state holds the watermark until the checkpoint. The rows of each split can then take one more step, to the held watermark. So a lane keeps at most 1,024 steps of a window in an epoch, and one more for each split whose rows reach it. A row that comes after that is late only if it is behind the held watermark. The windows that the watermark passes in that time close at the checkpoint, and are committed with the epoch, as before.

While the next attempt reads a lane's part again, it keeps in memory the rows that met a later watermark than the epoch's first, until it reaches the epoch's checkpoint. These rows can take at most half of the job's memory. If they need more, the attempt fails with `RESOURCE_EXHAUSTED`, and the epoch stays unsettled. This limit reserves no memory for the rows, and does not make the attempt report `RESOURCE_EXHAUSTED` first: if the rest of the attempt uses more than the other half, the attempt can run out of memory before the rows reach their limit. In either case, set a larger `resources.size`, so that the next attempt can settle the epoch.

### Requirements for reading an epoch again

For an exact stream, exactly-once recovery requires the same records from saved positions. Keep those records available until recovery completes.

If the part ended at a resume point, the source must return the resume point recorded by the epoch. Earlier resume points can differ. A source that stops where OpenETL asks sends that point when it stops. Other sources must reach that point themselves. The connector's documentation describes this behavior.

If the part completed its read, OpenETL asks the source to complete at the same position again. A connector that supports this stops there. Other connectors read to the end. They complete at the same position only if no records were added. In both cases, the read must return at least the original number of records.

Use the source's settings to preserve the records. These can include a fixed start position and retention longer than the recovery period. OpenETL reads without following the source. The source ends the read where OpenETL asks, or when it catches up.

The attempt fails with `POSITION_LOST` in these cases:

- The read ends before the saved position.
- The read returns fewer records up to the saved position than the epoch recorded. This check does not apply after a checkpoint without a resume point. In that case, restoration rereads records after the last resume point.
- A part that completed its read ends at a different position.

In the last case, the error says that the source did not complete where OpenETL asked. The connector version might not support this operation, or the current read might not support it. Check the connector's documentation. Use a version that supports completing this part at the requested position. Then resume the pipeline.

> [!CAUTION]
> Recreating the pipeline starts a new read from the source's start position. Check the destination effects before you continue.

If no connector version supports the required completion, delete and reapply the pipeline. [Delivery guarantees](./delivery.md#commits-as-a-reader-sees-them) explains destination effects during recovery.

If the source returns different records in the same number, or more records, OpenETL commits them. For a keyed stream without splits by key, it commits only records whose keys belong to the lane. This restriction also applies to CDC streams. If the source keeps the read open, the attempt waits until stopped.

Do not change the primary key of a keyed stream while an epoch waits to be finished. Each lane's part of such a stream holds the rows of some of the key's values, so OpenETL cannot read it again under another key. The attempt then fails with `CONFIG_INVALID`, and names the key that the epoch used. Apply the pipeline with that key again, and change the key once the pipeline has committed again.

With a SQL transform, do not change `transform.sql` while an epoch waits to be finished. The attempt fails with `CONFIG_INVALID` in these cases:

- The statements are not the ones that wrote the epoch, even if only a comment or a space changed.
- An output that the epoch wrote is no longer written, or is written from another stream.
- An output that the epoch sent to the lanes by its key now has another key.

An output's key comes from the primary key of the stream it is written from (see [Primary keys](../reference/sql.md#primary-keys)), so a changed key of that stream can change the output's key too. Apply the pipeline with the statements that wrote the epoch, and with the primary key that each stream they read had then. Change the statements and the keys once the pipeline has committed again.

## Logs and usage

`openetl logs PIPELINE`, and the sync's **Log** in the console, have the coordinator's log. It says where the workers join. If a worker fails, the attempt's error names the worker, such as `worker 2`, and gives its error, when the worker could report it. Workers are numbered from 1. The workers' own logs stay in their pods. [Logs of workers](../self-host/kubernetes.md#logs-of-workers) tells your operator how to read them.

A sync's page in the console shows **Size**, which is each worker's size, and **Used**, which is what the largest worker used. The sync's attempts in the API also say how many workers each attempt ran on (`workers`), and why an attempt ran on one pod (`onePod`).
