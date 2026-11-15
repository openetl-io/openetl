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
| `a SQL transform into a destination whose writer lanes commit on their own runs on one pod` | Nothing today: a pipeline with a [SQL transform](./sql-transforms.md) runs on one pod when its destination declares lane commits. |
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

A keyed stream that the source does not split by key, and a keyed stream read with `cdc`, has an **exchange** in the plan's [`partitioning` section](./index.md#partitioning-in-the-plan). On workers, the exchange sends each row to the writer lane that owns its key group, on whichever worker that writer lane is. Every worker writes the keys that it owns, whichever worker read them.

Where the rows are read does not depend on the exchange. The workers can share a stream's splits, unless the stream is read with `cdc` or written with `overwrite`; then one worker reads all of them, as [Streams that one worker reads](#streams-that-one-worker-reads) says. A worker gets no split when there are fewer splits than workers.

Each worker's engine connects to every other worker's engine once, on TCP port 7720 in Kubernetes. The connection uses TLS 1.3 with a certificate that the coordinator makes for each attempt. The engines trust only that certificate, and each connection proves a secret of the attempt. An engine refuses a connection of another attempt, or of an earlier try of the same attempt. [The route between pods](../self-host/kubernetes.md#the-route-between-pods) says what the network must allow.

The exchange costs a job on workers more than an exchange in one pod:

- Each row that goes to another worker is encoded, encrypted, sent, decrypted and decoded again. That takes CPU on both workers, and network bandwidth between them. With `N` workers, about `(N-1)/N` of such a stream's rows go to another worker.
- A writer lane that falls behind slows the workers that send it rows. Each worker's engine holds at most an eighth of the job's memory, and at most 8 MiB for each other worker, in rows that wait to be sent.
- A writer lane passes a checkpoint only after every lane of every worker has passed it. So the slowest worker sets the pace of every checkpoint.

If a connection between workers breaks, or a worker cannot reach another within a minute, the attempt fails with `TRANSIENT`, and OpenETL retries it from the last checkpoint. If the other worker failed with an error of its own, such as `SCHEMA_DRIFT`, within 5 seconds, the attempt fails with that error instead.

## Schema changes and holds

Each worker reads different rows, so one worker can see a schema change before the others. The coordinator decides each stream's schema for all the workers:

- A worker that sees a new column or a wider type sends the change to the coordinator. The coordinator takes the first change for each version of the schema, and sends it to every worker.
- A worker whose change came second writes its rows again by the schema that the coordinator took.
- With a SQL transform, the coordinator decides the schema of each stream that a statement reads, and of each statement's output.

[Holds](../get-started/concepts.md#schema-changes-and-holds) work the same way. When a worker starts to hold a column, the coordinator sends the hold to every worker, and every worker then holds that column. Each checkpoint records the hold once. If the destination refuses a column, the coordinator starts the attempt again at once, and the same worker pods join it and hold the column. When you release a hold, only the first worker writes the held values to the destination.

## When a pod fails

A job on workers fails as a whole. OpenETL then retries it from the last checkpoint, as it does any job, and keeps each stream's [delivery guarantee](./delivery.md):

- If a worker fails, the attempt fails, and OpenETL retries it. The next attempt restores every worker from the last checkpoint, and reads again what came after it.
- If a worker's pod is stopped, such as on a node drain or a spot interruption, the worker does not wait for a checkpoint. The attempt fails with the code `TRANSIENT`, as it does when a worker's pod is lost, and OpenETL retries it.
- If the coordinator's pod is stopped, the coordinator stops the attempt at a checkpoint, as a job's pod does. OpenETL then places the job again.
- Every worker must join its coordinator within 5 minutes after the coordinator starts the attempt. A worker that does not join in time fails the attempt.
- If a worker runs out of memory, the retry runs at a larger size only when the pod launcher reports the worker's pod before the coordinator reports the attempt. Otherwise the retry runs at the same size. If workers run out of memory, set a larger `resources.size`.

Some transactional destinations commit each writer lane in a transaction of its own. Their connector declares **lane commits**. With such a destination, a pod can fail after a checkpoint and before every lane commits. The next attempt finishes the epoch before it reads anything new. For each lane that did not commit, it does one of these:

- It commits the copy that the lane kept, if the destination keeps one.
- It reads that lane's part of the epoch again from the source, from the same positions, and commits it. With an exchange, the lane's part holds its keys' rows of every split that the epoch read, on any worker, so the next attempt reads all those splits again.

> [!IMPORTANT]
> Reading a lane's part again keeps an exact stream exactly once only if the source returns the same records from a saved position. If the part ended at a resume point, the source must also return the resume point the epoch recorded; the resume points before it may differ. A source that stops where OpenETL asks sends that resume point as it stops, and a source that doesn't must arrive at it by itself. The source's documentation says whether it stops where OpenETL asks. If the part ended its read in the epoch, OpenETL asks for no stop. It reads that part to its end, and the source must complete it at the same position as the first read, with no fewer records. You make sure that the records are the same through the source's settings, such as a fixed start position and a retention period longer than an epoch can take to recover. OpenETL reads again without following the source, so the source ends the read where OpenETL asks, or once it has caught up. The attempt fails with `POSITION_LOST` in these cases:
>
> - The read ends before the saved position.
> - The read returns fewer records up to the saved position than the epoch had, unless the epoch's checkpoint was placed on that part without a resume point. The restore reads the records after the last resume point again in that case.
> - A part that ended its read in the epoch ends at another position.
>
> If the source returns other records in the same number, or more records, OpenETL commits them. For a keyed stream that the source does not split by key, or that is read with `cdc`, OpenETL commits only the records whose keys belong to the lane. If the source keeps the read open, the attempt waits until it is stopped.

Do not change the primary key of a keyed stream while an epoch waits to be finished. Each lane's part of such a stream holds the rows of some of the key's values, so OpenETL cannot read it again under another key. The attempt then fails with `CONFIG_INVALID`, and names the key that the epoch used. Apply the pipeline with that key again, and change the key once the pipeline has committed again.

## Logs and usage

`openetl logs PIPELINE`, and the sync's **Log** in the console, have the coordinator's log. It says where the workers join. If a worker fails, the attempt's error names the worker and gives its error, when the worker could report it. The workers' own logs stay in their pods. [Logs of workers](../self-host/kubernetes.md#logs-of-workers) tells your operator how to read them.

A sync's page in the console shows **Size**, which is each worker's size, and **Used**, which is what the largest worker used. The sync's attempts in the API also say how many workers each attempt ran on (`workers`), and why an attempt ran on one pod (`onePod`).
