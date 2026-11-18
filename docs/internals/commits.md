---
title: Checkpoints and commits
description: Explain checkpoint storage, the commit protocol, worker recovery and delivery levels.
---

This page describes the commit protocol for contributors. [Delivery guarantees](../pipelines/delivery.md) explains the user-facing behavior.

## Checkpoints

[`internal/checkpoint`](../../internal/checkpoint) keeps each pipeline's checkpoint log under `{org}/{pipeline}/_checkpoints/` in object storage. Records are immutable, and `HEAD` points to the latest record. Compare-and-swap updates use conditional writes on S3, GCS and Azure Blob Storage. This prevents two attempts from advancing the same `HEAD` concurrently.

An attempt claims the pipeline by writing `HEAD` with the next generation. An older active attempt, called a **zombie**, receives `ErrFenced` on its next `HEAD` write and stops. Each record contains stream resume states and schemas, lane committables, and staged or quarantined file references.

The same storage holds, next to the log, each attempt's log as JSON lines (`_logs/`), staged epochs (`_staging/`), held values (`_held/`), quarantined records (`_quarantine/`) and stateful operators' runs (`_state/`). `Prune` lists all of `_state/`, and deletes each run that none of the records it keeps names: an earlier generation's, or the attempt's own of an epoch no later than its newest record's. The attempt's own runs of later epochs are kept, since the next record may name them.

A record lists its epoch's quarantine files, with their rows by cause. The runner reports every file of a committed epoch to the control plane. The control plane keeps an index of the files' paths and counts, never their records. In the same transaction, it records a `stream.quarantined` event. `Prune` deletes the files of epochs that never committed. With `quarantine.retention`, `PruneQuarantine` also deletes the files of committed epochs that are older than the retention, by the time that the store gives each object.

## The commit protocol

Data is committed in **epochs**: the records between two checkpoints. For each one:

1. The coordinator requests barrier N. The source pump inserts it after each split's next resume point. This aligns the barrier with a position the source can resume from.
2. The barrier travels through the engine to each writer lane. After all its streams cross the barrier, each lane calls `PreCommit(N)` and returns a committable. The lane's data is durable. Stage-and-apply and transactional destinations keep it invisible until commit; deliver-and-acknowledge destinations have already delivered it.
3. The coordinator writes record N to the checkpoint log and moves `HEAD` to it. This is the commit point.
4. Lane 0's writer calls `Commit(N)` with every lane's committable. Stage-and-apply destinations make the epoch visible with stream markers protected by the destination fence. Commit is idempotent and can be retried. If retries fail, the next attempt's `Recover` applies the epoch from its recorded committables. A destination with lane commits is different: each lane's writer commits its own committable, as [Attempts on workers](#attempts-on-workers) describes.
5. The source receives the positions it can acknowledge upstream, such as event offsets or a replication position. This notification is one checkpoint behind.

Commits run one at a time, in epoch order, while barriers for later epochs go on being placed.

After claiming the pipeline, an attempt calls its writer's `Recover`. This raises the destination fence to the new generation, preventing zombie commits. It then checks the epoch referenced by `HEAD`.

The epoch can be already applied, recoverable from its committables, or lost. If it is lost, the log returns to the preceding record. The attempt reads from the resume states in the selected record. A record of a destination with lane commits, written on workers, is settled lane by lane instead, and is never rolled back; [Attempts on workers](#attempts-on-workers) describes how.

A delayed split can use a forced barrier without a resume point only when replay preserves the required delivery guarantee. Recovery from that checkpoint reads some records again. The [`test/sim`](../../test/sim) simulation exercises the protocol.

## Attempts on workers

An attempt can run on several runners: one runner coordinates it, and the others, its workers, run its lanes. They use the attempt protocol, `openetl.attempt.v1` ([`internal/runner/attempt`](../../internal/runner/attempt)). Each worker opens one call to the coordinator. The coordinator claims the pipeline, writes the records and decides when each epoch commits. It reads and writes no data. It starts the source and destination connectors, with their credentials, only to set them up and discover the streams. It then keeps the destination's capabilities and stops the connectors before the workers start, and the workers skip the connectors' setup. The attempt runs this way:

- Before the coordinator assigns work, the first worker opens the attempt's first writer lane. It raises the destination fence and settles the epoch that `HEAD` points to, as the recovery of a single runner does, unless the epoch is settled lane by lane, as described below. It also gets the splits of the reads that start.
- Each worker reads its splits with its own source connector and runs its own engine. It writes its writer lanes with its own destination writers.
- All the splits of these streams go to one worker:
  - a stream written with overwrite;
  - a stream that the attempt does not read;
  - a stream read with `cdc`, and every such stream goes to the same worker: a source reads their changes from one log, such as a Postgres replication slot, which only one read can hold.

  With a SQL transform, a stream that a statement reads goes to one worker if it is read with `cdc`, or an output written from it, or the stream passed through, is written with overwrite. A keyed stream whose splits do not keep a key's rows together has an exchange on its path, which crosses workers, as [the exchange between engines](#the-exchange-between-engines) describes; a dataflow that cannot exchange between engines (`runner.Meshing`), such as the runner's tests' `Passthrough`, keeps such a stream on one worker.
- A worker sends its share of each epoch when its part is durable. The share holds its splits' positions, its rows, its committables and what its engine reported. The coordinator merges the shares into one record. The record also has each committable's coverage: the splits whose rows it holds, where each split's part of the epoch began, how many rows the split had, and the key groups of those rows with the key that grouped them. With a SQL transform, the coverage names a split of a stream that a statement reads once for each output whose rows of it the committable holds (`CoveredSplit.output`): every row of an output without an exchange, where the split's lane fed the committable's writer lane, and the writer lane's key groups of an output with one, by the output's key, whichever lane read the split.
- After the record, the first worker applies schemas and commits at the coordinator's request. A destination with lane commits (`DestinationCapabilities.lane_commits`) is different: each worker commits its own writer lanes, each with its own committable. The coordinator then sends the record's positions to the workers. Each worker acknowledges only the positions of its own splits, one checkpoint behind.
- Each stream's schema has one owner, the coordinator. An engine whose lane changes a schema proposes the change from the version that it has. Integers past ±2^53 in a column are such a change too, because a later float then makes the column text. Every engine shapes by the versions that the coordinator accepts. Lanes of one engine share these changes through the engine's own owner of the stream, which follows the coordinator's versions.
- A version of one of the destination's streams also holds the columns that the stream holds whole, and the schema that the destination gets (`SchemasResponse.holding` and `delivered`). An engine that starts a column hold proposes it as a change, so the first decision is every worker's. The coordinator records the owner's columns and delivered schema, and adds up the workers' held cells and files. Each engine names its held files by the attempt's index of its writer lanes, so the workers' files do not collide.
- With a SQL transform, the streams that the statements read have versions of their own (`SchemasRequest.input`). An engine proposes what its lanes typed that the owner's version lacks: new columns, and types that hold the owner's. The record keeps the owner's schema of each input and of each output.
- The coordinator starts the holds as a single runner does: from the restored record, with the job's releases, and with the columns that the destination refused. It sends them in the assignment (`Assignment.holds`), and the first worker's `Recover` answer gives it the destination's table limits. Only the engine with the attempt's lane 0 patches in released holds. A refusal ends the attempt, and the coordinator claims the pipeline again at once with the refused columns held. The same worker pods join the newer generation.
- A lost worker or a lost coordinator fails the attempt. The next attempt restores every worker from the record, in a new layout.

With lane commits, an epoch can be committed in part, so the next attempt settles the restored record committable by committable ([`internal/runner/redo.go`](../../internal/runner/redo.go)). The coordinator sends each worker a `Settle` call before it assigns any work. Committables whose coverage shares a split go to the same worker. So do all the committables whose coverage holds a split of a stream read with `cdc`, since one read holds the log of their changes. While the coordinator waits for any call's answer, its heartbeats say so (`Heartbeat.calling`), and a worker without its assignment goes on waiting for it. For each committable, the worker opens a writer as the lane that staged it, and its `Recover` fences that lane. The committable then ends one of three ways:

- The writer finds the lane's marker, so the lane is applied.
- The writer commits the lane's staging, if the lane kept one.
- The writer reports it lost. The worker then reads each covered split again, on a read of its own that does not follow the source, from where its part of the epoch began, with the record's state for the split as its `until`. If the split completed in the epoch, that state is the read's `complete_at` instead, unless it is empty. The read stops at the resume point that the record has for the split, or at a completion at the state that the record has. The worker runs as many of these reads at once as the job has lanes, but reads the splits of streams read with `cdc` one after another. It fails with `POSITION_LOST` if it ends first, ends elsewhere, or returns fewer rows than the coverage counts, unless the split's barrier was forced. The coverage also names the primary key of a stream split by key group; under another key, the redo fails with `CONFIG_INVALID`. The worker sends the rows through an engine that keeps only the committable's key groups, writes them as that epoch's on the writer, and commits them.

  With a SQL transform, the engine runs the statements on the rows read again, and keeps of each output the committable's key groups, where the coverage names them. Splits that reach the committable through different sets of outputs, such as one whose lane fed the writer lane and one that only an exchange brought there, are read on different lanes of the engine, each writing a writer lane of its own with no exchange. The runner drops each lane's rows of the outputs the coverage does not name for its splits, and passes each stream's barrier on once every lane has passed it. The record keeps the SHA-256 of the statements that wrote the epoch (`sql_digest`), and other statements fail the redo with `CONFIG_INVALID`: they could write other rows than the epoch's other lanes committed. So does an output that the SQL no longer writes from the same stream, and, where the coverage names an output's key groups, a key other than the one that grouped them.

A single runner settles such a record the same way. On one runner, a destination with lane commits gets one writer lane, and a record without coverage is recovered whole, as before. Resplitting is off for such a destination, because a split added during an epoch cannot be read again as it was.

These jobs run on one pod:

- a job with a Spark or Flink transform;
- a job whose destination loads staged epochs or is written by the engine;
- a job whose destination is transactional and does not have lane commits;
- a job whose destination takes fewer writer lanes than there are workers;
- a job with a SQL statement that keeps state, as a group window, when the engines of its dataflow do not exchange rows between workers: its keyed lanes then take the stream from every lane of the attempt in one engine. Without the exchange, the engine refuses such a statement in a part of an attempt. It accepts one only in the part that a redo writes again.

`plan.OnePod` ([`internal/plan`](../../internal/plan)) is the one check behind these cases, for the plan, for `EnqueueWork` and for `Job.OnWorkers`. `EnqueueWork` works out an attempt's workers when it sizes the attempt, so the workflows and their histories do not change. The placer sends an attempt on workers only to a pod launcher of its pool, all of its pods or none, and the launcher's `--max-pods` and `--max-pod-cpu` count every pod. The launcher ([`internal/runner/pods`](../../internal/runner/pods)) makes the workers' pods, then the coordinator's.

A placement on a launcher holds its pods, CPU and memory in the `launches` table from the pass that places it. The room stays held after the placement ends or the attempt is placed again, until the launcher's report says that no pod of it is left that has not ended. The report lists the placements with such a pod, or whose pods it is still making, and `listed` says it could list its pods. A report that could not list them frees nothing. `PlacementRunners` counts the rows of placements that have ended as the launcher's unreaped pods, which `fits` adds to its caps.

The coordinator listens at `--attempt-listen` with a certificate that it makes, claims the pipeline, and makes a token for the attempt. `OpenWorkers` gives the API its address, certificate and token, which the dispatcher keeps in `dispatches.worker_target`. Each worker's pod calls `PollWork` with its index until the API has them, then joins with the token. Workers take no part in the attempt's lease or result. If the coordinator finds that the job cannot run on workers, it reports `one_pod`, and the dispatcher places the same attempt again on one pod.

### The exchange between engines

Where a destination's stream has an exchange and an attempt runs on several workers, the workers' engines exchange rows between them ([`crates/engine/src/mesh.rs`](../../crates/engine/src/mesh.rs)). The coordinator lays the attempt out with `plan.NewWorkerLayout`: the attempt's writer lanes own the key groups' equal ranges, each worker's writer lanes follow the worker before it is, and each worker cuts its own writer lanes' ranges among its lanes as one engine does (`Layout::meshed` in the engine). It caps a worker's lanes at its writer lanes' key groups. `Assignment.lanes[].key_groups` and `writer_lanes[].key_groups` carry the layout, and `Assignment.exchange` a certificate, its key and a secret that the coordinator makes for each generation. The workers' addresses come from their joins (`Join.address`, from `--exchange-listen` and `--exchange-advertise`, or 127.0.0.1 and a free port). Each worker gives its engine `StartJobRequest.mesh`: every engine's address, lanes and writer lanes, where to listen, and the identity.

A lane sends each row of an exchanged stream to the attempt's writer lane that owns its key group, and the stream's schema changes, watermarks, barriers and end to every writer lane of the attempt. A writer lane on another engine gets them over that engine's link: a queue bounded in bytes, an eighth of the job's memory split among the links and at most 8 MiB each, which a lane waits on when it is full, and a connection that writes the queue in order. Each engine dials every other once and listens for them, over TLS 1.3 with AES-128-GCM, pinned to the attempt's certificate. The dialer's first frame is `MeshHello`, with the attempt, the generation, its index and the secret. The listener checks them, refuses another attempt's or generation's, a wrong secret, or an engine already connected, and answers with its own `MeshHello` before it reads any data frame. After the hellos, each `ROUTE` frame names the sending lane and the receiving writer lane of the frames after it, which are the engine's own `BATCH`, `SCHEMA_CHANGE`, `BARRIER`, `WATERMARK` and `END_OF_STREAM` frames. The receiver gives them to its writer lanes as from its own lanes, so a writer lane aligns barriers, and takes its watermark as the least, over every lane of the attempt, connected yet or not. A slow writer lane slows its senders through its channel, the connection's flow control and the link's bound.

An engine fails its job as `TRANSIENT` if it cannot reach another engine in a minute, if another engine does not connect in a minute, if a connection breaks before the other engine's lanes have ended every exchanged stream, or if the other engine refuses it. TCP keepalive finds a dead peer of an idle connection, and on Linux, `TCP_USER_TIMEOUT` one with unacknowledged data. A listener reads a hello of at most 4 KiB, and handshakes with at most 64 connections at once.

Such a failure carries `PeerLost` in its status, and the worker reports it as `AttendRequest.peer_lost`. The coordinator then waits for a grace, 5 seconds by default (`attempt.Options.PeerLostGrace`), for another worker's own failure or loss, which says why. If one comes before the attempt finishes, the coordinator fails the attempt with it. If the grace passes with none and the attempt has not finished, it fails the attempt with the `PeerLost`. An attempt that finishes first keeps its result, and the coordinator ignores what workers report after that. A failure of another kind from the worker that reported the loss, such as its writer's error, also fails the attempt at once. The worker keeps its call after the report, and goes on with the rest of its part until the coordinator's `End` comes.

Every worker stops its engine once the coordinator ends the attempt, so the engine of a worker that hasn't seen the end yet loses the others. The coordinator ends the attempt before any worker learns of it, so it ignores a `PeerLost` that comes after a finish, and that worker's part ends as finished. If the coordinator processed a worker's `PeerLost` before it finished the attempt, that worker fails, though the attempt finished: its `End` carries the `PeerLost`.

Each committable's coverage in the record holds its key groups of every split of an exchanged stream that any worker read in the epoch; the coordinator adds the other workers' splits when it merges the shares. A redo of a lane's part reads all of them. A SQL transform's coverage names the splits of the streams it reads, not of the outputs it exchanges, so it gets none of the other workers' splits; only a destination with lane commits reads coverage, and a SQL transform into one runs on one pod.

A frame kind crosses engines once the engine's `mesh::wire` and `mesh::unwire` carry it; a writer lane's feed aligns it per sending lane as it does its own lanes' frames.

## Delivery levels

[`internal/delivery`](../../internal/delivery) works out each stream's guarantee from the destination's class, the source's replay, the write mode, the primary key and the level the pipeline requires:

- An exact source into a stage-and-apply or transactional destination is **exactly once**: the protocol alone keeps every record to one commit.
- A superset source written with merge into a stage-and-apply destination is **effectively once**: a repeat lands on the same key, guarded by `_openetl_seq`, so the table ends up the same.
- A superset source written with append or overwrite into a destination that can skip rows by `_openetl_id` when it applies is exactly once, when the pipeline requires more than at least once.
- A destination that delivers and acknowledges, and anything else, is **at least once**.

[`internal/plan`](../../internal/plan) uses it to plan a pipeline version from its source's latest catalog, and the runner checks again before each sync, against the catalog it discovers.
