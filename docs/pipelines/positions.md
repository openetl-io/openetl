---
title: Reading from the start again
description: Read a pipeline's streams again from the start, restart it from an earlier checkpoint, and keep a paused pipeline's source from discarding what it hasn't read.
---

A pipeline carries on each sync from where the last one committed: a replication slot's position, Kafka offsets, Kinesis sequence numbers, or an incremental stream's cursor. This page covers what to do when it can't, and how OpenETL watches a source's retention while a pipeline isn't running.

## Read streams again from the start

`openetl reset` drops the positions of a pipeline's streams, so its next sync, or a continuous pipeline's run, reads them as it would a new stream's. Postgres CDC takes a new snapshot through the pipeline's replication slot, making a new slot if the old one is gone or invalidated; Kafka and Kinesis start where `startOffset` or `startPosition` says.

```sh
openetl reset orders-to-warehouse                  # every stream
openetl reset --stream public.orders orders-to-warehouse
```

A continuous pipeline's run restarts at a checkpoint to take it. A paused pipeline takes it once you resume it, and a stream paused on its own reads from the start once you resume the stream.

What reaches the destination already stays there:

| Write mode | What a reset does |
|---|---|
| `merge` | Updates each row in place. Rows deleted at the source since the last read stay in the destination. |
| `append` | Adds the rows again. |
| `overwrite` | Replaces the table, as every full refresh does. |

A stream read with CDC gets the rows as they are now, not the changes made in between, so a stream appended as a log of changes has a gap.

A pipeline with a Flink transform can't be reset, since Flink keeps state of its own; delete it and apply it again.

### After a lost position

When the source no longer has a pipeline's position, as when a replication slot is lost in a failover or invalidated by `max_slot_wal_keep_size`, or Kafka's or Kinesis's retention deleted records the read hadn't reached, the sync fails with `POSITION_LOST`. A **Position lost** incident opens and pauses the pipeline. Read again from the start and resume it:

```sh
openetl reset orders-to-warehouse
openetl resume orders-to-warehouse
```

For Postgres CDC, reset every stream: they share the slot, and a read with any stream still holding a position in a lost slot fails again.

## When the latest checkpoint can't be read

Each commit writes a checkpoint record to the data plane's storage under `_checkpoints`. If the one the pipeline would carry on from is missing or damaged, an attempt restarts from the record before it. That record's epoch, the data between the two checkpoints, was delivered already, so it's delivered again:

- With `heal.mode` `auto_safe`, the default, or `auto`, the attempt restarts by itself where that can't lower a stream's [delivery guarantee](./delivery.md): streams written with `merge`, deduped at apply, or delivered at least once anyway. A **Checkpoint corrupt** incident records that it did, and resolves once the pipeline commits again.
- Otherwise the sync fails with `CHECKPOINT_CORRUPT`, the incident pauses the pipeline, and its error says why the attempt didn't restart. To restart anyway, accepting that streams written with `append` and not deduped get the epoch's rows twice:

  ```sh
  openetl rewind --yes orders-to-warehouse
  openetl resume orders-to-warehouse
  ```

A pipeline with a Flink transform doesn't restart from an earlier checkpoint, since Flink's own checkpoint goes with the latest; delete it and apply it again.

## Retention while a pipeline isn't running

A source keeps what a pipeline hasn't read for only so long: a replication slot up to `max_slot_wal_keep_size`, a Kafka topic for its `retention.ms`, a Kinesis stream for its retention period. A running read reports how much of that it has used. When a pipeline hasn't reported for 15 minutes and no sync is running, as when it's paused or between syncs, OpenETL asks its source on a runner of its data plane every 15 minutes, without reading. At half of the source's retention a **Retention at risk** incident opens, critical at 80%, and it resolves under 40%. Resume the pipeline, or sync it more often, before the source discards what it hasn't read.
