---
title: Reading from the start again
description: Reset stream positions, recover from an unreadable checkpoint, and monitor source retention while a pipeline is stopped.
---

A pipeline resumes each sync from its last committed position. This can be a source log position, event stream offset or incremental cursor. This guide explains how to reset positions, recover from checkpoint errors and monitor retention while the pipeline is stopped.

## Read streams again from the start

`openetl reset` removes saved stream positions. The next sync or continuous run reads the affected streams as new streams. The source connector determines the starting point, such as a new snapshot or a configured event offset. Before resetting, check the connector's documentation for retention requirements and starting-position configuration.

```sh
openetl reset orders-to-warehouse                  # every stream
openetl reset --stream public.orders orders-to-warehouse
```

A continuous pipeline restarts at a checkpoint to apply the reset. A paused pipeline applies it after you resume the pipeline. A separately paused stream applies it after you resume that stream.

A reset does not directly delete destination data. The next write depends on the write mode:

| Write mode | What a reset does |
|---|---|
| `merge` | Updates each row in place. Rows deleted at the source since the last read stay in the destination. |
| `append` | Adds the rows again. |
| `overwrite` | Replaces the table, as every full refresh does. |

After a CDC reset, a new snapshot contains current rows. It does not recover intermediate changes. An appended change log therefore has a gap.

A pipeline with a Flink transform can't be reset, since Flink keeps state of its own; delete it and apply it again.

### After a lost position

If the source no longer retains a saved position, the sync fails with `POSITION_LOST`. This can happen after failover or when unread data expires. A **Position lost** incident opens and pauses the pipeline.

Check the reset consequences above and the connector's recovery instructions. Reset the affected positions. Resume the pipeline:

```sh
openetl reset orders-to-warehouse
openetl resume orders-to-warehouse
```

If the connector shares a source position across streams, follow its recovery instructions and reset all affected streams together. Resetting only one can leave other streams pointing at the position that was lost.

## When the latest checkpoint can't be read

Each commit writes a checkpoint record under `_checkpoints` in the data plane's storage. If the latest record is missing or damaged, recovery can use the preceding record. This reads an already delivered **epoch**, the data between two checkpoints, again. The pipeline's policy and delivery guarantees determine whether recovery is automatic:

- With `heal.mode: auto_safe` (the default) or `auto`, recovery is automatic only where it preserves each stream's [delivery guarantee](./delivery.md). This includes merge writes, deduplication at apply and streams already delivered at least once. A **Checkpoint corrupt** incident records the recovery and resolves after the next commit.
- Otherwise the sync fails with `CHECKPOINT_CORRUPT`, the incident pauses the pipeline, and its error says why the attempt didn't restart. To restart anyway, accepting that streams written with `append` and not deduped get the epoch's rows twice:

  ```sh
  openetl rewind --yes orders-to-warehouse
  openetl resume orders-to-warehouse
  ```

A Flink transform cannot restart from an earlier OpenETL checkpoint because its state corresponds to the latest checkpoint. Recovery requires deleting and applying the pipeline again. This creates a new pipeline; check the source starting position and destination effects before proceeding.

## Retention while a pipeline isn't running

A source can limit retained data by time or size. Its connector documentation describes the retention settings. If supported, an active read reports how much retention the pipeline has used.

For a stopped pipeline, OpenETL checks retention when no sync is running and no report has arrived for 15 minutes. It checks every 15 minutes through a runner in the pipeline's data plane, without reading records. The connector must declare support for this check.

A **Retention at risk** incident opens at 50% retention usage. It becomes critical at 80% and resolves below 40%. Resume the pipeline or increase sync frequency before the source discards unread data.
