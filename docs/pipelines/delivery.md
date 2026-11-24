---
title: Delivery guarantees
description: Understand delivery guarantees, review their requirements, and remove duplicate changes when reading data.
---

Each stream has a delivery guarantee that describes whether retries or repeated reads can produce duplicate changes. This page explains the guarantees, how OpenETL selects them and how to interpret a plan. It also shows queries for reading data delivered at least once. [Concepts](../get-started/concepts.md#delivery-guarantees) provides an introduction.

The examples use the [official Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md). The source `shop-db` has two tables: `orders` and `page_views`. `orders` has a primary key `id` and an `updated_at` cursor column. `page_views` has no primary key. Both write to the `warehouse` destination.

Check the capabilities documented for your connector version. OpenETL determines guarantees from those capabilities, rather than from the system's name.

## The guarantees

A row inserted and then updated twice has three **versions**. Each version has an `_openetl_seq` that identifies its source order and an `_openetl_id` that identifies the change. A delivery guarantee concerns repeated writes of the same version. From the strongest to the weakest:

| Guarantee | What can happen | What you do when reading |
|---|---|---|
| `exactly_once` | Each version is committed once, including after failures and retries. | No deduplication is required. |
| `effectively_once` | A repeated version updates the same row. The final row is equivalent to one write, but `_openetl_synced_at` changes with each write. | No deduplication is required. Do not use `_openetl_synced_at` as the source change time. |
| `exactly_once_within` | Only the output of a [dedupe](./dedupe.md) with a retention gets it. A row of a key whose event time is less than the retention after the key's first row is not written. A row at or after the first row's time plus the retention can be written as the key's next first row. | A key can have a second row after the retention. To keep one row per key, keep the earliest row of each key of the dedupe's `PARTITION BY`. |
| `at_least_once` | Duplicate rows can contain the same version and `_openetl_id`. | Keep one row per `_openetl_id`. See [reading a stream delivered at least once](#reading-a-stream-delivered-at-least-once). |

Append writes store each version as a separate row under every guarantee. Different versions of the same source row are separate changes. For a stream read with `incremental`, this needs a cursor that has a different value for each version. See [Choose a cursor that changes on every update](#choose-a-cursor-that-changes-on-every-update). To select only the latest version, [group by primary key](#reading-a-stream-delivered-at-least-once).

### How the guarantees rank

A `require` accepts its own guarantee and every guarantee above it in the table, and refuses those below it. `exactly_once_within` ranks below `effectively_once` and above `at_least_once`:

- **Below `effectively_once`.** A repeat whose event time is at or after the first row's time plus the retention is written as a second row of the key. `effectively_once` never gives a second row.
- **Above `at_least_once`.** A repeat whose event time is less than the retention after the first row is never written. `at_least_once` does not promise that.

So the default `exactly_once` refuses `exactly_once_within`, and so does `effectively_once`. A `require` of `exactly_once_within` or `at_least_once` accepts it. [Accept a dedupe's retention](#accept-a-dedupes-retention) shows how.

## What decides a stream's guarantee

**Source replay** describes the records returned after resuming a read:

- **Exact** replay returns exactly the records after the saved position.
- **Superset** replay can return previously read records as well.

The source connector declares its replay behavior. CDC and event streams usually provide exact replay. Incremental reads always use superset replay: they include rows at the previous highest cursor value again. `openetl discover` shows the source's declaration for each stream:

```sh
openetl discover shop-db
```

```text
orders (exact; key id)
  id          int64                  nullable
  status      utf8                   nullable
  amount      decimal(10, 2)         nullable
  updated_at  timestamp[us, tz=UTC]  nullable
page_views (exact)
  path       utf8                   nullable
  viewed_at  timestamp[us, tz=UTC]  nullable
```

`exact` is how the source replays these tables in a full refresh or by CDC. Read with `incremental`, the same tables replay as superset.

**The destination: how it commits.** A destination makes each commit visible in one of three ways:

- It stages records and applies them in one step (**stage and apply**). Exact replay gives `exactly_once`. Superset replay with merge gives `effectively_once`, because repeated versions update the same row. Superset replay with append or overwrite gives `at_least_once` unless the destination can skip existing `_openetl_id` values. That capability gives `exactly_once` through **deduplication at apply** when the stream requires more than at least once.
- It writes each commit in a transaction (**transactional stream**). An exact stream gets `exactly_once`, and a superset stream `at_least_once`.
- It delivers records as they are read and waits for each to be acknowledged (**deliver and acknowledge**). A delivery can be retried and land twice, so every stream gets `at_least_once`.

**The stream: its write mode and its key.** Merge needs a primary key. A stream without one is covered [below](#streams-without-a-primary-key).

## What a pipeline requires

`delivery.require` sets the minimum guarantee for the pipeline. The default is `exactly_once`. A stream's `require` overrides that requirement for the stream. Deduplication needs an index on `_openetl_id` and a lookup for each row. OpenETL enables it only when the stream requires more than at least once.

Here is an incremental read written with merge:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-orders
spec:
  schedule: { every: 1h }
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: incremental
        cursor: updated_at
  destination: { ref: warehouse, namespace: shop, write: merge }
```

```sh
openetl plan -f shop-orders.yaml
```

```text
would be created workspaces/default/pipelines/shop-orders (version 3714e5de146d)

workspaces/default/pipelines/shop-orders (version 3714e5de146d)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    orders  merge  effectively_once  superset replay, written with merge
  partitioning:
    orders  split by id -> writer, needs id
  problems:
    delivery.require: stream "orders" gets effectively_once (superset replay, written with merge), below exactly_once
openetl: the plan has problems, so apply would refuse it
```

The stream cannot have exactly once, so the plan fails, and `openetl apply` would refuse the file. Either accept what merge gives, which makes the plan's streams section read:

```yaml
  delivery:
    require: effectively_once
```

```text
  streams:
    orders  merge  effectively_once  superset replay, written with merge
```

Or write with append, the default, which Postgres dedupes at apply, for this streams section:

```text
  streams:
    orders  append  exactly_once  superset replay, deduped at apply
```

### Accept a dedupe's retention

A first-row [dedupe](./dedupe.md) with a `dedupe.retention` duration gets `exactly_once_within` over a stream that can repeat rows and is not read with `full_refresh`, into a destination that stages and applies or writes each commit in a transaction, unless it is merged into a destination that stages and applies. The default `require` refuses it. The plan's problem names the retention and the two ways to accept it:

```text
  problems:
    delivery.require: stream "events_once" gets exactly_once_within (a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, and writes a repeat after that again), below exactly_once: to accept that a key is written again by a row 24h0m0s or more of event time after its first row, set delivery.require: exactly_once_within; to remember every key from then on, set source.streams[events].dedupe.retention: unbounded (a key already forgotten stays forgotten)
```

Do one of these:

- To accept that a key can be written again by a row at or after its first row's time plus the retention, set `require: exactly_once_within`. Set it in `delivery.require` for the pipeline. If the statement writes a stream's name and so replaces it, set it in that stream's `require` instead.
- To remember every key from then on, set the stream's `dedupe: { retention: unbounded }`. The output is then `exactly_once`, and the dedupe's state grows with the number of keys. A key that the dedupe already forgot stays forgotten, so its next row is written again, and rows already in the destination stay there.
- If the destination stages and applies, write the output with `merge`, so that such a row updates its key's row. The output is then `effectively_once`, which the default `require` also refuses, so also set `require: effectively_once`.

For example, to accept the retention for the whole pipeline:

```yaml
  delivery:
    require: exactly_once_within
```

Run `openetl plan` again. The problem is gone, and the streams section shows the output's guarantee:

```text
  streams:
    events_once  append  exactly_once_within  a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, and writes a repeat after that again
```

## The plan's reasons

`openetl plan` gives each stream's write mode, guarantee and reason, and the console shows the same under **Delivery** in a pipeline's **Streams** table. Some reasons speak of an **epoch**, which is what one commit makes visible: everything a stream read since the commit before. The reasons are:

| Reason | Guarantee | What it means |
|---|---|---|
| `exact replay` | `exactly_once` | The stream replays exactly, and the destination commits in one step. |
| `superset replay, deduped at apply` | `exactly_once` | The destination skips rows whose `_openetl_id` it already has. |
| `superset replay, written with merge` | `effectively_once` | A repeated version is merged onto its own row again. |
| `superset replay, no primary key: collapsed within each epoch and against the target` | `exactly_once` | Rows with the same content become one row. See [streams without a primary key](#streams-without-a-primary-key). |
| `superset replay, no primary key: collapsed within each epoch` | `at_least_once` | Rows with the same content become one row within each commit, but a repeat of a row an earlier commit wrote lands again. |
| `superset replay, no primary key, and not collapsed` | `at_least_once` | Every row lands, repeats included. |
| `superset replay, not deduped under at_least_once` | `at_least_once` | The destination could dedupe, but the stream requires only at least once. |
| `superset replay, and the destination can't dedupe` | `at_least_once` | Repeats land, since the destination cannot skip them. |
| `the destination delivers and acknowledges` | `at_least_once` | A retried delivery can land twice. |
| `a transform's output over exact inputs` | `exactly_once` | A [transform](../integrate/index.md)'s output, when every stream it reads replays exactly. |
| `a transform's output over superset inputs, written with merge` | `effectively_once` | As for merge above. |
| `a transform's output over superset inputs, which isn't deduped unless written with merge` | `at_least_once` | Once your code has changed the rows, an `_openetl_id` cannot tell a repeated input from a new one. |
| `a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, and writes a repeat after that again` | `exactly_once_within` | A SQL [dedupe](./dedupe.md) that keeps each key's first row drops a row of a key whose event time is less than the retention after the key's first row. A row at or after the first row's time plus the retention can be written as the key's next first row. |
| `a dedupe's output over superset inputs, which remembers a key 24h0m0s of event time from its first row, written with merge, which merges a repeat after that onto its key's row` | `effectively_once` | The same dedupe, written with merge into a destination that stages and applies. A row at or after the first row's time plus the retention updates its key's row. |
| `a dedupe's output over superset inputs, which remembers every key, so its state grows with the number of keys` | `exactly_once` | A SQL [dedupe](./dedupe.md) with an `unbounded` retention, or one that keeps each key's latest row, drops every repeat. |
| `a dedupe's output read with full_refresh, overwritten with each sync's read, in which it drops every repeat` | `exactly_once` | A dedupe over a stream read with `full_refresh`, overwritten: each sync replaces the table with its own read, in which the dedupe drops every repeat. |
| `a dedupe's output read with full_refresh: each full read starts with no keys kept, so every sync writes every key again, written with merge, which merges it onto its key's row` | `effectively_once` | The same, merged into a destination that stages and applies. |
| `a dedupe's output read with full_refresh: each full read starts with no keys kept, so every sync writes every key again` | `at_least_once` | The same, appended, or merged into a destination that writes each commit in a transaction: each sync adds every key's rows again. |

The three `full_refresh` reasons apply to every dedupe, whatever its retention and whatever the stream's replay. Each full read starts with no keys kept, so each sync writes again every row that the dedupe keeps from its read. With a duration, they are each key's first row, then the key's earliest row at or after that row's time plus the retention, and so on. With `unbounded`, they are each key's first row. A latest-row dedupe writes each key's latest row again, as upserts.

A stream that its source replays exactly returns, after a restore, exactly the records after the saved position, and the dedupe's state is restored at that position. So a dedupe's output over streams that all replay exactly, and that are not read with `full_refresh`, gets `a transform's output over exact inputs`, whatever its retention, into a destination that stages and applies or writes each commit in a transaction. Into a destination that delivers and acknowledges, a dedupe's output gets `at_least_once`, as every stream does.

## Choose a cursor that changes on every update

An incremental read tells the versions of a row apart by its cursor value. For a stream with a primary key, the `_openetl_id` is a hash of the stream, the key and the cursor value. The `_openetl_seq` is the cursor value. Each read starts at the previous highest cursor value, and includes it.

A date, or a timestamp stored in whole seconds, can give two changes of a row one cursor value: two changes in one day, or in one second. If a sync reads the row between the two changes, OpenETL writes the first version, and the next sync reads the second version at the same cursor value.

In a stream deduped at apply, which `openetl plan` shows as `superset replay, deduped at apply`, OpenETL keeps a hash of the values of each version that a read gives an `_openetl_id`. It keeps them for the rows of the stream's last two reads. A row read again with other values than the version that holds its ID gets an `_openetl_id` of its own, made from that ID and its values, so the destination writes it. A row read again unchanged keeps its ID, and the destination skips it as a repeat. Both versions have the same `_openetl_seq`.

The table keeps the first version, and the second is skipped as a repeat, in these cases:

- **The first read after an upgrade** to a version of OpenETL that keeps these hashes, or after the stream becomes deduped at apply. A second version that was waiting to be read is skipped. Later versions land.
- **A row that neither of the last two reads gave.** Its hash is gone.
- **A schema change.** A column that the stream gains or loses, or a column that changes to another kind of value, such as from a number to text, starts the hashes again. A widening, such as from `int32` to `int64`, does not.
- **A stream that a SQL transform reads, and the outputs of a transform.** OpenETL keeps no hashes for them.

In these cases, the table ends with another row than the source has:

- **A row that changes back within one cursor value** to a version that a read already gave. That version holds its `_openetl_id`, so the destination skips the change back as a repeat, and the table ends with the version before. For example, the reads give the versions A, B, C and then B again: the table ends with C.
- **A column whose value changes on every read** while the row does not change, such as a computed value, the time a source fetched the row, or JSON whose keys come in another order. Each read gives the row new values, so the rows at the previous highest cursor value land again on every sync. List such columns in the stream's [`volatileColumns`](../reference/specs/pipeline.md#source-and-streams), which OpenETL leaves out of the hash.
- **A held cell whose source text differs from its value's shortest form.** For example, `amount` is an `int64` column, the source sends `12.50`, and OpenETL holds the cell. While the cell is held, the row's hash is of its text, `12.50`. Then the column becomes a decimal, as when the source's table changes it to `DECIMAL(10,2)`, and the [release](./schema-changes.md) patches the value in. From then on, the row's hash is of its value, `12.5`. So the next read of the row at the previous highest cursor value lands it once more, beside the row that the table has.
- **A destination with lane commits on workers** reads a lane's part of an epoch again from the source when the lane did not commit ([When a pod fails](./workers.md#when-a-pod-fails)). If the source changed a row since the epoch, the newer version lands under an `_openetl_id` of its own, and the version that the epoch read does not land. A later read that has no hash for the row lands the newer version again, under the row's first `_openetl_id`: for example, a read after a [reset](../reference/pipeline-cli.md#reset-and-rewind) once the row's hash has expired.

Use a cursor that every update changes, at a finer precision: a timestamp with microseconds, a row version or a sequence. Then each version has an `_openetl_id` and an `_openetl_seq` of its own.

Versions of a row at one cursor value have the same `_openetl_seq`. To read the latest version of each row, order the row's versions by `_openetl_seq` descending, then by `_openetl_synced_at` descending. The later version was written later.

Where OpenETL keeps no hashes, a stream deduped at apply skips the second version. If you cannot change the cursor of such a stream, and it is appended, set `require: at_least_once` on it. The destination then writes the second version too. It also writes again every row that a sync reads again, such as the rows at the previous highest cursor value. With a date cursor, these are all the rows changed on that day. Both versions have the same `_openetl_id`, so do not keep one row per `_openetl_id`: that drops the second version.

The plan warns about a stream that has these properties:

- It has a primary key and is read with `incremental`.
- It is deduped at apply.
- The source's catalog gives its cursor one of these types: `date32`, `date64`, `timestamp[s]` with or without a time zone, or `time32[s]`.

The warning starts with `its cursor "updated_at" is timestamp[s, tz=UTC], so two changes of a row can have one cursor value`. For a stream whose versions OpenETL tells apart, it goes on with the change back: a row that changes back within one cursor value to a version that a read already gave ends as the version before. For a stream that a SQL transform reads, where OpenETL keeps no hashes, it says that the apply skips the second version. The plan cannot see the precision of these cursors, so check them yourself:

- an integer that holds seconds since 1970, such as a declarative source's `unix` cursor;
- a timestamp with microseconds whose values are whole seconds, a type that a source can give to a database column that stores whole seconds;
- text, such as an RFC 3339 time.

## Commits, as a reader sees them

A sync reads and commits data in epochs. A commit is requested when any of these conditions occurs:

- `delivery.commit.interval` elapses. The scheduled-pipeline default is 5 minutes.
- The data read since the last commit reaches `delivery.commit.maxBytes`. The default is `256MB`.
- The read ends.

Uncommitted data is not visible in the destination. With `delivery.commit.scope: pipeline`, a commit makes all streams' rows visible together. This is the default when supported by the destination. With `stream`, each stream commits separately. A job on several workers with a destination that has lane commits is different: each writer lane commits its part of an epoch on its own, so one lane's part can be visible before another's. [Run a job on several pods](./workers.md#when-a-pod-fails) describes how such a job recovers.

A failed sync retains its committed data. The retry or next sync resumes from the last commit. Readers can therefore see part of a failed sync. For overwrite writes, the table is replaced only when the read ends. Until then, readers see the previous table.

A continuous pipeline commits every 10 seconds, unless the destination or `delivery.commit.interval` says otherwise. [The reference](../reference/specs/pipeline.md) has every `delivery` field.

## Streams without a primary key

Without a primary key, a row's identity depends on its whole content. A repeated read and a separate identical source row can therefore have the same `_openetl_id`. OpenETL cannot distinguish them. `collapse` controls whether to combine identical rows. The following choices apply to superset replay into a destination that supports deduplication at apply:

| `collapse` | `require` | What lands | What it costs |
|---|---|---|---|
| `true` | Above at least once | One row for each distinct content: repeats never land, and nor does the second of two identical rows. `exactly_once`. | A lookup for each row in an index on `_openetl_id`, which costs more as the table grows |
| `true` | `at_least_once` | Rows with the same content become one within each commit only. Repeats from earlier commits land again, such as the rows each incremental read reads again. | A `DISTINCT ON` over each commit's staged rows |
| `false` | `at_least_once` | Every row, repeats included. | Nothing |
| `false` | Above at least once | The plan fails, so nothing is applied. | |

`collapse` is `true` by default for a stream that may replay a superset and requires more than at least once, and `false` otherwise. Only a destination that dedupes at apply can collapse rows; for any other, the plan fails a stream with `collapse: true`. On a stream with a primary key, `collapse` does nothing, and the plan warns that it is ignored.

The plan warns about each stream without a primary key, with what its settings cost and what the others would. This pipeline reads `page_views` incrementally and keeps every row:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: shop-orders
spec:
  schedule: { every: 1h }
  source:
    ref: shop-db
    streams:
      - name: orders
        sync: incremental
        cursor: updated_at
      - name: page_views
        sync: incremental
        cursor: viewed_at
        collapse: false
        require: at_least_once
  destination: { ref: warehouse, namespace: shop }
```

```text
would be created workspaces/default/pipelines/shop-orders (version b88880ab7505)

workspaces/default/pipelines/shop-orders (version b88880ab7505)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  streams:
    orders      append  exactly_once   superset replay, deduped at apply
    page_views  append  at_least_once  superset replay, no primary key, and not collapsed
  partitioning:
    orders      split by id -> writer, needs id
    page_views  unpartitioned -> writer
  warnings:
    page_views: it has no primary key and doesn't collapse, so every row lands: both copies of a row the source holds twice, and repeats, such as the lookback window each sync reads again. Readers can dedupe on _openetl_id. collapse: true collapses rows with the same content within each epoch, and with require: exactly_once against the target too, so repeats never land.
```

Exact replay without a primary key preserves every row by default. For a guarantee above at least once, OpenETL commits only at positions the source can resume from. If the source cannot order the table, it cannot provide a resume point before the read ends. A large table can therefore delay commits and require staging space for the whole table.

With `collapse: true`, OpenETL can commit before the read ends. After a failure, identical rows collapse into one for the rest of the retry. The plan warns about this behavior.

## Reading a stream delivered at least once

Under `at_least_once`, nothing removes repeats. They come from three places:

- **The previous incremental cursor value.** Each read includes rows at the previous highest cursor value. Those rows are written again on each sync. Some sources read from further back. For example, a declarative source's stream that is not `ordered` reads from the highest cursor of the sync before the previous one. All rows changed since then are written again. Its second sync, and the second after a state reset, writes every row again.
- **A retried delivery** to a destination that delivers and acknowledges.
- **A repeated read after a failure.** OpenETL normally commits at source resume points. After `delivery.resumePointTimeout` (`1m`) without one, it can commit using a **forced barrier**. This is permitted only if replay preserves the required guarantee. Recovery resumes from the last source resume point and reads some records again. Higher guarantees require deduplication, collapse or merge; `at_least_once` permits duplicate rows.

Here is the `orders` table after three syncs of a pipeline that requires `at_least_once`. The table has 100 orders, 11 of which share the highest `updated_at`; between the second and third syncs, order 5 was updated:

```sql
select count(*) as rows, count(distinct _openetl_id) as versions, count(distinct id) as orders
from shop.orders;
```

```text
 rows | versions | orders
------+----------+--------
  123 |      101 |    100
```

The 11 rows with the highest `updated_at` landed three times, and order 5 has two versions. The same pipeline requiring `exactly_once` has 101 rows: Postgres skipped the repeats at apply.

To keep one row for each version, keep one per `_openetl_id`. This gives 101 rows:

```sql
select distinct on (_openetl_id) *
from shop.orders
order by _openetl_id;
```

To keep the latest version of each order, keep the row with the highest `_openetl_seq` for each primary key. This gives 100 rows, with order 5 as it was last updated. Do not use `_openetl_id` for this: each version has an ID of its own, if the cursor [changes on every update](#choose-a-cursor-that-changes-on-every-update). If it does not, order by `_openetl_synced_at` after `_openetl_seq`.

```sql
select distinct on (id) *
from shop.orders
order by id, _openetl_seq desc;
```

A view saves everyone who reads the table from writing it again:

```sql
create view shop.orders_latest as
select distinct on (id) *
from shop.orders
order by id, _openetl_seq desc;
```

[Reading what OpenETL writes](../integrate/read-data.md#reading-what-openetl-writes) has every `_openetl_*` column, and the same queries with `row_number()` for engines without `distinct on`.

### Which commits were read again

The **Checkpoints** tab lists commits from the last 30 days, newest first. Each entry shows its **Epoch** and per-stream row counts. A lightning icon marks a forced barrier.

After recovery from a forced barrier, an entry can show a guarantee below the planned level, such as **at least once**. Its rows can include changes written by earlier commits. Hover over the guarantee to see the planned level.

## When the plan can't tell

The plan needs the source's latest catalog and the destination connector's capabilities. `openetl discover` refreshes the catalog. The destination connector's package supplies its capabilities.

For a local executable without a package, the plan reports that it cannot check guarantees in advance. The console's **Delivery** column shows **Checked when it syncs**. The runner checks before each sync. [Package and publish](../build-connectors/publish.md#package-and-publish) explains connector packages; `openetl connector apply` installs them.
