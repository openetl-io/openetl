---
title: Delivery guarantees
description: Understand delivery guarantees, review their requirements, and remove duplicate changes when reading data.
---

Each stream has a delivery guarantee that describes whether retries or repeated reads can produce duplicate changes. This page explains the guarantees, how OpenETL selects them and how to interpret a plan. It also shows queries for reading data delivered at least once. [Concepts](../get-started/concepts.md#delivery-guarantees) provides an introduction.

The examples use the [official Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md). The source `shop-db` has two tables: `orders` and `page_views`. `orders` has a primary key `id` and an `updated_at` cursor column. `page_views` has no primary key. Both write to the `warehouse` destination.

Check the capabilities documented for your connector version. OpenETL determines guarantees from those capabilities, rather than from the system's name.

## The three guarantees

A row inserted and then updated twice has three **versions**. Each version has an `_openetl_seq` that identifies its source order and an `_openetl_id` that identifies the change. A delivery guarantee concerns repeated writes of the same version:

| Guarantee | What can happen | What you do when reading |
|---|---|---|
| `exactly_once` | Each version is committed once, including after failures and retries. | No deduplication is required. |
| `effectively_once` | A repeated version updates the same row. The final row is equivalent to one write, but `_openetl_synced_at` changes with each write. | No deduplication is required. Do not use `_openetl_synced_at` as the source change time. |
| `at_least_once` | Duplicate rows can contain the same version and `_openetl_id`. | Keep one row per `_openetl_id`. See [reading a stream delivered at least once](#reading-a-stream-delivered-at-least-once). |

Append writes store each version as a separate row under every guarantee. Different versions of the same source row are separate changes. To select only the latest version, [group by primary key](#reading-a-stream-delivered-at-least-once).

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
- It delivers records as they're read and waits for each to be acknowledged (**deliver and acknowledge**). A delivery can be retried and land twice, so every stream gets `at_least_once`.

**The stream: its write mode and its key.** Merge needs a primary key. A stream without one is covered [below](#streams-without-a-primary-key).

## What a pipeline requires

`delivery.require` sets the minimum guarantee for the pipeline. The default is `exactly_once`. A stream's `require` overrides that requirement for the stream. Deduplication needs an index on `_openetl_id` and a lookup for each row. OpenETL enables it only when the stream requires more than at least once.

Here's an incremental read written with merge:

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
  problems:
    delivery.require: stream "orders" gets effectively_once (superset replay, written with merge), below exactly_once
openetl: the plan has problems, so apply would refuse it
```

The stream can't have exactly once, so the plan fails, and `openetl apply` would refuse the file. Either accept what merge gives, which makes the plan's streams section read:

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
| `superset replay, and the destination can't dedupe` | `at_least_once` | Repeats land, since the destination can't skip them. |
| `the destination delivers and acknowledges` | `at_least_once` | A retried delivery can land twice. |
| `a transform's output over exact inputs` | `exactly_once` | A [transform](../integrate/index.md)'s output, when every stream it reads replays exactly. |
| `a transform's output over superset inputs, written with merge` | `effectively_once` | As for merge above. |
| `a transform's output over superset inputs, which isn't deduped unless written with merge` | `at_least_once` | Once your code has changed the rows, an `_openetl_id` can't tell a repeated input from a new one. |

## Commits, as a reader sees them

A sync reads and commits data in epochs. A commit is requested when any of these conditions occurs:

- `delivery.commit.interval` elapses. The scheduled-pipeline default is 5 minutes.
- The data read since the last commit reaches `delivery.commit.maxBytes`. The default is `256MB`.
- The read ends.

Uncommitted data is not visible in the destination. With `delivery.commit.scope: pipeline`, a commit makes all streams' rows visible together. This is the default when supported by the destination. With `stream`, each stream commits separately.

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

`collapse` is `true` by default for a stream that may replay a superset and requires more than at least once, and `false` otherwise. Only a destination that dedupes at apply can collapse rows; for any other, the plan fails a stream with `collapse: true`. On a stream with a primary key, `collapse` does nothing, and the plan warns that it's ignored.

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
  warnings:
    page_views: it has no primary key and doesn't collapse, so every row lands: both copies of a row the source holds twice, and repeats, such as the lookback window each sync reads again. Readers can dedupe on _openetl_id. collapse: true collapses rows with the same content within each epoch, and with require: exactly_once against the target too, so repeats never land.
```

Exact replay without a primary key preserves every row by default. For a guarantee above at least once, OpenETL commits only at positions the source can resume from. If the source cannot order the table, it cannot provide a resume point before the read ends. A large table can therefore delay commits and require staging space for the whole table.

With `collapse: true`, OpenETL can commit before the read ends. After a failure, identical rows collapse into one for the rest of the retry. The plan warns about this behavior.

## Reading a stream delivered at least once

Under `at_least_once`, nothing removes repeats. They come from three places:

- **The previous incremental cursor value.** Each read includes rows at the previous highest cursor value. Those rows are written again on each sync.
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

To keep the latest version of each order, keep the row with the highest `_openetl_seq` for each primary key. This gives 100 rows, with order 5 as it was last updated. Don't use `_openetl_id` for this: each version has an ID of its own.

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

[Reading what OpenETL writes](../integrate/index.md#reading-what-openetl-writes) has every `_openetl_*` column, and the same queries with `row_number()` for engines without `distinct on`.

### Which commits were read again

The **Checkpoints** tab lists commits from the last 30 days, newest first. Each entry shows its **Epoch** and per-stream row counts. A lightning icon marks a forced barrier.

After recovery from a forced barrier, an entry can show a guarantee below the planned level, such as **at least once**. Its rows can include changes written by earlier commits. Hover over the guarantee to see the planned level.

## When the plan can't tell

The plan needs the source's latest catalog and the destination connector's capabilities. `openetl discover` refreshes the catalog. The destination connector's package supplies its capabilities.

For a local executable without a package, the plan reports that it cannot check guarantees in advance. The console's **Delivery** column shows **Checked when it syncs**. The runner checks before each sync. [Package and publish](../build-connectors/index.md#package-and-publish) explains connector packages; `openetl connector apply` installs them.
