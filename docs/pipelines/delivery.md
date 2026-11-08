---
title: Delivery guarantees
description: What each delivery guarantee promises, what decides a stream's, and how to read a stream that may hold a row twice.
---

Every stream of a pipeline has a delivery guarantee: what OpenETL promises about a row landing twice after a retry or a second read. This page covers what each guarantee promises, what decides a stream's, how `openetl plan` shows it, and how to read a stream delivered at least once. [Concepts](../get-started/concepts.md#delivery-guarantees) has the short version.

The examples read two tables of a Postgres source, `shop-db`: `orders`, with a primary key `id` and an `updated_at` column, and `page_views`, which has no primary key. They write into a Postgres destination, `warehouse`. These worked examples use the [official Postgres connector](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md). Check your connector version's documentation and capabilities; OpenETL computes guarantees from those capabilities rather than from the system's name.

## The three guarantees

A row that the source inserts, then updates twice, has three **versions**. Each version has its own `_openetl_seq`, where it comes in the source's order, and its own `_openetl_id`. A guarantee is about the same version landing more than once:

| Guarantee | What can happen | What you do when reading |
|---|---|---|
| `exactly_once` | Each version lands once, through crashes and retries. | Nothing. |
| `effectively_once` | A version can be written again, but onto the row it was written to before, so the table ends up as if it were written once. The row's `_openetl_synced_at` moves when that happens. | Nothing. Don't read `_openetl_synced_at` as when the row last changed. |
| `at_least_once` | A version can land twice, as two rows with the same `_openetl_id`. | Keep one row per `_openetl_id`: see [reading a stream delivered at least once](#reading-a-stream-delivered-at-least-once). |

A stream written with append keeps every version as a row of its own under any guarantee, so an updated row is in the table once for each version. That's append, not a repeat: [read its latest versions](#reading-a-stream-delivered-at-least-once) by primary key.

## What decides a stream's guarantee

**The source: how a stream replays.** A stream read again from a point the source can resume from either yields exactly the rows after that point, which is **exact** replay, or may yield some of them again, which is **superset** replay. Which a stream does is up to its source, and reads from a database's log of changes (`cdc`) or of an event stream (`stream`) usually replay exactly. An incremental read is always superset: each read starts at the highest cursor value the last read saw, so it reads the rows with that value again. `openetl discover` shows what the source declares for each stream:

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

- It stages what was read and applies it in one step (**stage and apply**). An exact stream gets `exactly_once`. A superset stream written with merge gets `effectively_once`, since a repeated version is written onto its own row again. A superset stream written with append or overwrite gets `at_least_once`, unless the destination can also skip rows whose `_openetl_id` its table already has: then it gets `exactly_once`, by **deduping at apply**.
- It writes each commit in a transaction (**transactional stream**). An exact stream gets `exactly_once`, and a superset stream `at_least_once`.
- It delivers records as they're read and waits for each to be acknowledged (**deliver and acknowledge**). A delivery can be retried and land twice, so every stream gets `at_least_once`.

**The stream: its write mode and its key.** Merge needs a primary key. A stream without one is covered [below](#streams-without-a-primary-key).

## What a pipeline requires

`delivery.require` is the lowest guarantee any stream may have, and it's `exactly_once` unless the pipeline says otherwise. A stream's own `require` overrides it for that stream. Deduping costs an index on `_openetl_id` and a lookup for each row, so OpenETL dedupes only a stream that requires more than at least once.

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

A sync reads its streams and commits what it has read as it goes: every `delivery.commit.interval` (5 minutes in a scheduled pipeline), whenever `delivery.commit.maxBytes` have been read since the last commit (`256MB` by default), and when the read ends. What a sync has read but not yet committed isn't in the destination at all. With `delivery.commit.scope: pipeline`, the default where the destination can, each commit makes every stream's rows visible together; with `stream`, each stream commits on its own.

A sync that fails partway keeps what it committed, and its retry, or the next sync, carries on from its last commit. So a table can hold the first part of a sync that failed. A stream written with overwrite is the exception: its table is replaced only once its read ends, so until then readers see the table as it was.

A continuous pipeline commits every 10 seconds, unless the destination or `delivery.commit.interval` says otherwise. [The reference](../reference/specs/pipeline.md) has every `delivery` field.

## Streams without a primary key

A row without a primary key gets an `_openetl_id` made from its whole content. So a repeat and a second row the source holds with the same content get the same ID, and nothing can tell them apart. `collapse` says what to do about it. For a stream that replays a superset, such as one read incrementally, into a destination that dedupes at apply:

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

A stream without a primary key that replays exactly, such as a full refresh of a table without one, keeps every row by default. Under a guarantee above at least once, OpenETL commits it only at points the source can resume from, and a source can't resume partway through a table it can't order, so nothing of that stream is committed until its read ends: a large table holds back every commit, and what's staged for it grows by its size meanwhile. With `collapse: true`, OpenETL may commit partway through it instead, and after a crash, rows with the same content collapse into one for the rest of that retry. The plan's warning says so.

## Reading a stream delivered at least once

Under `at_least_once`, nothing removes repeats. They come from three places:

- **An incremental read's last cursor value.** Each read starts at the highest cursor value the last read saw, so the rows with that value land again on every sync.
- **A retried delivery** to a destination that delivers and acknowledges.
- **A read again after a crash.** OpenETL commits at points the source can resume from exactly. When the source offers none for `delivery.resumePointTimeout` (`1m`), OpenETL commits without one, a **forced barrier**, but only where the stream keeps its required guarantee even if it's read again. After a crash, a stream restored from a forced barrier is read again from its last resume point. Under a higher guarantee, the rows it reads again are deduped, collapsed or merged onto their own rows; under `at_least_once`, they land again.

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

A pipeline's **Checkpoints** tab in the console lists every commit of the last 30 days, newest first, by **Epoch**, with each stream's rows. A lightning icon marks a forced barrier. A stream read again after a restore from one shows the guarantee it got in that commit, such as **at least once**, where that's below its planned one: that commit's rows may repeat rows an earlier commit wrote. Hover over it for the planned guarantee.

## When the plan can't tell

The plan works out guarantees from the source's latest catalog, which `openetl discover` refreshes, and from what the destination can do, which the control plane learns from the destination connector's package. For a connector it has no package for, such as one a runner runs from a local directory, the plan has a note instead, the console's **Delivery** column says **Checked when it syncs**, and the runner checks the guarantees before each sync. [Package and publish](../build-connectors/index.md#package-and-publish) covers packages, and `openetl connector apply` applies one.
