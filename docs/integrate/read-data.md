---
title: Read delivered data
description: Interpret record metadata, source deletes and duplicate records in destination data.
---

Use this guide when you query data that OpenETL has copied. The destination connector’s documentation defines its table, file or event format.

## Reading what OpenETL writes

Every row OpenETL writes to a table or a file carries metadata columns besides the source's:

| Column | What it holds |
|---|---|
| `_openetl_id` | The ID of this version of the record: a hash of the stream, the record's primary key (or, without one, every column), and its `_openetl_seq`. The same version gets the same ID however often it is read. |
| `_openetl_op` | `insert`, `update` or `delete`. Only streams read with CDC have updates and deletes. |
| `_openetl_seq` | Where this version comes in the source's order: its position in the source's log, its cursor value, or when the read started. A later version of a record has a higher one. |
| `_openetl_synced_at` | When OpenETL wrote it, in UTC. |
| `_openetl_deleted` | `true` for a row deleted at the source. Only in streams written with merge and soft deletes, the default, which keep a deleted row rather than removing it. |

A stream whose [delivery guarantee](../get-started/concepts.md#delivery-guarantees) is at least once can hold the same version of a record twice, with the same `_openetl_id`. Keep one of each when you read it:

```sql
select *
from (
  select *, row_number() over (partition by _openetl_id order by _openetl_synced_at) as n
  from faker.purchases
) as t
where n = 1;
```

To read the latest version of each record from a stream written with append, partition by its primary key instead, and order by `_openetl_seq` descending. If two changes of a row can have the same cursor value, both versions have the same `_openetl_id` and `_openetl_seq`. Then also order by `_openetl_synced_at` descending, and do not keep one row per `_openetl_id`. See [Choose a cursor that changes on every update](../pipelines/delivery.md#choose-a-cursor-that-changes-on-every-update).

The destination connector defines the table, file or event format and how readers see committed data. Follow that connector version's documentation for catalog setup, reader compatibility and any destination-specific metadata.
