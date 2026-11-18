---
title: Pipeline commands by task
description: Find commands for running pipelines, reading logs and managing recovery.
---

Use this guide for command-line operations after you apply a pipeline. [Use the console](../use/index.md) covers browser tasks.

## Delivery guarantees in practice

The default requirement is `exactly_once`. Set `delivery.require` to change it for the pipeline, or a stream's `require` to change it for that stream. OpenETL refuses to run a pipeline if any stream falls below its requirement. The plan lists each stream's guarantee and reason.

Deliver-and-acknowledge destinations can repeat a record after a retry. They provide at-least-once delivery. A pipeline using such a destination must accept that guarantee:

```yaml
delivery:
  require: at_least_once
```

Under `at_least_once`, repeated changes remain as duplicate rows. For example, an incremental read includes rows at the previous highest cursor value again. With a higher guarantee, a capable destination removes duplicates during apply. With `at_least_once`, it writes them again. Recovery after a failure can also produce duplicates.

When reading a stream delivered at least once, keep one row per `_openetl_id`. This ID combines the stream, primary key and `_openetl_seq`. Without a primary key, it uses the row's whole content instead. Repeated reads of the same version produce the same ID. In Postgres:

```sql
select distinct on (_openetl_id) *
from faker.users
order by _openetl_id;
```

For the latest version of each row of an appended stream, keep the highest `_openetl_seq` for each primary key instead:

```sql
select distinct on (id) *
from faker.users
order by id, _openetl_seq desc;
```

[Delivery guarantees](./delivery.md) explains guarantee selection, plan reasons and streams without primary keys. It also shows how to read data delivered at least once.

## Day to day

| Command | What it does |
|---|---|
| `openetl plan -f FILE` | Show what applying the documents would change, and each pipeline's plan |
| `openetl apply -f FILE` | Create or update the documents' resources, then check what changed on a runner; `--no-check` skips that |
| `openetl sync NAME` | Start a sync of a scheduled pipeline now; it fails if the pipeline is paused or a sync is running |
| `openetl pause NAME` | Stop a pipeline's scheduled syncs; a sync that is running carries on. A continuous pipeline's job stops. `--stream STREAM` stops only that stream, which syncs skip ([Pause one stream](./schedules.md#pause-one-stream)) |
| `openetl resume NAME` | Start its scheduled syncs, or its job, again; `--stream STREAM` resumes a stream paused on its own |
| `openetl reset NAME` | Read its streams again from the start on the next sync or run; `--stream` picks streams. See [Reading from the start again](./positions.md) |
| `openetl rewind --yes NAME` | Let it restart from the checkpoint before its latest, if the latest cannot be read, delivering that epoch again |
| `openetl overrides NAME` | List its paused streams, and heal's overrides of its spec ([When heal slows syncs down](./schedules.md#when-heal-slows-syncs-down)) |
| `openetl overrides remove NAME FIELD` | Give the spec back its say in a field heal overrides: `schedule.every` or `source.rate_limit_scale` |
| `openetl syncs NAME` | List its syncs, newest first, with how long each took, the rows it copied, its version and any error |
| `openetl logs NAME` | Print the latest sync's log; `-f` follows a running one, and `--sync ID` picks another sync |
| `openetl versions NAME` | List its versions, newest first |
| `openetl rescale --size SIZE NAME` | Size its jobs until its next version; a running job restarts at a checkpoint. `--size auto` gives the spec back its say |
| `openetl check source NAME`, `openetl check destination NAME` | Check that a runner can reach the source or destination with its config |
| `openetl preflight NAME` | Check the pipeline's source and destination and the streams it reads and writes, refresh the source's catalog, and print its plan; fails if any part fails or is skipped |
| `openetl discover NAME` | List a source's streams and their columns, and refresh the catalog plans read |
| `openetl holds NAME` | List the columns and cells the pipeline holds back, why, and the ways to clear each |
| `openetl holds release NAME STREAM COLUMN` | Patch a hold's values in on the pipeline's next run; `--new-version`, before the names, sends a column held whole to a new column, `COLUMN__v2`, instead |
| `openetl holds discard NAME STREAM COLUMN` | Drop a hold's values; the column stays as the destination has it |
| `openetl quarantine NAME` | List the files of rows it quarantined, newest first, and their rows by stream and cause; `--stream` and `--since` narrow it ([Quarantine files](../reference/quarantine-files.md)) |
| `openetl quarantine show NAME PATH` | Print a quarantine file's first lines, at most 100, which a runner reads from the data plane's storage; editors and above |

Flags go before the names, and every command takes `--workspace`. `openetl COMMAND -h` lists a command's flags.

The console shows the same pipelines, syncs, versions and holds; [Use the console](../use/index.md) walks through it.

If a pipeline waits, fails repeatedly or falls behind, start with [Troubleshoot a pipeline](./troubleshooting.md).
