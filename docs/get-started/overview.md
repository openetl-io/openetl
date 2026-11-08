---
title: What is OpenETL?
description: What OpenETL does, who it's for, and what it can connect today.
---

OpenETL keeps copies of your data where people need them. It reads from the systems where data is made, such as an application's Postgres database or a Kafka topic, and writes into the systems where it's used, such as a warehouse, a data lake or another stream. It keeps those copies up to date, on a schedule or continuously, and tells you when something needs attention.

It's open source, under Apache 2.0, and you run it on your own machines or your own cloud account, so your data doesn't pass through anyone else's.

> [!NOTE]
> OpenETL is pre-alpha. Nothing has been released yet, and pipelines' format and the API can still change between versions.

## What it connects

| Read from | Write to |
|---|---|
| Postgres: whole tables, new rows by a column such as `updated_at`, or every change as it happens (CDC) | Postgres |
| Kafka | Iceberg tables, through a REST or AWS Glue catalog |
| Kinesis | Parquet files on S3 |
| | Kafka |
| | Kinesis |

Each of these is a [connector](./concepts.md#connectors). The official connectors' own pages, with how to set each one up, are in [openetl-connectors](https://github.com/openetl-io/openetl-connectors).

## What it does for you

- **Copies on a schedule or continuously.** A pipeline can run every hour, at set times, or without stopping, landing changes within seconds or minutes.
- **Says how far you can trust each table.** For every stream, OpenETL works out whether a change could ever land twice, and refuses to run a pipeline that would deliver less than it asks for.
- **Recovers by itself, and says when it can't.** When a machine fails or a sync is stopped, the next try picks up from the last checkpoint. When retrying won't help, such as when a password is refused, OpenETL pauses the pipeline, opens an incident, and says what to change.
- **Doesn't change your tables behind your back.** New columns are added, but an existing column's type is never changed unless the pipeline allows it. When values stop fitting, OpenETL holds them back and says what to change.
- **Shows what's going on.** The console shows each pipeline's syncs, how fresh its data is, how its tables' schemas changed, and any incident open on it. Metrics and traces go to your monitoring over OpenTelemetry, and events go to your own endpoints as webhooks.
- **Shapes data on the way.** dbt can run after each sync. Spark and Flink jobs on your own clusters can transform data between the source and the destination.
- **Lives in git.** Pipelines are YAML files. `openetl plan` shows what a change would do before `openetl apply` makes it, so changes can be reviewed like code.

## Who uses it

- **People who depend on the data,** such as analysts and product managers, use the console to see whether their tables are fresh and why one isn't. [Use the console](../use/index.md) is for them.
- **Data and analytics engineers** write and review pipelines. Start with [Pipelines](../pipelines/index.md).
- **Platform engineers** install and run OpenETL for everyone else. Start with [Self-host](../self-host/index.md).
- **Developers** connect it to other tools, or write connectors for systems it doesn't support: [Integrate](../integrate/index.md) and [Build a connector](../build-connectors/index.md).

## Next

- Try it on your laptop with the [quickstart](./quickstart.md).
- Read the [concepts](./concepts.md): a short page that explains every word the rest of the docs use.
