---
title: What is OpenETL?
description: What OpenETL does, who it's for, and how connectors link your systems.
---

OpenETL keeps copies of your data where people need them. It reads through source connectors from the systems where data is made, and writes through destination connectors into the systems where it's used, such as a warehouse, a data lake or another stream. It keeps those copies up to date, on a schedule or continuously, and tells you when something needs attention.

It's open source, under Apache 2.0, and you run it on your own machines or your own cloud account, so your data doesn't pass through anyone else's.

> [!NOTE]
> OpenETL is pre-alpha. Nothing has been released yet, and pipelines' format and the API can still change between versions.

## Connect your systems

[Connectors](./concepts.md#connectors) provide the connection to each system. A connector can read data, write data, or do both. Which systems you can connect, and which ways you can read or write them, depend on the connectors you install.

The official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Community members and organizations can also build and maintain connectors in their own repositories, including private ones. Connector authors choose their own versions and release cadence, independently of OpenETL.

For setup, permissions, config and limits, read the documentation for the connector version you use. [Choose and manage connectors](../pipelines/connectors.md) explains how to find, install and upgrade them. [Build a connector](../build-connectors/index.md) covers writing your own.

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
- **Developers** connect it to other tools, or write connectors for systems they need: [Integrate](../integrate/index.md) and [Build a connector](../build-connectors/index.md).

## Next

- Try it on your laptop with the [quickstart](./quickstart.md).
- Read the [concepts](./concepts.md): a short page that explains every word the rest of the docs use.
