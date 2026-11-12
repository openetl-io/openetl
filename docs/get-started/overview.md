---
title: What is OpenETL?
description: What OpenETL does, who it's for, and how connectors link your systems.
---

OpenETL copies data between your systems. Source connectors read data from databases and event streams. Destination connectors write that data into warehouses, data lakes and other streams. Pipelines keep the copies up to date, on a schedule or continuously. OpenETL reports failures and other conditions that need attention.

OpenETL is open source under Apache 2.0. You run it on your own machines or in your own cloud account.

> [!NOTE]
> OpenETL is in alpha. The current release is {{version}}. The pipeline format and the API can change between releases.

## Connect your systems

[Connectors](./concepts.md#connectors) provide the connection to each system. A connector can read data, write data, or do both. Which systems you can connect, and which ways you can read or write them, depend on the connectors you install.

The official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Community members and organizations can also build and maintain connectors in their own repositories, including private ones. Connector authors choose their own versions and release cadence, independently of OpenETL.

For setup, permissions, config and limits, read the documentation for the connector version you use. [Choose and manage connectors](../pipelines/connectors.md) explains how to find, install and upgrade them. [Build a connector](../build-connectors/index.md) covers writing your own.

## What it does for you

- **Runs on a schedule or continuously.** A pipeline can copy data at set times or keep reading changes without stopping.
- **Checks delivery guarantees.** OpenETL determines whether each stream can receive duplicate changes. It refuses to run a pipeline that cannot meet its required guarantee.
- **Recovers from failures.** After a machine failure, the next attempt resumes from the last checkpoint. If retries cannot resolve the error, OpenETL opens an incident with recovery steps. Some incidents also pause the pipeline.
- **Controls schema changes.** New columns are added by default. An existing destination column's type changes only if the pipeline permits it. Otherwise, OpenETL holds the affected values where supported, or stops the stream.
- **Reports pipeline health.** The console shows sync results, data freshness, schema changes and incidents. OpenETL exports metrics and traces through OpenTelemetry. Webhooks send events to your endpoints.
- **Transforms data.** dbt can run after each sync. Spark and Flink jobs on your clusters can transform data between the source and destination.
- **Keeps definitions in git.** Pipelines are YAML files. Use `openetl plan` to review a change before `openetl apply` makes it.

## Who uses it

- **Analysts, product managers and other data users** check freshness and diagnose stale tables in the console. Start with [Use the console](../use/index.md).
- **Data and analytics engineers** write and review pipelines. Start with [Pipelines](../pipelines/index.md).
- **Platform engineers** install and run OpenETL for everyone else. Start with [Self-host](../self-host/index.md).
- **Developers** connect it to other tools, or write connectors for systems they need: [Integrate](../integrate/index.md) and [Build a connector](../build-connectors/index.md).

## Next

- Try it on your laptop with the [quickstart](./quickstart.md).
- Read the [concepts](./concepts.md): a short page that explains every word the rest of the docs use.
