---
title: What is OpenETL?
description: Learn how OpenETL runs self-hosted data integration with batch pipelines, streaming and change data capture (CDC).
---

OpenETL is a self-hosted, open-source data integration platform for batch and streaming pipelines. It copies data between your systems. A pipeline defines which data to copy, where to write it and when to run.

Pipelines read from databases and event streams. They write into warehouses, data lakes and other streams. OpenETL reports failures and other conditions that need attention.

With a compatible connector, change data capture (CDC) reads inserts, updates and deletes from a database's log. This supports database replication without copying the whole table on every sync. [Concepts](./concepts.md#pipelines-and-streams) explains the read and write modes.

OpenETL also reads HTTP APIs through [declarative source manifests](../reference/manifest.md). A manifest describes the requests, pagination and records to extract.

OpenETL is open source under Apache 2.0. You run it on your own machines or in your own cloud account.

> [!NOTE]
> OpenETL {{version}} is in alpha. The pipeline format and API can change between releases.

## Connect your systems

[Connectors](./concepts.md#connectors) provide the connection to each system. A connector can read data, write data, or do both. Which systems you can connect, and which ways you can read or write them, depend on the connectors you install.

The official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Community members and organizations can also build and maintain connectors in their own repositories, including private ones. Connector authors choose their own versions and release cadence, independently of OpenETL.

For setup, permissions, configuration and limits, read the documentation for the connector version you use. [Choose and manage connectors](../pipelines/connectors.md) explains installation and upgrades.

## What it does for you

- **Runs on a schedule or continuously.** A pipeline can copy data at set times or keep reading changes without stopping.
- **Checks delivery guarantees.** OpenETL determines whether each stream can receive duplicate changes. It refuses to run a pipeline that cannot meet its required guarantee.
- **Recovers from failures.** After a machine failure, the next attempt resumes from the last checkpoint. If retries cannot resolve the error, OpenETL opens an incident with recovery steps. Some incidents also pause the pipeline.
- **Controls schema changes.** New columns are added by default. An existing destination column's type changes only if the pipeline permits it. Otherwise, OpenETL holds the affected values where supported, or stops the stream.
- **Reports pipeline health.** The console shows sync results, data freshness, schema changes and incidents. OpenETL exports metrics and traces through OpenTelemetry. Webhooks send events to your endpoints.
- **Processes streams with SQL.** Filter and reshape rows with [SQL transforms](../pipelines/sql-transforms.md). Calculate [time-window aggregates](../pipelines/windows.md), [running totals](../pipelines/over.md) or [one row per key](../pipelines/dedupe.md). OpenETL saves their progress in checkpoints so they can resume after failures.
- **Transforms before or after loading.** Transforming before loading is extract, transform, load (ETL). Use SQL in OpenETL, or Spark and Flink jobs on your clusters. For extract, load, transform (ELT), dbt can run after each scheduled sync.
- **Sizes and distributes work.** OpenETL can choose CPU and memory within your limits. Compatible jobs can run on [several Kubernetes workers](../pipelines/workers.md). Operators can add [runner scaling and spot nodes](../self-host/kubernetes.md).
- **Keeps definitions in git.** Pipelines are YAML files. Use `openetl plan` to review a change before `openetl apply` makes it.
- **Manages team access.** Use [workspace roles and service accounts](../self-host/access.md) to control access, and audit logs to review actions. Store [encrypted credentials or cloud secret references](../self-host/secrets.md) outside pipeline definitions.

## Who uses it

- **Analysts, product managers and other data users** check freshness and incidents in the console. Start with [Check data freshness](../use/freshness.md).
- **Data and analytics engineers** write and review pipelines. Start with [Pipelines](../pipelines/index.md).
- **Platform engineers** install and run OpenETL for everyone else. Start with [Self-host](../self-host/index.md).
- **Developers** connect it to other tools, or write connectors for systems they need: [Integrate](../integrate/index.md) and [Build a connector](../build-connectors/index.md).

## Next

- Try it on your laptop with the [quickstart](./quickstart.md).
- Read [Concepts](./concepts.md) for the terms used in the console and guides.
