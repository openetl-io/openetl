---
title: OpenETL documentation
description: Guides and reference for using, running, integrating and extending OpenETL.
---

OpenETL copies data from databases and event streams into warehouses, lakes and other streams, on a schedule or continuously, and tells you how far you can trust what lands. These docs are for everyone who works with it, whatever part they play.

New here? Read [What is OpenETL?](./get-started/overview.md), then run the [quickstart](./get-started/quickstart.md) on your own computer.

## Find your way

| If you want to | Start with |
|---|---|
| Learn what OpenETL is, in plain words | [What is OpenETL?](./get-started/overview.md) and [Concepts](./get-started/concepts.md) |
| Run OpenETL for your team | [Self-host](./self-host/index.md) |
| Diagnose failures or stale data | [Troubleshoot a pipeline](./pipelines/troubleshooting.md) |
| Manage access or recover an installation | [Access](./self-host/access.md) and [Back up and restore](./self-host/backup-and-restore.md) |
| Build pipelines | [Pipelines](./pipelines/index.md) |
| Check on pipelines and the data they deliver | [Use the console](./use/index.md) |
| Connect dbt, Spark, Flink, your alerts or your own code | [Integrate](./integrate/index.md) |
| Write a connector | [Build a connector](./build-connectors/index.md) |
| Look up a field, a command or a setting | [Reference](./reference/index.md) |
| Know how it works inside | [Internals](./internals/index.md) |

[Choose and manage connectors](./pipelines/connectors.md) covers official, community and private connectors, which have their own versions and release cadence. Each connector's documentation describes its setup, config, supported modes and limits. Official connector pages appear under Connectors on the docs site and live with their code in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Other authors host their own documentation; the console links to the package's **Docs**.

> [!NOTE]
> OpenETL is pre-alpha. Nothing has been released yet, and pipelines' format and the API can still change between versions.
