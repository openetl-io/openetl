---
title: OpenETL documentation
description: Guides and reference for using, running, integrating and extending OpenETL.
---

OpenETL copies data from databases and event streams into warehouses, data lakes and other streams. Pipelines run on a schedule or continuously. OpenETL checks each stream's delivery guarantee before it runs.

Choose the path that matches your work. Start with [What is OpenETL?](./get-started/overview.md) if you are new to OpenETL.

## Check your team's data

Use the console to check data that your team uses for reports, analysis or applications.

1. [Sign in](./use/sign-in.md) with the address and password link from your administrator.
2. [Check data freshness](./use/freshness.md) to see when a pipeline last delivered data.
3. If data needs attention, [review incidents](./use/incidents.md) and follow their recovery steps.

[Use the console](./use/index.md) lists the other console tasks. [Concepts](./get-started/concepts.md) defines terms such as pipeline, stream and sync.

## Build or run OpenETL

Follow the [quickstart](./get-started/quickstart.md) to run a pipeline on your computer. It requires a terminal and Docker.

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

[Choose and manage connectors](./pipelines/connectors.md) covers official, community and private connectors. Each connector has its own versions and release schedule. Its documentation describes setup, configuration, supported modes and limits.

Official connector pages appear under Connectors on the docs site. Their source files are in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). For other packages, use the **Docs** link in the console.

> [!NOTE]
> OpenETL is in alpha. The current release is {{version}}. The pipeline format and the API can change between releases.
