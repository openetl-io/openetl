---
title: Inspect connections
description: Find source, destination and connector configuration in the console.
---

Use the pages under **Connections** in the sidebar to inspect the systems that pipelines use.

## Sources, destinations and connectors

**Sources** and **Destinations** list connections. Each entry shows its **Connector**, connector **Version**, **Pipelines** and **Updated** time.

Open a connection to inspect its pipelines and **Config**. Secret fields show references instead of values. To test it, [run a connection check](./checks.md#check-a-source-or-destination).

**dbt projects** lists projects that run after syncs. **Run after** identifies the pipelines that use each project.

**Clusters** has **Spark** and **Flink** tabs. **Runs on** identifies the submission method or platform. **Used by** lists pipelines. Open a cluster to inspect **Settings** and **Pipelines**. **Runs** identifies use as a transform or after a sync.

**Connectors** has a **Catalog** of available packages. Its **Sources** tab lists the registries and repositories that supply this catalog.

Open a package to inspect its details and configuration fields. Each field shows its values, defaults and conditions. Nested fields appear under their parent unless the connector assigns another section. In another section, a nested field shows its full path, such as `consumer.interval`.

The package page also shows the installation command. An administrator runs this command to make the package available to connections.

Connectors can be official, community maintained or private to your organization. Each has its own versions and releases.

Use a package's **Docs** link for setup, supported modes and limits. [Choose and manage connectors](../pipelines/connectors.md) gives the installation and upgrade commands.
