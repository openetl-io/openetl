---
title: Concepts
description: The words OpenETL uses, what each one means, and how they fit together.
---

This page defines the terms used in the documentation, console and CLI.

## Sources and destinations

A **source** is a configured connection that pipelines read from. A **destination** is a configured connection that pipelines write to. Each connection specifies a connector and the configuration it needs to reach your system.

Connection details include the server, database and account. Passwords and keys use a **secret** reference instead of a value in the configuration. OpenETL encrypts stored secret values.

## Connectors

A **connector** implements how to read from or write to a system. It can provide a source role, a destination role, or both. Its config, supported read and write modes, type mappings and limits belong to that connector. OpenETL runs connectors through a shared contract.

Official connectors live in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Anyone can [build and maintain a connector](../build-connectors/index.md), including a community project or an organization's private connector. Each connector has its own versions and releases, separate from OpenETL's. A few [built-in connectors](../reference/built-in-connectors.md) for trying pipelines out ship with OpenETL itself.

A **connector package** distributes one or both roles as a version you can install. A **connector source** is a registry namespace or repository used to list packages in the connector catalog. It is separate from a pipeline's data source. [Choose and manage connectors](../pipelines/connectors.md) explains how to install a package and select a version.

## Pipelines and streams

A **pipeline** copies data from one source to one destination.

A source offers **streams**, such as database tables or event topics. Each stream writes to one table, one set of files or one topic in the destination. A pipeline copies all available streams unless its definition selects specific streams.

A stream's **sync mode** specifies how to read it:

- **Full refresh** reads the whole table every time.
- **Incremental** selects rows using a **cursor**, such as an `updated_at` column. Later reads include rows at the saved cursor value again.
- **CDC** (change data capture) reads every insert, update and delete from the database's own log of changes.
- **Stream** reads event records from the saved position.

A stream's **write mode** specifies how to write it:

- **Append** adds records to the destination.
- **Overwrite** replaces the destination's existing data with the data read during the sync.
- **Merge** uses the primary key to insert or update each row. The destination keeps the latest version of each row.

A pipeline can have a **transform**, which changes rows between the source and the destination. A **SQL transform** is SQL statements that the engine runs as it copies. Each `INSERT INTO` statement reads streams and writes an **output**, which the destination gets as a stream.

## Syncs, schedules and continuous pipelines

A **scheduled** pipeline copies data in **syncs**. A sync reads the selected data, writes it and stops. The pipeline's **schedule** specifies an interval or cron expression for starting syncs. You can also select **Sync now** in the console to start a sync manually.

A **continuous** pipeline keeps reading and commits data at intervals. Its streams must support reads without an end, such as CDC or event streams. These streams can also run on a schedule. The pipeline's `mode` selects scheduled or continuous operation.

## Checkpoints

When a pipeline commits data, it records its read positions in a **checkpoint**. After a machine failure or stopped sync, the next run resumes from the last checkpoint.

An **epoch** is the group of records between two checkpoints. A sync can commit several epochs before it finishes.

## Delivery guarantees

A **delivery guarantee** describes whether retries can produce duplicate changes in the destination. OpenETL determines the guarantee for each stream from its source and destination capabilities, write mode and primary key:

- **Exactly once:** each change is committed once, including after a failure and retry.
- **Effectively once:** a change can be written again, but merge updates the same row. The final row is the same as if the change were written once.
- **At least once:** retries can write duplicate changes. Each row has an `_openetl_id` that readers can use to remove duplicates.

A pipeline requires exactly-once delivery by default. OpenETL refuses to run it if any stream cannot meet the required guarantee.

`openetl plan` shows each stream's guarantee and its reason when the control plane has the necessary stream and connector information. The source catalog describes the streams. The destination connector's package describes its capabilities. Otherwise, the plan explains what it cannot check, and the runner checks before the sync.

## Schema changes and holds

The **schema** describes a stream's columns and types. The pipeline's **schema policy** controls how OpenETL handles new columns and type changes. By default, new columns are added to the destination. A stream's type can widen to represent larger values.

An integer column that has contained a value beyond ±2^53 (9,007,199,254,740,992) widens to text if fractional values arrive. Conversion to floating point could round those integers. Text preserves their values.

By default, OpenETL does not change an existing destination column's type. That operation can rewrite or lock a table. A pipeline can permit type changes for all streams or selected streams.

If the destination refuses a type change, OpenETL **holds** the affected values where supported, or stops the stream. A hold saves values while the pipeline continues to deliver the rest of the row. It explains the destination change needed to accept those values.

After that change, **releasing** the hold writes the saved values into the destination on the next run. **Discarding** the hold deletes the saved values and leaves the destination column's type unchanged. See [Handle schema changes and holds](../pipelines/schema-changes.md) for the procedure.

**Quarantine** saves a row or cell whose value does not fit its column's type. Row quarantine excludes the whole row from delivery. Cell quarantine holds only the affected cell where the destination supports later repair.

## Incidents

An **incident** records a condition that needs attention. Examples include a failed sync, rejected credentials, a held column or a continuous pipeline falling behind its source. Each incident explains what happened, gives recovery steps and records automatic actions.

**Heal** is OpenETL's response to incidents. Depending on the pipeline's policy, it can take recovery actions, propose them, or remain off. For example, it can pause a pipeline when retries would repeat the same failure. Most incidents resolve when the pipeline makes progress again. You can also resolve an incident manually.

## Versions

Each applied change to a pipeline creates a **version**. Each sync runs one version. Applying an earlier definition makes that version current again.

## Where it runs

OpenETL has two halves:

- The **control plane** serves the API and the console, keeps the pipelines, and decides what runs when and where.
- **Runners** execute jobs that copy data. Each run starts the connectors as separate programs, with OpenETL's data engine between them. A **data plane** groups runners that can reach the API and your data systems.

On a laptop, `openetld dev` runs both halves in one process. [Self-host](../self-host/index.md) covers running them on servers.

## Workspaces

Pipelines, sources, destinations and secrets belong to a **workspace**. Its name appears in console and API addresses. An installation has one workspace, named `default`.
