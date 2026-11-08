---
title: Concepts
description: The words OpenETL uses, what each one means, and how they fit together.
---

OpenETL has a small vocabulary. This page explains each word once, and the rest of the docs, the console and the CLI use them the same way.

## Sources and destinations

A **source** is a system data comes from, such as a Postgres database or a Kafka cluster. A **destination** is a system data goes to, such as a warehouse database, an Iceberg catalog or a bucket of Parquet files.

Each one holds the details of how to reach it: the server, the database, the account. Passwords and keys aren't written into it. It names a **secret** instead, which OpenETL stores encrypted.

## Connectors

A **connector** is the program that knows how to talk to one kind of system. The Postgres connector reads from Postgres and writes to it; the Kafka connector does the same for Kafka. A source or destination names the connector it uses.

A few connectors are built into OpenETL, for trying things out. The official ones, such as Postgres, Kafka and Iceberg, are in [openetl-connectors](https://github.com/openetl-io/openetl-connectors). Anyone can [write one](../build-connectors/index.md).

## Pipelines and streams

A **pipeline** copies data from one source to one destination.

A source offers **streams**: each table of a database, each topic of a Kafka cluster. Each stream lands as one table, one set of files or one topic in the destination. A pipeline copies every stream its source offers unless it names the ones it wants.

How a stream is read is its **sync mode**:

- **Full refresh** reads the whole table every time.
- **Incremental** reads the rows whose cursor, a column such as `updated_at`, has moved on since the last read.
- **CDC** (change data capture) reads every insert, update and delete from the database's own log of changes.
- **Stream** reads the records an event stream, such as a Kafka topic, has had since the last read.

How a stream is written is its **write mode**:

- **Append** adds what was read to what the destination has.
- **Overwrite** replaces what the destination has with what was read.
- **Merge** updates each row by its primary key, so the destination holds the latest version of every row.

## Syncs, schedules and continuous pipelines

A **scheduled** pipeline copies in **syncs**. A sync reads what's new, writes it, and stops. The pipeline's **schedule** says when the next one starts: every hour, or at the times a cron expression gives. You can also start one by hand, with **Sync now** in the console.

A **continuous** pipeline doesn't stop. It keeps reading and commits what it has read as it goes, so changes land within seconds or minutes of being made, depending on the destination. Its streams are ones that never end: tables read by CDC, and event streams. Those can run on a schedule too, and moving a pipeline from one to the other is a change to its `mode`.

## Checkpoints

As a pipeline commits data, it records how far it has read in a **checkpoint**. If a machine fails or a sync is stopped, the next run picks up from the last checkpoint instead of starting over.

## Delivery guarantees

A **delivery guarantee** says whether a change could reach the destination twice, such as after a crash and a retry. OpenETL works it out for each stream, from what the source and the destination can each promise:

- **Exactly once:** each change lands once, crash or not.
- **Effectively once:** a change can be written again, but onto the same row, so the destination ends up as if it were written once. This is what merge gives when the source may send a change again.
- **At least once:** a change can land twice. Every row carries an `_openetl_id`, so whoever reads the data can drop the repeats.

A pipeline asks for exactly once unless it says otherwise, and a pipeline whose streams can't all have the guarantee it asks for doesn't run. `openetl plan` shows each stream's guarantee and the reason for it before you apply the pipeline, when the control plane knows what the destination can do: it learns that from the connector's package.

## Schema changes and holds

Sources change: a column is added, or a number column starts holding bigger numbers. The pipeline's **schema policy** decides what happens. By default, new columns are added to the destination, and when a column's type gets wider, the stream's type widens with it.

What OpenETL never does by itself is change the type of a column you already have in your destination, because that can rewrite or lock a table other people use. When a stream's column widens and the destination's column can't take the new values, the destination refuses. The pipeline then **holds** that column back where it can, keeping on delivering the rest of each row, or stops the stream; either way it says what change to the destination would let the values in. Once the change is made, **releasing** the hold patches the held values in. **Discarding** it drops them, and the column stays as it is.

A pipeline can allow its destination to change column types itself, for every stream or only some.

## Incidents

An **incident** opens on a pipeline when something needs attention: a sync failed, a password stopped working, a column is held, a continuous pipeline fell behind its source. Each one says what happened and what to do about it, and records what OpenETL did itself, such as pausing a pipeline whose syncs would only fail the same way again. Most incidents close by themselves once the pipeline makes progress again; you can also resolve one by hand.

## Versions

Every change you apply to a pipeline makes a new **version** of it, and each sync runs exactly one version. Applying a pipeline as it was before makes that version current again.

## Where it runs

OpenETL has two halves:

- The **control plane** serves the API and the console, keeps the pipelines, and decides what runs when and where.
- **Runners** do the copying. Each run starts the connectors as programs of their own, with OpenETL's data engine between them. Runners can be anywhere that can reach the control plane and your systems; together they're a **data plane**.

On a laptop, `openetld dev` runs both halves in one process. [Self-host](../self-host/index.md) covers running them on servers.

## Workspaces

Pipelines, sources, destinations and secrets belong to a **workspace**, whose name appears in the console's addresses and in the API's. An installation has one workspace, named `default`.
