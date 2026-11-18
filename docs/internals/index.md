---
title: Internals
description: Choose an explanation of the control plane, data engine, commit protocol or test architecture.
---

This section is for contributors and readers who know distributed systems.

| Topic | Explanation |
|---|---|
| API, storage, orchestration and runners | [Control plane and runners](./architecture.md) |
| Lanes, watermarks, windows and state | [Data engine and state](./engine.md) |
| Checkpoints, commits and recovery | [Checkpoints and commits](./commits.md) |
| Simulation and other test coverage | [Test architecture](./testing.md) |

For data checks, use [the console guides](../use/index.md). For deployment, use [Self-host](../self-host/index.md).

## Find a topic

Use these links to open a specific topic.

## The parts

See [The parts](./architecture.md#the-parts).

## The API

See [The API](./architecture.md#the-api).

## The metadata store

See [The metadata store](./architecture.md#the-metadata-store).

## Orchestration

See [Orchestration](./architecture.md#orchestration).

## Runners

See [Runners](./architecture.md#runners).

## The engine

See [The engine](./engine.md#the-engine).

### Partitioning and exchanges

See [Partitioning and exchanges](./engine.md#partitioning-and-exchanges).

### Watermarks

See [Watermarks](./engine.md#watermarks).

### Keyed state

See [Keyed state](./engine.md#keyed-state).

#### Merges

See [Merges](./engine.md#merges).

### Group windows

See [Group windows](./engine.md#group-windows).

## Checkpoints

See [Checkpoints](./commits.md#checkpoints).

## The commit protocol

See [The commit protocol](./commits.md#the-commit-protocol).

## Attempts on workers

See [Attempts on workers](./commits.md#attempts-on-workers).

### The exchange between engines

See [The exchange between engines](./commits.md#the-exchange-between-engines).

## Delivery levels

See [Delivery levels](./commits.md#delivery-levels).

## Around the data path

See [Around the data path](./architecture.md#around-the-data-path).

## How it's tested

See [How it is tested](./testing.md#how-its-tested).
