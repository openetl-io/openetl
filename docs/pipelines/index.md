---
title: Build pipelines
description: Choose a task for defining, planning, running or troubleshooting pipelines.
---

Use this section to build and manage pipelines with YAML and the `openetl` CLI. It assumes that you know SQL, YAML and a terminal.

To check data without editing pipelines, use [the console guides](../use/index.md).

## Start a pipeline

1. Complete the [quickstart](../get-started/quickstart.md) with generated data.
2. Follow [Your first pipeline](./first-pipeline.md) to copy data from your own database.
3. Read [Define pipeline resources](./define-pipeline.md) to prepare YAML files.
4. [Plan and apply changes](./plan-and-apply.md).

## Choose a task

| Your task | Guide |
|---|---|
| Install or upgrade a connector | [Choose and manage connectors](./connectors.md) |
| Set when a pipeline runs | [Schedules](./schedules.md) or [Continuous pipelines](./continuous.md) |
| Choose how retries affect destination data | [Delivery guarantees](./delivery.md) |
| Handle new columns or type changes | [Schema changes](./schema-changes.md) |
| Recover values or rows that were not delivered | [Holds and quarantine](./holds-and-quarantine.md) |
| Transform records with SQL | [SQL transforms](./sql-transforms.md) |
| Aggregate records by time | [Group windows](./windows.md) |
| Distribute a job across pods | [Workers](./workers.md) |
| Read data again or recover a checkpoint | [Read positions](./positions.md) |
| Diagnose a problem | [Troubleshooting](./troubleshooting.md) |

Use [Pipeline settings by task](./settings.md) for common settings. Use the [Pipeline reference](../reference/specs/pipeline.md) for every field.

## Find a topic

Use these links to open a specific topic.

## The documents

See [The documents](./define-pipeline.md#the-documents).

## Secrets

See [Secrets](./define-pipeline.md#secrets).

## Plan, then apply

See [Plan, then apply](./plan-and-apply.md#plan-then-apply).

### A SQL transform in the plan

See [A SQL transform in the plan](./plan-and-apply.md#a-sql-transform-in-the-plan).

### Partitioning in the plan

See [Partitioning in the plan](./plan-and-apply.md#partitioning-in-the-plan).

### Apply the plan

See [Apply the plan](./plan-and-apply.md#apply-the-plan).

## Versions

See [Versions](./plan-and-apply.md#versions).

## The pipeline spec at a glance

See [The pipeline spec at a glance](./settings.md#the-pipeline-spec-at-a-glance).

### Running

See [Running](./settings.md#running).

### Source and streams

See [Source and streams](./settings.md#source-and-streams).

### Destination

See [Destination](./settings.md#destination).

### Schema and delivery

See [Schema and delivery](./settings.md#schema-and-delivery).

### After and between

See [After and between](./settings.md#after-and-between).

## Continuous pipelines

See [Continuous pipelines](./continuous.md#continuous-pipelines).

## Delivery guarantees in practice

See [Delivery guarantees in practice](./daily-operations.md#delivery-guarantees-in-practice).

## Day to day

See [Day to day](./daily-operations.md#day-to-day).
