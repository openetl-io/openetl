---
title: Integrate with other systems
description: Choose an integration for transforms, applications, event notifications or monitoring.
---

Use this section to connect OpenETL to a system that you already know.

| Your task | Guide |
|---|---|
| Build or test models after a scheduled sync | [Run dbt after a sync](./dbt.md) |
| Transform scheduled data or process completed destination tables | [Use Spark](./spark.md) |
| Transform a continuous stream with checkpointed state | [Use Flink](./flink.md) |
| Receive pipeline and incident events | [Receive and verify webhooks](./webhooks.md) |
| Connect your own application | [Connect to the API](./api.md) |
| Interpret API check responses | [Check results in the API](./api-checks.md) |
| Export monitoring data | [Export metrics and traces](./telemetry.md) |
| Query copied records | [Read delivered data](./read-data.md) |

Review YAML changes with `openetl plan`. Apply reviewed changes with `openetl apply`. [Plan and apply changes](../pipelines/plan-and-apply.md) explains this workflow.

## dbt after a sync

See [Run dbt after a sync](./dbt.md).

## Transforms on Spark

See [Transform data with Spark](./spark.md).

## Transforms on Flink

See [Transform a stream with Flink](./flink.md).

## Events and webhooks

See [Receive and verify webhooks](./webhooks.md).

## The API

See [Connect to the API](./api.md).

### Check and preflight results

See [Check results in the API](./api-checks.md).

## Metrics and traces

See [Export metrics and traces](./telemetry.md).

## Reading what OpenETL writes

See [Read delivered data](./read-data.md).
