# OpenETL: Batch and streaming data integration

[![OpenETL — Move more data. Use less compute. Open-source data movement, batch and streaming, on your infrastructure.](.github/assets/readme-header.svg)](https://openetl.io)

<p align="center">
  <a href="https://github.com/openetl-io/openetl/actions/workflows/nightly.yml?query=event%3Aschedule"><img src="https://github.com/openetl-io/openetl/actions/workflows/nightly.yml/badge.svg?event=schedule" alt="nightly"></a>
  <a href="https://sonarcloud.io/summary/new_code?id=openetl-io_openetl"><img src="https://sonarcloud.io/api/project_badges/measure?project=openetl-io_openetl&metric=alert_status" alt="Quality Gate Status"></a>
  <a href="https://sonarcloud.io/summary/new_code?id=openetl-io_openetl"><img src="https://sonarcloud.io/api/project_badges/measure?project=openetl-io_openetl&metric=security_rating" alt="Security Rating"></a>
  <a href="https://sonarcloud.io/summary/new_code?id=openetl-io_openetl"><img src="https://sonarcloud.io/api/project_badges/measure?project=openetl-io_openetl&metric=reliability_rating" alt="Reliability Rating"></a>
  <a href="https://sonarcloud.io/summary/new_code?id=openetl-io_openetl"><img src="https://sonarcloud.io/api/project_badges/measure?project=openetl-io_openetl&metric=sqale_rating" alt="Maintainability Rating"></a>
</p>

OpenETL is a self-hosted, open-source data integration platform for batch and streaming ETL/ELT pipelines. Replicate databases with change data capture (CDC), ingest HTTP APIs and events, and load data into warehouses and data lakes.

Its [Rust data engine](docs/internals/engine.md) uses Apache Arrow and runs SQL transforms, including stateful stream processing. Define pipelines as YAML and run on your own infrastructure under the [Apache 2.0 license](LICENSE).

[Quickstart](docs/get-started/quickstart.md) · [Documentation](https://docs.openetl.io) · [Connectors](https://github.com/openetl-io/openetl-connectors) · [Self-host](docs/self-host/index.md)

> [!NOTE]
> OpenETL is in alpha. The pipeline format and the API can change between releases.

## Why OpenETL?

### Designed for high throughput per core

The Rust engine processes columnar Apache Arrow batches. Transfers between engines reuse Arrow buffers, avoiding an extra payload copy. [Job presets](docs/reference/specs/pipeline.md#resources) start at **0.5 vCPU and 1 GiB**, excluding runner overhead. Jobs share runners or get their own Kubernetes pods. [How the engine works](docs/internals/engine.md).

### Stateless compute, durable progress

With shared object storage, runners keep checkpoints and SQL state outside their processes. Replace a runner or change a compatible job's worker count, and the next attempt restores from its checkpoint.

```mermaid
flowchart LR
  sources[Databases, HTTP APIs, events] --> compute[Replaceable runners: Rust + Arrow + SQL]
  compute --> destinations[Tables, data lakes, streams]
  compute -. checkpoints and SQL state .-> storage[(Shared object storage)]
```

Metadata and workflow state live in Postgres or the configured orchestration backend. Local scratch is a disposable cache. [Stateless compute and elastic scaling](docs/self-host/index.md#stateless-compute-and-elastic-scaling) explains what to persist and how capacity changes.

### Elastic capacity as workloads change

- **Size each job.** Choose CPU and memory automatically within your limits; adjust continuous-job parallelism from utilization and source lag.
- **Scale the fleet.** Use KEDA for shared runners and Karpenter for nodes. Compatible jobs can span [several workers](docs/pipelines/workers.md).
- **Use spot capacity.** Resume interrupted jobs from checkpoints, preserving their delivery guarantee. [Kubernetes deployment and scaling](docs/self-host/kubernetes.md).

## From ingestion to recovery

| What you need | What OpenETL provides |
|---|---|
| Batch and streaming ingestion | Full refreshes, incremental syncs, CDC database replication and event streams through [compatible connectors](docs/pipelines/connectors.md). |
| SQL stream processing | Filters and reshaping, [event-time windows](docs/pipelines/windows.md), [running totals](docs/pipelines/over.md) and [deduplication by key](docs/pipelines/dedupe.md), with checkpointed state. |
| Delivery you can require | [Per-stream delivery checks](docs/pipelines/delivery.md), including exactly once where supported. Pipelines that cannot meet their required guarantee are refused. |
| Controlled schema evolution | New columns by default, permission for destination type changes, [releasable holds](docs/pipelines/schema-changes.md) where supported, and [quarantine](docs/pipelines/holds-and-quarantine.md) for invalid rows. |
| Transforms across your stack | [dbt after a sync](docs/integrate/dbt.md), [Spark batch transforms](docs/integrate/spark.md) and [Flink streaming jobs](docs/integrate/flink.md) coordinated with destination commits. |
| Operations with evidence | [Preflight checks](docs/use/checks.md), freshness and incidents in the [console](docs/use/index.md), [OpenTelemetry](docs/integrate/telemetry.md) and signed [webhooks](docs/integrate/webhooks.md). |
| Automation and extensibility | YAML plans, a [REST and gRPC API](docs/integrate/api.md), [HTTP source manifests](docs/reference/manifest.md), and a [Go SDK and connector protocol](docs/build-connectors/index.md). |
| Team access | [Workspace roles, service accounts and audit logs](docs/self-host/access.md), with [encrypted secrets or cloud secret references](docs/self-host/secrets.md). |

## Get started

You need macOS or Linux, and [Docker](https://docs.docker.com/get-started/get-docker/) with Compose. Install the `openetl` CLI:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | sh
```

It installs the newest release, and shows a line to add to your shell's profile if `openetl` is not on your `PATH`. Start the same release with Docker Compose, in a new directory:

```sh
mkdir openetl && cd openetl
curl -fsSLO "https://github.com/openetl-io/openetl/releases/download/v$(openetl version)/compose.yml"
cat > .env <<EOF
POSTGRES_PASSWORD=$(openssl rand -hex 16)
OPENETL_SECRETS_KEY=base64key://$(openssl rand -base64 32 | tr '+/' '-_')
EOF
docker compose up -d
```

The console is at [localhost:8080](http://localhost:8080). The [quickstart](docs/get-started/quickstart.md) continues from here: it signs you in, installs a connector, and copies generated rows into Postgres.

To run OpenETL for your team, follow [Self-host](docs/self-host/index.md). To build OpenETL from source, see [CONTRIBUTING.md](CONTRIBUTING.md).

## Common questions

### Which sources and destinations can I connect?

For example, capture changes from [PostgreSQL](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) into [Apache Iceberg](https://github.com/openetl-io/openetl-connectors/blob/main/iceberg/README.md) tables, or read and write [Kafka](https://github.com/openetl-io/openetl-connectors/blob/main/kafka/README.md) events. The [official connector catalog](https://github.com/openetl-io/openetl-connectors#connectors) lists each package's source and destination roles. Supported modes and delivery guarantees depend on the connector version and pipeline configuration.

### Does OpenETL support both ETL and ELT?

Yes. For extract, transform, load (ETL), transform data before loading with SQL, Spark or Flink. For extract, load, transform (ELT), run dbt after a scheduled sync. See the [SQL transform guide](docs/pipelines/sql-transforms.md) and [integration guides](docs/integrate/index.md).

## Documentation

| If you want to | Start with |
|---|---|
| Learn what OpenETL is, in plain words | [Overview](docs/get-started/overview.md) |
| Run OpenETL for your team | [Self-host](docs/self-host/index.md) |
| Build pipelines | [Pipelines](docs/pipelines/index.md) |
| Check on pipelines and the data they deliver | [Use the console](docs/use/index.md) |
| Connect dbt, Spark, Flink, your alerts or your own code | [Integrate](docs/integrate/index.md) |
| Write a connector | [Build a connector](docs/build-connectors/index.md) |
| Know how it works inside | [Internals](docs/internals/index.md) |

[Choose and manage connectors](docs/pipelines/connectors.md) covers installation, compatibility and upgrades. External connectors have their own versions and release cadence, separate from OpenETL.

## Contributing

Issues and pull requests are welcome. [CONTRIBUTING.md](CONTRIBUTING.md) has how to build and test OpenETL.

## License

[Apache 2.0](LICENSE)
