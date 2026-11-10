---
title: Integrate
description: Connect OpenETL to dbt, Spark, Flink, your own code and your monitoring, and read what it writes.
---

This section explains how to connect OpenETL to other systems. Each integration describes the data OpenETL sends or expects and the configuration required on each side. Review YAML changes with `openetl plan`, then apply them with `openetl apply`. [Pipelines](../pipelines/index.md) explains this workflow.

## dbt after a sync

A scheduled pipeline can run a dbt project after each sync, against the database the sync wrote to. dbt steps run against Postgres destinations only.

The project is a `DbtProject` document, and the pipeline names it in `postSync`. From [`examples/faker-to-postgres-dbt.yaml`](../../examples/faker-to-postgres-dbt.yaml), with its comments left out:

```yaml
apiVersion: openetl.io/v1alpha1
kind: DbtProject
metadata:
  name: faker-dbt
spec:
  path: dbt
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-postgres-dbt
spec:
  schedule: { every: 1h }
  source: { ref: faker }
  destination:
    ref: local-postgres
    namespace: faker
  postSync:
    - dbt: { project: faker-dbt, schema: faker_marts }
```

- **The project.** `path` specifies the project directory relative to the YAML file. `openetl apply` packages and uploads it, excluding hidden files, `target/`, `logs/` and `profiles.yml`. Runners do not run `dbt deps`. Include required packages in `dbt_packages/`, which is uploaded. The maximum archive size is 16 MiB.
- **The step.** `command` is `build`, the default, `run` or `test`. `select` and `exclude` are dbt's `--select` and `--exclude`. `schema` is the schema dbt builds into; by default, the one the pipeline writes to.
- **The connection.** The runner creates `profiles.yml` with one profile for the pipeline's destination. Its name matches `profile` in `dbt_project.yml`. The runner passes credentials through environment variables. `OPENETL_NAMESPACE` identifies the schema written by the pipeline. Use it in source definitions, as in [`examples/dbt/models/sources.yml`](../../examples/dbt/models/sources.yml):

  ```yaml
  sources:
    - name: faker
      schema: "{{ env_var('OPENETL_NAMESPACE') }}"
      tables:
        - name: users
        - name: purchases
  ```

- **When it runs.** Steps run in order after the sync commits its data. A failed step fails the sync and prevents later steps from running. The committed data remains in the destination. Each sync uses the project version selected when the sync started.
- **Where it runs.** Runners execute `dbt` from `PATH` or the path specified by `--dbt` (`OPENETL_DBT`). Install the `dbt-postgres` adapter on each runner that executes these steps.

The [dbt project reference](../reference/specs/dbt-project.md) distinguishes the CLI's `path` input from the API's required `bundle`.

## Transforms on Spark

A scheduled pipeline can run Spark code between its source and destination on your Spark cluster. Each commit covers a group of records called an **epoch**. For each epoch, your code receives one DataFrame per input stream and returns one DataFrame per output. OpenETL writes the outputs as destination streams.

```yaml
apiVersion: openetl.io/v1alpha1
kind: SparkCluster
metadata:
  name: analytics
spec:
  submit:
    master: spark://spark-master:7077
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-spend
spec:
  schedule: { every: 1h }
  source: { ref: faker }
  destination: { ref: local-postgres, namespace: faker_spend }
  transform:
    spark:
      cluster: analytics
      code:
        jar: s3://my-bucket/jobs/spend.jar
        sha256: SHA256
        class: com.example.Spend
      outputs:
        - name: spend_per_user
          primaryKey: [user_id]
```

- **The cluster.** A `SparkCluster` selects either `submit` or `emr`. `submit` uses `spark-submit` in cluster deploy mode. Targets are a standalone master (`spark://HOST:PORT`), YARN (`yarn`) or Kubernetes (`k8s://https://HOST:PORT`). Runners need Spark 3.5's `spark-submit` on `PATH` or at the path specified by `--spark-submit`. `emr` adds steps to an EMR on EC2 cluster selected by `clusterId` or `clusterName`, in `region`.
- **The code.** `code` specifies a jar or Python file at an accessible `s3://` or `https://` URI. Supply its hexadecimal SHA-256 digest, which `shasum -a 256 spend.jar` prints. The runner verifies the digest and copies the artifact to the cluster. A standalone master requires a jar because it cannot run Python in cluster deploy mode.
- **The outputs.** `outputs` names each returned stream and its `primaryKey`. It can also specify the `write` mode and required delivery guarantee through `require`. Without `outputs`, each input stream is also an output with the same name.
- **Storage.** The cluster reads epoch inputs and writes results under the pipeline's `_spark/` prefix in the data plane's store. Use S3 or a directory mounted at the same path on the runner and cluster. `sparkAccess` supplies application-specific S3 credentials or an EMR runtime role. Otherwise, the application uses the cluster identity, which can reach every pipeline's prefix.

### Spark submission diagnostics

On a standalone master, OpenETL reads the driver ID from `spark-submit` output at `INFO` level. Keep the submission client's log level at `INFO`. If submission exits without that line, the step fails without retrying. A driver might still be running. Check the master's UI before submitting again.

On Kubernetes, a failed driver's sync log includes its last 100 log lines, up to 64 KiB. Property values are masked when their names contain `secret`, `password`, `token` or `access.key`.

The runner uses the kubeconfig or service account found by `spark-submit` to read driver logs. Those credentials must match the API server in `master` and permit `get` on `pods/log` in the application namespace. If log retrieval fails, the sync log records the reason. This does not change the step's result.

### Write a Spark transform

Your code uses [openetl-spark](../../libs/openetl-spark), built for Spark 3.5. In Java, a class implements `io.openetl.spark.Transform`, and `class` names it:

```java
Map<String, Dataset<Row>> apply(SparkSession spark, Map<String, Dataset<Row>> inputs, EpochContext ctx);
```

In Python, the file is the application, and hands its function to `openetl_spark.run`:

```python
import openetl_spark

def transform(spark, inputs, ctx):
    return {"spend_per_user": inputs["purchases"].groupBy("user_id").sum("amount")}

if __name__ == "__main__":
    openetl_spark.run(transform)
```

Each call receives one epoch. A join across streams cannot match rows from different epochs. Inputs include the [metadata columns](#reading-what-openetl-writes); outputs retain the columns your code preserves.

The library is not published to a package repository. `make spark-lib` builds the jar. The Python package is in `libs/openetl-spark/python`.

A `SparkCluster` can also run an application after a sync, as a `postSync` step, `spark: { cluster, code, args, conf }`. openetl-spark's `SyncContext` tells that application what the sync wrote.

The [Spark cluster reference](../reference/specs/spark-cluster.md) lists every target field, its requirements and defaults.

## Transforms on Flink

A continuous pipeline can run your Flink job between its source and its destination, on your own Flink cluster. The job reads each stream as a `DataStream` and writes its outputs back to OpenETL, which commits them to the destination. The job's checkpoints are the pipeline's.

```yaml
apiVersion: openetl.io/v1alpha1
kind: FlinkCluster
metadata:
  name: streaming
spec:
  rest:
    url: http://flink-jobmanager:8081
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: payments-scored
spec:
  mode: continuous
  source: { ref: payments }
  destination: { ref: local-postgres, namespace: payments }
  transform:
    flink:
      cluster: streaming
      code:
        jar: s3://my-bucket/jobs/score.jar
        sha256: SHA256
        class: com.example.Score
      outputs:
        - name: scored_payments
```

- **The cluster.** A `FlinkCluster` selects one target. `kubernetes` creates a `FlinkDeployment` through the Flink Kubernetes Operator in application mode. `rest` uses a standalone session cluster's REST API. `emr` submits a YARN step on EMR on EC2. Cluster-level `conf` applies to every job.
- **The code.** `code` specifies a jar at an `s3://` or `https://` URI and its SHA-256 digest. Supply the main `class` unless the jar manifest declares it. Job-level `parallelism` and `conf` configure that job.
- **The runner's bridge.** The job reads and writes through its pipeline's runner. Set `--bridge-listen`, such as `:7710`, to enable the bridge. Set `--bridge-advertise` to an address that reaches that specific runner. Do not use a load balancer address. A runner without a configured bridge cannot run Flink pipelines.
- **Storage.** Use S3 or a directory mounted at the same path on the runner and cluster. Job code, checkpoints and savepoints use the pipeline's `_flink/` prefix. `flinkAccess` supplies credentials or an EMR runtime role.

Build the job with [openetl-flink](../../libs/openetl-flink) for Flink 1.20. `OpenEtl.fromArgs` reads the arguments supplied by the runner. `stream` returns a `DataStream<RowData>` for an input stream, and `sink` writes an output. The input's `_openetl_op` becomes its `RowKind`.

OpenETL reads every pipeline stream, including streams the job does not use. The library is not published to a package repository. `make flink-lib` builds the jar.

The [Flink cluster reference](../reference/specs/flink-cluster.md) lists required target choices, authentication and resource defaults.

## Events and webhooks

OpenETL records an event when something happens to a pipeline, and sends the events you choose to an HTTP endpoint of yours. [`examples/webhook.yaml`](../../examples/webhook.yaml):

```yaml
apiVersion: openetl.io/v1alpha1
kind: Webhook
metadata:
  name: ops-alerts
spec:
  url: https://hooks.example.com/openetl
  events: ["sync.failed", "schema.*", "pipeline.lagging", "pipeline.recovered"]
  pipelines: [faker-to-postgres]
  secrets: [ops-alerts-key]
```

Each secret the webhook names holds a signing key. Make one before applying the webhook, then send a test event:

```sh
echo "whsec_$(openssl rand -base64 32)" | openetl secret set ops-alerts-key
openetl apply -f examples/webhook.yaml
openetl webhook test ops-alerts
```

- **Which events.** `events` takes event types, or globs of them such as `sync.*` or `*`. `pipelines` limits them to those pipelines; left out, the webhook gets every pipeline's events and the workspace's own.

  | Events | When |
  |---|---|
  | `sync.started`, `sync.succeeded`, `sync.failed`, `sync.stopped` | A sync, or a continuous pipeline's run, starts or ends |
  | `pipeline.created`, `pipeline.updated`, `pipeline.deleted` | A pipeline is applied or deleted |
  | `pipeline.paused`, `pipeline.resumed`, `pipeline.rescaled` | Someone pauses, resumes or rescales a pipeline |
  | `stream.paused`, `stream.resumed` | Someone, or heal, pauses or resumes one of a pipeline's streams |
  | `pipeline.overridden` | Heal changes how a pipeline runs, such as syncing less often, or someone undoes it |
  | `pipeline.reset` | Someone asks for a pipeline's streams to be read again from the start; `data.streams` names them, and is left out for every stream |
  | `pipeline.lagging`, `pipeline.recovered` | A continuous pipeline falls behind its source, and catches up |
  | `pipeline.teardown_failed` | What a deleted pipeline's connector keeps for it, such as a replication slot, couldn't be removed after 7 days of trying, and is left for someone to remove |
  | `schema.changed`, `schema.blocked` | A stream's schema changes, or a change can't be taken |
  | `incident.opened`, `incident.escalated`, `incident.resolved` | An incident opens, gets worse, or is resolved |
  | `webhook.test` | `openetl webhook test` sends one |

- **Payload.** OpenETL sends a `POST` with a JSON body containing `type`, `timestamp` and `data`, following [Standard Webhooks](https://www.standardwebhooks.com). Resource names use the API format, such as `workspaces/default/pipelines/faker-to-postgres`. [Platform events](../reference/events.md) lists the fields in each event's `data`.
- **Signatures.** Deliveries include `webhook-id`, `webhook-timestamp` and `webhook-signature` headers. `webhook-id` identifies the event. `webhook-signature` contains an HMAC-SHA256 signature for each configured key. Verify deliveries with a Standard Webhooks library. To rotate a key, configure both secrets before removing the old one.
- **Retries.** A `2xx` response within 15 seconds completes delivery. Missing responses, `408`, `429` and `5xx` responses trigger retries for about a day, honoring `Retry-After`. Other statuses fail delivery. Redirects are not followed. Use `webhook-id` to remove duplicate deliveries.
- **Automatic disabling.** A `410 Gone` response disables the webhook. Repeated failures for 3 days also disable it. A successful delivery or a gap of at least 24 hours between failures resets this period. After correcting the receiver, use `openetl webhook enable` to enable it again.
- **Deliveries.** `openetl webhook deliveries ops-alerts` lists the last 7 days' deliveries, with each one's state, status and error.
- **Which addresses.** The server sends webhooks to public addresses only, unless its `--egress-allow` setting names the private networks it may reach as well.

The [Webhook reference](../reference/specs/webhook.md) lists required fields and filter defaults.

## The API

Everything the CLI and the console do goes through OpenETL's API, served by `openetld server`. The same API is served four ways:

- **REST**, with JSON, under `/v1`, such as `GET /v1/workspaces/default/pipelines`. The server serves its OpenAPI description at `/openapi.yaml`.
- **Connect, gRPC and gRPC-Web**, from the protos in [`proto/openetl/api/v1`](../../proto/openetl/api/v1), such as `openetl.api.v1.PipelineService/ListPipelines`.

Send an API key in the `Authorization: Bearer KEY` header. Give each integration a separate service account with the minimum required role:

- `viewer` reads resources.
- `operator` can also start syncs and pause or resume pipelines.
- `editor` can also apply and delete resources.

```sh
openetl principal create reporting
openetl principal grant reporting viewer
openetl key create --out reporting.key reporting
curl -H "Authorization: Bearer $(cat reporting.key)" http://localhost:8080/v1/workspaces/default/pipelines
```

Responses use protobuf JSON. Enum values use their full names, such as `PIPELINE_MODE_SCHEDULED`. Durations use seconds, such as `3600s`.

Use `ApplyService` (`POST /v1/workspaces/default:apply`) to create or update sources, destinations, pipelines and other resources. It accepts the same documents sent by `openetl apply`.

## Metrics and traces

`openetld` exports metrics and traces through OpenTelemetry, configured by the standard `OTEL_*` environment variables. Nothing is exported until a signal's exporter, or an OTLP endpoint, is set:

```sh
export OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

To have Prometheus scrape each process instead, serve its metrics on a port:

```sh
export OTEL_METRICS_EXPORTER=prometheus
export OTEL_EXPORTER_PROMETHEUS_HOST=0.0.0.0
export OTEL_EXPORTER_PROMETHEUS_PORT=9464
```

The [Kubernetes manifests](../self-host/kubernetes.md) set exactly that. Every process reports as the service `openetld`, with its role in `openetl.role` and its data plane in `openetl.data_plane`. Metrics are named `openetl.*`, such as `openetl.job.rows.committed`, `openetl.job.lag` and `openetl.pipeline.freshness`. Traces cover API calls, the runners' work, and each attempt's recovery and commits.

[Monitor an installation](../self-host/monitoring.md) explains the metrics, missing-series behavior and alerts.

## Reading what OpenETL writes

Every row OpenETL writes to a table or a file carries metadata columns besides the source's:

| Column | What it holds |
|---|---|
| `_openetl_id` | The ID of this version of the record: a hash of the stream, the record's primary key (or, without one, every column), and its `_openetl_seq`. The same version gets the same ID however often it's read. |
| `_openetl_op` | `insert`, `update` or `delete`. Only streams read with CDC have updates and deletes. |
| `_openetl_seq` | Where this version comes in the source's order: its position in the source's log, its cursor value, or when the read started. A later version of a record has a higher one. |
| `_openetl_synced_at` | When OpenETL wrote it, in UTC. |
| `_openetl_deleted` | `true` for a row deleted at the source. Only in streams written with merge and soft deletes, the default, which keep a deleted row rather than removing it. |

A stream whose [delivery guarantee](../get-started/concepts.md#delivery-guarantees) is at least once can hold the same version of a record twice, with the same `_openetl_id`. Keep one of each when you read it:

```sql
select *
from (
  select *, row_number() over (partition by _openetl_id order by _openetl_synced_at) as n
  from faker.purchases
) as t
where n = 1;
```

To read the latest version of each record from a stream written with append, partition by its primary key instead, and order by `_openetl_seq` descending.

The destination connector defines the table, file or event format and how readers see committed data. Follow that connector version's documentation for catalog setup, reader compatibility and any destination-specific metadata.
