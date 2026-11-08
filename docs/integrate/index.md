---
title: Integrate
description: Connect OpenETL to dbt, Spark, Flink, your own code and your monitoring, and read what it writes.
---

This section is for people who know another system well and want it to work with OpenETL. Each part says what OpenETL sends or expects, and shows the least setup that works. The YAML documents here are applied like any other, with `openetl plan` and `openetl apply`; [Pipelines](../pipelines/index.md) covers that loop.

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

- **The project.** `path` is the project's directory, relative to the file. `openetl apply` packs the directory and sends it, leaving out hidden files, `target/`, `logs/` and `profiles.yml`. Runners don't run `dbt deps`, so vendor your packages into `dbt_packages/`, which is sent. A packed project is at most 16 MiB.
- **The step.** `command` is `build`, the default, `run` or `test`. `select` and `exclude` are dbt's `--select` and `--exclude`. `schema` is the schema dbt builds into; by default, the one the pipeline writes to.
- **The connection.** The runner writes a `profiles.yml` with one profile, named as your `dbt_project.yml`'s `profile` says, that points at the pipeline's destination. The credentials go to dbt in its environment, not in the file. `OPENETL_NAMESPACE` holds the schema the pipeline wrote to, so your sources can find the tables, as [`examples/dbt/models/sources.yml`](../../examples/dbt/models/sources.yml) does:

  ```yaml
  sources:
    - name: faker
      schema: "{{ env_var('OPENETL_NAMESPACE') }}"
      tables:
        - name: users
        - name: purchases
  ```

- **When it runs.** Steps run in order once the sync's data is committed. A step that fails fails the sync, and the steps after it don't run. A sync runs the project as it was when the sync started.
- **Where it runs.** Runners run dbt: the `dbt` on their `PATH`, or the executable their `--dbt` setting (`OPENETL_DBT`) names, with the destination's adapter installed, `dbt-postgres`.

## Transforms on Spark

A scheduled pipeline can run your Spark code between its source and its destination, on your own Spark cluster. A sync commits in steps called epochs. For each epoch, your code gets one DataFrame for each stream the source read, and returns a DataFrame for each output; OpenETL writes the outputs to the destination as streams of their own.

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

- **The cluster.** A `SparkCluster` has `submit` or `emr`. `submit` runs applications through `spark-submit` in cluster deploy mode, on a standalone master (`spark://HOST:PORT`), YARN (`yarn`) or Kubernetes (`k8s://https://HOST:PORT`); runners need Spark 3.5's `spark-submit`, on their `PATH` or where their `--spark-submit` setting says. `emr` adds steps to an EMR on EC2 cluster, by `clusterId` or `clusterName`, in `region`.
- **The code.** `code` is a jar or a Python file, at an `s3://` or `https://` URI the runner can read, with its SHA-256 in hex (`shasum -a 256 spend.jar`). The runner checks the digest and gives the cluster its own copy. A standalone master can't run a Python file in cluster deploy mode, so use a jar there.
- **The outputs.** `outputs` names what your code returns, each with its `primaryKey`, and optionally its `write` mode and the delivery it must `require`. Without `outputs`, each input stream is an output under its own name.
- **Storage.** The cluster reads each epoch from, and writes its results to, the data plane's store, under the pipeline's `_spark/` prefix. So that store has to be on S3, or a directory the cluster mounts at the same path. `sparkAccess` gives the application S3 keys of its own, or an EMR runtime role, to reach it; without it, the application uses the cluster's own identity, which reaches every pipeline's prefix.

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

Each call sees one epoch, so a join across streams misses rows that land in different epochs. Inputs carry the `_openetl_*` columns described [below](#reading-what-openetl-writes), and outputs keep the ones they don't drop. The library isn't published to a package repository: `make spark-lib` builds the jar, and its Python package is in `libs/openetl-spark/python`.

A `SparkCluster` can also run an application after a sync, as a `postSync` step, `spark: { cluster, code, args, conf }`. openetl-spark's `SyncContext` tells that application what the sync wrote.

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

- **The cluster.** A `FlinkCluster` has one of `kubernetes`, which runs each job as a `FlinkDeployment` of the Flink Kubernetes Operator, in application mode; `rest`, a standalone session cluster's REST API; or `emr`, a step on YARN on an EMR on EC2 cluster. Its `conf` is Flink configuration for every job.
- **The code.** `code` is a jar at an `s3://` or `https://` URI, with its SHA-256, and its main `class` unless the jar's manifest names one. `parallelism` and `conf` set the job's own.
- **The runner's bridge.** The job reads and writes through the runner that runs the pipeline. Runners serve that bridge when their `--bridge-listen` setting is set, such as `:7710`, and tell jobs to reach them at `--bridge-advertise`, which must reach that runner itself rather than a load balancer. A runner without it doesn't run Flink pipelines.
- **Storage.** As with Spark, the data plane's store must be on S3 or in a directory the cluster mounts at the same path; the job's code, checkpoints and savepoints go under the pipeline's `_flink/` prefix. `flinkAccess` gives it keys or an EMR runtime role.

The job is built with [openetl-flink](../../libs/openetl-flink), for Flink 1.20. `OpenEtl.fromArgs` reads the arguments the runner passes the job, `stream` gives each stream as a `DataStream<RowData>`, and `sink` writes an output. A change's `_openetl_op` becomes the row's `RowKind`. Every stream of the pipeline is read, whether the job uses it or not. The library isn't published to a package repository: `make flink-lib` builds the jar.

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
  | `pipeline.lagging`, `pipeline.recovered` | A continuous pipeline falls behind its source, and catches up |
  | `schema.changed`, `schema.blocked` | A stream's schema changes, or a change can't be taken |
  | `incident.opened`, `incident.escalated`, `incident.resolved` | An incident opens, gets worse, or is resolved |
  | `webhook.test` | `openetl webhook test` sends one |

- **What's sent.** A `POST` with a JSON body, laid out as [Standard Webhooks](https://www.standardwebhooks.com) says: `type`, `timestamp`, and the event's `data`, which names resources as the API does, such as `workspaces/default/pipelines/faker-to-postgres`.
- **Signatures.** Each delivery carries the headers `webhook-id`, the event's ID, `webhook-timestamp` and `webhook-signature`, an HMAC-SHA256 under each of the webhook's keys. Any Standard Webhooks library verifies them. Naming two secrets lets you add a new key before removing the old one.
- **Retries.** A `2xx` answer within 15 seconds is a delivery. No answer, a `408`, a `429` or a `5xx` is tried again for about a day, honoring `Retry-After`; another status fails the delivery. Redirects aren't followed. A delivery can arrive more than once, so drop repeats by `webhook-id`. An endpoint that answers `410 Gone`, or fails every delivery for 3 days, has its webhook disabled; `openetl webhook enable` turns it back on.
- **Deliveries.** `openetl webhook deliveries ops-alerts` lists the last 7 days' deliveries, with each one's state, status and error.
- **Which addresses.** The server sends webhooks to public addresses only, unless its `--egress-allow` setting names the private networks it may reach as well.

## The API

Everything the CLI and the console do goes through OpenETL's API, served by `openetld server`. The same API is served four ways:

- **REST**, with JSON, under `/v1`, such as `GET /v1/workspaces/default/pipelines`. The server serves its OpenAPI description at `/openapi.yaml`.
- **Connect, gRPC and gRPC-Web**, from the protos in [`proto/openetl/api/v1`](../../proto/openetl/api/v1), such as `openetl.api.v1.PipelineService/ListPipelines`.

Every call sends an API key as `Authorization: Bearer KEY`. Give each integration a service account of its own, with the least role it needs: `viewer` reads, `operator` also syncs, pauses and resumes pipelines, and `editor` also applies and deletes.

```sh
openetl principal create reporting
openetl principal grant reporting viewer
openetl key create reporting --out reporting.key
curl -H "Authorization: Bearer $(cat reporting.key)" http://localhost:8080/v1/workspaces/default/pipelines
```

Responses use the protos' JSON form, which isn't quite the YAML you apply: enums are spelled in full (`PIPELINE_MODE_SCHEDULED`) and durations in seconds (`3600s`). Sources, destinations, pipelines and the other kinds are created and changed through `ApplyService` (`POST /v1/workspaces/default:apply`), which takes the same documents `openetl apply` sends.

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

Iceberg tables are committed through your REST or Glue catalog, so any engine that reads that catalog reads them as it would any other Iceberg table.
