---
title: Transform data with Spark
description: Build a Spark transform, configure submission and shared storage, and verify or recover its pipeline.
---

Use a Spark transform to process each epoch of a scheduled sync before OpenETL writes it to the destination. An epoch is the data between two checkpoints. Your code receives DataFrames and returns named output DataFrames.

Use a Spark post-sync step instead when your application must read tables after the entire sync commits. Both operations use a cluster that you provide. A `SparkCluster` resource registers that cluster; it does not install Spark.

## Prepare submission and storage

Use Spark 3.5. The Java example uses Spark 3.5.8 and builds with JDK 17 and Maven 3.9. Run the commands below from a source checkout of this repository.

Choose the submission target:

| Target | Runner requirements | Cluster requirements |
|---|---|---|
| Standalone | Spark 3.5 `spark-submit`, access to the master and its web UI | Java applications; Python cluster deploy mode is unavailable. |
| YARN | Spark 3.5 `spark-submit` and the Hadoop configuration and credentials for the cluster | Driver and executors can read the pipeline's storage. |
| Kubernetes | Spark 3.5 `spark-submit` and credentials that can create the application | A Spark image with your runtime dependencies and access to the pipeline's storage. |
| EMR on EC2 | AWS credentials that can submit and monitor steps on the selected cluster | An existing cluster with Spark and storage access. |

For `submit` targets, set the runner's `OPENETL_SPARK_SUBMIT` or `--spark-submit` if the executable is outside `PATH`. OpenETL submits in cluster deploy mode. The driver runs on the cluster.

The runner and cluster exchange input and output Parquet files through the data plane's checkpoint storage. The supported locations are S3 and a shared directory. A directory must be mounted at the same absolute path on the runner, driver and executors.

For S3, the cluster needs its S3A filesystem dependencies and read, write, list and cleanup permissions under the pipeline's `_spark/` prefix. The runner's bucket credentials do not automatically become cluster credentials.

Use the cluster's identity or the pipeline's `sparkAccess`. The latter can supply S3A keys for the bucket or an EMR runtime role. See [storage access fields](../reference/specs/pipeline.md#access-to-the-data-planes-storage).

## Get the library

Your application depends on `openetl-spark`. Use the version of your OpenETL installation. Each release has the jar, `openetl-spark-VERSION.jar`, as a release file. The jar is not in a Maven repository. Download it, and install it in your local Maven repository:

```sh
curl -fsSLO https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha/openetl-spark-0.1.0-alpha.jar
mvn install:install-file -Dfile=openetl-spark-0.1.0-alpha.jar \
  -DgroupId=io.openetl -DartifactId=openetl-spark -Dversion=0.1.0-alpha -Dpackaging=jar
```

Your project then depends on `io.openetl:openetl-spark:0.1.0-alpha`, and on Spark with `provided` scope. The release's `checksums.txt` has the jar's SHA-256. [Install by hand](../get-started/install.md#install-by-hand) shows how to check a file against it.

## Build the example transform

[`examples/spark-transform`](../../examples/spark-transform) is a Maven project that adds `transform_epoch` to every input stream. It preserves each stream's name, rows and metadata columns.

Get the release's source, and package the example with the library that you installed:

```sh
git clone --depth 1 --branch v0.1.0-alpha https://github.com/openetl-io/openetl
cd openetl
mvn -f examples/spark-transform/pom.xml package -Dopenetl.version=0.1.0-alpha
```

The application jar is `examples/spark-transform/target/spark-transform-0.1.0.jar`. It includes `openetl-spark`. Spark itself has `provided` scope and comes from the cluster.

To build the library from the checkout instead, run `mvn -f libs/openetl-spark/java/pom.xml install`. Then package the example without `-Dopenetl.version`.

The [`AddEpoch` class](../../examples/spark-transform/src/main/java/io/openetl/examples/AddEpoch.java) implements `io.openetl.spark.Transform`. OpenETL's driver instantiates the class and calls its `apply` method. The class needs a public no-argument constructor.

Your implementation must follow this contract:

| Input or result | Meaning |
|---|---|
| `SparkSession` | The application's session. |
| `inputs` | A DataFrame for every selected input stream, keyed by stream name. A stream can be empty in an epoch. |
| `EpochContext.generation()` | The attempt generation. |
| `EpochContext.epoch()` | The epoch being processed. |
| `EpochContext.outputs()` | The output names that the transform must return. |
| Returned map | One DataFrame for each declared output. Missing or undeclared outputs fail the transform. |

Return empty DataFrames when an output has no rows. Preserve the appropriate schema. Inputs include `_openetl_*` metadata columns; the output retains only the columns your code returns.

Each call sees one epoch. A join between two input streams can miss rows in different epochs. An aggregate calculates that epoch's values, not a total over the complete destination table. Choose a post-sync application for operations that need all committed tables.

An attempt can process an epoch again after recovery. Keep external side effects safe to repeat. Let OpenETL write the returned DataFrames to the destination.

## Publish the application and configure the pipeline

Put the jar at an `https://`, `s3://` or `s3a://` URI that the runner can read. The runner reads an `s3://` URI with the same credentials and query parameters as its own bucket. See [Buckets](../self-host/index.md#buckets). Calculate the jar's digest:

```sh
shasum -a 256 examples/spark-transform/target/spark-transform-0.1.0.jar
```

The runner checks the SHA-256 before using the file and copies it into the pipeline's storage. Rebuild, publish and update the digest together when you change code.

Copy [`examples/faker-spark-to-parquet.yaml`](../../examples/faker-spark-to-parquet.yaml) and edit it for your installation. It uses only built-in connectors. Its transform is:

```yaml
transform:
  spark:
    cluster: example-spark
    code:
      jar: https://artifacts.example.com/spark-transform-0.1.0.jar
      sha256: "0000000000000000000000000000000000000000000000000000000000000000"
      class: io.openetl.examples.AddEpoch
```

Replace the example URI and zero digest with your published jar and its actual digest before planning or applying. Also set the cluster's master and the destination's output path. The destination path is on the runner's filesystem.

The default outputs keep the input stream names and settings. Declare `outputs` when you rename streams or change their primary keys, write modes or required delivery guarantees. Names returned by the application must match the declared names. See [transform fields](../reference/specs/pipeline.md#transforms).

The application jar includes the Java driver and library. For a thin application jar, supply the library separately through `spark.jars` at a location the cluster can read.

## Plan, run and inspect

After editing the example file:

```sh
openetl plan -f examples/faker-spark-to-parquet.yaml
openetl apply -f examples/faker-spark-to-parquet.yaml
openetl discover spark-data
openetl syncs faker-spark-to-parquet
openetl logs -f faker-spark-to-parquet
```

The first apply starts a sync. Confirm that it succeeds and that output Parquet files contain `transform_epoch` on both `users` and `purchases`. Compare output row counts with the source's configured count. The example emits 100 rows per stream.

Parquet output appends files. Another sync adds another copy of the generated rows. If the first plan had no discovery result, repeat it after discovery to inspect the stream settings.

Use the application's driver ID and cluster UI to inspect its execution. OpenETL records submission output in the sync log. Standalone submission uses the master's web UI to monitor and stop the driver.

## Use Python instead of Java

Python transforms run on YARN, Kubernetes or EMR, where Python cluster deploy mode is supported. Install [`openetl-spark`'s Python package](../../libs/openetl-spark/python) in the application environment and make it available to the driver and executors. PySpark comes from the cluster.

Each release has the package as a wheel. Python's version rules write `0.1.0-alpha` as `0.1.0a0`:

```sh
pip install https://github.com/openetl-io/openetl/releases/download/v0.1.0-alpha/openetl_spark-0.1.0a0-py3-none-any.whl
```

Your Python file defines `transform(spark, inputs, ctx)` and calls `openetl_spark.run(transform)` from its main entry point. It returns a dictionary of output DataFrames. The same epoch and output rules apply as in Java.

Set `code.python` to the file's URI and supply its SHA-256. Omit `code.jar` and `code.class`. Additional Python dependencies must be installed in the cluster environment or distributed through Spark's application configuration.

## Run a Spark application after a sync

Use `postSync[].spark` with a cluster, code URI, SHA-256, optional arguments and Spark properties. A jar's `code.class` names its main class. This is an ordinary Spark application, rather than a `Transform` implementation.

The same example jar includes [`ReportSync`](../../examples/spark-transform/src/main/java/io/openetl/examples/ReportSync.java). It reads the context and prints the sync ID and source row counts. The complete [post-sync pipeline](../../examples/faker-spark-post-sync.yaml) selects that class:

```yaml
postSync:
  - spark:
      cluster: example-spark
      code:
        jar: https://artifacts.example.com/spark-transform-0.1.0.jar
        sha256: "0000000000000000000000000000000000000000000000000000000000000000"
        class: io.openetl.examples.ReportSync
```

Set its cluster, artifact URI, digest and destination path before applying:

```sh
openetl plan -f examples/faker-spark-post-sync.yaml
openetl apply -f examples/faker-spark-post-sync.yaml
openetl syncs faker-spark-post-sync
openetl logs -f faker-spark-post-sync
```

Confirm that the sync succeeds and that the application prints 200 source rows for the example's two streams. Driver output is available through your cluster's logging. Stop the example's scheduled work when you finish:

```sh
openetl pause faker-spark-post-sync
```

Load `io.openetl.spark.SyncContext` with `SyncContext.load(spark)` to identify the committed sync. The Python package provides `openetl_spark.SyncContext.load(spark)`.

The context includes:

- The pipeline and sync identifiers.
- The destination connector and namespace.
- Input stream names, source row counts and destination table names where available.
- The checkpoint range committed by the sync.

Source row counts include quarantined rows. They are not destination table counts. Your application must configure its own destination readers, catalog and credentials. The context describes the sync; it does not contain database credentials.

Post-sync steps run in order after copied data commits. A failed Spark step fails the sync and prevents later steps. It does not roll back copied data. The [pipeline reference](../reference/specs/pipeline.md#steps-after-a-sync) lists the step fields.

## Diagnose failures and update code

| Symptom | Action |
|---|---|
| `spark-submit` cannot start | Install Spark 3.5 on the runner or correct `OPENETL_SPARK_SUBMIT`. |
| Driver or transform class is missing | Include the application class and `openetl-spark` in the runtime classpath. |
| Digest mismatch | Publish the intended jar and update its URI and SHA-256 together. |
| Files are missing or access is denied | Check storage from the driver and executors, including identical directory mount paths or S3A permissions. |
| The output map is rejected | Return exactly the declared names, including empty DataFrames where required. |
| An aggregate or join has incomplete results | Check whether the operation incorrectly assumes that one epoch contains the entire dataset. |
| Standalone submission reports no driver ID | Keep the submission client's log level at `INFO`. Inspect the master's UI for an existing driver before submitting again. |

On Kubernetes, the runner can attach the failed driver pod's last log lines to the sync log. Its target-cluster identity needs `get` on `pods/log` in the application namespace.

Application failures report `SPARK_FAILED` without an automatic retry. Correct the code or configuration, update the pipeline document and apply it. Cluster setting updates create new dependent pipeline versions. An existing sync keeps its pinned settings.

Pause the example when you finish:

```sh
openetl pause faker-spark-to-parquet
```

Pausing does not remove the output files. See [Spark cluster](../reference/specs/spark-cluster.md) for target settings and [delivery guarantees](../pipelines/delivery.md) for output behavior.
