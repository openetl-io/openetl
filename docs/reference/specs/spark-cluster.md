---
title: Spark cluster
description: Every field of a SparkCluster document, with required target choices and configuration defaults.
---

A `SparkCluster` names a cluster on which runners submit Spark applications. It does not create the cluster. Pipelines use it for a scheduled transform or a post-sync step. [Transform data with Spark](../../integrate/spark.md) covers the application contract and storage requirements.

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `SparkCluster` |
| `metadata.name` | name | Yes | — | Lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | No | The CLI's `--workspace`, otherwise `default` | The workspace the resource belongs to |
| `spec` | object | Yes | — | The fields below |

Fields that aren't listed are refused. **Required** applies within the containing object; an optional object may be omitted, but its required children must be set when you include it. **Default** describes what omission means.

## spec

Exactly one of `submit` and `emr` is required.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `submit` | object | One of `submit` or `emr` | — | Submit with the runner's `spark-submit` in cluster deploy mode. |
| `submit.master` | string | Yes in `submit` | — | `spark://HOST:PORT`, `yarn` or `k8s://https://HOST:PORT`. |
| `submit.conf` | map of scalar values or secret references | No | None | Spark properties for every application. Values are converted to text; a secret is `{ secret: NAME }` in the same workspace. Application settings override cluster settings. |
| `submit.ui` | HTTP or HTTPS URL | No | Port 8080 on the standalone master's host | The standalone master's web UI, used to watch and stop drivers. Allowed only with a `spark://` master. |
| `emr` | object | One of `submit` or `emr` | — | Add steps to an existing EMR on EC2 cluster. |
| `emr.clusterId` | string | One of `clusterId` or `clusterName` in `emr` | — | The cluster's ID. |
| `emr.clusterName` | string | One of `clusterId` or `clusterName` in `emr` | — | Find the one active cluster of this name at each submission. |
| `emr.region` | string | Yes in `emr` | — | The AWS region containing the cluster. |

Each `submit.conf` property starts with `spark.`. The runner sets these itself, so you cannot set them: `spark.master`, `spark.submit.deployMode`, `spark.app.name`, `spark.yarn.tags`, `spark.kubernetes.driver.pod.name`, or any property starting with `spark.openetl.`.

```yaml
apiVersion: openetl.io/v1alpha1
kind: SparkCluster
metadata:
  name: analytics
spec:
  submit:
    master: spark://spark-master:7077
```

A source-build runner needs Spark 3.5's `spark-submit`, and the cluster must reach the pipeline's storage. Code URIs, hashes, arguments and outputs are [Pipeline fields](./pipeline.md#transforms). The resource's settings are versioned, and pipeline versions pin them.
