---
title: Flink cluster
description: Every field of a FlinkCluster document, including target choices, credentials and resource defaults.
---

A `FlinkCluster` names where runners deploy continuous pipelines' Flink 1.20 jobs. It does not install Flink or its Kubernetes Operator. [Transform a stream with Flink](../../integrate/flink.md) covers the job contract, runner bridge and storage requirements.

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `FlinkCluster` |
| `metadata.name` | name | Yes | — | Lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | No | The CLI's `--workspace`, otherwise `default` | The workspace the resource belongs to |
| `spec` | object | Yes | — | The fields below |

Fields that aren't listed are refused. **Required** applies within the containing object; an optional object may be omitted, but its required children must be set when you include it. **Default** describes what omission means.

## spec

Exactly one of `kubernetes`, `rest` and `emr` is required.

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `kubernetes` | object | One of `kubernetes`, `rest` or `emr` | — | Create each job as a `FlinkDeployment` using the Flink Kubernetes Operator. |
| `kubernetes.namespace` | name | Yes in `kubernetes` | — | The namespace for job deployments. |
| `kubernetes.image` | string | No | `flink:1.20` | Flink 1.20 image for job pods. The job needs the S3 filesystem plugin when its storage is on S3. |
| `kubernetes.serviceAccount` | name | No | `flink` | Job pods' service account. Kubernetes high availability needs permission to edit ConfigMaps. |
| `kubernetes.kubeconfig` | secret reference | No | The runner's in-cluster identity | `{ secret: NAME }` holding a kubeconfig for the target cluster. |
| `kubernetes.jobManager` | object | No | 1 CPU and `2048m` memory | JobManager resources. |
| `kubernetes.jobManager.cpu` | number, 0 or more | No | `1`; `0` also selects the default | CPUs for the JobManager. |
| `kubernetes.jobManager.memory` | quantity | No | `2048m` | Memory, such as `2048m` or `2g`. |
| `kubernetes.taskManager` | object | No | 1 CPU and `2048m` memory | TaskManager resources. |
| `kubernetes.taskManager.cpu` | number, 0 or more | No | `1`; `0` also selects the default | CPUs for each TaskManager. |
| `kubernetes.taskManager.memory` | quantity | No | `2048m` | Memory, such as `2048m` or `2g`. |
| `rest` | object | One of `kubernetes`, `rest` or `emr` | — | Use an existing session cluster's REST API. |
| `rest.url` | HTTP or HTTPS URL | Yes in `rest` | — | The REST API's base URL, without a query string. A trailing slash is removed. |
| `rest.auth` | secret reference | No | No bearer token | `{ secret: NAME }` holding a bearer token for a proxy in front of the API. |
| `emr` | object | One of `kubernetes`, `rest` or `emr` | — | Run each job on YARN as an EMR on EC2 step. |
| `emr.clusterId` | string | One of `clusterId` or `clusterName` in `emr` | — | The cluster's ID. |
| `emr.clusterName` | string | One of `clusterId` or `clusterName` in `emr` | — | Find the one active cluster of this name at each deployment. |
| `emr.region` | string | Yes in `emr` | — | The AWS region containing the cluster. |
| `conf` | map of scalar values or secret references | No | None | Flink configuration for every job. Values become text; secrets use `{ secret: NAME }` in the same workspace. Job settings override cluster settings. |

Configuration keys contain a dot and must not be options [reserved by the runner](./pipeline.md#transforms), such as checkpoint locations, restart strategy or deployment identity.

```yaml
apiVersion: openetl.io/v1alpha1
kind: FlinkCluster
metadata:
  name: streaming
spec:
  rest:
    url: http://flink-jobmanager:8081
```

The runner must expose its bridge at an address the Flink job can reach directly. Code, parallelism, output streams and storage credentials are [Pipeline fields](./pipeline.md#transforms). The resource's settings are versioned, and pipeline versions pin them.
