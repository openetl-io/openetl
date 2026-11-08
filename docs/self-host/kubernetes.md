---
title: Install on Kubernetes
description: Run OpenETL's API servers, orchestrators, runners and pod launcher on a Kubernetes cluster, and scale them with KEDA and Karpenter.
---

[`deploy/kubernetes`](../../deploy/kubernetes) holds plain manifests for running OpenETL on a Kubernetes cluster, applied with `kubectl apply -k`. Run the commands on this page from a clone of the repository. The manifests install:

| File | What it runs |
|---|---|
| [`config.yaml`](../../deploy/kubernetes/config.yaml) | The settings every component shares (ConfigMap `openetl`) |
| [`server.yaml`](../../deploy/kubernetes/server.yaml) | The API server: two replicas, Service `openetl-server` on port 8080 |
| [`orchestrator.yaml`](../../deploy/kubernetes/orchestrator.yaml) | Two orchestrators, which run workflows and place jobs on runners |
| [`runner-default.yaml`](../../deploy/kubernetes/runner-default.yaml) | The `default` pool's shared runners |
| [`launcher-default.yaml`](../../deploy/kubernetes/launcher-default.yaml) | The `default` pool's pod launcher, which runs large jobs in pods of their own, and the PodTemplate they're made from |
| [`keda/`](../../deploy/kubernetes/keda) | A component that scales the runners with KEDA |
| [`karpenter/`](../../deploy/kubernetes/karpenter) | Karpenter node pools, and a component that sends runners and job pods to them |

Postgres and object storage aren't included: point OpenETL at your own.

## Before you start

- **Postgres** for the metadata database. On the default orchestrator (DBOS), the orchestrators also keep workflows in a second database on the same server, named after the first with `_dbos`, which they create if the user may. Otherwise create it, or name another with `OPENETL_DBOS_DATABASE_URL`. Don't connect through a transaction pooler.
- **A bucket** in S3, GCS or Azure Blob Storage for checkpoints and staging. Runners and job pods need read and write access to it, through a workload identity or keys in the Secret `openetl-runner`.
- **`openetld`'s image**, pushed to a registry your cluster pulls from, and set under `images` in `kustomization.yaml`. No image has been released, so build it with `make engine-linux image IMAGE=REGISTRY/openetl:TAG`, with [openetl-connectors](https://github.com/openetl-io/openetl-connectors) cloned next to this repository; the [Compose guide](./compose.md#before-you-start) says what that builds. On Linux, `cargo build -p openetl-engine` in place of `make engine-linux` will do too.
- **kubectl** 1.27 or later.

## Install

1. Set `OPENETL_CHECKPOINTS` in `config.yaml` to your bucket, such as `s3://my-bucket?region=eu-west-1`.

2. Create the namespace, and the Secret the server and orchestrators read:

   ```sh
   kubectl create namespace openetl
   kubectl -n openetl create secret generic openetl \
     --from-literal=OPENETL_DATABASE_URL='postgres://openetl:PASSWORD@HOST:5432/openetl' \
     --from-literal=OPENETL_SECRETS_KEY="base64key://$(openssl rand -base64 32 | tr '+/' '-_')"
   ```

   `OPENETL_SECRETS_KEY` encrypts the secrets pipelines use. Keep a copy: without it they can't be read. A KMS key works too (`awskms://`, `gcpkms://`, `azurekeyvault://`).

3. Apply the manifests and wait for the server:

   ```sh
   kubectl apply -k deploy/kubernetes
   kubectl -n openetl rollout status deploy/openetl-server
   ```

   Runners and the launcher wait for their Secret, made in step 5.

4. Get an owner key. The server writes one on its first start; this writes a new one and prints it, whichever server pod answers:

   ```sh
   kubectl -n openetl exec deploy/openetl-server -- sh -c 'openetld bootstrap-key 2>/dev/null && cat .openetl/admin.key'
   ```

5. Create a runner token through the API, and the Secret runners read. Add the bucket's keys to the Secret if the pods have no workload identity:

   ```sh
   kubectl -n openetl port-forward svc/openetl-server 8080 &
   export OPENETL_API_URL=http://localhost:8080 OPENETL_API_KEY=<the owner key>
   openetl dataplane token create default --out runner.token
   kubectl -n openetl create secret generic openetl-runner \
     --from-literal=OPENETL_RUNNER_TOKEN="$(cat runner.token)"
   ```

To serve the API outside the cluster, put an Ingress or a load balancer in front of the Service `openetl-server`, and set `OPENETL_PUBLIC_URL` in `config.yaml` to the URL people use.

Every component serves Prometheus metrics on port 9464, and its pods carry the `prometheus.io/scrape` and `prometheus.io/port` annotations.

## How jobs are placed

Each job has a size, from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB), which the pipeline's `spec.resources` sets or openetl picks. The orchestrators place it in its pool (`spec.resources.pool`, `default` if not set):

- **On a shared runner** with room. A runner's room is its limits less `OPENETL_RESERVE` (0.5 vCPU and 512 MiB), so in `runner-default.yaml` it's 3.5 vCPUs and 7.5 GiB. Keep its requests equal to its limits.
- **In a pod of its own,** made by the pool's launcher, when the job is `OPENETL_POD_MIN_SIZE` or larger (`m` here), or when no runner of the pool could ever take it. The pod's requests and limits are the job's size plus the reserve. `OPENETL_MAX_PODS` (20) and `OPENETL_MAX_POD_CPU` (64) cap them; jobs over the caps wait.

Jobs that don't fit anywhere wait until there's room. A runner told to stop finishes its jobs at a checkpoint first, up to 2 minutes for a sync and 10 for a stream, and they're placed again; its grace period covers that.

A pod's spec comes from the PodTemplate `openetl-attempt-default`, to which the launcher adds the job, labels, resources and node constraints. The launcher reads the template when it starts, so restart it after changing the template (`kubectl -n openetl rollout restart deploy/openetl-launcher-default`).

### Spot nodes

Sync pods tolerate GKE's and AKS's spot taints, and run on spot nodes wherever they're offered: on an interruption notice they finish at a checkpoint and are placed again. Stream pods tolerate them too, but prefer nodes that aren't spot, so they run on spot only when there's nothing else, and they carry `karpenter.sh/do-not-disrupt`, so consolidation leaves them alone. A stream interrupted on spot finishes at a checkpoint if it can within the notice, and otherwise starts again from its last one; either way nothing is lost or written twice.

To keep streams off spot nodes entirely, set `OPENETL_POD_STREAM_SPOT` to `avoid` on the launcher. They then never start on a cluster that only has spot nodes. Karpenter asks for a node that isn't spot before it relaxes the preference, but the Cluster Autoscaler doesn't weigh it when it picks a node group, so with the Cluster Autoscaler, set `avoid` if streams must never run on spot.

To add a pool, copy `runner-default.yaml` and `launcher-default.yaml` with another name and `OPENETL_POOL`, and point pipelines at it with `spec.resources.pool`. A pool with only a launcher runs every job in pods.

## Scaling

### Runners, with KEDA

The `keda` component scales the `default` pool's runners on the orchestrators' placement metrics: the CPU and memory of jobs running on the pool's runners or waiting for one, over what one runner takes. Jobs bound for pods aren't counted, since the launcher makes those. It needs [KEDA](https://keda.sh) 2.x, and a Prometheus that scrapes the annotated pods.

1. In `keda/runner-default.yaml`, set `serverAddress` to your Prometheus. If you change the runners' size, change both `threshold`s to match their room.
2. Uncomment `components` in `kustomization.yaml`, with `keda` only, and apply.

The queries, for scaling with something else:

```promql
(sum(max by (size) (openetl_placement_cpu_reserved{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
+ (sum(max by (size) (openetl_placement_cpu_pending{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
```

and the same over `openetl_placement_memory_reserved_bytes` and `openetl_placement_memory_pending_bytes`. Every orchestrator exports the same values, so `max by (size)` counts them once. `runs_on="pod"` is what the launcher's pods have or wait for, and `openetl_placement_pending_age_seconds` how long the oldest job has waited.

KEDA keeps at least one runner: a pool with a launcher and no runners sends every job to pods. Scaling in waits ten minutes of less demand, removes one runner a minute, and may remove one that's running jobs; they move to another runner after a checkpoint.

### Nodes, with Karpenter

`karpenter/nodepools.yaml` has two Karpenter node pools for AWS, which expect an `EC2NodeClass` named `default`:

- `openetl-attempts`, for job pods, on spot or on-demand instances. Consolidation leaves stream pods alone, and sync pods on spot finish at a checkpoint on an interruption notice and are placed again.
- `openetl-runners`, for shared runners, on-demand only, removed only when empty.

Apply it on its own (`kubectl apply -f deploy/kubernetes/karpenter/nodepools.yaml`), then add `karpenter` to `components` in `kustomization.yaml`, which gives runners and job pods the node selector and toleration for their pool. A pool of runners that only runs syncs can use spot instances too. Where only spot instances can be added, allow spot in `openetl-runners` as well; streams on those runners are interrupted as [stream pods on spot](#spot-nodes) are.

With the Cluster Autoscaler instead, make node groups with the same `openetl.io/nodes` labels and taints.

### The control plane

API servers and orchestrators hold no state but the database, so a plain HorizontalPodAutoscaler on CPU scales them. On DBOS, an orchestrator that stops has its workflows taken over by another within a minute.

## Temporal

To run workflows on Temporal instead of DBOS, set `OPENETL_ORCHESTRATOR: temporal`, `OPENETL_TEMPORAL_ADDRESS` and `OPENETL_TEMPORAL_NAMESPACE` in `config.yaml` before the first install. Changing an installation's orchestrator later isn't supported; [changing it later](./index.md#changing-it-later) has the steps for when there's no other way.

## Upgrading

Change the image in `kustomization.yaml` and apply. The new orchestrators start the old version's workflows again, and syncs carry on from their checkpoints. Each old runner finishes its jobs at a checkpoint before it stops, and they're placed again. Job pods run the launcher's image, so a pod made before the upgrade finishes on the old one.
