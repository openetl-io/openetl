---
title: Install on Kubernetes
description: Run OpenETL's API servers, orchestrators, runners and pod launcher on a Kubernetes cluster, and scale them with KEDA and Karpenter.
---

The manifests in [`deploy/kubernetes`](../../deploy/kubernetes) install OpenETL on Kubernetes through `kubectl apply -k`. Run these commands from the repository root. The manifests provide these components:

| File | What it runs |
|---|---|
| [`config.yaml`](../../deploy/kubernetes/config.yaml) | The settings every component shares (ConfigMap `openetl`) |
| [`server.yaml`](../../deploy/kubernetes/server.yaml) | The API server: two replicas, Service `openetl-server` on port 8080 |
| [`orchestrator.yaml`](../../deploy/kubernetes/orchestrator.yaml) | Two orchestrators, which run workflows and place jobs on runners |
| [`runner-default.yaml`](../../deploy/kubernetes/runner-default.yaml) | The `default` pool's shared runners |
| [`launcher-default.yaml`](../../deploy/kubernetes/launcher-default.yaml) | The `default` pool's pod launcher, which runs large jobs in pods of their own, and the PodTemplate they're made from |
| [`keda/`](../../deploy/kubernetes/keda) | A component that scales the runners with KEDA |
| [`karpenter/`](../../deploy/kubernetes/karpenter) | Karpenter node pools, and a component that sends runners and job pods to them |

The manifests do not include Postgres or object storage. Configure those services before installation.

## Before you start

- **Postgres** for metadata. The default DBOS backend stores workflows in a second database, named after the metadata database with `_dbos` appended. Orchestrators create it if their database user has permission. Otherwise, create it yourself or specify another database with `OPENETL_DBOS_DATABASE_URL`. Do not connect through a transaction pooler.
- **A bucket** in S3, GCS or Azure Blob Storage for checkpoints and staging. Runners, job pods and the pod launcher need read and write access to it, through a workload identity or keys in the Secret `openetl-runner`.
- **OpenETL's image** in a registry reachable by the cluster. There are no released images. Clone [openetl-connectors](https://github.com/openetl-io/openetl-connectors) beside this repository. Build with `make engine-linux image IMAGE=REGISTRY/openetl:TAG`, then push the image to your registry. Set that image under `images` in `kustomization.yaml`. The [Compose guide](./compose.md#before-you-start) explains the build prerequisites. On Linux, `cargo build -p openetl-engine` can replace `make engine-linux`.
- **kubectl** 1.27 or later.

## Install

1. Set `OPENETL_CHECKPOINTS` in `config.yaml` to your bucket, such as `s3://my-bucket`. [Buckets](./index.md#buckets) gives the order in which runners look for credentials.

   On EKS, runners, job pods and the launcher can use the bucket through an IAM role for a service account or through Pod Identity, without keys. Give the role to two service accounts in the `openetl` namespace:

   - `default`, which runners, job pods, the server and the orchestrators use.
   - `openetl-launcher`, which the launcher uses. The launcher checks the bucket when it starts, and does not start without access.

   This role is not available to connectors. [Credentials for connectors](#credentials-for-connectors) explains what to do.

2. Create the namespace, and the Secret the server and orchestrators read:

   ```sh
   kubectl create namespace openetl
   kubectl -n openetl create secret generic openetl \
     --from-literal=OPENETL_DATABASE_URL='postgres://openetl:PASSWORD@HOST:5432/openetl' \
     --from-literal=OPENETL_SECRETS_KEY="base64key://$(openssl rand -base64 32 | tr '+/' '-_')"
   ```

   `OPENETL_SECRETS_KEY` encrypts pipeline secrets. Keep a backup of this key; stored secrets cannot be decrypted without it. A KMS key is also supported (`awskms://`, `gcpkms://`, `azurekeyvault://`).

3. Apply the manifests and wait for the server:

   ```sh
   kubectl apply -k deploy/kubernetes
   kubectl -n openetl rollout status deploy/openetl-server
   ```

   Runners and the launcher wait for their Secret, made in step 5.

4. Create an owner key. This command writes and prints a new key in the server pod selected by `kubectl`:

   ```sh
   kubectl -n openetl exec deploy/openetl-server -- sh -c 'openetld bootstrap-key 2>/dev/null && cat .openetl/admin.key'
   ```

5. Replace `OWNER_KEY` with the key from step 4. Create a runner token and the `openetl-runner` Secret. If the pods have no workload identity, also add bucket credentials to this Secret:

   ```sh
   kubectl -n openetl port-forward svc/openetl-server 8080 &
   export OPENETL_API_URL=http://localhost:8080 OPENETL_API_KEY="OWNER_KEY"
   openetl dataplane token create --out runner.token default
   kubectl -n openetl create secret generic openetl-runner \
     --from-literal=OPENETL_RUNNER_TOKEN="$(cat runner.token)"
   ```

To expose the API outside the cluster, configure an Ingress or load balancer for the `openetl-server` Service. Set `OPENETL_PUBLIC_URL` in `config.yaml` to the public console URL.

Every component serves Prometheus metrics on port 9464, and its pods carry the `prometheus.io/scrape` and `prometheus.io/port` annotations.

## Credentials for connectors

A runner does not pass its environment variables to the connectors it starts, except a few such as `PATH` and `HOME`. So a connector does not get the IAM role that an IAM role for a service account or Pod Identity gives the pod. On EKS, a connector that calls AWS needs an access key in its own config. Store the secret access key with `openetl secret set`, and refer to it with `{ secret: NAME }`. Each connector's documentation names its key fields.

[Connectors are trusted code](./index.md#connectors-are-trusted-code) lists what a connector gets from the runner.

## How jobs are placed

Each job has a size, from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB), which the pipeline's `spec.resources` sets or openetl picks. The orchestrators place it in its pool (`spec.resources.pool`, `default` if not set):

- **On a shared runner** with room. A runner's room is its limits less `OPENETL_RESERVE` (0.5 vCPU and 512 MiB), so in `runner-default.yaml` it's 3.5 vCPUs and 7.5 GiB. Keep its requests equal to its limits.
- **In a separate pod** created by the pool's launcher for jobs at or above `OPENETL_POD_MIN_SIZE` (`m` here). This also applies when no runner in the pool can fit the job. Pod requests and limits include the job's size plus the reserve. `OPENETL_MAX_PODS` (20) and `OPENETL_MAX_POD_CPU` (64) limit total pod capacity. Jobs wait when these limits are reached.

Jobs wait until capacity is available. Before stopping, a runner finishes jobs at a checkpoint. It allows up to 2 minutes for a sync and 10 minutes for a stream. Its termination grace period covers that time. Jobs are then placed again.

A pod's spec comes from the PodTemplate `openetl-attempt-default`, to which the launcher adds the job, labels, resources and node constraints. The launcher reads the template when it starts, so restart it after changing the template (`kubectl -n openetl rollout restart deploy/openetl-launcher-default`).

### Spot nodes

Sync pods tolerate GKE and AKS spot taints and can run on spot nodes. On an interruption notice, they stop at a checkpoint and are placed again.

Stream pods also tolerate spot taints but prefer non-spot nodes. They carry `karpenter.sh/do-not-disrupt` to prevent consolidation. An interrupted stream checkpoints within the notice period when possible. Otherwise, it resumes from its last checkpoint. Recovery preserves the stream's delivery guarantee.

To prohibit stream pods on spot nodes, set `OPENETL_POD_STREAM_SPOT=avoid` on the launcher. These pods cannot start if only spot nodes are available.

Karpenter requests a non-spot node before relaxing the default preference. The Cluster Autoscaler does not consider that preference when selecting a node group. With the Cluster Autoscaler, use `avoid` when streams must never run on spot.

To add a pool, copy `runner-default.yaml` and `launcher-default.yaml` with another name and `OPENETL_POOL`, and point pipelines at it with `spec.resources.pool`. A pool with only a launcher runs every job in pods.

## Scaling

### Runners, with KEDA

The `keda` component scales shared runners in the `default` pool using orchestrator placement metrics. It compares running and pending job demand with each runner's available CPU and memory. Jobs assigned to separate pods are excluded. This requires [KEDA](https://keda.sh) 2.x and Prometheus scraping the annotated pods.

1. In `keda/runner-default.yaml`, set `serverAddress` to your Prometheus. If you change the runners' size, change both `threshold`s to match their room.
2. Uncomment `components` in `kustomization.yaml`, with `keda` only, and apply.

The queries, for scaling with something else:

```promql
(sum(max by (size) (openetl_placement_cpu_reserved{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
+ (sum(max by (size) (openetl_placement_cpu_pending{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
```

and the same over `openetl_placement_memory_reserved_bytes` and `openetl_placement_memory_pending_bytes`. Every orchestrator exports the same values, so `max by (size)` counts them once. `runs_on="pod"` is what the launcher's pods have or wait for, and `openetl_placement_pending_age_seconds` how long the oldest job has waited.

KEDA keeps at least one shared runner. Without one, a pool with a launcher sends every job to a separate pod. Scale-down starts after ten minutes of lower demand and removes one runner per minute. A removed runner can have active jobs; they move after a checkpoint.

### Nodes, with Karpenter

`karpenter/nodepools.yaml` has two Karpenter node pools for AWS, which expect an `EC2NodeClass` named `default`:

- `openetl-attempts`, for job pods, on spot or on-demand instances. Consolidation leaves stream pods alone, and sync pods on spot finish at a checkpoint on an interruption notice and are placed again.
- `openetl-runners`, for shared runners, on-demand only, removed only when empty.

Apply the node pools with `kubectl apply -f deploy/kubernetes/karpenter/nodepools.yaml`. Add `karpenter` to `components` in `kustomization.yaml`. This gives runners and job pods the node selector and toleration for their pool.

A runner pool used only for scheduled syncs can also use spot instances. If the cluster can add only spot instances, allow spot in `openetl-runners`. Streams on those runners have the same interruption behavior as [stream pods on spot](#spot-nodes).

With the Cluster Autoscaler instead, make node groups with the same `openetl.io/nodes` labels and taints.

### The control plane

API servers and orchestrators hold no state but the database, so a plain HorizontalPodAutoscaler on CPU scales them. On DBOS, an orchestrator that stops has its workflows taken over by another within a minute.

## Temporal

To run workflows on Temporal instead of DBOS, set `OPENETL_ORCHESTRATOR: temporal`, `OPENETL_TEMPORAL_ADDRESS` and `OPENETL_TEMPORAL_NAMESPACE` in `config.yaml` before the first install. Changing an installation's orchestrator later isn't supported; [changing it later](./index.md#changing-it-later) has the steps for when there's no other way.

## Upgrading

Change the image in `kustomization.yaml`. Apply the manifests. New orchestrators resume existing workflows, and syncs resume from their checkpoints. Old runners checkpoint their jobs before stopping, then those jobs are placed again. Existing job pods finish with the image they started with.

A newer version can write a checkpoint format that older runners cannot read. An older runner refuses that pipeline with `CONFIG_INVALID`. If this occurs after a downgrade, run the affected pipelines with the newer version.
