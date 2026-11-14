---
title: Install on Kubernetes
description: Run OpenETL's API servers, orchestrators, runners and pod launcher on a Kubernetes cluster, and scale them with KEDA and Karpenter.
---

The manifests in [`deploy/kubernetes`](../../deploy/kubernetes) install OpenETL on Kubernetes through `kubectl apply -k`. You use them from the release's tag, as the base of a kustomization of your own. The manifests provide these components:

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
- **Network access to `ghcr.io`** from the cluster's nodes and runners. Nodes pull OpenETL's image, `ghcr.io/openetl-io/openetl:{{version}}`, for `linux/amd64` or `linux/arm64`. Runners download the connector packages that you install. The image holds no external connectors.
- **kubectl** 1.27 or later, and **git**, which kubectl uses to read the manifests from GitHub.
- **The `openetl` CLI** on your computer, as [Install the CLI](../get-started/install.md) shows.

## Install

1. Make a directory for your installation's kustomization. In it, write `kustomization.yaml`. Replace `BUCKET` with your bucket's URL, such as `s3://my-bucket`:

   ```yaml
   apiVersion: kustomize.config.k8s.io/v1beta1
   kind: Kustomization
   namespace: openetl
   resources:
     - https://github.com/openetl-io/openetl//deploy/kubernetes?ref=v{{version}}
   images:
     - name: openetl
       newName: ghcr.io/openetl-io/openetl
       newTag: {{version}}
   patches:
     - patch: |
         apiVersion: v1
         kind: ConfigMap
         metadata:
           name: openetl
         data:
           OPENETL_CHECKPOINTS: BUCKET
   ```

   The resource is the release's manifests, at the tag `v{{version}}`. `images` sets the same release's image: without it, the manifests run `edge`, the image built from `main`. The patch changes the ConfigMap `openetl`, which every component reads. Add your other settings to its `data`, such as `OPENETL_PUBLIC_URL`. [Buckets](./index.md#buckets) gives the order in which runners look for credentials.

   To check what you are about to apply, run `kubectl kustomize .` in the directory. It prints the manifests and changes nothing.

2. Give runners access to the bucket. On EKS, runners, job pods and the launcher can use the bucket through an IAM role for a service account or through Pod Identity, without keys. Give the role to two service accounts in the `openetl` namespace:

   - `default`, which runners, job pods, the server and the orchestrators use.
   - `openetl-launcher`, which the launcher uses. The launcher checks the bucket when it starts, and does not start without access.

   [Run on AWS](./aws.md#on-eks) shows the commands for both. The service account `openetl-launcher` exists after step 4. If you annotate service accounts for IRSA, do it after that step. By default, connectors do not get this role. [Credentials for connectors](#credentials-for-connectors) explains what to do.

3. Create the namespace, and the Secret the server and orchestrators read:

   ```sh
   kubectl create namespace openetl
   kubectl -n openetl create secret generic openetl \
     --from-literal=OPENETL_DATABASE_URL='postgres://openetl:PASSWORD@HOST:5432/openetl' \
     --from-literal=OPENETL_SECRETS_KEY="base64key://$(openssl rand -base64 32 | tr '+/' '-_')"
   ```

   `OPENETL_SECRETS_KEY` encrypts pipeline secrets. Keep a backup of this key; stored secrets cannot be decrypted without it. A KMS key is also supported (`awskms://`, `gcpkms://`, `azurekeyvault://`).

4. In your kustomization's directory, apply the manifests and wait for the server:

   ```sh
   kubectl apply -k .
   kubectl -n openetl rollout status deploy/openetl-server
   ```

   Runners and the launcher wait for their Secret, made in step 6.

5. Create an owner key. This command writes and prints a new key in the server pod selected by `kubectl`:

   ```sh
   kubectl -n openetl exec deploy/openetl-server -- sh -c 'openetld bootstrap-key 2>/dev/null && cat .openetl/admin.key'
   ```

6. Replace `OWNER_KEY` with the key from step 5. Create a runner token and the `openetl-runner` Secret. If the pods have no workload identity, also add bucket credentials to this Secret:

   ```sh
   kubectl -n openetl port-forward svc/openetl-server 8080 &
   export OPENETL_API_URL=http://localhost:8080 OPENETL_API_KEY="OWNER_KEY"
   openetl dataplane token create --out runner.token default
   kubectl -n openetl create secret generic openetl-runner \
     --from-literal=OPENETL_RUNNER_TOKEN="$(cat runner.token)"
   ```

7. List the official connectors. On a new installation, the catalog can be empty for up to 16 minutes. This command lists the official registry now:

   ```sh
   openetl connector catalog --refresh
   ```

   The output lists each official connector and the command that installs it.

To expose the API outside the cluster, configure an Ingress or load balancer for the `openetl-server` Service. Set `OPENETL_PUBLIC_URL` in your ConfigMap patch to the public console URL, then apply again.

Install the connectors that your pipelines need with `openetl connector apply`. `openetl connector catalog` lists the official ones, with the command that installs each. [Choose and manage connectors](../pipelines/connectors.md) covers versions and private registries.

Every component serves Prometheus metrics on port 9464, and its pods carry the `prometheus.io/scrape` and `prometheus.io/port` annotations.

## Credentials for connectors

By default, a connector does not get the role that IRSA or Pod Identity gives the pod. Give a source or destination its own role with `aws.role`, which the runner assumes for its connector. Set the roles that runners may assume in `OPENETL_AWS_ASSUME_ROLES`, in your ConfigMap patch. [Run on AWS](./aws.md#credentials-for-connectors) explains the setup, the other ways, and what OpenETL can and cannot block.

[Connectors are trusted code](./index.md#connectors-are-trusted-code) lists what a connector gets from the runner.

## How jobs are placed

Each job has a size, from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB), which the pipeline's `spec.resources` sets or openetl picks. The orchestrators place it in its pool (`spec.resources.pool`, `default` if not set):

- **On a shared runner** with room. A runner's room is its limits less `OPENETL_RESERVE` (0.5 vCPU and 512 MiB), so in `runner-default.yaml` it's 3.5 vCPUs and 7.5 GiB. Keep its requests equal to its limits.
- **In a separate pod** created by the pool's launcher for jobs at or above `OPENETL_POD_MIN_SIZE` (`m` here). This also applies when no runner in the pool can fit the job. Pod requests and limits include the job's size plus the reserve. `OPENETL_MAX_PODS` (20) and `OPENETL_MAX_POD_CPU` (64) limit total pod capacity. Jobs wait when these limits are reached.

Jobs wait until capacity is available. Before stopping, a runner finishes jobs at a checkpoint. It allows up to 2 minutes for a sync and 10 minutes for a stream. Its termination grace period covers that time. Jobs are then placed again.

A pod's spec comes from the PodTemplate `openetl-attempt-default`, to which the launcher adds the job, labels, resources and node constraints. To change it, add a patch for the PodTemplate to your kustomization. The launcher reads the template when it starts, so restart it after you apply the change (`kubectl -n openetl rollout restart deploy/openetl-launcher-default`).

### Jobs on several pods

A pipeline with `spec.resources.workers` above 1 can run each job on that many workers and a coordinator. The pool's launcher makes all the job's pods, or none of them:

- one pod for each worker, of the job's size plus the reserve;
- one pod for the coordinator, of size `xs` plus the reserve.

A job runs on one pod instead in these cases:

- The pipeline's plan says that the job runs on one pod, such as for a transform or for a stream that holds columns. [Pipeline](../reference/specs/pipeline.md#workers) lists these cases.
- The job's coordinator finds one of these cases before the job starts, where the control plane could not check it. The job is then placed again on one pod.
- No launcher of the pool reports, while other runners of the pool do.

The workers connect to the coordinator's pod IP on TCP port 7720, over TLS with a certificate that the coordinator makes for each job. If your namespace has NetworkPolicies, allow traffic between the pods that have the label `app.kubernetes.io/component: attempt` on that port. The launcher deletes a job's workers' pods with the coordinator's pod.

#### Room for every pod

`OPENETL_MAX_PODS` (default 20) and `OPENETL_MAX_POD_CPU` (default 64 vCPUs) count every pod of a job: its workers and its coordinator. A job waits until all of them fit. A job that needs more than the limits allow never fits, even on an empty launcher. For example, 20 workers need 21 pods. Such a job fails after it has waited 2 minutes, and its message names the limit.

To let larger jobs run, raise the limits:

1. Add the limits to the `data` of the ConfigMap patch in your `kustomization.yaml`. Set `OPENETL_MAX_PODS` to at least the workers plus one. Set `OPENETL_MAX_POD_CPU` to at least the workers times the job's vCPUs, plus 0.5 vCPU for the coordinator:

   ```yaml
   data:
     OPENETL_CHECKPOINTS: BUCKET
     OPENETL_MAX_PODS: "40"
     OPENETL_MAX_POD_CPU: "128"
   ```

2. Apply the change, and restart the launcher, which reads its settings when it starts:

   ```sh
   kubectl apply -k .
   kubectl -n openetl rollout restart deploy/openetl-launcher-default
   ```

3. Check the pipeline with `openetl plan -f PIPELINE.yaml`. The plan shows `workers:` after the job size. It no longer reports that the job's workers and their coordinator need more pods than any pod launcher of the pool takes.

To undo the change, remove the two keys from the patch, and run the commands of step 2 again.

### Spot nodes

Sync pods tolerate GKE and AKS spot taints and can run on spot nodes. On an interruption notice, they stop at a checkpoint and are placed again.

Stream pods also tolerate spot taints but prefer non-spot nodes. They carry `karpenter.sh/do-not-disrupt` to prevent consolidation. An interrupted stream checkpoints within the notice period when possible. Otherwise, it resumes from its last checkpoint. Recovery preserves the stream's delivery guarantee.

To prohibit stream pods on spot nodes, set `OPENETL_POD_STREAM_SPOT=avoid` on the launcher. These pods cannot start if only spot nodes are available.

Karpenter requests a non-spot node before relaxing the default preference. The Cluster Autoscaler does not consider that preference when selecting a node group. With the Cluster Autoscaler, use `avoid` when streams must never run on spot.

To add a pool, download [`runner-default.yaml`](../../deploy/kubernetes/runner-default.yaml) and [`launcher-default.yaml`](../../deploy/kubernetes/launcher-default.yaml) from the release's tag into your kustomization's directory. Change their names, the pool's label and `OPENETL_POOL`, and add both files to `resources`. Point pipelines at the pool with `spec.resources.pool`. A pool with only a launcher runs every job in pods.

## Scaling

### Runners, with KEDA

The `keda` component scales shared runners in the `default` pool using orchestrator placement metrics. It compares running and pending job demand with each runner's available CPU and memory. Jobs assigned to separate pods are excluded. This requires [KEDA](https://keda.sh) 2.x and Prometheus scraping the annotated pods.

1. Add the component, and a patch that sets `serverAddress` to your Prometheus, to your `kustomization.yaml`:

   ```yaml
   components:
     - https://github.com/openetl-io/openetl//deploy/kubernetes/keda?ref=v{{version}}
   patches:
     - target:
         kind: ScaledObject
         name: openetl-runner-default
       patch: |
         - op: replace
           path: /spec/triggers/0/metadata/serverAddress
           value: http://prometheus.monitoring.svc:9090
         - op: replace
           path: /spec/triggers/1/metadata/serverAddress
           value: http://prometheus.monitoring.svc:9090
   ```

   Put this patch in the same `patches` list as the ConfigMap's. If you change the runners' size, also replace both triggers' `threshold` (`/spec/triggers/0/metadata/threshold` and `/spec/triggers/1/metadata/threshold`) to match their room.
2. Apply with `kubectl apply -k .`.

The queries, for scaling with something else:

```promql
(sum(max by (size) (openetl_placement_cpu_reserved{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
+ (sum(max by (size) (openetl_placement_cpu_pending{data_plane="default", pool="default", runs_on="runner"})) or vector(0))
```

and the same over `openetl_placement_memory_reserved_bytes` and `openetl_placement_memory_pending_bytes`. Every orchestrator exports the same values, so `max by (size)` counts them once. `runs_on="pod"` is what the launcher's pods have or wait for, and `openetl_placement_pending_age_seconds` how long the oldest job has waited. A job on workers counts under `runs_on="pod"` and its workers' size, with its workers and its coordinator together.

KEDA keeps at least one shared runner. Without one, a pool with a launcher sends every job to a separate pod. Scale-down starts after ten minutes of lower demand and removes one runner per minute. A removed runner can have active jobs; they move after a checkpoint.

### Nodes, with Karpenter

`karpenter/nodepools.yaml` has two Karpenter node pools for AWS, which expect an `EC2NodeClass` named `default`:

- `openetl-attempts`, for job pods, on spot or on-demand instances. Consolidation leaves stream pods alone, and sync pods on spot finish at a checkpoint on an interruption notice and are placed again.
- `openetl-runners`, for shared runners, on-demand only, removed only when empty.

Apply the node pools:

```sh
kubectl apply -f https://raw.githubusercontent.com/openetl-io/openetl/v{{version}}/deploy/kubernetes/karpenter/nodepools.yaml
```

Then add `https://github.com/openetl-io/openetl//deploy/kubernetes/karpenter?ref=v{{version}}` to `components` in your `kustomization.yaml`, and apply. This gives runners and job pods the node selector and toleration for their pool.

A runner pool used only for scheduled syncs can also use spot instances. If the cluster can add only spot instances, allow spot in `openetl-runners`. Streams on those runners have the same interruption behavior as [stream pods on spot](#spot-nodes).

With the Cluster Autoscaler instead, make node groups with the same `openetl.io/nodes` labels and taints.

### The control plane

API servers and orchestrators hold no state but the database, so a plain HorizontalPodAutoscaler on CPU scales them. On DBOS, an orchestrator that stops has its workflows taken over by another within a minute.

## Temporal

To run workflows on Temporal instead of DBOS, set `OPENETL_ORCHESTRATOR: temporal`, `OPENETL_TEMPORAL_ADDRESS` and `OPENETL_TEMPORAL_NAMESPACE` in your ConfigMap patch before the first install. OpenETL does not support a change of an installation's orchestrator later. [Changing it later](./index.md#changing-it-later) gives the steps for when there is no other way.

## Upgrading

Each release has a version, such as `0.1.0-alpha`. Its notes on the [releases page](https://github.com/openetl-io/openetl/releases) list what changed. In the alpha, the pipeline format and the API can change between releases.

1. Read the notes of each release between your version and the new one.
2. Back up the metadata database and keep a copy of `OPENETL_SECRETS_KEY`. [Back up and restore](./backup-and-restore.md) lists what an installation's state is.
3. In your `kustomization.yaml`, change the version in `ref=vVERSION` in every URL, and `newTag`, to the new version.
4. Check the change with `kubectl kustomize .`, then apply it with `kubectl apply -k .`.
5. Upgrade the CLI to the same version, as [Install the CLI](../get-started/install.md#upgrade) shows.

New orchestrators resume existing workflows, and syncs resume from their checkpoints. Old runners checkpoint their jobs before stopping, then those jobs are placed again. Existing job pods finish with the image they started with.

A newer version can write a checkpoint format that older runners cannot read. An older runner refuses that pipeline with `CONFIG_INVALID`. If this occurs after a downgrade, run the affected pipelines with the newer version.

## Uninstall

> [!WARNING]
> Deleting the namespace deletes the Secret `openetl`, with `OPENETL_SECRETS_KEY`. Without a copy of that key, nobody can read the secrets that the metadata database holds.

In your kustomization's directory, delete the manifests, then the namespace with its Secrets and any job pods that remain:

```sh
kubectl delete -k .
kubectl delete namespace openetl
```

If you applied the Karpenter node pools, delete them with `kubectl delete -f` and the same URL. The metadata database, the DBOS database and the bucket are outside the cluster. Delete them separately when you no longer need their data.
