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
     - name: ghcr.io/openetl-io/openetl
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

To expose the API outside the cluster, configure an Ingress or load balancer for the `openetl-server` Service. Set `OPENETL_PUBLIC_URL` in your ConfigMap patch to the public console URL, then apply again.

Install the connectors that your pipelines need with `openetl connector apply`. `openetl connector catalog` lists the official ones, with the command that installs each. [Choose and manage connectors](../pipelines/connectors.md) covers versions and private registries.

Every component serves Prometheus metrics on port 9464, and its pods carry the `prometheus.io/scrape` and `prometheus.io/port` annotations.

## Credentials for connectors

By default, a connector does not get the role that IRSA or Pod Identity gives the pod. Give a source or destination its own role with `aws.role`, which the runner assumes for its connector. Set the roles that runners may assume in `OPENETL_AWS_ASSUME_ROLES`, in your ConfigMap patch. [Run on AWS](./aws.md#credentials-for-connectors) explains the setup, the other ways, and what OpenETL can and cannot block.

[Connectors are trusted code](./index.md#connectors-are-trusted-code) lists what a connector gets from the runner.

## How jobs are placed

Each job has a size, from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB), which the pipeline's `spec.resources` sets or openetl picks. The orchestrators place it in its pool (`spec.resources.pool`, `default` if not set):

- **On a shared runner** with room. A runner's room is its limits less `OPENETL_RESERVE` (0.5 vCPU and 512 MiB), so in `runner-default.yaml` it's 3.5 vCPUs and 7.5 GiB. Keep its requests equal to its limits.
- **In a separate pod** created by the pool's launcher for jobs at or above `OPENETL_POD_MIN_SIZE` (`m` here). This also applies when no runner in the pool can fit the job. Pod requests and limits include the job's size plus the reserve. `OPENETL_MAX_PODS` (20) and `OPENETL_MAX_POD_CPU` (64) limit total pod capacity. Jobs wait when these limits are reached. A pod counts until it stops, after its job has ended too: [Room for every pod](#room-for-every-pod) explains this.

Jobs wait until capacity is available. Before stopping, a runner finishes jobs at a checkpoint. It allows up to 2 minutes for a sync and 10 minutes for a stream. Its termination grace period covers that time. Jobs are then placed again.

A runner has 30 seconds to start a job placed on it, and a launcher has 30 seconds to take one from the API. If a runner or launcher does not, such as when its node fails, the orchestrators place the job again. They place no job on a runner or launcher whose last report is more than 30 seconds old. After a launcher takes a job, the job has 5 minutes to start in its pod, including the start-up of a new node. If it does not start in time, the orchestrators place it again.

A pod's spec comes from the PodTemplate `openetl-attempt-default`, to which the launcher adds the job, labels, resources and node constraints. To change it, add a patch for the PodTemplate to your kustomization. The launcher reads the template when it starts, so restart it after you apply the change (`kubectl -n openetl rollout restart deploy/openetl-launcher-default`).

### Jobs on several pods

A pipeline with `spec.resources.workers` above 1 runs each job on that many workers and a coordinator, each in a pod of its own. [Run a job on several pods](../pipelines/workers.md) covers this for the pipeline's author. The orchestrators send such a job only to a pod launcher of the pipeline's pool, whatever the job's size: `OPENETL_POD_MIN_SIZE` does not apply. The launcher makes all the job's pods, or none of them.

A job runs on one pod instead in these cases:

- The pipeline's plan says that the job runs on one pod, such as for a Spark or Flink transform or for a destination that takes fewer writer lanes than workers. [Pipeline](../reference/specs/pipeline.md#workers) lists these cases.
- The job's coordinator finds one of these cases before the job starts, where the control plane could not check it. The job is then placed again on one pod.
- No launcher of the pool reports, while other runners of the data plane do. The job then needs a runner of its own pool, like any job on one pod, and waits until one can take it. A runner of another pool does not run it.

To check which case applies, run `openetl plan -f PIPELINE.yaml`. A warning that no pod launcher of the pool has reported means that the job runs on one pod. A note that no runner of the pool has reported means that the job also has nowhere to run until one does.

#### What the launcher makes

For each attempt, the launcher makes a pod for each worker, then the coordinator's pod, without waiting between them. Each worker's pod asks the API every second, for up to 10 minutes, where its coordinator is.

| Pod | Name | Requests and limits | What the launcher adds |
|---|---|---|---|
| The coordinator | `openetl-attempt-HASH-PLACEMENT` | `xs` plus the reserve | `OPENETL_POD_IP`, from the pod's IP; `OPENETL_ATTEMPT_LISTEN` set to `:7720`; `OPENETL_ATTEMPT_ADVERTISE` set to `$(OPENETL_POD_IP)`; the container port `attempt`, 7720/TCP |
| Worker `N`, from 1 | `openetl-attempt-HASH-PLACEMENT-wN` | The job's size plus the reserve | `OPENETL_WORKER` set to `N`; the annotation `openetl.io/worker: "N"`; `OPENETL_POD_IP`, from the pod's IP; `OPENETL_EXCHANGE_LISTEN` set to `:7720`; `OPENETL_EXCHANGE_ADVERTISE` set to `$(OPENETL_POD_IP)`; the container port `exchange`, 7720/TCP |

`HASH` comes from the attempt's ID, and `PLACEMENT` counts the attempt's placements. Every pod also has what any job's pod has:

- the labels `app.kubernetes.io/managed-by: openetl`, `app.kubernetes.io/component: attempt`, `openetl.io/data-plane`, `openetl.io/pool`, `openetl.io/job` (`batch` or `stream`) and `openetl.io/pipeline`, the pipeline's ID;
- the annotations `openetl.io/attempt` and `openetl.io/placement`;
- requests equal to its limits, and `OPENETL_POD_SCRATCH` of ephemeral storage, 10 GiB unless you set it;
- `OPENETL_SCRATCH` set to half of `OPENETL_POD_SCRATCH`, the disk its engine may use, or to `0` if `OPENETL_POD_SCRATCH` is `0` ([Scratch disk](./index.md#scratch-disk)).

No label tells a coordinator's pod from a worker's pod. The reserve is the launcher's `OPENETL_RESERVE`, 0.5 vCPU and 512 MiB unless you set it. If you set `OPENETL_POD_MAX_SIZE`, it must allow both the job's size and `xs`.

If the cluster refuses one of the pods, the launcher deletes the pods that it made for the attempt. If the cluster refuses for lack of room, such as a full ResourceQuota, the launcher tries again for about two minutes. Then the orchestrators place the attempt again. If the pod is invalid or the launcher may not make it, the attempt fails with the cluster's error. When the attempt ends, the launcher deletes all its pods.

The launcher's Role in `launcher-default.yaml` needs nothing more for workers: `create`, `list` and `delete` on pods, and `get` on PodTemplates. Job pods do not call the Kubernetes API.

Each pod needs what any job's pod needs: the API, the bucket, the connector registry, and your source and destination. This includes the coordinator. Before the workers start, the coordinator starts the source and destination connectors with their credentials. It sets them up and discovers the source's streams, then stops them. Each worker then runs its own connectors and its own engine.

#### The route between pods

Each worker connects to its coordinator's pod IP on TCP port 7720. No Service is involved. The connection uses TLS 1.3 with a certificate that the coordinator makes for the attempt, and the workers trust only that certificate. Each call carries a token that the coordinator makes for the attempt.

The workers get the address, the certificate and the token from the API. The coordinator gives them to the API after it claims the pipeline. While the attempt runs, the API gives them to a caller with the data plane's runner token that asks for that attempt's workers. The control plane's database keeps them in the attempt's queue entry, which it deletes a day after the attempt ends.

Each end sends a heartbeat every 2 seconds. If either end hears nothing from the other for 10 seconds, the attempt fails, and OpenETL retries it from the last checkpoint. Every worker must join within 5 minutes after the coordinator starts the attempt.

When a job's rows go through an [exchange](../pipelines/workers.md#the-exchange-between-workers), each worker's engine also connects to every other worker's pod IP on TCP port 7720. Each worker gives its address when it joins, and the coordinator sends every worker the others' addresses. These connections use TLS 1.3 with a certificate, a key and a secret that the coordinator makes for each try of the attempt and sends to the workers. Each engine trusts only that certificate, and refuses a connection that does not prove the secret. A pod is either a coordinator or a worker, so each pod listens on port 7720 for one of these purposes. If an engine cannot reach another within a minute, or a connection between two of them breaks, the attempt fails, and OpenETL retries it.

#### Allow the route in a NetworkPolicy

If NetworkPolicies in your namespace deny traffic by default, allow the route between job pods. This policy allows job pods to connect to each other on TCP port 7720, and nothing else:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: openetl-attempt-workers
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/managed-by: openetl
      app.kubernetes.io/component: attempt
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/managed-by: openetl
              app.kubernetes.io/component: attempt
      ports:
        - protocol: TCP
          port: 7720
  egress:
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/managed-by: openetl
              app.kubernetes.io/component: attempt
      ports:
        - protocol: TCP
          port: 7720
```

Coordinators listen on port 7720 for their workers, and workers for each other's engines. No label tells them apart, so the policy opens the port on every job pod, both ways. Each attempt's certificates, token and secret keep other pods out of it. To allow the route only within one pool, add `openetl.io/pool: POOL` to the three selectors, and make one such policy for each pool.

Before applying this policy, check that your other policies permit these routes:

- Job pods to DNS, the API, object storage, connector registries and data systems.
- Prometheus to the job pods' metrics endpoint.

> [!CAUTION]
> This policy blocks traffic that no policy allows. Keep the required service routes open before you apply it.

1. Save the policy as `network-policy.yaml` in your kustomization's directory.
2. Add the file to `resources` in your `kustomization.yaml`:

   ```yaml
   resources:
     - https://github.com/openetl-io/openetl//deploy/kubernetes?ref=v{{version}}
     - network-policy.yaml
   ```

3. Apply the change with `kubectl apply -k .`.
4. Check that the policy is there with `kubectl -n openetl get networkpolicy openetl-attempt-workers`.
5. Sync a pipeline whose plan shows `workers:`. The sync's log says where the workers join, and the sync succeeds.

To undo the change, remove `network-policy.yaml` from `resources`, and delete the policy:

```sh
kubectl -n openetl delete networkpolicy openetl-attempt-workers
```

#### Room for every pod

`OPENETL_MAX_PODS` (default 20) and `OPENETL_MAX_POD_CPU` (default 64 vCPUs) count every pod of a job: its workers and its coordinator. A job waits until all of them fit. A job that needs more than the limits allow never fits, even on an empty launcher. For example, 20 workers need 21 pods. Such a job fails after it has waited 2 minutes, and its message names the limit.

The limits count a job's pods until they stop, not until the job ends. When a job ends, or the orchestrators place it again, its pods keep their room until the launcher reports them stopped or gone. The launcher reports every 10 seconds, and then deletes the pods of jobs that have ended. If the launcher deletes a pod that still runs, the pod keeps its room until it stops: up to 2 minutes and 30 seconds for a sync, and 10 minutes and 30 seconds for a stream. So the next job can wait about 10 seconds after a job ends, and longer while a pod stops. If the launcher cannot list its pods, the room stays held until it can.

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

A ResourceQuota in the namespace needs room for every pod of the job at once, with the reserve and the scratch. For example, four workers of size `m` and their coordinator request 5 pods, 11 vCPUs, 19.5 GiB of memory and 50 GiB of ephemeral storage. The launcher's limits count only the sizes: 8.5 vCPUs for the same job.

#### Logs of workers

The sync's log, which `openetl logs` and the console show, has only the coordinator's lines. Each worker writes its log where its runner's logs go, which is the pod's log unless you set `OPENETL_LOG_OUTPUT`. List the job pods, and read a worker's log by its pod's name:

```sh
kubectl -n openetl get pods -l app.kubernetes.io/component=attempt
kubectl -n openetl logs POD
```

The launcher deletes the pods when the attempt ends, and their logs with them. To keep workers' logs, collect pod logs with your cluster's log collector. A worker's lines about its part of the attempt carry the `pipeline` and `sync` IDs, and `worker`, the worker's number. Workers are numbered from 1, as in their pods' names: `worker=2` is the pod whose name ends in `-w2`.

#### The process launcher

`OPENETL_LAUNCHER=process`, for development and tests, starts a process for each worker and one for the coordinator, on the launcher's own machine. Each coordinator listens on 127.0.0.1 at a free port, which only that machine reaches. Leave `OPENETL_ATTEMPT_LISTEN` unset on such a launcher, or give it port 0: its processes take its settings, so every coordinator would listen at the same address. With any other port, `openetld` does not start. The processes also take the launcher's log settings, so by default their logs go to the launcher's standard error. As a pod does, a process that the launcher stops counts against `OPENETL_MAX_PODS` and `OPENETL_MAX_POD_CPU` until it has exited.

### Spot nodes

Sync pods tolerate GKE and AKS spot taints and can run on spot nodes. On an interruption notice, they stop at a checkpoint and are placed again.

A worker's pod of a [job on several pods](#jobs-on-several-pods) gets the same tolerations and preferences, but does not stop at a checkpoint. An interruption fails its attempt, and OpenETL retries it from the last checkpoint.

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

Each release has a version, such as `{{version}}`. Its notes on the [releases page](https://github.com/openetl-io/openetl/releases) list what changed. In the alpha, the pipeline format and the API can change between releases.

1. Read the notes of each release between your version and the new one.
2. Back up the metadata database and keep a copy of `OPENETL_SECRETS_KEY`. [Back up and restore](./backup-and-restore.md) lists what an installation's state is.
3. In your `kustomization.yaml`, change the version in `ref=vVERSION` in every URL, and `newTag`, to the new version.
4. Check the change with `kubectl kustomize .`, then apply it with `kubectl apply -k .`.
5. Upgrade the CLI to the same version, as [Install the CLI](../get-started/install.md#upgrade) shows.

New orchestrators resume existing workflows, and syncs resume from their checkpoints. Old runners checkpoint their jobs before stopping, then those jobs are placed again. Existing job pods finish with the image they started with.

A newer version can write a checkpoint format that older runners cannot read. An older runner refuses that pipeline with `CONFIG_INVALID`. If this occurs after a downgrade, run the affected pipelines with the newer version.

## Uninstall

> [!WARNING]
> Deleting the namespace removes the encryption key. Without a copy of `OPENETL_SECRETS_KEY`, stored secrets cannot be read.

In your kustomization's directory, delete the manifests, then the namespace with its Secrets and any job pods that remain:

```sh
kubectl delete -k .
kubectl delete namespace openetl
```

If you applied the Karpenter node pools, delete them with `kubectl delete -f` and the same URL. The metadata database, the DBOS database and the bucket are outside the cluster. Delete them separately when you no longer need their data.
