---
title: Self-host
description: What an OpenETL installation is made of, what it needs, and the settings every installation sets.
---

This section explains how to install, operate and upgrade OpenETL for a team. It assumes experience with Linux, containers, Postgres and object storage. The [Kubernetes guide](./kubernetes.md) also requires experience with Kubernetes.

To try OpenETL on your computer, follow the [quickstart](../get-started/quickstart.md).

## The parts

The `openetld` program runs each component through a separate role:

| Role | What it does | What it needs |
|---|---|---|
| `openetld server` | Serves the API, and the console when `--web` names a built UI. Holds no state but the database, so you can run several. | Postgres, the key that encrypts secrets |
| `openetld orchestrator` | Runs the control plane's workflows: schedules syncs, retries them, opens incidents, and places each job on a runner. Run two or more; when one stops, another takes over its work. | Postgres, or Temporal (see [orchestrators](#choose-an-orchestrator)) |
| `openetld runner` | Takes jobs from the API and runs them: the connectors as programs of their own, with `openetl-engine` between them. Runners make up a data plane, and serve a pool within it. | The API's URL, a runner token, object storage for checkpoints, and the engine and connectors next to `openetld` |
| `openetld runner --launcher kubernetes` | A runner that makes a pod of its own for each job placed on it, sized for that job, instead of running it itself. | As a runner, and a service account that may create pods |
| `openetld dev` | All of the above in one process, without sign-in, for one computer. Not for an installation others use. | Postgres |

Runners communicate with the control plane through the API. They can run in the same cluster or on another network near your databases. Each runner needs access to the API and the data systems used by its pipelines.

## What you need

- **Postgres** stores pipelines, versions, syncs, encrypted secrets, users and audit records. The default DBOS backend stores workflows in a second database on the same server. Its name is the metadata database name with `_dbos` appended. Orchestrators create it if the database user has permission.
- **Object storage** for checkpoints and staging: an S3 bucket (or one on an S3-compatible store), a GCS bucket or an Azure Blob Storage container. A local directory works only if every runner, and every Spark or Flink cluster, sees it at the same path. A container on the same machine sees the directory only if it is mounted in the container.
- **`openetld`, `openetl-engine` and the connectors.** A runner searches for the engine beside `openetld`, then on `PATH`. It searches for local connectors in `connectors/` beside `openetld`. The container image includes these components, the CLI and the console. There are no released images or binaries. Build from source using [CONTRIBUTING.md](../../CONTRIBUTING.md) or the [Compose image instructions](./compose.md#before-you-start).

## The settings every installation sets

Each `openetld` flag has an environment variable equivalent. `openetld ROLE -h` lists the settings and defaults. Configure these settings for your installation:

| Variable | Set on | What it is |
|---|---|---|
| `OPENETL_DATABASE_URL` | Servers and orchestrators | The metadata database. Don't connect through a transaction pooler. |
| `OPENETL_SECRETS_KEY` | Servers and orchestrators | The key that encrypts secrets: `base64key://KEY`, a file holding one, or a KMS key (`awskms://…`, `gcpkms://…`, `azurekeyvault://…`). Keep a copy: without it, stored secrets can't be read. |
| `OPENETL_PUBLIC_URL` | Servers | The URL people open the console at. Password links start with it, and an `https://` URL makes the session cookie secure. |
| `OPENETL_API_URL` | Runners | The API's URL, as runners reach it. |
| `OPENETL_RUNNER_TOKEN` | Runners | The token runners authenticate with, which `openetl dataplane token create` makes. `OPENETL_RUNNER_TOKEN_FILE` names a file holding it instead. |
| `OPENETL_CHECKPOINTS` | Runners | Where checkpoints and staging go, such as `s3://my-bucket`. Required: a runner does not start without it. A directory works only when every runner is on one machine, and the Kubernetes launcher needs a bucket. [Buckets](#buckets) tells how runners reach the bucket. |
| `OPENETL_ORCHESTRATOR` | Servers and orchestrators | `dbos`, the default, or `temporal`. |

OpenETL permits outbound HTTP requests to public addresses and the network ranges listed in `OPENETL_EGRESS_ALLOW`. This restriction covers webhooks, registry downloads and connector requests made through OpenETL's HTTP service.

## Buckets

A runner, and the engine that it starts, use the first S3 credentials that they find:

1. `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`. For temporary keys, also set `AWS_SESSION_TOKEN`.
2. The role that AWS gives the process: an EKS IAM role for a service account, EKS Pod Identity, an ECS task role or an EC2 instance profile.

The runner also reads AWS profiles (`AWS_PROFILE`). The engine does not read profiles. If a runner uses a profile, also give it keys or a role.

When a runner starts, it asks S3 for the region of the bucket. To change how runners reach the bucket, add these query parameters to its URL:

| Parameter | What it does |
|---|---|
| `region=REGION` | Names the region of the bucket, so the runner does not ask S3. Use it if runners cannot reach the public S3 endpoints. |
| `endpoint=URL` | Points at an S3-compatible store instead of AWS, with path-style addressing. Without `region`, the region is `AWS_REGION`, or `us-east-1` if `AWS_REGION` is not set. |
| `anonymous=true` | Sends unsigned requests, for a store that accepts them, such as a development store. |

For example: `s3://openetl?endpoint=http://seaweedfs:8333&anonymous=true`.

GCS buckets use the Google application default credentials. Azure containers use `AZURE_STORAGE_KEY` if it is set. Otherwise, they use the Azure default credential chain.

## Connectors are trusted code

A connector runs as a separate program under the runner's user account, without a sandbox. It has the runner's file and network access, including access to cloud metadata addresses. `OPENETL_EGRESS_ALLOW` does not restrict sockets opened directly by a connector.

A connector can access the secrets for the sources and destinations it runs. OpenETL does not restrict where the connector sends those values. The descriptor's `egress` field declares intended destinations but is not enforced.

Install only connectors you trust. Give runners only the credentials and network access their pipelines need.

## Install and upgrade connectors

External connector packages have their own release cadence. Use the versions your pipelines need, whether maintained by OpenETL, the community or your organization. The source-build image described here includes official connector executables as a development convenience; those executables are not the set of systems the engine can connect.

[Choose and manage connectors](../pipelines/connectors.md) covers applying packages, private registry credentials and controlled upgrades. A runner downloads the applied package's executable for its operating system and architecture. Keep the engine's storage and network requirements separate from each connector's requirements for reaching your data systems.

## Sign-in and the first owner

The server requires password sign-in unless started with `--auth none`. `openetld dev` uses this option. With sign-in off, requests without an API key have owner permissions.

In this mode, the server rejects requests from other websites. It accepts host names matching `localhost`, a loopback address or `--public-url`. `--auth-none-allow-remote` permits other host names.

On first start, the server creates the installation's organization and writes an owner API key. The default file is `.openetl/admin.key`; `OPENETL_BOOTSTRAP_KEY_FILE` overrides it. Use that key to create a password link for an existing user:

```sh
export OPENETL_API_URL=https://openetl.example.com OPENETL_API_KEY="$(cat admin.key)"
openetl principal password-link NAME
```

The command prints a link for the user to set their password. `openetl principal` creates users and service accounts and grants roles. `openetl key` manages API keys. If all owner keys are lost, `openetld bootstrap-key` creates a replacement. See [Manage access and credentials](./access.md) for the full procedure.

## Choose an orchestrator

Orchestrators run workflows on one of two backends, chosen when you install:

- **DBOS**, the default, keeps workflows in Postgres, so an installation needs nothing else.
- **Temporal** runs them on a Temporal cluster you already have: set `OPENETL_ORCHESTRATOR=temporal`, `OPENETL_TEMPORAL_ADDRESS` and `OPENETL_TEMPORAL_NAMESPACE`.

Choose before the first start, and give every API server and orchestrator the same settings.

### Changing it later

OpenETL does not support changing the orchestration backend while the installation is running. There is no automatic migration. If you must change it, stop the control plane:

1. Pause every pipeline with `openetl pause NAME`. Wait until `openetl syncs NAME` shows no active runs.
2. Stop every API server and orchestrator.
3. Change `OPENETL_ORCHESTRATOR`, and Temporal's address and namespace, on all of them.
4. Start the API servers and orchestrators. Resume the pipelines with `openetl resume NAME`.

At startup, the control plane rebuilds schedules and job monitors from the metadata database. It does not copy workflow state from the old backend.

Before returning to a previously used backend, remove its old workflow state. For DBOS, drop the installation's `_dbos` database while the control plane is stopped. For Temporal, use a new namespace. Otherwise, old workflows can restart with outdated pipeline definitions.

## Monitoring

`openetld` exports metrics and traces through OpenTelemetry. Configure an exporter or OTLP endpoint with the standard `OTEL_*` variables to enable export. `OTEL_METRICS_EXPORTER=prometheus` serves metrics for Prometheus to scrape. The Kubernetes manifests expose this endpoint on port 9464.

By default, logs use JSON on standard error. `OPENETL_LOG_FORMAT` and `OPENETL_LOG_OUTPUT` control their format and destination.

[Monitor an installation](./monitoring.md) explains the metrics and how to choose alerts. [Troubleshoot a pipeline](../pipelines/troubleshooting.md) connects symptoms to checks and recovery.

## Operating the installation

- [Manage access and credentials](./access.md): users, scoped service accounts, key and runner-token rotation, and offboarding.
- [Back up and restore](./backup-and-restore.md): the complete state inventory, a stopped Compose backup, and verification after recovery.

## Install

- [With Docker Compose](./compose.md), on one machine: an API server with the console, an orchestrator, runners and Postgres.
- [On Kubernetes](./kubernetes.md): manifests for API servers, orchestrators, shared runners and a pod launcher. The guide includes KEDA runner scaling and Karpenter node scaling.

These docs have no guide for other platforms. There, run the same roles as processes or containers, with the settings above.
