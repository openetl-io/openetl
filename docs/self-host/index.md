---
title: Self-host
description: What an OpenETL installation is made of, what it needs, and the settings every installation sets.
---

This section is for the people who install OpenETL, keep it running and upgrade it for everyone else. It assumes you know Linux, containers, Postgres and object storage; the [Kubernetes guide](./kubernetes.md) assumes Kubernetes too.

To try OpenETL on one computer instead, the [quickstart](../get-started/quickstart.md) is quicker.

## The parts

Everything is one program, `openetld`, started in a role:

| Role | What it does | What it needs |
|---|---|---|
| `openetld server` | Serves the API, and the console when `--web` names a built UI. Holds no state but the database, so you can run several. | Postgres, the key that encrypts secrets |
| `openetld orchestrator` | Runs the control plane's workflows: schedules syncs, retries them, opens incidents, and places each job on a runner. Run two or more; when one stops, another takes over its work. | Postgres, or Temporal (see [orchestrators](#choose-an-orchestrator)) |
| `openetld runner` | Takes jobs from the API and runs them: the connectors as programs of their own, with `openetl-engine` between them. Runners make up a data plane, and serve a pool within it. | The API's URL, a runner token, object storage for checkpoints, and the engine and connectors next to `openetld` |
| `openetld runner --launcher kubernetes` | A runner that makes a pod of its own for each job placed on it, sized for that job, instead of running it itself. | As a runner, and a service account that may create pods |
| `openetld dev` | All of the above in one process, without sign-in, for one computer. Not for an installation others use. | Postgres |

Runners reach the control plane only through the API, so they can run anywhere that can reach it and your data: in the same cluster, or near your databases on another network.

## What you need

- **Postgres** for the metadata database: pipelines, versions, syncs, secrets (encrypted), users and audit. On the default orchestrator, the orchestrators also keep their workflows in a second database on the same server, named after the first with `_dbos`, which they create if the user may.
- **Object storage** for checkpoints and staging: an S3 bucket (or one on an S3-compatible store), a GCS bucket or an Azure Blob Storage container. A local directory works when everything runs on one machine.
- **`openetld`, `openetl-engine` and the connectors.** A runner looks for the engine next to `openetld`, then on its `PATH`, and for connectors in `connectors/` next to `openetld`. `openetld`'s image holds all three, with the CLI and the console. No image or binary has been released, so build them from source: [CONTRIBUTING.md](../../CONTRIBUTING.md) has how, and the [Compose guide](./compose.md#before-you-start) how to build the image.

## The settings every installation sets

Every flag of `openetld` can also be set by an environment variable; `openetld ROLE -h` lists them all, with their defaults. These are the ones an installation can't do without:

| Variable | Set on | What it is |
|---|---|---|
| `OPENETL_DATABASE_URL` | Servers and orchestrators | The metadata database. Don't connect through a transaction pooler. |
| `OPENETL_SECRETS_KEY` | Servers and orchestrators | The key that encrypts secrets: `base64key://KEY`, a file holding one, or a KMS key (`awskms://…`, `gcpkms://…`, `azurekeyvault://…`). Keep a copy: without it, stored secrets can't be read. |
| `OPENETL_PUBLIC_URL` | Servers | The URL people open the console at. Password links start with it, and an `https://` URL makes the session cookie secure. |
| `OPENETL_API_URL` | Runners | The API's URL, as runners reach it. |
| `OPENETL_RUNNER_TOKEN` | Runners | The token runners authenticate with, which `openetl dataplane token create` makes. `OPENETL_RUNNER_TOKEN_FILE` names a file holding it instead. |
| `OPENETL_CHECKPOINTS` | Runners | Where checkpoints and staging go, such as `s3://my-bucket?region=eu-west-1`. Required: a runner won't start without it. A directory works only when every runner is on one machine, and the Kubernetes launcher needs a bucket. |
| `OPENETL_ORCHESTRATOR` | Servers and orchestrators | `dbos`, the default, or `temporal`. |

The HTTP requests OpenETL makes itself, for webhooks, for downloads from registries, and for connectors that make their requests through it, reach only public addresses, plus the ranges `OPENETL_EGRESS_ALLOW` lists, such as your own networks.

## Connectors are trusted code

A connector runs as a program of its own, as the runner's user, with no sandbox. It can read what the runner can read and reach what the runner can reach, including a cloud's metadata address, whatever `OPENETL_EGRESS_ALLOW` says: that setting covers requests OpenETL makes for it, not the sockets it opens itself. It can also use the secrets of the sources and destinations it runs for, and OpenETL doesn't yet check where it sends them. Its descriptor's `egress` states what it means to reach, and isn't enforced.

So install only connectors you trust, as you would any other program on those machines. Give runners credentials for what their pipelines need and no more, and keep them off networks their pipelines don't use.

## Install and upgrade connectors

External connector packages have their own release cadence. Use the versions your pipelines need, whether maintained by OpenETL, the community or your organization. The source-build image described here includes official connector executables as a development convenience; those executables are not the set of systems the engine can connect.

[Choose and manage connectors](../pipelines/connectors.md) covers applying packages, private registry credentials and controlled upgrades. A runner downloads the applied package's executable for its operating system and architecture. Keep the engine's storage and network requirements separate from each connector's requirements for reaching your data systems.

## Sign-in and the first owner

A server asks people to sign in with a password unless it's started with `--auth none`, which `openetld dev` does. With sign-in off, a call with no API key acts as an owner, so the server refuses calls from other sites' web pages, and answers only to `localhost`, a loopback address or its `--public-url`. `--auth-none-allow-remote` lifts the second limit, for a server reached by other names.

On its first start, the server creates the installation's org and writes an owner's API key to `.openetl/admin.key`, or to the file `OPENETL_BOOTSTRAP_KEY_FILE` names. With that key:

```sh
export OPENETL_API_URL=https://openetl.example.com OPENETL_API_KEY="$(cat admin.key)"
openetl principal password-link NAME
```

gives a person a link to set their password. `openetl principal` creates people and service accounts and grants them roles, and `openetl key` manages API keys. If every owner key is lost, `openetld bootstrap-key` writes a new one.

## Choose an orchestrator

Orchestrators run workflows on one of two backends, chosen when you install:

- **DBOS**, the default, keeps workflows in Postgres, so an installation needs nothing else.
- **Temporal** runs them on a Temporal cluster you already have: set `OPENETL_ORCHESTRATOR=temporal`, `OPENETL_TEMPORAL_ADDRESS` and `OPENETL_TEMPORAL_NAMESPACE`.

Choose before the first start, and give every API server and orchestrator the same settings.

### Changing it later

Changing the orchestrator of an installation that's running isn't supported, and nothing in OpenETL does it for you. If there's no other way, change it with the control plane stopped:

1. Pause every pipeline with `openetl pause NAME`, and wait until `openetl syncs NAME` shows none running.
2. Stop every API server and orchestrator.
3. Change `OPENETL_ORCHESTRATOR`, and Temporal's address and namespace, on all of them.
4. Start them again, and resume the pipelines with `openetl resume NAME`.

When they start, they rebuild schedules, and what watches each running job, from the database, so nothing carries over from the old backend and nothing needs migrating. To go back to a backend the installation used before, clear what it kept first: drop the `_dbos` database, or use a new Temporal namespace. Otherwise its old workflows start again with an old view of the pipelines.

## Monitoring

`openetld` exports metrics and traces with OpenTelemetry, configured by the standard `OTEL_*` variables: nothing is exported until an exporter or an OTLP endpoint is set. `OTEL_METRICS_EXPORTER=prometheus` serves metrics for Prometheus to scrape instead, which the Kubernetes manifests do on port 9464. Logs are JSON on standard error by default (`OPENETL_LOG_FORMAT`, `OPENETL_LOG_OUTPUT`).

[Monitor an installation](./monitoring.md) explains the metrics and how to choose alerts. [Troubleshoot a pipeline](../pipelines/troubleshooting.md) connects symptoms to checks and recovery.

## Operating the installation

- [Manage access and credentials](./access.md): users, scoped service accounts, key and runner-token rotation, and offboarding.
- [Manage connection secrets](./secrets.md): stored credentials, cloud-secret references, replacement and deletion.
- [Back up and restore](./backup-and-restore.md): the complete state inventory, a stopped Compose backup, and verification after recovery.

## Install

- [With Docker Compose](./compose.md), on one machine: an API server with the console, an orchestrator, runners and Postgres.
- [On Kubernetes](./kubernetes.md), with plain manifests: API servers, orchestrators, a pool of runners and a pod launcher, with recipes for scaling runners with KEDA and nodes with Karpenter.

These docs have no guide for other platforms. There, run the same roles as processes or containers, with the settings above.
