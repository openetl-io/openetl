---
title: Install with Docker Compose
description: Run OpenETL on one machine with Docker Compose, with its API server and console, an orchestrator, runners and Postgres.
---

[`deploy/compose/compose.yml`](../../deploy/compose/compose.yml) runs OpenETL on one machine with Docker Compose. It starts:

| Service | What it runs |
|---|---|
| `postgres` | Postgres 18, for OpenETL's own records and, on the default orchestrator, its workflows |
| `server` | The API and the console, on `127.0.0.1:8080` |
| `orchestrator` | An orchestrator, which runs workflows and places jobs on runners |
| `runner-token` | Runs and exits on each start; on the first, it makes the runners' token |
| `runner` | A runner, which runs syncs and streams |

Runners keep checkpoints and staging in the volume `runner`, so the stack needs no object storage. Your sources and destinations aren't part of it: pipelines reach them from the runner's container.

## Before you start

- **Docker with Compose** 2.24 or later. For an installation other people use, a Linux machine; Docker Desktop or OrbStack on a Mac will do for trying it.
- **Room for jobs.** A runner places jobs up to the machine's CPUs and memory, less the 0.5 vCPU and 512 MiB it keeps for itself. Each job has a size, from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB), which a pipeline's `spec.resources` sets or OpenETL picks.
- **OpenETL's image.** No image has been released, so build it from a clone of this repository, with [openetl-connectors](https://github.com/openetl-io/openetl-connectors) cloned next to it, and Go, pnpm and Docker installed:

  ```sh
  make engine-linux image
  ```

  `make engine-linux` builds `openetl-engine` for Linux in a Rust container, and `make image` builds the image `openetl:dev` from it, `openetld`, the CLI, the console and the official connectors. The Rust container keeps its build in the Docker volumes `openetl-engine-target` and `openetl-cargo-registry`, about 1 GB; remove them with `docker volume rm` when you're done. To build the image on one machine and run it on another, copy it across with `docker save openetl:dev | ssh HOST docker load`, or push it to a registry with `make image IMAGE=REGISTRY/openetl:TAG` and set `OPENETL_IMAGE` to the same name in `.env`.

## Install

Run these in `deploy/compose`, or in a directory of its own that holds a copy of `compose.yml`.

1. Write `.env`, beside `compose.yml`, with a password for Postgres and the key that encrypts the secrets pipelines use:

   ```sh
   umask 077
   cat > .env <<EOF
   POSTGRES_PASSWORD=$(openssl rand -hex 16)
   OPENETL_SECRETS_KEY=base64key://$(openssl rand -base64 32 | tr '+/' '-_')
   EOF
   ```

   Keep a copy of `.env` somewhere safe. Without `OPENETL_SECRETS_KEY`, the secrets OpenETL holds can't be read.

2. Start the stack, and check that it's up:

   ```sh
   docker compose up -d
   docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
   ```

   ```text
   SERVICE        STATUS
   orchestrator   Up Less than a second
   postgres       Up 5 seconds (healthy)
   runner         Up Less than a second
   runner-token   Exited (0) Less than a second ago
   server         Up 3 seconds (healthy)
   ```

   On its first start, the server creates the installation's org and writes an owner's API key to `.openetl/admin.key` in the volume `server`. `runner-token` uses it to make the runners' token, and exits.

3. Make yourself a user who can sign in to the console, as an owner:

   ```sh
   docker compose exec server openetl principal create --user --email YOU@EXAMPLE.COM --password-link YOUR-NAME
   docker compose exec server openetl principal grant YOUR-NAME owner
   ```

   ```text
   created principals/YOUR-NAME
   a link for principals/YOUR-NAME to set their password, which works once until 2026-11-09 10:05:57; send it to them yourself:
   http://localhost:8080/set-password#…
   granted principals/YOUR-NAME owner on the org
   ```

   The link is cut short here. Open it, set your password, and sign in at `http://localhost:8080`.

`docker compose exec server openetl` runs the CLI inside the server's container, where it uses the owner key the server wrote. To use the CLI from your own computer, give it the API's URL and that key:

```sh
export OPENETL_API_URL=http://localhost:8080
export OPENETL_API_KEY="$(docker compose exec -T server cat .openetl/admin.key)"
```

## Check that it works

Write a pipeline of generated rows into Parquet files on the runners' volume to `faker-to-files.yaml`:

```yaml
apiVersion: openetl.io/v1alpha1
kind: Source
metadata:
  name: faker
spec:
  connector: faker
  config:
    seed: 42
    count: 1000
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: files
spec:
  connector: parquet
  config:
    url: file:///var/lib/openetl/files
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-files
spec:
  schedule: { every: 1h }
  source: { ref: faker }
  destination: { ref: files }
```

Apply it, which starts its first sync, and look at the sync a few seconds later:

```sh
docker compose exec -T server openetl apply -f /dev/stdin < faker-to-files.yaml
docker compose exec server openetl syncs faker-to-files
```

```text
created workspaces/default/sources/faker (version 91f9debd5abe)
created workspaces/default/destinations/files (version 36f53c4cd306)
created workspaces/default/pipelines/faker-to-files (version 710045ab31ca)
SYNC                                  STATE      STARTED              TOOK  ROWS  VERSION                                                           LAST COMMIT          LAG  ERROR
01a1ba9f-aa38-7a01-b467-c09838110cbe  succeeded  2026-11-08 10:06:28  0s    2000  710045ab31caeecaba8027eccdd9caa37c5f6f67ecc70057f2e472d0470e6710  2026-11-08 10:06:28  -
```

The files are in the volume `runner`, under `files/`, one folder for each stream: `docker compose exec runner ls files` lists them. The pipeline syncs every hour until you pause it with `docker compose exec server openetl pause faker-to-files`.

## Settings

Every `openetld` in the stack reads `.env`, so any setting goes there: the [settings every installation sets](./index.md#the-settings-every-installation-sets), the rest of [`openetld`'s settings](../reference/openetld.md), and the `OTEL_*` variables for [monitoring](./index.md#monitoring). After changing `.env`, run `docker compose up -d` again, which restarts the services with it.

`compose.yml` reads these from `.env` itself:

| Variable | Default | What it sets |
|---|---|---|
| `POSTGRES_PASSWORD` | required | The password of the Postgres user `openetl` |
| `OPENETL_SECRETS_KEY` | required | The key that encrypts secrets: `base64key://`, or a KMS key (`awskms://`, `gcpkms://`, `azurekeyvault://`) |
| `OPENETL_IMAGE` | `openetl:dev` | The image every `openetld` runs |
| `OPENETL_PORT` | `127.0.0.1:8080` | Where the machine publishes the API and the console, as `ADDRESS:PORT` |
| `OPENETL_PUBLIC_URL` | `http://localhost:8080` | The URL people open the console at, which password links start with |

And it sets these, which `.env` can't change: the server's `OPENETL_DATABASE_URL`, `OPENETL_LISTEN` (`:8080`) and `OPENETL_WEB`; the orchestrator's `OPENETL_DATABASE_URL` and `OPENETL_EXECUTOR_ID`; and the runners' `OPENETL_API_URL` and `OPENETL_RUNNER_TOKEN_FILE`.

Logs are JSON, one line each: `docker compose logs -f runner` follows the runners'.

## Serve it to other people

The console and the API are published on the machine's loopback address only. To let other people reach them, put a reverse proxy that terminates TLS in front of `127.0.0.1:8080`, and set the URL they'll use in `.env`:

```sh
OPENETL_PUBLIC_URL=https://openetl.example.com
```

An `https` URL also makes the console's session cookie `Secure`. To publish the port on every address instead, on a network you trust, set `OPENETL_PORT=8080`.

## Sources and destinations

Connectors run in the runner's container, where `localhost` is the container itself. Give sources and destinations addresses the container can reach: a database on the same machine by the machine's own address, or `host.docker.internal` on Docker Desktop and OrbStack. The stack's Postgres holds OpenETL's own records; keep your data elsewhere.

## Checkpoints in a bucket

Runners keep checkpoints, which say where each pipeline resumes from, and staging in `.openetl/checkpoints` in the volume `runner`. To keep them in S3, GCS or Azure Blob Storage instead, set `OPENETL_CHECKPOINTS` in `.env`, such as `s3://my-bucket?region=eu-west-1`, with the bucket's credentials (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for S3), before the first sync. Runners don't move checkpoints from one store to another.

## More runners

```sh
docker compose up -d --scale runner=2
```

Runners share the volume `runner`, with its checkpoints, staging and token. Each places jobs up to the whole machine, so to share one machine between runners, give each its part with `OPENETL_CAPACITY` in `.env`, as `CPU,MEMORY`, such as `4,8Gi`.

For API keys and runner tokens, follow [Manage access and credentials](./access.md). For telemetry and delivery alerts, see [Monitor an installation](./monitoring.md).

## Choose an orchestrator

The stack runs workflows on DBOS, in Postgres. To run them on a Temporal cluster you already have, add these to `.env` before the first start:

```sh
OPENETL_ORCHESTRATOR=temporal
OPENETL_TEMPORAL_ADDRESS=temporal.example.com:7233
OPENETL_TEMPORAL_NAMESPACE=openetl
```

Runners never connect to Temporal. Changing the orchestrator of an installation that's running isn't supported; [changing it later](./index.md#changing-it-later) has the steps for when there's no other way. In this stack, stopping the control plane is `docker compose stop server orchestrator`, and starting it again with the new `.env` is `docker compose up -d`. Going back to DBOS after Temporal clears its old workflows first, with `docker compose exec postgres dropdb -U openetl openetl_dbos`.

## Upgrade

Build or pull the new image, set `OPENETL_IMAGE` in `.env` if its name changed, and run `docker compose up -d`, which replaces each container whose image changed. The new orchestrator starts the old version's workflows again, and syncs carry on from their checkpoints. Each runner finishes its jobs at a checkpoint before it stops, taking up to 2 minutes for a sync and 10 for a stream, and they're placed again; `compose.yml` gives runners 11 minutes to stop.

A version can write checkpoints in a newer format. A runner older than that refuses to run such a pipeline, and the sync fails with `CONFIG_INVALID`, rather than lose what the newer format holds. If you go back to an older version, run the affected pipelines on the newer one again.

## Back up

[Back up and restore](./backup-and-restore.md) has the stopped-backup procedure and recovery into fresh volumes. It covers both the metadata and DBOS databases, checkpoint and staging files, the encryption key and credentials. Test a restore before relying on a backup.

## Stop and remove

`docker compose stop` stops the stack, and `docker compose down` removes its containers too. Both keep the volumes, and `docker compose up -d` starts it again where it was.

> [!CAUTION]
> `docker compose down -v` also deletes the volumes: the database, the owner key and the checkpoints.
