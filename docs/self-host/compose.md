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

Runners store checkpoints and staging in the `runner` volume. The default stack does not need external object storage. Source and destination systems are separate from this stack. Their addresses must be reachable from the runner container.

## Before you start

- **Docker with Compose** 2.24 or later. Use a Linux machine for a shared installation. Docker Desktop or OrbStack on macOS is suitable for evaluation.
- **Job capacity.** A runner reserves 0.5 vCPU and 512 MiB for itself. The remaining machine capacity is available to jobs. Sizes range from `xs` (0.5 vCPU, 1 GiB) to `xl` (8 vCPUs, 16 GiB). The pipeline's `spec.resources` selects the size, or OpenETL selects it automatically.
- **Network access to `ghcr.io`.** Docker pulls OpenETL's image, `ghcr.io/openetl-io/openetl:0.1.0-alpha`, for `linux/amd64` or `linux/arm64`. The image holds `openetld`, the `openetl` CLI, `openetl-engine` and the console. It holds no external connectors: runners download each connector package that you install.
- **The `openetl` CLI** on the computer you work from, if it is not this machine. [Install the CLI](../get-started/install.md) shows how.

## Install

1. Make a directory for the installation. Download the release's Compose file into it:

   ```sh
   mkdir openetl && cd openetl
   curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/v0.1.0-alpha/deploy/compose/compose.yml
   ```

   Run all the commands on this page in this directory.

2. Write `.env`, beside `compose.yml`. It names the image, a password for Postgres and the key that encrypts the secrets pipelines use:

   ```sh
   (umask 077 && cat > .env) <<EOF
   OPENETL_IMAGE=ghcr.io/openetl-io/openetl:0.1.0-alpha
   POSTGRES_PASSWORD=$(openssl rand -hex 16)
   OPENETL_SECRETS_KEY=base64key://$(openssl rand -base64 32 | tr '+/' '-_')
   EOF
   ```

   The parentheses keep `umask 077` to this command, so only you can read `.env`. Keep a copy of `.env` in a safe place. Without `OPENETL_SECRETS_KEY`, OpenETL cannot read the secrets that it holds.

3. Start the stack. Check the service states:

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

   On first start, the server creates the organization and writes an owner API key to `.openetl/admin.key` in the `server` volume. The `runner-token` service uses this key to create a runner token, then exits successfully. Its `Exited (0)` state is expected.

   If `docker compose up -d` fails with `dependency failed to start`, an earlier installation on this machine can be the cause. [An earlier installation on this machine](#an-earlier-installation-on-this-machine) shows how to continue it, run beside it, or delete it.

4. Make yourself a user who can sign in to the console, as an owner:

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

   This example truncates the link. Open the complete link from your command output. Set your password. Sign in at `http://localhost:8080`.

`docker compose exec server openetl` runs the CLI inside the server's container, where it uses the owner key the server wrote. To use the CLI from your own computer, give it the API's URL and that key:

```sh
export OPENETL_API_URL=http://localhost:8080
export OPENETL_API_KEY="$(docker compose exec -T server cat .openetl/admin.key)"
```

## An earlier installation on this machine

Compose names the volumes after the project, `openetl`: `openetl_postgres`, `openetl_server` and `openetl_runner`. The project name comes from `compose.yml`, not from the directory. So a stack in a new directory uses the volumes of an earlier installation on the same machine, such as the [quickstart](../get-started/quickstart.md)'s.

The `postgres` volume keeps the earlier installation's password. If the new `.env` has another `POSTGRES_PASSWORD`, the server cannot connect to Postgres, and `docker compose up -d` fails:

```text
dependency failed to start: container openetl-server-1 is unhealthy
```

`docker compose logs server` then shows `password authentication failed for user "openetl"`. To list the volumes of an earlier installation, run:

```sh
docker volume ls --filter name=openetl_
```

Then do one of the procedures that follow.

### Continue the earlier installation

1. Copy the earlier installation's `.env` into this directory, in place of the new one.
2. Run `docker compose up -d`.

The earlier `POSTGRES_PASSWORD` opens the database, and the earlier `OPENETL_SECRETS_KEY` reads the stored secrets. The earlier pipelines, secrets and checkpoints stay. If you do not have the earlier `.env`, OpenETL cannot read the stored secrets. Then use one of the other procedures.

### Run a second installation beside it

Give the new installation a project name of its own. Compose then makes new volumes for it, and keeps the earlier ones.

1. Add this line to the new `.env`. Replace `openetl-2` with a name of your choice:

   ```sh
   COMPOSE_PROJECT_NAME=openetl-2
   ```

2. If the earlier installation runs at the same time, also give the new one another port:

   ```sh
   OPENETL_PORT=127.0.0.1:8081
   OPENETL_PUBLIC_URL=http://localhost:8081
   ```

3. Run `docker compose up -d`.

The new installation's volumes are `openetl-2_postgres`, `openetl-2_server` and `openetl-2_runner`, and its network is `openetl-2_default`. If you changed the port, set `OPENETL_API_URL=http://localhost:8081` for the CLI.

### Delete the earlier installation

> [!WARNING]
> These commands delete the earlier installation's database, owner key, checkpoints and staging files. You cannot recover them, except from a backup.

In this directory, remove the earlier installation's containers and volumes. Then start the new stack:

```sh
docker compose down -v
docker compose up -d
```

`docker compose down -v` acts on the project `openetl`. It removes that project's containers and volumes, also if they came from another directory. It does not change an installation that has another project name.

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

Apply the file to start its first sync. After a few seconds, check the sync:

```sh
docker compose exec -T server openetl apply -f /dev/stdin < faker-to-files.yaml
docker compose exec server openetl syncs faker-to-files
```

```text
created workspaces/default/sources/faker (version 91f9debd5abe)
created workspaces/default/destinations/files (version 36f53c4cd306)
created workspaces/default/pipelines/faker-to-files (version 710045ab31ca)
checked source faker, destination files
SYNC                                  STATE      STARTED              TOOK  ROWS  VERSION                                                           LAST COMMIT          LAG  ERROR
01a1ba9f-aa38-7a01-b467-c09838110cbe  succeeded  2026-11-08 10:06:28  0s    2000  710045ab31caeecaba8027eccdd9caa37c5f6f67ecc70057f2e472d0470e6710  2026-11-08 10:06:28  -
```

The files are in the volume `runner`, under `files/`, one folder for each stream: `docker compose exec runner ls files` lists them. The pipeline syncs every hour until you pause it with `docker compose exec server openetl pause faker-to-files`.

## Settings

Every `openetld` service reads `.env`. Use it for [installation settings](./index.md#the-settings-every-installation-sets), other [`openetld` settings](../reference/openetld.md) and the `OTEL_*` variables for [monitoring](./index.md#monitoring). After editing `.env`, run `docker compose up -d` to recreate affected services with the new settings.

`compose.yml` reads these from `.env` itself:

| Variable | Default | What it sets |
|---|---|---|
| `POSTGRES_PASSWORD` | required | The password of the Postgres user `openetl` |
| `OPENETL_SECRETS_KEY` | required | The key that encrypts secrets: `base64key://`, or a KMS key (`awskms://`, `gcpkms://`, `azurekeyvault://`) |
| `OPENETL_IMAGE` | `ghcr.io/openetl-io/openetl:0.1.0-alpha` | The image every `openetld` runs |
| `OPENETL_PORT` | `127.0.0.1:8080` | Where the machine publishes the API and the console, as `ADDRESS:PORT` |
| `OPENETL_PUBLIC_URL` | `http://localhost:8080` | The URL people open the console at, which password links start with |

The Compose file explicitly sets these values, overriding `.env`:

| Service | Fixed settings |
|---|---|
| Server | `OPENETL_DATABASE_URL`, `OPENETL_LISTEN` (`:8080`), `OPENETL_WEB` |
| Orchestrator | `OPENETL_DATABASE_URL`, `OPENETL_EXECUTOR_ID` |
| Runner | `OPENETL_API_URL`, `OPENETL_RUNNER_TOKEN_FILE` |

Logs are JSON, one line each: `docker compose logs -f runner` follows the runners'.

## Serve it to other people

The console and the API are published on the machine's loopback address only. To let other people reach them, put a reverse proxy that terminates TLS in front of `127.0.0.1:8080`, and set the URL they'll use in `.env`:

```sh
OPENETL_PUBLIC_URL=https://openetl.example.com
```

An `https` URL also makes the console's session cookie `Secure`. To publish the port on every address instead, on a network you trust, set `OPENETL_PORT=8080`.

## Sources and destinations

The image has the built-in connectors only. Install the other connectors that your pipelines need, such as the official Postgres connector:

```sh
docker compose exec server openetl connector catalog
docker compose exec server openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha
```

`catalog` lists the official connectors, with the command that installs each one. The runner downloads a connector's executable from its registry the first time that a job needs it. The runner container must have network access to that registry. [Choose and manage connectors](../pipelines/connectors.md) covers versions, private registries and upgrades.

Connectors run inside the runner container. There, `localhost` refers to that container. Use addresses reachable from the container. For a database on the host, use the host's address or `host.docker.internal` on Docker Desktop and OrbStack. Keep pipeline data separate from the stack's metadata database.

## Checkpoints in a bucket

Runners keep checkpoints and staging in `.openetl/checkpoints` on the `runner` volume. To use S3, GCS or Azure Blob Storage, configure storage before the first sync. Set `OPENETL_CHECKPOINTS` in `.env` to the bucket URL, such as `s3://my-bucket`. For S3, put `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in `.env`. On EC2, you can use the instance profile instead. Set the instance metadata hop limit to 2, so that containers can reach the instance profile. [Run on AWS](./aws.md#on-ec2) shows the steps, and [Buckets](./index.md#buckets) gives the other options. Changing the setting does not move existing checkpoints.

## More runners

```sh
docker compose up -d --scale runner=2
```

Runners share checkpoints, staging and the token on the `runner` volume. Each runner otherwise reports the whole machine's capacity. When multiple runners share a machine, limit each with `OPENETL_CAPACITY` in `.env`. Use `CPU,MEMORY`, such as `4,8Gi`, and keep the total within the machine's capacity.

For API keys and runner tokens, follow [Manage access and credentials](./access.md). For telemetry and delivery alerts, see [Monitor an installation](./monitoring.md).

## Choose an orchestrator

The stack runs workflows on DBOS, in Postgres. To run them on a Temporal cluster you already have, add these to `.env` before the first start:

```sh
OPENETL_ORCHESTRATOR=temporal
OPENETL_TEMPORAL_ADDRESS=temporal.example.com:7233
OPENETL_TEMPORAL_NAMESPACE=openetl
```

Runners do not connect to Temporal. To change an existing installation's backend, follow [Changing it later](./index.md#changing-it-later). Stop the control plane with `docker compose stop server orchestrator`. After updating `.env`, restart it with `docker compose up -d`.

Before returning to DBOS, remove its old workflows while the control plane is stopped. `docker compose exec postgres dropdb -U openetl openetl_dbos` deletes that workflow database. The control plane recreates it on startup.

## Upgrade

Each release has a version, such as `0.1.0-alpha`. Its notes on the [releases page](https://github.com/openetl-io/openetl/releases) list what changed. In the alpha, the pipeline format and the API can change between releases.

1. Read the notes of each release between your version and the new one.
2. Make a backup, as [Back up](#back-up) describes.
3. Download the new release's `compose.yml`, which can change between releases. Replace `VERSION`:

   ```sh
   curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/vVERSION/deploy/compose/compose.yml
   ```

4. In `.env`, set `OPENETL_IMAGE=ghcr.io/openetl-io/openetl:VERSION`.
5. Replace the containers whose image changed:

   ```sh
   docker compose up -d
   ```

6. Upgrade the CLI on your computer to the same version, as [Install the CLI](../get-started/install.md#upgrade) shows.

The new orchestrator resumes existing workflows. Runners stop jobs at a checkpoint before exiting, with up to 2 minutes for a sync or 10 minutes for a stream. Jobs are then placed again and resume from their checkpoints. The Compose file allows runners 11 minutes to stop.

A newer version can write a checkpoint format that older runners cannot read. An older runner refuses that pipeline with `CONFIG_INVALID`. If this occurs after a downgrade, run the affected pipelines with the newer version.

## Back up

[Back up and restore](./backup-and-restore.md) has the stopped-backup procedure and recovery into fresh volumes. It covers both the metadata and DBOS databases, checkpoint and staging files, the encryption key and credentials. Test a restore before relying on a backup.

## Stop and remove

`docker compose stop` stops the stack, and `docker compose down` removes its containers too. Both keep the volumes, and `docker compose up -d` starts it again where it was.

> [!CAUTION]
> `docker compose down -v` also deletes the volumes: the database, the owner key and the checkpoints. You cannot recover the installation after this command, except from a backup.

To remove the installation and all its data, run `docker compose down -v`. Then delete the directory, which holds `.env`.
