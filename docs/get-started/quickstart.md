---
title: Quickstart
description: Run the OpenETL release on your computer with Docker Compose, copy generated rows into Postgres, and check the result in the console and the database.
---

In this tutorial, you run OpenETL 0.1.0-alpha on your computer with Docker Compose. You copy generated rows into a Postgres database, then check the result in the console and the database. Allow about 15 minutes.

This example uses the official Postgres connector. Its [setup and configuration documentation](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) is in `openetl-connectors`. For other systems, [choose a connector](../pipelines/connectors.md) and follow its documentation.

## What you need

- macOS or Linux, on an amd64 or arm64 processor.
- [Docker](https://docs.docker.com/get-started/get-docker/) with Compose 2.24 or later. Docker Desktop, OrbStack and Docker Engine on Linux all work.
- `curl` and `openssl`. macOS and most Linux distributions include them.
- About 2 GB of free disk space for the images.

## 1. Install the CLI

Install `openetl`, the command-line tool:

```sh
curl -fsSL https://raw.githubusercontent.com/openetl-io/openetl/main/install.sh | sh
```

The script puts `openetl` in `~/.local/bin`. If that directory is not on your `PATH`, the script shows the line to add. [Install the CLI](./install.md) has the other ways to install it and how to check the download.

Check that it works:

```sh
openetl version
```

```text
0.1.0-alpha
```

## 2. Start OpenETL

Make a directory for OpenETL, and download its Compose file from the release:

```sh
mkdir openetl && cd openetl
curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/v0.1.0-alpha/deploy/compose/compose.yml
```

Write the settings file, `.env`. It names the release's image, a Postgres password, and the key that encrypts the secrets you store:

```sh
cat > .env <<EOF
OPENETL_IMAGE=ghcr.io/openetl-io/openetl:0.1.0-alpha
POSTGRES_PASSWORD=$(openssl rand -hex 16)
OPENETL_SECRETS_KEY=base64key://$(openssl rand -base64 32 | tr '+/' '-_')
EOF
chmod 600 .env
```

If OpenETL ran on this computer before, its volumes can stop the new stack from starting. [An earlier installation on this machine](../self-host/compose.md#an-earlier-installation-on-this-machine) shows how to continue that installation, run beside it, or delete it.

Start the services. The first start downloads the images, which can take a few minutes:

```sh
docker compose up -d
docker compose ps -a --format 'table {{.Service}}\t{{.Status}}'
```

```text
SERVICE        STATUS
orchestrator   Up 3 seconds
postgres       Up 8 seconds (healthy)
runner         Up 3 seconds
runner-token   Exited (0) 3 seconds ago
server         Up 6 seconds (healthy)
```

The stack has five services:

- `postgres` stores OpenETL's own records.
- `server` serves the API and the console on `localhost:8080`.
- `orchestrator` schedules syncs and gives them to runners.
- `runner` runs the syncs.
- `runner-token` makes the runner's credential on the first start, then stops. Its `Exited (0)` status is correct.

The stack listens only on your computer.

## 3. Sign in

On its first start, the server writes an owner API key in its container. Copy the key to `.openetl/admin.key`, where the CLI looks for it:

```sh
mkdir -p .openetl
docker compose exec -T server cat .openetl/admin.key > .openetl/admin.key
chmod 600 .openetl/admin.key
openetl whoami
```

```text
principal    principals/admin
roles        owner
```

This example shows only the first lines of the output. The other lines give the key and its permissions. Run all the commands that follow from this directory, so that the CLI finds the key.

The console needs a user with a password. Make one for yourself, and make it an owner. Replace `YOU@EXAMPLE.COM` and `YOUR-NAME`:

```sh
openetl principal create --user --email YOU@EXAMPLE.COM --password-link YOUR-NAME
openetl principal grant YOUR-NAME owner
```

```text
created principals/YOUR-NAME
a link for principals/YOUR-NAME to set their password, which works once until 2026-11-12 14:03:42; send it to them yourself:
http://localhost:8080/set-password#…
granted principals/YOUR-NAME owner on the org
```

This example shortens the link. Open the full link from your output, and set your password. Sign in at [localhost:8080](http://localhost:8080). There are no pipelines yet.

## 4. Prepare the destination

The pipeline writes into a Postgres database of its own. Start one in a container on the stack's network, `openetl_default`:

```sh
docker run -d --name quickstart-postgres --network openetl_default -e POSTGRES_PASSWORD=quickstart postgres:18
```

The runner runs in a container on the same network. It reaches this database by its container name, `quickstart-postgres`.

Install the Postgres connector from the official connector registry:

```sh
openetl connector apply oci://ghcr.io/openetl-io/openetl-connectors/postgres:0.1.0-alpha
```

```text
created connectors/openetl/postgres/versions/0.1.0-alpha (version sha256:93636)
```

`openetl connector catalog` lists the official connectors and the command that installs each one. In the console, **Connections**, then **Connectors**, shows the same catalog.

Store the database's connection URL as the secret `local-postgres-dsn`. The pipeline refers to this secret, so its file does not contain the password:

```sh
echo postgres://postgres:quickstart@quickstart-postgres:5432/postgres | openetl secret set local-postgres-dsn
```

```text
created workspaces/default/secrets/local-postgres-dsn
```

## 5. Read the pipeline

Download the example pipeline:

```sh
curl -fsSLO https://raw.githubusercontent.com/openetl-io/openetl/v0.1.0-alpha/examples/faker-to-postgres.yaml
```

The file, [`examples/faker-to-postgres.yaml`](../../examples/faker-to-postgres.yaml), holds three documents:

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
    batchSize: 100
---
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata:
  name: local-postgres
spec:
  connector: postgres
  config:
    dsn: { secret: local-postgres-dsn }
---
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata:
  name: faker-to-postgres
spec:
  schedule: { every: 1h }
  source: { ref: faker }
  destination:
    ref: local-postgres
    namespace: faker
```

- The **source** `faker` uses the built-in `faker` connector. It generates the same 1,000 users and 1,000 purchases on each read.
- The **destination** `local-postgres` uses the Postgres connector that you installed. It reads its connection URL from the secret that you stored.
- The **pipeline** `faker-to-postgres` copies all the source's streams into the destination's `faker` schema once an hour.

[Concepts](./concepts.md) explains each of these words.

## 6. Plan, then apply

See what applying the file would do:

```sh
openetl plan -f faker-to-postgres.yaml
```

```text
would be created workspaces/default/sources/faker (version b6c77e6b0617)
would be created workspaces/default/destinations/local-postgres (version a1e5d4fde373)
would be created workspaces/default/pipelines/faker-to-postgres (version known after apply)

workspaces/default/pipelines/faker-to-postgres (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  warnings:
    destination "local-postgres" names connector postgres without a version, so applying it again may pin a newer one; postgres@MAJOR keeps to one major version
  notes:
    source "faker" hasn't been discovered, so the plan checks only the delivery settings (openetl discover faker)
```

The plan does not change anything. It tells you two things:

- The warning says that `connector: postgres` has no version. Each apply selects the newest installed version. [Select a version in a connection](../pipelines/connectors.md#select-a-version-in-a-connection) shows how to pin one.
- The note says that the source is not discovered yet, so the plan cannot check each stream. A sync, or `openetl discover faker`, stores the source's streams. Later plans use them to check [delivery guarantees](./concepts.md#delivery-guarantees).

Apply the file:

```sh
openetl apply -f faker-to-postgres.yaml
```

```text
created workspaces/default/sources/faker (version b6c77e6b0617)
created workspaces/default/destinations/local-postgres (version a1e5d4fde373)
created workspaces/default/pipelines/faker-to-postgres (version 60a7c76ede3e)

warnings for workspaces/default/pipelines/faker-to-postgres:
  destination "local-postgres" names connector postgres without a version, so applying it again may pin a newer one; postgres@MAJOR keeps to one major version
checked source faker, destination local-postgres
```

A new pipeline starts its first sync immediately.

## 7. Watch it in the console

In the console, open **Pipelines**. Select **faker-to-postgres**. Its page shows:

- whether it is **Healthy**, its schedule, and when it last succeeded;
- a strip of its latest syncs, and the buttons **Sync now** and **Pause**;
- on **Overview**, its two streams, `users` and `purchases`, with the rows that each one's latest sync copied;
- on **Syncs**, every sync, how long it took and how many rows it copied. Open a sync to see its log.

The CLI shows the same information:

```sh
openetl syncs faker-to-postgres
openetl logs faker-to-postgres
```

## 8. Look at the data

Wait until the first sync succeeds. Then query the copied table with `psql` in the database's container:

```sh
docker exec quickstart-postgres psql -U postgres \
  -c 'select id, name, email, _openetl_op, _openetl_synced_at from faker.users limit 3'
```

```text
 id |      name       |             email             | _openetl_op |      _openetl_synced_at
----+-----------------+-------------------------------+-------------+-------------------------------
  1 | Margaret Turing | Margaret.Turing.1@example.com | insert      | 2026-11-11 08:34:15.394187+00
  2 | Ken Lovelace    | Ken.Lovelace.2@example.com    | insert      | 2026-11-11 08:34:15.394187+00
  3 | Alan Lamport    | Alan.Lamport.3@example.com    | insert      | 2026-11-11 08:34:15.394187+00
(3 rows)
```

OpenETL adds these four columns to each row:

| Column | What it holds |
|---|---|
| `_openetl_id` | A stable ID for this version of the row. Repeated reads of the same change produce the same ID for deduplication. |
| `_openetl_op` | Whether the change was an `insert`, an `update` or a `delete` |
| `_openetl_seq` | The position of this version in the source's order. It lets the destination identify later changes. |
| `_openetl_synced_at` | When OpenETL wrote it |

The Postgres connector also makes the tables `_openetl_fence` and `_openetl_loads`. It uses them to commit each sync exactly once. Do not change these tables.

## 9. Sync again

Select **Sync now** in the console, or run this command:

```sh
openetl sync faker-to-postgres
```

After the sync succeeds, `faker.users` still has 1,000 rows. The source uses full refresh: by default, each sync replaces the data in the destination.

## 10. Change the pipeline

In `faker-to-postgres.yaml`, change `count: 1000` to `count: 2000`. Review the plan:

```sh
openetl plan -f faker-to-postgres.yaml
```

```text
would be updated workspaces/default/sources/faker (version 0eec602529fe)
would be unchanged workspaces/default/destinations/local-postgres (version a1e5d4fde373)
would be updated workspaces/default/pipelines/faker-to-postgres (version f7b79cd0024a)

workspaces/default/pipelines/faker-to-postgres (version f7b79cd0024a)
  changes:
    source.version: "b6c77e6b06170e5af61938ee22e3bf14cb8a5a43c40fb28b2d574ed5a92b5173" -> "0eec602529fedf3d8b95b4e646c06a0337e0e58c0099132fcd9d285dfbb7f3bb"
  job size: xs (0.5 vCPU, 1 GiB), from its recent peaks, in pool default
  streams:
    users      overwrite  exactly_once  exact replay
    purchases  overwrite  exactly_once  exact replay
  warnings:
    destination "local-postgres" names connector postgres without a version, so applying it again may pin a newer one; postgres@MAJOR keeps to one major version
  notes:
    source "faker" was last discovered with another config, and the plan uses that catalog (openetl discover faker)
```

The source's configuration changed, so the pipeline also gets a new version. The first sync stored the source's streams, so this plan shows each stream's write mode and delivery guarantee. Apply the change, and start another sync:

```sh
openetl apply -f faker-to-postgres.yaml
openetl sync faker-to-postgres
```

After the sync succeeds, `faker.users` has 2,000 rows. The pipeline's **Versions** tab and `openetl versions faker-to-postgres` list both versions. If you set `count` back to `1000` and apply again, the first version becomes current again.

## Clean up

Stop the services. This keeps their data:

```sh
docker compose stop
docker stop quickstart-postgres
```

To start them again where they stopped, run `docker start quickstart-postgres` and `docker compose up -d`.

> [!WARNING]
> These commands delete the stack's volumes and the destination database. This removes all pipelines, secrets, checkpoints and copied data of this installation.

To remove everything:

```sh
docker rm -f -v quickstart-postgres
docker compose down -v
```

Then delete the `openetl` directory, which holds `.env` and the copied key.

## Next

- [Concepts](./concepts.md) explains pipelines, streams, delivery guarantees and the other words in a few minutes.
- [Use the console](../use/index.md) shows what else the console tells you.
- [Pipelines](../pipelines/index.md) covers writing your own: other sources and destinations, schedules, continuous pipelines and schema changes.
- [Your first pipeline](../pipelines/first-pipeline.md) copies tables from your own Postgres database into another.
- [Install with Docker Compose](../self-host/compose.md) runs this stack for your team, with your own settings.
