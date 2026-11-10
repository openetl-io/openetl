---
title: Quickstart
description: Run OpenETL locally, copy generated rows into Postgres, and verify the result in the console and database.
---

In this tutorial, you run OpenETL locally and copy generated rows into Postgres. You then verify the result in the console and database. Allow about 15 minutes, mainly for the first build.

This example uses the official Postgres connector. Its [setup and configuration documentation](https://github.com/openetl-io/openetl-connectors/blob/main/postgres/README.md) is in `openetl-connectors`. These steps build the connector from a local checkout. For other systems, [choose a connector](../pipelines/connectors.md) and follow its documentation.

## What you need

Until the first release, OpenETL runs from source. You need:

- Git
- [Go](https://go.dev/dl/) 1.26 or later
- [Rust](https://rustup.rs), installed with rustup, which picks the version OpenETL needs
- [Node](https://nodejs.org) 22 and [pnpm](https://pnpm.io/installation), to build the console
- [Docker](https://docs.docker.com/get-started/get-docker/) with Compose, to run Postgres

## 1. Start OpenETL

Clone both repositories into the same parent directory. Start OpenETL from its repository directory:

```sh
git clone https://github.com/openetl-io/openetl
git clone https://github.com/openetl-io/openetl-connectors
cd openetl
make dev DEPS=postgres
```

`make dev` does four things:

1. Starts Postgres in Docker. It stores OpenETL's metadata and the data copied by this example.
2. Builds the connectors from `openetl-connectors`, the console, and OpenETL's data engine.
3. Runs `openetld dev`. This process provides the API, console, scheduler and runner.
4. Writes an API key for you to `.openetl/admin.key`, which the CLI reads.

When the log shows `API serving`, OpenETL is ready. Leave this process running.

> [!NOTE]
> The log can report that the `official` connector source failed to list packages. This tutorial uses local connector executables and does not need published packages.

## 2. Open the console

Go to [localhost:8080](http://localhost:8080). There are no pipelines yet.

The **Sign-in is off** banner appears because `openetld dev` permits owner access without authentication. It listens only on your computer. For a shared installation with sign-in, see [Self-host](../self-host/index.md).

## 3. Build the CLI

Open a second terminal in the same `openetl` directory. Build the CLI and add its directory to `PATH`:

```sh
go build -o bin/ ./cmd/openetl
export PATH="$PWD/bin:$PATH"
```

Run all subsequent commands from this directory. The CLI reads its API key from `.openetl/admin.key` here.

## 4. Store a secret

The pipeline writes to the Postgres database started by `make dev`. Store its connection URL as the `local-postgres-dsn` secret. The pipeline references this secret instead of including the password:

```sh
echo postgres://openetl:openetl@localhost:5432/openetl | openetl secret set local-postgres-dsn
```

```text
created workspaces/default/secrets/local-postgres-dsn
```

## 5. Read the pipeline

[`examples/faker-to-postgres.yaml`](../../examples/faker-to-postgres.yaml) holds three documents:

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
- The **destination** `local-postgres` uses the Postgres connector. It reads its connection string from the secret you stored.
- The **pipeline** `faker-to-postgres` copies all source streams into the destination's `faker` schema once an hour.

[Concepts](./concepts.md) explains each of these words.

## 6. Plan, then apply

See what applying the file would do:

```sh
openetl plan -f examples/faker-to-postgres.yaml
```

```text
would be created workspaces/default/sources/faker (version b6c77e6b0617)
would be created workspaces/default/destinations/local-postgres (version 2322957661cf)
would be created workspaces/default/pipelines/faker-to-postgres (version known after apply)

workspaces/default/pipelines/faker-to-postgres (version known after apply)
  job size: s (1 vCPU, 2 GiB), the default, in pool default
  notes:
    source "faker" hasn't been discovered, so the plan checks only the delivery settings (openetl discover faker)
```

The plan does not apply changes. In this output, the source has not been discovered, so the plan cannot check its individual streams.

After applying the source, `openetl discover faker` lists and stores its streams. Later plans can use this catalog to check [delivery guarantees](./concepts.md#delivery-guarantees). They also need the destination capabilities from its connector package. For a local connector executable without a package, the runner checks the guarantees before each sync.

Apply it:

```sh
openetl apply -f examples/faker-to-postgres.yaml
```

```text
created workspaces/default/sources/faker (version b6c77e6b0617)
created workspaces/default/destinations/local-postgres (version 2322957661cf)
created workspaces/default/pipelines/faker-to-postgres (version 9e052e7e6881)
checked source faker, destination local-postgres
```

A new pipeline starts its first sync immediately.

## 7. Watch it in the console

In the console, open **Pipelines**. Select **faker-to-postgres**. Its page shows:

- whether it's **Healthy**, its schedule, and when it last succeeded;
- a strip of its latest syncs, and the buttons **Sync now** and **Pause**;
- on **Overview**, its two streams, `users` and `purchases`, with the rows each one's latest sync copied;
- on **Syncs**, every sync, how long it took and how many rows it copied; open one to see its log.

The CLI shows the same:

```sh
openetl syncs faker-to-postgres
openetl logs faker-to-postgres
```

## 8. Look at the data

Wait until the first sync succeeds. Query the copied table with `psql` inside the Postgres container:

```sh
docker compose -f deploy/compose/dev.yml exec postgres \
  psql -U openetl -c 'select id, name, email, _openetl_op, _openetl_synced_at from faker.users limit 3'
```

```text
 id |      name       |             email             | _openetl_op |      _openetl_synced_at
----+-----------------+-------------------------------+-------------+------------------------------
  1 | Margaret Turing | Margaret.Turing.1@example.com | insert      | 2026-11-08 06:58:24.84168+00
  2 | Ken Lovelace    | Ken.Lovelace.2@example.com    | insert      | 2026-11-08 06:58:24.84168+00
  3 | Alan Lamport    | Alan.Lamport.3@example.com    | insert      | 2026-11-08 06:58:24.84168+00
```

OpenETL adds these four columns to each row:

| Column | What it holds |
|---|---|
| `_openetl_id` | A stable ID for this version of the row. Repeated reads of the same change produce the same ID for deduplication. |
| `_openetl_op` | Whether the change was an `insert`, an `update` or a `delete` |
| `_openetl_seq` | The position of this version in the source's order. It lets the destination distinguish later changes. |
| `_openetl_synced_at` | When OpenETL wrote it |

The Postgres connector also creates `_openetl_fence` and `_openetl_loads` to commit syncs exactly once. Do not modify these tables.

## 9. Sync again

Select **Sync now** in the console, or run this command:

```sh
openetl sync faker-to-postgres
```

After the sync succeeds, `faker.users` still has 1,000 rows. The source uses full refresh. By default, each sync replaces the destination data.

## 10. Change the pipeline

In `examples/faker-to-postgres.yaml`, change `count: 1000` to `count: 2000`. Review the plan:

```sh
openetl plan -f examples/faker-to-postgres.yaml
```

```text
would be updated workspaces/default/sources/faker (version 0eec602529fe)
would be unchanged workspaces/default/destinations/local-postgres (version 2322957661cf)
would be updated workspaces/default/pipelines/faker-to-postgres (version edab40da6d76)
```

The source configuration changed, so the pipeline also gets a new version. Apply the change. Start another sync:

```sh
openetl apply -f examples/faker-to-postgres.yaml
openetl sync faker-to-postgres
```

After the sync succeeds, `faker.users` has 2,000 rows. The pipeline's **Versions** tab and `openetl versions faker-to-postgres` list both versions. Restoring the original `count` and applying again makes the first version current.

## Clean up

Stop `openetld dev` with Ctrl-C. Stop Postgres:

```sh
make deps-down
```

Postgres retains its data in a Docker volume. The next `make dev` uses your existing pipeline.

To remove the stored data, delete the volumes and OpenETL's local files:

> [!WARNING]
> These commands delete all data in the Compose stack's volumes and the local `.openetl` directory. This includes data outside this tutorial.

```sh
docker compose -f deploy/compose/dev.yml down --volumes
rm -rf .openetl
```

## Next

- [Concepts](./concepts.md) explains pipelines, streams, delivery guarantees and the rest in a few minutes.
- [Use the console](../use/index.md) shows what else the console tells you.
- [Pipelines](../pipelines/index.md) covers writing your own: other sources and destinations, schedules, continuous pipelines and schema changes.
- [Your first pipeline](../pipelines/first-pipeline.md) copies tables from your own Postgres database into another.
