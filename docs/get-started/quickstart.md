---
title: Quickstart
description: Run OpenETL on your computer, copy generated rows into Postgres, and see what happened in the console and in the database.
---

In this quickstart you run OpenETL on your own computer, create a pipeline that copies generated rows into a Postgres database, and look at the result in the console and in the database. It takes about 15 minutes, most of it the first build.

## What you need

Until the first release, OpenETL runs from source. You need:

- Git
- [Go](https://go.dev/dl/) 1.26 or later
- [Rust](https://rustup.rs), installed with rustup, which picks the version OpenETL needs
- [Node](https://nodejs.org) 22 and [pnpm](https://pnpm.io/installation), to build the console
- [Docker](https://docs.docker.com/get-started/get-docker/) with Compose, to run Postgres

## 1. Start OpenETL

Clone OpenETL and its official connectors next to each other, and start it:

```sh
git clone https://github.com/openetl-io/openetl
git clone https://github.com/openetl-io/openetl-connectors
cd openetl
make dev DEPS=postgres
```

`make dev` does four things:

1. Starts Postgres in Docker. OpenETL keeps its own records there, and this quickstart copies data into it too.
2. Builds the connectors from `openetl-connectors`, the console, and OpenETL's data engine.
3. Runs `openetld dev`, which is the whole of OpenETL in one process: the API, the console, the scheduler and a runner that does the copying.
4. Writes an API key for you to `.openetl/admin.key`, which the CLI reads.

When the log says `API serving`, it's ready. Leave it running.

> [!NOTE]
> The log may warn that the connector source `official` failed to list. No connector packages have been published to it, and this quickstart doesn't need them.

## 2. Open the console

Go to [localhost:8080](http://localhost:8080). There are no pipelines yet.

The banner says sign-in is off: `openetld dev` lets anyone who can reach it act as an owner, and it listens only on your own computer. A real installation asks people to sign in; see [Self-host](../self-host/index.md).

## 3. Build the CLI

Open a second terminal in the same `openetl` directory, build the `openetl` command, and put it on your `PATH`:

```sh
go build -o bin/ ./cmd/openetl
export PATH="$PWD/bin:$PATH"
```

Run the commands that follow from this directory: that's where the CLI finds `.openetl/admin.key`.

## 4. Store a secret

The pipeline writes into the Postgres that `make dev` started. Store its address as a secret named `local-postgres-dsn`, so the pipeline can name the secret instead of holding the password:

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

- The **source** `faker` uses the built-in `faker` connector, which makes up the same 1,000 users and 1,000 purchases every time.
- The **destination** `local-postgres` uses the Postgres connector, and reads its connection string from the secret you stored.
- The **pipeline** `faker-to-postgres` copies every stream the source offers into the destination's `faker` schema, once an hour.

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
    the control plane doesn't know destination "local-postgres"'s capabilities, so only the runner checks delivery, before each sync
```

Nothing has changed yet. The note says the runner checks the [delivery guarantees](./concepts.md#delivery-guarantees) before each sync, rather than the plan checking them now. The control plane learns what a destination can do from its connector's package, and this quickstart runs the connectors from a local build instead.

Apply it:

```sh
openetl apply -f examples/faker-to-postgres.yaml
```

```text
created workspaces/default/sources/faker (version b6c77e6b0617)
created workspaces/default/destinations/local-postgres (version 2322957661cf)
created workspaces/default/pipelines/faker-to-postgres (version 9e052e7e6881)
```

A new pipeline starts its first sync straight away.

## 7. Watch it in the console

In the console, open **Pipelines**, then **faker-to-postgres**. Its page shows:

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

Query the tables the pipeline made, with the `psql` inside the Postgres container:

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

Besides the source's columns, every row carries four that OpenETL adds:

| Column | What it holds |
|---|---|
| `_openetl_id` | The ID of this version of the row: the same change gets the same ID however often it's read, so a repeat can be dropped |
| `_openetl_op` | Whether the change was an `insert`, an `update` or a `delete` |
| `_openetl_seq` | Where this version of the row comes in the source's order, so a later change always wins |
| `_openetl_synced_at` | When OpenETL wrote it |

The schema also has two tables, `_openetl_fence` and `_openetl_loads`, that the Postgres connector keeps to commit each sync exactly once. Leave them as they are.

## 9. Sync again

Press **Sync now** in the console, or:

```sh
openetl sync faker-to-postgres
```

`faker.users` still has 1,000 rows. The source reads the whole table every time, so by default each sync replaces what the destination has rather than adding to it.

## 10. Change the pipeline

Open `examples/faker-to-postgres.yaml`, change `count: 1000` to `count: 2000`, and plan again:

```sh
openetl plan -f examples/faker-to-postgres.yaml
```

```text
would be updated workspaces/default/sources/faker (version 0eec602529fe)
would be unchanged workspaces/default/destinations/local-postgres (version 2322957661cf)
would be updated workspaces/default/pipelines/faker-to-postgres (version edab40da6d76)
```

The source's config changed, so the pipeline gets a new version too. Apply it, sync, and the table has 2,000 rows:

```sh
openetl apply -f examples/faker-to-postgres.yaml
openetl sync faker-to-postgres
```

`openetl versions faker-to-postgres`, or the pipeline's **Versions** tab, lists both versions. Changing `count` back and applying again makes the first version current again.

## Clean up

Stop `openetld dev` with Ctrl-C, and stop Postgres:

```sh
make deps-down
```

Postgres keeps its data in a Docker volume, so the next `make dev` finds your pipeline where you left it. To start from nothing instead, delete the volumes and OpenETL's local files:

```sh
docker compose -f deploy/compose/dev.yml down --volumes
rm -rf .openetl
```

> [!WARNING]
> That deletes everything the compose stack's containers stored, not only this quickstart's data.

## Next

- [Concepts](./concepts.md) explains pipelines, streams, delivery guarantees and the rest in a few minutes.
- [Use the console](../use/index.md) shows what else the console tells you.
- [Pipelines](../pipelines/index.md) covers writing your own: other sources and destinations, schedules, continuous pipelines and schema changes.
- [Your first pipeline](../pipelines/first-pipeline.md) copies tables from your own Postgres database into another.
