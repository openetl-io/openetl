---
title: Back up and restore
description: Make a stopped backup of the default Compose installation and restore it into fresh volumes.
---

A recoverable installation needs its metadata database, orchestration state, checkpoint storage and secrets encryption key. The database alone is not enough. This guide backs up the [default Compose installation](./compose.md) while its writers are stopped, then restores it into fresh volumes with the same OpenETL image.

The commands assume the supplied Compose file, its default DBOS backend, and local checkpoint storage on the `runner` volume. Run them beside `compose.yml` and `.env`. For Kubernetes or managed storage, use the same inventory and shutdown order with your database and storage backup tools.

## Know what you are restoring

| State | Where the default stack keeps it | Why it matters |
|---|---|---|
| Resources, versions, secrets, users and audit | Postgres database `openetl` | The control plane's records; secret values are encrypted. |
| Workflow state | Postgres database `openetl_dbos` | DBOS's durable schedules and workflows. |
| Checkpoints, staging and held values | The `runner` volume | Resume positions and data needed for recovery. |
| Runner token | `.openetl/runner.token` on the `runner` volume | Matches the token record in the metadata database. |
| Bootstrap API key | `.openetl/admin.key` on the `server` volume | An existing owner credential. An owner key can also be recovered with `openetld bootstrap-key`. |
| Configuration and encryption key | `.env`, or the configured KMS and its access | Required to decrypt stored secrets and start the same installation. |

This does not back up your sources or destinations. Restoring an older backup restores older positions and configuration, while destination data may have advanced. Check the source's retained history and the destination connector's recovery behavior before using an older backup. A reset or rewind is not a substitute for restoring matching state.

## Make a stopped backup

Plan a maintenance window. Stop the runners first, allowing them to finish at a checkpoint, then stop the API server and orchestrator so neither accepts changes or advances workflows:

```sh
docker compose stop runner
docker compose stop server orchestrator
```

Keep Postgres running. With the default stack now stopped, create the backup:

```sh
umask 077
mkdir -p backup
cp .env backup/openetl.env
docker compose exec -T postgres pg_dump -U openetl -Fc openetl > backup/openetl.dump
docker compose exec -T postgres pg_dump -U openetl -Fc openetl_dbos > backup/openetl_dbos.dump
docker compose run --rm --no-deps -T --entrypoint tar runner \
  -C /var/lib/openetl -czf - . > backup/runner.tar.gz
docker compose run --rm --no-deps -T --entrypoint tar server \
  -C /var/lib/openetl -czf - . > backup/server.tar.gz
```

Check every command succeeded before relying on the backup. `pg_dump -Fc` produces a custom-format archive for `pg_restore`. Inspect the archives:

```sh
docker compose exec -T postgres pg_restore --list < backup/openetl.dump
docker compose exec -T postgres pg_restore --list < backup/openetl_dbos.dump
tar -tzf backup/runner.tar.gz
tar -tzf backup/server.tar.gz
```

Record the exact image tag or digest and PostgreSQL major version with the backup. Keep `openetl.env`, which may contain the decryption key, separate from the database backup in your backup storage. Protect both volume archives as credentials: they include API keys or runner tokens.

Restart the existing installation after the backup succeeds:

```sh
docker compose up -d
```

If you resumed production after taking the backup, its destinations may now be ahead of the stored checkpoints. Account for that when planning a restore; this guide does not provide a cross-system point-in-time rollback.

## Restore into fresh volumes

Use a replacement host or an isolated Compose project with empty volumes. Keep the original installation stopped throughout recovery so it cannot take work or write to the same destinations. Do not run the restore commands against a live installation.

Copy the Compose file and backup set to the replacement directory, restore `openetl.env` as `.env`, and select the same image used for the backup. Check paths, storage endpoints and network access before starting application services. The copied database password and encryption key must match the backup.

Start only Postgres and wait until it is ready:

```sh
cp backup/openetl.env .env
docker compose up -d --wait postgres
```

Restore the metadata into the fresh `openetl` database the container created, and create the DBOS database before restoring it:

```sh
docker compose exec -T postgres pg_restore --exit-on-error --clean --if-exists \
  -U openetl -d openetl < backup/openetl.dump
docker compose exec -T postgres createdb -U openetl openetl_dbos
docker compose exec -T postgres pg_restore --exit-on-error \
  -U openetl -d openetl_dbos < backup/openetl_dbos.dump
```

Restore both application volumes before starting their services. Use the same image as the backup for recovery; upgrade separately after verification.

Existing schedules and continuous pipelines can start as soon as the restored control plane runs. Complete storage and destination checks before the final `docker compose up` command:

```sh
docker compose run --rm --no-deps -T --entrypoint tar runner \
  -C /var/lib/openetl -xzf - < backup/runner.tar.gz
docker compose run --rm --no-deps -T --entrypoint tar server \
  -C /var/lib/openetl -xzf - < backup/server.tar.gz
docker compose up -d --wait
```

## Verify the recovery

Check service health and that the restored owner key authenticates:

```sh
docker compose ps -a
docker compose exec server openetl whoami
```

Open the console and compare pipeline versions, latest checkpoints and incidents with the backup record. Verify the API can decrypt a source or destination's secret by running its connection check. For a test pipeline, inspect its previous syncs, run a new sync, and verify the destination rows:

```sh
docker compose exec server openetl syncs PIPELINE
docker compose exec server openetl sync PIPELINE
docker compose exec server openetl logs PIPELINE
```

A continuous pipeline resumes its job rather than starting a scheduled sync. Check that its checkpoint advances and any open holds remain accounted for. Resolve recovery errors before returning the installation to users.

## Other backends and storage

For object-store checkpoints, stop all runners and job pods before backing up pipeline objects. Restore those objects at the same paths before restarting the runners. The store includes staging and held values as well as checkpoint records. Changing `OPENETL_CHECKPOINTS` does not move existing state.

With a custom DBOS URL, back up and restore that database too, even when it lives on another server. With Temporal, the separate workflow state belongs to your Temporal deployment; its backup and recovery must be planned alongside OpenETL's metadata and checkpoint storage. Restoring metadata alone does not restore Temporal's workflow history. Keep the selected orchestration backend unchanged during recovery.

A backup is proven by restoring it. Repeat the recovery on an isolated installation, using test destinations, and record the checks and recovery time before relying on the procedure for production.
