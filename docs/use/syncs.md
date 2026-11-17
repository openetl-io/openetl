---
title: Inspect and control syncs
description: Read sync results and logs, start a sync, or pause and resume a pipeline.
---

Open a pipeline from **Pipelines**. Use **Syncs** to inspect its runs. Your [role](./access.md#what-your-role-lets-you-do) determines which actions are available.

## Sync now, Pause and Resume

- **Sync now** starts a sync immediately. It is disabled while a sync is running and hidden while the pipeline is paused. Continuous pipelines do not have this action.
- **Pause** prevents future scheduled syncs. An active scheduled sync continues. For a continuous pipeline, it stops the running job.
- **Resume** enables scheduled starts again. For a continuous pipeline, it restarts the job from its last checkpoint.

These buttons, and **Preflight**, show only if your role may use them.

## Syncs

**Syncs** lists syncs from newest to oldest. Each row shows **Status**, **Started**, **Took**, **Rows**, **Version** and any **Error**. Select **Load older syncs** to view earlier results.

## Schemas

**Schemas** shows each stream's columns, **Type** and whether null values are allowed. Renamed columns also show their source names. Column changes create schema versions: `v1`, `v2` and later versions. Select a version to compare it with the previous one. Columns are marked **Added**, **Removed** or **Changed**.

A pipeline with a transform has two lists. **Outputs** are the tables that the destination gets. **Inputs** are the source's streams as the transform reads them. An output and an input can have the same name, and each has its own versions.

## Checkpoints

**Checkpoints** lists commits from the last 30 days, newest first. Each entry shows its time, total rows and rows per stream, including quarantined, held or collapsed data. Use this tab to check which records were committed and when.

## Versions

**Versions** lists pipeline versions and marks the **Current** version. Each entry shows its **Applied from** file and the versions of its source, destination and Spark or Flink clusters. **Copy YAML** copies the complete definition. Applying an earlier definition makes that version current again.

## A sync's page

Open a sync from the strip or **Syncs** tab. The header shows its status, **Started** time, **Took** duration and **Rows**. For active syncs, **Running for** shows elapsed time.

The header also shows the pipeline **Version**, **Data plane**, **Size**, and CPU and memory **Used**. For a sync on several [workers](../get-started/concepts.md#where-it-runs), **Size** is the size of each worker. **Used** shows the largest worker's usage.

Stream row counts follow the header. Failed syncs include **Why it failed**.

A sync that needed more than one attempt lists them under **Attempts**, each with what it failed with.

**Log** shows the sync's log by attempt. Select **All**, **Warnings and errors**, or **Errors** to filter it. Use **Find in the log** to search it. While a sync runs, **Follow** keeps the newest lines in view.
