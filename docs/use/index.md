---
title: Use the console
description: A tour of OpenETL's console, for checking whether your data is fresh, what each sync did, and what needs attention.
---

The console is OpenETL's web app. Use it to check pipeline health, data freshness, sync results and incidents. Your [role](#what-your-role-lets-you-do) determines which resources you can view or change.

If a word on this page is new, [Concepts](../get-started/concepts.md) explains it in a few lines.

## Sign in

Whoever runs OpenETL gives you two things: the console's address, and a link to set your password.

1. Open the link. The page is **Set your password**.
2. Choose a password of 15 to 256 characters. It can't be your email or a common password.
3. If you want a persistent session, select **Keep me signed in on this device**.
4. Select **Set password and sign in**.

A link works once. If the page says the link has expired or was already used, ask an admin for a new one.

For later visits, open the console address. On **Sign in to OpenETL**, enter your **Email** and **Password**. Select **Sign in**.

Your name is at the bottom of the sidebar. Its menu shows your roles, and has:

- **Theme**: **Light**, **Dark** or **Match the system**.
- **Rows**: **Comfortable** or **Compact**, for how much fits on a page.
- **Change password**. Changing it signs you out everywhere else, but not in this browser.
- **Sign out**.

> [!NOTE]
> With **Sign-in is off**, anyone who can reach the console has owner access. This is the default for local development with `openetld dev`.

## Find your way around

The sidebar has:

- **Pipelines**, the place to start.
- **Incidents**, with a count of the ones open.
- Under **Connections**: **Sources**, **Destinations**, **dbt projects**, **Clusters** and **Connectors**.
- Under **Settings**: **Webhooks**, **Secrets**, **Access** and **Audit log**, which only admins can read.

Select **Search** to find a pipeline or page. The keyboard shortcut is ⌘K on macOS or Ctrl+K elsewhere. The search menu also provides theme, row density and sign-out actions.

## Pipelines

**Pipelines** lists every pipeline, the most urgent first. Search by name, or filter by **Status**, **Source** or **Destination**.

| Column | What it shows |
|---|---|
| **Pipeline** | Its name. Open it for its page. |
| **Status** | The latest sync result, as defined below. During retries, it includes the failed-attempt count. Open incidents also have a count. |
| **Source**, **Destination** | Where it reads from and writes to. |
| **Schedule** | How often it syncs: **Every 1h**, a cron expression, **Continuous**, or **On demand** when it only syncs when someone starts one. |
| **Freshness** | The latest successful sync's end time, or the latest commit time for a continuous pipeline. An interval-based pipeline is highlighted after two intervals without a successful sync. |
| **Last sync** | How many rows its latest sync copied. |

A pipeline's status is its latest sync's, unless it's paused:

| Status | Meaning |
|---|---|
| **Healthy** | The latest sync succeeded. |
| **Running** | A sync is running now. |
| **Degraded** | The latest sync succeeded, but held something back: rows whose values didn't fit their columns, or whole columns. The pipeline's page says what. |
| **Failed** | The latest sync failed. |
| **Paused** | Someone paused it, so it doesn't sync on its schedule. |
| **Stopped** | A continuous pipeline's run stopped when it was asked to, such as for a new version or a pause. |
| **Never synced** | It hasn't synced yet. |

## A pipeline's page

The top of a pipeline's page has its status, and:

- **Source** and **Destination**, with the schema it writes into.
- **Transform**, if its data goes through Spark or Flink on the way: which, and the cluster it runs on. A pipeline with SQL statements shows **SQL**.
- **Schedule**, or **Paused**.
- **Last success**, or **Last commit** for a continuous pipeline, which is its freshness.
- **Behind source**, for a continuous pipeline: how far its reading was behind the source at its latest commit.
- **Size**: how much CPU and memory its syncs get, and why.
- **Version**: which version of the pipeline is current. Click it to see that version.

Below that, a strip shows its latest syncs, oldest on the left. Each one's color is its status, and its length grows with how long it took. Hover over one for its rows, or click it to open it.

### Sync now, Pause and Resume

- **Sync now** starts a sync immediately. It is disabled while a sync is running and hidden while the pipeline is paused. Continuous pipelines do not have this action.
- **Pause** prevents future scheduled syncs. An active scheduled sync continues. For a continuous pipeline, it stops the running job.
- **Resume** enables scheduled starts again. For a continuous pipeline, it restarts the job from its last checkpoint.

These buttons show only if your role may use them.

### Overview

**Overview** first shows open incidents, retries after failed attempts and the latest failed sync. It links to the failed sync's log.

**Not as the spec says** follows when the pipeline runs differently from how it was set up, until someone undoes it:

- A **Paused** stream is skipped while its saved position is retained. The entry explains why and links to the incident if heal paused it. **Resume stream** enables reads again.
- **Changed by heal** identifies a setting changed by automatic recovery. It shows the active value beside the pipeline definition's value. **Use the spec's** restores the definition's value. While a schedule override applies, the header's **Schedule** shows that interval.

A diagram then shows the source, each stream, the destination, and any step that runs after each sync, such as a dbt project. The **Streams** table has a row for each stream:

| Column | What it shows |
|---|---|
| **Stream** | The table, topic or stream it reads, marked **Paused** if syncs skip it. |
| **Read** | How it's read: full refresh, incremental (and on which column), CDC or stream. **Source default** when the pipeline leaves it to the source. |
| **Write** | How it's written: append, overwrite or merge. |
| **Delivery** | Its [delivery guarantee](../get-started/concepts.md#delivery-guarantees): **Exactly once**, **Effectively once** or **At least once**. Hover over it for the reason. **Checked when it syncs** means the control plane can't work it out ahead of time, and checks it before each sync instead. |
| **Schema** | Its schema's version. Click it to see the columns. |
| **Rows** | Rows the latest sync copied. |
| **Quarantined** | Rows held back because a value didn't fit its column's type. |
| **Held** | Columns [held back](../get-started/concepts.md#schema-changes-and-holds). |
| **Collapsed** | Repeated rows that weren't written again, because a row with the same ID was already there. |

Messages above the table say what's wrong with the pipeline's plan, what to look out for, and what the plan couldn't check.

If the pipeline holds values, **Holds** lists each affected column. Entries identify whole-column or individual-cell holds, their start time, reason and current state:

| State | Meaning |
|---|---|
| **Held** | Its values are kept until someone releases or discards them. |
| **Pending** | It was released or discarded, and waits for the pipeline's next run. |
| **Releasing** | Its held values are being written in. |
| **Release refused** | The last release didn't clear it, and won't be tried again until it's released again. |
| **Discarded** | Its values were dropped. The column stays as the destination has it. |

**To clear it** lists the available recovery choices in recommended order. These include a destination repair, where applicable, and commands to release or discard the hold.

### SQL

A pipeline with a SQL transform has a **SQL** tab. A SQL transform is SQL statements that change rows on their way to the destination. Each `INSERT INTO` statement writes an **output**, which the destination gets as a stream.

- **Messages** at the top are the plan's problems and notes about the SQL that do not belong to one statement. A problem is something a sync fails on. Each message starts with its place in the SQL, as line and column, such as `9:10`.
- **Statements** shows the SQL as the pipeline's current version has it, with line numbers. Lines with a problem are marked in red, and lines with a note in blue. A line under it says what happens to streams that no statement writes: they pass through to the destination unchanged, or they are not delivered.
- **Plan** shows how the statements plan against the source's latest list of streams and columns. There is a card for each statement and each view, in the order they are written.

Each statement's card shows:

| Part | What it shows |
|---|---|
| **INSERT INTO** | The output it writes, and the destination table it goes to if the pipeline names one |
| Its place | The line and column where the statement starts |
| **Row-wise** | The kind of statement. Only row-wise statements run. |
| **Replaces stream** | The output takes the place of the source's stream of the same name. |
| **Primary key** | The output's key. **None, so append only** means rows are only added, never merged. |
| **Reads** | The source's streams it reads, and the views it reads them through |
| **Exchange** | Whether rows move between lanes before the statement. **None** means they do not. |
| Notes and problems | The plan's messages about this statement |
| **Column**, **Type**, **Nulls** | The output's columns |

A **CREATE VIEW** card shows a view: a named part of the SQL that statements read. It writes nothing itself.

Below the cards, **Passed through** lists the source's streams that no statement writes, and **Not delivered** the ones left out.

If the statements have errors, the **Plan** side says so, and the errors are at the top, each with its place. If the statements could not be planned, for example because no runner answered, a note at the top says why.

### Syncs

**Syncs** lists syncs from newest to oldest. Each row shows **Status**, **Started**, **Took**, **Rows**, **Version** and any **Error**. Select **Load older syncs** to view earlier results.

### Schemas

**Schemas** shows each stream's columns, **Type** and whether null values are allowed. Renamed columns also show their source names. Column changes create schema versions: `v1`, `v2` and later versions. Select a version to compare it with the previous one. Columns are marked **Added**, **Removed** or **Changed**.

A pipeline with a transform has two lists. **Outputs** are the tables that the destination gets. **Inputs** are the source's streams as the transform reads them. An output and an input can have the same name, and each has its own versions.

### Checkpoints

**Checkpoints** lists commits from the last 30 days, newest first. Each entry shows its time, total rows and rows per stream, including quarantined, held or collapsed data. Use this tab to check which records were committed and when.

### Versions

**Versions** lists pipeline versions and marks the **Current** version. Each entry shows its **Applied from** file and the versions of its source, destination and Spark or Flink clusters. **Copy YAML** copies the complete definition. Applying an earlier definition makes that version current again.

## A sync's page

Open a sync from the strip or **Syncs** tab. The header shows its status, **Started** time, **Took** duration and **Rows**. For active syncs, **Running for** shows elapsed time. It also shows the pipeline **Version**, **Data plane**, **Size**, and CPU and memory **Used**. Stream row counts follow the header. Failed syncs include **Why it failed**.

A sync that needed more than one attempt lists them under **Attempts**, each with what it failed with.

**Log** is the sync's log, attempt by attempt. Show **All** of it, **Warnings and errors**, or only **Errors**, and use **Find in the log** to search it. While a sync runs, **Follow** keeps the newest lines in view.

## Incidents

An incident records a condition that needs attention. **Incidents** lists incidents across pipelines, with open incidents first. Each entry shows a summary, **Severity**, **State**, **Kind**, **Seen** count and **Last seen** time. Filter by **State**, **Severity**, **Pipeline** or **Kind**.

There are two severities:

- **Critical**: data has stopped flowing, or will be lost.
- **Warning**: data still flows, or will soon stop if nothing changes.

And these kinds:

| Kind | What happened |
|---|---|
| **Sync failed** | A sync failed with an error no other kind covers. |
| **Auth** | A source or destination refused its credentials, or they expired. |
| **Schema blocked** | A stream's schema changed in a way the pipeline doesn't take. |
| **Rate limited** | The source kept limiting requests until the sync ran out of retries. |
| **Position lost** | The source no longer has the point the pipeline was reading from, so reading can't carry on from there. |
| **Crash loop** | A continuous pipeline's attempts fail or lose their runner, three in a row within a day, without committing anything. |
| **Lagging** | A continuous pipeline has fallen too far behind its source. |
| **Retention at risk** | The pipeline's reading is so far behind that it has used half or more of what the source keeps for it, such as a database's log of changes. |
| **Quarantine** | A stream is holding back a large share of its rows. |
| **Held** | A stream is holding a column, or some of its cells, back. |
| **Checkpoint corrupt** | The checkpoint the pipeline would carry on from can't be read, or the pipeline restarted from the one before it. |

An incident page has four sections:

- **What happened** describes the condition and links to the sync log.
- **Next steps** gives recovery instructions.
- **Heal** records automatic actions or proposed actions.
- **Details** provides additional diagnostic information.

Some incidents pause the pipeline because another sync would repeat the failure. Others change how it runs:

- **Quarantine** pauses the stream, so it holds back no more rows while the others carry on. A stream that can't stop on its own, such as one read with CDC, isn't paused, and **Next steps** says why. A stream that's the pipeline's only one pauses the pipeline. **Next steps** names the file the held-back rows are in, each with the reason it didn't fit.
- **Rate limited** reduces request rate where supported, or increases the sync interval, after 3 consecutive rate-limit failures. It repeats the adjustment after every 3 further failures.

What they changed shows under **Not as the spec says** on the pipeline's **Overview**, where you can undo it. A pipeline's definition decides how far heal goes: it can propose these actions without taking them, or take none.

Most incidents resolve automatically when the pipeline makes progress. If you repaired a condition that OpenETL cannot detect, select **Resolve** to close the incident. An optional **Comment** records the repair. If the condition recurs, a new incident opens. A **Lagging** incident reopens at the next checkpoint if the pipeline is still behind.

For a pipeline that waits, fails repeatedly or stops delivering fresh data, follow [Troubleshoot a pipeline](../pipelines/troubleshooting.md). [Handle schema changes and holds](../pipelines/schema-changes.md) shows a complete hold and release.

## Sources, destinations and connectors

- **Sources** and **Destinations** list each one with its **Connector**, the connector's **Version**, the **Pipelines** that use it, and when it was **Updated**. A source's or destination's page shows its pipelines and its **Config**. Configs name secrets, and never show their values.
- **dbt projects** lists the dbt projects that run after pipelines' syncs, and under **Run after**, the pipelines each one runs after.
- **Clusters** has **Spark** and **Flink** tabs. Each entry shows what the cluster **Runs on**, such as **spark-submit**, **EMR on EC2** or **Kubernetes**. **Used by** lists its pipelines. Open a cluster to inspect **Settings**, defaults, job configuration and **Pipelines**. **Runs** identifies use as a transform or after a sync. Secret settings show references without their values.
- **Connectors** has a **Catalog** of the connector packages available to install, and the **Sources** they're listed from. Open a package for its details, and the command that applies it, which makes it available to sources and destinations. A catalog source is a package registry or repository, separate from a pipeline's data source.

Connectors can be official, community maintained or private to your organization. Each has its own versions and releases, separate from OpenETL. Use a package's **Docs** link for its setup, supported modes and limits. An administrator manages installation and upgrades; [Choose and manage connectors](../pipelines/connectors.md) gives the CLI steps.

## Settings

- **Webhooks** lists the addresses OpenETL sends events to, such as failed syncs and incidents, and whether each is **Enabled**, **Failing** or **Disabled**. A webhook's page lists its **Deliveries**, and has **Send test event**, **Disable** and **Enable**.
- **Secrets** lists the passwords and keys that configs name, and which sources and destinations each is **Used by**. No page shows a secret's value.
- **Access** lists the people and service accounts under **Principals**, with their roles, and the **Runner tokens** that let runners join. A person's page lists their API keys, and an admin can **Sign out everywhere** for them there.
- **Audit log** lists every change, and every read of a secret, with who made it. Only admins can read it.

[Manage access and credentials](../self-host/access.md) covers adding users and service accounts, rotating credentials and removing access.

## What your role lets you do

Each role includes the ones before it. A role is given for one workspace, or for the whole organization; admin and owner are only for the whole organization.

| Role | What it adds |
|---|---|
| Viewer | Reads resources in their role's scope, including sync logs and incidents. The audit log requires Admin; no role sees secret values. |
| Operator | **Sync now**, **Pause** and **Resume**; resolving incidents; sending a webhook a test event, and disabling or enabling it. With the CLI, also checking that sources and destinations can be reached, and resizing a pipeline's syncs. |
| Editor | Creating, changing and deleting sources, destinations, pipelines, dbt projects, clusters and webhooks; setting secrets; releasing and discarding holds. With the CLI, also reading a pipeline's streams again from the start, and letting it restart from an earlier checkpoint. |
| Admin | Installing connectors; adding people and service accounts, and giving them roles, API keys and password links; runner tokens; the audit log; signing people out everywhere. |
| Owner | Managing other owners. |

## What the console doesn't do

Some changes are made with the `openetl` command instead, by someone whose role allows them:

- **Creating or changing pipelines**, sources, destinations, dbt projects, clusters and webhooks. **New pipeline** shows the commands that do it. Pipelines are YAML files: see [Pipelines](../pipelines/index.md).
- **Setting secrets.**
- **Releasing or discarding holds.** Each hold shows the command under **To clear it**.
- **Adding people, giving roles and making API keys.**

## With the CLI

The `openetl` command shows most of what the console does, and does what the console can't. Replace `PIPELINE` with the pipeline's name:

```sh
openetl syncs PIPELINE                   # its syncs, newest first
openetl logs PIPELINE                    # its latest sync's log; -f follows a running one
openetl versions PIPELINE                # its versions, newest first
openetl plan -f FILE                     # SQL: the plan of the pipelines in FILE
openetl sync PIPELINE                    # Sync now
openetl pause PIPELINE                   # Pause
openetl resume PIPELINE                  # Resume
openetl resume --stream STREAM PIPELINE  # Resume stream
openetl overrides PIPELINE               # Not as the spec says
openetl overrides remove PIPELINE FIELD  # Use the spec's
openetl incident list                    # open incidents
openetl incident get PIPELINE INCIDENT   # one incident: what happened and what to do
openetl incident resolve PIPELINE INCIDENT
openetl holds PIPELINE                   # what it holds back
openetl holds release PIPELINE STREAM COLUMN
openetl holds discard PIPELINE STREAM COLUMN
```

`openetl principal password-link NAME` makes the link a person sets their password with, and `openetl principal sign-out NAME` signs them out everywhere. `openetl COMMAND -h` lists a command's flags.
