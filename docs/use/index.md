---
title: Use the console
description: A tour of OpenETL's console, for checking whether your data is fresh, what each sync did, and what needs attention.
---

The console is OpenETL's web app. It shows every pipeline, how fresh the data it delivers is, what each sync did, and anything that needs someone's attention. Anyone with an account can look around; what you can change depends on your [role](#what-your-role-lets-you-do).

If a word on this page is new, [Concepts](../get-started/concepts.md) explains it in a few lines.

## Sign in

Whoever runs OpenETL gives you two things: the console's address, and a link to set your password.

1. Open the link. The page is **Set your password**.
2. Choose a password of 15 to 256 characters. It can't be your email or a common password.
3. Tick **Keep me signed in on this device** if you want to stay signed in, and press **Set password and sign in**.

A link works once. If the page says the link has expired or was already used, ask an admin for a new one.

After that, the console's address shows **Sign in to OpenETL**: your **Email** and **Password**, then **Sign in**.

Your name is at the bottom of the sidebar. Its menu shows your roles, and has:

- **Theme**: **Light**, **Dark** or **Match the system**.
- **Rows**: **Comfortable** or **Compact**, for how much fits on a page.
- **Change password**. Changing it signs you out everywhere else, but not in this browser.
- **Sign out**.

> [!NOTE]
> A banner that says **Sign-in is off** means this OpenETL asks no one to sign in, and everyone who can reach it is an owner. That's how `openetld dev` runs on a laptop; a shared installation shouldn't.

## Find your way around

The sidebar has:

- **Pipelines**, the place to start.
- **Incidents**, with a count of the ones open.
- Under **Connections**: **Sources**, **Destinations**, **dbt projects**, **Clusters** and **Connectors**.
- Under **Settings**: **Webhooks**, **Secrets**, **Access** and **Audit log**, which only admins can read.

**Search**, at the top, or ⌘K on a Mac and Ctrl+K elsewhere, jumps to any pipeline or page. It also switches the theme and the row density, and signs you out.

## Pipelines

**Pipelines** lists every pipeline, the most urgent first. Search by name, or filter by **Status**, **Source** or **Destination**.

| Column | What it shows |
|---|---|
| **Pipeline** | Its name. Open it for its page. |
| **Status** | How its latest sync went, from the table below. A count of failed attempts shows while a sync is retrying, and a count of open incidents when it has any. |
| **Source**, **Destination** | Where it reads from and writes to. |
| **Schedule** | How often it syncs: **Every 1h**, a cron expression, **Continuous**, or **On demand** when it only syncs when someone starts one. |
| **Freshness** | When its data last changed: when its last successful sync ended, or for a continuous pipeline, when it last committed. For a pipeline that syncs every so often, it's highlighted once it's older than two of those intervals. |
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
- **Transform**, if its data goes through Spark or Flink on the way: which, and the cluster it runs on.
- **Schedule**, or **Paused**.
- **Last success**, or **Last commit** for a continuous pipeline, which is its freshness.
- **Behind source**, for a continuous pipeline: how far its reading was behind the source at its latest commit.
- **Size**: how much CPU and memory its syncs get, and why.
- **Version**: which version of the pipeline is current. Click it to see that version.

Below that, a strip shows its latest syncs, oldest on the left. Each one's color is its status, and its length grows with how long it took. Hover over one for its rows, or click it to open it.

### Sync now, Pause and Resume

- **Sync now** starts a sync straight away. It's greyed out while a sync is running, and isn't there while the pipeline is paused. Continuous pipelines don't have it, as they never stop.
- **Pause** stops a pipeline's scheduled syncs; a sync that's running carries on. Pausing a continuous pipeline stops it.
- **Resume** starts its schedule again.

These buttons show only if your role may use them.

### Overview

**Overview** starts with anything wrong: the pipeline's open incidents, a sync that's retrying after failed attempts, or a last sync that failed, with a link to read its log.

**Not as the spec says** follows when the pipeline runs differently from how it was set up, until someone undoes it:

- A stream marked **Paused** is skipped by syncs, which keep its place for when it's resumed. It says why, and links to the incident if heal paused it. **Resume stream** starts syncing it again.
- An entry marked **Changed by heal** is a setting heal changed, such as how often syncs start after the source kept limiting them, next to what the pipeline's definition has. **Use the spec's** goes back to the definition's value. The header's **Schedule** shows heal's interval while it applies.

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

When the pipeline holds anything back, **Holds** comes last, with an entry for each column. Each says whether the whole column or some of its cells are held, since when, and why, and what state it's in:

| State | Meaning |
|---|---|
| **Held** | Its values are kept until someone releases or discards them. |
| **Pending** | It was released or discarded, and waits for the pipeline's next run. |
| **Releasing** | Its held values are being written in. |
| **Release refused** | The last release didn't clear it, and won't be tried again until it's released again. |
| **Discarded** | Its values were dropped. The column stays as the destination has it. |

Under **To clear it**, each entry lists the ways out, likeliest first: the change to make to the destination, if there is one, and the command that releases or discards the hold.

### Syncs

**Syncs** lists every sync, newest first, with its **Status**, when it **Started**, how long it **Took**, its **Rows**, which **Version** of the pipeline it ran, and its **Error** if it failed. **Load older syncs** goes further back.

### Schemas

**Schemas** shows each stream's columns, with their **Type**, whether they allow nulls, and the source's name for any column that was renamed on the way. Every time a stream's columns change, it gets a new version, `v1`, `v2` and so on. Pick a version to see its columns, with each one marked **Added**, **Removed** or **Changed** against the version before it.

### Checkpoints

**Checkpoints** lists every commit the pipeline's syncs made over the last 30 days, newest first: when it was committed, how many rows it committed, and each stream's rows, along with anything quarantined, held or collapsed. Most people won't need it; it's there to show exactly what landed when.

### Versions

**Versions** lists every version of the pipeline, marking the **Current** one. Each shows the file it was **Applied from**, the versions of its source, destination and Spark or Flink clusters, and the whole pipeline as YAML, with **Copy YAML**. Applying an earlier version's YAML makes it current again.

## A sync's page

Open a sync from the strip or from **Syncs**. The top shows its status, when it **Started**, how long it **Took** (or has been **Running for**), its **Rows**, the pipeline's **Version**, where it ran (its **Data plane**), its **Size**, and the CPU and memory it **Used**. Each stream's rows follow, and **Why it failed** if it did.

A sync that needed more than one attempt lists them under **Attempts**, each with what it failed with.

**Log** is the sync's log, attempt by attempt. Show **All** of it, **Warnings and errors**, or only **Errors**, and use **Find in the log** to search it. While a sync runs, **Follow** keeps the newest lines in view.

## Incidents

An incident opens on a pipeline when something needs attention. **Incidents** lists every pipeline's, open ones first, with a summary, **Severity**, **State**, **Kind**, how many times it was **Seen**, and when it was **Last seen**. Filter by **State**, **Severity**, **Pipeline** or **Kind**.

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
| **Crash loop** | A continuous pipeline keeps failing without committing anything. |
| **Lagging** | A continuous pipeline has fallen too far behind its source. |
| **Retention at risk** | The pipeline's reading is so far behind that it has used half or more of what the source keeps for it, such as a database's log of changes. |
| **Quarantine** | A stream is holding back a large share of its rows. |
| **Held** | A stream is holding a column, or some of its cells, back. |
| **Checkpoint corrupt** | The checkpoint the pipeline would carry on from can't be read, or the pipeline restarted from the one before it. |

An incident's page has **What happened**, with a link to the sync's log; **Next steps**, which says what to do; **Heal**, what OpenETL did about it itself or proposes; and **Details**. Some incidents pause their pipeline, when syncing again would only fail the same way. Others change how it runs:

- **Quarantine** pauses the stream, so it holds back no more rows while the others carry on. A stream that can't stop on its own, such as one read with CDC, isn't paused, and **Next steps** says why. A stream that's the pipeline's only one pauses the pipeline. **Next steps** names the file the held-back rows are in, each with the reason it didn't fit.
- **Rate limited**, once 3 syncs in a row have failed this way, makes the source send requests more slowly, where it can, or syncs start less often. It does it again after every 3 more.

What they changed shows under **Not as the spec says** on the pipeline's **Overview**, where you can undo it. A pipeline's definition decides how far heal goes: it can propose these actions without taking them, or take none.

Most incidents resolve themselves once the pipeline makes progress again. When you fixed something in a way OpenETL can't see, **Resolve** closes it by hand, with an optional **Comment** that's kept with the resolution. If it happens again, a new incident opens.

## Sources, destinations and connectors

- **Sources** and **Destinations** list each one with its **Connector**, the connector's **Version**, the **Pipelines** that use it, and when it was **Updated**. A source's or destination's page shows its pipelines and its **Config**. Configs name secrets, and never show their values.
- **dbt projects** lists the dbt projects that run after pipelines' syncs, and under **Run after**, the pipelines each one runs after.
- **Clusters** lists the Spark and Flink clusters that pipelines' transforms run on, on its **Spark** and **Flink** tabs, with what each **Runs on** (such as **spark-submit**, **EMR on EC2** or **Kubernetes**) and the pipelines it's **Used by**. A cluster's page shows its **Settings**, with what applies to any that aren't set, the **Pipelines** that use it and whether each **Runs** it as a transform or after each sync, and the properties or configuration it gives every job. Settings name secrets, and never show their values.
- **Connectors** has a **Catalog** of the connector packages available to install, and the **Sources** they're listed from. Open a package for its details, and the command that applies it, which makes it available to sources and destinations.

## Settings

- **Webhooks** lists the addresses OpenETL sends events to, such as failed syncs and incidents, and whether each is **Enabled**, **Failing** or **Disabled**. A webhook's page lists its **Deliveries**, and has **Send test event**, **Disable** and **Enable**.
- **Secrets** lists the passwords and keys that configs name, and which sources and destinations each is **Used by**. No page shows a secret's value.
- **Access** lists the people and service accounts under **Principals**, with their roles, and the **Runner tokens** that let runners join. A person's page lists their API keys, and an admin can **Sign out everywhere** for them there.
- **Audit log** lists every change, and every read of a secret, with who made it. Only admins can read it.

## What your role lets you do

Each role includes the ones before it. A role is given for one workspace, or for the whole organization; admin and owner are only for the whole organization.

| Role | What it adds |
|---|---|
| Viewer | Sees everything in the console, except secrets' values, which no one sees. |
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
