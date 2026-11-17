---
title: Check connections and pipelines
description: Run a connection check or pipeline preflight and read the result.
---

Use a connection check to test one source or destination. Use a preflight to check a pipeline before it moves data.

You need the operator role or a higher role. [Console roles](./access.md#what-your-role-lets-you-do) lists permissions.

## Preflight

**Preflight** checks whether a pipeline can run. It does not read or write records.

1. Open the pipeline.
2. Select **Preflight**.
3. Wait for the result.

OpenETL checks the connections, plans the sync and asks each connector to check the pipeline's streams.

When it finishes, the panel says **The pipeline would run.** or **The pipeline wouldn't run as it is.** Then it shows each part, in the order it ran:

1. **Source NAME: connection** and **Destination NAME: connection**: the checks of the two connections.
2. **Sync plan**: the plan of the sync. Each problem of the plan is a failed item, and each warning is a warning item.
3. **Source NAME: pipeline's streams** and **Destination NAME: pipeline's streams**: each connector's checks of the streams the pipeline reads or writes. **Passed. The connector has no checks of its own for a pipeline.** means that the connector has no such checks.

Each part counts its items, such as **1 passed, 1 failed, 1 skipped.** [Read a check's items](#read-a-checks-items) explains the results.

A red **Failed:** box gives errors that the items do not fully explain. Examples include a connector that cannot start or a part that exceeds its time limit.

A part that did not run shows **Skipped:** and the reason. A part is skipped when a part before it failed:

- **Skipped: the source didn't start.** or **Skipped: the destination didn't start.**
- **Skipped: the source's check failed.** or **Skipped: the destination's check failed.**
- **Skipped: a sync can't be planned.** The **Sync plan** failed, so the connectors cannot check the pipeline's streams.

A skipped part means that the pipeline would not run. Correct the part that failed first.

Select **Run again** after you correct something. If the preflight cannot run, for example because no runner takes it, the panel shows **Couldn't run the preflight** and the reason. That says nothing about the pipeline itself.

## Check a source or destination

A [check](../get-started/concepts.md#checks-and-preflights) tests one connection with its saved configuration.

1. Open the source or destination.
2. Select **Check**.
3. Wait for **Checking…** to finish.

The result appears in **Check**. Its first line gives the verdict and counts the items. For example: **The check passed: 4 passed, 1 warning.**

A warning does not fail the check. [Read a check's items](#read-a-checks-items) explains the individual results.

The **Check** section can also show:

- **The check passed** with **The connector reached it with this config. It doesn't say what it looked at.** The connector does not report items.
- A red **The check failed** box gives errors that the items do not fully explain. Some connectors report an error without individual items.
- **Couldn't run the check**, with the reason. The check did not run. For example, no runner took it, or the connector did not start or did not answer within 1 minute. That says nothing about the connection.

## Read a check's items

Each item of a check or a preflight is one row:

- An icon gives its status: a check mark for passed, a triangle for a warning, a cross for failed, and a minus for skipped.
- Its title says what the connector checked. Values from your config, such as a host or a table, are in a monospace font.
- A badge gives its kind, as the table below shows.
- Its message explains the result or the reason for a skipped check.
- If a description is available, **What it checks** explains the check's purpose. This button uses an information icon.

A failed or warning item can also show:

| Label | What it shows |
|---|---|
| **Detail** | The cause, in the words of the system the connector connects to, such as a database's error message |
| **Fix:** | What to do to correct it |
| **Field** | The config field it concerns, such as `consumer.name` |
| **Code** | The error code, such as `PERMISSION_DENIED` |
| **Reason** | The kind of failure, such as `credentials_rejected` |

| Kind | What the item verifies |
|---|---|
| **Configuration** | The config's values work together. |
| **Connection** | The network path: the name resolves, the port answers, and TLS works. |
| **Authentication** | The system accepts the credentials. |
| **Permission** | The account can do what the connector needs, such as read a table. |
| **Resource** | Something that the config names exists, such as a database, a bucket or a topic. |
| **Compatibility** | The system's version or settings work with the connector. |
| **Quota** | Limits, such as rate limits, leave room for the connector. |
| **Custom** | Something else that the connector checks. |

To correct a failed item, follow its **Fix:**. If you cannot make the change, contact the person responsible for the connection. Give them the item's **Field**, **Code** and **Detail**.
