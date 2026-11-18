---
title: Console settings and permissions
description: Find your role, available actions and settings that require an administrator.
---

Your role determines which resources you can inspect and which actions you can use.

## Settings

| Page | What you can inspect |
|---|---|
| **Webhooks** | Event receiver addresses and their **Enabled**, **Failing** or **Disabled** status. Open a webhook to inspect **Deliveries**. |
| **Secrets** | Credential names and connections listed under **Used by**. Secret values remain hidden. |
| **Access** | People and service accounts under **Principals**, their roles, and **Runner tokens**. A person's page also lists API keys. |
| **Audit log** | Changes and secret reads, with the identity responsible for each action. Only administrators can read it. |

A webhook has **Send test event**, **Disable** and **Enable** actions. An administrator can select **Sign out everywhere** on a person's page.

[Manage access and credentials](../self-host/access.md) covers adding users and service accounts, rotating credentials and removing access.

## What your role lets you do

Each role includes the permissions of the preceding roles. Roles can apply to one workspace or the whole organization. Admin and owner roles apply to the whole organization.

| Role | What it adds |
|---|---|
| Viewer | Reads resources in their role's scope, including sync logs and incidents. The audit log requires Admin; no role sees secret values. |
| Operator | **Sync now**, **Pause**, **Resume**, **Preflight** and connection **Check**. Resolve incidents and test, disable or enable webhooks. |
| Editor | Read quarantined records with **Show records**. Use the CLI to change resource definitions, secrets and held data. |
| Admin | Read the audit log and sign users out. Use the CLI to install connectors and manage access. |
| Owner | Manage other owners through the CLI. |

[Roles](../reference/index.md#roles) lists the complete API and CLI permissions. These include resizing, source discovery, stream resets and checkpoint recovery.

## What the console doesn't do

Some changes are made with the `openetl` command instead, by someone whose role allows them:

- **Creating or changing pipelines**, sources, destinations, dbt projects, clusters and webhooks. **New pipeline** shows the commands that do it. Pipelines are YAML files: see [Pipelines](../pipelines/index.md).
- **Setting secrets.**
- **Releasing or discarding holds.** Each hold shows the command under **To clear it**.
- **Adding people, giving roles and making API keys.**
