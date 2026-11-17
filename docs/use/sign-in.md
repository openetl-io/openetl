---
title: Sign in to the console
description: Sign in, change your password and choose console display settings.
---

Use the console address and password link from your OpenETL administrator.

## Sign in

Before you start, get the console address and a password link from your administrator.

1. Open the link. The page is **Set your password**.
2. Choose a password of 15 to 256 characters. Do not use your email address or a common password.
3. If you want a persistent session, select **Keep me signed in on this device**.
4. Select **Set password and sign in**.

A link works once. If the link has expired or was already used, ask an administrator for a new link.

For later visits:

1. Open the console address.
2. On **Sign in to OpenETL**, enter your **Email**.
3. Enter your **Password**.
4. Select **Sign in**.

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
- Under **Settings**: **Webhooks**, **Secrets**, **Access** and **Audit log**. Only administrators can read the audit log.

Select **Search** to find a pipeline or page. The keyboard shortcut is ⌘K on macOS or Ctrl+K elsewhere. The search menu also provides theme, row density and sign-out actions.

Next, [check data freshness](./freshness.md).
