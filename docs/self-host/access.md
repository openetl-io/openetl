---
title: Manage access and credentials
description: Add users and service accounts, scope their permissions, rotate keys and runner tokens, and remove access.
---

Use a user account for someone who signs in to the console, and a service account for an integration that calls the API. Give each the role it needs. [Roles](../reference/index.md#roles) lists the permissions; `admin` and `owner` apply to the whole org, while other roles can be scoped to a workspace.

Run these commands with an administrator's API key and the correct `OPENETL_API_URL`. An owner must manage other owners and their keys. Flags go before positional names. Commands that create credentials show their values once; `--out` writes a file readable only by you.

## Add a user

Create a user, grant access to the default workspace, and make a password link:

```sh
openetl principal create --user --email alex@example.com alex
openetl principal grant --workspace default alex operator
openetl principal password-link alex
```

Give the link to that user through your own trusted channel. It works once and expires after 24 hours by default; `password-link --expires 2h alex` selects another lifetime, up to 7 days. The link uses the installation's `OPENETL_PUBLIC_URL`, so set that before inviting people.

The user sets a password and signs in to the console. Under **Settings**, **Access**, check the principal and its role. `openetl principal list` shows the same accounts and bindings.

Leaving out `--workspace` grants a role on the org, covering every workspace. A viewer can read resources in their scope, logs and incidents included, but cannot read the audit log; that requires an admin. No user-facing API or console page returns stored secret values.

## Create a service account

Give an integration an identity of its own, rather than an owner's bootstrap key:

```sh
openetl principal create reporting
openetl principal grant --workspace default reporting viewer
openetl key create --expires 90d --out reporting.key reporting
```

`create` without `--user` creates a service account. Choose `operator` if it must start syncs, or `editor` if it must apply pipeline definitions. `--expires` is optional; a key without it does not expire.

Configure the integration to read `reporting.key`, or send its contents as `Authorization: Bearer KEY`. Verify the key's scope before using it:

```sh
openetl whoami --api-key-file reporting.key --workspace default
```

`OPENETL_API_KEY` takes precedence over `--api-key-file`. Unset it in a separate shell when testing a specific key file, or the command checks the environment's key instead.

## Rotate an API key

Create the replacement while the existing key still works:

```sh
openetl key create --expires 90d --out reporting-next.key reporting
openetl key list reporting
```

Update the integration's credential, then verify a real request with the new key. To test with the CLI, use `whoami --api-key-file reporting-next.key --workspace default` in a shell without `OPENETL_API_KEY`.

Once every client has switched, delete the old key using its ID from `key list`:

```sh
openetl key delete reporting OLD_KEY_ID
```

Deletion takes effect immediately. Keep an administrator credential available while rotating an integration's key, so you can issue another replacement if needed.

## Rotate a runner token

Runner tokens authenticate runners to one data plane. They are separate from user and service-account API keys:

```sh
openetl dataplane token create --out runner-next.token default
openetl dataplane token list default
```

Update every runner and launcher for that data plane, including the credentials passed to new job pods. With a token file, replace the file named by `OPENETL_RUNNER_TOKEN_FILE`; with a Kubernetes Secret, update `openetl-runner`. Restart the affected runners and launchers so they read the replacement. Let runners stop at a checkpoint as described in your [Compose](./compose.md#upgrade) or [Kubernetes](./kubernetes.md#upgrading) installation.

On the documented Compose stack, `.openetl/runner.token` lives on the shared runner volume. Its `runner-token` service only creates a token when that file is missing; it does not replace an existing one. From the directory containing `compose.yml`, write the replacement to that volume:

```sh
docker compose stop runner
docker compose run --rm --no-deps -T --entrypoint sh runner \
  -c 'umask 077; cat > .openetl/runner.token' < runner-next.token
docker compose up -d --wait runner
```

On Kubernetes, preserve any bucket credentials already in the `openetl-runner` Secret while replacing its token. Restart both the shared runner Deployment and the launcher after the update; new job pods must receive the replacement too.

Check that runners reconnect and a test pipeline makes progress, then revoke the old token:

```sh
openetl dataplane token delete default OLD_TOKEN_ID
```

Every runner still using the old token loses access immediately. Existing tokens remain valid until you delete them; creating a replacement alone does not revoke them.

## Remove access

Disable a principal to stop its API keys and end its console sessions:

```sh
openetl principal disable alex
```

`openetl principal enable alex` restores the account's ability to authenticate. To remove one role without disabling the account, revoke that binding:

```sh
openetl principal revoke --workspace default alex operator
```

To end console sessions while leaving the account and its API keys active:

```sh
openetl principal sign-out alex
```

Delete a principal only when you intend to remove it, its API keys and its role bindings: `openetl principal delete alex`. Removing a user's account does not rotate separate service-account keys or runner tokens they may have known; rotate those separately when needed.

## Check the audit trail

An administrator can read **Settings**, **Audit log**, or run:

```sh
openetl audit list
```

The log records changes and runner reads of secrets, with the acting principal or runner credential. If a request is unexpectedly denied, inspect the current identity with `openetl whoami --workspace default`. Check whether its role applies to the organization or the requested workspace.
