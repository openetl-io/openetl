---
title: Manage connection secrets
description: Store connection credentials, use cloud-secret references, replace values and remove unused secrets.
---

Store a connection credential as a named secret. A source or destination refers to that name in its config. You can replace the credential without putting its value in the resource document.

Use an API key with the `editor` role or higher in the target workspace. Set the API URL and credentials as described in [Manage access and credentials](./access.md). The commands below use the `default` workspace. For another workspace, add `--workspace WORKSPACE` before the secret name.

## Choose where to store the value

| Method | What OpenETL stores | Who reads the value |
|---|---|---|
| Standard input or `--from-file` | An encrypted value in the metadata database | The API server decrypts it for an authorized runner operation. |
| `--ref` | A cloud-secret reference | The runner reads the cloud store with its own identity. |

Use stored values for webhook signing keys and private connector catalog credentials. The control plane reads these credentials itself. It cannot use a cloud-secret reference for them.

The server needs its [secrets key](../reference/openetld.md#secrets-and-keys) to store and decrypt values. Keep that key with your [backup](./backup-and-restore.md). Replacing a connection credential does not require changing the server's secrets key.

## Store a value

To enter a value without displaying it in a terminal:

```sh
openetl secret set warehouse-password
```

Enter the value at the prompt. With redirected input, the command reads standard input without a prompt. To read a file instead:

```sh
openetl secret set --from-file PASSWORD_FILE warehouse-password
```

Replace `PASSWORD_FILE` with the path to your credential file. Keep the file readable only by the account that runs the command.

`set` creates the secret if it does not exist. Otherwise, it replaces the current value or reference. The value must be nonempty and at most 64 KiB. The CLI removes one final newline and its preceding carriage return, if present.

Apply the connection after you create its secret. Use the secret name in the field that the connector marks as secret. For example, a connection config can contain `password: {secret: warehouse-password}`. The secret must exist in the connection's workspace.

The [first pipeline tutorial](../pipelines/first-pipeline.md#2-store-the-connections-as-secrets) shows a complete connection example. The connector's documentation defines which fields require secret references.

## Use a cloud-secret reference

Give the runner access to the cloud secret before you configure the reference. Include runners launched as job pods. Credentials on the CLI's machine do not give a runner access.

For example, to select the `password` field of an AWS Secrets Manager JSON value:

```sh
openetl secret set --ref 'aws-sm://production/warehouse?region=us-east-1&field=password' warehouse-password
```

Replace the cloud path and region with your own values. Quote references that contain `&` so the shell passes the complete reference to OpenETL. `--ref` and `--from-file` are mutually exclusive. `--ref` does not read standard input.

OpenETL accepts these reference forms:

| Store | Reference form | Version selection |
|---|---|---|
| AWS Secrets Manager | `aws-sm://NAME_OR_ARN?region=REGION` | Optional `version` selects a version ID; optional `stage` selects a staging label. Without either, the store selects the current version. |
| GCP Secret Manager | `gcp-sm://projects/PROJECT/secrets/SECRET/versions/VERSION` | Omit `/versions/VERSION` to use `latest`. Alternatively, supply `?version=VERSION`. Do not supply both forms. |
| GCP regional secret | `gcp-sm://projects/PROJECT/locations/LOCATION/secrets/SECRET/versions/VERSION` | The same version rules apply. |
| Azure Key Vault | `azure-kv://VAULT/SECRET` | Optional `version` selects a version. Omit it to use the latest version. |

For AWS, an ARN supplies its region. Without an ARN or `region`, the runner's AWS configuration must supply the region. For Azure, `VAULT` can be a vault name or a Key Vault hostname in a supported Azure cloud.

Every form accepts `field` to select one top-level field of a JSON object. It does not select nested paths. A string field becomes its text value. Other non-null fields become JSON text. A missing or null field fails the read. Without `field`, the value must be UTF-8 text. The runner removes one final newline and its preceding carriage return from that text, as the CLI does. A field's value is used without changes.

The runner uses the AWS SDK's default credentials, Google's application default credentials, or Azure's default credential. It needs permission to read the selected secret version and any required decryption permissions.

Creating the OpenETL secret validates the reference syntax. It does not prove that the runner can read the cloud secret. After applying the connection, run its source or destination check from [the pipeline CLI](../reference/pipeline-cli.md#check-connections-and-discover-streams).

## Check what uses a secret

```sh
openetl secret list
```

The list shows the name, update time and current users of each secret. The `VALUE` column shows `sealed` for a stored value, or the cloud reference. It does not display the credential value. User-facing API responses do not return stored values either.

## Replace a credential

1. Create a replacement credential in the source system. Keep the old credential valid while you test the replacement.
2. Run `openetl secret set` with the same OpenETL secret name and the replacement value, file or reference.
3. Run the connection check on the pipeline's data plane. Confirm that the next sync or attempt can connect and commit.
4. Revoke the old credential after all consumers have switched.

A running connector can retain credentials it already read. Updating a secret does not update an existing connection in place. For a continuous pipeline, verify a new attempt with the replacement before revoking the old credential.

## Remove an unused secret

Remove references from current resource definitions before deleting the secret. Apply those definitions, then check the secret's users again.

```sh
openetl secret delete warehouse-password
```

Deletion fails if a current source, destination, webhook, Spark cluster, Flink cluster or pipeline still names the secret. Also check private [connector sources](../reference/specs/connector-source.md): catalog credentials can use secrets outside the connection's workspace. Keep credentials needed by retained versions until you no longer need those versions to run.

Deleting an OpenETL secret does not revoke the credential in the source system or delete a value from a cloud secret store.
