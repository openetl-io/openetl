---
title: Webhook
description: Every field of a Webhook document, including required signing secrets and optional pipeline filters.
---

A `Webhook` sends selected platform events to your HTTP endpoint. It belongs to a workspace. [Events and webhooks](../../integrate/index.md#events-and-webhooks) covers resource setup. [Receive and verify webhooks](../../integrate/webhooks.md) covers signatures, retries and key rotation.

## The document

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `apiVersion` | string | Yes | — | `openetl.io/v1alpha1` |
| `kind` | string | Yes | — | `Webhook` |
| `metadata.name` | name | Yes | — | Lowercase letters, digits and hyphens, at most 63 characters, starting and ending with a letter or digit |
| `metadata.workspace` | name | No | The CLI's `--workspace`, otherwise `default` | The workspace the resource belongs to |
| `spec` | object | Yes | — | The fields below |

Fields that aren't listed are refused. **Required** applies within the containing object; an optional object may be omitted, but its required children must be set when you include it. **Default** describes what omission means.

## spec

| Field | Type | Required | Default | Meaning |
|---|---|---|---|---|
| `url` | HTTP or HTTPS URL | Yes | — | The receiving endpoint. It must have a host and no username or password in the URL. |
| `events` | nonempty list of strings | Yes | — | Event types or globs, such as `sync.failed`, `schema.*` or `*`. See [platform events](../index.md#platform-events). |
| `pipelines` | list of names | No | All pipelines and workspace events | Send only events for these pipeline names. Names follow the same rules as `metadata.name`. |
| `secrets` | list of secret names | Yes | — | One to three distinct secrets in the same workspace. Each holds a signing key: `whsec_` followed by standard base64 encoding of 24 to 64 key bytes. Cloud-secret references are not supported. All configured keys sign each delivery, allowing rotation. |

The secrets must exist when you apply. Outbound requests are subject to the server's [network settings](../../self-host/index.md#the-settings-every-installation-sets). A receiver must verify the signature and deduplicate by `webhook-id`.

```yaml
apiVersion: openetl.io/v1alpha1
kind: Webhook
metadata:
  name: ops-alerts
spec:
  url: https://hooks.example.com/openetl
  events: ["sync.failed", "incident.*"]
  secrets: [ops-alerts-key]
```

[`examples/webhook.yaml`](../../../examples/webhook.yaml) is a complete document you can adapt. Create its signing secret before applying it.
