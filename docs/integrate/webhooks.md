---
title: Receive and verify webhooks
description: Verify event signatures, prevent duplicate processing, inspect delivery failures and rotate webhook signing keys.
---

Use a webhook to send selected platform events to your HTTP receiver. The server sends a signed JSON `POST`. Your receiver verifies the request, saves the event and returns a success status.

Create the resource with [Events and webhooks](./index.md#events-and-webhooks). Use the [Webhook reference](../reference/specs/webhook.md) for filters and required fields. The steps below use the `default` workspace and the example webhook name `ops-alerts`.

## Prepare the receiver

The receiver must be reachable from the API server. Public addresses are allowed by default. For a private endpoint, configure the server's `--egress-allow` with the required CIDR range. A blocked address fails delivery. Redirects are not followed.

Give the receiver the same signing key that you store in OpenETL. A key is `whsec_` followed by the standard base64 encoding of 24 to 64 random bytes. Store its value with `openetl secret set`. Webhook signing keys cannot use `--ref`; only runners read cloud-secret references.

Read the raw request body before JSON parsing. Preserve its bytes. The request has these headers:

| Header | Meaning |
|---|---|
| `webhook-id` | The event ID. Repeated deliveries of that event have the same ID. |
| `webhook-timestamp` | The delivery attempt time as Unix seconds. A retry has a new timestamp. |
| `webhook-signature` | One or more space-separated signatures. Each has the form `v1,BASE64_SIGNATURE`. |

The JSON body has `type`, `timestamp` and `data`. The body's `timestamp` is the event time. It can be older than the delivery attempt time. [Platform events](../reference/events.md) defines the data fields for each event type.

## Verify the signature

You can use a Standard Webhooks verification library. Configure it with your signing key and a timestamp tolerance. Keep the receiver's clock synchronized.

The following Python function uses the standard library. It accepts a bytes body and a header mapping with case-insensitive lookup or lowercase names. It rejects timestamps more than 5 minutes from the receiver's clock. This tolerance is a receiver policy.

```python
import base64
import hashlib
import hmac
import time


def verify_webhook(secret, headers, body):
    if not secret.startswith("whsec_"):
        raise ValueError("Invalid signing key prefix")
    key = base64.b64decode(secret[6:], validate=True)
    if not 24 <= len(key) <= 64:
        raise ValueError("Invalid signing key length")

    event_id = headers["webhook-id"]
    timestamp = headers["webhook-timestamp"]
    if not event_id or abs(time.time() - int(timestamp)) > 300:
        raise ValueError("Missing event ID or expired timestamp")

    message = event_id.encode() + b"." + timestamp.encode() + b"." + body
    digest = hmac.new(key, message, hashlib.sha256).digest()
    expected = base64.b64encode(digest).decode("ascii")
    for signature in headers["webhook-signature"].split():
        version, separator, value = signature.partition(",")
        if version == "v1" and separator and hmac.compare_digest(value, expected):
            return event_id
    raise ValueError("No matching webhook signature")
```

Call the function before you parse or process the event. Treat missing headers, invalid encodings and verification errors as a rejected request. Do not log signing keys. With more than one configured key, a matching signature from a trusted key is sufficient.

## Save the event before acknowledging it

After verification:

1. Parse the JSON and check its event type.
2. Save the event to a durable queue or store. Use `webhook-id` as a unique key to prevent duplicate processing.
3. Return a `2xx` status within 15 seconds. Return a `2xx` status for an event already saved.
4. Process the saved event separately. Keep the event ID through downstream work so a retry cannot repeat its effects.

Do not return success before you have saved the event. A response can be lost after processing, so OpenETL can deliver an event more than once. Do not depend on delivery order.

## Send and inspect a test event

Use an API key with the `operator` role or higher:

```sh
openetl webhook test ops-alerts
openetl webhook deliveries ops-alerts
```

The test command creates a `webhook.test` event for this webhook even if its filters do not select that type. The command's `sent` message identifies the queued delivery. It does not confirm receipt. Wait until the delivery state is `succeeded`, then confirm that the receiver saved the event.

To inspect response details as one JSON object per delivery:

```sh
openetl webhook deliveries --json ops-alerts
```

The list includes attempts, the latest HTTP status, error and response details. Events and their delivery records are retained for 7 days.

## Diagnose a failed delivery

| Result | OpenETL behavior | Receiver action |
|---|---|---|
| `2xx` | Mark the delivery as succeeded. | Check your queue if downstream work is missing. |
| Timeout, connection error, `408`, `429` or `5xx` | Retry while attempts remain. | Restore the endpoint. Use `429` or `503` for a temporary inability to save events. |
| `3xx` | Fail the delivery without following the redirect. | Configure the final URL in the webhook. |
| `410 Gone` | Fail the delivery and disable the webhook. | Restore or replace the endpoint, then enable the webhook. |
| Other non-success status | Fail the delivery without retry. | Correct routing, authentication or request validation. |
| Blocked address or unreadable signing key | Record an error. A blocked address is not retried; a key error is retried. | Correct the server's network policy or stored signing key. |

The default retry waits are 5 seconds, 5 minutes, 30 minutes, 2 hours, 5 hours, 10 hours and 10 hours. Each wait varies by up to 10 percent. `Retry-After` can extend a wait, up to 10 hours. It accepts seconds or an HTTP date.

OpenETL also disables a webhook after 3 days of endpoint failures without a success or a gap of at least 24 hours between failures. Signing-key errors do not count toward this limit.

Inspect the state and disabled reason:

```sh
openetl webhook list
```

After repair, enable the webhook and send a new test event:

```sh
openetl webhook enable ops-alerts
openetl webhook test ops-alerts
```

Disabling a webhook fails its pending deliveries. Enabling it does not replay those deliveries or events missed while it was disabled. Reconcile affected pipeline state through the API if you need to recover missed notifications.

## Rotate a signing key

Use an `editor` or higher role to change the webhook and its secrets.

1. Generate a replacement key. Store it under a new OpenETL secret name. Give the receiver the replacement through your credential distribution process.
2. Keep the old key trusted by the receiver. Add the new secret name to the webhook's `spec.secrets` and apply the resource. Each request now has signatures for both keys.
3. Verify a test delivery using the new key.
4. Remove the old secret name from `spec.secrets` and apply the resource again.
5. After requests signed with the old key have finished, remove it from the receiver. Delete the unused OpenETL secret.

Pending delivery attempts use the webhook's current configuration and signing keys. A webhook can name up to three distinct signing secrets.
