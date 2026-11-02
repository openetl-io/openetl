-- name: GetWebhook :one
SELECT * FROM webhooks WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: LockWebhook :one
SELECT * FROM webhooks WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR UPDATE;

-- name: InsertWebhook :exec
INSERT INTO webhooks (org_id, workspace_id, id, name, spec, version, secrets, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, @secrets, 1);

-- name: UpdateWebhook :exec
UPDATE webhooks SET spec = @spec, version = @version, secrets = @secrets, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: ListWebhooks :many
SELECT * FROM webhooks
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: DeleteWebhook :exec
DELETE FROM webhooks WHERE org_id = @org_id AND id = @id;

-- Disabling fails the webhook's pending deliveries.
-- name: DisableWebhook :exec
WITH w AS (
    UPDATE webhooks SET disabled = true, disabled_reason = @reason, revision = revision + 1, update_time = now()
    WHERE webhooks.org_id = @org_id AND webhooks.id = @id AND NOT disabled
    RETURNING webhooks.org_id, webhooks.id
)
UPDATE webhook_deliveries d SET state = 'failed', error = 'the webhook was disabled: ' || @reason
FROM w WHERE d.org_id = w.org_id AND d.webhook_id = w.id AND d.state = 'pending';

-- name: EnableWebhook :exec
UPDATE webhooks SET disabled = false, disabled_reason = NULL, failing_since = NULL, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id AND disabled;

-- The webhooks whose signing secrets include any of the names.
-- name: ListWebhooksUsingSecrets :many
SELECT name, secrets FROM webhooks
WHERE org_id = @org_id AND workspace_id = @workspace_id AND secrets && @names::text[]
ORDER BY name;

-- name: InsertEvent :exec
INSERT INTO outbox (org_id, id, workspace_id, type, pipeline, data, key, webhook_id)
VALUES (@org_id, @id, @workspace_id, @type, @pipeline, @data, @key, @webhook_id)
ON CONFLICT (org_id, key) DO NOTHING;

-- Events not yet fanned out, oldest first. Notifiers skip each other's.
-- name: ClaimEvents :many
SELECT * FROM outbox
WHERE org_id = @org_id AND dispatch_time IS NULL
ORDER BY create_time, id
LIMIT @max_rows
FOR UPDATE SKIP LOCKED;

-- name: MarkEventsDispatched :exec
UPDATE outbox SET dispatch_time = now() WHERE org_id = @org_id AND id = ANY(@ids::uuid[]);

-- name: ListEnabledWebhooks :many
SELECT * FROM webhooks WHERE org_id = @org_id AND workspace_id = ANY(@workspaces::uuid[]) AND NOT disabled;

-- name: InsertDelivery :exec
INSERT INTO webhook_deliveries (org_id, id, webhook_id, event_id)
VALUES (@org_id, @id, @webhook_id, @event_id)
ON CONFLICT (org_id, webhook_id, event_id) DO NOTHING;

-- Due deliveries of enabled webhooks, oldest due first, each leased for
-- @lease_seconds so no other notifier takes it meanwhile. Times are the
-- database's, so notifiers' clocks don't matter.
-- name: ClaimDeliveries :many
WITH due AS (
    SELECT d.org_id, d.id FROM webhook_deliveries d
    JOIN webhooks w ON w.org_id = d.org_id AND w.id = d.webhook_id
    WHERE d.org_id = @org_id AND d.state = 'pending' AND d.next_attempt_time <= now() AND NOT w.disabled
    ORDER BY d.next_attempt_time
    LIMIT @max_rows
    FOR UPDATE OF d SKIP LOCKED
)
UPDATE webhook_deliveries d SET next_attempt_time = now() + make_interval(secs => @lease_seconds::float8)
FROM due, webhooks w, outbox e
WHERE d.org_id = due.org_id AND d.id = due.id
  AND w.org_id = d.org_id AND w.id = d.webhook_id
  AND e.org_id = d.org_id AND e.id = d.event_id
RETURNING d.id, d.attempts, d.webhook_id, w.workspace_id, w.spec AS webhook_spec, w.failing_since,
          e.id AS event_id, e.type AS event_type, e.data AS event_data, e.create_time AS event_time;

-- Records an attempt, unless another notifier claimed the delivery after
-- this one's lease ran out and recorded its own.
-- name: RecordAttempt :execrows
UPDATE webhook_deliveries SET
    state = @state, attempts = attempts + 1, next_attempt_time = now() + make_interval(secs => @wait_seconds::float8), last_attempt_time = now(),
    response_status = @response_status, response_body = @response_body, error = @error, latency_ms = @latency_ms
WHERE org_id = @org_id AND id = @id AND attempts = @attempts;

-- name: WebhookSucceeded :exec
UPDATE webhooks SET failing_since = NULL WHERE org_id = @org_id AND id = @id AND failing_since IS NOT NULL;

-- How long the webhook has been failing.
-- name: WebhookFailed :one
UPDATE webhooks SET failing_since = coalesce(failing_since, now())
WHERE org_id = @org_id AND id = @id
RETURNING extract(epoch FROM now() - failing_since)::float8 AS failing_seconds;

-- name: ListWebhookDeliveries :many
-- Newest first, after (before_time, before_id) if given.
SELECT d.*, e.type AS event_type FROM webhook_deliveries d
JOIN outbox e ON e.org_id = d.org_id AND e.id = d.event_id
WHERE d.org_id = @org_id AND d.webhook_id = @webhook_id
  AND (sqlc.narg('before_time')::timestamptz IS NULL OR (d.create_time, d.id) < (sqlc.narg('before_time'), @before_id::uuid))
ORDER BY d.create_time DESC, d.id DESC
LIMIT @max_rows;

-- Events and their deliveries older than @before.
-- name: PruneEvents :execrows
DELETE FROM outbox WHERE org_id = @org_id AND create_time < @before;
