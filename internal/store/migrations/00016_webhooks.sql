-- +goose Up
-- Where a workspace's platform events go. spec is the document's WebhookSpec;
-- secrets are the ones it signs with, which can't be deleted while it names
-- them.
CREATE TABLE webhooks (
    org_id          uuid NOT NULL,
    workspace_id    uuid NOT NULL,
    id              uuid NOT NULL,
    name            text NOT NULL,
    spec            jsonb NOT NULL,
    version         text NOT NULL,
    secrets         text[] NOT NULL DEFAULT '{}',
    revision        bigint NOT NULL,
    disabled        boolean NOT NULL DEFAULT false,
    disabled_reason text,
    failing_since   timestamptz,
    create_time     timestamptz NOT NULL DEFAULT now(),
    update_time     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Platform events, each written in the transaction that causes it. The
-- notifier fans each out to the webhooks that match it once, and sets
-- dispatch_time. A key, where set, records an event once however often the
-- code that writes it is retried. A webhook_id sends it to that webhook alone.
CREATE TABLE outbox (
    org_id        uuid NOT NULL REFERENCES orgs (id),
    id            uuid NOT NULL,
    workspace_id  uuid NOT NULL,
    type          text NOT NULL,
    pipeline      text,
    data          jsonb NOT NULL,
    key           text,
    webhook_id    uuid,
    create_time   timestamptz NOT NULL DEFAULT now(),
    dispatch_time timestamptz,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, key),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id),
    FOREIGN KEY (org_id, webhook_id) REFERENCES webhooks (org_id, id) ON DELETE CASCADE
);
CREATE INDEX outbox_undispatched ON outbox (org_id, create_time) WHERE dispatch_time IS NULL;
CREATE INDEX outbox_create_time ON outbox (org_id, create_time);

-- One event's delivery to one webhook, with its latest attempt. A notifier
-- claims a pending one by moving next_attempt_time past its lease.
CREATE TABLE webhook_deliveries (
    org_id            uuid NOT NULL,
    id                uuid NOT NULL,
    webhook_id        uuid NOT NULL,
    event_id          uuid NOT NULL,
    -- pending, succeeded or failed.
    state             text NOT NULL DEFAULT 'pending',
    attempts          integer NOT NULL DEFAULT 0,
    next_attempt_time timestamptz NOT NULL DEFAULT now(),
    last_attempt_time timestamptz,
    response_status   integer,
    response_body     text,
    error             text,
    latency_ms        integer,
    create_time       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, webhook_id, event_id),
    FOREIGN KEY (org_id, webhook_id) REFERENCES webhooks (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, event_id) REFERENCES outbox (org_id, id) ON DELETE CASCADE
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (org_id, next_attempt_time) WHERE state = 'pending';
CREATE INDEX webhook_deliveries_by_webhook ON webhook_deliveries (org_id, webhook_id, create_time DESC, id DESC);

-- Since when a continuous pipeline's read has been further behind its source
-- than the server's --lagging-after; unset while it isn't.
ALTER TABLE pipelines ADD COLUMN lagging_since timestamptz;

-- +goose Down
ALTER TABLE pipelines DROP COLUMN lagging_since;
DROP TABLE webhook_deliveries;
DROP TABLE outbox;
DROP TABLE webhooks;
