-- +goose Up
-- Failures and conditions that need more than a retry, one per pipeline,
-- stream and fingerprint while open. The fingerprint leaves the pipeline out,
-- so the same one on many pipelines points at their connector.
CREATE TABLE incidents (
    org_id          uuid NOT NULL,
    id              uuid NOT NULL,
    workspace_id    uuid NOT NULL,
    pipeline_id     uuid NOT NULL,
    -- Empty: the whole pipeline.
    stream          text NOT NULL DEFAULT '',
    -- sync_failed, auth, schema_blocked, rate_limited, position_lost,
    -- crash_loop, lagging, retention_at_risk or quarantine.
    class           text NOT NULL,
    -- The taxonomy code of the error it came from, if any.
    code            text,
    fingerprint     text NOT NULL,
    -- open or resolved.
    status          text NOT NULL DEFAULT 'open',
    -- warning or critical.
    severity        text NOT NULL,
    summary         text NOT NULL,
    message         text NOT NULL,
    next_steps      text NOT NULL DEFAULT '',
    -- What it was seen with last: lag, shares and the like.
    details         jsonb NOT NULL DEFAULT '{}',
    -- [{kind, state, description, time}]: what heal did, or proposes.
    actions         jsonb NOT NULL DEFAULT '[]',
    -- The sync or run it was seen in last.
    sync_id         uuid,
    -- What made the last occurrence; a repeat with the same key isn't one.
    occurrence_key  text NOT NULL,
    occurrences     integer NOT NULL DEFAULT 1,
    first_seen_time timestamptz NOT NULL DEFAULT now(),
    last_seen_time  timestamptz NOT NULL DEFAULT now(),
    resolve_time    timestamptz,
    resolution      text,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);
CREATE UNIQUE INDEX incidents_open ON incidents (org_id, pipeline_id, stream, fingerprint) WHERE status = 'open';
CREATE INDEX incidents_by_pipeline ON incidents (org_id, pipeline_id, first_seen_time DESC, id DESC);
CREATE INDEX incidents_by_workspace ON incidents (org_id, workspace_id, first_seen_time DESC, id DESC);

-- A sync's or run's attempts that failed, as runners report them. A
-- continuous run's failures since its last commit tell a crash loop.
ALTER TABLE syncs
    ADD COLUMN failed_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN last_failed_attempt integer NOT NULL DEFAULT 0,
    ADD COLUMN failures_since_commit integer NOT NULL DEFAULT 0,
    ADD COLUMN last_attempt_error text,
    ADD COLUMN last_attempt_code text,
    ADD COLUMN last_attempt_time timestamptz;

-- +goose Down
ALTER TABLE syncs
    DROP COLUMN failed_attempts,
    DROP COLUMN last_failed_attempt,
    DROP COLUMN failures_since_commit,
    DROP COLUMN last_attempt_error,
    DROP COLUMN last_attempt_code,
    DROP COLUMN last_attempt_time;
DROP TABLE incidents;
