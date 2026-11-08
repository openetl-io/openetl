-- +goose Up
-- What a person asked of a pipeline's checkpoint log, for its next attempt
-- to take: reading streams again from the start (reset), or restarting from
-- the checkpoint before one that can't be read (rewind).
CREATE TABLE checkpoint_requests (
    org_id      uuid NOT NULL,
    -- A UUIDv7, so IDs order requests by creation.
    id          uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('reset', 'rewind')),
    -- A reset's streams; empty for every stream.
    streams     text[] NOT NULL DEFAULT '{}',
    principal   text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    -- When an attempt was seen to take it, or for a rewind, when the
    -- pipeline made progress without needing it; null while pending.
    take_time   timestamptz,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE INDEX checkpoint_requests_pending ON checkpoint_requests (org_id, pipeline_id, id) WHERE take_time IS NULL;

-- When the control plane last asked a pipeline's source how much of its
-- retention the pipeline has used, while it wasn't running.
ALTER TABLE pipelines ADD COLUMN retention_probe_time timestamptz;

-- +goose Down
ALTER TABLE pipelines DROP COLUMN retention_probe_time;
DROP TABLE checkpoint_requests;
