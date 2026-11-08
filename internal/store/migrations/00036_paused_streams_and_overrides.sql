-- +goose Up
-- Streams paused on their own. Syncs skip them and carry their state
-- forward. incident_id is the incident whose playbook paused one; null if
-- a person did.
CREATE TABLE paused_streams (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    stream      text NOT NULL,
    reason      text NOT NULL,
    incident_id uuid,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, stream),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Values heal set for fields of a pipeline's spec, which syncs use in place
-- of the spec's: field is schedule.every or source.rate_limit_scale, and
-- value its JSON, as a PlanChange has it.
CREATE TABLE pipeline_overrides (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    field       text NOT NULL,
    value       jsonb NOT NULL,
    reason      text NOT NULL,
    incident_id uuid,
    update_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, field),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE pipeline_overrides;
DROP TABLE paused_streams;
