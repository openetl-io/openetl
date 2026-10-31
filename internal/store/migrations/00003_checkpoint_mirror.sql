-- +goose Up
-- A mirror of each pipeline's checkpoint log: one row per record, reported
-- by runners after each commit. Partitioned by day of commit_time; the
-- store creates each day's partition when a report first needs it.
CREATE TABLE checkpoint_mirror (
    org_id              uuid NOT NULL,
    pipeline_id         uuid NOT NULL,
    epoch               bigint NOT NULL,
    generation          bigint NOT NULL,
    -- The record before it in the log; null for the first.
    previous_epoch      bigint,
    previous_generation bigint,
    -- The sync's Temporal run (syncs.run_id).
    run_id              text NOT NULL,
    -- When the data plane recorded it.
    commit_time         timestamptz NOT NULL,
    -- [{name, rows, quarantined_rows, forced, complete, schema_digest}];
    -- schema_digest joins stream_schemas.digest.
    streams             jsonb NOT NULL,
    create_time         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, epoch, generation, commit_time),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
) PARTITION BY RANGE (commit_time);

-- +goose Down
DROP TABLE checkpoint_mirror;
