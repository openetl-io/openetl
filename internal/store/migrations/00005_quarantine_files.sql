-- +goose Up
-- The quarantine files of a pipeline's committed epochs: where each is and
-- how many of its rows each cause quarantined, never what they hold. Kept
-- until the pipeline's quarantine.retention drops them, or the pipeline is
-- deleted.
CREATE TABLE quarantine_files (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    -- Below the pipeline's _quarantine directory, such as 3/17/orders.jsonl.
    path        text NOT NULL,
    -- Where it is in the data plane's object storage.
    url         text NOT NULL,
    stream      text NOT NULL,
    -- stream is one of a transform's outputs.
    output      boolean NOT NULL DEFAULT false,
    -- The checkpoint that committed it, and its sync's workflow run
    -- (syncs.run_id).
    generation  bigint NOT NULL,
    epoch       bigint NOT NULL,
    run_id      text NOT NULL,
    commit_time timestamptz NOT NULL,
    rows        bigint NOT NULL,
    -- [{code, column, not_held, rows}], the most rows first.
    causes      jsonb NOT NULL DEFAULT '[]',
    PRIMARY KEY (org_id, pipeline_id, path),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE INDEX quarantine_files_by_time ON quarantine_files (org_id, pipeline_id, commit_time DESC, path DESC);

-- +goose Down
DROP TABLE quarantine_files;
