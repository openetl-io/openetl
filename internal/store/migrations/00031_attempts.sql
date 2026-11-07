-- +goose Up
-- Each attempt of a sync or a continuous run: what it was sized at and why,
-- and what it used. Dispatches are gone a day after they finish;
-- sizing goes by the last few attempts, however long ago they ran.
CREATE TABLE attempts (
    org_id             uuid NOT NULL,
    -- Its dispatch's.
    id                 text NOT NULL,
    pipeline_id        uuid NOT NULL,
    sync_id            uuid NOT NULL,
    attempt            integer NOT NULL,
    -- In millicores and bytes.
    size_cpu           bigint NOT NULL,
    size_memory        bigint NOT NULL,
    -- sizing.Reason.
    size_reason        text NOT NULL,
    -- All as its runner last reported.
    cpu_seconds        double precision NOT NULL DEFAULT 0,
    peak_memory        bigint NOT NULL DEFAULT 0,
    runner_cpu_seconds double precision NOT NULL DEFAULT 0,
    runner_memory      bigint NOT NULL DEFAULT 0,
    seconds            double precision NOT NULL DEFAULT 0,
    rows               bigint NOT NULL DEFAULT 0,
    bytes              bigint NOT NULL DEFAULT 0,
    -- The taxonomy code it failed with, as its runner reported it.
    code               text NOT NULL DEFAULT '',
    create_time        timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, sync_id) REFERENCES syncs (org_id, id) ON DELETE CASCADE
);
CREATE INDEX attempts_by_pipeline ON attempts (org_id, pipeline_id, create_time DESC);
CREATE INDEX attempts_by_sync ON attempts (org_id, sync_id, attempt);

-- What the attempt had used when it committed the checkpoint, as JSON.
ALTER TABLE checkpoint_mirror ADD COLUMN usage jsonb;

-- +goose Down
ALTER TABLE checkpoint_mirror DROP COLUMN usage;
DROP TABLE attempts;
