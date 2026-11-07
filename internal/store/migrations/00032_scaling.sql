-- +goose Up
-- Each attempt's lanes, as its runner last reported them: how
-- many, the most it could use (0: no limit), and where their time went, summed
-- over them.
ALTER TABLE attempts
    ADD COLUMN lanes                 integer NOT NULL DEFAULT 0,
    ADD COLUMN lane_limit            integer NOT NULL DEFAULT 0,
    ADD COLUMN busy_seconds          double precision NOT NULL DEFAULT 0,
    ADD COLUMN backpressured_seconds double precision NOT NULL DEFAULT 0,
    ADD COLUMN idle_seconds          double precision NOT NULL DEFAULT 0,
    ADD COLUMN lane_rows             bigint NOT NULL DEFAULT 0;

-- A rescale asked the running attempt to stop at a checkpoint and start again.
ALTER TABLE dispatches ADD COLUMN drain boolean NOT NULL DEFAULT false;

-- What rescaled a pipeline's jobs besides its spec: RescalePipeline's size,
-- for one version, and the job loop's lanes, for a continuous pipeline.
CREATE TABLE job_scales (
    org_id       uuid NOT NULL,
    pipeline_id  uuid NOT NULL,
    -- RescalePipeline's, in millicores and bytes; 0 if none.
    size_cpu     bigint NOT NULL DEFAULT 0,
    size_memory  bigint NOT NULL DEFAULT 0,
    -- The version it holds for.
    size_version text NOT NULL DEFAULT '',
    -- The job loop's; 0 if it hasn't chosen.
    lanes        integer NOT NULL DEFAULT 0,
    -- Why the job loop isn't rescaling it, such as destination_bound.
    note         text NOT NULL DEFAULT '',
    rescale_time timestamptz,
    PRIMARY KEY (org_id, pipeline_id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Running stream jobs' lanes every 30 s, for the job loop's windows. Kept
-- for an hour.
CREATE TABLE lane_samples (
    org_id                uuid NOT NULL,
    attempt_id            text NOT NULL,
    pipeline_id           uuid NOT NULL,
    sample_time           timestamptz NOT NULL DEFAULT now(),
    lanes                 integer NOT NULL,
    lane_limit            integer NOT NULL,
    busy_seconds          double precision NOT NULL,
    backpressured_seconds double precision NOT NULL,
    idle_seconds          double precision NOT NULL,
    lane_rows             bigint NOT NULL,
    PRIMARY KEY (org_id, attempt_id, sample_time)
);
CREATE INDEX lane_samples_by_time ON lane_samples (org_id, sample_time);

-- +goose Down
DROP TABLE lane_samples;
DROP TABLE job_scales;
ALTER TABLE dispatches DROP COLUMN drain;
ALTER TABLE attempts
    DROP COLUMN lanes,
    DROP COLUMN lane_limit,
    DROP COLUMN busy_seconds,
    DROP COLUMN backpressured_seconds,
    DROP COLUMN idle_seconds,
    DROP COLUMN lane_rows;
