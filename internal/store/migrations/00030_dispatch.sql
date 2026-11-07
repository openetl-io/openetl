-- +goose Up
-- What the placer needs from each runner's report. Capacities
-- and sizes are in millicores and bytes. A runner's orphans are attempts it
-- reports that aren't placed on it any more, which still use its room until
-- they stop.
ALTER TABLE runners
    ADD COLUMN pool text NOT NULL DEFAULT 'default',
    ADD COLUMN capacity_cpu bigint NOT NULL DEFAULT 0,
    ADD COLUMN capacity_memory bigint NOT NULL DEFAULT 0,
    ADD COLUMN reserved_cpu bigint NOT NULL DEFAULT 0,
    ADD COLUMN reserved_memory bigint NOT NULL DEFAULT 0,
    ADD COLUMN orphan_cpu bigint NOT NULL DEFAULT 0,
    ADD COLUMN orphan_memory bigint NOT NULL DEFAULT 0,
    ADD COLUMN draining boolean NOT NULL DEFAULT false;

-- When the API last started a sync by hand, which the sync's placement goes
-- by; StartSync clears it.
ALTER TABLE pipelines ADD COLUMN manual_sync_time timestamptz;

-- Work a workflow sent to a data plane: attempts, which the
-- placer places on a runner, and light operations, which any runner takes.
-- pending -> placed -> running -> finished for an attempt; pending -> running
-- -> finished for a light operation. Deadlines are on the database's clock.
CREATE TABLE dispatches (
    org_id           uuid NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    id               text NOT NULL,
    data_plane_id    text NOT NULL,
    -- Woken when the work finishes.
    workflow_id      text NOT NULL,
    kind             text NOT NULL,
    input            jsonb NOT NULL,
    -- Empty for an attempt; otherwise the kind of light operation.
    light            text NOT NULL DEFAULT '',
    pool             text NOT NULL DEFAULT 'default',
    size_cpu         bigint NOT NULL DEFAULT 0,
    size_memory      bigint NOT NULL DEFAULT 0,
    priority         smallint NOT NULL DEFAULT 0,
    -- What a runner needs to run it: dispatch.Need.
    needs            jsonb NOT NULL DEFAULT '[]',
    -- The trace it's part of, as W3C headers.
    trace            jsonb NOT NULL DEFAULT '{}',
    state            text NOT NULL DEFAULT 'pending',
    runner           text,
    heartbeat_ms     bigint NOT NULL,
    -- How long after being placed it must start; for a light operation, how
    -- long it waits for a runner in all.
    start_timeout_ms bigint NOT NULL,
    timeout_ms       bigint NOT NULL,
    enqueue_time     timestamptz NOT NULL DEFAULT now(),
    start_deadline   timestamptz,
    run_deadline     timestamptz,
    lease_deadline   timestamptz,
    -- A workflow asked it to stop.
    cancel           boolean NOT NULL DEFAULT false,
    epoch            bigint NOT NULL DEFAULT 0,
    -- dispatch.Outcome.
    outcome          jsonb,
    finish_time      timestamptz,
    -- The workflow has been told it finished.
    woken            boolean NOT NULL DEFAULT false,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id)
);
CREATE INDEX dispatches_pending ON dispatches (org_id, data_plane_id, priority, enqueue_time) WHERE state = 'pending' AND light = '';
CREATE INDEX dispatches_light ON dispatches (org_id, data_plane_id, light, enqueue_time) WHERE state = 'pending' AND light <> '';
CREATE INDEX dispatches_on_runner ON dispatches (org_id, data_plane_id, runner) WHERE state IN ('placed', 'running');
CREATE INDEX dispatches_leases ON dispatches (org_id, lease_deadline) WHERE state = 'running';
CREATE INDEX dispatches_starts ON dispatches (org_id, start_deadline) WHERE state IN ('placed', 'pending');
CREATE INDEX dispatches_unwoken ON dispatches (org_id, finish_time) WHERE state = 'finished' AND NOT woken;
CREATE INDEX dispatches_finished ON dispatches (org_id, finish_time) WHERE state = 'finished';

-- +goose Down
DROP TABLE dispatches;
ALTER TABLE pipelines DROP COLUMN manual_sync_time;
ALTER TABLE runners
    DROP COLUMN pool,
    DROP COLUMN capacity_cpu,
    DROP COLUMN capacity_memory,
    DROP COLUMN reserved_cpu,
    DROP COLUMN reserved_memory,
    DROP COLUMN orphan_cpu,
    DROP COLUMN orphan_memory,
    DROP COLUMN draining;
