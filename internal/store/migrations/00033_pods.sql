-- +goose Up
-- A pod per attempt (ADR-0027 §5). A pod launcher reports the jobs it takes
-- and its caps; a pod reports the one attempt it runs, and is never placed on.
-- Sizes are in millicores and bytes; a max of 0 is no limit.
ALTER TABLE runners
    ADD COLUMN launcher        boolean NOT NULL DEFAULT false,
    ADD COLUMN pod_min_cpu     bigint NOT NULL DEFAULT 0,
    ADD COLUMN pod_min_memory  bigint NOT NULL DEFAULT 0,
    ADD COLUMN pod_max_cpu     bigint NOT NULL DEFAULT 0,
    ADD COLUMN pod_max_memory  bigint NOT NULL DEFAULT 0,
    ADD COLUMN max_pods        integer NOT NULL DEFAULT 0,
    ADD COLUMN max_pod_cpu     bigint NOT NULL DEFAULT 0,
    ADD COLUMN attempt         text NOT NULL DEFAULT '';

-- An attempt placed on a launcher keeps the launcher's ID while its pod runs
-- it, which counts against the launcher's caps. Each placement has a number,
-- so a pod of an earlier one can't take it. Launched: the launcher has taken
-- it to make a pod.
ALTER TABLE dispatches
    ADD COLUMN launcher   text,
    ADD COLUMN placements integer NOT NULL DEFAULT 0,
    ADD COLUMN launched   boolean NOT NULL DEFAULT false;
CREATE INDEX dispatches_on_launcher ON dispatches (org_id, data_plane_id, launcher) WHERE state IN ('placed', 'running') AND launcher IS NOT NULL;

-- +goose Down
DROP INDEX dispatches_on_launcher;
ALTER TABLE dispatches
    DROP COLUMN launcher,
    DROP COLUMN placements,
    DROP COLUMN launched;
ALTER TABLE runners
    DROP COLUMN launcher,
    DROP COLUMN pod_min_cpu,
    DROP COLUMN pod_min_memory,
    DROP COLUMN pod_max_cpu,
    DROP COLUMN pod_max_memory,
    DROP COLUMN max_pods,
    DROP COLUMN max_pod_cpu,
    DROP COLUMN attempt;
