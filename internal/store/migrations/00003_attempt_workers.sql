-- +goose Up
-- An attempt on workers: how many, each of the dispatch's size, which a
-- coordinator of coordinator_cpu and coordinator_memory leads; 0: one
-- runner or pod.
ALTER TABLE dispatches ADD COLUMN workers integer NOT NULL DEFAULT 0;
ALTER TABLE dispatches ADD COLUMN coordinator_cpu bigint NOT NULL DEFAULT 0;
ALTER TABLE dispatches ADD COLUMN coordinator_memory bigint NOT NULL DEFAULT 0;
-- Where its workers join the coordinator, once it has claimed the pipeline:
-- an openetl.dataplane.v1.WorkerTarget as JSON, without the worker.
ALTER TABLE dispatches ADD COLUMN worker_target jsonb;

-- The workers it ran on, and why one that asked for them ran on one pod.
ALTER TABLE attempts ADD COLUMN workers integer NOT NULL DEFAULT 0;
ALTER TABLE attempts ADD COLUMN one_pod text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE attempts DROP COLUMN one_pod;
ALTER TABLE attempts DROP COLUMN workers;
ALTER TABLE dispatches DROP COLUMN worker_target;
ALTER TABLE dispatches DROP COLUMN coordinator_memory;
ALTER TABLE dispatches DROP COLUMN coordinator_cpu;
ALTER TABLE dispatches DROP COLUMN workers;
