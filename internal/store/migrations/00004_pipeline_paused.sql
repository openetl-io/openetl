-- +goose Up
-- A paused pipeline's schedule is paused, and its syncs don't run.
ALTER TABLE pipelines ADD COLUMN paused boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE pipelines DROP COLUMN paused;
