-- +goose Up
-- When a run first took the release: an attempt fetched it to apply. A
-- discard takes the place of a release no run has taken, and ends one a run
-- has.
ALTER TABLE hold_releases ADD COLUMN take_time timestamptz;

-- +goose Down
ALTER TABLE hold_releases DROP COLUMN take_time;
