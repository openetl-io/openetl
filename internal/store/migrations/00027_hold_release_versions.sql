-- +goose Up
-- A release can send a column hold's values to a new version of the
-- column, col__v2, instead of the column (ADR-0025).
ALTER TABLE hold_releases ADD COLUMN new_version boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE hold_releases DROP COLUMN new_version;
