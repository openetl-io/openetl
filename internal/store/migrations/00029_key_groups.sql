-- +goose Up
-- Each pipeline's key groups (ADR-0027), fixed when it's created and kept in
-- its versions. A restore from a record with another count fails.
ALTER TABLE pipelines ADD COLUMN key_groups integer NOT NULL DEFAULT 256;
ALTER TABLE pipeline_versions ADD COLUMN key_groups integer NOT NULL DEFAULT 256;

-- +goose Down
ALTER TABLE pipelines DROP COLUMN key_groups;
ALTER TABLE pipeline_versions DROP COLUMN key_groups;
