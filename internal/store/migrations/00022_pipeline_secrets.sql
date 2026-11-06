-- +goose Up
-- The secrets a pipeline version's sparkAccess names, so they can't be
-- deleted while its current version does.
ALTER TABLE pipeline_versions ADD COLUMN secrets text[] NOT NULL DEFAULT '{}';

-- +goose Down
ALTER TABLE pipeline_versions DROP COLUMN secrets;
