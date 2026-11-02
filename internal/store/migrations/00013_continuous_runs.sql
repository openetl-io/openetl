-- +goose Up
-- A continuous pipeline's runs are syncs too: each runs one version until it
-- is stopped (status stopped) or fails.
ALTER TABLE syncs ADD COLUMN continuous boolean NOT NULL DEFAULT false;

-- +goose Down
DELETE FROM syncs WHERE continuous;
ALTER TABLE syncs DROP COLUMN continuous;
