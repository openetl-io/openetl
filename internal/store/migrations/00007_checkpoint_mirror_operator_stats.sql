-- +goose Up
-- What each stateful operator did in a checkpoint's epoch, as a JSON array
-- of openetl.api.v1.OperatorStats; null without any.
ALTER TABLE checkpoint_mirror ADD COLUMN operator_stats jsonb;

-- +goose Down
ALTER TABLE checkpoint_mirror DROP COLUMN operator_stats;
