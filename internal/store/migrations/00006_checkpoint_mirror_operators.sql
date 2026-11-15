-- +goose Up
-- The stateful operators whose state a checkpoint names; null in
-- checkpoints mirrored before it was kept.
ALTER TABLE checkpoint_mirror ADD COLUMN operators text[];

-- +goose Down
ALTER TABLE checkpoint_mirror DROP COLUMN operators;
