-- +goose Up
-- What the read last reported of its progress when the checkpoint was
-- recorded: [{stream, lag_bytes, lag_records, lag_ms, retention_left_bytes,
-- retention_left_ms}], each field present only if the source reported it.
ALTER TABLE checkpoint_mirror ADD COLUMN progress jsonb;

-- +goose Down
ALTER TABLE checkpoint_mirror DROP COLUMN progress;
