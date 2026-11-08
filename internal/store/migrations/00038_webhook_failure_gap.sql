-- +goose Up
-- When the webhook's endpoint last failed: a quiet spell restarts how long
-- it has been failing, so one failure after it doesn't disable the webhook.
ALTER TABLE webhooks ADD COLUMN last_failure_time timestamptz;

-- +goose Down
ALTER TABLE webhooks DROP COLUMN last_failure_time;
