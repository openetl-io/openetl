-- +goose Up
-- Why each hold is held, as the API's HoldCause in JSON, and the
-- release that didn't clear it, which isn't sent again until the hold is
-- released again.
ALTER TABLE holds
    ADD COLUMN cause jsonb,
    ADD COLUMN refused_release uuid,
    ADD COLUMN refusal text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE holds
    DROP COLUMN cause,
    DROP COLUMN refused_release,
    DROP COLUMN refusal;
