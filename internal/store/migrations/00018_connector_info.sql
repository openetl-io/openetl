-- +goose Up
-- What a connector version's metadata says about it, for the UI.
ALTER TABLE connector_versions
    ADD COLUMN author      text NOT NULL DEFAULT '',
    ADD COLUMN logo        text NOT NULL DEFAULT '',
    ADD COLUMN release_tag text NOT NULL DEFAULT '',
    ADD COLUMN release_url text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE connector_versions
    DROP COLUMN author,
    DROP COLUMN logo,
    DROP COLUMN release_tag,
    DROP COLUMN release_url;
