-- +goose Up
-- A person discarded the release while it was being patched in: a run in
-- which its stream holds goes on with it, and one in which the stream
-- doesn't hold drops the values it hasn't patched in. It ends with the
-- release, so the values a finished release leaves held stay held.
ALTER TABLE hold_releases ADD COLUMN ends boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE hold_releases DROP COLUMN ends;
