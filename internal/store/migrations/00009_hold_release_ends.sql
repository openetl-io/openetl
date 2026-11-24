-- +goose Up
-- A person discarded the release: a run takes it as a discard of its hold,
-- unless the replay in the pipeline's record took it. Then a run in which
-- the stream holds goes on with it, and one in which the stream doesn't
-- hold drops the values it hasn't patched in. It ends with the replay, so
-- the values a finished release leaves held stay held.
ALTER TABLE hold_releases ADD COLUMN ends boolean NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE hold_releases DROP COLUMN ends;
