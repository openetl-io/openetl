-- +goose Up
-- A person discarded the release: a run takes it as a discard of its hold,
-- unless the replay in the pipeline's record took it. Then a run in which
-- the stream holds goes on with it, and one in which the stream doesn't
-- hold drops the values it hasn't patched in. It ends with the replay, so
-- the values a finished release leaves held stay held.
ALTER TABLE hold_releases ADD COLUMN ends boolean NOT NULL DEFAULT false;
-- With ends, the held cells of the release's column that the person saw
-- when they discarded it: an ended replay drops its column's cells only
-- while it holds no more than these, as later ones are held after the
-- discard.
ALTER TABLE hold_releases ADD COLUMN ends_cells bigint NOT NULL DEFAULT 0;

-- +goose Down
ALTER TABLE hold_releases DROP COLUMN ends_cells;
ALTER TABLE hold_releases DROP COLUMN ends;
