-- name: ClaimHoldCheckpoint :execrows
INSERT INTO hold_checkpoints (org_id, pipeline_id, generation, epoch)
VALUES (@org_id, @pipeline_id, @generation, @epoch)
ON CONFLICT (org_id, pipeline_id) DO UPDATE SET generation = EXCLUDED.generation, epoch = EXCLUDED.epoch
WHERE (hold_checkpoints.generation, hold_checkpoints.epoch) < (EXCLUDED.generation, EXCLUDED.epoch);

-- name: DeleteHolds :exec
DELETE FROM holds WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: InsertHold :exec
INSERT INTO holds (org_id, pipeline_id, stream, column_name, whole, reason, since_generation, since_epoch, start_time, cells, discarded, releasing)
VALUES (@org_id, @pipeline_id, @stream, @column_name, @whole, @reason, @since_generation, @since_epoch, @start_time, @cells, @discarded, @releasing)
ON CONFLICT DO NOTHING;

-- name: ListHolds :many
-- With whether a release waits for the next run to take it.
SELECT h.stream, h.column_name, h.whole, h.reason, h.since_generation, h.since_epoch, h.start_time, h.cells,
    h.discarded, h.releasing, r.id AS release_id, r.discard AS release_discards
FROM holds h
LEFT JOIN hold_releases r ON r.org_id = h.org_id AND r.pipeline_id = h.pipeline_id AND r.stream = h.stream
    AND r.column_name = h.column_name AND r.whole = h.whole
    AND r.since_generation = h.since_generation AND r.since_epoch = h.since_epoch
WHERE h.org_id = @org_id AND h.pipeline_id = @pipeline_id
ORDER BY h.stream, h.column_name, h.whole DESC;

-- name: InsertHoldRelease :one
INSERT INTO hold_releases (org_id, id, pipeline_id, stream, column_name, whole, since_generation, since_epoch, discard, principal)
VALUES (@org_id, @id, @pipeline_id, @stream, @column_name, @whole, @since_generation, @since_epoch, @discard, @principal)
ON CONFLICT (org_id, pipeline_id, stream, column_name, whole, since_generation, since_epoch) DO UPDATE SET id = hold_releases.id
RETURNING id, discard;

-- name: PendingHoldReleases :many
-- The releases of holds still open, not discarded and not being patched in,
-- for a run to take.
SELECT r.stream, r.column_name, r.since_generation, r.since_epoch, r.discard
FROM hold_releases r
JOIN holds h ON h.org_id = r.org_id AND h.pipeline_id = r.pipeline_id AND h.stream = r.stream
    AND h.column_name = r.column_name AND h.whole = r.whole
    AND h.since_generation = r.since_generation AND h.since_epoch = r.since_epoch
WHERE r.org_id = @org_id AND r.pipeline_id = @pipeline_id AND NOT h.discarded AND NOT h.releasing
ORDER BY r.create_time, r.id;

-- name: NewestHoldRelease :one
SELECT coalesce(max(id::text), '')::text FROM hold_releases WHERE org_id = @org_id AND pipeline_id = @pipeline_id;
