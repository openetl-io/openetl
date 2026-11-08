-- name: ClaimHoldCheckpoint :execrows
INSERT INTO hold_checkpoints (org_id, pipeline_id, generation, epoch)
VALUES (@org_id, @pipeline_id, @generation, @epoch)
ON CONFLICT (org_id, pipeline_id) DO UPDATE SET generation = EXCLUDED.generation, epoch = EXCLUDED.epoch
WHERE (hold_checkpoints.generation, hold_checkpoints.epoch) < (EXCLUDED.generation, EXCLUDED.epoch);

-- name: DeleteHolds :exec
DELETE FROM holds WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: InsertHold :exec
INSERT INTO holds (org_id, pipeline_id, stream, column_name, whole, reason, since_generation, since_epoch, start_time, cells, discarded, releasing,
    cause, refused_release, refusal)
VALUES (@org_id, @pipeline_id, @stream, @column_name, @whole, @reason, @since_generation, @since_epoch, @start_time, @cells, @discarded, @releasing,
    @cause, @refused_release, @refusal)
ON CONFLICT DO NOTHING;

-- name: ListHolds :many
-- With the hold's release, if any: one that waits for the next run, or the
-- one the hold refused.
SELECT h.stream, h.column_name, h.whole, h.reason, h.since_generation, h.since_epoch, h.start_time, h.cells,
    h.discarded, h.releasing, h.cause, h.refused_release, h.refusal, r.id AS release_id, r.discard AS release_discards
FROM holds h
LEFT JOIN hold_releases r ON r.org_id = h.org_id AND r.pipeline_id = h.pipeline_id AND r.stream = h.stream
    AND r.column_name = h.column_name AND r.whole = h.whole
    AND r.since_generation = h.since_generation AND r.since_epoch = h.since_epoch
WHERE h.org_id = @org_id AND h.pipeline_id = @pipeline_id
ORDER BY h.stream, h.column_name, h.whole DESC;

-- name: InsertHoldRelease :one
INSERT INTO hold_releases (org_id, id, pipeline_id, stream, column_name, whole, since_generation, since_epoch, discard, new_version, principal)
VALUES (@org_id, @id, @pipeline_id, @stream, @column_name, @whole, @since_generation, @since_epoch, @discard, @new_version, @principal)
ON CONFLICT (org_id, pipeline_id, stream, column_name, whole, since_generation, since_epoch) DO UPDATE SET id = hold_releases.id
RETURNING id, discard, new_version;

-- name: PendingHoldReleases :many
-- The releases of holds still open, not discarded, not being patched in,
-- and not refused, for a run to take.
SELECT r.id, r.stream, r.column_name, r.since_generation, r.since_epoch, r.discard, r.new_version
FROM hold_releases r
JOIN holds h ON h.org_id = r.org_id AND h.pipeline_id = r.pipeline_id AND h.stream = r.stream
    AND h.column_name = r.column_name AND h.whole = r.whole
    AND h.since_generation = r.since_generation AND h.since_epoch = r.since_epoch
WHERE r.org_id = @org_id AND r.pipeline_id = @pipeline_id AND NOT h.discarded AND NOT h.releasing
    AND r.id IS DISTINCT FROM h.refused_release
ORDER BY r.create_time, r.id;

-- name: DeleteHoldRelease :exec
DELETE FROM hold_releases WHERE org_id = @org_id AND id = @id;

-- name: NewestHoldRelease :one
-- What a supervisor compares to see a new release. With the count, because a
-- server with a slower clock gives a release an ID older than the newest.
SELECT coalesce(count(*)::text || '/' || max(id::text), '')::text FROM hold_releases WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: NewestHoldReleaseAt :one
SELECT coalesce(count(*)::text || '/' || max(id::text), '')::text FROM hold_releases
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND create_time <= @at;
