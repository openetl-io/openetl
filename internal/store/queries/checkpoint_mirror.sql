-- name: InsertMirroredCheckpoint :execrows
INSERT INTO checkpoint_mirror (org_id, pipeline_id, epoch, generation, previous_epoch, previous_generation, run_id, commit_time, streams)
VALUES (@org_id, @pipeline_id, @epoch, @generation, @previous_epoch, @previous_generation, @run_id, @commit_time, @streams)
ON CONFLICT DO NOTHING;

-- name: ListCheckpoints :many
-- Newest first, after (before_epoch, before_generation) if given, with the
-- sync that committed each.
SELECT c.epoch, c.generation, c.previous_epoch, c.previous_generation, c.commit_time, c.streams,
    s.id AS sync_id
FROM checkpoint_mirror c
LEFT JOIN syncs s ON s.org_id = c.org_id AND s.run_id = c.run_id AND s.pipeline_id = c.pipeline_id
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id
  AND (sqlc.narg('before_epoch')::bigint IS NULL OR (c.epoch, c.generation) < (sqlc.narg('before_epoch'), @before_generation::bigint))
ORDER BY c.epoch DESC, c.generation DESC
LIMIT @max_rows;

-- name: SyncStreams :many
-- What the syncs' committed epochs carried, by sync and stream. Checkpoints
-- are committed after since.
SELECT c.run_id, (st ->> 'name')::text AS name,
    sum((st ->> 'rows')::bigint)::bigint AS rows,
    sum((st ->> 'quarantined_rows')::bigint)::bigint AS quarantined_rows
FROM checkpoint_mirror c
CROSS JOIN LATERAL jsonb_array_elements(c.streams) AS st
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id AND c.run_id = ANY(@run_ids::text[])
  AND c.commit_time >= @since
GROUP BY c.run_id, st ->> 'name'
ORDER BY c.run_id, st ->> 'name';
