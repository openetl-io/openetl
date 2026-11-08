-- name: InsertMirroredCheckpoint :execrows
INSERT INTO checkpoint_mirror (org_id, pipeline_id, epoch, generation, previous_epoch, previous_generation, run_id, commit_time, streams, progress, usage)
VALUES (@org_id, @pipeline_id, @epoch, @generation, @previous_epoch, @previous_generation, @run_id, @commit_time, @streams, @progress, @usage)
ON CONFLICT DO NOTHING;

-- name: ListCheckpoints :many
-- Newest first, after (before_epoch, before_generation) if given, with the
-- sync that committed each.
SELECT c.epoch, c.generation, c.previous_epoch, c.previous_generation, c.commit_time, c.streams, c.usage,
    s.id AS sync_id
FROM checkpoint_mirror c
LEFT JOIN syncs s ON s.org_id = c.org_id AND s.run_id = c.run_id AND s.pipeline_id = c.pipeline_id
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id
  AND (sqlc.narg('before_epoch')::bigint IS NULL OR (c.epoch, c.generation) < (sqlc.narg('before_epoch'), @before_generation::bigint))
ORDER BY c.epoch DESC, c.generation DESC
LIMIT @max_rows;

-- name: SyncStreams :many
-- What the syncs' committed epochs carried, by sync and stream. Checkpoints
-- are committed after since. Rows are the newest checkpoint's total for the
-- sync, which a report that was dropped can't leave low; the epochs' sum is
-- for checkpoints from before they carried one.
SELECT c.run_id, (st ->> 'name')::text AS name,
    coalesce(
        (array_agg((st ->> 'sync_rows')::bigint ORDER BY c.epoch DESC, c.generation DESC)
            FILTER (WHERE st ->> 'sync_rows' IS NOT NULL))[1],
        sum((st ->> 'rows')::bigint)
    )::bigint AS rows,
    sum((st ->> 'quarantined_rows')::bigint)::bigint AS quarantined_rows,
    -- Checkpoints mirrored before it was counted have none.
    coalesce(sum((st ->> 'collapsed_rows')::bigint), 0)::bigint AS collapsed_rows,
    coalesce(sum((st ->> 'held_cells')::bigint), 0)::bigint AS held_cells,
    -- The columns it held at its latest commit.
    ((array_agg(st -> 'held_columns' ORDER BY c.epoch DESC, c.generation DESC))[1])::jsonb AS held_columns,
    max(c.commit_time)::timestamptz AS last_commit_time
FROM checkpoint_mirror c
CROSS JOIN LATERAL jsonb_array_elements(c.streams) AS st
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id AND c.run_id = ANY(@run_ids::text[])
  AND c.commit_time >= @since
GROUP BY c.run_id, st ->> 'name'
ORDER BY c.run_id, st ->> 'name';

-- name: SyncProgress :many
-- Each sync's latest checkpoint that carried the read's progress, committed
-- after since.
SELECT DISTINCT ON (c.run_id) c.run_id, c.commit_time, c.progress
FROM checkpoint_mirror c
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id AND c.run_id = ANY(@run_ids::text[])
  AND c.commit_time >= @since AND c.progress IS NOT NULL
ORDER BY c.run_id, c.commit_time DESC;

-- name: LastCommits :many
-- Each pipeline's latest mirrored checkpoint, for freshness; pipelines the
-- mirror has none of are left out.
SELECT p.workspace_id, p.id AS pipeline_id, c.commit_time
FROM pipelines p
CROSS JOIN LATERAL (
    SELECT m.commit_time FROM checkpoint_mirror m
    WHERE m.org_id = p.org_id AND m.pipeline_id = p.id
    ORDER BY m.epoch DESC, m.generation DESC
    LIMIT 1
) c
WHERE p.org_id = @org_id;

-- name: LatestMirroredStreams :one
-- The streams of the pipeline's newest mirrored checkpoint that has stream.
SELECT c.streams FROM checkpoint_mirror c
WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id
  AND c.streams @> jsonb_build_array(jsonb_build_object('name', @stream::text))
ORDER BY c.epoch DESC, c.generation DESC
LIMIT 1;
