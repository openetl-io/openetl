-- name: StartSync :one
INSERT INTO syncs (org_id, id, pipeline_id, version, workflow_id, run_id, status, continuous)
VALUES (@org_id, @id, @pipeline_id, @version, @workflow_id, @run_id, 'running', @continuous)
ON CONFLICT (org_id, run_id) DO UPDATE SET status = syncs.status
RETURNING id;

-- name: FinishSync :exec
UPDATE syncs SET status = @status, error = @error, stats = @stats, end_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: ListSyncs :many
-- Newest first, after the sync (before_time, before_id) if given. The data
-- plane is the version's, or the pipeline's if the version is gone.
SELECT s.*, coalesce(v.data_plane_id, p.data_plane_id)::text AS data_plane_id
FROM syncs s
JOIN pipelines p ON p.org_id = s.org_id AND p.id = s.pipeline_id
LEFT JOIN pipeline_versions v ON v.org_id = s.org_id AND v.pipeline_id = s.pipeline_id AND v.version = s.version
WHERE s.org_id = @org_id AND s.pipeline_id = @pipeline_id
  AND (sqlc.narg('before_time')::timestamptz IS NULL OR (s.start_time, s.id) < (sqlc.narg('before_time'), @before_id::uuid))
ORDER BY s.start_time DESC, s.id DESC
LIMIT @max_rows;

-- name: GetSync :one
SELECT s.*, coalesce(v.data_plane_id, p.data_plane_id)::text AS data_plane_id
FROM syncs s
JOIN pipelines p ON p.org_id = s.org_id AND p.id = s.pipeline_id
LEFT JOIN pipeline_versions v ON v.org_id = s.org_id AND v.pipeline_id = s.pipeline_id AND v.version = s.version
WHERE s.org_id = @org_id AND s.pipeline_id = @pipeline_id AND s.id = @id;

-- name: LastSyncSuccess :one
-- When the latest sync that succeeded ended.
SELECT end_time FROM syncs
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND status = 'succeeded'
ORDER BY start_time DESC, id DESC
LIMIT 1;
