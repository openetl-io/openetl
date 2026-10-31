-- name: StartSync :one
INSERT INTO syncs (org_id, id, pipeline_id, version, workflow_id, run_id, status)
VALUES (@org_id, @id, @pipeline_id, @version, @workflow_id, @run_id, 'running')
ON CONFLICT (org_id, run_id) DO UPDATE SET status = syncs.status
RETURNING id;

-- name: FinishSync :exec
UPDATE syncs SET status = @status, error = @error, stats = @stats, end_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: ListSyncs :many
SELECT * FROM syncs WHERE org_id = @org_id AND pipeline_id = @pipeline_id
ORDER BY start_time DESC LIMIT @max_rows;
