-- name: InsertCheckpointRequest :one
INSERT INTO checkpoint_requests (org_id, id, pipeline_id, kind, streams, principal)
VALUES (@org_id, @id, @pipeline_id, @kind, @streams, @principal)
RETURNING id, create_time;

-- name: PendingCheckpointRequests :many
-- Oldest first.
SELECT id, kind, streams FROM checkpoint_requests
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND take_time IS NULL
ORDER BY id;

-- name: TakeResets :exec
-- A checkpoint that took a reset took every older one.
UPDATE checkpoint_requests SET take_time = now()
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND kind = 'reset' AND take_time IS NULL
  AND id <= @newest;

-- name: TakeRewinds :exec
-- A rewind is taken by the restart it allowed, or not needed once the
-- pipeline makes progress.
UPDATE checkpoint_requests SET take_time = now()
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND kind = 'rewind' AND take_time IS NULL;

-- name: NewestCheckpointRequest :one
-- What a supervisor compares to see a new request. With the count, because a
-- server with a slower clock gives a request an ID older than the newest.
SELECT coalesce(count(*)::text || '/' || max(id::text), '')::text FROM checkpoint_requests WHERE org_id = @org_id AND pipeline_id = @pipeline_id;

-- name: NewestCheckpointRequestAt :one
SELECT coalesce(count(*)::text || '/' || max(id::text), '')::text FROM checkpoint_requests
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND create_time <= @at;

-- name: ClaimRetentionProbes :many
-- Up to max_rows pipelines of any org to ask their source about retention:
-- the last checkpoint carried the source's retention, and is from before
-- @before; no sync or run is going; and they weren't asked since @before.
-- Claiming sets their probe time.
WITH due AS (
    SELECT p.org_id, p.id
    FROM pipelines p
    CROSS JOIN LATERAL (
        SELECT m.commit_time, m.progress FROM checkpoint_mirror m
        WHERE m.org_id = p.org_id AND m.pipeline_id = p.id
        ORDER BY m.epoch DESC, m.generation DESC
        LIMIT 1
    ) c
    WHERE c.commit_time < @before::timestamptz
      AND (jsonb_path_exists(c.progress, '$[*].retention_left_bytes') OR jsonb_path_exists(c.progress, '$[*].retention_left_ms'))
      AND (p.retention_probe_time IS NULL OR p.retention_probe_time < @before::timestamptz)
      AND NOT EXISTS (
          SELECT 1 FROM syncs s WHERE s.org_id = p.org_id AND s.pipeline_id = p.id AND s.end_time IS NULL
      )
    ORDER BY p.retention_probe_time NULLS FIRST
    LIMIT @max_rows::int
    FOR UPDATE OF p SKIP LOCKED
)
UPDATE pipelines p SET retention_probe_time = now()
FROM due, sources src
WHERE p.org_id = due.org_id AND p.id = due.id AND src.org_id = p.org_id AND src.id = p.source_id
RETURNING p.org_id, p.workspace_id, p.id, p.data_plane_id, p.source_id, src.spec AS source_spec;
