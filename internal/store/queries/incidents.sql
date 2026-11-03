-- Nothing if the pipeline has the incident open already.
-- name: InsertIncident :one
INSERT INTO incidents (org_id, id, workspace_id, pipeline_id, stream, class, code, fingerprint, severity, summary, message,
    next_steps, details, actions, sync_id, occurrence_key)
VALUES (@org_id, @id, @workspace_id, @pipeline_id, @stream, @class, @code, @fingerprint, @severity, @summary, @message,
    @next_steps, @details, @actions, @sync_id, @occurrence_key)
ON CONFLICT (org_id, pipeline_id, stream, fingerprint) WHERE status = 'open' DO NOTHING
RETURNING *;

-- name: RepeatIncident :one
-- The open incident seen again, with its key and severity from before. It's
-- another occurrence only if its key is new.
UPDATE incidents i SET
    occurrences = i.occurrences + (old.occurrence_key <> @occurrence_key)::int,
    occurrence_key = @occurrence_key,
    severity = @severity, summary = @summary, message = @message, details = @details,
    sync_id = coalesce(sqlc.narg('sync_id'), i.sync_id),
    last_seen_time = now()
FROM (
    SELECT o.id, o.occurrence_key, o.severity FROM incidents o
    WHERE o.org_id = @org_id AND o.pipeline_id = @pipeline_id AND o.stream = @stream AND o.fingerprint = @fingerprint
      AND o.status = 'open'
    FOR UPDATE
) old
WHERE i.org_id = @org_id AND i.id = old.id
RETURNING i.*, old.occurrence_key::text AS was_key, old.severity::text AS was_severity;

-- name: SetIncidentActions :exec
UPDATE incidents SET actions = @actions WHERE org_id = @org_id AND id = @id;

-- name: ResolveIncidents :many
-- The pipeline's open incidents of the classes, and of the stream if one is
-- given, last seen before this transaction.
UPDATE incidents SET status = 'resolved', resolve_time = now(), resolution = @resolution
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND status = 'open' AND class = ANY(@classes::text[])
  AND (sqlc.narg('stream')::text IS NULL OR stream = sqlc.narg('stream'))
  AND last_seen_time < now()
RETURNING *;

-- name: ResolveIncident :one
UPDATE incidents SET status = 'resolved', resolve_time = now(), resolution = @resolution
WHERE org_id = @org_id AND id = @id AND status = 'open'
RETURNING *;

-- name: OpenIncidents :many
SELECT * FROM incidents
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND status = 'open' AND class = @class;

-- name: GetIncident :one
SELECT sqlc.embed(i), p.name AS pipeline FROM incidents i
JOIN pipelines p ON p.org_id = i.org_id AND p.id = i.pipeline_id
WHERE i.org_id = @org_id AND i.pipeline_id = @pipeline_id AND i.id = @id;

-- name: ListIncidents :many
-- Newest first, after the incident (before_time, before_id) if given; of one
-- pipeline, or the workspace's.
SELECT sqlc.embed(i), p.name AS pipeline FROM incidents i
JOIN pipelines p ON p.org_id = i.org_id AND p.id = i.pipeline_id
WHERE i.org_id = @org_id AND i.workspace_id = @workspace_id
  AND (sqlc.narg('pipeline_id')::uuid IS NULL OR i.pipeline_id = sqlc.narg('pipeline_id'))
  AND (sqlc.narg('status')::text IS NULL OR i.status = sqlc.narg('status'))
  AND (sqlc.narg('before_time')::timestamptz IS NULL OR (i.first_seen_time, i.id) < (sqlc.narg('before_time'), @before_id::uuid))
ORDER BY i.first_seen_time DESC, i.id DESC
LIMIT @max_rows;

-- name: CountOpenIncidents :many
SELECT pipeline_id, count(*)::int AS open FROM incidents
WHERE org_id = @org_id AND pipeline_id = ANY(@pipeline_ids::uuid[]) AND status = 'open'
GROUP BY pipeline_id;
