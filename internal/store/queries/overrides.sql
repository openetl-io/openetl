-- name: PauseStream :execrows
INSERT INTO paused_streams (org_id, pipeline_id, stream, reason, incident_id)
VALUES (@org_id, @pipeline_id, @stream, @reason, @incident_id)
ON CONFLICT DO NOTHING;

-- name: ResumeStream :execrows
DELETE FROM paused_streams WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND stream = @stream;

-- name: ListPausedStreams :many
SELECT * FROM paused_streams
WHERE org_id = @org_id AND pipeline_id = ANY(@pipelines::uuid[])
ORDER BY pipeline_id, stream;

-- name: ResumeStreamsNotIn :exec
-- A version that no longer reads them leaves nothing to pause.
DELETE FROM paused_streams
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND NOT stream = ANY(@streams::text[]);

-- name: SetPipelineOverride :one
-- The value it replaces; null for none.
WITH old AS (
    SELECT value FROM pipeline_overrides
    WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND field = @field
)
INSERT INTO pipeline_overrides (org_id, pipeline_id, field, value, reason, incident_id)
VALUES (@org_id, @pipeline_id, @field, @value, @reason, @incident_id)
ON CONFLICT (org_id, pipeline_id, field) DO UPDATE
SET value = excluded.value, reason = excluded.reason, incident_id = excluded.incident_id, update_time = now()
RETURNING (SELECT value FROM old)::jsonb AS replaced;

-- name: GetPipelineOverride :one
SELECT * FROM pipeline_overrides WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND field = @field;

-- name: ListPipelineOverrides :many
SELECT * FROM pipeline_overrides
WHERE org_id = @org_id AND pipeline_id = ANY(@pipelines::uuid[])
ORDER BY pipeline_id, field;

-- name: DeletePipelineOverride :execrows
DELETE FROM pipeline_overrides WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND field = @field;
