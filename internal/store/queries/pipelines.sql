-- name: GetPipeline :one
SELECT * FROM pipelines WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: GetPipelineByID :one
SELECT * FROM pipelines WHERE org_id = @org_id AND id = @id;

-- name: ListPipelines :many
SELECT * FROM pipelines
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: InsertPipeline :exec
INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
VALUES (@org_id, @workspace_id, @id, @name, @source_id, @destination_id, @data_plane_id, @version, 1);

-- name: UpdatePipeline :exec
UPDATE pipelines
SET source_id = @source_id, destination_id = @destination_id, data_plane_id = @data_plane_id,
    version = @version, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeletePipeline :execrows
DELETE FROM pipelines WHERE org_id = @org_id AND id = @id;

-- name: InsertPipelineVersion :exec
INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id)
VALUES (@org_id, @pipeline_id, @version, @spec, @source_id, @destination_id, @data_plane_id)
ON CONFLICT (org_id, pipeline_id, version) DO NOTHING;

-- name: GetPipelineVersion :one
SELECT * FROM pipeline_versions WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND version = @version;

-- name: GetSyncSpec :one
SELECT v.version, v.data_plane_id, v.spec AS pipeline_spec, s.spec AS source_spec, d.spec AS destination_spec,
    s.connector_digest AS source_connector, d.connector_digest AS destination_connector,
    s.workspace_id AS source_workspace, s.secrets AS source_secrets,
    d.workspace_id AS destination_workspace, d.secrets AS destination_secrets
FROM pipeline_versions v
JOIN sources s ON s.org_id = v.org_id AND s.id = v.source_id
JOIN destinations d ON d.org_id = v.org_id AND d.id = v.destination_id
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version;

-- name: SetPipelinePaused :exec
UPDATE pipelines SET paused = @paused WHERE org_id = @org_id AND id = @id;
