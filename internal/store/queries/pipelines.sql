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
INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id,
    source_version, destination_version, dbt, provenance)
VALUES (@org_id, @pipeline_id, @version, @spec, @source_id, @destination_id, @data_plane_id,
    @source_version, @destination_version, @dbt, @provenance)
ON CONFLICT (org_id, pipeline_id, version) DO NOTHING;

-- name: GetPipelineVersion :one
SELECT * FROM pipeline_versions WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND version = @version;

-- name: GetSyncSpec :one
SELECT v.version, v.data_plane_id, v.spec AS pipeline_spec, v.dbt, v.source_version, v.destination_version,
    sv.spec AS source_spec, dv.spec AS destination_spec,
    sv.connector_digest AS source_connector, dv.connector_digest AS destination_connector,
    sv.secrets AS source_secrets, dv.secrets AS destination_secrets,
    p.workspace_id, p.name AS pipeline, w.name AS workspace
FROM pipeline_versions v
JOIN pipelines p ON p.org_id = v.org_id AND p.id = v.pipeline_id
JOIN workspaces w ON w.org_id = p.org_id AND w.id = p.workspace_id
JOIN source_versions sv ON sv.org_id = v.org_id AND sv.source_id = v.source_id AND sv.version = v.source_version
JOIN destination_versions dv ON dv.org_id = v.org_id AND dv.destination_id = v.destination_id AND dv.version = v.destination_version
WHERE v.org_id = @org_id AND v.pipeline_id = @pipeline_id AND v.version = @version;

-- The pipelines whose current versions use any of the sources, destinations
-- or dbt projects.
-- name: ListPipelinesUsing :many
SELECT p.id, p.name, p.version, p.workspace_id, p.source_id, p.destination_id, w.name AS workspace, v.spec, v.dbt
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
JOIN workspaces w ON w.org_id = p.org_id AND w.id = p.workspace_id
WHERE p.org_id = @org_id AND (
    p.source_id = ANY(@sources::uuid[]) OR p.destination_id = ANY(@destinations::uuid[])
    OR EXISTS (
        SELECT 1 FROM jsonb_object_keys(v.dbt) k
        JOIN dbt_projects d ON d.org_id = p.org_id AND d.workspace_id = p.workspace_id AND d.name = k
        WHERE d.id = ANY(@dbt_projects::uuid[])))
ORDER BY w.name, p.name;

-- name: SetPipelinePaused :exec
UPDATE pipelines SET paused = @paused WHERE org_id = @org_id AND id = @id;
