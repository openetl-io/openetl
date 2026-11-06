-- name: GetDbtProject :one
SELECT * FROM dbt_projects WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: InsertDbtProject :exec
INSERT INTO dbt_projects (org_id, workspace_id, id, name, digest, revision)
VALUES (@org_id, @workspace_id, @id, @name, @digest, 1);

-- name: UpdateDbtProject :exec
UPDATE dbt_projects SET digest = @digest, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- Keeps the project from being deleted until the transaction ends.
-- name: ShareDbtProject :one
SELECT * FROM dbt_projects WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR SHARE;

-- name: LockDbtProject :one
SELECT * FROM dbt_projects WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name
FOR UPDATE;

-- name: ListDbtProjects :many
SELECT * FROM dbt_projects
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: DeleteDbtProject :exec
DELETE FROM dbt_projects WHERE org_id = @org_id AND id = @id;

-- The pipelines whose current versions run any of the projects.
-- name: ListPipelinesUsingDbtProjects :many
SELECT k.project::text AS project, p.name
FROM pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
CROSS JOIN LATERAL jsonb_object_keys(v.dbt) AS k(project)
WHERE p.org_id = @org_id AND p.workspace_id = @workspace_id AND k.project = ANY(@names::text[])
ORDER BY p.name;
