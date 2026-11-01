-- name: GetDbtProject :one
SELECT * FROM dbt_projects WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: InsertDbtProject :exec
INSERT INTO dbt_projects (org_id, workspace_id, id, name, digest, revision)
VALUES (@org_id, @workspace_id, @id, @name, @digest, 1);

-- name: UpdateDbtProject :exec
UPDATE dbt_projects SET digest = @digest, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;
