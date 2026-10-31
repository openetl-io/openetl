-- name: GetSource :one
SELECT * FROM sources WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: ListSources :many
SELECT * FROM sources
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: InsertSource :exec
INSERT INTO sources (org_id, workspace_id, id, name, spec, version, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, 1);

-- name: UpdateSource :exec
UPDATE sources SET spec = @spec, version = @version, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeleteSource :execrows
DELETE FROM sources WHERE org_id = @org_id AND id = @id;
