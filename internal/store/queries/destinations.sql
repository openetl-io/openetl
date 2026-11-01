-- name: GetDestination :one
SELECT * FROM destinations WHERE org_id = @org_id AND workspace_id = @workspace_id AND name = @name;

-- name: ListDestinations :many
SELECT * FROM destinations
WHERE org_id = @org_id AND workspace_id = @workspace_id AND name > @after
ORDER BY name LIMIT @max_rows;

-- name: InsertDestination :exec
INSERT INTO destinations (org_id, workspace_id, id, name, spec, version, connector_digest, revision)
VALUES (@org_id, @workspace_id, @id, @name, @spec, @version, @connector_digest, 1);

-- name: UpdateDestination :exec
UPDATE destinations SET spec = @spec, version = @version, connector_digest = @connector_digest, revision = revision + 1, update_time = now()
WHERE org_id = @org_id AND id = @id;

-- name: DeleteDestination :execrows
DELETE FROM destinations WHERE org_id = @org_id AND id = @id;

-- name: GetDestinationByID :one
SELECT * FROM destinations WHERE org_id = @org_id AND id = @id;
