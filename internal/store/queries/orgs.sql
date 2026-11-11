-- name: FirstOrg :one
SELECT id, name FROM orgs ORDER BY create_time, id LIMIT 1;

-- name: CreateOrg :exec
INSERT INTO orgs (id, name) VALUES (@id, @name);

-- name: CreateWorkspace :exec
INSERT INTO workspaces (org_id, id, name) VALUES (@org_id, @id, @name)
ON CONFLICT (org_id, name) DO NOTHING;

-- name: GetWorkspace :one
SELECT id, name FROM workspaces WHERE org_id = @org_id AND name = @name;

-- name: GetWorkspaceName :one
SELECT name FROM workspaces WHERE org_id = @org_id AND id = @id;

-- name: CreateDataPlane :exec
INSERT INTO data_planes (id, org_id) VALUES (@id, @org_id) ON CONFLICT (id) DO NOTHING;

-- name: GetDataPlane :one
SELECT id FROM data_planes WHERE org_id = @org_id AND id = @id;

-- name: ListOrgs :many
SELECT id FROM orgs ORDER BY id;

-- name: LockApply :exec
-- An org's applies run one at a time, until the transaction ends.
SELECT pg_advisory_xact_lock(hashtextextended('openetl.apply.' || (@org_id::uuid)::text, 0));
