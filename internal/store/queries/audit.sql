-- name: InsertAuditEntry :exec
INSERT INTO audit_log (org_id, id, time, actor, credential, method, resources, details, peer, user_agent, code, error, end_time)
VALUES (@org_id, @id, @time, @actor, @credential, @method, @resources, @details, @peer, @user_agent, sqlc.narg('code'), @error, sqlc.narg('end_time'));

-- name: CompleteAuditEntry :execrows
UPDATE audit_log SET resources = @resources, details = @details, code = @code, error = @error, end_time = @end_time
WHERE org_id = @org_id AND id = @id AND code IS NULL;

-- Newest first, before the entry before, if set. Empty filters match every
-- entry. A method matches by its procedure, its service and name, or its
-- name.
-- name: ListAuditEntries :many
SELECT * FROM audit_log
WHERE org_id = @org_id
  AND (sqlc.narg('before')::uuid IS NULL OR id < sqlc.narg('before'))
  AND (@actor::text = '' OR actor = @actor)
  AND (@resource::text = '' OR @resource::text = ANY(resources))
  AND (@method::text = '' OR method = @method OR right(method, length(@method) + 1) IN ('.' || @method, '/' || @method))
  AND (sqlc.narg('start_time')::timestamptz IS NULL OR time >= sqlc.narg('start_time'))
  AND (sqlc.narg('end_time')::timestamptz IS NULL OR time < sqlc.narg('end_time'))
ORDER BY id DESC
LIMIT @max_rows;
