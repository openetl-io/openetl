-- name: InsertTeardown :exec
INSERT INTO teardowns (org_id, id, workspace_id, data_plane_id, pipeline_id, pipeline, op, connector, source_id, destination_id,
    name, spec, version, connector_digest, secrets)
VALUES (@org_id, @id, @workspace_id, @data_plane_id, @pipeline_id, @pipeline, @op, @connector, @source_id, @destination_id,
    @name, @spec, @version, @connector_digest, @secrets);

-- Teardowns that are due, each leased for @lease_seconds so no other
-- orchestrator takes it meanwhile. Times are the database's.
-- name: ClaimTeardowns :many
UPDATE teardowns t SET next_time = now() + make_interval(secs => @lease_seconds::float8), tries = t.tries + 1
FROM (
    SELECT d.org_id, d.id FROM teardowns d
    WHERE d.org_id = @org_id AND d.next_time <= now()
    ORDER BY d.next_time
    LIMIT @max_rows
    FOR UPDATE SKIP LOCKED
) due
WHERE t.org_id = due.org_id AND t.id = due.id
RETURNING t.*;

-- name: RetryTeardown :exec
UPDATE teardowns SET last_error = @last_error, next_time = now() + make_interval(secs => @wait_seconds::float8)
WHERE org_id = @org_id AND id = @id;

-- name: DeleteTeardown :exec
DELETE FROM teardowns WHERE org_id = @org_id AND id = @id;

-- The teardown, if it was sent to the data plane.
-- name: GetTeardown :one
SELECT * FROM teardowns WHERE org_id = @org_id AND id = @id AND data_plane_id = @data_plane_id;
