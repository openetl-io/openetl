-- name: InsertQuarantineFile :execrows
INSERT INTO quarantine_files (org_id, pipeline_id, path, url, stream, output, generation, epoch, run_id, commit_time, rows, causes)
VALUES (@org_id, @pipeline_id, @path, @url, @stream, @output, @generation, @epoch, @run_id, @commit_time, @rows, @causes)
ON CONFLICT DO NOTHING;

-- name: ListQuarantineFiles :many
-- Newest first, after (before_time, before_path) if given, with the sync
-- that committed each.
SELECT f.path, f.url, f.stream, f.output, f.generation, f.epoch, f.commit_time, f.rows, f.causes,
    s.id AS sync_id
FROM quarantine_files f
LEFT JOIN syncs s ON s.org_id = f.org_id AND s.run_id = f.run_id AND s.pipeline_id = f.pipeline_id
WHERE f.org_id = @org_id AND f.pipeline_id = @pipeline_id
  AND (sqlc.narg('stream')::text IS NULL OR f.stream = sqlc.narg('stream'))
  AND (sqlc.narg('since')::timestamptz IS NULL OR f.commit_time >= sqlc.narg('since'))
  AND (sqlc.narg('before_time')::timestamptz IS NULL OR (f.commit_time, f.path) < (sqlc.narg('before_time'), @before_path::text))
ORDER BY f.commit_time DESC, f.path DESC
LIMIT @max_rows;

-- name: CountQuarantineFiles :one
-- How many files a stream's commits at or after since quarantined rows to.
SELECT count(*)::bigint FROM quarantine_files
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND stream = @stream AND commit_time >= @since;

-- name: DropExpiredQuarantineFiles :execrows
-- Drops the files older than the pipeline's current version's
-- quarantine.retention, a protobuf JSON duration such as "86400s"; none
-- without one.
DELETE FROM quarantine_files f
USING pipelines p
JOIN pipeline_versions v ON v.org_id = p.org_id AND v.pipeline_id = p.id AND v.version = p.version
WHERE f.org_id = @org_id AND f.pipeline_id = @pipeline_id
  AND p.org_id = f.org_id AND p.id = f.pipeline_id
  AND v.spec -> 'quarantine' ->> 'retention' IS NOT NULL
  AND f.commit_time < now() - rtrim(v.spec -> 'quarantine' ->> 'retention', 's')::numeric * interval '1 second';
