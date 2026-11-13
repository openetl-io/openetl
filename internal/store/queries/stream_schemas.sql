-- name: LockPipelineSchemas :one
SELECT id FROM pipelines WHERE org_id = @org_id AND id = @pipeline_id FOR NO KEY UPDATE;

-- The version added; none if the history has the schema.
-- name: AddStreamSchema :many
INSERT INTO stream_schemas (org_id, pipeline_id, output, stream, version, digest, "table", columns, arrow_schema, generation, epoch)
SELECT @org_id, @pipeline_id, @output, @stream,
       coalesce(max(version), 0) + 1, @digest, @table_name, @columns, @arrow_schema, @generation, @epoch
FROM stream_schemas WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND output = @output AND stream = @stream
ON CONFLICT (org_id, pipeline_id, output, stream, digest) DO NOTHING
RETURNING version;

-- name: GetStreamSchemaColumns :one
SELECT columns FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND output = @output AND stream = @stream AND version = @version;

-- name: ListStreamSchemas :many
-- Outputs first, then by stream and version, after (after_input,
-- after_stream, after_version); only the stream named, if one is.
SELECT output, stream, version, digest, "table", columns, generation, epoch, create_time
FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id
  AND (@stream::text = '' OR stream = @stream)
  AND (NOT output, stream, version) > (@after_input::boolean, @after_stream::text, @after_version::integer)
ORDER BY NOT output, stream, version
LIMIT @max_rows;

-- The source's streams' schemas, which the checkpoint mirror names.
-- name: StreamSchemaVersions :many
SELECT stream, digest, version FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND NOT output;

-- Each of the pipeline's source streams' schema in its newest mirrored
-- checkpoint, or its newest version where that checkpoint doesn't name it. A
-- schema the history has is never added again, so the newest version isn't
-- always the one the stream has.
-- name: LatestStreamSchemas :many
WITH latest AS (
    SELECT e->>'name' AS stream, e->>'schema_digest' AS digest
    FROM (
        SELECT c.streams FROM checkpoint_mirror c
        WHERE c.org_id = @org_id AND c.pipeline_id = @pipeline_id
        ORDER BY c.epoch DESC, c.generation DESC
        LIMIT 1
    ) m, jsonb_array_elements(m.streams) e
)
SELECT DISTINCT ON (s.stream) s.stream, s.columns
FROM stream_schemas s
LEFT JOIN latest l ON l.stream = s.stream AND l.digest = s.digest
WHERE s.org_id = @org_id AND s.pipeline_id = @pipeline_id AND NOT s.output
ORDER BY s.stream, l.digest IS NULL, s.version DESC;
