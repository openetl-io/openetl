-- name: LockPipelineSchemas :one
SELECT id FROM pipelines WHERE org_id = @org_id AND id = @pipeline_id FOR NO KEY UPDATE;

-- The version added; none if the history has the schema.
-- name: AddStreamSchema :many
INSERT INTO stream_schemas (org_id, pipeline_id, stream, version, digest, "table", columns, arrow_schema, generation, epoch)
SELECT @org_id, @pipeline_id, @stream,
       coalesce(max(version), 0) + 1, @digest, @table_name, @columns, @arrow_schema, @generation, @epoch
FROM stream_schemas WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND stream = @stream
ON CONFLICT (org_id, pipeline_id, stream, digest) DO NOTHING
RETURNING version;

-- name: GetStreamSchemaColumns :one
SELECT columns FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND stream = @stream AND version = @version;

-- name: ListStreamSchemas :many
-- By stream, then version, after (after_stream, after_version); only the
-- stream named, if one is.
SELECT stream, version, digest, "table", columns, generation, epoch, create_time
FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id
  AND (@stream::text = '' OR stream = @stream)
  AND (stream, version) > (@after_stream::text, @after_version::integer)
ORDER BY stream, version
LIMIT @max_rows;

-- name: StreamSchemaVersions :many
SELECT stream, digest, version FROM stream_schemas
WHERE org_id = @org_id AND pipeline_id = @pipeline_id;
