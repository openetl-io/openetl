-- name: LockPipelineSchemas :one
SELECT id FROM pipelines WHERE org_id = @org_id AND id = @pipeline_id FOR NO KEY UPDATE;

-- name: AddStreamSchema :execrows
INSERT INTO stream_schemas (org_id, pipeline_id, stream, version, digest, "table", columns, arrow_schema, generation, epoch)
SELECT @org_id, @pipeline_id, @stream,
       coalesce(max(version), 0) + 1, @digest, @table_name, @columns, @arrow_schema, @generation, @epoch
FROM stream_schemas WHERE org_id = @org_id AND pipeline_id = @pipeline_id AND stream = @stream
ON CONFLICT (org_id, pipeline_id, stream, digest) DO NOTHING;

-- name: ListStreamSchemas :many
SELECT * FROM stream_schemas WHERE org_id = @org_id AND pipeline_id = @pipeline_id
ORDER BY stream, version;
