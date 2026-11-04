-- +goose Up
-- A Spark transform's outputs have their own schema history, apart from its
-- input streams', which can share their names.
ALTER TABLE stream_schemas ADD COLUMN output boolean NOT NULL DEFAULT false;
ALTER TABLE stream_schemas DROP CONSTRAINT stream_schemas_pkey;
ALTER TABLE stream_schemas DROP CONSTRAINT stream_schemas_org_id_pipeline_id_stream_digest_key;
ALTER TABLE stream_schemas ADD PRIMARY KEY (org_id, pipeline_id, output, stream, version);
ALTER TABLE stream_schemas ADD UNIQUE (org_id, pipeline_id, output, stream, digest);

-- +goose Down
DELETE FROM stream_schemas WHERE output;
ALTER TABLE stream_schemas DROP CONSTRAINT stream_schemas_pkey;
ALTER TABLE stream_schemas DROP CONSTRAINT stream_schemas_org_id_pipeline_id_output_stream_digest_key;
ALTER TABLE stream_schemas ADD PRIMARY KEY (org_id, pipeline_id, stream, version);
ALTER TABLE stream_schemas ADD UNIQUE (org_id, pipeline_id, stream, digest);
ALTER TABLE stream_schemas DROP COLUMN output;
