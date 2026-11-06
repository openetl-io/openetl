-- +goose Up
-- A mirror of the stream schemas in each pipeline's checkpoint log: a new
-- version per stream each time its schema changes.
CREATE TABLE stream_schemas (
    org_id       uuid NOT NULL,
    pipeline_id  uuid NOT NULL,
    stream       text NOT NULL,
    -- 1, 2, ... per stream.
    version      integer NOT NULL,
    -- Of the table and columns, so a schema is recorded once.
    digest       text NOT NULL,
    "table"      text NOT NULL,
    -- [{name, type, nullable, source}], source only where renamed.
    columns      jsonb NOT NULL,
    arrow_schema bytea NOT NULL,
    -- The first checkpoint that carried it.
    generation   bigint NOT NULL,
    epoch        bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, stream, version),
    UNIQUE (org_id, pipeline_id, stream, digest),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE stream_schemas;
