-- +goose Up
-- What a deleted pipeline's connectors and Flink jobs still have to remove,
-- such as a replication slot. A row lives until its teardown succeeds or has
-- been retried for days. It keeps what its runner reads, so deleting the
-- source, destination or Flink cluster doesn't lose it.
CREATE TABLE teardowns (
    org_id           uuid NOT NULL,
    -- The operation's, every try's.
    id               uuid NOT NULL,
    workspace_id     uuid NOT NULL,
    data_plane_id    text NOT NULL,
    -- The pipeline is gone, so no foreign key.
    pipeline_id      uuid NOT NULL,
    pipeline         text NOT NULL,
    op               text NOT NULL,
    connector        text NOT NULL DEFAULT '',
    -- One of them, or neither for a Flink cluster's.
    source_id        uuid,
    destination_id   uuid,
    -- The source's, destination's or Flink cluster's.
    name             text NOT NULL,
    spec             jsonb NOT NULL,
    version          text NOT NULL,
    connector_digest text NOT NULL DEFAULT '',
    secrets          text[] NOT NULL DEFAULT '{}',
    tries            integer NOT NULL DEFAULT 0,
    last_error       text,
    next_time        timestamptz NOT NULL DEFAULT now(),
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id)
);
CREATE INDEX teardowns_by_next_time ON teardowns (org_id, next_time);

-- +goose Down
DROP TABLE teardowns;
