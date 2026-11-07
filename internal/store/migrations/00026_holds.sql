-- +goose Up
-- Each pipeline's holds, as its newest mirrored checkpoint left
-- them. They're kept apart from the mirror, whose partitions go after 30
-- days, and replaced whole by each newer checkpoint.
CREATE TABLE holds (
    org_id           uuid NOT NULL,
    pipeline_id      uuid NOT NULL,
    stream           text NOT NULL,
    column_name      text NOT NULL,
    -- The column is held whole; otherwise some of its cells are.
    whole            boolean NOT NULL,
    reason           text NOT NULL,
    since_generation bigint NOT NULL,
    since_epoch      bigint NOT NULL,
    start_time       timestamptz,
    cells            bigint NOT NULL DEFAULT 0,
    discarded        boolean NOT NULL DEFAULT false,
    releasing        boolean NOT NULL DEFAULT false,
    PRIMARY KEY (org_id, pipeline_id, stream, column_name, whole),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- The checkpoint the pipeline's holds come from.
CREATE TABLE hold_checkpoints (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    generation  bigint NOT NULL,
    epoch       bigint NOT NULL,
    PRIMARY KEY (org_id, pipeline_id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Holds released or discarded, one each. A run takes a release while its
-- hold is open.
CREATE TABLE hold_releases (
    org_id           uuid NOT NULL,
    id               uuid NOT NULL,
    pipeline_id      uuid NOT NULL,
    stream           text NOT NULL,
    column_name      text NOT NULL,
    whole            boolean NOT NULL,
    since_generation bigint NOT NULL,
    since_epoch      bigint NOT NULL,
    discard          boolean NOT NULL,
    principal        text NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, pipeline_id, stream, column_name, whole, since_generation, since_epoch),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE hold_releases;
DROP TABLE hold_checkpoints;
DROP TABLE holds;
