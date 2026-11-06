-- +goose Up
CREATE TABLE orgs (
    id          uuid PRIMARY KEY,
    name        text NOT NULL UNIQUE,
    create_time timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE workspaces (
    org_id      uuid NOT NULL REFERENCES orgs (id),
    id          uuid NOT NULL,
    name        text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, name)
);

-- The ID names the data plane's task queues, so it is unique per install.
CREATE TABLE data_planes (
    id          text PRIMARY KEY,
    org_id      uuid NOT NULL REFERENCES orgs (id),
    create_time timestamptz NOT NULL DEFAULT now(),
    UNIQUE (org_id, id)
);

CREATE TABLE sources (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    spec         jsonb NOT NULL,
    version      text NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

CREATE TABLE destinations (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    spec         jsonb NOT NULL,
    version      text NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

CREATE TABLE pipelines (
    org_id         uuid NOT NULL,
    workspace_id   uuid NOT NULL,
    id             uuid NOT NULL,
    name           text NOT NULL,
    source_id      uuid NOT NULL,
    destination_id uuid NOT NULL,
    data_plane_id  text NOT NULL,
    -- The current entry in pipeline_versions.
    version        text NOT NULL,
    revision       bigint NOT NULL,
    create_time    timestamptz NOT NULL DEFAULT now(),
    update_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id),
    FOREIGN KEY (org_id, destination_id) REFERENCES destinations (org_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id)
);

-- Immutable. A version names its source and destination by ID, so it
-- changes when a source with the same name is recreated.
CREATE TABLE pipeline_versions (
    org_id         uuid NOT NULL,
    pipeline_id    uuid NOT NULL,
    version        text NOT NULL,
    spec           jsonb NOT NULL,
    source_id      uuid NOT NULL,
    destination_id uuid NOT NULL,
    data_plane_id  text NOT NULL,
    create_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, version),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE TABLE syncs (
    org_id      uuid NOT NULL,
    id          uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    version     text NOT NULL,
    workflow_id text NOT NULL,
    run_id      text NOT NULL,
    -- running, succeeded or failed.
    status      text NOT NULL,
    error       text,
    -- Rows, epochs and the like, as JSON.
    stats       jsonb,
    start_time  timestamptz NOT NULL DEFAULT now(),
    end_time    timestamptz,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, run_id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE INDEX syncs_by_pipeline ON syncs (org_id, pipeline_id, start_time DESC);

-- +goose Down
DROP TABLE syncs;
DROP TABLE pipeline_versions;
DROP TABLE pipelines;
DROP TABLE destinations;
DROP TABLE sources;
DROP TABLE data_planes;
DROP TABLE workspaces;
DROP TABLE orgs;
