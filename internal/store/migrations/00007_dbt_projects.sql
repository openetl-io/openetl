-- +goose Up
-- A dbt project is a name for its current bundle, an artifact. Pipelines name
-- projects in their post-sync steps, and a sync runs the bundle that was
-- current when it started.
CREATE TABLE dbt_projects (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    digest       text NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id),
    FOREIGN KEY (org_id, digest) REFERENCES artifacts (org_id, digest)
);

-- +goose Down
DROP TABLE dbt_projects;
