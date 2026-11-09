-- +goose Up
CREATE TABLE orgs (
    id                            uuid PRIMARY KEY,
    name                          text NOT NULL UNIQUE,
    create_time                   timestamptz NOT NULL DEFAULT now(),
    -- Whether the official source was ever connected, so deleting it sticks.
    official_connectors_connected boolean NOT NULL DEFAULT false
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

-- What each runner last reported of itself, for the plan's checks of
-- connectors against a data plane's runners, and for the placer. Capacities
-- and sizes are in millicores and bytes. A runner's orphans are attempts it
-- reports that aren't placed on it any more, which still use its room until
-- they stop. A pod launcher reports the jobs it takes and its caps, where a
-- max of 0 is no limit; a pod reports the one attempt it runs, and is never
-- placed on.
CREATE TABLE runners (
    org_id          uuid NOT NULL,
    data_plane_id   text NOT NULL,
    id              text NOT NULL,
    release         text NOT NULL,
    protocol        text NOT NULL,
    features        text[] NOT NULL,
    launchers       text[] NOT NULL,
    platform        text NOT NULL,
    report_time     timestamptz NOT NULL,
    pool            text NOT NULL DEFAULT 'default',
    capacity_cpu    bigint NOT NULL DEFAULT 0,
    capacity_memory bigint NOT NULL DEFAULT 0,
    reserved_cpu    bigint NOT NULL DEFAULT 0,
    reserved_memory bigint NOT NULL DEFAULT 0,
    orphan_cpu      bigint NOT NULL DEFAULT 0,
    orphan_memory   bigint NOT NULL DEFAULT 0,
    draining        boolean NOT NULL DEFAULT false,
    launcher        boolean NOT NULL DEFAULT false,
    pod_min_cpu     bigint NOT NULL DEFAULT 0,
    pod_min_memory  bigint NOT NULL DEFAULT 0,
    pod_max_cpu     bigint NOT NULL DEFAULT 0,
    pod_max_memory  bigint NOT NULL DEFAULT 0,
    max_pods        integer NOT NULL DEFAULT 0,
    max_pod_cpu     bigint NOT NULL DEFAULT 0,
    attempt         text NOT NULL DEFAULT '',
    PRIMARY KEY (org_id, data_plane_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id)
);

-- Content-addressed: the digest is sha256:{hex} of the content.
CREATE TABLE artifacts (
    org_id      uuid NOT NULL REFERENCES orgs (id),
    digest      text NOT NULL,
    media_type  text NOT NULL,
    content     bytea NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, digest)
);

-- Immutable. A connector version is a package's: owner/name and a version,
-- with a source role, a destination role or both. Each role's artifact is in
-- connector_roles; a v1alpha2 descriptor is one artifact for every role it
-- has, and v1alpha1 descriptors and manifests are one artifact per role.
CREATE TABLE connector_versions (
    org_id      uuid NOT NULL,
    name        text NOT NULL,
    version     text NOT NULL,
    -- Who applied it, and from where: such as cli:connectors.yaml#1.
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    author      text NOT NULL DEFAULT '',
    logo        text NOT NULL DEFAULT '',
    release_tag text NOT NULL DEFAULT '',
    release_url text NOT NULL DEFAULT '',
    description text NOT NULL DEFAULT '',
    license     text NOT NULL DEFAULT '',
    repository  text NOT NULL DEFAULT '',
    docs        text NOT NULL DEFAULT '',
    tags        text[] NOT NULL DEFAULT '{}',
    title       text NOT NULL DEFAULT '',
    logo_digest text,
    PRIMARY KEY (org_id, name, version)
);

CREATE TABLE connector_roles (
    org_id  uuid NOT NULL,
    name    text NOT NULL,
    version text NOT NULL,
    role    text NOT NULL CHECK (role IN ('source', 'destination')),
    digest  text NOT NULL,
    PRIMARY KEY (org_id, name, version, role),
    FOREIGN KEY (org_id, digest) REFERENCES artifacts (org_id, digest),
    FOREIGN KEY (org_id, name, version) REFERENCES connector_versions (org_id, name, version)
);
CREATE INDEX connector_roles_digest ON connector_roles (org_id, digest);

-- Connected sources of connectors: an OCI registry namespace or a GitHub
-- repository, listed into the catalog. secret is a secret's resource name,
-- workspaces/{workspace}/secrets/{secret}, read when the source is listed or
-- a runner fetches under its URL.
CREATE TABLE connector_sources (
    org_id        uuid NOT NULL REFERENCES orgs (id),
    name          text NOT NULL,
    url           text NOT NULL,
    username      text NOT NULL DEFAULT '',
    secret        text NOT NULL DEFAULT '',
    official      boolean NOT NULL DEFAULT false,
    create_time   timestamptz NOT NULL DEFAULT now(),
    update_time   timestamptz NOT NULL DEFAULT now(),
    -- The last listing without an error, and the last error since.
    refresh_time  timestamptz,
    error         text NOT NULL DEFAULT '',
    error_time    timestamptz,
    PRIMARY KEY (org_id, name)
);

-- Each package a source last listed, at its highest version, as its index
-- card or release says. card_digest tells a listing what it has fetched
-- already; descriptor_digest is the highest version's descriptor, fetched
-- when someone opens the package, and logo_digest its logo; both are
-- artifacts.
CREATE TABLE connector_catalog (
    org_id            uuid NOT NULL,
    source            text NOT NULL,
    package           text NOT NULL,
    -- The index tag or repository it was listed by.
    key               text NOT NULL,
    title             text NOT NULL,
    version           text NOT NULL,
    author            text NOT NULL DEFAULT '',
    description       text NOT NULL DEFAULT '',
    license           text NOT NULL DEFAULT '',
    repository        text NOT NULL DEFAULT '',
    docs              text NOT NULL DEFAULT '',
    roles             text[] NOT NULL,
    tags              text[] NOT NULL DEFAULT '{}',
    reference         text NOT NULL,
    card_digest       text NOT NULL,
    logo_digest       text,
    descriptor_digest text,
    update_time       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source, package),
    FOREIGN KEY (org_id, source) REFERENCES connector_sources (org_id, name) ON DELETE CASCADE
);

-- Each org's data key, wrapped by the install's key-encryption key.
CREATE TABLE org_keys (
    org_id      uuid PRIMARY KEY REFERENCES orgs (id),
    wrapped_key bytea NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now()
);

-- A secret holds a value, sealed with its org's data key and bound to its ID,
-- or a reference to a cloud secret store that runners read. Only sealed
-- values need the org's data key.
CREATE TABLE secrets (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    ciphertext   bytea,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    ref          text,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id),
    CONSTRAINT secrets_value_or_ref CHECK ((ciphertext IS NULL) <> (ref IS NULL))
);

-- Users and service accounts. A disabled principal's keys don't work.
CREATE TABLE principals (
    org_id       uuid NOT NULL REFERENCES orgs (id),
    id           uuid NOT NULL,
    name         text NOT NULL,
    -- user or service_account.
    type         text NOT NULL,
    display_name text NOT NULL DEFAULT '',
    email        text NOT NULL DEFAULT '',
    disabled     boolean NOT NULL DEFAULT false,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, name)
);

-- Users sign in with their email, so it's unique among an org's users.
CREATE UNIQUE INDEX principals_user_email ON principals (org_id, lower(email)) WHERE type = 'user' AND email <> '';

-- Keys by their SHA-256; prefix is the key's first characters, to tell
-- keys apart.
CREATE TABLE api_keys (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    principal_id   uuid NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    prefix         text NOT NULL,
    expire_time    timestamptz,
    last_used_time timestamptz,
    create_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- A NULL workspace means the org.
CREATE TABLE role_bindings (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    role         text NOT NULL,
    workspace_id uuid,
    create_time  timestamptz NOT NULL DEFAULT now(),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

CREATE UNIQUE INDEX role_bindings_unique ON role_bindings (org_id, principal_id, role, workspace_id) NULLS NOT DISTINCT;

CREATE TABLE runner_tokens (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    data_plane_id  text NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    prefix         text NOT NULL,
    last_used_time timestamptz,
    create_time    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id) ON DELETE CASCADE
);

-- Users' passwords, as argon2id PHC strings. failures counts sign-ins that
-- failed in a row; locked_until refuses them for a while after too many.
CREATE TABLE passwords (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    hash         text NOT NULL,
    failures     integer NOT NULL DEFAULT 0,
    locked_until timestamptz,
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, principal_id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- One-use links that set a user's password, by their token's SHA-256; a
-- user has at most one.
CREATE TABLE password_links (
    org_id       uuid NOT NULL,
    principal_id uuid NOT NULL,
    hash         bytea NOT NULL UNIQUE,
    expire_time  timestamptz NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, principal_id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

-- Browsers' sign-ins, by their cookie's SHA-256. A session ends at
-- expire_time, or once it's idle for the server's idle timeout.
CREATE TABLE sessions (
    org_id         uuid NOT NULL,
    id             uuid NOT NULL,
    principal_id   uuid NOT NULL,
    hash           bytea NOT NULL UNIQUE,
    expire_time    timestamptz NOT NULL,
    last_used_time timestamptz NOT NULL DEFAULT now(),
    create_time    timestamptz NOT NULL DEFAULT now(),
    peer           text NOT NULL DEFAULT '',
    user_agent     text NOT NULL DEFAULT '',
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, principal_id) REFERENCES principals (org_id, id) ON DELETE CASCADE
);

CREATE INDEX sessions_principal ON sessions (org_id, principal_id);

CREATE TABLE sources (
    org_id           uuid NOT NULL,
    workspace_id     uuid NOT NULL,
    id               uuid NOT NULL,
    name             text NOT NULL,
    spec             jsonb NOT NULL,
    version          text NOT NULL,
    revision         bigint NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    update_time      timestamptz NOT NULL DEFAULT now(),
    -- The connector version it resolved to when it was applied; empty for a
    -- built-in connector, or one a runner finds in its connectors directory.
    connector_digest text NOT NULL DEFAULT '',
    -- The secrets it names, in its workspace. A secret can't be deleted while
    -- one names it.
    secrets          text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Immutable: every config a source has had. A pipeline version names the one
-- it runs.
CREATE TABLE source_versions (
    org_id           uuid NOT NULL,
    source_id        uuid NOT NULL,
    version          text NOT NULL,
    spec             jsonb NOT NULL,
    connector_digest text NOT NULL,
    secrets          text[] NOT NULL,
    -- Who applied it, and from where: such as cli:shop.yaml#1.
    provenance       text NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source_id, version),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE
);

-- Catalogs that runners discovered, one row per distinct catalog of a source.
CREATE TABLE source_catalogs (
    org_id         uuid NOT NULL,
    source_id      uuid NOT NULL,
    -- SHA-256 of the streams (openetl.api.v1.Catalog, deterministic).
    digest         text NOT NULL,
    -- The source version last discovered with it.
    source_version text NOT NULL,
    -- openetl.api.v1.Catalog as protojson, streams only.
    catalog        jsonb NOT NULL,
    -- When it was last discovered.
    discover_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, source_id, digest),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE
);

-- As sources.
CREATE TABLE destinations (
    org_id           uuid NOT NULL,
    workspace_id     uuid NOT NULL,
    id               uuid NOT NULL,
    name             text NOT NULL,
    spec             jsonb NOT NULL,
    version          text NOT NULL,
    revision         bigint NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    update_time      timestamptz NOT NULL DEFAULT now(),
    connector_digest text NOT NULL DEFAULT '',
    secrets          text[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- As source_versions.
CREATE TABLE destination_versions (
    org_id           uuid NOT NULL,
    destination_id   uuid NOT NULL,
    version          text NOT NULL,
    spec             jsonb NOT NULL,
    connector_digest text NOT NULL,
    secrets          text[] NOT NULL,
    provenance       text NOT NULL,
    create_time      timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, destination_id, version),
    FOREIGN KEY (org_id, destination_id) REFERENCES destinations (org_id, id) ON DELETE CASCADE
);

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

-- A Spark cluster is where a pipeline's Spark applications run. Pipeline
-- versions pin a version of its settings, as they pin a source's.
CREATE TABLE spark_clusters (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    spec         jsonb NOT NULL,
    version      text NOT NULL,
    secrets      text[] NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Immutable: every setting a Spark cluster has had.
CREATE TABLE spark_cluster_versions (
    org_id      uuid NOT NULL,
    cluster_id  uuid NOT NULL,
    version     text NOT NULL,
    spec        jsonb NOT NULL,
    secrets     text[] NOT NULL,
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, cluster_id, version),
    FOREIGN KEY (org_id, cluster_id) REFERENCES spark_clusters (org_id, id) ON DELETE CASCADE
);

-- A Flink cluster is where a continuous pipeline's Flink job runs. Pipeline
-- versions pin a version of its settings, as they pin a source's.
CREATE TABLE flink_clusters (
    org_id       uuid NOT NULL,
    workspace_id uuid NOT NULL,
    id           uuid NOT NULL,
    name         text NOT NULL,
    spec         jsonb NOT NULL,
    version      text NOT NULL,
    secrets      text[] NOT NULL,
    revision     bigint NOT NULL,
    create_time  timestamptz NOT NULL DEFAULT now(),
    update_time  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Immutable: every setting a Flink cluster has had.
CREATE TABLE flink_cluster_versions (
    org_id      uuid NOT NULL,
    cluster_id  uuid NOT NULL,
    version     text NOT NULL,
    spec        jsonb NOT NULL,
    secrets     text[] NOT NULL,
    provenance  text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, cluster_id, version),
    FOREIGN KEY (org_id, cluster_id) REFERENCES flink_clusters (org_id, id) ON DELETE CASCADE
);

CREATE TABLE pipelines (
    org_id               uuid NOT NULL,
    workspace_id         uuid NOT NULL,
    id                   uuid NOT NULL,
    name                 text NOT NULL,
    source_id            uuid NOT NULL,
    destination_id       uuid NOT NULL,
    data_plane_id        text NOT NULL,
    -- The current entry in pipeline_versions.
    version              text NOT NULL,
    revision             bigint NOT NULL,
    create_time          timestamptz NOT NULL DEFAULT now(),
    update_time          timestamptz NOT NULL DEFAULT now(),
    -- A paused pipeline's schedule is paused, and its syncs don't run.
    paused               boolean NOT NULL DEFAULT false,
    -- Since when a continuous pipeline's read has been further behind its
    -- source than the server's --lagging-after; unset while it isn't.
    lagging_since        timestamptz,
    -- Fixed when it's created and kept in its versions. A restore from a
    -- record with another count fails.
    key_groups           integer NOT NULL DEFAULT 256,
    -- When the API last started a sync by hand, which the sync's placement
    -- goes by; StartSync clears it.
    manual_sync_time     timestamptz,
    -- When the control plane last asked its source how much of its retention
    -- the pipeline has used, while it wasn't running.
    retention_probe_time timestamptz,
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
    org_id              uuid NOT NULL,
    pipeline_id         uuid NOT NULL,
    version             text NOT NULL,
    spec                jsonb NOT NULL,
    source_id           uuid NOT NULL,
    destination_id      uuid NOT NULL,
    data_plane_id       text NOT NULL,
    create_time         timestamptz NOT NULL DEFAULT now(),
    source_version      text NOT NULL,
    destination_version text NOT NULL,
    -- The bundle of each dbt project the post-sync steps name, by name.
    dbt                 jsonb NOT NULL,
    provenance          text NOT NULL,
    -- The version of each Spark cluster the post-sync steps name, by name.
    spark_clusters      jsonb NOT NULL DEFAULT '{}',
    -- The secrets its sparkAccess names, so they can't be deleted while it is
    -- the pipeline's current version.
    secrets             text[] NOT NULL DEFAULT '{}',
    -- The version of the Flink cluster the transform names, by name.
    flink_clusters      jsonb NOT NULL DEFAULT '{}',
    key_groups          integer NOT NULL DEFAULT 256,
    PRIMARY KEY (org_id, pipeline_id, version),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Streams paused on their own. Syncs skip them and carry their state
-- forward. incident_id is the incident whose playbook paused one; null if
-- a person did.
CREATE TABLE paused_streams (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    stream      text NOT NULL,
    reason      text NOT NULL,
    incident_id uuid,
    create_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, stream),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Values heal set for fields of a pipeline's spec, which syncs use in place
-- of the spec's: field is schedule.every or source.rate_limit_scale, and
-- value its JSON, as a PlanChange has it.
CREATE TABLE pipeline_overrides (
    org_id      uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    field       text NOT NULL,
    value       jsonb NOT NULL,
    reason      text NOT NULL,
    incident_id uuid,
    update_time timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, pipeline_id, field),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- What rescaled a pipeline's jobs besides its spec: RescalePipeline's size,
-- for one version, and the job loop's lanes, for a continuous pipeline.
CREATE TABLE job_scales (
    org_id       uuid NOT NULL,
    pipeline_id  uuid NOT NULL,
    -- RescalePipeline's, in millicores and bytes; 0 if none.
    size_cpu     bigint NOT NULL DEFAULT 0,
    size_memory  bigint NOT NULL DEFAULT 0,
    -- The version it holds for.
    size_version text NOT NULL DEFAULT '',
    -- The job loop's; 0 if it hasn't chosen.
    lanes        integer NOT NULL DEFAULT 0,
    -- Why the job loop isn't rescaling it, such as destination_bound.
    note         text NOT NULL DEFAULT '',
    rescale_time timestamptz,
    PRIMARY KEY (org_id, pipeline_id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- Checks and discovers the API has sent to a data plane. Its runners read a
-- connector's config only for one of these. A teardown of a deleted
-- pipeline's Flink jobs names the pipeline and the version of the Flink
-- cluster its last version pinned, since the pipeline's own rows go with it.
CREATE TABLE connector_ops (
    org_id           uuid NOT NULL,
    id               uuid NOT NULL,
    data_plane_id    text NOT NULL,
    source_id        uuid,
    destination_id   uuid,
    expire_time      timestamptz NOT NULL,
    pipeline_id      uuid,
    flink_cluster_id uuid,
    flink_version    text,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, source_id) REFERENCES sources (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, destination_id) REFERENCES destinations (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, flink_cluster_id, flink_version)
        REFERENCES flink_cluster_versions (org_id, cluster_id, version) ON DELETE CASCADE
);

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

-- A continuous pipeline's runs are syncs too: each runs one version until it
-- is stopped (status stopped) or fails.
CREATE TABLE syncs (
    org_id                uuid NOT NULL,
    id                    uuid NOT NULL,
    pipeline_id           uuid NOT NULL,
    version               text NOT NULL,
    workflow_id           text NOT NULL,
    run_id                text NOT NULL,
    -- running, succeeded or failed.
    status                text NOT NULL,
    error                 text,
    -- Rows, epochs and the like, as JSON.
    stats                 jsonb,
    start_time            timestamptz NOT NULL DEFAULT now(),
    end_time              timestamptz,
    continuous            boolean NOT NULL DEFAULT false,
    -- Its attempts that failed, as runners report them. A continuous run's
    -- failures since its last commit tell a crash loop.
    failed_attempts       integer NOT NULL DEFAULT 0,
    last_failed_attempt   integer NOT NULL DEFAULT 0,
    failures_since_commit integer NOT NULL DEFAULT 0,
    last_attempt_error    text,
    last_attempt_code     text,
    last_attempt_time     timestamptz,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, run_id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE INDEX syncs_by_pipeline ON syncs (org_id, pipeline_id, start_time DESC);

-- Work a workflow sent to a data plane: attempts, which the placer places on
-- a runner, and light operations, which any runner takes. pending -> placed
-- -> running -> finished for an attempt; pending -> running -> finished for a
-- light operation. Deadlines are on the database's clock.
CREATE TABLE dispatches (
    org_id           uuid NOT NULL REFERENCES orgs (id) ON DELETE CASCADE,
    id               text NOT NULL,
    data_plane_id    text NOT NULL,
    -- Woken when the work finishes.
    workflow_id      text NOT NULL,
    kind             text NOT NULL,
    input            jsonb NOT NULL,
    -- Empty for an attempt; otherwise the kind of light operation.
    light            text NOT NULL DEFAULT '',
    pool             text NOT NULL DEFAULT 'default',
    size_cpu         bigint NOT NULL DEFAULT 0,
    size_memory      bigint NOT NULL DEFAULT 0,
    priority         smallint NOT NULL DEFAULT 0,
    -- What a runner needs to run it: dispatch.Need.
    needs            jsonb NOT NULL DEFAULT '[]',
    -- The trace it's part of, as W3C headers.
    trace            jsonb NOT NULL DEFAULT '{}',
    state            text NOT NULL DEFAULT 'pending',
    runner           text,
    heartbeat_ms     bigint NOT NULL,
    -- How long after being placed it must start; for a light operation, how
    -- long it waits for a runner in all.
    start_timeout_ms bigint NOT NULL,
    timeout_ms       bigint NOT NULL,
    enqueue_time     timestamptz NOT NULL DEFAULT now(),
    start_deadline   timestamptz,
    run_deadline     timestamptz,
    lease_deadline   timestamptz,
    -- A workflow asked it to stop.
    cancel           boolean NOT NULL DEFAULT false,
    epoch            bigint NOT NULL DEFAULT 0,
    -- dispatch.Outcome.
    outcome          jsonb,
    finish_time      timestamptz,
    -- The workflow has been told it finished.
    woken            boolean NOT NULL DEFAULT false,
    -- A rescale asked the running attempt to stop at a checkpoint and start
    -- again.
    drain            boolean NOT NULL DEFAULT false,
    -- An attempt placed on a pod launcher keeps the launcher's ID while its
    -- pod runs it, which counts against the launcher's caps.
    launcher         text,
    -- Each placement has a number, so a pod of an earlier one can't take it.
    placements       integer NOT NULL DEFAULT 0,
    -- The launcher has taken it to make a pod.
    launched         boolean NOT NULL DEFAULT false,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, data_plane_id) REFERENCES data_planes (org_id, id)
);
CREATE INDEX dispatches_pending ON dispatches (org_id, data_plane_id, priority, enqueue_time) WHERE state = 'pending' AND light = '';
CREATE INDEX dispatches_light ON dispatches (org_id, data_plane_id, light, enqueue_time) WHERE state = 'pending' AND light <> '';
CREATE INDEX dispatches_on_runner ON dispatches (org_id, data_plane_id, runner) WHERE state IN ('placed', 'running');
CREATE INDEX dispatches_on_launcher ON dispatches (org_id, data_plane_id, launcher) WHERE state IN ('placed', 'running') AND launcher IS NOT NULL;
CREATE INDEX dispatches_leases ON dispatches (org_id, lease_deadline) WHERE state = 'running';
CREATE INDEX dispatches_starts ON dispatches (org_id, start_deadline) WHERE state IN ('placed', 'pending');
CREATE INDEX dispatches_unwoken ON dispatches (org_id, finish_time) WHERE state = 'finished' AND NOT woken;
CREATE INDEX dispatches_finished ON dispatches (org_id, finish_time) WHERE state = 'finished';

-- Each attempt of a sync or a continuous run: what it was sized at and why,
-- and what it used. Dispatches are gone a day after they finish; sizing goes
-- by the last few attempts, however long ago they ran.
CREATE TABLE attempts (
    org_id                uuid NOT NULL,
    -- Its dispatch's.
    id                    text NOT NULL,
    pipeline_id           uuid NOT NULL,
    sync_id               uuid NOT NULL,
    attempt               integer NOT NULL,
    -- In millicores and bytes.
    size_cpu              bigint NOT NULL,
    size_memory           bigint NOT NULL,
    -- sizing.Reason.
    size_reason           text NOT NULL,
    -- All as its runner last reported.
    cpu_seconds           double precision NOT NULL DEFAULT 0,
    peak_memory           bigint NOT NULL DEFAULT 0,
    runner_cpu_seconds    double precision NOT NULL DEFAULT 0,
    runner_memory         bigint NOT NULL DEFAULT 0,
    seconds               double precision NOT NULL DEFAULT 0,
    rows                  bigint NOT NULL DEFAULT 0,
    bytes                 bigint NOT NULL DEFAULT 0,
    -- The taxonomy code it failed with, as its runner reported it.
    code                  text NOT NULL DEFAULT '',
    create_time           timestamptz NOT NULL DEFAULT now(),
    -- Its lanes, as its runner last reported them: how many, the most it
    -- could use (0: no limit), and where their time went, summed over them.
    lanes                 integer NOT NULL DEFAULT 0,
    lane_limit            integer NOT NULL DEFAULT 0,
    busy_seconds          double precision NOT NULL DEFAULT 0,
    backpressured_seconds double precision NOT NULL DEFAULT 0,
    idle_seconds          double precision NOT NULL DEFAULT 0,
    lane_rows             bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, sync_id) REFERENCES syncs (org_id, id) ON DELETE CASCADE
);
CREATE INDEX attempts_by_pipeline ON attempts (org_id, pipeline_id, create_time DESC);
CREATE INDEX attempts_by_sync ON attempts (org_id, sync_id, attempt);

-- Running stream jobs' lanes every 30 s, for the job loop's windows. Kept
-- for an hour.
CREATE TABLE lane_samples (
    org_id                uuid NOT NULL,
    attempt_id            text NOT NULL,
    pipeline_id           uuid NOT NULL,
    sample_time           timestamptz NOT NULL DEFAULT now(),
    lanes                 integer NOT NULL,
    lane_limit            integer NOT NULL,
    busy_seconds          double precision NOT NULL,
    backpressured_seconds double precision NOT NULL,
    idle_seconds          double precision NOT NULL,
    lane_rows             bigint NOT NULL,
    PRIMARY KEY (org_id, attempt_id, sample_time)
);
CREATE INDEX lane_samples_by_time ON lane_samples (org_id, sample_time);

-- A mirror of the stream schemas in each pipeline's checkpoint log: a new
-- version per stream each time its schema changes. A Spark transform's
-- outputs (output) have their own schema history, apart from its input
-- streams', which can share their names.
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
    output       boolean NOT NULL DEFAULT false,
    PRIMARY KEY (org_id, pipeline_id, output, stream, version),
    UNIQUE (org_id, pipeline_id, output, stream, digest),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- A mirror of each pipeline's checkpoint log: one row per record, reported
-- by runners after each commit. Partitioned by day of commit_time; the
-- store creates each day's partition when a report first needs it.
CREATE TABLE checkpoint_mirror (
    org_id              uuid NOT NULL,
    pipeline_id         uuid NOT NULL,
    epoch               bigint NOT NULL,
    generation          bigint NOT NULL,
    -- The record before it in the log; null for the first.
    previous_epoch      bigint,
    previous_generation bigint,
    -- The sync's workflow run (syncs.run_id).
    run_id              text NOT NULL,
    -- When the data plane recorded it.
    commit_time         timestamptz NOT NULL,
    -- [{name, rows, quarantined_rows, forced, complete, schema_digest}];
    -- schema_digest joins stream_schemas.digest.
    streams             jsonb NOT NULL,
    create_time         timestamptz NOT NULL DEFAULT now(),
    -- What the read last reported of its progress when the checkpoint was
    -- recorded: [{stream, lag_bytes, lag_records, lag_ms,
    -- retention_left_bytes, retention_left_ms}], each field present only if
    -- the source reported it.
    progress            jsonb,
    -- What the attempt had used when it committed the checkpoint, as JSON.
    usage               jsonb,
    PRIMARY KEY (org_id, pipeline_id, epoch, generation, commit_time),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
) PARTITION BY RANGE (commit_time);

-- What a person asked of a pipeline's checkpoint log, for its next attempt
-- to take: reading streams again from the start (reset), or restarting from
-- the checkpoint before one that can't be read (rewind).
CREATE TABLE checkpoint_requests (
    org_id      uuid NOT NULL,
    -- A UUIDv7, so IDs order requests by creation.
    id          uuid NOT NULL,
    pipeline_id uuid NOT NULL,
    kind        text NOT NULL CHECK (kind IN ('reset', 'rewind')),
    -- A reset's streams; empty for every stream.
    streams     text[] NOT NULL DEFAULT '{}',
    principal   text NOT NULL,
    create_time timestamptz NOT NULL DEFAULT now(),
    -- When an attempt was seen to take it, or for a rewind, when the
    -- pipeline made progress without needing it; null while pending.
    take_time   timestamptz,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

CREATE INDEX checkpoint_requests_pending ON checkpoint_requests (org_id, pipeline_id, id) WHERE take_time IS NULL;

-- Each pipeline's holds, as its newest mirrored checkpoint left them. They're
-- kept apart from the mirror, whose partitions go after 30 days, and replaced
-- whole by each newer checkpoint.
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
    -- Why it's held, as the API's HoldCause in JSON.
    cause            jsonb,
    -- The release that didn't clear it, which isn't sent again until the hold
    -- is released again.
    refused_release  uuid,
    refusal          text NOT NULL DEFAULT '',
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
    -- Send the hold's values to a new version of the column, col__v2, instead
    -- of the column.
    new_version      boolean NOT NULL DEFAULT false,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, pipeline_id, stream, column_name, whole, since_generation, since_epoch),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE
);

-- One row per audited call, written before the call runs and completed once
-- when it ends. code is null until then, and stays null if the server stopped
-- first. Nothing refers to principals or resources, so rows outlive them.
-- Partitioned by month of time (UTC) so retention drops whole months:
-- audit_log_{yyyymm}. The store creates a month's partition when an entry
-- first needs it, and Migrate this month's and the next ahead.
CREATE TABLE audit_log (
    org_id     uuid NOT NULL,
    -- UUIDv7, so ids sort by time.
    id         uuid NOT NULL,
    time       timestamptz NOT NULL,
    -- principals/{p} or dataPlanes/{dp}, or openetld for its own changes.
    actor      text NOT NULL,
    -- principals/{p}/keys/{k} or dataPlanes/{dp}/tokens/{t}; empty for
    -- openetld.
    credential text NOT NULL,
    -- The RPC's procedure, or what openetld did.
    method     text NOT NULL,
    -- The resources the call named or touched, secrets it read included.
    resources  text[] NOT NULL,
    details    jsonb NOT NULL DEFAULT '{}',
    peer       text NOT NULL DEFAULT '',
    user_agent text NOT NULL DEFAULT '',
    -- A Connect code, such as ok or permission_denied.
    code       text,
    error      text NOT NULL DEFAULT '',
    end_time   timestamptz,
    PRIMARY KEY (org_id, id, time)
) PARTITION BY RANGE (time);

CREATE INDEX audit_log_resources ON audit_log USING gin (resources);
CREATE INDEX audit_log_actor ON audit_log (org_id, actor, id);

-- +goose StatementBegin
CREATE FUNCTION audit_log_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND OLD.code IS NULL AND NEW.code IS NOT NULL
        AND (NEW.org_id, NEW.id, NEW.time, NEW.actor, NEW.credential, NEW.method, NEW.peer, NEW.user_agent)
            = (OLD.org_id, OLD.id, OLD.time, OLD.actor, OLD.credential, OLD.method, OLD.peer, OLD.user_agent) THEN
        RETURN NEW;
    END IF;
    RAISE EXCEPTION 'audit_log is append-only: an entry is completed once, and never changes after';
END
$$;
-- +goose StatementEnd

CREATE TRIGGER audit_log_append_only BEFORE UPDATE OR DELETE ON audit_log
FOR EACH ROW EXECUTE FUNCTION audit_log_append_only();

-- Where a workspace's platform events go. spec is the document's WebhookSpec;
-- secrets are the ones it signs with, which can't be deleted while it names
-- them.
CREATE TABLE webhooks (
    org_id            uuid NOT NULL,
    workspace_id      uuid NOT NULL,
    id                uuid NOT NULL,
    name              text NOT NULL,
    spec              jsonb NOT NULL,
    version           text NOT NULL,
    secrets           text[] NOT NULL DEFAULT '{}',
    revision          bigint NOT NULL,
    disabled          boolean NOT NULL DEFAULT false,
    disabled_reason   text,
    failing_since     timestamptz,
    create_time       timestamptz NOT NULL DEFAULT now(),
    update_time       timestamptz NOT NULL DEFAULT now(),
    -- When its endpoint last failed: a quiet spell restarts how long it has
    -- been failing, so one failure after it doesn't disable the webhook.
    last_failure_time timestamptz,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, workspace_id, name),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);

-- Platform events, each written in the transaction that causes it. The
-- notifier fans each out to the webhooks that match it once, and sets
-- dispatch_time. A key, where set, records an event once however often the
-- code that writes it is retried. A webhook_id sends it to that webhook alone.
CREATE TABLE outbox (
    org_id        uuid NOT NULL REFERENCES orgs (id),
    id            uuid NOT NULL,
    workspace_id  uuid NOT NULL,
    type          text NOT NULL,
    pipeline      text,
    data          jsonb NOT NULL,
    key           text,
    webhook_id    uuid,
    create_time   timestamptz NOT NULL DEFAULT now(),
    dispatch_time timestamptz,
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, key),
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id),
    FOREIGN KEY (org_id, webhook_id) REFERENCES webhooks (org_id, id) ON DELETE CASCADE
);
CREATE INDEX outbox_undispatched ON outbox (org_id, create_time) WHERE dispatch_time IS NULL;
CREATE INDEX outbox_create_time ON outbox (org_id, create_time);

-- One event's delivery to one webhook, with its latest attempt. A notifier
-- claims a pending one by moving next_attempt_time past its lease.
CREATE TABLE webhook_deliveries (
    org_id            uuid NOT NULL,
    id                uuid NOT NULL,
    webhook_id        uuid NOT NULL,
    event_id          uuid NOT NULL,
    -- pending, succeeded or failed.
    state             text NOT NULL DEFAULT 'pending',
    attempts          integer NOT NULL DEFAULT 0,
    next_attempt_time timestamptz NOT NULL DEFAULT now(),
    last_attempt_time timestamptz,
    response_status   integer,
    response_body     text,
    error             text,
    latency_ms        integer,
    create_time       timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (org_id, id),
    UNIQUE (org_id, webhook_id, event_id),
    FOREIGN KEY (org_id, webhook_id) REFERENCES webhooks (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, event_id) REFERENCES outbox (org_id, id) ON DELETE CASCADE
);
CREATE INDEX webhook_deliveries_due ON webhook_deliveries (org_id, next_attempt_time) WHERE state = 'pending';
CREATE INDEX webhook_deliveries_by_webhook ON webhook_deliveries (org_id, webhook_id, create_time DESC, id DESC);

-- Failures and conditions that need more than a retry, one per pipeline,
-- stream and fingerprint while open. The fingerprint leaves the pipeline out,
-- so the same one on many pipelines points at their connector.
CREATE TABLE incidents (
    org_id          uuid NOT NULL,
    id              uuid NOT NULL,
    workspace_id    uuid NOT NULL,
    pipeline_id     uuid NOT NULL,
    -- Empty: the whole pipeline.
    stream          text NOT NULL DEFAULT '',
    -- sync_failed, auth, schema_blocked, rate_limited, position_lost,
    -- crash_loop, lagging, retention_at_risk or quarantine.
    class           text NOT NULL,
    -- The taxonomy code of the error it came from, if any.
    code            text,
    fingerprint     text NOT NULL,
    -- open or resolved.
    status          text NOT NULL DEFAULT 'open',
    -- warning or critical.
    severity        text NOT NULL,
    summary         text NOT NULL,
    message         text NOT NULL,
    next_steps      text NOT NULL DEFAULT '',
    -- What it was seen with last: lag, shares and the like.
    details         jsonb NOT NULL DEFAULT '{}',
    -- [{kind, state, description, time}]: what heal did, or proposes.
    actions         jsonb NOT NULL DEFAULT '[]',
    -- The sync or run it was seen in last.
    sync_id         uuid,
    -- What made the last occurrence; a repeat with the same key isn't one.
    occurrence_key  text NOT NULL,
    occurrences     integer NOT NULL DEFAULT 1,
    first_seen_time timestamptz NOT NULL DEFAULT now(),
    last_seen_time  timestamptz NOT NULL DEFAULT now(),
    resolve_time    timestamptz,
    resolution      text,
    PRIMARY KEY (org_id, id),
    FOREIGN KEY (org_id, pipeline_id) REFERENCES pipelines (org_id, id) ON DELETE CASCADE,
    FOREIGN KEY (org_id, workspace_id) REFERENCES workspaces (org_id, id)
);
CREATE UNIQUE INDEX incidents_open ON incidents (org_id, pipeline_id, stream, fingerprint) WHERE status = 'open';
CREATE INDEX incidents_by_pipeline ON incidents (org_id, pipeline_id, first_seen_time DESC, id DESC);
CREATE INDEX incidents_by_workspace ON incidents (org_id, workspace_id, first_seen_time DESC, id DESC);

-- +goose Down
DROP TABLE incidents;
DROP TABLE webhook_deliveries;
DROP TABLE outbox;
DROP TABLE webhooks;
DROP TABLE audit_log;
DROP FUNCTION audit_log_append_only;
DROP TABLE hold_releases;
DROP TABLE hold_checkpoints;
DROP TABLE holds;
DROP TABLE checkpoint_requests;
DROP TABLE checkpoint_mirror;
DROP TABLE stream_schemas;
DROP TABLE lane_samples;
DROP TABLE attempts;
DROP TABLE dispatches;
DROP TABLE syncs;
DROP TABLE teardowns;
DROP TABLE connector_ops;
DROP TABLE job_scales;
DROP TABLE pipeline_overrides;
DROP TABLE paused_streams;
DROP TABLE pipeline_versions;
DROP TABLE pipelines;
DROP TABLE flink_cluster_versions;
DROP TABLE flink_clusters;
DROP TABLE spark_cluster_versions;
DROP TABLE spark_clusters;
DROP TABLE dbt_projects;
DROP TABLE destination_versions;
DROP TABLE destinations;
DROP TABLE source_catalogs;
DROP TABLE source_versions;
DROP TABLE sources;
DROP TABLE sessions;
DROP TABLE password_links;
DROP TABLE passwords;
DROP TABLE runner_tokens;
DROP TABLE role_bindings;
DROP TABLE api_keys;
DROP TABLE principals;
DROP TABLE secrets;
DROP TABLE org_keys;
DROP TABLE connector_catalog;
DROP TABLE connector_sources;
DROP TABLE connector_roles;
DROP TABLE connector_versions;
DROP TABLE artifacts;
DROP TABLE runners;
DROP TABLE data_planes;
DROP TABLE workspaces;
DROP TABLE orgs;
