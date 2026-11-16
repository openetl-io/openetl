-- +goose Up
-- Attempts' placements on pod launchers, and the room each holds of its
-- launcher's caps. A placement's pods outlive it until its launcher has
-- deleted them, so it holds that room from when it's placed until it has
-- ended and its launcher reports no pod of it left that hasn't.
CREATE TABLE launches (
    org_id        uuid NOT NULL,
    data_plane_id text NOT NULL,
    launcher      text NOT NULL,
    -- The attempt's dispatch, and which of its placements.
    id            text NOT NULL,
    placement     integer NOT NULL,
    -- Its pods, each worker's and its coordinator's or its one, and their
    -- CPU and memory between them.
    pods          integer NOT NULL,
    cpu           bigint NOT NULL,
    memory        bigint NOT NULL,
    PRIMARY KEY (org_id, data_plane_id, launcher, id, placement),
    FOREIGN KEY (org_id, data_plane_id, launcher) REFERENCES runners (org_id, data_plane_id, id) ON DELETE CASCADE
);

-- +goose Down
DROP TABLE launches;
