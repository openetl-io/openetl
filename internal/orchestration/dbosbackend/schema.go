package dbosbackend

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// schemaLock keeps two orchestrators starting at once from making the schema
// together.
const schemaLock = 7236401

const schema = `
CREATE SCHEMA IF NOT EXISTS openetl;

CREATE TABLE IF NOT EXISTS openetl.generations (
	id         text PRIMARY KEY,
	generation int  NOT NULL
);

CREATE TABLE IF NOT EXISTS openetl.schedules (
	id            text        PRIMARY KEY,
	every_ms      bigint      NOT NULL,
	cron          text        NOT NULL,
	jitter_ms     bigint      NOT NULL,
	workflow_id   text        NOT NULL,
	workflow      text        NOT NULL,
	input         bytea       NOT NULL,
	org           text        NOT NULL,
	paused        boolean     NOT NULL,
	-- When it's next due, and that with its jitter.
	due           timestamptz NOT NULL,
	fire_at       timestamptz NOT NULL,
	-- The row of the workflow it started last.
	last_workflow text        NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS schedules_fire_at ON openetl.schedules (fire_at) WHERE NOT paused;

CREATE TABLE IF NOT EXISTS openetl.executors (
	id          text        PRIMARY KEY,
	version     text        NOT NULL,
	reported_at timestamptz NOT NULL
);
`

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, schemaLock); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, schema)
		return err
	})
}
