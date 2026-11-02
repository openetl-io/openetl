// Package store is the metadata database. Every query outside the orgs
// table is scoped by org_id; TestQueriesAreOrgScoped checks it.
package store

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/lock"

	"github.com/unsuitable001/openetl/internal/store/db"
)

//go:embed migrations/*.sql
var migrations embed.FS

type Store struct {
	*db.Queries
	pool *pgxpool.Pool
}

func Open(ctx context.Context, url string) (*Store, error) {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("store: connect: %w", err)
	}
	return &Store{Queries: db.New(pool), pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

// Migrate applies pending migrations, and creates the audit log's
// partitions for this month and the next. Concurrent callers wait on a
// session lock.
func (s *Store) Migrate(ctx context.Context) error {
	if err := s.migrate(ctx, 0); err != nil {
		return err
	}
	return s.EnsureAuditPartitions(ctx, time.Now())
}

// migrate applies pending migrations up to version, or every one if it's 0.
func (s *Store) migrate(ctx context.Context, version int64) error {
	sqlDB := stdlib.OpenDBFromPool(s.pool)
	defer func() { _ = sqlDB.Close() }()
	locker, err := lock.NewPostgresSessionLocker()
	if err != nil {
		return err
	}
	dir, err := fs.Sub(migrations, "migrations")
	if err != nil {
		return err
	}
	p, err := goose.NewProvider(goose.DialectPostgres, sqlDB, dir, goose.WithSessionLocker(locker))
	if err != nil {
		return fmt.Errorf("store: migrations: %w", err)
	}
	if version == 0 {
		_, err = p.Up(ctx)
	} else {
		_, err = p.UpTo(ctx, version)
	}
	if err != nil {
		return fmt.Errorf("store: migrate: %w", err)
	}
	return nil
}

// Tx runs f in a transaction, and commits if f returns nil.
func (s *Store) Tx(ctx context.Context, f func(q *db.Queries) error) error {
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error { return f(s.WithTx(tx)) })
}

// Tenant is the install's one org until multi-tenancy arrives.
type Tenant struct {
	Org       uuid.UUID
	Workspace uuid.UUID
}

const DefaultName = "default"

// Bootstrap creates the default org and workspace, and dataPlane, unless
// they exist. Then it runs then, if set, in the same transaction, which
// holds the bootstrap lock.
func (s *Store) Bootstrap(ctx context.Context, dataPlane string, then func(q *db.Queries, org uuid.UUID) error) (Tenant, error) {
	var t Tenant
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('openetl.bootstrap'))"); err != nil {
			return err
		}
		q := s.WithTx(tx)
		org, err := q.FirstOrg(ctx)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			t.Org = uuid.New()
			if err := q.CreateOrg(ctx, db.CreateOrgParams{ID: t.Org, Name: DefaultName}); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			t.Org = org.ID
		}
		if err := q.CreateWorkspace(ctx, db.CreateWorkspaceParams{OrgID: t.Org, ID: uuid.New(), Name: DefaultName}); err != nil {
			return err
		}
		ws, err := q.GetWorkspace(ctx, db.GetWorkspaceParams{OrgID: t.Org, Name: DefaultName})
		if err != nil {
			return err
		}
		t.Workspace = ws.ID
		if err := q.CreateDataPlane(ctx, db.CreateDataPlaneParams{ID: dataPlane, OrgID: t.Org}); err != nil {
			return err
		}
		if then != nil {
			return then(q, t.Org)
		}
		return nil
	})
	if err != nil {
		return t, fmt.Errorf("store: bootstrap: %w", err)
	}
	return t, nil
}

// IsForeignKeyViolation reports whether err is a foreign key violation, such
// as deleting a source a pipeline uses.
func IsForeignKeyViolation(err error) bool {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pe.Code == "23503"
}

func IsUniqueViolation(err error) bool {
	pe, ok := errors.AsType[*pgconn.PgError](err)
	return ok && pe.Code == "23505"
}
