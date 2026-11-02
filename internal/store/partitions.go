package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// partitions are a table's partitions by range of a time, one per period:
// {table}_{the period's start in layout}, in UTC.
type partitions struct {
	table, layout string
	start         func(time.Time) time.Time
	next          func(time.Time) time.Time
}

var (
	mirrorPartitions = partitions{
		table: "checkpoint_mirror", layout: "20060102",
		start: func(t time.Time) time.Time { return t.UTC().Truncate(24 * time.Hour) },
		next:  func(t time.Time) time.Time { return t.AddDate(0, 0, 1) },
	}
	auditPartitions = partitions{
		table: "audit_log", layout: "200601",
		start: func(t time.Time) time.Time {
			t = t.UTC()
			return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		},
		next: func(t time.Time) time.Time { return t.AddDate(0, 1, 0) },
	}
)

// create creates the partition that holds t, unless it exists.
func (p partitions) create(ctx context.Context, pool *pgxpool.Pool, t time.Time) error {
	start := p.start(t)
	_, err := pool.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')`,
		pgx.Identifier{p.table + "_" + start.Format(p.layout)}.Sanitize(), pgx.Identifier{p.table}.Sanitize(),
		start.Format(time.RFC3339), p.next(start).Format(time.RFC3339)))
	return err
}

// insert runs insert, and if no partition holds its row, creates the one
// that holds t and runs it again. It must not run in a transaction.
func (p partitions) insert(ctx context.Context, pool *pgxpool.Pool, t time.Time, insert func() error) error {
	err := insert()
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "23514" { // no partition for the row
		perr := p.create(ctx, pool, t)
		// A concurrent insert may have created it, so try again either way.
		if err = insert(); err != nil && perr != nil {
			err = fmt.Errorf("%w; creating its partition: %w", err, perr)
		}
	}
	return err
}

// drop drops the partitions whose period ends by before, and returns them.
func (p partitions) drop(ctx context.Context, pool *pgxpool.Pool, before time.Time) ([]string, error) {
	rows, err := pool.Query(ctx, `SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = $1::regclass ORDER BY 1`, p.table)
	if err != nil {
		return nil, fmt.Errorf("store: list %s partitions: %w", p.table, err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("store: list %s partitions: %w", p.table, err)
	}
	var dropped []string
	for _, name := range names {
		period, ok := strings.CutPrefix(name, p.table+"_")
		start, err := time.Parse(p.layout, period)
		if !ok || err != nil || p.next(start).After(before) {
			continue
		}
		if _, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return dropped, fmt.Errorf("store: drop %s: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}
