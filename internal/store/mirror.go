package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/unsuitable001/openetl/internal/store/db"
)

// MirrorCheckpoint adds a checkpoint to the mirror, and reports whether it
// wasn't there. It creates the partition for the checkpoint's day if no
// report has needed it yet, so it must not run in a transaction.
func (s *Store) MirrorCheckpoint(ctx context.Context, arg db.InsertMirroredCheckpointParams) (bool, error) {
	n, err := s.InsertMirroredCheckpoint(ctx, arg)
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok && pe.Code == "23514" { // no partition for the row
		perr := s.createMirrorPartition(ctx, arg.CommitTime)
		// A concurrent report may have created it, so try again either way.
		if n, err = s.InsertMirroredCheckpoint(ctx, arg); err != nil && perr != nil {
			err = fmt.Errorf("%w; creating its partition: %w", err, perr)
		}
	}
	if err != nil {
		return false, fmt.Errorf("store: mirror checkpoint: %w", err)
	}
	return n > 0, nil
}

// Each day's partition is checkpoint_mirror_{yyyymmdd}.
const (
	mirrorPartition = "checkpoint_mirror_"
	mirrorDay       = "20060102"
)

func (s *Store) createMirrorPartition(ctx context.Context, t time.Time) error {
	day := t.UTC().Truncate(24 * time.Hour)
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s%s PARTITION OF checkpoint_mirror FOR VALUES FROM ('%s') TO ('%s')`,
		mirrorPartition, day.Format(mirrorDay), day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339)))
	return err
}

// DropMirrorPartitions drops the checkpoint mirror's days that end by
// before, and returns the partitions it dropped.
func (s *Store) DropMirrorPartitions(ctx context.Context, before time.Time) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'checkpoint_mirror'::regclass ORDER BY 1`)
	if err != nil {
		return nil, fmt.Errorf("store: list mirror partitions: %w", err)
	}
	names, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return nil, fmt.Errorf("store: list mirror partitions: %w", err)
	}
	var dropped []string
	for _, name := range names {
		day, ok := strings.CutPrefix(name, mirrorPartition)
		start, err := time.Parse(mirrorDay, day)
		if !ok || err != nil || start.AddDate(0, 0, 1).After(before) {
			continue
		}
		if _, err := s.pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); err != nil {
			return dropped, fmt.Errorf("store: drop %s: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}
