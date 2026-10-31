package store

import (
	"context"
	"errors"
	"fmt"
	"time"

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

func (s *Store) createMirrorPartition(ctx context.Context, t time.Time) error {
	day := t.UTC().Truncate(24 * time.Hour)
	_, err := s.pool.Exec(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS checkpoint_mirror_%s PARTITION OF checkpoint_mirror FOR VALUES FROM ('%s') TO ('%s')`,
		day.Format("20060102"), day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339)))
	return err
}
