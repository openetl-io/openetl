package store

import (
	"context"
	"fmt"
	"time"

	"github.com/openetl-io/openetl/internal/store/db"
)

// MirrorCheckpoint adds a checkpoint to the mirror, and reports whether it
// wasn't there. It creates the partition for the checkpoint's day if no
// report has needed it yet, so it must not run in a transaction.
func (s *Store) MirrorCheckpoint(ctx context.Context, arg db.InsertMirroredCheckpointParams) (bool, error) {
	var n int64
	err := mirrorPartitions.insert(ctx, s.pool, arg.CommitTime, func() (err error) {
		n, err = s.InsertMirroredCheckpoint(ctx, arg)
		return err
	})
	if err != nil {
		return false, fmt.Errorf("store: mirror checkpoint: %w", err)
	}
	return n > 0, nil
}

// DropMirrorPartitions drops the checkpoint mirror's days that end by
// before, and returns the partitions it dropped.
func (s *Store) DropMirrorPartitions(ctx context.Context, before time.Time) ([]string, error) {
	return mirrorPartitions.drop(ctx, s.pool, before)
}
