package store

import (
	"context"
	"fmt"
	"time"

	"github.com/openetl-io/openetl/internal/store/db"
)

// MirrorCheckpoint adds a checkpoint to the mirror, and reports whether it
// wasn't there. It creates the partition for the checkpoint's day if
// EnsureMirrorPartitions hasn't, so it must not run in a transaction.
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

// EnsureMirrorPartitions creates the checkpoint mirror's partitions for now's
// day and the next, unless they exist, so that reports needn't: creating one
// locks the table and then its pipelines, the opposite order to deleting a
// pipeline.
func (s *Store) EnsureMirrorPartitions(ctx context.Context, now time.Time) error {
	return mirrorPartitions.ensure(ctx, s.pool, now)
}

// DropMirrorPartitions drops the checkpoint mirror's days that end by
// before, and returns the partitions it dropped.
func (s *Store) DropMirrorPartitions(ctx context.Context, before time.Time) ([]string, error) {
	return mirrorPartitions.drop(ctx, s.pool, before)
}
