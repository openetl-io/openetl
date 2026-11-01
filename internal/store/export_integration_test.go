//go:build integration

package store

import "context"

func (s *Store) MigrateTo(ctx context.Context, version int64) error { return s.migrate(ctx, version) }
