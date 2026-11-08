package store

import (
	"context"
	"time"

	"github.com/openetl-io/openetl/internal/store/db"
)

// WriteAuditEntry adds an entry to the audit log. It creates the partition
// for the entry's month if no entry has needed it yet, so it must not run in
// a transaction; Migrate creates this month's and the next.
func (s *Store) WriteAuditEntry(ctx context.Context, arg db.InsertAuditEntryParams) error {
	return auditPartitions.insert(ctx, s.pool, arg.Time, func() error { return s.InsertAuditEntry(ctx, arg) })
}

// EnsureAuditPartitions creates the audit log's partitions for now's month
// and the next, unless they exist, so that entries written in a transaction
// have one.
func (s *Store) EnsureAuditPartitions(ctx context.Context, now time.Time) error {
	return auditPartitions.ensure(ctx, s.pool, now)
}

// DropAuditPartitions drops the audit log's months that end by before, and
// returns the partitions it dropped.
func (s *Store) DropAuditPartitions(ctx context.Context, before time.Time) ([]string, error) {
	return auditPartitions.drop(ctx, s.pool, before)
}
