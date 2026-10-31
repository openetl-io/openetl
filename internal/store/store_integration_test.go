//go:build integration

package store_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

func TestMigrateAndBootstrapAreIdempotent(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	a, err := s.Bootstrap(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Bootstrap(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("second bootstrap gave %+v, first %+v", b, a)
	}
}

// A day's partition goes once the whole day is before the cutoff.
func TestDropMirrorPartitions(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, day := range []string{"2026-09-01", "2026-09-30", "2026-10-01", "2026-10-31"} {
		start, _ := time.Parse(time.DateOnly, day)
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE checkpoint_mirror_%s PARTITION OF checkpoint_mirror FOR VALUES FROM ('%s') TO ('%s')`,
			start.Format("20060102"), start.Format(time.RFC3339), start.AddDate(0, 0, 1).Format(time.RFC3339))); err != nil {
			t.Fatal(err)
		}
	}
	dropped, err := s.DropMirrorPartitions(ctx, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"checkpoint_mirror_20260901", "checkpoint_mirror_20260930"}; !slices.Equal(dropped, want) {
		t.Fatalf("dropped %v, want %v", dropped, want)
	}
	var left []string
	rows, err := conn.Query(ctx, `SELECT c.relname::text FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
		WHERE i.inhparent = 'checkpoint_mirror'::regclass ORDER BY 1`)
	if err == nil {
		left, err = pgx.CollectRows(rows, pgx.RowTo[string])
	}
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"checkpoint_mirror_20261001", "checkpoint_mirror_20261031"}; !slices.Equal(left, want) {
		t.Fatalf("partitions left %v, want %v", left, want)
	}
}
