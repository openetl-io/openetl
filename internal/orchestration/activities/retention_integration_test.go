//go:build integration

package activities_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/orchestration/activities"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// The daily step that drops the mirror's old days also makes today's and
// tomorrow's, so the first report of a day finds its partition.
func TestDropMirrorPartitionsMakesTheComingDays(t *testing.T) {
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
	today := time.Now().UTC().Truncate(24 * time.Hour)
	days := []string{"checkpoint_mirror_" + today.Format("20060102"), "checkpoint_mirror_" + today.AddDate(0, 0, 1).Format("20060102")}
	for _, name := range days {
		if _, err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+name); err != nil {
			t.Fatal(err)
		}
	}

	a := &activities.Activities{Store: s}
	if _, err := a.DropMirrorPartitions(ctx, contract.DropMirrorPartitionsInput{Before: today.AddDate(0, 0, -30)}); err != nil {
		t.Fatal(err)
	}
	for _, name := range days {
		var exists bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&exists); err != nil || !exists {
			t.Errorf("partition %s exists: %v (%v)", name, exists, err)
		}
	}
}
