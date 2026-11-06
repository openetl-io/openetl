//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.temporal.io/sdk/client"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// The retention schedule drops the checkpoint mirror's days older than the
// checkpoint history, and keeps the rest.
func TestRetentionDropsOldMirrorDays(t *testing.T) {
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	today := time.Now().UTC().Truncate(24 * time.Hour)
	partition := func(daysAgo int) string {
		day := today.AddDate(0, 0, -daysAgo)
		name := "checkpoint_mirror_" + day.Format("20060102")
		if _, err := conn.Exec(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s PARTITION OF checkpoint_mirror FOR VALUES FROM ('%s') TO ('%s')`,
			name, day.Format(time.RFC3339), day.AddDate(0, 0, 1).Format(time.RFC3339))); err != nil {
			t.Fatal(err)
		}
		return name
	}
	exists := func(name string) bool {
		var ok bool
		if err := conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", name).Scan(&ok); err != nil {
			t.Fatal(err)
		}
		return ok
	}
	old, recent := partition(40), partition(2)

	h := temporalClient(t).ScheduleClient().GetHandle(ctx, contract.RetentionScheduleID)
	if err := h.Trigger(ctx, client.ScheduleTriggerOptions{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the old day to be dropped", func() bool { return !exists(old) })
	if !exists(recent) {
		t.Errorf("%s was dropped, though it is within the checkpoint history", recent)
	}
}
