//go:build integration

package schedules

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// fakeBackend is what a pass reconciles against. A pass that calls anything
// else of a backend panics.
type fakeBackend struct {
	orchestration.Backend

	mu        sync.Mutex
	schedules []orchestration.ScheduleState
	// How many of the first listings fail.
	failures int
	crons    map[string]string
	paused   map[string]bool
}

func (b *fakeBackend) ListRunning(context.Context, string) ([]orchestration.RunningWorkflow, error) {
	return nil, nil
}

func (b *fakeBackend) ListSchedules(context.Context) ([]orchestration.ScheduleState, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failures > 0 {
		b.failures--
		return nil, errors.New("the backend is down")
	}
	return b.schedules, nil
}

func (b *fakeBackend) UpsertSchedule(_ context.Context, s orchestration.Schedule) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.crons[s.ID] = s.Cron
	return nil
}

func (b *fakeBackend) PauseSchedule(_ context.Context, id string, paused bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.paused[id] = paused
	return nil
}

func (b *fakeBackend) cron(id string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.crons[id]
}

func (b *fakeBackend) pauses() map[string]bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return maps.Clone(b.paused)
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); !ok(); time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// A schedule follows its pipeline without an orchestrator starting again, even
// when a change never reached it and a first pass failed.
func TestRunKeepsSchedulesWithTheirPipelines(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.Bootstrap(ctx, "default", func(*db.Queries, uuid.UUID) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// A pipeline's source and destination don't matter here.
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		t.Fatal(err)
	}
	pipeline := uuid.New()
	if _, err := conn.Exec(ctx, `INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
		VALUES ($1, $2, $3, 'p', $2, $2, 'default', 'v1', 1)`, tenant.Org, uuid.New(), pipeline); err != nil {
		t.Fatal(err)
	}
	setCron := func(cron string) {
		t.Helper()
		_, err := conn.Exec(ctx, `INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id, source_version, destination_version, dbt, provenance)
			VALUES ($1, $2, 'v1', jsonb_build_object('schedule', jsonb_build_object('cron', $3::text)), $2, $2, 'default', 'v', 'v', '{}', '{}')
			ON CONFLICT (org_id, pipeline_id, version) DO UPDATE SET spec = excluded.spec`, tenant.Org, pipeline, cron)
		if err != nil {
			t.Fatal(err)
		}
	}
	setCron("0 * * * *")

	id := contract.ScheduleID(pipeline.String())
	backend := &fakeBackend{
		schedules: []orchestration.ScheduleState{{ID: id, Org: tenant.Org.String()}},
		failures:  1, crons: map[string]string{}, paused: map[string]bool{},
	}
	r := &Reconciler{Store: s, Schedules: &Schedules{Backend: backend}, Backend: backend, Logger: slog.New(slog.DiscardHandler), Every: 20 * time.Millisecond}
	running, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.Run(running)
	}()
	defer func() {
		stop()
		<-done
	}()

	eventually(t, "a pass after the one that failed", func() bool { return backend.cron(id) == "0 * * * *" })

	setCron("5 * * * *")
	eventually(t, "the changed cron to reach the schedule", func() bool { return backend.cron(id) == "5 * * * *" })

	if _, err := conn.Exec(ctx, `UPDATE pipelines SET paused = true WHERE id = $1`, pipeline); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the schedule to be paused", func() bool { return backend.pauses()[id] })
}
