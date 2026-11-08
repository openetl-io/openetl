//go:build integration

package activities

import (
	"context"
	"slices"
	"testing"

	"github.com/google/uuid"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type fakeSchedules struct{ deleted []string }

func (f *fakeSchedules) SetPaused(context.Context, string, bool) error { return nil }

func (f *fakeSchedules) Upsert(context.Context, string, string, *apiv1.Schedule, bool) error {
	return nil
}

func (f *fakeSchedules) Delete(_ context.Context, pipeline string) error {
	f.deleted = append(f.deleted, pipeline)
	return nil
}

// A pipeline's ID is never used again, so a schedule that outlived its pipeline
// has nothing left to start.
func TestStartSyncDeletesTheScheduleOfAPipelineThatsGone(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, storetest.NewDatabase(t))
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
	schedules := &fakeSchedules{}
	a := &Activities{Store: s, Schedules: schedules}
	gone := uuid.NewString()
	_, err = a.StartSync(ctx, contract.SyncInput{OrgID: tenant.Org.String(), PipelineID: gone})
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("got %v, want %s", err, errs.ConfigInvalid)
	}
	if !slices.Equal(schedules.deleted, []string{gone}) {
		t.Errorf("deleted %v", schedules.deleted)
	}
}
