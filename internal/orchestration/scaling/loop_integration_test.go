//go:build integration

package scaling

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type fixture struct {
	t         *testing.T
	s         *store.Store
	conn      *pgx.Conn
	d         *dispatch.Dispatcher
	org       uuid.UUID
	workspace uuid.UUID
}

func setup(t *testing.T) *fixture {
	t.Helper()
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
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(t.Context()) })
	tenant, err := s.Bootstrap(ctx, "default", func(*db.Queries, uuid.UUID) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, s: s, conn: conn, d: &dispatch.Dispatcher{Store: s}, org: tenant.Org, workspace: tenant.Workspace}
	f.report(nil)
	return f
}

func (f *fixture) report(running []dispatch.RunningWork) (drain []string) {
	f.t.Helper()
	_, drain, err := f.d.Report(f.t.Context(), dispatch.Report{Org: f.org, DataPlane: "default", Running: running, Runner: db.ReportRunnerParams{
		ID: "r", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{dispatch.LauncherSubprocess}, Platform: "linux/amd64",
		Pool: "default", CapacityCpu: 64000, CapacityMemory: 128 * resources.GiB,
	}})
	if err != nil {
		f.t.Fatal(err)
	}
	return drain
}

// job is a continuous pipeline whose spec has these resources, with a stream
// job running at lanes, whose lanes have been busy and backpressured for
// those shares of their time for the last d, sampled every 30 s, and whose
// lag went from one value to another meanwhile.
func (f *fixture) job(name, resourcesJSON string, lanes int, d time.Duration, busy, backpressured, lagFrom, lagTo float64) (pipeline uuid.UUID, attempt string) {
	f.t.Helper()
	ctx, q := f.t.Context(), f.s.Queries
	src, dst := uuid.New(), uuid.New()
	pipeline, sync := uuid.New(), uuid.New()
	spec := fmt.Sprintf(`{"mode":"PIPELINE_MODE_CONTINUOUS","resources":%s}`, resourcesJSON)
	err := errors.Join(
		q.InsertSource(ctx, db.InsertSourceParams{OrgID: f.org, WorkspaceID: f.workspace, ID: src, Name: name, Spec: []byte("{}"), Version: "v", Secrets: []string{}}),
		q.InsertDestination(ctx, db.InsertDestinationParams{OrgID: f.org, WorkspaceID: f.workspace, ID: dst, Name: name, Spec: []byte("{}"), Version: "v", Secrets: []string{}}),
		q.InsertPipeline(ctx, db.InsertPipelineParams{OrgID: f.org, WorkspaceID: f.workspace, ID: pipeline, Name: name, SourceID: src, DestinationID: dst, DataPlaneID: "default", Version: "v"}),
		q.InsertPipelineVersion(ctx, db.InsertPipelineVersionParams{
			OrgID: f.org, PipelineID: pipeline, Version: "v", Spec: []byte(spec), SourceID: src, DestinationID: dst, DataPlaneID: "default",
			Dbt: []byte("{}"), SparkClusters: []byte("{}"), FlinkClusters: []byte("{}"), Secrets: []string{},
		}),
	)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := q.StartSync(ctx, db.StartSyncParams{OrgID: f.org, ID: sync, PipelineID: pipeline, Version: "v", WorkflowID: "wf", RunID: name}); err != nil {
		f.t.Fatal(err)
	}
	attempt = name + "/job"
	err = f.d.Enqueue(ctx, contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: attempt, WorkflowID: "wf", Kind: contract.RunStreamJob, Input: map[string]string{},
		Size: resources.Size{CPU: int64(lanes) * 1000, Memory: resources.GiB}, Heartbeat: time.Hour, Timeout: time.Hour,
		PipelineID: pipeline.String(), Version: "v", SyncID: sync.String(), Attempt: 1,
	}, nil)
	if err != nil {
		f.t.Fatal(err)
	}
	if work, err := f.d.Poll(ctx, f.org, "default", "r", 0, 0); err != nil || len(work) != 1 {
		f.t.Fatalf("poll: %v, %v", work, err)
	}
	now := time.Now()
	var last resources.Lanes
	for at := d; at >= time.Second; at -= 30 * time.Second {
		lane := (d - at).Seconds() * float64(lanes)
		last = resources.Lanes{Count: lanes, Busy: lane * busy, Backpressured: lane * backpressured, Idle: lane * (1 - busy - backpressured)}
		_, err := f.conn.Exec(ctx, `INSERT INTO lane_samples (org_id, attempt_id, pipeline_id, sample_time, lanes, lane_limit, busy_seconds, backpressured_seconds, idle_seconds, lane_rows)
			VALUES ($1, $2, $3, $4, $5, 0, $6, $7, $8, 0)`, f.org, attempt, pipeline, now.Add(-at), lanes, last.Busy, last.Backpressured, last.Idle)
		if err != nil {
			f.t.Fatal(err)
		}
		lag := lagFrom + (lagTo-lagFrom)*(d-at).Seconds()/d.Seconds()
		progress, _ := json.Marshal([]map[string]any{{"stream": "s", "lag_ms": int64(lag * 1000)}})
		if _, err := f.s.MirrorCheckpoint(ctx, db.InsertMirroredCheckpointParams{
			OrgID: f.org, PipelineID: pipeline, Epoch: int64((d - at) / time.Second), Generation: 1, RunID: name,
			CommitTime: now.Add(-at), Streams: []byte("[]"), Progress: progress,
		}); err != nil {
			f.t.Fatal(err)
		}
	}
	f.report([]dispatch.RunningWork{{ID: attempt, Size: resources.S, Usage: &dispatch.AttemptUsage{Lanes: last}}})
	return pipeline, attempt
}

func (f *fixture) scale(pipeline uuid.UUID) db.JobScale {
	f.t.Helper()
	j, err := f.s.GetJobScale(f.t.Context(), db.GetJobScaleParams{OrgID: f.org, PipelineID: pipeline})
	if errors.Is(err, pgx.ErrNoRows) {
		return db.JobScale{}
	}
	if err != nil {
		f.t.Fatal(err)
	}
	return j
}

func (f *fixture) rescaled() int {
	f.t.Helper()
	var n int
	if err := f.conn.QueryRow(f.t.Context(), `SELECT count(*) FROM outbox WHERE type = 'pipeline.rescaled'`).Scan(&n); err != nil {
		f.t.Fatal(err)
	}
	return n
}

// A busy job falling behind grows, through a drain its runner hears of; one
// waiting for its destination gets a note; one under half busy for half an
// hour shrinks; a set size and a quiet job stay.
func TestLoop(t *testing.T) {
	f := setup(t)
	ctx := t.Context()
	busy, busyJob := f.job("busy", `{}`, 1, 10*time.Minute, 0.95, 0, 1, 60)
	waiting, _ := f.job("waiting", `{}`, 2, 10*time.Minute, 0.3, 0.6, 1, 60)
	idle, idleJob := f.job("idle", `{}`, 4, 40*time.Minute, 0.2, 0, 5, 5)
	fixed, _ := f.job("fixed", `{"size":{"preset":"s"}}`, 1, 10*time.Minute, 0.95, 0, 1, 60)
	quiet, _ := f.job("quiet", `{}`, 2, 10*time.Minute, 0.6, 0, 5, 5)

	loop := &Loop{Store: f.s}
	if err := loop.Tick(ctx, f.org); err != nil {
		t.Fatal(err)
	}
	if j := f.scale(busy); j.Lanes != 2 || j.RescaleTime == nil || j.Note != "" {
		t.Errorf("the busy job's scale %+v", j)
	}
	if j := f.scale(waiting); j.Lanes != 0 || j.Note != DestinationBound {
		t.Errorf("the waiting job's scale %+v", j)
	}
	if j := f.scale(idle); j.Lanes != 1 {
		t.Errorf("the idle job's scale %+v", j)
	}
	for _, p := range []uuid.UUID{fixed, quiet} {
		if j := f.scale(p); j != (db.JobScale{}) {
			t.Errorf("a job that should stay was scaled: %+v", j)
		}
	}
	if n := f.rescaled(); n != 2 {
		t.Errorf("%d pipeline.rescaled events, want 2", n)
	}
	drain := f.report([]dispatch.RunningWork{{ID: busyJob, Size: resources.S}, {ID: idleJob, Size: resources.L}})
	if len(drain) != 2 || drain[0] != busyJob || drain[1] != idleJob {
		t.Errorf("drain %v", drain)
	}

	// A job draining isn't decided about again.
	if err := loop.Tick(ctx, f.org); err != nil {
		t.Fatal(err)
	}
	if n := f.rescaled(); n != 2 {
		t.Errorf("%d pipeline.rescaled events after another tick, want 2", n)
	}
}
