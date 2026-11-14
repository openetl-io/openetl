//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dbosbackend"
	"github.com/openetl-io/openetl/internal/orchestration/schedules"
	"github.com/openetl-io/openetl/internal/orchestration/temporalbackend"
	"github.com/openetl-io/openetl/internal/platform/temporalx"
	"github.com/openetl-io/openetl/internal/store"
)

func temporalClient(t *testing.T) client.Client {
	t.Helper()
	tc, err := client.DialContext(t.Context(), client.Options{HostPort: env.temporal, Namespace: env.ns})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tc.Close)
	return tc
}

// A runner told to stop drains its attempt to a checkpoint and exits; another
// runner resumes from that checkpoint, and the drain doesn't count as a retry.
func TestRunnerDrains(t *testing.T) {
	first := startRunner(t)
	name := unique("drained")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	first.engine(t, name)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	if err := first.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-first.done:
	case <-time.After(30 * time.Second):
		t.Fatal("the runner didn't drain within 30 s")
	}
	if !strings.Contains(first.out.String(), "sync attempt drained") {
		t.Fatalf("the runner exited without draining:\n%s", first.out.String())
	}
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
	// The restarted attempt is the sync's second.
	log := wholeLog(t, latestSync(t, name).GetName())
	if logged(log, 0, 1, "sync attempt drained") != 1 || logged(log, 0, 2, "sync attempt finished") != 1 {
		t.Errorf("the log has no drained first attempt and finished second: %v", log)
	}

	got := attemptCodes(t, name)
	if len(got) != 2 || got[0] != "DRAINING" || got[1] != "" {
		t.Errorf("attempts ended %q, want a drain, then a success", got)
	}
}

// attemptCodes is how each attempt of the pipeline's latest sync ended, in
// order: its code, or nothing if it succeeded.
func attemptCodes(t *testing.T, pipeline string) []string {
	t.Helper()
	run, err := query[string](t, `SELECT s.run_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()
	rows, err := conn.Query(t.Context(), `SELECT COALESCE(outcome->>'Code', '') FROM dispatches
		WHERE kind = $1 AND input->>'RunKey' = $2 ORDER BY (input->>'Attempt')::int`, contract.RunSyncAttempt, run)
	if err != nil {
		t.Fatal(err)
	}
	codes, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		t.Fatal(err)
	}
	return codes
}

func pipelines() apiv1connect.PipelineServiceClient {
	return apiv1connect.NewPipelineServiceClient(env.http, env.apiURL)
}

func syncCount(t *testing.T, pipeline string) int {
	t.Helper()
	n, err := query[int](t, `SELECT count(*) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// syncNow starts a sync and waits for that one to finish. The last sync's
// workflow can still be running for a moment after its row says it ended,
// and a pipeline's first sync, which applying it starts, can be on its way
// when syncNow is called: the sync it waits for is the first to start after
// SyncPipeline took the request, which no other can, as one runs at a time.
func syncNow(t *testing.T, pipeline string) (status, stats string) {
	t.Helper()
	var since time.Time
	eventually(t, 10*time.Second, "the last sync's workflow to end", func() bool {
		var err error
		if since, err = query[time.Time](t, "SELECT now()"); err != nil {
			t.Fatal(err)
		}
		_, err = pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: "workspaces/default/pipelines/" + pipeline}))
		if err != nil && connect.CodeOf(err) != connect.CodeFailedPrecondition {
			t.Fatal(err)
		}
		return err == nil
	})
	q := `SELECT s.status || ' ' || coalesce(s.stats::text, s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 AND s.start_time > $2 ORDER BY s.start_time LIMIT 1`
	eventually(t, 90*time.Second, "pipeline "+pipeline+" to finish the sync it was asked for", func() bool {
		v, err := query[string](t, q, pipeline, since)
		if err != nil {
			return false
		}
		status, stats, _ = strings.Cut(v, " ")
		return status != "running"
	})
	return status, stats
}

func wantCode(t *testing.T, err error, code connect.Code, what string) {
	t.Helper()
	if connect.CodeOf(err) != code {
		t.Errorf("%s: got %v, want %s", what, err, code)
	}
}

func schedulePaused(t *testing.T, pipeline string) bool {
	t.Helper()
	id, err := query[string](t, `SELECT id::text FROM pipelines WHERE name = $1`, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	list, err := testBackend(t).ListSchedules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	i := slices.IndexFunc(list, func(s orchestration.ScheduleState) bool { return s.ID == contract.ScheduleID(id) })
	if i < 0 {
		t.Fatalf("no schedule for %s", pipeline)
	}
	return list[i].Paused
}

// Pausing pauses the schedule, applying again leaves it paused, and a paused
// pipeline or one that is syncing doesn't start another sync.
func TestPauseResumeAndSyncNow(t *testing.T) {
	startRunner(t)
	name := unique("paused")
	full := "workspaces/default/pipelines/" + name
	faker := `{"seed": 4, "count": 1000, "batchSize": 50, "delay": "200ms"}`
	apply(t, pipelineDocs(name, faker, `{}`)...)
	eventually(t, 30*time.Second, "the first sync to start", func() bool { return syncCount(t, name) == 1 })
	_, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: full}))
	wantCode(t, err, connect.CodeFailedPrecondition, "sync while syncing")
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}

	res, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: full}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Msg.GetPipeline().GetPaused() || !schedulePaused(t, name) {
		t.Fatalf("paused %v, schedule paused %v", res.Msg.GetPipeline().GetPaused(), schedulePaused(t, name))
	}
	_, err = pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: full}))
	wantCode(t, err, connect.CodeFailedPrecondition, "sync while paused")

	docs := pipelineDocs(name, faker, `{}`)
	docs[2] = strings.Replace(docs[2], `"every": "720h"`, `"every": "721h"`, 1)
	if r := apply(t, docs...); r[2].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Fatalf("reapplied the pipeline: %v", r[2].GetAction())
	}
	got, err := pipelines().GetPipeline(t.Context(), connect.NewRequest(&apiv1.GetPipelineRequest{Name: full}))
	if err != nil {
		t.Fatal(err)
	}
	if !got.Msg.GetPipeline().GetPaused() || !schedulePaused(t, name) {
		t.Fatalf("after applying, paused %v, schedule paused %v", got.Msg.GetPipeline().GetPaused(), schedulePaused(t, name))
	}

	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: full})); err != nil {
		t.Fatal(err)
	}
	if schedulePaused(t, name) {
		t.Fatal("the schedule is still paused")
	}
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if n := syncCount(t, name); n != 2 {
		t.Fatalf("%d syncs, want 2", n)
	}
}

// pgExec runs statements against the e2e database.
func pgExec(t *testing.T, stmts ...string) {
	t.Helper()
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	for _, q := range stmts {
		if _, err := conn.Exec(t.Context(), q); err != nil {
			t.Fatal(err)
		}
	}
}

// A full refresh reads the table whole on every sync, and appends a copy.
func TestFullRefreshReadsEverySync(t *testing.T) {
	startRunner(t)
	name := unique("full-refresh")
	sch := pgx.Identifier{schema(name)}.Sanitize()
	pgExec(t,
		"CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i FROM generate_series(1, 300) i",
	)
	dir := t.TempDir()
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 100}}`, schema(name))
	apply(t, docs(name, src, parquetDest(dir), `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if status, stats := syncNow(t, name); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if rows, ids, _ := parquetRows(t, dir, "items"); rows != 600 || ids != 300 {
		t.Fatalf("%d rows and %d distinct ids after two syncs, want 600 and 300", rows, ids)
	}
}

// A schema change the block policy stops fails the sync once, with no
// retries.
func TestBlockedSchemaChangeIsNotRetried(t *testing.T) {
	startRunner(t)
	name := unique("blocked")
	sch := pgx.Identifier{schema(name)}.Sanitize()
	pgExec(t,
		"CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i FROM generate_series(1, 100) i",
	)
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, schema(name))
	ds := docs(name, src, parquetDest(t.TempDir()), `{}`)
	ds[2] = strings.Replace(ds[2], `"delivery"`, `"schemaPolicy": "block", "delivery"`, 1)
	apply(t, ds...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}

	pgExec(t, "ALTER TABLE "+sch+".items ADD COLUMN price numeric(10, 2)")
	status, stats := syncNow(t, name)
	if status != "failed" || !strings.Contains(stats, "SCHEMA_DRIFT") {
		t.Fatalf("sync %s: %s; want it failed with SCHEMA_DRIFT", status, stats)
	}
	if got := attemptCodes(t, name); len(got) != 1 || got[0] != "SCHEMA_DRIFT" {
		t.Errorf("attempts ended %q, want one SCHEMA_DRIFT", got)
	}
}

// A retry doesn't read again a stream that its sync completed: faker reads
// users before purchases, and the engine dies once users has completed.
func TestCompletedStreamIsNotReadAgain(t *testing.T) {
	r := startRunner(t)
	name := unique("completed")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	pid := r.engine(t, name)
	eventually(t, 30*time.Second, "users to complete", func() bool {
		n, err := query[int](t, `SELECT count(*) FROM checkpoint_mirror m JOIN pipelines p ON p.id = m.pipeline_id,
			jsonb_array_elements(m.streams) s WHERE p.name = $1 AND s->>'name' = 'users' AND (s->>'complete')::boolean`, name)
		return err == nil && n > 0
	})
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	status, stats := waitSync(t, name, 90*time.Second)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if !strings.Contains(stats, `"Generation": 2`) {
		t.Errorf("stats %s: want the second attempt to have finished", stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
}

// Syncs and connector operations can be found by their search attributes.
func TestSearchAttributes(t *testing.T) {
	startRunner(t)
	name := unique("attrs")
	apply(t, pipelineDocs(name, `{"seed": 5, "count": 10}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if _, err := apiv1connect.NewSourceServiceClient(env.http, env.apiURL).CheckSource(t.Context(),
		connect.NewRequest(&apiv1.CheckSourceRequest{Name: "workspaces/default/sources/" + name})); err != nil {
		t.Fatal(err)
	}
	id, err := query[string](t, `SELECT id::text FROM pipelines WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := query[string](t, `SELECT workspace_id::text FROM sources WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	if env.orchestrator != "temporal" {
		system, err := dbosbackend.SystemDatabaseURL(env.dbURL)
		if err != nil {
			t.Fatal(err)
		}
		conn, err := pgx.Connect(t.Context(), system)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(context.Background()) }()
		for workflow, attrs := range map[string]string{
			contract.SyncWorkflow:        fmt.Sprintf(`{"pipeline": %q, "connectors": ["faker", "postgres"], "dataPlane": "default", "workspace": %q}`, id, ws),
			contract.ConnectorOpWorkflow: fmt.Sprintf(`{"connectors": ["faker"], "workspace": %q}`, ws),
		} {
			eventually(t, 30*time.Second, workflow+"s matching "+attrs, func() bool {
				var n int
				if err := conn.QueryRow(t.Context(), `SELECT count(*) FROM dbos.workflow_status WHERE name = $1 AND attributes @> $2::jsonb`, workflow, attrs).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n > 0
			})
		}
		return
	}
	tc := temporalClient(t)
	for _, q := range []string{
		fmt.Sprintf(`WorkflowType = 'SyncWorkflow' AND PipelineID = '%s' AND ConnectorDef = 'faker' AND ConnectorDef = 'postgres' AND DataPlaneID = 'default' AND WorkspaceID = '%s'`, id, ws),
		fmt.Sprintf(`WorkflowType = 'ConnectorOpWorkflow' AND ConnectorDef = 'faker' AND WorkspaceID = '%s'`, ws),
	} {
		eventually(t, 30*time.Second, "workflows matching "+q, func() bool {
			res, err := tc.CountWorkflow(t.Context(), &workflowservice.CountWorkflowExecutionsRequest{Namespace: env.ns, Query: q})
			return err == nil && res.GetCount() > 0
		})
	}
}

// reconcile does what an orchestrator does when it starts.
func reconcile(t *testing.T) {
	t.Helper()
	st, err := store.Open(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	backend := testBackend(t)
	r := &schedules.Reconciler{Store: st, Schedules: &schedules.Schedules{Backend: backend}, Backend: backend, Logger: slog.New(slog.DiscardHandler)}
	if err := r.Reconcile(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func testBackend(t *testing.T) orchestration.Backend {
	t.Helper()
	var (
		backend orchestration.Backend
		err     error
	)
	if env.orchestrator == "temporal" {
		backend, err = temporalbackend.Open(t.Context(), temporalx.Options{Address: env.temporal, Namespace: env.ns})
	} else {
		var system string
		if system, err = dbosbackend.SystemDatabaseURL(env.dbURL); err == nil {
			backend, err = dbosbackend.Open(t.Context(), dbosbackend.Options{DatabaseURL: system, ExecutorID: "e2e-test"})
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(backend.Close)
	return backend
}

// lose makes a workflow disappear, as a workflow can, with nothing it could
// do about it. It reaches the orchestrator's own workflows on its next step.
func lose(t *testing.T, workflowID string) {
	t.Helper()
	switch b := testBackend(t).(type) {
	case *dbosbackend.Backend:
		if err := b.Terminate(t.Context(), workflowID); err != nil {
			t.Fatal(err)
		}
	default:
		if err := temporalClient(t).TerminateWorkflow(t.Context(), workflowID, "", "the test lost it"); err != nil {
			t.Fatal(err)
		}
	}
}

// running reports whether a workflow of that name is listed under that ID.
func running(t *testing.T, workflow, id string) bool {
	t.Helper()
	list, err := testBackend(t).ListRunning(t.Context(), workflow)
	if err != nil {
		t.Fatal(err)
	}
	return slices.ContainsFunc(list, func(w orchestration.RunningWorkflow) bool { return w.ID == id })
}

func pipelineID(t *testing.T, name string) string {
	t.Helper()
	id, err := query[string](t, `SELECT id::text FROM pipelines WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// A stream job's workflow can be lost, and on DBOS it is at each upgrade: a
// supervisor started again takes over the run and its job, without starting
// another or losing a row.
func TestSupervisorIsLostAndTakesOver(t *testing.T) {
	startRunner(t)
	name := unique("taken-over")
	startContinuous(t, name)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	id := pipelineID(t, name)
	wf := contract.StreamWorkflowID(id)
	jobs := func() int {
		n, err := query[int](t, `SELECT count(*) FROM attempts WHERE pipeline_id = $1`, id)
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	if n := jobs(); n != 1 {
		t.Fatalf("%d jobs before", n)
	}
	lose(t, wf)
	before := committedEpochs(t, schema(name))
	// Listing lags, so the first reconcile may still see the old one running.
	eventually(t, 30*time.Second, "a supervisor to run again", func() bool {
		reconcile(t)
		return running(t, contract.StreamSupervisorWorkflow, wf)
	})
	eventually(t, 30*time.Second, "the job to carry on", func() bool { return committedEpochs(t, schema(name)) > before+2 })
	if got := statuses(runs(t, name)); got != "running" {
		t.Errorf("runs %q, want the one still running", got)
	}
	if n := jobs(); n != 1 {
		t.Errorf("%d jobs, want the same one still running", n)
	}
	// It still answers for it: a new version stops the job at a checkpoint.
	docs := continuousDocs(name, `{"seed": 4, "endless": true, "batchSize": 10, "delay": "200ms"}`)
	apply(t, docs...)
	eventually(t, 60*time.Second, "the run to stop and another to start", func() bool { return statuses(runs(t, name)) == "stopped running" })
	// And becoming scheduled stops it, and ends the supervisor.
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 60*time.Second, "the last run to stop", func() bool { return statuses(runs(t, name)) == "stopped stopped" })
}

// A sync's workflow can be lost too. The next sync stops what it left running
// and takes over from the checkpoint log, and nothing lands twice.
func TestSyncIsLostAndTheNextTakesOver(t *testing.T) {
	startRunner(t)
	name := unique("lost-sync")
	apply(t, pipelineDocs(name, slowFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 30*time.Second, "a committed epoch", func() bool { return committedEpochs(t, schema(name)) > 0 })
	wf, err := query[string](t, `SELECT s.workflow_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	lose(t, wf)
	status, stats := syncNow(t, name)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 2000, "users", "purchases")
	got, err := query[string](t, `SELECT string_agg(s.status || ': ' || coalesce(s.error, ''), ',' ORDER BY s.start_time) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "stopped: its workflow was lost") || !strings.HasSuffix(got, ",succeeded: ") {
		t.Errorf("syncs %q, want one stopped and one succeeded", got)
	}
	// Nothing it left is still running.
	eventually(t, 30*time.Second, "the lost sync's attempt to end", func() bool {
		n, err := query[int](t, `SELECT count(*) FROM dispatches d JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
			JOIN pipelines p ON p.id = a.pipeline_id WHERE p.name = $1 AND d.state <> 'finished'`, name)
		return err == nil && n == 0
	})
}

// An orchestrator starting makes what runs pipelines match the database:
// what's lost comes back, what nothing wants goes, and what's another
// install's stays.
func TestReconcileAtStart(t *testing.T) {
	startRunner(t)
	name := unique("reconciled")
	apply(t, pipelineDocs(name, `{"seed": 5, "count": 10, "batchSize": 5}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	id := pipelineID(t, name)
	backend := testBackend(t)
	if err := backend.DeleteSchedule(t.Context(), contract.ScheduleID(id)); err != nil {
		t.Fatal(err)
	}
	org, err := query[string](t, `SELECT org_id::text FROM pipelines WHERE id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
	stray := func(pipeline, org string) string {
		sched := orchestration.Schedule{
			ID: contract.ScheduleID(pipeline), Every: time.Hour, WorkflowID: contract.SyncWorkflowID(pipeline), Workflow: contract.SyncWorkflow,
			Input: contract.SyncInput{OrgID: org, PipelineID: pipeline}, Org: org, Paused: true,
		}
		if err := backend.UpsertSchedule(t.Context(), sched); err != nil {
			t.Fatal(err)
		}
		return sched.ID
	}
	ours, theirs := stray(uuid.NewString(), org), stray(uuid.NewString(), uuid.NewString())
	t.Cleanup(func() { _ = backend.DeleteSchedule(context.Background(), theirs) })

	listed := func() map[string]bool {
		list, err := backend.ListSchedules(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, s := range list {
			out[s.ID] = true
		}
		return out
	}
	// Listing lags.
	eventually(t, 30*time.Second, "the schedules to be listed", func() bool {
		l := listed()
		return l[ours] && l[theirs] && !l[contract.ScheduleID(id)]
	})
	reconcile(t)
	eventually(t, 30*time.Second, "the schedules to match", func() bool {
		l := listed()
		return !l[ours] && l[theirs] && l[contract.ScheduleID(id)]
	})
	// The new schedule starts its first sync.
	eventually(t, 60*time.Second, "a sync from the schedule that came back", func() bool { return syncCount(t, name) == 2 })
}
