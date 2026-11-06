package workflows_test

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// streamEnv runs StreamSupervisor against a pipeline the test changes.
type streamEnv struct {
	t     *testing.T
	env   *testsuite.TestWorkflowEnvironment
	plane *plane
	state contract.StreamState
	jobs  []contract.StreamJobInput
	// When each job started, on the workflow's clock.
	starts   []time.Time
	finished []contract.FinishSyncInput
}

// job answers the nth RunStreamJob with how long it runs on the workflow's
// clock, and how it ends.
type job func(n int, in contract.StreamJobInput) (time.Duration, contract.WorkOutcome)

func newStreamEnv(t *testing.T, w workflows.Workflows, answer job) *streamEnv {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	e := &streamEnv{t: t, env: s.NewTestWorkflowEnvironment(), state: contract.StreamState{Continuous: true, Version: "v1", DataPlane: "default"}}
	e.env.RegisterWorkflowWithOptions(w.StreamSupervisor, workflow.RegisterOptions{Name: contract.StreamSupervisorWorkflow})
	e.env.RegisterActivityWithOptions(func(context.Context, contract.StreamInput) (contract.StreamState, error) {
		return e.state, nil
	}, activity.RegisterOptions{Name: contract.GetStreamState})
	runs := 0
	e.env.RegisterActivityWithOptions(func(_ context.Context, in contract.StartRunInput) (contract.StartRunResult, error) {
		runs++
		if in.Version != e.state.Version {
			t.Errorf("run %d of %s, want %s", runs, in.Version, e.state.Version)
		}
		return contract.StartRunResult{SyncID: fmt.Sprintf("r%d", runs), RunKey: fmt.Sprintf("k%d", runs)}, nil
	}, activity.RegisterOptions{Name: contract.StartRun})
	e.env.RegisterActivityWithOptions(func(_ context.Context, in contract.FinishSyncInput) error {
		e.finished = append(e.finished, in)
		return nil
	}, activity.RegisterOptions{Name: contract.FinishSync})
	e.plane = newPlane(e.env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		j := input[contract.StreamJobInput](t, in)
		e.jobs = append(e.jobs, j)
		e.starts = append(e.starts, e.env.Now())
		return answer(len(e.jobs), j)
	})
	return e
}

// at changes the pipeline after d on the workflow's clock, then signals.
func (e *streamEnv) at(d time.Duration, change func(*contract.StreamState)) {
	e.env.RegisterDelayedCallback(func() {
		change(&e.state)
		e.env.SignalWorkflow(contract.RefreshSignal, nil)
	}, d)
}

func (e *streamEnv) run(in contract.StreamInput) error {
	e.t.Helper()
	e.env.ExecuteWorkflow(contract.StreamSupervisorWorkflow, in)
	if !e.env.IsWorkflowCompleted() {
		e.t.Fatal("the workflow didn't complete")
	}
	return e.env.GetWorkflowError()
}

func (e *streamEnv) versions() []string {
	var v []string
	for _, j := range e.jobs {
		v = append(v, j.SyncID+"/"+j.Version)
	}
	return v
}

func (e *streamEnv) ends() []string {
	var out []string
	for _, f := range e.finished {
		switch {
		case f.Stopped:
			out = append(out, f.SyncID+" stopped")
		case f.Error != "":
			out = append(out, f.SyncID+" failed")
		default:
			out = append(out, f.SyncID+" succeeded")
		}
	}
	return out
}

func runsForever(int, contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
	return forever, contract.WorkOutcome{Cancelled: true}
}

var stream = contract.StreamInput{OrgID: "o", PipelineID: "p"}

func gone(s *contract.StreamState) { s.Continuous = false }

// A new version stops the run at a checkpoint and starts one of the new
// version; deleting the pipeline stops that one and ends the supervisor.
func TestStreamUpgradesAndEnds(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(10*time.Second, func(s *contract.StreamState) { s.Version = "v2" })
	e.at(2*time.Minute, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1", "r2/v2"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped", "r2 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// A released hold stops the run at a checkpoint and starts another of the
// same version, which takes the release.
func TestStreamRestartsForARelease(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(10*time.Second, func(s *contract.StreamState) { s.Releases = "rel1" })
	e.at(2*time.Minute, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1", "r2/v1"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped", "r2 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// A refresh that changes nothing the run depends on leaves its job alone.
func TestStreamRefreshWithoutChange(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(time.Minute, func(*contract.StreamState) {})
	e.at(2*time.Minute, func(s *contract.StreamState) { s.Connectors = []string{"postgres"} })
	e.at(3*time.Minute, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1"}) {
		t.Errorf("jobs %v", got)
	}
}

func TestStreamPausesAndResumes(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(time.Minute, func(s *contract.StreamState) { s.Paused = true })
	e.at(time.Hour, func(s *contract.StreamState) { s.Paused = false })
	e.at(2*time.Hour, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1", "r2/v1"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped", "r2 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// A run that fails with a code that isn't retried waits for a refresh, which
// starts a new run.
func TestStreamFailureWaitsForARefresh(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, func(n int, _ contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
		if n == 1 {
			return time.Second, fail(errs.ConfigInvalid)
		}
		return runsForever(n, contract.StreamJobInput{})
	})
	e.at(time.Hour, func(*contract.StreamState) {})
	e.at(2*time.Hour, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1", "r2/v1"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 failed", "r2 stopped"}) || !strings.Contains(e.finished[0].Error, errs.ConfigInvalid.String()) {
		t.Errorf("runs ended %v: %+v", got, e.finished)
	}
}

// Retryable failures, FENCED among them, and drains restart the job within
// the run. A drain restarts it at once, and the next attempt knows its
// number in the run.
func TestStreamRestartsWithinTheRun(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{AttemptRetry: time.Second}, func(n int, in contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
		if int(in.Attempt) != n {
			t.Errorf("call %d is the run's attempt %d", n, in.Attempt)
		}
		switch n {
		case 1:
			return time.Minute, fail(errs.Transient)
		case 2:
			// Fenced by an attempt that started late.
			return time.Minute, fail(errs.Fenced)
		case 3:
			return time.Minute, fail(errs.Draining)
		}
		return runsForever(n, in)
	})
	e.at(time.Hour, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v1", "r1/v1", "r1/v1", "r1/v1"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// A failed job waits before its retry, from AttemptRetry and doubling to 5
// minutes, a dead runner's heartbeat timeout included. A job that lasted 10
// minutes waits the first again.
func TestStreamRetriesBackOffAndStartOver(t *testing.T) {
	ran := map[int]time.Duration{12: 11 * time.Minute, 13: 2 * time.Minute, 14: forever}
	e := newStreamEnv(t, workflows.Workflows{AttemptRetry: time.Second}, func(n int, in contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
		if int(in.Attempt) != n {
			t.Errorf("call %d is the run's attempt %d", n, in.Attempt)
		}
		switch {
		case n == 14:
			return runsForever(n, in)
		case ran[n] > 0:
			return ran[n], fail(errs.Transient)
		}
		return time.Second, fail(errs.Transient)
	})
	e.at(10*time.Hour, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if len(e.jobs) != 14 {
		t.Fatalf("%d jobs, want 14", len(e.jobs))
	}
	want := []time.Duration{1, 2, 4, 8, 16, 32, 64, 128, 256, 300, 300, 1, 2}
	for i, w := range want {
		took := cmp.Or(ran[i+1], time.Second)
		if got := e.starts[i+1].Sub(e.starts[i]) - took; got != w*time.Second {
			t.Errorf("job %d started %s after job %d failed, want %ds", i+2, got, i+1, w)
		}
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// Past its history limit, a supervisor continues as new after a failure too,
// carrying the wait. A refresh during the wait stops the run.
func TestStreamContinuesAsNewAfterAFailure(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, func(int, contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
		return time.Minute, fail(errs.Transient)
	})
	e.env.SetCurrentHistoryLength(2001)
	run := &contract.StreamRun{State: e.state, SyncID: "r7", RunKey: "k7", Earlier: 2, Backoff: 20 * time.Second}
	err := e.run(contract.StreamInput{OrgID: "o", PipelineID: "p", Run: run})
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("got %v, want to continue as new", err)
	}
	var next contract.StreamInput
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if r := next.Run; r == nil || r.SyncID != "r7" || r.Earlier != 3 || r.Backoff != 40*time.Second || !r.Retry {
		t.Errorf("continued with %+v", r)
	}

	e = newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(10*time.Second, func(s *contract.StreamState) { s.Version = "v2" })
	e.at(time.Hour, gone)
	if err := e.run(next); err != nil {
		t.Fatal(err)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r1/v2"}) {
		t.Errorf("jobs %v", got)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r7 stopped", "r1 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// A run continued as new during a wait waits it out first.
func TestStreamContinuedRetryWaits(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(time.Hour, gone)
	start := e.env.Now()
	run := &contract.StreamRun{State: e.state, SyncID: "r7", RunKey: "k7", Earlier: 3, Backoff: 4 * time.Minute, Retry: true}
	if err := e.run(contract.StreamInput{OrgID: "o", PipelineID: "p", Run: run}); err != nil {
		t.Fatal(err)
	}
	if len(e.starts) != 1 || e.starts[0].Sub(start) != 4*time.Minute || e.jobs[0].Attempt != 4 {
		t.Errorf("jobs %+v at %v, from %v", e.jobs, e.starts, start)
	}
}

// Past its history limit, a supervisor continues as new after a drain,
// carrying the run.
func TestStreamContinuesAsNewAfterADrain(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, func(n int, in contract.StreamJobInput) (time.Duration, contract.WorkOutcome) {
		if n > 1 {
			return runsForever(n, in)
		}
		return time.Minute, fail(errs.Draining)
	})
	e.env.SetCurrentHistoryLength(2001)
	run := &contract.StreamRun{State: e.state, SyncID: "r7", RunKey: "k7", Earlier: 2}
	err := e.run(contract.StreamInput{OrgID: "o", PipelineID: "p", Run: run})
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("got %v, want to continue as new", err)
	}
	var next contract.StreamInput
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if r := next.Run; r == nil || r.SyncID != "r7" || r.RunKey != "k7" || r.Earlier != 3 {
		t.Errorf("continued with %+v", r)
	}
	if got := e.versions(); !slices.Equal(got, []string{"r7/v1"}) || e.jobs[0].Attempt != 3 {
		t.Errorf("jobs %+v", e.jobs)
	}
	if len(e.finished) != 0 {
		t.Errorf("runs ended %v", e.ends())
	}
}

// A job outlives its workflow run. Past its history limit, a supervisor
// continues as new while the job runs, and the next workflow run waits for
// that job instead of starting another.
func TestStreamContinuesAsNewWhileAJobRuns(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.env.SetCurrentHistoryLength(2001)
	run := &contract.StreamRun{State: e.state, SyncID: "r7", RunKey: "k7"}
	err := e.run(contract.StreamInput{OrgID: "o", PipelineID: "p", Run: run})
	var can *workflow.ContinueAsNewError
	if !errors.As(err, &can) {
		t.Fatalf("got %v, want to continue as new", err)
	}
	var next contract.StreamInput
	if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &next); err != nil {
		t.Fatal(err)
	}
	if r := next.Run; r == nil || r.Job == "" || r.Job != e.plane.sent[0].ID || r.JobStart.IsZero() {
		t.Fatalf("continued with %+v", r)
	}
	if len(e.plane.cancelled) != 0 || len(e.finished) != 0 {
		t.Errorf("cancelled %v, runs ended %v", e.plane.cancelled, e.ends())
	}

	// The job ended while nothing was listening, so its wake went nowhere.
	e = newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.plane.done[next.Run.Job] = fail(errs.ConfigInvalid)
	e.at(time.Hour, gone)
	if err := e.run(next); err != nil {
		t.Fatal(err)
	}
	if len(e.jobs) != 0 {
		t.Errorf("it started %d jobs", len(e.jobs))
	}
	if got := e.ends(); !slices.Equal(got, []string{"r7 failed"}) {
		t.Errorf("runs ended %v", got)
	}
}

// Stopping an adopted job stops that job.
func TestStreamStopsAnAdoptedJob(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(time.Minute, gone)
	run := &contract.StreamRun{State: e.state, SyncID: "r7", RunKey: "k7", Job: "j1", JobStart: e.env.Now()}
	if err := e.run(contract.StreamInput{OrgID: "o", PipelineID: "p", Run: run}); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(e.plane.cancelled, []string{"j1"}) || len(e.jobs) != 0 {
		t.Errorf("cancelled %v, %d jobs started", e.plane.cancelled, len(e.jobs))
	}
	if got := e.ends(); !slices.Equal(got, []string{"r7 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

// Runs of one workflow run don't share work IDs, which would make the second
// find the first's outcome.
func TestStreamRunsHaveTheirOwnWorkIDs(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.at(time.Minute, func(s *contract.StreamState) { s.Version = "v2" })
	e.at(2*time.Minute, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if len(e.plane.sent) != 2 || e.plane.sent[0].ID == e.plane.sent[1].ID {
		t.Fatalf("sent %+v", e.plane.sent)
	}
	if w := e.plane.sent[0]; w.Kind != contract.RunStreamJob || w.Light != "" || w.PipelineID != "p" || w.Version != "v1" || w.Heartbeat != 90*time.Second || w.SyncID == "" || w.Attempt != 1 {
		t.Errorf("sent %+v", w)
	}
}

// Cancelling the supervisor stops the run.
func TestStreamCancelled(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, time.Minute)
	if err := e.run(stream); !temporal.IsCanceledError(err) {
		t.Fatalf("got %v, want cancelled", err)
	}
	if got := e.ends(); !slices.Equal(got, []string{"r1 stopped"}) {
		t.Errorf("runs ended %v", got)
	}
}

func TestStreamStartsPausedAndEnds(t *testing.T) {
	e := newStreamEnv(t, workflows.Workflows{}, runsForever)
	e.state.Paused = true
	e.at(time.Hour, gone)
	if err := e.run(stream); err != nil {
		t.Fatal(err)
	}
	if len(e.jobs) != 0 || len(e.finished) != 0 {
		t.Errorf("jobs %v, runs ended %v", e.versions(), e.ends())
	}
}
