package workflows_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

// syncEnv runs SyncWorkflow with attempt answering each RunSyncAttempt.
type syncEnv struct {
	env      *testsuite.TestWorkflowEnvironment
	plane    *plane
	finished contract.FinishSyncInput
	calls    int
	started  contract.StartSyncResult
}

func newSyncEnv(t *testing.T, attempt func(n int, in contract.AttemptInput) contract.WorkOutcome) *syncEnv {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	e := &syncEnv{env: s.NewTestWorkflowEnvironment(), started: contract.StartSyncResult{SyncID: "s1", Version: "v1", DataPlane: "default"}}
	e.env.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	e.env.RegisterActivityWithOptions(func(context.Context, contract.SyncInput) (contract.StartSyncResult, error) {
		return e.started, nil
	}, activity.RegisterOptions{Name: contract.StartSync})
	e.env.RegisterActivityWithOptions(func(_ context.Context, in contract.FinishSyncInput) error {
		e.finished = in
		return nil
	}, activity.RegisterOptions{Name: contract.FinishSync})
	e.plane = newPlane(e.env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		e.calls++
		return time.Second, attempt(e.calls, input[contract.AttemptInput](t, in))
	})
	return e
}

func (e *syncEnv) run(t *testing.T) error {
	t.Helper()
	e.env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
	if !e.env.IsWorkflowCompleted() {
		t.Fatal("the workflow didn't complete")
	}
	return e.env.GetWorkflowError()
}

func runSync(t *testing.T, attempt func(n int, in contract.AttemptInput) contract.WorkOutcome) (calls int, finished contract.FinishSyncInput, err error) {
	t.Helper()
	e := newSyncEnv(t, attempt)
	err = e.run(t)
	return e.calls, e.finished, err
}

func TestNonRetryableCodesFailTheSync(t *testing.T) {
	for _, code := range []errs.Code{errs.SchemaDrift, errs.ConfigInvalid, errs.Fenced} {
		calls, finished, err := runSync(t, func(int, contract.AttemptInput) contract.WorkOutcome { return fail(code) })
		if err == nil || calls != 1 {
			t.Errorf("%s: %d attempts and error %v, want 1 attempt and an error", code, calls, err)
		}
		if want := code.String() + ": attempt failed"; finished.Error != want || finished.Code != code.String() {
			t.Errorf("%s: recorded %q (%s), want %q without Temporal's wrapping", code, finished.Error, finished.Code, want)
		}
	}
}

func TestRetryableCodesRetryFiveTimes(t *testing.T) {
	calls, _, err := runSync(t, func(int, contract.AttemptInput) contract.WorkOutcome { return fail(errs.Transient) })
	if err == nil || calls != 5 {
		t.Fatalf("%d attempts and error %v, want 5 and an error", calls, err)
	}
}

// Retries wait, doubling, or as long as the failure says.
func TestSyncRetriesWait(t *testing.T) {
	e := newSyncEnv(t, func(n int, _ contract.AttemptInput) contract.WorkOutcome {
		o := fail(errs.RateLimited)
		if n == 3 {
			o.RetryAfter = 5 * time.Minute
		}
		return o
	})
	if err := e.run(t); err == nil {
		t.Fatal("want an error")
	}
	want := []time.Duration{30 * time.Second, time.Minute, 5 * time.Minute, 4 * time.Minute}
	for i, w := range want {
		// Each attempt takes a second.
		if got := e.plane.times[i+1].Sub(e.plane.times[i]) - time.Second; got.Round(time.Second) != w {
			t.Errorf("attempt %d started %s after attempt %d failed, want %s", i+2, got, i+1, w)
		}
	}
}

// A drained attempt restarts at once, and doesn't count: one failure, a
// drain, then four more failures. Every attempt knows its number in the
// sync, for its log.
func TestDrainingDoesNotCount(t *testing.T) {
	calls, _, err := runSync(t, func(n int, in contract.AttemptInput) contract.WorkOutcome {
		if int(in.Attempt) != n || in.SyncID != "s1" || in.RunKey == "" {
			t.Errorf("call %d got %+v", n, in)
		}
		if n == 2 {
			return fail(errs.Draining)
		}
		return fail(errs.Transient)
	})
	if calls != 6 || err == nil {
		t.Fatalf("%d calls and error %v, want 6 and an error", calls, err)
	}
	var app *temporal.ApplicationError
	if !errors.As(err, &app) || app.Type() != errs.Transient.String() {
		t.Fatalf("got %v, want the last TRANSIENT", err)
	}
}

func TestDrainedSyncSucceeds(t *testing.T) {
	calls, finished, err := runSync(t, func(n int, _ contract.AttemptInput) contract.WorkOutcome {
		if n < 3 {
			return fail(errs.Draining)
		}
		return result(t, contract.AttemptResult{Generation: uint64(n), Committed: 1})
	})
	if err != nil || calls != 3 {
		t.Fatalf("%d calls and error %v, want 3 and none", calls, err)
	}
	if finished.Error != "" || finished.Result == nil || finished.Result.Generation != 3 {
		t.Fatalf("recorded %+v", finished)
	}
}

// Attempts are placed with the sync's priority and the pipeline version whose
// connectors a runner must run, each under an ID of its own.
func TestAttemptsSayHowToPlaceThem(t *testing.T) {
	e := newSyncEnv(t, func(n int, _ contract.AttemptInput) contract.WorkOutcome {
		if n == 1 {
			return fail(errs.Transient)
		}
		return result(t, contract.AttemptResult{})
	})
	e.started.Manual = true
	e.started.Pool = "batch"
	if err := e.run(t); err != nil {
		t.Fatal(err)
	}
	first, second := e.plane.sent[0], e.plane.sent[1]
	if first.ID == second.ID || first.Priority != contract.PriorityManual || first.Light != "" ||
		first.Pool != "batch" || first.SyncID != e.started.SyncID || first.Attempt != 1 || second.Attempt != 2 ||
		first.PipelineID != "p" || first.Version != "v1" || first.Kind != contract.RunSyncAttempt || first.WorkflowID == "" || first.Heartbeat != 2*time.Minute {
		t.Errorf("sent %+v, then %+v", first, second)
	}
	// EnqueueWork sizes it, so a size doesn't go in the workflow's history.
	if first.Size != (resources.Size{}) {
		t.Errorf("the workflow sized an attempt: %+v", first.Size)
	}
}

// A wake that gets lost costs a wait, not the sync.
func TestLostWakeIsFoundByLooking(t *testing.T) {
	e := newSyncEnv(t, func(int, contract.AttemptInput) contract.WorkOutcome {
		return result(t, contract.AttemptResult{Committed: 1})
	})
	e.plane.silent = true
	if err := e.run(t); err != nil {
		t.Fatal(err)
	}
	if e.finished.Result == nil || e.finished.Result.Committed != 1 || e.plane.looks == 0 {
		t.Fatalf("recorded %+v after %d looks", e.finished, e.plane.looks)
	}
}

// Cancelling the workflow cancels its attempt.
func TestCancellingASyncCancelsItsAttempt(t *testing.T) {
	e := newSyncEnv(t, func(int, contract.AttemptInput) contract.WorkOutcome { return result(t, contract.AttemptResult{}) })
	e.plane.run = func(contract.WorkInput) (time.Duration, contract.WorkOutcome) { return forever, contract.WorkOutcome{} }
	e.env.RegisterDelayedCallback(e.env.CancelWorkflow, time.Minute)
	if err := e.run(t); !temporal.IsCanceledError(err) {
		t.Fatalf("got %v, want cancelled", err)
	}
	if len(e.plane.cancelled) != 1 || e.plane.cancelled[0] != e.plane.sent[0].ID {
		t.Fatalf("cancelled %v of %+v", e.plane.cancelled, e.plane.sent)
	}
}

func TestPausedPipelineDoesNotSync(t *testing.T) {
	e := newSyncEnv(t, func(int, contract.AttemptInput) contract.WorkOutcome { return contract.WorkOutcome{} })
	e.started = contract.StartSyncResult{Paused: true}
	if err := e.run(t); err != nil || e.calls > 0 || e.finished.SyncID != "" {
		t.Fatalf("error %v, %d attempts, recorded %+v", err, e.calls, e.finished)
	}
}

// Check and Discover are light operations on the data plane, which wait a
// while for a runner.
func TestConnectorOpRunsOnTheDataPlane(t *testing.T) {
	for op, kind := range map[string]string{contract.OpCheck: contract.CheckConnector, contract.OpDiscover: contract.DiscoverSource} {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment()
		env.RegisterWorkflowWithOptions(workflows.Workflows{}.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
		p := newPlane(env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
			return time.Second, result(t, contract.ConnectorOpResult{Version: "v1", Catalog: input[contract.ConnectorOpInput](t, in).Op})
		})
		env.ExecuteWorkflow(contract.ConnectorOpWorkflow, contract.ConnectorOpInput{OrgID: "o", DataPlane: "dp1", Op: op, SourceID: "s"})
		var res contract.ConnectorOpResult
		if err := env.GetWorkflowResult(&res); err != nil {
			t.Fatal(err)
		}
		w := p.sent[0]
		if len(p.sent) != 1 || w.Kind != kind || w.Light != contract.LightOps || w.DataPlane != "dp1" || w.Pickup != contract.OpPickup || w.Size.CPU != 0 || res.Catalog != op {
			t.Errorf("%s: sent %+v, result %+v", op, p.sent, res)
		}
	}
}

// No runner taking an operation fails it, without a retry, in a way the API
// can tell.
func TestConnectorOpNoRunner(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	p := newPlane(env, func(contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		return contract.OpPickup, contract.WorkOutcome{Code: contract.NoRunner, Message: "no runner took it in time", NonRetryable: true}
	})
	env.ExecuteWorkflow(contract.ConnectorOpWorkflow, contract.ConnectorOpInput{OrgID: "o", DataPlane: "dp1", Op: contract.OpCheck, SourceID: "s"})
	var app *temporal.ApplicationError
	if err := env.GetWorkflowError(); !errors.As(err, &app) || app.Type() != contract.NoRunner {
		t.Fatalf("got %v", env.GetWorkflowError())
	}
	if len(p.sent) != 1 {
		t.Errorf("sent %d times", len(p.sent))
	}
}

func TestConnectorOpRetriesThreeTimes(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
	p := newPlane(env, func(contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		return time.Second, fail(errs.Transient)
	})
	env.ExecuteWorkflow(contract.ConnectorOpWorkflow, contract.ConnectorOpInput{OrgID: "o", DataPlane: "dp1", Op: contract.OpCheck, SourceID: "s"})
	if err := env.GetWorkflowError(); err == nil || len(p.sent) != 3 {
		t.Fatalf("%d sent, error %v", len(p.sent), err)
	}
	if !strings.Contains(p.sent[0].ID, contract.OpCheck) || p.sent[0].ID == p.sent[1].ID {
		t.Errorf("IDs %s and %s", p.sent[0].ID, p.sent[1].ID)
	}
}

func TestReadLogsIsALightOperation(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.ReadLogs, workflow.RegisterOptions{Name: contract.ReadLogsWorkflow})
	p := newPlane(env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		return time.Second, result(t, contract.ReadLogsResult{Lines: []contract.LogLine{{Task: "attempt-001", Text: "{}"}}, Next: contract.LogCursor{Key: input[contract.ReadLogsInput](t, in).SyncID, Lines: 1}})
	})
	env.ExecuteWorkflow(contract.ReadLogsWorkflow, contract.ReadLogsInput{DataPlane: "dp1", OrgID: "o", SyncID: "s1"})
	var res contract.ReadLogsResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.Lines) != 1 || res.Next.Key != "s1" {
		t.Fatalf("got %+v", res)
	}
	if w := p.sent[0]; w.Light != contract.LightLogs || w.Kind != contract.ReadLogs || w.Pickup != contract.LogsPickup {
		t.Errorf("sent %+v", w)
	}
}
