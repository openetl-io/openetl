package workflows_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

// runSync runs SyncWorkflow with attempt answering each RunSyncAttempt. It
// returns the number of attempts, what FinishSync recorded and the
// workflow's error.
func runSync(t *testing.T, attempt func(n int, info activity.Info) (contract.AttemptResult, error)) (calls int, finished contract.FinishSyncInput, err error) {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	env.RegisterActivityWithOptions(func(context.Context, contract.SyncInput) (contract.StartSyncResult, error) {
		return contract.StartSyncResult{SyncID: "s1", Version: "v1", DataPlane: "default"}, nil
	}, activity.RegisterOptions{Name: contract.StartSync})
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.FinishSyncInput) error {
		finished = in
		return nil
	}, activity.RegisterOptions{Name: contract.FinishSync})
	env.RegisterActivityWithOptions(func(ctx context.Context, _ contract.AttemptInput) (contract.AttemptResult, error) {
		calls++
		return attempt(calls, activity.GetInfo(ctx))
	}, activity.RegisterOptions{Name: contract.RunSyncAttempt})
	env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
	if !env.IsWorkflowCompleted() {
		t.Fatal("the workflow didn't complete")
	}
	return calls, finished, env.GetWorkflowError()
}

func fail(code errs.Code) error { return temporalx.ApplicationError(errs.New(code, "attempt failed")) }

func drain(info activity.Info) error {
	return temporal.NewApplicationErrorWithOptions("drained", errs.Draining.String(), temporal.ApplicationErrorOptions{Details: []any{info.Attempt}})
}

func TestNonRetryableCodesFailTheSync(t *testing.T) {
	for _, code := range []errs.Code{errs.SchemaDrift, errs.ConfigInvalid, errs.Fenced} {
		calls, finished, err := runSync(t, func(int, activity.Info) (contract.AttemptResult, error) {
			return contract.AttemptResult{}, fail(code)
		})
		if err == nil || calls != 1 {
			t.Errorf("%s: %d attempts and error %v, want 1 attempt and an error", code, calls, err)
		}
		if !strings.Contains(finished.Error, code.String()) {
			t.Errorf("%s: recorded %q", code, finished.Error)
		}
	}
}

func TestRetryableCodesRetryFiveTimes(t *testing.T) {
	calls, _, err := runSync(t, func(int, activity.Info) (contract.AttemptResult, error) {
		return contract.AttemptResult{}, fail(errs.Transient)
	})
	if err == nil || calls != 5 {
		t.Fatalf("%d attempts and error %v, want 5 and an error", calls, err)
	}
}

// A drained attempt restarts at once, and doesn't count: one failure, a
// drain, then four more failures.
func TestDrainingDoesNotCount(t *testing.T) {
	calls, _, err := runSync(t, func(n int, info activity.Info) (contract.AttemptResult, error) {
		if n == 2 {
			if info.Attempt != 2 {
				t.Errorf("the drained call was attempt %d, want 2", info.Attempt)
			}
			return contract.AttemptResult{}, drain(info)
		}
		return contract.AttemptResult{}, fail(errs.Transient)
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
	calls, finished, err := runSync(t, func(n int, info activity.Info) (contract.AttemptResult, error) {
		if n < 3 {
			return contract.AttemptResult{}, drain(info)
		}
		return contract.AttemptResult{Generation: uint64(n), Committed: 1}, nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("%d calls and error %v, want 3 and none", calls, err)
	}
	if finished.Error != "" || finished.Result == nil || finished.Result.Generation != 3 {
		t.Fatalf("recorded %+v", finished)
	}
}

func TestPausedPipelineDoesNotSync(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	env.RegisterActivityWithOptions(func(context.Context, contract.SyncInput) (contract.StartSyncResult, error) {
		return contract.StartSyncResult{Paused: true}, nil
	}, activity.RegisterOptions{Name: contract.StartSync})
	ran := false
	env.RegisterActivityWithOptions(func(context.Context, contract.AttemptInput) (contract.AttemptResult, error) {
		ran = true
		return contract.AttemptResult{}, nil
	}, activity.RegisterOptions{Name: contract.RunSyncAttempt})
	env.RegisterActivityWithOptions(func(context.Context, contract.FinishSyncInput) error {
		ran = true
		return nil
	}, activity.RegisterOptions{Name: contract.FinishSync})
	env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
	if err := env.GetWorkflowError(); err != nil || ran {
		t.Fatalf("error %v, attempt or finish ran: %v", err, ran)
	}
}

// Check and Discover run on the data plane's queue, Discover with a
// heartbeat.
func TestConnectorOpRunsOnTheDataPlane(t *testing.T) {
	for _, op := range []string{contract.OpCheck, contract.OpDiscover} {
		var s testsuite.WorkflowTestSuite
		env := s.NewTestWorkflowEnvironment()
		env.RegisterWorkflowWithOptions(workflows.Workflows{}.ConnectorOp, workflow.RegisterOptions{Name: contract.ConnectorOpWorkflow})
		var ran []string
		for _, name := range []string{contract.CheckConnector, contract.DiscoverSource} {
			env.RegisterActivityWithOptions(func(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
				info := activity.GetInfo(ctx)
				ran = append(ran, name)
				if info.TaskQueue != contract.BatchQueue("dp1") {
					t.Errorf("%s ran on %s", name, info.TaskQueue)
				}
				if (name == contract.DiscoverSource) != (info.HeartbeatTimeout > 0) {
					t.Errorf("%s has heartbeat timeout %s", name, info.HeartbeatTimeout)
				}
				return contract.ConnectorOpResult{Version: "v1", Catalog: in.Op}, nil
			}, activity.RegisterOptions{Name: name})
		}
		env.ExecuteWorkflow(contract.ConnectorOpWorkflow, contract.ConnectorOpInput{OrgID: "o", DataPlane: "dp1", Op: op, SourceID: "s"})
		var res contract.ConnectorOpResult
		if err := env.GetWorkflowResult(&res); err != nil {
			t.Fatal(err)
		}
		want := contract.CheckConnector
		if op == contract.OpDiscover {
			want = contract.DiscoverSource
		}
		if len(ran) != 1 || ran[0] != want || res.Catalog != op {
			t.Errorf("%s: ran %v, result %+v", op, ran, res)
		}
	}
}
