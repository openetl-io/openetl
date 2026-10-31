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
