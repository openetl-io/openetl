package workflows_test

import (
	"context"
	"strings"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

func steps(projects ...string) []contract.PostSyncStep {
	var s []contract.PostSyncStep
	for _, p := range projects {
		s = append(s, contract.PostSyncStep{Dbt: &contract.DbtStep{Project: p, Digest: "sha256:" + p, Command: "build"}})
	}
	return s
}

// runPostSync runs SyncWorkflow with post-sync steps. Its one attempt fails
// with attemptErr, if set; dbt answers each RunDbt. It returns the projects
// RunDbt was called for, in order, and what FinishSync recorded.
func runPostSync(t *testing.T, s []contract.PostSyncStep, attemptErr error, dbt func(contract.DbtInput) (contract.DbtResult, error)) (ran []string, finished contract.FinishSyncInput, err error) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
	env.RegisterActivityWithOptions(func(context.Context, contract.SyncInput) (contract.StartSyncResult, error) {
		return contract.StartSyncResult{SyncID: "s1", Version: "v1", DataPlane: "dp1", PostSync: s}, nil
	}, activity.RegisterOptions{Name: contract.StartSync})
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.FinishSyncInput) error {
		finished = in
		return nil
	}, activity.RegisterOptions{Name: contract.FinishSync})
	env.RegisterActivityWithOptions(func(context.Context, contract.AttemptInput) (contract.AttemptResult, error) {
		return contract.AttemptResult{Generation: 1, Committed: 2}, attemptErr
	}, activity.RegisterOptions{Name: contract.RunSyncAttempt})
	env.RegisterActivityWithOptions(func(ctx context.Context, in contract.DbtInput) (contract.DbtResult, error) {
		info := activity.GetInfo(ctx)
		if info.TaskQueue != contract.BatchQueue("dp1") || info.HeartbeatTimeout == 0 {
			t.Errorf("RunDbt ran on %s with heartbeat timeout %s", info.TaskQueue, info.HeartbeatTimeout)
		}
		if in.Version != "v1" || in.PipelineID != "p" || in.SyncID != "s1" || in.StepNumber < 1 || in.StepNumber > len(s) || s[in.StepNumber-1].Dbt.Project != in.Step.Project {
			t.Errorf("RunDbt got %+v", in)
		}
		ran = append(ran, in.Step.Project)
		return dbt(in)
	}, activity.RegisterOptions{Name: contract.RunDbt})
	env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
	if !env.IsWorkflowCompleted() {
		t.Fatal("the workflow didn't complete")
	}
	return ran, finished, env.GetWorkflowError()
}

func succeed(in contract.DbtInput) (contract.DbtResult, error) {
	return contract.DbtResult{Project: in.Step.Project, Digest: in.Step.Digest, Nodes: map[string]int{"success": 1}}, nil
}

func TestPostSyncStepsRunInOrder(t *testing.T) {
	ran, finished, err := runPostSync(t, steps("a", "b"), nil, succeed)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(ran, ",") != "a,b" {
		t.Errorf("ran %v", ran)
	}
	if finished.Error != "" || finished.Result == nil || len(finished.PostSync) != 2 || finished.PostSync[1].Dbt.Digest != "sha256:b" {
		t.Errorf("recorded %+v", finished)
	}
}

// A failed dbt command isn't retried, and fails the sync after its data is
// committed; the steps after it don't run.
func TestFailedDbtFailsTheSync(t *testing.T) {
	ran, finished, err := runPostSync(t, steps("a", "b"), nil, func(contract.DbtInput) (contract.DbtResult, error) {
		return contract.DbtResult{}, temporal.NewNonRetryableApplicationError("dbt build exited with 1: model.a: boom", contract.DbtFailed, nil)
	})
	if err == nil || strings.Join(ran, ",") != "a" {
		t.Fatalf("ran %v, error %v", ran, err)
	}
	if !strings.Contains(finished.Error, "post-sync step 1, dbt project a") || !strings.Contains(finished.Error, "model.a: boom") {
		t.Errorf("recorded error %q", finished.Error)
	}
	if finished.Result == nil || finished.Result.Committed != 2 || len(finished.PostSync) != 0 {
		t.Errorf("recorded %+v", finished)
	}
}

func TestPostSyncRetries(t *testing.T) {
	calls := 0
	ran, _, err := runPostSync(t, steps("a"), nil, func(in contract.DbtInput) (contract.DbtResult, error) {
		if calls++; calls < 3 {
			return contract.DbtResult{}, fail(errs.Transient)
		}
		return succeed(in)
	})
	if err != nil || len(ran) != 3 {
		t.Errorf("%d calls, error %v; want 3 and none", len(ran), err)
	}
	ran, _, err = runPostSync(t, steps("a"), nil, func(contract.DbtInput) (contract.DbtResult, error) {
		return contract.DbtResult{}, fail(errs.Transient)
	})
	if err == nil || len(ran) != 3 {
		t.Errorf("%d calls, error %v; want 3 and an error", len(ran), err)
	}
}

func TestNoPostSyncAfterAFailedSync(t *testing.T) {
	ran, finished, err := runPostSync(t, steps("a"), fail(errs.ConfigInvalid), succeed)
	if err == nil || len(ran) != 0 || finished.Result != nil {
		t.Errorf("ran %v, error %v, recorded %+v", ran, err, finished)
	}
}
