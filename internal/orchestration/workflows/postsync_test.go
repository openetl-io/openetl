package workflows_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
	"github.com/openetl-io/openetl/internal/platform/errs"
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
func runPostSync(t *testing.T, s []contract.PostSyncStep, attemptErr contract.WorkOutcome, dbt func(contract.DbtInput) contract.WorkOutcome) (ran []string, finished contract.FinishSyncInput, err error) {
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
	newPlane(env, func(w contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		if w.Kind == contract.RunSyncAttempt {
			if attemptErr.Code != "" {
				return time.Second, attemptErr
			}
			return time.Second, result(t, contract.AttemptResult{Generation: 1, Committed: 2})
		}
		in := input[contract.DbtInput](t, w)
		if w.Kind != contract.RunDbt || w.DataPlane != "dp1" || w.Heartbeat == 0 || w.Light != "" {
			t.Errorf("post-sync step sent as %+v", w)
		}
		if in.Version != "v1" || in.PipelineID != "p" || in.SyncID != "s1" || in.StepNumber < 1 || in.StepNumber > len(s) || s[in.StepNumber-1].Dbt.Project != in.Step.Project {
			t.Errorf("RunDbt got %+v", in)
		}
		ran = append(ran, in.Step.Project)
		return time.Second, dbt(in)
	})
	env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
	if !env.IsWorkflowCompleted() {
		t.Fatal("the workflow didn't complete")
	}
	return ran, finished, env.GetWorkflowError()
}

func succeeds(t *testing.T) func(contract.DbtInput) contract.WorkOutcome {
	return func(in contract.DbtInput) contract.WorkOutcome {
		return result(t, contract.DbtResult{Project: in.Step.Project, Digest: in.Step.Digest, Nodes: map[string]int{"success": 1}})
	}
}

func TestPostSyncStepsRunInOrder(t *testing.T) {
	ran, finished, err := runPostSync(t, steps("a", "b"), contract.WorkOutcome{}, succeeds(t))
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
	ran, finished, err := runPostSync(t, steps("a", "b"), contract.WorkOutcome{}, func(contract.DbtInput) contract.WorkOutcome {
		return contract.WorkOutcome{Code: contract.DbtFailed, Message: "dbt build exited with 1: model.a: boom", NonRetryable: true}
	})
	if err == nil || strings.Join(ran, ",") != "a" {
		t.Fatalf("ran %v, error %v", ran, err)
	}
	if want := "post-sync step 1, dbt project a: " + contract.DbtFailed + ": dbt build exited with 1: model.a: boom"; finished.Error != want {
		t.Errorf("recorded error %q", finished.Error)
	}
	if finished.Result == nil || finished.Result.Committed != 2 || len(finished.PostSync) != 0 {
		t.Errorf("recorded %+v", finished)
	}
}

func TestPostSyncRetries(t *testing.T) {
	calls := 0
	ran, _, err := runPostSync(t, steps("a"), contract.WorkOutcome{}, func(in contract.DbtInput) contract.WorkOutcome {
		if calls++; calls < 3 {
			return fail(errs.Transient)
		}
		return succeeds(t)(in)
	})
	if err != nil || len(ran) != 3 {
		t.Errorf("%d calls, error %v; want 3 and none", len(ran), err)
	}
	ran, _, err = runPostSync(t, steps("a"), contract.WorkOutcome{}, func(contract.DbtInput) contract.WorkOutcome {
		return fail(errs.Transient)
	})
	if err == nil || len(ran) != 3 {
		t.Errorf("%d calls, error %v; want 3 and an error", len(ran), err)
	}
}

func TestNoPostSyncAfterAFailedSync(t *testing.T) {
	ran, finished, err := runPostSync(t, steps("a"), fail(errs.ConfigInvalid), succeeds(t))
	if err == nil || len(ran) != 0 || finished.Result != nil {
		t.Errorf("ran %v, error %v, recorded %+v", ran, err, finished)
	}
}

// A Spark step runs RunSpark between dbt steps, and its failure fails the
// sync without a retry.
func TestSparkStep(t *testing.T) {
	for _, failed := range []bool{false, true} {
		var suite testsuite.WorkflowTestSuite
		env := suite.NewTestWorkflowEnvironment()
		env.RegisterWorkflowWithOptions(workflows.Workflows{}.Sync, workflow.RegisterOptions{Name: contract.SyncWorkflow})
		s := append(steps("a"), contract.PostSyncStep{Spark: &contract.SparkStep{Cluster: "lab", ClusterVersion: "cv1", Jar: "s3://j/a.jar"}})
		s = append(s, steps("b")...)
		env.RegisterActivityWithOptions(func(context.Context, contract.SyncInput) (contract.StartSyncResult, error) {
			return contract.StartSyncResult{SyncID: "s1", Version: "v1", DataPlane: "dp1", PostSync: s}, nil
		}, activity.RegisterOptions{Name: contract.StartSync})
		var finished contract.FinishSyncInput
		env.RegisterActivityWithOptions(func(_ context.Context, in contract.FinishSyncInput) error {
			finished = in
			return nil
		}, activity.RegisterOptions{Name: contract.FinishSync})
		var ran []string
		sparks := 0
		newPlane(env, func(w contract.WorkInput) (time.Duration, contract.WorkOutcome) {
			switch w.Kind {
			case contract.RunDbt:
				in := input[contract.DbtInput](t, w)
				ran = append(ran, in.Step.Project)
				return time.Second, succeeds(t)(in)
			case contract.RunSpark:
				in := input[contract.SparkInput](t, w)
				sparks++
				ran = append(ran, "spark")
				if w.DataPlane != "dp1" || in.StepNumber != 2 || in.SyncID != "s1" || in.Step.ClusterVersion != "cv1" || in.RunKey == "" {
					t.Errorf("RunSpark got %+v, sent as %+v", in, w)
				}
				if failed {
					return time.Second, contract.WorkOutcome{Code: contract.SparkFailed, Message: "driver FAILED", NonRetryable: true}
				}
				return time.Second, result(t, contract.SparkResult{Cluster: in.Step.Cluster, Submission: "driver-1", State: "FINISHED"})
			}
			return time.Second, result(t, contract.AttemptResult{Generation: 1, Committed: 2})
		})
		env.ExecuteWorkflow(contract.SyncWorkflow, contract.SyncInput{OrgID: "o", PipelineID: "p"})
		err := env.GetWorkflowError()
		switch {
		case !failed && (err != nil || strings.Join(ran, ",") != "a,spark,b" || len(finished.PostSync) != 3 || finished.PostSync[1].Spark.Submission != "driver-1"):
			t.Errorf("ran %v, recorded %+v, error %v", ran, finished, err)
		case failed && (err == nil || sparks != 1 || strings.Join(ran, ",") != "a,spark" || !strings.Contains(finished.Error, "post-sync step 2, Spark cluster lab: "+contract.SparkFailed)):
			t.Errorf("failed: ran %v, %d RunSpark, recorded %q", ran, sparks, finished.Error)
		}
	}
}
