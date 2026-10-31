package workflows_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/orchestration/workflows"
)

func TestRetentionDropsTheMirrorsOldDays(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	now := time.Date(2026, 11, 1, 0, 30, 0, 0, time.UTC)
	env.SetStartTime(now)
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	var before time.Time
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.DropMirrorPartitionsInput) ([]string, error) {
		before = in.Before
		return []string{"checkpoint_mirror_20261001"}, nil
	}, activity.RegisterOptions{Name: contract.DropMirrorPartitions})
	env.ExecuteWorkflow(contract.RetentionWorkflow, contract.RetentionInput{CheckpointHistory: 30 * 24 * time.Hour})
	var res contract.RetentionResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if want := now.AddDate(0, 0, -30); !before.Equal(want) {
		t.Errorf("dropped days before %v, want %v", before, want)
	}
	if !slices.Equal(res.MirrorPartitions, []string{"checkpoint_mirror_20261001"}) {
		t.Errorf("result %+v", res)
	}
}

func TestRetentionNeedsAHistory(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	dropped := false
	env.RegisterActivityWithOptions(func(context.Context, contract.DropMirrorPartitionsInput) ([]string, error) {
		dropped = true
		return nil, nil
	}, activity.RegisterOptions{Name: contract.DropMirrorPartitions})
	env.ExecuteWorkflow(contract.RetentionWorkflow, contract.RetentionInput{})
	if env.GetWorkflowError() == nil || dropped {
		t.Fatal("a zero checkpoint history dropped everything")
	}
}
