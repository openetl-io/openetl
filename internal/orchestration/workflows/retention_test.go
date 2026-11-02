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
	var audit contract.AuditPartitionsInput
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.AuditPartitionsInput) ([]string, error) {
		audit = in
		return []string{"audit_log_202607"}, nil
	}, activity.RegisterOptions{Name: contract.AuditPartitions})
	env.ExecuteWorkflow(contract.RetentionWorkflow, contract.RetentionInput{CheckpointHistory: 30 * 24 * time.Hour, AuditHistory: 90 * 24 * time.Hour})
	var res contract.RetentionResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if want := now.AddDate(0, 0, -30); !before.Equal(want) {
		t.Errorf("dropped days before %v, want %v", before, want)
	}
	if !audit.Now.Equal(now) || !audit.Before.Equal(now.AddDate(0, 0, -90)) {
		t.Errorf("audit log retention %+v, want months before %v dropped", audit, now.AddDate(0, 0, -90))
	}
	if !slices.Equal(res.MirrorPartitions, []string{"checkpoint_mirror_20261001"}) || !slices.Equal(res.AuditPartitions, []string{"audit_log_202607"}) {
		t.Errorf("result %+v", res)
	}
}

// Without an audit history, the audit log keeps every month, and still gets
// the next one's partition.
func TestRetentionKeepsTheAuditLog(t *testing.T) {
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(workflows.Workflows{}.Retention, workflow.RegisterOptions{Name: contract.RetentionWorkflow})
	env.RegisterActivityWithOptions(func(context.Context, contract.DropMirrorPartitionsInput) ([]string, error) { return nil, nil },
		activity.RegisterOptions{Name: contract.DropMirrorPartitions})
	var audit *contract.AuditPartitionsInput
	env.RegisterActivityWithOptions(func(_ context.Context, in contract.AuditPartitionsInput) ([]string, error) {
		audit = &in
		return nil, nil
	}, activity.RegisterOptions{Name: contract.AuditPartitions})
	env.ExecuteWorkflow(contract.RetentionWorkflow, contract.RetentionInput{CheckpointHistory: time.Hour})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatal(err)
	}
	if audit == nil || audit.Now.IsZero() || !audit.Before.IsZero() {
		t.Errorf("audit log retention %+v, want the next month only", audit)
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
