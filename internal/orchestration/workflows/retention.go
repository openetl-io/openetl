package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

// Retention deletes what the control plane no longer needs: the checkpoint
// mirror's days older than the checkpoint history. Sync attempts prune the
// checkpoint log themselves.
func (w Workflows) Retention(ctx workflow.Context, in contract.RetentionInput) (*contract.RetentionResult, error) {
	if in.CheckpointHistory <= 0 {
		return nil, temporalx.ApplicationError(errs.New(errs.ConfigInvalid, "the checkpoint history is %v; it must be positive", in.CheckpointHistory))
	}
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           contract.OrchestratorQueue,
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 5},
	})
	drop := contract.DropMirrorPartitionsInput{Before: workflow.Now(ctx).Add(-in.CheckpointHistory)}
	var res contract.RetentionResult
	if err := workflow.ExecuteActivity(ctx, contract.DropMirrorPartitions, drop).Get(ctx, &res.MirrorPartitions); err != nil {
		return nil, err
	}
	return &res, nil
}
