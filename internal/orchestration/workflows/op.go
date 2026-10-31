package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

// ConnectorOp runs Check or Discover on a runner of the data plane.
func (w Workflows) ConnectorOp(ctx workflow.Context, in contract.ConnectorOpInput) (*contract.ConnectorOpResult, error) {
	opts := workflow.ActivityOptions{
		TaskQueue:              contract.BatchQueue(in.DataPlane),
		ScheduleToStartTimeout: contract.OpPickup,
		StartToCloseTimeout:    2 * time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: errs.NonRetryable()},
	}
	activity := contract.CheckConnector
	if in.Op == contract.OpDiscover {
		activity = contract.DiscoverSource
		opts.StartToCloseTimeout, opts.HeartbeatTimeout = 10*time.Minute, time.Minute
	}
	var res contract.ConnectorOpResult
	if err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, opts), activity, in).Get(ctx, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
