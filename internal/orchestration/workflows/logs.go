package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

// ReadLogs reads a page of a sync's log on a runner of the data plane that
// ran it, where the log is.
func (w Workflows) ReadLogs(ctx workflow.Context, in contract.ReadLogsInput) (*contract.ReadLogsResult, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:              contract.LogsQueue(in.DataPlane),
		ScheduleToStartTimeout: contract.LogsPickup,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy:            &temporal.RetryPolicy{MaximumAttempts: 3, NonRetryableErrorTypes: errs.NonRetryable()},
	})
	var res contract.ReadLogsResult
	if err := workflow.ExecuteActivity(ctx, contract.ReadLogs, in).Get(ctx, &res); err != nil {
		return nil, err
	}
	return &res, nil
}
