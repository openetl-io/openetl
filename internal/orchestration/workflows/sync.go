// Package workflows holds the control plane's workflows. They do no I/O;
// activities do.
package workflows

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

const (
	attemptLimit = 5
	// Restarts after DRAINING, which don't count against attemptLimit.
	maxDrains = 20
)

// Workflows holds settings shared by every run. They must not change while
// workflows are running, or replays diverge.
type Workflows struct {
	// The first retry delay of a failed sync attempt (zero: 30 s) or stream
	// job (zero: 5 s).
	AttemptRetry time.Duration
	// How long an attempt may go without a heartbeat. Zero: 2 min.
	AttemptHeartbeat time.Duration
	// How long a stream job may go without a heartbeat. Zero: 90 s.
	StreamHeartbeat time.Duration
}

// cpOptions are for control-plane activities.
var cpOptions = workflow.ActivityOptions{
	TaskQueue:           contract.OrchestratorQueue,
	StartToCloseTimeout: 30 * time.Second,
	RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 10, NonRetryableErrorTypes: errs.NonRetryable()},
}

// Sync runs one sync of a pipeline: it records the sync, runs attempts on
// the pipeline's data plane until one succeeds or retries run out, runs the
// post-sync steps there, then records the outcome. It does nothing if the
// pipeline is paused or continuous.
func (w Workflows) Sync(ctx workflow.Context, in contract.SyncInput) (*contract.SyncResult, error) {
	cp := workflow.WithActivityOptions(ctx, cpOptions)
	var started contract.StartSyncResult
	if err := workflow.ExecuteActivity(cp, contract.StartSync, in).Get(ctx, &started); err != nil {
		return nil, err
	}
	switch {
	case started.Paused:
		workflow.GetLogger(ctx).Info("the pipeline is paused; not syncing")
		return nil, nil
	case started.Continuous:
		workflow.GetLogger(ctx).Info("the pipeline is continuous; not syncing")
		return nil, nil
	}
	err := workflow.UpsertTypedSearchAttributes(ctx,
		contract.OrgIDAttr.ValueSet(in.OrgID),
		contract.WorkspaceIDAttr.ValueSet(started.WorkspaceID),
		contract.PipelineIDAttr.ValueSet(in.PipelineID),
		contract.DataPlaneIDAttr.ValueSet(started.DataPlane),
		contract.ConnectorDefAttr.ValueSet(started.Connectors),
	)
	if err != nil {
		return nil, err
	}

	retry, heartbeat := w.AttemptRetry, w.AttemptHeartbeat
	if retry == 0 {
		retry = 30 * time.Second
	}
	if heartbeat == 0 {
		heartbeat = 2 * time.Minute
	}
	attempts := int32(attemptLimit)
	var earlier int32
	var res contract.AttemptResult
	for drains := 0; ; drains++ {
		dp := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			TaskQueue:           contract.BatchQueue(started.DataPlane),
			StartToCloseTimeout: 24 * time.Hour,
			HeartbeatTimeout:    heartbeat,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    retry,
				BackoffCoefficient: 2,
				MaximumInterval:    30 * time.Minute,
				MaximumAttempts:    attempts,
				// DRAINING comes back here, to restart without counting.
				NonRetryableErrorTypes: append(errs.NonRetryable(), errs.Draining.String()),
			},
		})
		err = workflow.ExecuteActivity(dp, contract.RunSyncAttempt, contract.AttemptInput{
			OrgID:      in.OrgID,
			PipelineID: in.PipelineID,
			Version:    started.Version,
			SyncID:     started.SyncID,
			Earlier:    earlier,
		}).Get(ctx, &res)
		attempt, ok := drained(err)
		if !ok || drains == maxDrains {
			break
		}
		// The attempts that failed before the drained one still count.
		attempts -= attempt - 1
		earlier += attempt
	}

	finish := contract.FinishSyncInput{OrgID: in.OrgID, SyncID: started.SyncID}
	if err == nil {
		finish.Result = &res
		finish.PostSync, err = postSync(ctx, in, started, retry, heartbeat)
	}
	if err != nil {
		finish.Error, finish.Code = err.Error(), code(err)
	}
	// Recorded even if the workflow is being cancelled.
	fctx, _ := workflow.NewDisconnectedContext(cp)
	if ferr := workflow.ExecuteActivity(fctx, contract.FinishSync, finish).Get(fctx, nil); ferr != nil && err == nil {
		err = ferr
	}
	if err != nil {
		return nil, err
	}
	return &contract.SyncResult{SyncID: started.SyncID, Attempt: res}, nil
}

// postSync runs the post-sync steps in order, until one fails. A dbt
// command that fails isn't retried.
func postSync(ctx workflow.Context, in contract.SyncInput, started contract.StartSyncResult, retry, heartbeat time.Duration) ([]contract.PostSyncResult, error) {
	dp := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		TaskQueue:           contract.BatchQueue(started.DataPlane),
		StartToCloseTimeout: 24 * time.Hour,
		HeartbeatTimeout:    heartbeat,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        retry,
			BackoffCoefficient:     2,
			MaximumAttempts:        3,
			NonRetryableErrorTypes: errs.NonRetryable(),
		},
	})
	var results []contract.PostSyncResult
	for i, step := range started.PostSync {
		var r contract.DbtResult
		err := workflow.ExecuteActivity(dp, contract.RunDbt, contract.DbtInput{
			OrgID: in.OrgID, PipelineID: in.PipelineID, Version: started.Version, Step: *step.Dbt,
			SyncID: started.SyncID, StepNumber: i + 1,
		}).Get(ctx, &r)
		if err != nil {
			return results, fmt.Errorf("post-sync step %d, dbt project %s: %w", i+1, step.Dbt.Project, err)
		}
		results = append(results, contract.PostSyncResult{Dbt: &r})
	}
	return results, nil
}

// code is the taxonomy code an activity failed with, if any.
func code(err error) string {
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return app.Type()
	}
	return ""
}

// drained returns the attempt number of an attempt that failed with DRAINING.
func drained(err error) (int32, bool) {
	app, ok := errors.AsType[*temporal.ApplicationError](err)
	if !ok || app.Type() != errs.Draining.String() {
		return 0, false
	}
	attempt := int32(1)
	if app.HasDetails() {
		_ = app.Details(&attempt)
	}
	return attempt, true
}
