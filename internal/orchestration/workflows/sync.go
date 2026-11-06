// Package workflows holds the control plane's workflows. They do no I/O;
// activities do.
package workflows

import (
	"cmp"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

const (
	attemptLimit = 5
	// Restarts after DRAINING, which don't count against attemptLimit.
	maxDrains = 20
	// How long a sync's attempt or a post-sync step may run.
	attemptTimeout = 24 * time.Hour
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
		temporalx.OrgIDAttr.ValueSet(in.OrgID),
		temporalx.WorkspaceIDAttr.ValueSet(started.WorkspaceID),
		temporalx.PipelineIDAttr.ValueSet(in.PipelineID),
		temporalx.DataPlaneIDAttr.ValueSet(started.DataPlane),
		temporalx.ConnectorDefAttr.ValueSet(started.Connectors),
	)
	if err != nil {
		return nil, err
	}

	priority := contract.PriorityScheduled
	if started.Manual {
		priority = contract.PriorityManual
	}
	retry, heartbeat := cmp.Or(w.AttemptRetry, 30*time.Second), cmp.Or(w.AttemptHeartbeat, 2*time.Minute)
	runKey := workflow.GetInfo(ctx).WorkflowExecution.RunID
	var res contract.AttemptResult
	err = runWork(ctx, retryPolicy{wait: retry, maxWait: 30 * time.Minute, attempts: attemptLimit}, &res, func(n int32) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: started.DataPlane, ID: workID(ctx, "attempt", n), Kind: contract.RunSyncAttempt,
			Priority: priority, Size: resources.Default, Heartbeat: heartbeat, Timeout: attemptTimeout,
			PipelineID: in.PipelineID, Version: started.Version,
			Input: contract.AttemptInput{
				OrgID: in.OrgID, PipelineID: in.PipelineID, Version: started.Version,
				SyncID: started.SyncID, RunKey: runKey, Attempt: n,
			},
		}
	})

	finish := contract.FinishSyncInput{OrgID: in.OrgID, SyncID: started.SyncID}
	if err == nil {
		finish.Result = &res
		finish.PostSync, err = postSync(ctx, in, started, priority, retry, heartbeat)
	}
	if err != nil {
		finish.Error, finish.Code = message(err), code(err)
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
// command or Spark application that fails isn't retried.
func postSync(ctx workflow.Context, in contract.SyncInput, started contract.StartSyncResult, priority int, retry, heartbeat time.Duration) ([]contract.PostSyncResult, error) {
	policy := retryPolicy{wait: retry, maxWait: 100 * retry, attempts: 3}
	runKey := workflow.GetInfo(ctx).WorkflowExecution.RunID
	step := func(i int, n int32, kind string, input any) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: started.DataPlane, ID: workID(ctx, fmt.Sprintf("step-%d", i+1), n), Kind: kind,
			Priority: priority, Size: resources.Default, Heartbeat: heartbeat, Timeout: attemptTimeout, Input: input,
		}
	}
	var results []contract.PostSyncResult
	for i, s := range started.PostSync {
		if sp := s.Spark; sp != nil {
			var r contract.SparkResult
			err := runWork(ctx, policy, &r, func(n int32) contract.WorkInput {
				return step(i, n, contract.RunSpark, contract.SparkInput{
					OrgID: in.OrgID, PipelineID: in.PipelineID, Version: started.Version, Step: *sp,
					SyncID: started.SyncID, RunKey: runKey, StepNumber: i + 1, Attempt: n,
				})
			})
			if err != nil {
				return results, fmt.Errorf("post-sync step %d, Spark cluster %s: %w", i+1, sp.Cluster, err)
			}
			results = append(results, contract.PostSyncResult{Spark: &r})
			continue
		}
		var r contract.DbtResult
		err := runWork(ctx, policy, &r, func(n int32) contract.WorkInput {
			return step(i, n, contract.RunDbt, contract.DbtInput{
				OrgID: in.OrgID, PipelineID: in.PipelineID, Version: started.Version, Step: *s.Dbt,
				SyncID: started.SyncID, StepNumber: i + 1, Attempt: n,
			})
		})
		if err != nil {
			return results, fmt.Errorf("post-sync step %d, dbt project %s: %w", i+1, s.Dbt.Project, err)
		}
		results = append(results, contract.PostSyncResult{Dbt: &r})
	}
	return results, nil
}
