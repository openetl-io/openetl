// Package workflows holds the control plane's workflows. They do no I/O;
// steps do.
package workflows

import (
	"cmp"
	"fmt"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/resources"
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

// All is every workflow, for a backend to run.
func (w Workflows) All() []orchestration.Workflow {
	return []orchestration.Workflow{
		orchestration.NewWorkflow(contract.SyncWorkflow, w.Sync),
		orchestration.NewWorkflow(contract.StreamSupervisorWorkflow, func(ctx orchestration.Context, in contract.StreamInput) (struct{}, error) {
			return struct{}{}, w.StreamSupervisor(ctx, in)
		}),
		orchestration.NewWorkflow(contract.ConnectorOpWorkflow, w.ConnectorOp),
		orchestration.NewWorkflow(contract.RetentionWorkflow, w.Retention),
		orchestration.NewWorkflow(contract.CatalogRefreshWorkflow, w.CatalogRefresh),
		orchestration.NewWorkflow(contract.ReadLogsWorkflow, w.ReadLogs),
		orchestration.NewWorkflow(contract.RetentionProbeWorkflow, w.RetentionProbe),
	}
}

// cpStep is for most steps on the control plane.
var cpStep = orchestration.StepOptions{Timeout: 30 * time.Second, Attempts: 10}

// finishStep records how a sync or run ended, which nothing else does: its
// tries last through about an hour and a half of the database being away. They
// end, as a schedule starts nothing while its workflow is still here.
var finishStep = orchestration.StepOptions{Timeout: 30 * time.Second, Attempts: 60}

// Sync runs one sync of a pipeline: it records the sync, runs attempts on
// the pipeline's data plane until one succeeds or retries run out, runs the
// post-sync steps there, then records the outcome. It does nothing if the
// pipeline is paused or continuous. If it's lost, the next sync of the
// pipeline takes over: it stops what this one left running, and carries on
// from the checkpoint log.
func (w Workflows) Sync(ctx orchestration.Context, in contract.SyncInput) (*contract.SyncResult, error) {
	var started contract.StartSyncResult
	if err := ctx.Step(contract.StartSync, cpStep, in, &started); err != nil {
		return nil, err
	}
	switch {
	case started.Paused:
		ctx.Logger().Info("the pipeline is paused; not syncing")
		return nil, nil
	case started.Continuous:
		ctx.Logger().Info("the pipeline is continuous; not syncing")
		return nil, nil
	}
	ctx.SetAttributes(orchestration.Attributes{
		Org: in.OrgID, Workspace: started.WorkspaceID, Pipeline: in.PipelineID, DataPlane: started.DataPlane, Connectors: started.Connectors,
	})

	priority := contract.PriorityScheduled
	if started.Manual {
		priority = contract.PriorityManual
	}
	retry, heartbeat := cmp.Or(w.AttemptRetry, 30*time.Second), cmp.Or(w.AttemptHeartbeat, 2*time.Minute)
	runKey := ctx.Info().RunID
	b := &inbox{ctx: ctx}
	var res contract.AttemptResult
	err := b.runWork(retryPolicy{wait: retry, maxWait: 30 * time.Minute, attempts: attemptLimit}, &res, func(n int32) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: started.DataPlane, ID: workID(ctx, "attempt", n), Kind: contract.RunSyncAttempt,
			Priority: priority, Pool: started.Pool, Heartbeat: heartbeat, Timeout: attemptTimeout,
			PipelineID: in.PipelineID, Version: started.Version, SyncID: started.SyncID, Attempt: n,
			Input: contract.AttemptInput{
				OrgID: in.OrgID, PipelineID: in.PipelineID, Version: started.Version,
				SyncID: started.SyncID, RunKey: runKey, Attempt: n,
			},
		}
	})

	finish := contract.FinishSyncInput{OrgID: in.OrgID, SyncID: started.SyncID}
	if err == nil {
		finish.Result = &res
		finish.PostSync, err = postSync(b, in, started, priority, retry, heartbeat)
	}
	if err != nil {
		finish.Error, finish.Code = err.Error(), orchestration.CodeOf(err)
	}
	// Recorded even if the workflow is being cancelled.
	if ferr := ctx.Step(contract.FinishSync, finishStep, finish, nil); ferr != nil && err == nil {
		err = ferr
	}
	if err != nil {
		return nil, err
	}
	return &contract.SyncResult{SyncID: started.SyncID, Attempt: res}, nil
}

// postSync runs the post-sync steps in order, until one fails. A dbt
// command or Spark application that fails isn't retried.
func postSync(b *inbox, in contract.SyncInput, started contract.StartSyncResult, priority int, retry, heartbeat time.Duration) ([]contract.PostSyncResult, error) {
	ctx := b.ctx
	policy := retryPolicy{wait: retry, maxWait: 100 * retry, attempts: 3}
	runKey := ctx.Info().RunID
	step := func(i int, n int32, kind string, input any) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: started.DataPlane, ID: workID(ctx, fmt.Sprintf("step-%d", i+1), n), Kind: kind,
			Priority: priority, Pool: started.Pool, Size: resources.Default, Heartbeat: heartbeat, Timeout: attemptTimeout, Input: input,
		}
	}
	var results []contract.PostSyncResult
	for i, s := range started.PostSync {
		if sp := s.Spark; sp != nil {
			var r contract.SparkResult
			err := b.runWork(policy, &r, func(n int32) contract.WorkInput {
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
		err := b.runWork(policy, &r, func(n int32) contract.WorkInput {
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
