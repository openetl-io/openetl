package workflows

import (
	"cmp"
	"errors"
	"slices"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

const (
	// A supervisor continues as new past this many events, at its next
	// chance: between runs, or after a job drains.
	maxStreamHistory = 2000
	// The first wait before a stream job's retry, unless AttemptRetry is set,
	// and the longest.
	streamRetry    = 5 * time.Second
	maxStreamRetry = 5 * time.Minute
)

type runEnd int

const (
	runFailed runEnd = iota + 1
	runStopped
	runContinued // continued as new in the middle of the run
)

// StreamSupervisor runs a continuous pipeline: a run of its current version
// at a time, each one long-running job on its data plane's stream queue,
// which Temporal retries after failures without limit. A refresh signal makes
// it look at the pipeline again, and stop the run at a checkpoint if the
// pipeline has a new version, is paused, became scheduled or is gone. A run
// that fails with a code that isn't retried waits for the next refresh. It
// ends when the pipeline is gone or scheduled.
func (w Workflows) StreamSupervisor(ctx workflow.Context, in contract.StreamInput) error {
	refresh := workflow.GetSignalChannel(ctx, contract.RefreshSignal)
	cp := workflow.WithActivityOptions(ctx, cpOptions)
	log := workflow.GetLogger(ctx)
	for {
		if in.Run == nil {
			if continueAsNew(ctx) {
				return workflow.NewContinueAsNewError(ctx, contract.StreamSupervisorWorkflow, in)
			}
			var st contract.StreamState
			if err := workflow.ExecuteActivity(cp, contract.GetStreamState, contract.StreamInput{OrgID: in.OrgID, PipelineID: in.PipelineID}).Get(ctx, &st); err != nil {
				return err
			}
			// Refreshes so far are answered.
			for refresh.ReceiveAsync(nil) {
			}
			if !st.Continuous {
				log.Info("the pipeline is gone or scheduled; stopping")
				return nil
			}
			if st.Paused {
				log.Info("the pipeline is paused; waiting")
				if err := waitRefresh(ctx, refresh); err != nil {
					return err
				}
				continue
			}
			var started contract.StartRunResult
			if err := workflow.ExecuteActivity(cp, contract.StartRun, contract.StartRunInput{OrgID: in.OrgID, PipelineID: in.PipelineID, Version: st.Version}).Get(ctx, &started); err != nil {
				return err
			}
			in.Run = &contract.StreamRun{State: st, SyncID: started.SyncID, RunKey: started.RunKey}
		}
		err := workflow.UpsertTypedSearchAttributes(ctx,
			contract.OrgIDAttr.ValueSet(in.OrgID),
			contract.WorkspaceIDAttr.ValueSet(in.Run.State.WorkspaceID),
			contract.PipelineIDAttr.ValueSet(in.PipelineID),
			contract.DataPlaneIDAttr.ValueSet(in.Run.State.DataPlane),
			contract.ConnectorDefAttr.ValueSet(in.Run.State.Connectors),
		)
		if err != nil {
			return err
		}

		end, err := w.runJobs(ctx, &in, refresh)
		if end == runContinued {
			return workflow.NewContinueAsNewError(ctx, contract.StreamSupervisorWorkflow, in)
		}
		finish := contract.FinishSyncInput{OrgID: in.OrgID, SyncID: in.Run.SyncID, Stopped: end == runStopped}
		if end == runFailed {
			finish.Error = err.Error()
		} else if err != nil {
			log.Info("the run stopped", "err", err)
		}
		// Recorded even if the workflow is being cancelled.
		fctx, _ := workflow.NewDisconnectedContext(cp)
		if ferr := workflow.ExecuteActivity(fctx, contract.FinishSync, finish).Get(fctx, nil); ferr != nil {
			log.Warn("recording the run's end failed", "sync", in.Run.SyncID, "err", ferr)
		}
		in.Run = nil
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if end == runFailed {
			log.Warn("the run failed; waiting for a refresh", "err", err)
			if err := waitRefresh(ctx, refresh); err != nil {
				return err
			}
		}
	}
}

// runJobs runs in.Run's job until it stops when asked to, fails with a code
// that isn't retried, or drains when the supervisor should continue as new.
// A drained job starts again at once, on another runner.
func (w Workflows) runJobs(ctx workflow.Context, in *contract.StreamInput, refresh workflow.ReceiveChannel) (runEnd, error) {
	run := in.Run
	cp := workflow.WithActivityOptions(ctx, cpOptions)
	for {
		jobCtx, cancel := workflow.WithCancel(ctx)
		dp := workflow.WithActivityOptions(jobCtx, workflow.ActivityOptions{
			TaskQueue: contract.StreamQueue(run.State.DataPlane),
			// A job runs until it's stopped; Temporal retries one that times out.
			StartToCloseTimeout: 365 * 24 * time.Hour,
			HeartbeatTimeout:    cmp.Or(w.StreamHeartbeat, 90*time.Second),
			// Cancelling stops the job at a checkpoint; wait for it.
			WaitForCancellation: true,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:        cmp.Or(w.AttemptRetry, streamRetry),
				BackoffCoefficient:     2,
				MaximumInterval:        maxStreamRetry,
				NonRetryableErrorTypes: streamNonRetryable(),
			},
		})
		job := workflow.ExecuteActivity(dp, contract.RunStreamJob, contract.StreamJobInput{
			OrgID: in.OrgID, PipelineID: in.PipelineID, Version: run.State.Version,
			SyncID: run.SyncID, RunKey: run.RunKey, Earlier: run.Earlier,
		})
		stopping := false
		var jobErr error
		for done := false; !done; {
			refreshed := false
			sel := workflow.NewSelector(ctx)
			sel.AddFuture(job, func(f workflow.Future) { jobErr, done = f.Get(ctx, nil), true })
			if !stopping {
				sel.AddReceive(refresh, func(c workflow.ReceiveChannel, _ bool) {
					for c.ReceiveAsync(nil) {
					}
					refreshed = true
				})
			}
			sel.Select(ctx)
			if !refreshed {
				continue
			}
			var st contract.StreamState
			if err := workflow.ExecuteActivity(cp, contract.GetStreamState, contract.StreamInput{OrgID: in.OrgID, PipelineID: in.PipelineID}).Get(ctx, &st); err != nil {
				workflow.GetLogger(ctx).Warn("reading the pipeline failed; the run carries on", "err", err)
				continue
			}
			if st.Continuous && !st.Paused && st.Version == run.State.Version && st.DataPlane == run.State.DataPlane {
				continue
			}
			stopping = true
			cancel()
		}
		cancel()
		if stopping || ctx.Err() != nil {
			return runStopped, jobErr
		}
		if attempt, ok := drained(jobErr); ok {
			run.Earlier += attempt
			// A refresh waiting would be lost.
			if refresh.Len() == 0 && continueAsNew(ctx) {
				return runContinued, nil
			}
			continue
		}
		if jobErr == nil {
			jobErr = errors.New("the job ended")
		}
		return runFailed, jobErr
	}
}

// streamNonRetryable is the codes that fail a run. DRAINING comes back to
// the supervisor, to restart at once. FENCED is retried: only another
// attempt of the same job claims a continuous pipeline, one Temporal timed
// out that started late, and the job carries on by claiming it back.
func streamNonRetryable() []string {
	codes := slices.DeleteFunc(errs.NonRetryable(), func(c string) bool { return c == errs.Fenced.String() })
	return append(codes, errs.Draining.String())
}

func continueAsNew(ctx workflow.Context) bool {
	info := workflow.GetInfo(ctx)
	return info.GetContinueAsNewSuggested() || info.GetCurrentHistoryLength() > maxStreamHistory
}

// waitRefresh waits for a refresh, and takes any others waiting with it.
func waitRefresh(ctx workflow.Context, refresh workflow.ReceiveChannel) error {
	refresh.Receive(ctx, nil)
	for refresh.ReceiveAsync(nil) {
	}
	return ctx.Err()
}
