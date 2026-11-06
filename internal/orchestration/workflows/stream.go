package workflows

import (
	"cmp"
	"errors"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
)

const (
	// A supervisor continues as new past this many events, at its next
	// chance: between runs, or after a job drains.
	maxStreamHistory = 2000
	// The first wait before a stream job's retry, unless AttemptRetry is set,
	// and the longest. A job that lasted healthyJob waits the first again.
	streamRetry    = 5 * time.Second
	maxStreamRetry = 5 * time.Minute
	healthyJob     = 10 * time.Minute
)

type runEnd int

const (
	runFailed runEnd = iota + 1
	runStopped
	runContinued // continued as new in the middle of the run
)

// StreamSupervisor runs a continuous pipeline: a run of its current version
// at a time, each one long-running job on a runner of its data plane,
// which it retries after failures without limit. A refresh signal makes
// it look at the pipeline again, and stop the run at a checkpoint if the
// pipeline has a new version or a hold released, is paused, became
// scheduled or is gone. A run
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
			temporalx.OrgIDAttr.ValueSet(in.OrgID),
			temporalx.WorkspaceIDAttr.ValueSet(in.Run.State.WorkspaceID),
			temporalx.PipelineIDAttr.ValueSet(in.PipelineID),
			temporalx.DataPlaneIDAttr.ValueSet(in.Run.State.DataPlane),
			temporalx.ConnectorDefAttr.ValueSet(in.Run.State.Connectors),
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
			finish.Error, finish.Code = message(err), code(err)
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
// that isn't retried, or the supervisor should continue as new. A drained job
// starts again at once, on another runner. A failed one starts again after a
// wait that doubles from AttemptRetry to maxStreamRetry, and starts over once
// a job has lasted healthyJob. A job outlives its workflow run, so continuing
// as new leaves it running, and the next workflow run waits for it.
func (w Workflows) runJobs(ctx workflow.Context, in *contract.StreamInput, refresh workflow.ReceiveChannel) (runEnd, error) {
	run := in.Run
	for {
		adopted := run.Job != ""
		if !adopted && run.Retry {
			stop, err := w.waitBackoff(ctx, in, refresh)
			if err != nil {
				return runStopped, err
			}
			if stop {
				run.Retry = false
				return runStopped, nil
			}
		}
		run.Retry = false
		if !adopted {
			attempt := run.Earlier + 1
			run.Job = workID(ctx, "job-"+run.SyncID, attempt)
			run.JobStart = workflow.Now(ctx)
			err := sendWork(ctx, contract.WorkInput{
				OrgID: in.OrgID, DataPlane: run.State.DataPlane, ID: run.Job, Kind: contract.RunStreamJob,
				Priority: contract.PriorityScheduled, Pool: run.State.Pool,
				Heartbeat: cmp.Or(w.StreamHeartbeat, 90*time.Second), Timeout: 365 * 24 * time.Hour,
				PipelineID: in.PipelineID, Version: run.State.Version, SyncID: run.SyncID, Attempt: attempt,
				Input: contract.StreamJobInput{
					OrgID: in.OrgID, PipelineID: in.PipelineID, Version: run.State.Version,
					SyncID: run.SyncID, RunKey: run.RunKey, Attempt: attempt,
				},
			})
			if err != nil {
				// It may have got through; a retry has another ID, and the
				// checkpoint log fences this one.
				workflow.GetLogger(ctx).Warn("sending the job failed", "err", err)
				run.Job = ""
				if ctx.Err() != nil {
					return runStopped, err
				}
				run.Earlier++
				run.Backoff = w.backoff(run.Backoff)
				run.Retry = true
				continue
			}
		}
		end, stopped, jobErr := w.awaitJob(ctx, in, refresh, adopted)
		if end == runContinued {
			return runContinued, nil
		}
		if stopped {
			run.Job = ""
			return runStopped, jobErr
		}
		if workflow.Now(ctx).Sub(run.JobStart) >= healthyJob {
			run.Backoff = 0
		}
		run.Job = ""
		run.Earlier++
		if drained(jobErr) {
			// A refresh waiting would be lost.
			if refresh.Len() == 0 && continueAsNew(ctx) {
				return runContinued, nil
			}
			continue
		}
		if jobErr == nil {
			return runFailed, errors.New("the job ended")
		}
		if !retryable(jobErr) {
			return runFailed, jobErr
		}
		run.Backoff = w.backoff(run.Backoff)
		run.Retry = true
		workflow.GetLogger(ctx).Warn("the job failed; retrying", "in", run.Backoff, "err", jobErr)
		if refresh.Len() == 0 && continueAsNew(ctx) {
			return runContinued, nil
		}
	}
}

// waitBackoff waits before a job's retry. A refresh that should stop the run
// ends the wait, and so does cancelling the workflow.
func (w Workflows) waitBackoff(ctx workflow.Context, in *contract.StreamInput, refresh workflow.ReceiveChannel) (stop bool, err error) {
	timerCtx, stopTimer := workflow.WithCancel(ctx)
	defer stopTimer()
	timer := workflow.NewTimer(timerCtx, in.Run.Backoff)
	for {
		elapsed := false
		sel := workflow.NewSelector(ctx)
		sel.AddFuture(timer, func(workflow.Future) { elapsed = true })
		sel.AddReceive(refresh, func(c workflow.ReceiveChannel, _ bool) {
			for c.ReceiveAsync(nil) {
			}
		})
		sel.Select(ctx)
		if ctx.Err() != nil {
			return true, ctx.Err()
		}
		if elapsed {
			return false, nil
		}
		if w.outdated(ctx, in) {
			return true, nil
		}
	}
}

// outdated looks at the pipeline again, and reports whether the run should
// stop. If it can't look, the run carries on.
func (w Workflows) outdated(ctx workflow.Context, in *contract.StreamInput) bool {
	run := in.Run
	var st contract.StreamState
	cp := workflow.WithActivityOptions(ctx, cpOptions)
	if err := workflow.ExecuteActivity(cp, contract.GetStreamState, contract.StreamInput{OrgID: in.OrgID, PipelineID: in.PipelineID}).Get(ctx, &st); err != nil {
		workflow.GetLogger(ctx).Warn("reading the pipeline failed; the run carries on", "err", err)
		return false
	}
	return !st.Continuous || st.Paused || st.Version != run.State.Version || st.DataPlane != run.State.DataPlane || st.Releases != run.State.Releases
}

// awaitJob waits for the run's job to end, stopping it at a checkpoint if a
// refresh or cancelling the workflow says to. adopted says it was running
// before this workflow run, which may have missed its wake. It returns
// runContinued if the supervisor should continue as new while the job runs,
// and otherwise whether it was stopped, and the error it ended with.
func (w Workflows) awaitJob(ctx workflow.Context, in *contract.StreamInput, refresh workflow.ReceiveChannel, adopted bool) (end runEnd, stopped bool, jobErr error) {
	run := in.Run
	ref := contract.WorkRef{OrgID: in.OrgID, ID: run.Job}
	wake := workflow.GetSignalChannel(ctx, contract.WakeSignal)
	// Stopping waits for the job, on a context cancelling doesn't reach.
	wctx, _ := workflow.NewDisconnectedContext(ctx)
	look := adopted
	stopping, stopSent := false, false
	var timer workflow.Future
	stopTimer := func() {}
	defer func() { stopTimer() }()
	for {
		if look {
			look = false
			st, err := workStatus(wctx, ref)
			switch {
			case err != nil:
				workflow.GetLogger(ctx).Warn("looking at the job failed; waiting", "err", err)
			case st.Done:
				return 0, stopping || ctx.Err() != nil, outcomeError(st.Outcome)
			}
		}
		if stopping && !stopSent {
			if err := cancelWork(wctx, ref); err != nil {
				workflow.GetLogger(ctx).Warn("stopping the job failed; trying again", "err", err)
				_ = workflow.Sleep(wctx, workCheckRetry)
			} else {
				stopSent = true
			}
			// It may have ended, or never started.
			look = true
			continue
		}
		if timer == nil {
			var timerCtx workflow.Context
			timerCtx, stopTimer = workflow.WithCancel(wctx)
			timer = workflow.NewTimer(timerCtx, workCheckEvery)
		}
		ticked, refreshed := false, false
		sel := workflow.NewSelector(wctx)
		sel.AddReceive(wake, func(c workflow.ReceiveChannel, _ bool) {
			var id string
			c.Receive(wctx, &id)
			look = id == ref.ID
		})
		sel.AddFuture(timer, func(workflow.Future) { ticked, look = true, true })
		if !stopping {
			sel.AddReceive(refresh, func(c workflow.ReceiveChannel, _ bool) {
				for c.ReceiveAsync(nil) {
				}
				refreshed = true
			})
			sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) { stopping = true })
		}
		sel.Select(wctx)
		if ticked {
			timer = nil
			// A refresh waiting would be lost.
			if !stopping && refresh.Len() == 0 && continueAsNew(ctx) {
				return runContinued, false, nil
			}
		}
		if refreshed && w.outdated(ctx, in) {
			stopping = true
		}
	}
}

// backoff is the wait before a job's retry, after a wait of last before the
// retry before it, or none since a job was healthy.
func (w Workflows) backoff(last time.Duration) time.Duration {
	if last == 0 {
		return cmp.Or(w.AttemptRetry, streamRetry)
	}
	return min(2*last, maxStreamRetry)
}

// retryable is whether a job that failed with err is retried: unless it was
// cancelled or failed with a code that fails the run. FENCED is retried: only
// another attempt of the same job claims a continuous pipeline, one that
// started late, and the job carries on by claiming it back.
func retryable(err error) bool {
	if temporal.IsCanceledError(err) {
		return false
	}
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return app.Type() == errs.Fenced.String() || !app.NonRetryable()
	}
	return true
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
