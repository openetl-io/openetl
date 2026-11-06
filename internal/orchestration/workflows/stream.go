package workflows

import (
	"cmp"
	"errors"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
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
//
// Nothing it knows lives only in the workflow: one started after another was
// lost takes over the run the database shows, and its job.
func (w Workflows) StreamSupervisor(ctx orchestration.Context, in contract.StreamInput) error {
	b := &inbox{ctx: ctx}
	log := ctx.Logger()
	resumed := in.Run != nil
	for {
		if in.Run == nil {
			b.drain()
			if b.stopped {
				return orchestration.ErrStopped
			}
			if ctx.ShouldContinue() {
				return ctx.Continue(in)
			}
			// Refreshes so far are answered by what this reads.
			b.refreshed = false
			var st contract.StreamState
			if err := ctx.Step(contract.GetStreamState, cpStep, contract.StreamInput{OrgID: in.OrgID, PipelineID: in.PipelineID}, &st); err != nil {
				return err
			}
			switch {
			case st.Left != nil:
				log.Info("taking over the run left running", "sync", st.Left.SyncID)
				in.Run, resumed = takeOver(st), true
			case !st.Continuous:
				log.Info("the pipeline is gone or scheduled; stopping")
				return nil
			case st.Paused:
				log.Info("the pipeline is paused; waiting")
				b.waitRefresh()
				continue
			default:
				var started contract.StartRunResult
				if err := ctx.Step(contract.StartRun, cpStep, contract.StartRunInput{OrgID: in.OrgID, PipelineID: in.PipelineID, Version: st.Version}, &started); err != nil {
					return err
				}
				in.Run = &contract.StreamRun{State: st, SyncID: started.SyncID, RunKey: started.RunKey}
			}
		}
		ctx.SetAttributes(orchestration.Attributes{
			Org: in.OrgID, Workspace: in.Run.State.WorkspaceID, Pipeline: in.PipelineID, DataPlane: in.Run.State.DataPlane, Connectors: in.Run.State.Connectors,
		})

		end, err := w.runJobs(b, &in, resumed)
		resumed = false
		if end == runContinued {
			return ctx.Continue(in)
		}
		finish := contract.FinishSyncInput{OrgID: in.OrgID, SyncID: in.Run.SyncID, Stopped: end == runStopped}
		if end == runFailed {
			finish.Error, finish.Code = err.Error(), orchestration.CodeOf(err)
		} else if err != nil {
			log.Info("the run stopped", "err", err)
		}
		// Recorded even if the workflow is being cancelled.
		if ferr := ctx.Step(contract.FinishSync, cpStep, finish, nil); ferr != nil {
			log.Warn("recording the run's end failed", "sync", in.Run.SyncID, "err", ferr)
		}
		in.Run = nil
		if end == runFailed && !b.stopped {
			log.Warn("the run failed; waiting for a refresh", "err", err)
			b.waitRefresh()
		}
		if b.stopped {
			return orchestration.ErrStopped
		}
	}
}

// takeOver is the run st says an earlier supervisor left running, as it was
// when its latest job started.
func takeOver(st contract.StreamState) *contract.StreamRun {
	left := st.Left
	state := st
	state.Left = nil
	state.Version, state.DataPlane, state.Releases = left.Version, left.DataPlane, left.Releases
	return &contract.StreamRun{State: state, SyncID: left.SyncID, RunKey: left.RunKey, Earlier: left.Earlier, Job: left.Job, JobStart: left.JobStart}
}

// runJobs runs in.Run's job until it stops when asked to, fails with a code
// that isn't retried, or the supervisor should continue as new. A drained job
// starts again at once, on another runner. A failed one starts again after a
// wait that doubles from AttemptRetry to maxStreamRetry, and starts over once
// a job has lasted healthyJob. A job outlives its workflow run, so continuing
// as new leaves it running, and the next workflow run waits for it. resumed
// says this workflow run didn't start the run, so a refresh may have gone to
// the one that did, and it looks at the pipeline first.
func (w Workflows) runJobs(b *inbox, in *contract.StreamInput, resumed bool) (runEnd, error) {
	ctx := b.ctx
	run := in.Run
	for {
		adopted := run.Job != ""
		if !adopted && ((resumed && w.outdated(b, in)) || (run.Retry && w.waitBackoff(b, in))) {
			run.Retry = false
			return runStopped, nil
		}
		look := resumed && adopted
		resumed = false
		run.Retry = false
		if b.stopped && !adopted {
			return runStopped, nil
		}
		if !adopted {
			attempt := run.Earlier + 1
			run.Job = workID(ctx, "job-"+run.SyncID, attempt)
			run.JobStart = ctx.Now()
			err := b.sendWork(contract.WorkInput{
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
				ctx.Logger().Warn("sending the job failed", "err", err)
				run.Job = ""
				if b.stopped {
					return runStopped, err
				}
				run.Earlier++
				run.Backoff = w.backoff(run.Backoff)
				run.Retry = true
				continue
			}
		}
		end, stopped, jobErr := w.awaitJob(b, in, adopted, look)
		if end == runContinued {
			return runContinued, nil
		}
		if stopped {
			run.Job = ""
			return runStopped, jobErr
		}
		if ctx.Now().Sub(run.JobStart) >= healthyJob {
			run.Backoff = 0
		}
		run.Job = ""
		run.Earlier++
		if drained(jobErr) {
			if ctx.ShouldContinue() {
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
		ctx.Logger().Warn("the job failed; retrying", "in", run.Backoff, "err", jobErr)
		if ctx.ShouldContinue() {
			return runContinued, nil
		}
	}
}

// waitBackoff waits before a job's retry. A refresh that should stop the run
// ends the wait, and so does cancelling the workflow.
func (w Workflows) waitBackoff(b *inbox, in *contract.StreamInput) (stop bool) {
	until := b.ctx.Now().Add(in.Run.Backoff)
	for {
		if b.stopped {
			return true
		}
		if b.refreshed {
			b.refreshed = false
			if w.outdated(b, in) {
				return true
			}
		}
		left := until.Sub(b.ctx.Now())
		if left <= 0 || !b.next(left) {
			return b.stopped
		}
	}
}

// outdated looks at the pipeline again, and reports whether the run should
// stop. If it can't look, the run carries on.
func (w Workflows) outdated(b *inbox, in *contract.StreamInput) bool {
	run := in.Run
	var st contract.StreamState
	if err := b.ctx.Step(contract.GetStreamState, cpStep, contract.StreamInput{OrgID: in.OrgID, PipelineID: in.PipelineID}, &st); err != nil {
		b.ctx.Logger().Warn("reading the pipeline failed; the run carries on", "err", err)
		return false
	}
	return !st.Continuous || st.Paused || st.Version != run.State.Version || st.DataPlane != run.State.DataPlane || st.Releases != run.State.Releases
}

// awaitJob waits for the run's job to end, stopping it at a checkpoint if a
// refresh or cancelling the workflow says to. adopted says it was running
// before this workflow run, which may have missed its wake, and resumed that
// it may have missed a refresh. It returns runContinued if the supervisor
// should continue as new while the job runs, and otherwise whether it was
// stopped, and the error it ended with.
func (w Workflows) awaitJob(b *inbox, in *contract.StreamInput, adopted, resumed bool) (end runEnd, stopped bool, jobErr error) {
	ctx := b.ctx
	ref := contract.WorkRef{OrgID: in.OrgID, ID: in.Run.Job}
	look := adopted
	stopping, stopSent := resumed && w.outdated(b, in), false
	for {
		if b.woken == ref.ID {
			b.woken = ""
			look = true
		}
		stopping = stopping || b.stopped
		if b.refreshed {
			b.refreshed = false
			stopping = stopping || w.outdated(b, in)
		}
		if look {
			look = false
			st, err := b.workStatus(ref)
			switch {
			case err != nil:
				ctx.Logger().Warn("looking at the job failed; waiting", "err", err)
			case st.Done:
				return 0, stopping, outcomeError(st.Outcome)
			}
		}
		if stopping && !stopSent {
			if err := b.cancelWork(ref); err != nil {
				ctx.Logger().Warn("stopping the job failed; trying again", "err", err)
				b.pause(workCheckRetry)
			} else {
				stopSent = true
			}
			// It may have ended, or never started.
			look = true
			continue
		}
		timedOut := !b.next(workCheckEvery)
		if b.woken == ref.ID || b.refreshed || b.stopped {
			continue
		}
		look = timedOut
		if !stopping && ctx.ShouldContinue() {
			return runContinued, false, nil
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
	if errors.Is(err, orchestration.ErrStopped) {
		return false
	}
	if f, ok := errors.AsType[*orchestration.Failure](err); ok {
		return f.Code == errs.Fenced.String() || !f.NonRetryable
	}
	return true
}
