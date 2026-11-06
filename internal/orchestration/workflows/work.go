package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
	// How often a workflow waiting for work looks at it anyway. Waking it is
	// reliable, so this is only for what shouldn't happen, and it costs a few
	// events each time.
	workCheckEvery = 15 * time.Minute
	// How soon it looks again if it couldn't.
	workCheckRetry = 30 * time.Second
)

// inbox is a workflow's mailbox, and what it has taken from it and not acted
// on yet.
type inbox struct {
	ctx orchestration.Context
	// The workflow was cancelled.
	stopped bool
	// It was told to look at its pipeline again.
	refreshed bool
	// The last work it was told finished. Only the work it's waiting for
	// matters; a wake for earlier work is late.
	woken string
}

// next waits up to timeout for a message and takes it, or returns false.
func (b *inbox) next(timeout time.Duration) bool {
	m, ok := b.ctx.Receive(timeout)
	if !ok {
		return false
	}
	switch m.Kind {
	case orchestration.Stop:
		b.stopped = true
	case contract.RefreshMessage:
		b.refreshed = true
	case contract.WakeMessage:
		b.woken = m.Work
	}
	return true
}

// drain takes what's waiting.
func (b *inbox) drain() {
	for b.next(0) {
	}
}

// waitRefresh waits to be told to look at the pipeline again, or until the
// workflow is cancelled.
func (b *inbox) waitRefresh() {
	for !b.refreshed && !b.stopped {
		b.next(orchestration.Forever)
	}
	b.refreshed = false
}

// sleep waits for d, or until the workflow is cancelled.
func (b *inbox) sleep(d time.Duration) {
	until := b.ctx.Now().Add(d)
	for !b.stopped {
		left := until.Sub(b.ctx.Now())
		if left <= 0 || !b.next(left) {
			return
		}
	}
}

// pause waits for d, whatever happens.
func (b *inbox) pause(d time.Duration) {
	until := b.ctx.Now().Add(d)
	for {
		left := until.Sub(b.ctx.Now())
		if left <= 0 || !b.next(left) {
			return
		}
	}
}

// sendWork sends work to the data plane.
func (b *inbox) sendWork(in contract.WorkInput) error {
	in.WorkflowID = b.ctx.Info().WorkflowID
	return b.ctx.Step(contract.EnqueueWork, cpStep, in, nil)
}

// cancelWork stops work.
func (b *inbox) cancelWork(ref contract.WorkRef) error {
	return b.ctx.Step(contract.CancelWork, cpStep, ref, nil)
}

// workStatus looks at work.
func (b *inbox) workStatus(ref contract.WorkRef) (contract.WorkStatus, error) {
	var st contract.WorkStatus
	err := b.ctx.Step(contract.GetWork, cpStep, ref, &st)
	return st, err
}

// awaitWork waits for work to finish, as a wake says or a look finds. If the
// workflow is cancelled it cancels the work, and returns ErrStopped without
// waiting for it.
func (b *inbox) awaitWork(ref contract.WorkRef) (contract.WorkOutcome, error) {
	look := false
	for {
		if b.stopped {
			if err := b.cancelWork(ref); err != nil {
				b.ctx.Logger().Warn("cancelling the work failed", "work", ref.ID, "err", err)
			}
			return contract.WorkOutcome{}, orchestration.ErrStopped
		}
		if b.woken == ref.ID {
			b.woken = ""
			look = true
		}
		if !look {
			look = !b.next(workCheckEvery)
			continue
		}
		look = false
		st, err := b.workStatus(ref)
		if err != nil {
			b.ctx.Logger().Warn("looking at the work failed; waiting", "work", ref.ID, "err", err)
			b.sleep(workCheckRetry)
			look = true
			continue
		}
		if st.Done {
			return st.Outcome, nil
		}
	}
}

// outcomeError is the error work ended with, or nil.
func outcomeError(o contract.WorkOutcome) error {
	switch {
	case o.Cancelled:
		return orchestration.ErrStopped
	case o.Code == "":
		return nil
	}
	return &orchestration.Failure{Code: o.Code, Message: o.Message, NonRetryable: o.NonRetryable, RetryAfter: o.RetryAfter}
}

// retryPolicy is how a piece of work retries after failing.
type retryPolicy struct {
	// The first wait, doubled up to maxWait.
	wait, maxWait time.Duration
	// How many failures end it; zero: no limit.
	attempts int32
}

// runWork runs work until it succeeds or the policy gives up, and decodes its
// result into result. A drained attempt starts again at once, without
// counting. attempt builds the nth attempt.
func (b *inbox) runWork(policy retryPolicy, result any, attempt func(n int32) contract.WorkInput) error {
	var failures, drains, n int32
	for {
		n++
		in := attempt(n)
		var out contract.WorkOutcome
		err := b.sendWork(in)
		if err == nil {
			out, err = b.awaitWork(contract.WorkRef{OrgID: in.OrgID, ID: in.ID})
		}
		if err == nil {
			err = outcomeError(out)
		}
		if err == nil {
			return json.Unmarshal(out.Result, result)
		}
		if drained(err) && drains < maxDrains {
			drains++
			continue
		}
		failures++
		f, _ := errors.AsType[*orchestration.Failure](err)
		if b.stopped || errors.Is(err, orchestration.ErrStopped) || (f != nil && f.NonRetryable) || (policy.attempts > 0 && failures >= policy.attempts) {
			return err
		}
		wait := policy.wait << (failures - 1)
		if wait <= 0 || wait > policy.maxWait {
			wait = policy.maxWait
		}
		if f != nil && f.RetryAfter > 0 {
			wait = f.RetryAfter
		}
		b.sleep(wait)
		if b.stopped {
			return orchestration.ErrStopped
		}
	}
}

// workID names an attempt of a piece of work in this workflow run.
func workID(ctx orchestration.Context, what string, n int32) string {
	return fmt.Sprintf("%s/%s/%d", ctx.Info().RunID, what, n)
}

// drained reports whether work failed with DRAINING.
func drained(err error) bool {
	return orchestration.CodeOf(err) == errs.Draining.String()
}
