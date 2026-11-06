package workflows

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

const (
	// How often a workflow waiting for work looks at it anyway. Waking it is
	// reliable, so this is only for what shouldn't happen, and it costs a few
	// events each time.
	workCheckEvery = 15 * time.Minute
	// How soon it looks again if it couldn't.
	workCheckRetry = 30 * time.Second
)

// sendWork sends work to the data plane. Cancelling ctx doesn't reach it, so
// the workflow knows whether it went, and can cancel it.
func sendWork(ctx workflow.Context, in contract.WorkInput) error {
	in.WorkflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	return workflow.ExecuteActivity(workflow.WithActivityOptions(dctx, cpOptions), contract.EnqueueWork, in).Get(dctx, nil)
}

// cancelWork stops work, on a context cancelling doesn't reach.
func cancelWork(ctx workflow.Context, ref contract.WorkRef) error {
	dctx, _ := workflow.NewDisconnectedContext(ctx)
	return workflow.ExecuteActivity(workflow.WithActivityOptions(dctx, cpOptions), contract.CancelWork, ref).Get(dctx, nil)
}

// workStatus looks at work, on the context given, which may be disconnected.
func workStatus(ctx workflow.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	var st contract.WorkStatus
	err := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx, cpOptions), contract.GetWork, ref).Get(ctx, &st)
	return st, err
}

// awaitWork waits for work to finish, as a wake says or a look finds. If ctx
// is cancelled it cancels the work, and returns ctx's error without waiting
// for it.
func awaitWork(ctx workflow.Context, ref contract.WorkRef) (contract.WorkOutcome, error) {
	wake := workflow.GetSignalChannel(ctx, contract.WakeSignal)
	for {
		look := false
		timerCtx, stopTimer := workflow.WithCancel(ctx)
		sel := workflow.NewSelector(ctx)
		sel.AddReceive(wake, func(c workflow.ReceiveChannel, _ bool) {
			var id string
			c.Receive(ctx, &id)
			look = id == ref.ID
		})
		sel.AddFuture(workflow.NewTimer(timerCtx, workCheckEvery), func(workflow.Future) { look = true })
		sel.AddReceive(ctx.Done(), func(workflow.ReceiveChannel, bool) {})
		sel.Select(ctx)
		stopTimer()
		if ctx.Err() != nil {
			if err := cancelWork(ctx, ref); err != nil {
				workflow.GetLogger(ctx).Warn("cancelling the work failed", "work", ref.ID, "err", err)
			}
			return contract.WorkOutcome{}, ctx.Err()
		}
		if !look {
			continue
		}
		st, err := workStatus(ctx, ref)
		if err != nil {
			workflow.GetLogger(ctx).Warn("looking at the work failed; waiting", "work", ref.ID, "err", err)
			if err := workflow.Sleep(ctx, workCheckRetry); err != nil {
				return contract.WorkOutcome{}, err
			}
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
		return temporal.NewCanceledError()
	case o.Code == "":
		return nil
	}
	return temporal.NewApplicationErrorWithOptions(o.Message, o.Code, temporal.ApplicationErrorOptions{NonRetryable: o.NonRetryable, NextRetryDelay: o.RetryAfter})
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
func runWork(ctx workflow.Context, policy retryPolicy, result any, attempt func(n int32) contract.WorkInput) error {
	var failures, drains, n int32
	for {
		n++
		in := attempt(n)
		var out contract.WorkOutcome
		err := sendWork(ctx, in)
		if err == nil {
			out, err = awaitWork(ctx, contract.WorkRef{OrgID: in.OrgID, ID: in.ID})
		}
		if err == nil {
			err = outcomeError(out)
		}
		if err == nil {
			// Decoding a result is deterministic, whatever reflect does inside.
			//workflowcheck:ignore
			return json.Unmarshal(out.Result, result)
		}
		if drained(err) && drains < maxDrains {
			drains++
			continue
		}
		failures++
		app, _ := errors.AsType[*temporal.ApplicationError](err)
		if ctx.Err() != nil || temporal.IsCanceledError(err) || (app != nil && app.NonRetryable()) || (policy.attempts > 0 && failures >= policy.attempts) {
			return err
		}
		wait := policy.wait << (failures - 1)
		if wait <= 0 || wait > policy.maxWait {
			wait = policy.maxWait
		}
		if app != nil && app.NextRetryDelay() > 0 {
			wait = app.NextRetryDelay()
		}
		if err := workflow.Sleep(ctx, wait); err != nil {
			return err
		}
	}
}

// workID names an attempt of a piece of work in this workflow run.
func workID(ctx workflow.Context, what string, n int32) string {
	return fmt.Sprintf("%s/%s/%d", workflow.GetInfo(ctx).WorkflowExecution.RunID, what, n)
}

// message is what err says without Temporal's wrapping: the failure's own
// message, after its code.
func message(err error) string {
	app, ok := errors.AsType[*temporal.ApplicationError](err)
	if !ok {
		return err.Error()
	}
	cause := app.Message()
	if t := app.Type(); t != "" && !strings.HasPrefix(cause, t+": ") {
		cause = t + ": " + cause
	}
	wrapped := app.Error()
	if act, ok := errors.AsType[*temporal.ActivityError](err); ok {
		wrapped = act.Error()
	}
	return strings.Replace(err.Error(), wrapped, cause, 1)
}

// code is the taxonomy code work failed with, if any.
func code(err error) string {
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return app.Type()
	}
	return ""
}

// drained reports whether work failed with DRAINING.
func drained(err error) bool {
	return code(err) == errs.Draining.String()
}
