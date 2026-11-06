package temporalbackend

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/temporalx"
)

const (
	// Queue is the one task queue: workflows and their steps, on orchestrators.
	Queue = "cp.orchestrator"
	// The one signal, which fills a workflow's mailbox.
	Mailbox = "message"
	// A workflow should continue past this many events, at its next chance.
	maxHistory = 2000
)

// WorkflowRegistry is what workflows register with: a worker, a replayer or a
// test environment.
type WorkflowRegistry interface {
	RegisterWorkflowWithOptions(w any, o workflow.RegisterOptions)
}

// ActivityRegistry is what steps register with: a worker or a test
// environment.
type ActivityRegistry interface {
	RegisterActivityWithOptions(a any, o activity.RegisterOptions)
}

// RegisterWorkflow registers w under its name.
func RegisterWorkflow(r WorkflowRegistry, w orchestration.Workflow) {
	r.RegisterWorkflowWithOptions(func(ctx workflow.Context, in json.RawMessage) (json.RawMessage, error) {
		// Nothing in orchestration.Context is cancelled with the workflow.
		disconnected, _ := workflow.NewDisconnectedContext(ctx)
		c := &workflowContext{cancelled: ctx, ctx: disconnected, name: w.Name, mail: workflow.GetSignalChannel(ctx, Mailbox)}
		out, err := w.Run(c, in)
		return out, workflowError(err)
	}, workflow.RegisterOptions{Name: w.Name})
}

// RegisterStep registers s under its name, as an activity.
func RegisterStep(r ActivityRegistry, s orchestration.Step) {
	r.RegisterActivityWithOptions(func(ctx context.Context, in json.RawMessage) (json.RawMessage, error) {
		info := activity.GetInfo(ctx)
		out, err := s.Run(orchestration.WithStepInfo(ctx, orchestration.StepInfo{
			WorkflowID: info.WorkflowExecution.ID, RunID: info.WorkflowExecution.RunID, StepID: info.ActivityID,
		}), in)
		return out, activityError(err)
	}, activity.RegisterOptions{Name: s.Name})
}

// activityError is err as an application error whose type is its code.
// Context cancellation passes through, so Temporal sees the activity
// cancelled.
func activityError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return err
	}
	f := orchestration.NewFailure(err)
	return temporal.NewApplicationErrorWithOptions(f.Message, f.Code, temporal.ApplicationErrorOptions{
		NonRetryable: f.NonRetryable, NextRetryDelay: f.RetryAfter, Cause: err,
	})
}

// workflowError is what a workflow's error ends it with on Temporal.
func workflowError(err error) error {
	if errors.Is(err, orchestration.ErrStopped) {
		return temporal.NewCanceledError()
	}
	f, ok := errors.AsType[*orchestration.Failure](err)
	if !ok {
		return err
	}
	return temporal.NewApplicationErrorWithOptions(err.Error(), f.Code, temporal.ApplicationErrorOptions{NonRetryable: true})
}

// FailureOf is the error a step or a workflow ended with, without Temporal's
// wrapping if it failed by itself. Timeouts and the like stay as they are.
func FailureOf(err error) error {
	if err == nil {
		return nil
	}
	if temporal.IsCanceledError(err) {
		return orchestration.ErrStopped
	}
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		return &orchestration.Failure{Code: app.Type(), Message: app.Message(), NonRetryable: app.NonRetryable(), RetryAfter: app.NextRetryDelay()}
	}
	return err
}

type workflowContext struct {
	// Cancelled with the workflow, unlike ctx, which everything else runs on.
	cancelled workflow.Context
	ctx       workflow.Context
	name      string
	mail      workflow.ReceiveChannel
	// The workflow received Stop.
	stopped bool
}

func (c *workflowContext) Step(name string, o orchestration.StepOptions, in, out any) error {
	ctx := workflow.WithActivityOptions(c.ctx, workflow.ActivityOptions{
		TaskQueue:           Queue,
		StartToCloseTimeout: o.Timeout,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: o.Attempts, NonRetryableErrorTypes: errs.NonRetryable()},
	})
	return FailureOf(workflow.ExecuteActivity(ctx, name, in).Get(ctx, out))
}

func (c *workflowContext) Receive(timeout time.Duration) (orchestration.Message, bool) {
	var m orchestration.Message
	if c.mail.ReceiveAsync(&m) {
		return m, true
	}
	stopping := func() bool {
		if c.stopped || c.cancelled.Err() == nil {
			return false
		}
		c.stopped = true
		return true
	}
	if stopping() {
		return orchestration.Message{Kind: orchestration.Stop}, true
	}
	if timeout == 0 {
		return m, false
	}
	got := false
	sel := workflow.NewSelector(c.ctx)
	sel.AddReceive(c.mail, func(ch workflow.ReceiveChannel, _ bool) {
		ch.Receive(c.ctx, &m)
		got = true
	})
	if !c.stopped {
		sel.AddReceive(c.cancelled.Done(), func(workflow.ReceiveChannel, bool) {})
	}
	if timeout > 0 {
		timerCtx, stopTimer := workflow.WithCancel(c.ctx)
		defer stopTimer()
		sel.AddFuture(workflow.NewTimer(timerCtx, timeout), func(workflow.Future) {})
	}
	sel.Select(c.ctx)
	if got {
		return m, true
	}
	if stopping() {
		return orchestration.Message{Kind: orchestration.Stop}, true
	}
	return m, false
}

func (c *workflowContext) Now() time.Time { return workflow.Now(c.ctx) }

func (c *workflowContext) Version(change string) bool {
	return workflow.GetVersion(c.ctx, change, workflow.DefaultVersion, 1) != workflow.DefaultVersion
}

func (c *workflowContext) Info() orchestration.Info {
	e := workflow.GetInfo(c.ctx).WorkflowExecution
	return orchestration.Info{WorkflowID: e.ID, RunID: e.RunID}
}

func (c *workflowContext) SetAttributes(a orchestration.Attributes) {
	if err := workflow.UpsertTypedSearchAttributes(c.ctx, searchAttributes(a)...); err != nil {
		c.Logger().Warn("setting the search attributes failed", "err", err)
	}
}

func (c *workflowContext) ShouldContinue() bool {
	// Continuing would lose the cancelling.
	if !c.stopped && c.cancelled.Err() != nil {
		return false
	}
	i := workflow.GetInfo(c.ctx)
	return i.GetContinueAsNewSuggested() || i.GetCurrentHistoryLength() > maxHistory
}

func (c *workflowContext) Continue(input any) error {
	return workflow.NewContinueAsNewError(c.ctx, c.name, input)
}

func (c *workflowContext) Logger() orchestration.Logger { return workflow.GetLogger(c.ctx) }

func searchAttributes(a orchestration.Attributes) []temporal.SearchAttributeUpdate {
	var out []temporal.SearchAttributeUpdate
	for _, s := range []struct {
		key   temporal.SearchAttributeKeyKeyword
		value string
	}{{temporalx.OrgIDAttr, a.Org}, {temporalx.WorkspaceIDAttr, a.Workspace}, {temporalx.PipelineIDAttr, a.Pipeline}, {temporalx.DataPlaneIDAttr, a.DataPlane}} {
		if s.value != "" {
			out = append(out, s.key.ValueSet(s.value))
		}
	}
	if len(a.Connectors) > 0 {
		out = append(out, temporalx.ConnectorDefAttr.ValueSet(a.Connectors))
	}
	return out
}
