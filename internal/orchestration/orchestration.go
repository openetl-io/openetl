// Package orchestration is what workflows are written against, so each runs
// on either backend (ADR-0028). Backends differ most in cancelling: Temporal
// lets a cancelled workflow run steps, and DBOS doesn't, so here cancelling
// is a message, and a workflow cleans up and ends by itself.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
)

// Stop is the kind of the message a cancelled workflow receives, once.
const Stop = "stop"

// Forever is a Receive that waits for as long as it takes.
const Forever time.Duration = -1

var (
	// ErrStopped is what a workflow ends with when it stopped because it was
	// cancelled, and what work it sent ends with if that was.
	ErrStopped = errors.New("cancelled")
	// ErrRunning: a schedule's workflow is running, so it can't be triggered.
	ErrRunning = errors.New("orchestration: a schedule's workflow is running")
)

// Context is a running workflow's way to do anything. A workflow runs again
// from its start after a crash, and its calls here return what they did the
// first time, so it does nothing else that could come out differently: no
// I/O, clocks, randomness or goroutines.
//
// Only Step can fail. A backend that has to stop a workflow midway unwinds it
// and runs it again later, so workflows needn't handle that.
type Context interface {
	// Step runs a step, retried as o says, and decodes its result into out,
	// which may be nil. A workflow's being cancelled doesn't interrupt it. It
	// fails with a *Failure if the step did.
	Step(name string, o StepOptions, in, out any) error
	// Receive returns the next message, waiting up to timeout, or false if
	// none came. Zero doesn't wait.
	Receive(timeout time.Duration) (Message, bool)
	Now() time.Time
	// Version reports whether the workflow runs the code this change adds. A
	// workflow that got here before the change was made doesn't.
	Version(change string) bool
	Info() Info
	// SetAttributes says what to find the workflow by.
	SetAttributes(Attributes)
	// ShouldContinue reports whether the workflow has done enough that it
	// should end with Continue, if it's long-lived.
	ShouldContinue() bool
	// Continue returns the error to end the workflow with so that it starts
	// over with this input, under the same ID and with nothing done yet. A
	// message waiting may be lost, so the new run must look at what a message
	// would have told it.
	Continue(input any) error
	Logger() Logger
}

// Logger logs from a workflow, once however often it runs again.
type Logger interface {
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
}

// StepOptions say how a step runs.
type StepOptions struct {
	// How long one try may take.
	Timeout time.Duration
	// How many tries it gets. A failure with a code that isn't retried ends
	// it sooner.
	Attempts int32
}

// Message is what can be sent to a workflow. A workflow has one mailbox, as
// DBOS can't wait on two.
type Message struct {
	Kind string
	// Which piece of work it's about, if any.
	Work string `json:",omitempty"`
}

// Info says which workflow is running.
type Info struct {
	WorkflowID string
	// Differs between runs of the workflow, and stays the same when it's run
	// again after a crash.
	RunID string
}

// Attributes are what to find a workflow by. Empty ones aren't set.
type Attributes struct {
	Org        string
	Workspace  string
	Pipeline   string
	DataPlane  string
	Connectors []string
}

// StepInfo says what a step belongs to, for what it records.
type StepInfo struct {
	WorkflowID string
	RunID      string
	// Stays the same across a step's tries.
	StepID string
}

type stepInfoKey struct{}

// WithStepInfo is for backends, on the context they run a step with.
func WithStepInfo(ctx context.Context, i StepInfo) context.Context {
	return context.WithValue(ctx, stepInfoKey{}, i)
}

// StepInfoOf says what the running step belongs to.
func StepInfoOf(ctx context.Context) StepInfo {
	i, _ := ctx.Value(stepInfoKey{}).(StepInfo)
	return i
}

// Failure is how a step or a workflow failed, with a taxonomy code or one of
// its own.
type Failure struct {
	Code         string
	Message      string
	NonRetryable bool
	// How long to wait before a retry, if it says.
	RetryAfter time.Duration
}

func (f *Failure) Error() string {
	if f.Code == "" || strings.HasPrefix(f.Message, f.Code+": ") {
		return f.Message
	}
	return f.Code + ": " + f.Message
}

// NewFailure classifies err by the taxonomy, unless it's a Failure already.
func NewFailure(err error) *Failure {
	if f, ok := errors.AsType[*Failure](err); ok {
		return f
	}
	code := errs.CodeOf(err)
	return &Failure{Code: code.String(), Message: err.Error(), NonRetryable: !code.Retryable(), RetryAfter: errs.RetryAfterOf(err)}
}

// CodeOf is the code err failed with, if any.
func CodeOf(err error) string {
	if f, ok := errors.AsType[*Failure](err); ok {
		return f.Code
	}
	return ""
}

// Workflow is a workflow a backend can run.
type Workflow struct {
	Name string
	Run  func(ctx Context, in json.RawMessage) (json.RawMessage, error)
}

// NewWorkflow makes a Workflow of a typed function.
func NewWorkflow[In, Out any](name string, run func(Context, In) (Out, error)) Workflow {
	return Workflow{Name: name, Run: func(ctx Context, raw json.RawMessage) (json.RawMessage, error) {
		var in In
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		out, err := run(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}}
}

// Step is a step a backend can run.
type Step struct {
	Name string
	Run  func(ctx context.Context, in json.RawMessage) (json.RawMessage, error)
}

// NewStep makes a Step of a typed function.
func NewStep[In, Out any](name string, run func(context.Context, In) (Out, error)) Step {
	return Step{Name: name, Run: func(ctx context.Context, raw json.RawMessage) (json.RawMessage, error) {
		var in In
		if err := json.Unmarshal(raw, &in); err != nil {
			return nil, err
		}
		out, err := run(ctx, in)
		if err != nil {
			return nil, err
		}
		return json.Marshal(out)
	}}
}

// NewAction makes a Step of a typed function with no result.
func NewAction[In any](name string, run func(context.Context, In) error) Step {
	return NewStep(name, func(ctx context.Context, in In) (struct{}, error) { return struct{}{}, run(ctx, in) })
}

// StartOptions say what to start.
type StartOptions struct {
	// Starting an ID that's running does nothing.
	ID       string
	Workflow string
	Input    any
	// How long it may take in all. Zero: as long as it likes.
	Timeout    time.Duration
	Attributes Attributes
}

// Run is a started workflow.
type Run interface {
	// Get waits for it to end and decodes its result into out, which may be
	// nil. It fails with a *Failure if the workflow did, and ErrStopped if it
	// was cancelled.
	Get(ctx context.Context, out any) error
}

// Schedule starts a workflow on a timetable, or when triggered, and never
// while one of its workflows is running.
type Schedule struct {
	ID string
	// One of these two. A cron is in UTC unless it starts with CRON_TZ=.
	Every time.Duration
	Cron  string
	// Up to how much later each start may be, so a fleet doesn't start at
	// once.
	Jitter time.Duration
	// The ID its workflows are named after. One schedule's workflows may get
	// more added to it, to keep them apart.
	WorkflowID string
	Workflow   string
	Input      any
	// Whose it is; empty for a whole deployment's.
	Org string
	// A new schedule starts paused, and otherwise, if StartNow, starts its
	// first workflow right away. Updating one leaves these as they were.
	Paused   bool
	StartNow bool
}

// ScheduleState is a schedule as listed.
type ScheduleState struct {
	ID     string
	Org    string
	Paused bool
}

// RunningWorkflow is a workflow as listed.
type RunningWorkflow struct {
	ID  string
	Org string
}

// Backend runs workflows. Its methods are safe to repeat.
type Backend interface {
	// Run runs workflows and steps here until ctx ends.
	Run(ctx context.Context, workflows []Workflow, steps []Step) error
	Close()

	// Start starts a workflow, or returns the one running under its ID.
	Start(ctx context.Context, o StartOptions) (Run, error)
	// Send sends a message to a workflow. It returns nil if the workflow
	// isn't running.
	Send(ctx context.Context, workflowID string, m Message) error
	// SendWithStart sends a message to a workflow, starting it if need be.
	SendWithStart(ctx context.Context, o StartOptions, m Message) error
	// Cancel tells a workflow to stop. It returns nil if it isn't running.
	Cancel(ctx context.Context, workflowID string) error
	// ListRunning lists the running workflows of a name. It may lag a little.
	ListRunning(ctx context.Context, workflow string) ([]RunningWorkflow, error)

	// UpsertSchedule creates a schedule, or updates what it runs and when.
	UpsertSchedule(ctx context.Context, s Schedule) error
	// DeleteSchedule returns nil if there's no such schedule. A running
	// workflow isn't stopped.
	DeleteSchedule(ctx context.Context, id string) error
	PauseSchedule(ctx context.Context, id string, paused bool) error
	// TriggerSchedule starts its workflow now, or returns ErrRunning.
	TriggerSchedule(ctx context.Context, id string) error
	ListSchedules(ctx context.Context) ([]ScheduleState, error)
}
