package dbosbackend

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
	// A workflow should continue past this many steps, at its next chance.
	// Running it again after a crash costs a query for each.
	maxSteps = 1000
	// Far enough off that nothing waits this long.
	forever = 100 * 365 * 24 * time.Hour
	// The wait before a step's first retry, doubled up to 100 times this.
	firstRetry = time.Second
	// A workflow recovered more often than this would be given up on, and a
	// long-lived one is recovered at every restart.
	maxRecoveries = 1 << 30
)

// Hooks are what a workflow runs with besides its steps.
type Hooks struct {
	Logger *slog.Logger
	// Continue starts a workflow's next generation, and returns its row.
	Continue func(g Generation, input json.RawMessage) (string, error)
	// Wrap changes the Context a workflow gets, for tests.
	Wrap func(orchestration.Context) orchestration.Context
	// The wait before a step's first retry. Zero: a second.
	FirstRetry time.Duration
}

// Generation says which generation of a workflow ends by continuing.
type Generation struct {
	ID         string
	Number     int
	Workflow   string
	Attributes orchestration.Attributes
	// What it starts the next with.
	Ctx dbos.Context
}

// Register registers workflows on c, to run with these steps.
func Register(c dbos.Context, workflows []orchestration.Workflow, steps []orchestration.Step, h Hooks) {
	if h.Logger == nil {
		h.Logger = slog.New(slog.DiscardHandler)
	}
	if h.FirstRetry == 0 {
		h.FirstRetry = firstRetry
	}
	byName := make(map[string]orchestration.Step, len(steps))
	for _, s := range steps {
		byName[s.Name] = s
	}
	for _, w := range workflows {
		dbos.RegisterWorkflow(c, runner(w, byName, h), dbos.WithWorkflowName(w.Name), dbos.WithInstance(instance(w.Name)), dbos.WithMaxRecoveryAttempts(maxRecoveries))
	}
}

// instance tells workflows apart: DBOS names them after their function, which
// is the same closure here, unless each is an instance of something.
type instance string

func (i instance) ConfigName() string { return string(i) }

// Enqueue starts a workflow on a context directly, on a queue of its own, for
// tests.
func Enqueue(c dbos.Context, queue, workflow, id string, input json.RawMessage) (dbos.WorkflowHandle[json.RawMessage], error) {
	return dbos.Enqueue[json.RawMessage](c, queue, workflow, envelope{ID: id, Input: input}, dbos.WithEnqueueWorkflowID(id), dbos.WithEnqueueConfigName(workflow))
}

// Send puts a message in a workflow's mailbox on a context directly, for tests.
func Send(c dbos.Client, row string, m orchestration.Message) error {
	return dbos.Send(c, row, m, mailbox)
}

// stopHere unwinds a workflow that has to stop midway: DBOS told a call to,
// because this process is stopping or another took the workflow over. The
// workflow runs again from its start, so nothing in it needs to know.
type stopHere struct{ err error }

func runner(w orchestration.Workflow, steps map[string]orchestration.Step, h Hooks) func(dbos.Context, envelope) (json.RawMessage, error) {
	return func(ctx dbos.Context, in envelope) (out json.RawMessage, err error) {
		row, err := dbos.GetWorkflowID(ctx)
		if err != nil {
			return nil, err
		}
		id := in.ID
		if id == "" {
			id = row
		}
		traced, span := tracer().Start(otel.GetTextMapPropagator().Extract(context.Background(), propagation.MapCarrier(in.Trace)), "RunWorkflow:"+w.Name)
		defer span.End()
		c := &workflowContext{
			ctx: ctx, id: id, row: row, generation: max(in.Generation, 1), name: w.Name, steps: steps, hooks: h, attrs: in.Attributes, replaying: true,
			span: trace.SpanContextFromContext(traced),
		}
		defer func() {
			if r := recover(); r != nil {
				s, ok := r.(stopHere)
				if !ok {
					panic(r)
				}
				out, err = nil, s.err
			}
		}()
		var wf orchestration.Context = c
		if h.Wrap != nil {
			wf = h.Wrap(c)
		}
		out, err = w.Run(wf, in.Input)
		if cont, ok := errors.AsType[*continued](err); ok {
			return c.continueWith(cont.input)
		}
		return out, workflowError(err)
	}
}

// workflowError is what a workflow's error ends it with: one that keeps its
// fields in the database.
func workflowError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, orchestration.ErrStopped):
		return &orchestration.Failure{Code: stoppedCode, Message: err.Error(), NonRetryable: true}
	}
	f := orchestration.NewFailure(err)
	return &orchestration.Failure{Code: f.Code, Message: err.Error(), NonRetryable: true}
}

// continued ends a workflow that should start over.
type continued struct{ input any }

func (c *continued) Error() string { return "continued as new" }

// stepResult is what's recorded of a step, whose failure is an outcome like
// any other. An error from DBOS means it isn't recorded.
type stepResult struct {
	Out     json.RawMessage        `json:",omitempty"`
	Failure *orchestration.Failure `json:",omitempty"`
}

type workflowContext struct {
	ctx        dbos.Context
	id         string
	row        string
	generation int
	name       string
	steps      map[string]orchestration.Step
	hooks      Hooks
	attrs      orchestration.Attributes
	// What its steps' spans are under.
	span trace.SpanContext
	// The workflow received Stop.
	stopped bool
	// Nothing has run yet that wasn't recorded, so it's going over old ground
	// after a crash, or hasn't logged anything to repeat.
	replaying bool
}

func (c *workflowContext) stop(err error) {
	panic(stopHere{err})
}

// plainContext ends with a step's context and carries none of DBOS's values,
// so that a step can call the backend like anything else. DBOS refuses those
// calls from inside a step.
type plainContext struct{ context.Context }

func (plainContext) Value(any) any { return nil }

func (c *workflowContext) Step(name string, o orchestration.StepOptions, in, out any) error {
	step, ok := c.steps[name]
	if !ok {
		return &orchestration.Failure{Code: errs.Internal.String(), Message: "no step named " + name, NonRetryable: true}
	}
	input, err := json.Marshal(in)
	if err != nil {
		return orchestration.NewFailure(err)
	}
	res, err := dbos.RunAsStep(c.ctx, func(ctx context.Context) (stepResult, error) {
		c.replaying = false
		n, err := c.ctx.GetStepID()
		if err != nil {
			return stepResult{}, err
		}
		info := orchestration.StepInfo{WorkflowID: c.id, RunID: c.row, StepID: strconv.Itoa(n)}
		return c.tries(ctx, step, o, input, info), nil
	}, dbos.WithStepName(name))
	if err != nil {
		c.stop(err)
	}
	if res.Failure != nil {
		return res.Failure
	}
	if out == nil || len(res.Out) == 0 {
		return nil
	}
	return json.Unmarshal(res.Out, out)
}

// tries runs a step until it succeeds or its tries run out.
func (c *workflowContext) tries(ctx context.Context, s orchestration.Step, o orchestration.StepOptions, input json.RawMessage, info orchestration.StepInfo) stepResult {
	wait := c.hooks.FirstRetry
	for try := int32(1); ; try++ {
		out, err := c.try(ctx, s, o, input, info)
		if err == nil {
			return stepResult{Out: out}
		}
		f := orchestration.NewFailure(err)
		if f.NonRetryable || (o.Attempts > 0 && try >= o.Attempts) {
			return stepResult{Failure: f}
		}
		next := wait
		if f.RetryAfter > 0 {
			next = f.RetryAfter
		}
		select {
		case <-ctx.Done():
			return stepResult{Failure: f}
		case <-time.After(next):
		}
		wait = min(2*wait, 100*c.hooks.FirstRetry)
	}
}

func (c *workflowContext) try(ctx context.Context, s orchestration.Step, o orchestration.StepOptions, input json.RawMessage, info orchestration.StepInfo) (json.RawMessage, error) {
	run := orchestration.WithStepInfo(plainContext{ctx}, info)
	if o.Timeout > 0 {
		var cancel context.CancelFunc
		run, cancel = context.WithTimeout(run, o.Timeout)
		defer cancel()
	}
	run, span := tracer().Start(trace.ContextWithSpanContext(run, c.span), "RunStep:"+s.Name)
	defer span.End()
	out, err := s.Run(run, input)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if run.Err() != nil && ctx.Err() == nil {
			return nil, errs.New(errs.Transient, "step %s took longer than %s: %v", s.Name, o.Timeout, err)
		}
	}
	return out, err
}

func tracer() trace.Tracer {
	return otel.Tracer("github.com/openetl-io/openetl/internal/orchestration/dbosbackend")
}

// traceHeaders are ctx's trace, for what it starts to continue.
func traceHeaders(ctx context.Context) map[string]string {
	headers := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, headers)
	if len(headers) == 0 {
		return nil
	}
	return headers
}

func (c *workflowContext) Receive(timeout time.Duration) (orchestration.Message, bool) {
	if timeout == orchestration.Forever {
		timeout = forever
	}
	for {
		m, err := dbos.Recv[orchestration.Message](c.ctx, mailbox, timeout)
		switch {
		case errors.Is(err, dbos.ErrTimeout):
			return orchestration.Message{}, false
		case err != nil:
			c.stop(err)
		case m.Kind != orchestration.Stop:
			return m, true
		case !c.stopped:
			c.stopped = true
			return m, true
		}
		// Told again, by a cancel that crossed a continue.
	}
}

// Now is a step, so it says the same when the workflow runs again.
func (c *workflowContext) Now() time.Time {
	t, err := dbos.RunAsStep(c.ctx, func(context.Context) (time.Time, error) {
		c.replaying = false
		return time.Now(), nil
	}, dbos.WithStepName("now"))
	if err != nil {
		c.stop(err)
	}
	return t
}

func (c *workflowContext) Version(change string) bool {
	patched, err := dbos.Patch(c.ctx, change)
	if err != nil {
		c.stop(err)
	}
	return patched
}

func (c *workflowContext) Info() orchestration.Info {
	return orchestration.Info{WorkflowID: c.id, RunID: c.row}
}

func (c *workflowContext) SetAttributes(a orchestration.Attributes) {
	if err := dbos.SetWorkflowAttributes(c.ctx, c.row, attributes(c.id, a), dbos.WithMergeAttributes()); err != nil {
		c.stop(err)
	}
	c.attrs = a
}

func (c *workflowContext) ShouldContinue() bool {
	n, err := c.ctx.GetStepID()
	if err != nil {
		c.stop(err)
	}
	return n > maxSteps
}

func (c *workflowContext) Continue(input any) error { return &continued{input} }

func (c *workflowContext) continueWith(input any) (json.RawMessage, error) {
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, orchestration.NewFailure(err)
	}
	if c.hooks.Continue == nil {
		return nil, workflowError(fmt.Errorf("%s continued, and nothing starts its next generation", c.id))
	}
	next, err := c.hooks.Continue(Generation{ID: c.id, Number: c.generation, Workflow: c.name, Attributes: c.attrs, Ctx: c.ctx}, raw)
	if err != nil {
		return nil, err
	}
	return json.Marshal(continuedOutput{ContinuedTo: next})
}

func (c *workflowContext) Logger() orchestration.Logger { return workflowLogger{c} }

// workflowLogger keeps quiet while a workflow goes over old ground, which it
// has logged already.
type workflowLogger struct{ c *workflowContext }

func (l workflowLogger) Info(msg string, keyvals ...any) {
	if !l.c.replaying {
		l.c.hooks.Logger.Info(msg, append(keyvals, "workflow", l.c.id)...)
	}
}

func (l workflowLogger) Warn(msg string, keyvals ...any) {
	if !l.c.replaying {
		l.c.hooks.Logger.Warn(msg, append(keyvals, "workflow", l.c.id)...)
	}
}
