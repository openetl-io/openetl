package workflows_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

// forever is longer than any test runs on the workflow's clock.
const forever = 1000 * time.Hour

// plane fakes a data plane for a workflow: the work it's sent ends after a
// wait on the workflow's clock, which wakes the workflow unless silent says
// not to.
type plane struct {
	env *testsuite.TestWorkflowEnvironment
	// How the work goes.
	run func(in contract.WorkInput) (time.Duration, contract.WorkOutcome)
	// Wakes get lost, and the workflow must look.
	silent bool

	mu sync.Mutex
	// Everything sent, in order, and when.
	sent  []contract.WorkInput
	times []time.Time
	done  map[string]contract.WorkOutcome
	// What it was asked to cancel.
	cancelled []string
	looks     int
}

func newPlane(env *testsuite.TestWorkflowEnvironment, run func(contract.WorkInput) (time.Duration, contract.WorkOutcome)) *plane {
	p := &plane{env: env, run: run, done: map[string]contract.WorkOutcome{}}
	env.RegisterActivityWithOptions(p.enqueue, activity.RegisterOptions{Name: contract.EnqueueWork})
	env.RegisterActivityWithOptions(p.status, activity.RegisterOptions{Name: contract.GetWork})
	env.RegisterActivityWithOptions(p.cancel, activity.RegisterOptions{Name: contract.CancelWork})
	return p
}

func (p *plane) enqueue(_ context.Context, in contract.WorkInput) error {
	p.mu.Lock()
	for _, s := range p.sent {
		if s.ID == in.ID {
			p.mu.Unlock()
			return nil
		}
	}
	p.sent = append(p.sent, in)
	p.times = append(p.times, p.env.Now())
	p.mu.Unlock()
	wait, out := p.run(in)
	p.env.RegisterDelayedCallback(func() { p.finish(in.ID, out) }, max(wait, time.Millisecond))
	return nil
}

func (p *plane) finish(id string, out contract.WorkOutcome) {
	p.mu.Lock()
	_, was := p.done[id]
	if !was {
		p.done[id] = out
	}
	p.mu.Unlock()
	if !was && !p.silent {
		p.env.SignalWorkflow(contract.WakeSignal, id)
	}
}

func (p *plane) status(_ context.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.looks++
	out, ok := p.done[ref.ID]
	return contract.WorkStatus{Done: ok, Outcome: out}, nil
}

// cancel stops work right away, as a runner would at a checkpoint.
func (p *plane) cancel(_ context.Context, ref contract.WorkRef) error {
	p.mu.Lock()
	p.cancelled = append(p.cancelled, ref.ID)
	p.mu.Unlock()
	p.env.RegisterDelayedCallback(func() { p.finish(ref.ID, contract.WorkOutcome{Cancelled: true}) }, time.Millisecond)
	return nil
}

func input[T any](t *testing.T, in contract.WorkInput) T {
	t.Helper()
	var v T
	b, err := json.Marshal(in.Input)
	if err == nil {
		err = json.Unmarshal(b, &v)
	}
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func result(t *testing.T, v any) contract.WorkOutcome {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return contract.WorkOutcome{Result: b}
}

func fail(code errs.Code) contract.WorkOutcome {
	return contract.WorkOutcome{Code: code.String(), Message: errs.New(code, "attempt failed").Error(), NonRetryable: !code.Retryable()}
}
