package workflows_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

// What every backend's Context must do, which the workflows rely on.

// runContext runs body as a workflow, on a new harness. It gives it a step
// named "step" that step answers.
func runContext(t *testing.T, step func(context.Context, string) (string, error), setup func(harness), body func(orchestration.Context) (string, error)) (string, error) {
	t.Helper()
	h := newHarness(t)
	h.RegisterWorkflow(orchestration.NewWorkflow("test", func(ctx orchestration.Context, _ string) (string, error) { return body(ctx) }))
	if step != nil {
		h.RegisterStep(orchestration.NewStep("step", step))
	}
	if setup != nil {
		setup(h)
	}
	h.ExecuteWorkflow("test", "in")
	if !h.IsWorkflowCompleted() {
		t.Fatal("the workflow didn't complete")
	}
	if err := h.GetWorkflowError(); err != nil {
		return "", err
	}
	var out string
	if err := h.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	return out, nil
}

var oneTry = orchestration.StepOptions{Timeout: time.Minute, Attempts: 1}

func TestReceiveTimesOutOnTheClock(t *testing.T) {
	_, err := runContext(t, nil, nil, func(ctx orchestration.Context) (string, error) {
		start := ctx.Now()
		if _, ok := ctx.Receive(0); ok {
			t.Error("a message from nowhere")
		}
		if ctx.Now() != start {
			t.Error("not waiting took time")
		}
		if _, ok := ctx.Receive(time.Hour); ok {
			t.Error("a message from nowhere")
		}
		if got := ctx.Now().Sub(start); got != time.Hour {
			t.Errorf("waited %v", got)
		}
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Messages come in the order they were sent, ahead of a timeout, whether or
// not anything was waiting for them.
func TestMessagesComeInOrder(t *testing.T) {
	setup := func(h harness) {
		h.RegisterDelayedCallback(func() {
			h.Send(orchestration.Message{Kind: "a"})
			h.Send(orchestration.Message{Kind: "b", Work: "w"})
		}, time.Minute)
	}
	_, err := runContext(t, nil, setup, func(ctx orchestration.Context) (string, error) {
		first, ok := ctx.Receive(orchestration.Forever)
		if !ok || first.Kind != "a" {
			t.Errorf("first %+v", first)
		}
		second, ok := ctx.Receive(time.Hour)
		if !ok || second.Kind != "b" || second.Work != "w" {
			t.Errorf("second %+v", second)
		}
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A cancelled workflow is told once, and can still do what it needs to clean
// up: steps, waits and messages, before it ends however it likes.
func TestCancellingIsAMessage(t *testing.T) {
	ran := 0
	step := func(context.Context, string) (string, error) {
		ran++
		return "done", nil
	}
	setup := func(h harness) {
		h.RegisterDelayedCallback(h.CancelWorkflow, time.Minute)
		h.RegisterDelayedCallback(func() { h.Send(orchestration.Message{Kind: "late"}) }, 2*time.Minute)
	}
	out, err := runContext(t, step, setup, func(ctx orchestration.Context) (string, error) {
		start := ctx.Now()
		m, ok := ctx.Receive(orchestration.Forever)
		if !ok || m.Kind != orchestration.Stop || ctx.Now().Sub(start) != time.Minute {
			t.Errorf("got %+v, %v, after %v", m, ok, ctx.Now().Sub(start))
		}
		var res string
		if err := ctx.Step("step", oneTry, "x", &res); err != nil {
			t.Errorf("a step after cancelling: %v", err)
		}
		// Not told again.
		m, ok = ctx.Receive(time.Hour)
		if !ok || m.Kind != "late" || ctx.Now().Sub(start) != 2*time.Minute {
			t.Errorf("got %+v, %v, after %v", m, ok, ctx.Now().Sub(start))
		}
		if _, ok := ctx.Receive(time.Hour); ok {
			t.Error("told again")
		}
		return res, nil
	})
	if err != nil || out != "done" || ran != 1 {
		t.Fatalf("got %q, %v, after %d steps", out, err, ran)
	}
}

// Ending with ErrStopped is how a workflow says it was cancelled.
func TestStoppedWorkflow(t *testing.T) {
	setup := func(h harness) { h.RegisterDelayedCallback(h.CancelWorkflow, time.Minute) }
	_, err := runContext(t, nil, setup, func(ctx orchestration.Context) (string, error) {
		ctx.Receive(orchestration.Forever)
		return "", orchestration.ErrStopped
	})
	if !errors.Is(err, orchestration.ErrStopped) {
		t.Fatalf("got %v", err)
	}
}

// A step that keeps failing gets its tries and then fails with its code.
func TestStepRetriesThenFails(t *testing.T) {
	tries := 0
	step := func(context.Context, string) (string, error) {
		tries++
		return "", errs.New(errs.Transient, "try again")
	}
	_, err := runContext(t, step, nil, func(ctx orchestration.Context) (string, error) {
		err := ctx.Step("step", orchestration.StepOptions{Timeout: time.Minute, Attempts: 3}, "x", nil)
		f, ok := errors.AsType[*orchestration.Failure](err)
		if !ok || f.Code != errs.Transient.String() || f.NonRetryable || f.Error() != "TRANSIENT: try again" {
			t.Errorf("got %#v", err)
		}
		return "", nil
	})
	if err != nil || tries != 3 {
		t.Fatalf("%d tries, error %v", tries, err)
	}
}

// A code that isn't retried gets one try, and fails the workflow if it isn't
// handled, with its code.
func TestStepWithACodeNotRetried(t *testing.T) {
	tries := 0
	step := func(context.Context, string) (string, error) {
		tries++
		return "", errs.New(errs.ConfigInvalid, "no such thing")
	}
	_, err := runContext(t, step, nil, func(ctx orchestration.Context) (string, error) {
		return "", ctx.Step("step", orchestration.StepOptions{Timeout: time.Minute, Attempts: 3}, "x", nil)
	})
	if tries != 1 || orchestration.CodeOf(err) != errs.ConfigInvalid.String() {
		t.Fatalf("%d tries, error %v", tries, err)
	}
}

// What a step records keeps to one workflow run, and one step across its tries.
func TestStepInfo(t *testing.T) {
	var seen []orchestration.StepInfo
	step := func(ctx context.Context, _ string) (string, error) {
		seen = append(seen, orchestration.StepInfoOf(ctx))
		if len(seen) == 1 {
			return "", errs.New(errs.Transient, "try again")
		}
		return "", nil
	}
	var info orchestration.Info
	_, err := runContext(t, step, nil, func(ctx orchestration.Context) (string, error) {
		info = ctx.Info()
		if err := ctx.Step("step", orchestration.StepOptions{Timeout: time.Minute, Attempts: 2}, "x", nil); err != nil {
			return "", err
		}
		return "", ctx.Step("step", oneTry, "x", nil)
	})
	if err != nil || len(seen) != 3 {
		t.Fatalf("%d calls, error %v", len(seen), err)
	}
	if info.RunID == "" || seen[0].WorkflowID != info.WorkflowID || seen[0].RunID != info.RunID {
		t.Errorf("step %+v of workflow %+v", seen[0], info)
	}
	if seen[0].StepID == "" || seen[0].StepID != seen[1].StepID || seen[1].StepID == seen[2].StepID {
		t.Errorf("steps %+v", seen)
	}
}

// A new workflow runs the code a change adds.
func TestNewWorkflowHasTheVersion(t *testing.T) {
	_, err := runContext(t, nil, nil, func(ctx orchestration.Context) (string, error) {
		if !ctx.Version("a-change") {
			t.Error("no version")
		}
		return "", nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestContinueCarriesItsInput(t *testing.T) {
	setup := func(h harness) { h.PastHistoryLimit() }
	_, err := runContext(t, nil, setup, func(ctx orchestration.Context) (string, error) {
		if !ctx.ShouldContinue() {
			t.Error("it shouldn't continue")
		}
		return "", ctx.Continue(map[string]string{"next": "input"})
	})
	c, ok := errors.AsType[*continued](err)
	if !ok || string(c.input) != `{"next":"input"}` {
		t.Fatalf("got %v", err)
	}
}
