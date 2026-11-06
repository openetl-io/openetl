// Package orchestrationtest checks what every orchestration.Backend must do
// from outside a workflow, against a real one.
package orchestrationtest

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
	echo    = "conformance-echo"
	failing = "conformance-failing"
	mailbox = "conformance-mailbox"
)

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(100 * time.Millisecond) {
		if ok() {
			return
		}
	}
	t.Fatalf("timed out waiting for %s", what)
}

// Check runs the checks on b, which it runs workflows on until the test ends.
func Check(t *testing.T, b orchestration.Backend) {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- b.Run(ctx, []orchestration.Workflow{
			orchestration.NewWorkflow(echo, func(ctx orchestration.Context, in string) (string, error) {
				var out string
				err := ctx.Step(echo, orchestration.StepOptions{Timeout: 10 * time.Second, Attempts: 1}, in, &out)
				return out, err
			}),
			orchestration.NewWorkflow(failing, func(orchestration.Context, string) (string, error) {
				return "", orchestration.NewFailure(errs.New(errs.ConfigInvalid, "no such thing"))
			}),
			// Ends when told to stop, or returns the kinds of what it was sent.
			orchestration.NewWorkflow(mailbox, func(ctx orchestration.Context, _ string) (string, error) {
				kinds := ""
				for {
					m, _ := ctx.Receive(orchestration.Forever)
					switch m.Kind {
					case orchestration.Stop:
						return "", orchestration.ErrStopped
					case "end":
						return kinds, nil
					}
					kinds += m.Kind
				}
			}),
		}, []orchestration.Step{
			orchestration.NewStep(echo, func(_ context.Context, in string) (string, error) { return in, nil }),
		})
	}()
	t.Cleanup(func() {
		stop()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("running workflows: %v", err)
		}
	})

	t.Run("a workflow's result", func(t *testing.T) {
		run, err := b.Start(t.Context(), orchestration.StartOptions{ID: uuid.NewString(), Workflow: echo, Input: "hello"})
		if err != nil {
			t.Fatal(err)
		}
		var out string
		if err := run.Get(t.Context(), &out); err != nil || out != "hello" {
			t.Fatalf("got %q, %v", out, err)
		}
	})

	t.Run("a workflow's failure keeps its code", func(t *testing.T) {
		run, err := b.Start(t.Context(), orchestration.StartOptions{ID: uuid.NewString(), Workflow: failing, Input: ""})
		if err != nil {
			t.Fatal(err)
		}
		err = run.Get(t.Context(), nil)
		if f, ok := errors.AsType[*orchestration.Failure](err); !ok || f.Code != errs.ConfigInvalid.String() {
			t.Fatalf("got %#v", err)
		}
	})

	t.Run("nothing running is no error", func(t *testing.T) {
		id := uuid.NewString()
		if err := b.Send(t.Context(), id, orchestration.Message{Kind: "x"}); err != nil {
			t.Errorf("send: %v", err)
		}
		if err := b.Cancel(t.Context(), id); err != nil {
			t.Errorf("cancel: %v", err)
		}
		if err := b.DeleteSchedule(t.Context(), id); err != nil {
			t.Errorf("delete a schedule: %v", err)
		}
	})

	t.Run("an ID that's running starts nothing", func(t *testing.T) {
		o := orchestration.StartOptions{ID: uuid.NewString(), Workflow: mailbox, Input: ""}
		run, err := b.Start(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		again, err := b.Start(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Send(t.Context(), o.ID, orchestration.Message{Kind: "end"}); err != nil {
			t.Fatal(err)
		}
		for _, r := range []orchestration.Run{run, again} {
			if err := r.Get(t.Context(), nil); err != nil {
				t.Errorf("got %v", err)
			}
		}
	})

	t.Run("messages arrive in order", func(t *testing.T) {
		o := orchestration.StartOptions{ID: uuid.NewString(), Workflow: mailbox, Input: ""}
		run, err := b.Start(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.SendWithStart(t.Context(), o, orchestration.Message{Kind: "a"}); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"b", "end"} {
			if err := b.Send(t.Context(), o.ID, orchestration.Message{Kind: kind}); err != nil {
				t.Fatal(err)
			}
		}
		var kinds string
		if err := run.Get(t.Context(), &kinds); err != nil || kinds != "ab" {
			t.Fatalf("received %q, %v", kinds, err)
		}
	})

	t.Run("a message can start a workflow", func(t *testing.T) {
		o := orchestration.StartOptions{ID: uuid.NewString(), Workflow: mailbox, Input: ""}
		if err := b.SendWithStart(t.Context(), o, orchestration.Message{Kind: "a"}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Cancel(context.Background(), o.ID) })
		eventually(t, "the workflow to be listed", func() bool {
			list, err := b.ListRunning(t.Context(), mailbox)
			if err != nil {
				t.Fatal(err)
			}
			return slices.ContainsFunc(list, func(w orchestration.RunningWorkflow) bool { return w.ID == o.ID })
		})
	})

	t.Run("cancelling tells it to stop", func(t *testing.T) {
		o := orchestration.StartOptions{ID: uuid.NewString(), Workflow: mailbox, Input: ""}
		run, err := b.Start(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Cancel(t.Context(), o.ID); err != nil {
			t.Fatal(err)
		}
		if err := run.Get(t.Context(), nil); !errors.Is(err, orchestration.ErrStopped) {
			t.Fatalf("got %v", err)
		}
	})

	t.Run("running workflows are listed with their orgs", func(t *testing.T) {
		org := uuid.NewString()
		o := orchestration.StartOptions{ID: uuid.NewString(), Workflow: mailbox, Input: "", Attributes: orchestration.Attributes{Org: org}}
		if _, err := b.Start(t.Context(), o); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.Cancel(context.Background(), o.ID) })
		eventually(t, "the workflow to be listed", func() bool {
			list, err := b.ListRunning(t.Context(), mailbox)
			if err != nil {
				t.Fatal(err)
			}
			return slices.Contains(list, orchestration.RunningWorkflow{ID: o.ID, Org: org})
		})
	})

	t.Run("schedules", func(t *testing.T) {
		org := uuid.NewString()
		id := uuid.NewString()
		s := orchestration.Schedule{ID: id, Every: time.Hour, WorkflowID: id + "-run", Workflow: mailbox, Input: "", Org: org, Paused: true, StartNow: true}
		if err := b.UpsertSchedule(t.Context(), s); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = b.DeleteSchedule(context.Background(), id) })
		state := func() (orchestration.ScheduleState, bool) {
			list, err := b.ListSchedules(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			i := slices.IndexFunc(list, func(s orchestration.ScheduleState) bool { return s.ID == id })
			if i < 0 {
				return orchestration.ScheduleState{}, false
			}
			return list[i], true
		}
		eventually(t, "the schedule to be listed, paused", func() bool {
			got, ok := state()
			return ok && got.Paused && got.Org == org
		})
		// Updating it leaves it paused.
		s.Every, s.Paused = 2*time.Hour, false
		if err := b.UpsertSchedule(t.Context(), s); err != nil {
			t.Fatal(err)
		}
		if got, ok := state(); !ok || !got.Paused {
			t.Errorf("after an update: %+v", got)
		}
		if err := b.PauseSchedule(t.Context(), id, false); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the schedule to be listed, running", func() bool {
			got, ok := state()
			return ok && !got.Paused
		})

		// Triggering starts a workflow, and not while one is running.
		if err := b.TriggerSchedule(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the second trigger to refuse", func() bool {
			return errors.Is(b.TriggerSchedule(t.Context(), id), orchestration.ErrRunning)
		})
		var running string
		eventually(t, "its workflow to be listed", func() bool {
			list, err := b.ListRunning(t.Context(), mailbox)
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range list {
				if strings.HasPrefix(w.ID, s.WorkflowID) {
					running = w.ID
				}
			}
			return running != ""
		})
		if err := b.Cancel(t.Context(), running); err != nil {
			t.Fatal(err)
		}

		if err := b.DeleteSchedule(t.Context(), id); err != nil {
			t.Fatal(err)
		}
		eventually(t, "the schedule to go", func() bool {
			_, ok := state()
			return !ok
		})
	})
}
