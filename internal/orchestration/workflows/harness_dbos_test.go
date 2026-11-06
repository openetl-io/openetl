package workflows_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-transact-golang/dbos"
	_ "github.com/dbos-inc/dbos-transact-golang/dbos/driver/sqlite"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/dbosbackend"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
)

// onDBOS, set, runs this package's workflow tests on DBOS.
const onDBOS = "OPENETL_TEST_WORKFLOWS_ON_DBOS"

// TestWorkflowsOnDBOS runs the workflows' tests again on DBOS, in a process of
// their own so that each stays a plain test.
func TestWorkflowsOnDBOS(t *testing.T) {
	if os.Getenv(onDBOS) != "" {
		t.Skip("this is that run")
	}
	cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.skip", "TestReplayHistories|TestWorkflowsAreDeterministic")
	cmd.Env = append(os.Environ(), onDBOS+"=1")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}

const harnessQueue = "test"

// dbosHarness runs a workflow on DBOS, on SQLite. DBOS has no clock
// of its own to move, so the harness keeps one: a Receive that would wait runs
// the callbacks due in that time, and takes no real time itself.
type dbosHarness struct {
	t         *testing.T
	workflows []orchestration.Workflow
	steps     []orchestration.Step
	ctx       dbos.Context
	row       string

	mu        sync.Mutex
	now       time.Time
	callbacks []delayed
	limit     bool

	completed bool
	err       error
	out       json.RawMessage
	cont      *continued
}

type delayed struct {
	at time.Time
	f  func()
}

func newDBOSHarness(t *testing.T) *dbosHarness {
	t.Helper()
	return &dbosHarness{t: t, now: time.Now()}
}

func (h *dbosHarness) RegisterWorkflows(w workflows.Workflows) {
	h.workflows = append(h.workflows, w.All()...)
}

func (h *dbosHarness) RegisterWorkflow(w orchestration.Workflow) {
	h.workflows = append(h.workflows, w)
}

func (h *dbosHarness) RegisterStep(s orchestration.Step) { h.steps = append(h.steps, s) }

func (h *dbosHarness) RegisterDelayedCallback(f func(), d time.Duration) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.callbacks = append(h.callbacks, delayed{h.now.Add(d), f})
}

func (h *dbosHarness) Send(m orchestration.Message) {
	if err := dbosbackend.Send(h.ctx, h.row, m); err != nil {
		h.t.Errorf("sending %+v: %v", m, err)
	}
}

func (h *dbosHarness) CancelWorkflow() { h.Send(orchestration.Message{Kind: orchestration.Stop}) }

func (h *dbosHarness) Now() time.Time {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.now
}

func (h *dbosHarness) SetStartTime(at time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.now = at
}

func (h *dbosHarness) PastHistoryLimit() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.limit = true
}

func (h *dbosHarness) IsWorkflowCompleted() bool { return h.completed }

func (h *dbosHarness) GetWorkflowError() error {
	if h.cont != nil {
		return h.cont
	}
	return h.err
}

func (h *dbosHarness) GetWorkflowResult(out any) error { return json.Unmarshal(h.out, out) }

// ExecuteWorkflow runs the workflow until it ends, or waits with nothing left
// to wake it.
func (h *dbosHarness) ExecuteWorkflow(name any, in ...any) {
	h.t.Helper()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	c, err := dbos.NewContext(ctx, dbos.Config{AppName: "test", DatabaseURL: "sqlite:" + filepath.Join(h.t.TempDir(), "system.db"), Logger: slog.New(slog.DiscardHandler)})
	if err != nil {
		h.t.Fatal(err)
	}
	defer func() { _ = dbos.Shutdown(c, time.Second) }()
	h.ctx = c
	dbosbackend.Register(c, h.workflows, h.steps, dbosbackend.Hooks{
		FirstRetry: time.Millisecond,
		Wrap:       func(inner orchestration.Context) orchestration.Context { return &clocked{inner, h} },
		Continue: func(_ dbosbackend.Generation, input json.RawMessage) (string, error) {
			h.cont = &continued{input}
			return "next", nil
		},
	})
	if _, err := dbos.RegisterQueue(c, harnessQueue, dbos.WithQueueBasePollingInterval(5*time.Millisecond)); err != nil {
		h.t.Fatal(err)
	}
	if err := dbos.Launch(c); err != nil {
		h.t.Fatal(err)
	}
	var input json.RawMessage
	if len(in) > 0 {
		if input, err = json.Marshal(in[0]); err != nil {
			h.t.Fatal(err)
		}
	}
	h.row = fmt.Sprintf("test-%s", name)
	run, err := dbosbackend.Enqueue(c, harnessQueue, fmt.Sprint(name), h.row, input)
	if err != nil {
		h.t.Fatal(err)
	}
	type result struct {
		out json.RawMessage
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := run.GetResult(dbos.WithHandlePollingInterval(time.Millisecond))
		done <- result{out, err}
	}()
	select {
	case r := <-done:
		h.completed, h.out, h.err = true, r.out, dbosbackend.FailureOf(r.err)
	case <-time.After(time.Minute):
		h.t.Errorf("no end to the workflow after a minute")
	}
}

// clocked is a workflow's Context on the harness's clock.
type clocked struct {
	orchestration.Context
	h *dbosHarness
}

func (c *clocked) Now() time.Time { return c.h.Now() }

func (c *clocked) ShouldContinue() bool {
	c.h.mu.Lock()
	defer c.h.mu.Unlock()
	return c.h.limit
}

func (c *clocked) Receive(timeout time.Duration) (orchestration.Message, bool) {
	h := c.h
	deadline := h.Now().Add(timeout)
	for {
		if m, ok := c.Context.Receive(0); ok {
			return m, true
		}
		if timeout == 0 {
			return orchestration.Message{}, false
		}
		h.mu.Lock()
		i := -1
		for j, d := range h.callbacks {
			if (timeout == orchestration.Forever || !d.at.After(deadline)) && (i < 0 || d.at.Before(h.callbacks[i].at)) {
				i = j
			}
		}
		if i < 0 {
			if timeout == orchestration.Forever {
				h.mu.Unlock()
				select {} // Never wakes, as on Temporal's test environment.
			}
			h.now = deadline
			h.mu.Unlock()
			return orchestration.Message{}, false
		}
		d := h.callbacks[i]
		h.callbacks = slices.Delete(h.callbacks, i, i+1)
		if d.at.After(h.now) {
			h.now = d.at
		}
		h.mu.Unlock()
		d.f()
	}
}
