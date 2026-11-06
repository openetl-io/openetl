//go:build integration

package dbosbackend_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/dbosbackend"
	"github.com/openetl-io/openetl/internal/orchestration/orchestrationtest"
)

func metadataURL() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_DATABASE_URL"), "postgres://openetl:openetl@localhost:5432/openetl")
}

// open opens a backend on a system database of its own.
func open(t *testing.T) (*dbosbackend.Backend, dbosbackend.Options) {
	t.Helper()
	system, err := dbosbackend.SystemDatabaseURL(metadataURL())
	if err != nil {
		t.Fatal(err)
	}
	name := "openetl_it_" + strings.ToLower(rand.Text())[:10]
	system = strings.Replace(system, "openetl_dbos", name, 1)
	t.Cleanup(func() { dropSystem(t, name) })
	o := dbosbackend.Options{DatabaseURL: system, ExecutorID: "it"}
	b, err := dbosbackend.Open(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b, o
}

// A server and an orchestrator starting together on a new install both create
// the system database.
func TestOpeningTogether(t *testing.T) {
	_, o := open(t)
	name := "openetl_it_" + strings.ToLower(rand.Text())[:10]
	url := strings.Replace(o.DatabaseURL, o.DatabaseURL[strings.LastIndex(o.DatabaseURL, "/")+1:], name, 1)
	t.Cleanup(func() { dropSystem(t, name) })
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			b, err := dbosbackend.Open(t.Context(), dbosbackend.Options{DatabaseURL: url, ExecutorID: "it"})
			if err != nil {
				t.Error(err)
				return
			}
			b.Close()
		})
	}
	wg.Wait()
}

func TestBackend(t *testing.T) {
	b, _ := open(t)
	orchestrationtest.Check(t, b)
}

const waitLong = "wait-long"

// counted is a workflow that takes a step, waits for a message, takes another
// and returns what it was sent. It says how often each step really ran.
type counted struct {
	mu            sync.Mutex
	before, after int
	// What it was last started with.
	input string
}

func (c *counted) count(n *int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	*n++
	return *n
}

func (c *counted) get(n *int) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return *n
}

func (c *counted) register(name string) ([]orchestration.Workflow, []orchestration.Step) {
	return []orchestration.Workflow{orchestration.NewWorkflow(name, func(ctx orchestration.Context, in string) (string, error) {
			if err := ctx.Step("before", once, in, nil); err != nil {
				return "", err
			}
			m, _ := ctx.Receive(orchestration.Forever)
			return m.Kind, ctx.Step("after", once, "", nil)
		})}, []orchestration.Step{
			orchestration.NewAction("before", func(_ context.Context, in string) error {
				c.count(&c.before)
				c.mu.Lock()
				defer c.mu.Unlock()
				c.input = in
				return nil
			}),
			orchestration.NewAction("after", func(context.Context, string) error { c.count(&c.after); return nil }),
		}
}

var once = orchestration.StepOptions{Timeout: 10 * time.Second, Attempts: 1}

// run runs workflows on b until stop is called.
func run(t *testing.T, b *dbosbackend.Backend, workflows []orchestration.Workflow, steps []orchestration.Step) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- b.Run(ctx, workflows, steps) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Errorf("running workflows: %v", err)
			}
		})
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	for deadline := time.Now().Add(timeout); !ok(); time.Sleep(50 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

func dropSystem(t *testing.T, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	c, err := pgx.Connect(ctx, metadataURL())
	if err != nil {
		t.Log(err)
		return
	}
	defer func() { _ = c.Close(ctx) }()
	if _, err := c.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
		t.Log(err)
	}
}

func another(t *testing.T, o dbosbackend.Options) *dbosbackend.Backend {
	t.Helper()
	b, err := dbosbackend.Open(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}

// An orchestrator that starts again goes on from where its workflows were.
func TestRestartGoesOn(t *testing.T) {
	b, _ := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	stop := run(t, b, workflows, steps)
	r, err := b.Start(t.Context(), orchestration.StartOptions{ID: "restarted", Workflow: waitLong, Input: ""})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the first step", func() bool { return c.get(&c.before) == 1 })
	stop()
	// What's sent while nothing runs waits.
	if err := b.Send(t.Context(), "restarted", orchestration.Message{Kind: "go"}); err != nil {
		t.Fatal(err)
	}
	run(t, b, workflows, steps)
	var out string
	if err := r.Get(t.Context(), &out); err != nil || out != "go" {
		t.Fatalf("got %q, %v", out, err)
	}
	if got := c.get(&c.before); got != 1 {
		t.Errorf("the first step ran %d times", got)
	}
	if got := c.get(&c.after); got != 1 {
		t.Errorf("the second step ran %d times", got)
	}
}

// A workflow that continues keeps its ID: what's sent finds the newest
// generation, what was waiting moves with it, and Get follows it.
func TestContinuing(t *testing.T) {
	b, _ := open(t)
	release := make(chan struct{})
	var mu sync.Mutex
	var runs []orchestration.Info
	wf := orchestration.NewWorkflow("continues", func(ctx orchestration.Context, n int) (string, error) {
		mu.Lock()
		runs = append(runs, ctx.Info())
		mu.Unlock()
		if n == 0 {
			if err := ctx.Step("hold", once, "", nil); err != nil {
				return "", err
			}
		}
		if n < 2 {
			return "", ctx.Continue(n + 1)
		}
		first, _ := ctx.Receive(orchestration.Forever)
		second, _ := ctx.Receive(orchestration.Forever)
		return first.Kind + second.Kind, nil
	})
	hold := orchestration.NewAction("hold", func(ctx context.Context, _ string) error {
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	run(t, b, []orchestration.Workflow{wf}, []orchestration.Step{hold})
	r, err := b.Start(t.Context(), orchestration.StartOptions{ID: "continuing", Workflow: wf.Name, Input: 0, Attributes: orchestration.Attributes{Org: "o"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Send(t.Context(), "continuing", orchestration.Message{Kind: "a"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	eventually(t, 30*time.Second, "two generations more", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(runs) == 3
	})
	list, err := b.ListRunning(t.Context(), wf.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(list, orchestration.RunningWorkflow{ID: "continuing", Org: "o"}) {
		t.Errorf("listed %+v", list)
	}
	if err := b.Send(t.Context(), "continuing", orchestration.Message{Kind: "b"}); err != nil {
		t.Fatal(err)
	}
	var out string
	if err := r.Get(t.Context(), &out); err != nil || out != "ab" {
		t.Fatalf("got %q, %v", out, err)
	}
	mu.Lock()
	defer mu.Unlock()
	seen := map[string]bool{}
	for _, i := range runs {
		if i.WorkflowID != "continuing" {
			t.Errorf("a generation's ID is %q", i.WorkflowID)
		}
		if seen[i.RunID] {
			t.Errorf("run ID %q again", i.RunID)
		}
		seen[i.RunID] = true
	}
}

// An ID that has ended can start again, and what's sent goes to the new one.
func TestAnEndedIDStartsAgain(t *testing.T) {
	b, _ := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	run(t, b, workflows, steps)
	o := orchestration.StartOptions{ID: "again", Workflow: waitLong, Input: ""}
	for i, kind := range []string{"first", "second", "third"} {
		r, err := b.Start(t.Context(), o)
		if err != nil {
			t.Fatal(err)
		}
		if err := b.Send(t.Context(), o.ID, orchestration.Message{Kind: kind}); err != nil {
			t.Fatal(err)
		}
		var out string
		if err := r.Get(t.Context(), &out); err != nil || out != kind {
			t.Fatalf("start %d: got %q, %v", i, out, err)
		}
	}
	if got := c.get(&c.before); got != 3 {
		t.Errorf("%d runs", got)
	}
	// Cancelling tells each generation, once each.
	r, err := b.Start(t.Context(), o)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Cancel(t.Context(), o.ID); err != nil {
		t.Fatal(err)
	}
	var out string
	if err := r.Get(t.Context(), &out); err != nil || out != orchestration.Stop {
		t.Fatalf("after cancelling: got %q, %v", out, err)
	}
}

// Another replica recovers the workflows of one whose reports have lapsed,
// from where they were.
func TestAReplicaRecoversAnother(t *testing.T) {
	client, o := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	lost := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "lost", Lapse: 2 * time.Second, Version: "v1"})
	stop := run(t, lost, workflows, steps)
	r, err := client.Start(t.Context(), orchestration.StartOptions{ID: "recovered", Workflow: waitLong, Input: ""})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the first step", func() bool { return c.get(&c.before) == 1 })
	second := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "survivor", Lapse: 2 * time.Second, Version: "v1"})
	run(t, second, workflows, steps)
	stop()
	if err := client.Send(t.Context(), "recovered", orchestration.Message{Kind: "go"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	var out string
	if err := r.Get(ctx, &out); err != nil || out != "go" {
		t.Fatalf("got %q, %v, after %v", out, err, time.Since(start))
	}
	if got := c.get(&c.before); got != 1 {
		t.Errorf("the first step ran %d times; it should carry on from where it was", got)
	}
}

// A version can't run another's workflows, so they're cancelled and started
// again, from their input. An operation isn't, and whoever waits hears.
func TestAnotherVersionStartsThemAgain(t *testing.T) {
	for _, tc := range []struct {
		name string
		// The new orchestrator keeps the old one's ID, as a StatefulSet's pod
		// or a laptop does.
		sameExecutor bool
	}{{"same executor", true}, {"another executor", false}} {
		t.Run(tc.name, func(t *testing.T) {
			client, o := open(t)
			c := &counted{}
			workflows, steps := c.register(contract.StreamSupervisorWorkflow)
			ops, _ := c.register(contract.ConnectorOpWorkflow)
			workflows = append(workflows, ops...)
			old := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "old", Lapse: 2 * time.Second, Version: "v1"})
			stop := run(t, old, workflows, steps)
			if _, err := client.Start(t.Context(), orchestration.StartOptions{ID: "supervisor", Workflow: contract.StreamSupervisorWorkflow, Input: "its input", Attributes: orchestration.Attributes{Org: "o"}}); err != nil {
				t.Fatal(err)
			}
			eventually(t, 30*time.Second, "the supervisor's first step", func() bool { return c.get(&c.before) == 1 })
			opRun, err := client.Start(t.Context(), orchestration.StartOptions{ID: "op", Workflow: contract.ConnectorOpWorkflow, Input: "the op's"})
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, 30*time.Second, "the operation's first step", func() bool { return c.get(&c.before) == 2 })
			stop()

			executor := "new"
			if tc.sameExecutor {
				executor = "old"
			}
			upgraded := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: executor, Lapse: 2 * time.Second, Version: "v2"})
			run(t, upgraded, workflows, steps)
			eventually(t, 30*time.Second, "the supervisor to start again", func() bool { return c.get(&c.before) == 3 })
			c.mu.Lock()
			input := c.input
			c.mu.Unlock()
			if input != "its input" {
				t.Errorf("started again with %q", input)
			}
			list, err := client.ListRunning(t.Context(), contract.StreamSupervisorWorkflow)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Contains(list, orchestration.RunningWorkflow{ID: "supervisor", Org: "o"}) {
				t.Errorf("listed %+v", list)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if err := opRun.Get(ctx, nil); err == nil || ctx.Err() != nil {
				t.Errorf("the operation: %v, %v", err, ctx.Err())
			}
			if err := client.Send(t.Context(), "supervisor", orchestration.Message{Kind: "go"}); err != nil {
				t.Fatal(err)
			}
			eventually(t, 30*time.Second, "the new one to hear", func() bool { return c.get(&c.after) == 1 })
		})
	}
}

// A rollback is started last, and takes what's enqueued, though it's an old
// version.
func TestARollbackTakesWhatsEnqueued(t *testing.T) {
	client, o := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	for _, version := range []string{"v1", "v2", "v1"} {
		b := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "it", Version: version})
		stop := run(t, b, workflows, steps)
		before := c.get(&c.before)
		id := "at-" + version + "-" + strconv.Itoa(before)
		if _, err := client.Start(t.Context(), orchestration.StartOptions{ID: id, Workflow: waitLong, Input: ""}); err != nil {
			t.Fatal(err)
		}
		eventually(t, 30*time.Second, id+" to run", func() bool { return c.get(&c.before) == before+1 })
		if err := client.Send(t.Context(), id, orchestration.Message{Kind: "go"}); err != nil {
			t.Fatal(err)
		}
		eventually(t, 30*time.Second, id+" to end", func() bool { return c.get(&c.after) == before+1 })
		stop()
	}
}

// What's enqueued goes to the latest version. When no one's left running it,
// as when a canary is taken away, those that are take over.
func TestTheLatestVersionGoes(t *testing.T) {
	client, o := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	stable := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "stable", Lapse: 2 * time.Second, Version: "stable"})
	run(t, stable, workflows, steps)
	canary := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "canary", Lapse: 2 * time.Second, Version: "canary"})
	stopCanary := run(t, canary, workflows, steps)
	// Started after the canary took over as the latest, and it's gone.
	eventually(t, 30*time.Second, "the canary to take a workflow", func() bool {
		if _, err := client.Start(t.Context(), orchestration.StartOptions{ID: "for-the-canary", Workflow: waitLong, Input: ""}); err != nil {
			t.Fatal(err)
		}
		return c.get(&c.before) == 1
	})
	stopCanary()
	if _, err := client.Start(t.Context(), orchestration.StartOptions{ID: "after", Workflow: waitLong, Input: ""}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the stable version to take new workflows", func() bool { return c.get(&c.before) >= 2 })
}

func TestSchedulesFireOneAtATime(t *testing.T) {
	b, o := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	// Two replicas, so what fires is what's due and not what each saw.
	run(t, b, workflows, steps)
	second := another(t, dbosbackend.Options{DatabaseURL: o.DatabaseURL, ExecutorID: "second"})
	run(t, second, workflows, steps)
	s := orchestration.Schedule{ID: "every-second", Every: time.Second, WorkflowID: "scheduled", Workflow: waitLong, Input: "", StartNow: true}
	if err := b.UpsertSchedule(t.Context(), s); err != nil {
		t.Fatal(err)
	}
	eventually(t, 10*time.Second, "its first workflow, started now", func() bool { return c.get(&c.before) == 1 })
	// It's still running, so nothing else starts however long it takes.
	time.Sleep(3 * time.Second)
	if got := c.get(&c.before); got != 1 {
		t.Fatalf("%d workflows at once", got)
	}
	running := func() []orchestration.RunningWorkflow {
		list, err := b.ListRunning(t.Context(), waitLong)
		if err != nil {
			t.Fatal(err)
		}
		return list
	}
	for got := 1; got <= 3; got++ {
		list := running()
		if len(list) != 1 || !strings.HasPrefix(list[0].ID, "scheduled-") {
			t.Fatalf("running %+v", list)
		}
		if err := b.Send(t.Context(), list[0].ID, orchestration.Message{Kind: "go"}); err != nil {
			t.Fatal(err)
		}
		eventually(t, 10*time.Second, "the next to start", func() bool { return c.get(&c.before) == got+1 })
	}
	if err := b.DeleteSchedule(t.Context(), s.ID); err != nil {
		t.Fatal(err)
	}
}

// Rows of finished workflows go, and what's running stays.
func TestFinishedWorkflowsAreCollected(t *testing.T) {
	b, o := open(t)
	c := &counted{}
	workflows, steps := c.register(waitLong)
	run(t, b, workflows, steps)
	start := func(id string) {
		if _, err := b.Start(t.Context(), orchestration.StartOptions{ID: id, Workflow: waitLong, Input: ""}); err != nil {
			t.Fatal(err)
		}
	}
	start("ended")
	start("running")
	eventually(t, 30*time.Second, "both to start", func() bool { return c.get(&c.before) == 2 })
	if err := b.Send(t.Context(), "ended", orchestration.Message{Kind: "go"}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "one to end", func() bool { return c.get(&c.after) == 1 })
	conn, err := pgx.Connect(t.Context(), o.DatabaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(t.Context(), `UPDATE dbos.workflow_status SET created_at = 0`); err != nil {
		t.Fatal(err)
	}
	if err := b.Collect(t.Context()); err != nil {
		t.Fatal(err)
	}
	var rows []string
	r, err := conn.Query(t.Context(), `SELECT workflow_uuid FROM dbos.workflow_status`)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err = pgx.CollectRows(r, pgx.RowTo[string]); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rows, []string{"running"}) {
		t.Errorf("left %v", rows)
	}
}
