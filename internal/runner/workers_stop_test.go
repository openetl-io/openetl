package runner_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A join timeout long enough that an attempt waiting it out fails the
// tests' bound.
var stopOpts = attempt.Options{Heartbeat: 50 * time.Millisecond, Timeout: 2 * time.Second, JoinTimeout: time.Minute}

// stopSoon is how soon an attempt stopped before its loop must end.
const stopSoon = 5 * time.Second

// An attempt on workers that's stopped as it waits for its first worker to
// join, which a stopped attempt's workers never do, ends drained at once
// rather than waiting out the join timeout; the next attempt commits every
// row.
func TestAStopBeforeTheWorkersJoinDrains(t *testing.T) {
	ctx, p := stopPool(t)
	j := p.job("a1", noSource{t}, p.destination(true))
	j.Workers = &runner.Workers{Count: 2, Server: p.srv, Options: stopOpts, Launch: func(context.Context, []attempt.Target) error { return nil }}
	stopAt(&j, "recover")
	drainsAtOnce(ctx, t, p, j)
	commitsEveryRow(ctx, t, p, "a2")
}

// An attempt on workers that's stopped while its first worker recovers the
// destination, after the workers joined, ends drained at once: the
// worker's call stops rather than holding the attempt's end. Its workers
// end, and the next attempt recovers and commits every row.
func TestAStopDuringRecoveryOnWorkersDrains(t *testing.T) {
	ctx, p := stopPool(t)
	recovering := make(chan struct{}, 4)
	var block atomic.Bool
	block.Store(true)
	workerDest(t, p, func(d stagingDest) destination.Destination {
		if !block.Load() {
			return d
		}
		return openBlocks{stagingDest: d, entered: recovering}
	})
	j := onStopWorkers(p, "a1", 2)
	stopWhen(&j, recovering)
	drainsAtOnce(ctx, t, p, j)
	block.Store(false)
	commitsEveryRow(ctx, t, p, "a2")
}

// An attempt on workers whose first worker joined and recovered, and whose
// second never joins, is stopped as it waits to assign their work: it ends
// drained at once, and its first worker ends.
func TestAStopWhileAWorkerHasntJoinedDrains(t *testing.T) {
	ctx, p := stopPool(t)
	j := onStopWorkers(p, "a1", 2)
	launch := j.Workers.Launch
	j.Workers.Launch = func(ctx context.Context, targets []attempt.Target) error { return launch(ctx, targets[:1]) }
	stopAt(&j, "assign")
	drainsAtOnce(ctx, t, p, j)
}

// A group window's attempt on workers, whose engines would exchange rows to
// keyed lanes that keep its state, stopped as it waits for its first
// worker to join, or for its second once the first has joined and
// recovered, ends drained at once, and the worker that joined ends.
func TestAStopWhileAStatefulJobsWorkersJoinDrains(t *testing.T) {
	for _, step := range []string{"recover", "assign"} {
		t.Run(step, func(t *testing.T) {
			ctx, p := stopPool(t)
			j := onStopWorkers(p, "a1", 2)
			j.SQL = &runner.SQLTransform{SQL: "…", Operators: []string{"window-1"},
				Expected: []runner.ExpectedOutput{{Name: "w", Input: "s", Operator: "window-1", Need: []string{"id"}}}}
			j.Outputs = []runner.Stream{{Name: "w", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"window_start", "id"}, From: "s", Transformed: true}}
			j.Dataflow, j.StateURL = meshingPassthrough{}, "file://"+t.TempDir()
			launch := j.Workers.Launch
			j.Workers.Launch = func(ctx context.Context, targets []attempt.Target) error {
				if step == "recover" {
					return nil
				}
				return launch(ctx, targets[:1])
			}
			stopAt(&j, step)
			drainsAtOnce(ctx, t, p, j)
		})
	}
}

// A stop while the workers redo a lost lane's part of an epoch, from the
// state of the record before it, ends the attempt drained at once, with
// the lane uncommitted; the next attempt redoes it, and every row lands
// once.
func TestAStopDuringAStatefulRedoLeavesTheLaneToRedo(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	var mu sync.Mutex
	var plans []runner.Plan
	s := newTxnStore(t)
	p := txnPool(t, s, ranges(6, 100, 5))
	stateful := func(j *runner.Job) { j.Dataflow = statePlans{inner: j.Dataflow, mu: &mu, plans: &plans} }
	p.change = stateful
	head := p.lose(t, s, 2, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
	lost := laneMark{head.Generation, head.Epoch, 0}

	redoing := make(chan struct{}, 4)
	p.change = func(j *runner.Job) {
		stateful(j)
		j.Dataflow = redoBlocks{Dataflow: j.Dataflow, entered: redoing}
	}
	j := p.workers("a2", 2)
	j.Workers.Options = stopOpts
	stopWhen(&j, redoing)
	drainsAtOnce(ctx, t, p, j)
	s.mu.Lock()
	marked := s.marked[lost]
	s.mu.Unlock()
	if marked {
		t.Fatalf("the stopped redo committed lane %d of epoch %d", lost.Lane, lost.N)
	}

	p.change = stateful
	if res := p.settle(t, s, "a3", 2, head); res.Recovery != destination.Redone {
		t.Errorf("the attempt after the stop recovered %s", res.Recovery)
	}
	s.check(t, 600)
}

// redoBlocks is a dataflow whose redos say so and wait until they're
// stopped.
type redoBlocks struct {
	runner.Dataflow
	entered chan<- struct{}
}

func (f redoBlocks) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	if !plan.StateReadOnly {
		return f.Dataflow.Run(ctx, plan, in, out)
	}
	defer close(out)
	select {
	case f.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

// stopPool is a pool of two splits of ten rows, and the test's bound.
func stopPool(t *testing.T) (context.Context, *pool) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	return ctx, newPool(t, ranges(2, 10, 5))
}

// onStopWorkers is the pool's job for attempt owner on n workers, which
// join within stopOpts' timeout.
func onStopWorkers(p *pool, owner string, n int) runner.Job {
	j := p.workers(owner, n)
	j.Workers.Options = stopOpts
	return j
}

// workerDest makes each worker's destination wrap of the pool's staging
// one, and leaves the coordinator's as it is.
func workerDest(t *testing.T, p *pool, wrap func(stagingDest) destination.Destination) {
	p.dest = func(coordinator bool) destination.Destination {
		d := stagingDest{s: p.store, t: t, coordinator: coordinator}
		if coordinator {
			return d
		}
		return wrap(d)
	}
}

// stopAt stops j's attempt as its coordinator starts to wait on its
// workers at step.
func stopAt(j *runner.Job, step string) {
	drain := make(chan struct{})
	var once sync.Once
	j.Drain = drain
	runner.OnWait(j.Workers, func(s string) {
		if s == step {
			once.Do(func() { close(drain) })
		}
	})
}

// stopWhen stops j's attempt once signal says.
func stopWhen(j *runner.Job, signal <-chan struct{}) {
	drain := make(chan struct{})
	j.Drain = drain
	go func() {
		<-signal
		close(drain)
	}()
}

// drainsAtOnce runs j's attempt, which must end drained within stopSoon,
// as must the workers it launched.
func drainsAtOnce(ctx context.Context, t *testing.T, p *pool, j runner.Job) {
	t.Helper()
	start := time.Now()
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("got %v, want ErrDrained", err)
	}
	if d := time.Since(start); d > stopSoon {
		t.Fatalf("the stopped attempt ended after %v", d)
	}
	p.mu.Lock()
	_, launched := p.done[j.Owner]
	p.mu.Unlock()
	if !launched {
		return
	}
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		p.wait(j.Owner)
	}()
	select {
	case <-ended:
	case <-time.After(stopSoon):
		t.Fatal("the stopped attempt's workers didn't end")
	}
}

// commitsEveryRow runs attempt owner on two workers, which commits all 20
// rows.
func commitsEveryRow(ctx context.Context, t *testing.T, p *pool, owner string) {
	t.Helper()
	if _, err := runner.RunAttempt(ctx, onStopWorkers(p, owner, 2)); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait(owner) {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 20)
}

// openBlocks is a worker's destination whose Open, which recovery does
// first, says so and waits until its call ends.
type openBlocks struct {
	stagingDest
	entered chan<- struct{}
}

func (d openBlocks) Open(ctx context.Context, _ destination.OpenConfig) (destination.Writer, error) {
	select {
	case d.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
