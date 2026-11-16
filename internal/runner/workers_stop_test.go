package runner_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// A join timeout long enough that an attempt waiting it out fails the
// tests' bound.
var stopOpts = attempt.Options{Heartbeat: 50 * time.Millisecond, Timeout: 2 * time.Second, JoinTimeout: time.Minute}

// stopSoon is how soon an attempt stopped before its loop must end.
const stopSoon = 5 * time.Second

// An attempt on workers that's stopped before its workers join, which
// leave an attempt that's stopped, ends drained at once rather than
// waiting out the join timeout; and the next attempt commits every row.
func TestAStopBeforeTheWorkersJoinDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(2, 10, 5))
	j := p.job("a1", noSource{t}, p.destination(true))
	launched, drain := make(chan struct{}), make(chan struct{})
	j.Workers = &runner.Workers{Count: 2, Server: p.srv, Options: stopOpts, Launch: func(context.Context, []attempt.Target) error {
		close(launched)
		return nil
	}}
	j.Drain = drain
	go func() {
		<-launched
		close(drain)
	}()
	start := time.Now()
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("got %v, want ErrDrained", err)
	}
	if d := time.Since(start); d > stopSoon {
		t.Fatalf("the stopped attempt ended after %v", d)
	}

	next := p.workers("a2", 2)
	next.Workers.Options = stopOpts
	if _, err := runner.RunAttempt(ctx, next); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a2") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 20)
}

// An attempt on workers that's stopped while its first worker recovers the
// destination, after the workers joined, ends drained at once: the
// worker's call stops rather than holding the attempt's end. Its workers
// end, and the next attempt recovers and commits every row.
func TestAStopDuringRecoveryOnWorkersDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(2, 10, 5))
	recovering := make(chan struct{}, 4)
	var block atomic.Bool
	block.Store(true)
	p.dest = func(coordinator bool) destination.Destination {
		d := stagingDest{s: p.store, t: t, coordinator: coordinator}
		if coordinator || !block.Load() {
			return d
		}
		return openBlocks{stagingDest: d, entered: recovering}
	}
	j := p.workers("a1", 2)
	j.Workers.Options = stopOpts
	drain := make(chan struct{})
	j.Drain = drain
	go func() {
		<-recovering
		close(drain)
	}()
	start := time.Now()
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("got %v, want ErrDrained", err)
	}
	if d := time.Since(start); d > stopSoon {
		t.Fatalf("the stopped attempt ended after %v", d)
	}
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		p.wait("a1")
	}()
	select {
	case <-ended:
	case <-time.After(stopSoon):
		t.Fatal("the stopped attempt's workers didn't end")
	}

	block.Store(false)
	next := p.workers("a2", 2)
	next.Workers.Options = stopOpts
	if _, err := runner.RunAttempt(ctx, next); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a2") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	p.store.check(t, 20)
}

// An attempt on workers whose first worker joined and recovered, and whose
// second never joins, is stopped while it waits to assign their work: it
// ends drained at once, and its first worker ends.
func TestAStopWhileAWorkerHasntJoinedDrains(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, ranges(2, 10, 5))
	opened := make(chan struct{}, 4)
	p.dest = func(coordinator bool) destination.Destination {
		d := stagingDest{s: p.store, t: t, coordinator: coordinator}
		if coordinator {
			return d
		}
		return openSays{stagingDest: d, opened: opened}
	}
	j := p.workers("a1", 2)
	j.Workers.Options = stopOpts
	launch := j.Workers.Launch
	j.Workers.Launch = func(ctx context.Context, targets []attempt.Target) error { return launch(ctx, targets[:1]) }
	drain := make(chan struct{})
	j.Drain = drain
	go func() {
		<-opened
		time.Sleep(100 * time.Millisecond)
		close(drain)
	}()
	start := time.Now()
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("got %v, want ErrDrained", err)
	}
	if d := time.Since(start); d > stopSoon {
		t.Fatalf("the stopped attempt ended after %v", d)
	}
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		p.wait("a1")
	}()
	select {
	case <-ended:
	case <-time.After(stopSoon):
		t.Fatal("the stopped attempt's worker didn't end")
	}
}

// openSays is a worker's destination that says when it has opened.
type openSays struct {
	stagingDest
	opened chan<- struct{}
}

func (d openSays) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w, err := d.stagingDest.Open(ctx, cfg)
	select {
	case d.opened <- struct{}{}:
	default:
	}
	return w, err
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
