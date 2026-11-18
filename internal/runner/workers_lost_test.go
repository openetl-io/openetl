package runner_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
)

// meshFlow is Passthrough on engines that reach each other, as the
// workers' engines do across an exchange: an engine that stops before its
// input ends, as when its worker's part ends, closes its connections, and
// every other engine still running fails with PeerLost. One whose input
// ends ends its connections as it does, which loses nobody. With lose set,
// the engine that takes it fails with PeerLost after its first frame.
type meshFlow struct {
	runner.Passthrough
	once sync.Once
	// Closed once an engine stopped.
	gone chan struct{}
	lose chan struct{}
	// Engines that lost another that stopped.
	losses atomic.Int32
}

func newMeshFlow() *meshFlow {
	return &meshFlow{Passthrough: runner.Passthrough{Convert: idRows}, gone: make(chan struct{}), lose: make(chan struct{}, 1)}
}

func (m *meshFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	rctx, cancel := context.WithCancel(ctx)
	defer cancel()
	first := make(chan struct{})
	seen := make(chan runner.Frame)
	go func() {
		defer close(seen)
		once := sync.Once{}
		for f := range in {
			once.Do(func() { close(first) })
			select {
			case seen <- f:
			case <-rctx.Done():
				return
			}
		}
	}()
	done := make(chan error, 1)
	go func() { done <- m.Passthrough.Run(rctx, plan, seen, out) }()
	lost := func() error {
		cancel()
		if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return fmt.Errorf("engine: %w", &attempt.PeerLost{Err: errs.New(errs.Transient, "engine 2 at w1:7720 was lost")})
	}
	for {
		select {
		case err := <-done:
			if ctx.Err() != nil {
				m.once.Do(func() { close(m.gone) })
			}
			return err
		case <-m.gone:
			m.losses.Add(1)
			return lost()
		case <-first:
			first = nil
			select {
			case <-m.lose:
				return lost()
			default:
			}
		}
	}
}

// Once the coordinator finishes an attempt, its workers' parts end, and
// with them their engines, which takes their connections down: the engine
// of a worker whose part hasn't ended yet then loses the others. That's no
// loss, and every worker's part of the drained attempt ends as finished,
// whichever learns of the finish first.
func TestAPeerStoppingAfterTheFinishLosesNoWorker(t *testing.T) {
	losses := 0
	for range 20 {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		p := newPool(t, func() *rangeSource {
			s := ranges(4, 5, 5)()
			s.open = true
			return s
		})
		mesh := newMeshFlow()
		p.flow = mesh
		j := p.workers("a1", 2)
		drains(&j)
		if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("the attempt: %v", err)
		}
		for i, err := range p.wait("a1") {
			if err != nil {
				t.Fatalf("worker %d: %v", i, err)
			}
		}
		losses += int(mesh.losses.Load())
		cancel()
	}
	// Each drained attempt's first engine to stop loses the other, unless the
	// other stopped as soon.
	if losses == 0 {
		t.Error("no engine lost another that stopped first")
	}
	t.Logf("%d engines of 20 attempts lost another that stopped first", losses)
}

// An engine that loses another before the coordinator finishes the attempt
// fails its worker with that, and the attempt after the grace, as
// transient.
func TestAPeerLostBeforeTheFinishFailsTheWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	p := newPool(t, func() *rangeSource {
		s := ranges(4, 5, 5)()
		s.open = true
		return s
	})
	mesh := newMeshFlow()
	mesh.lose <- struct{}{}
	p.flow = mesh
	j := p.workers("a1", 2)
	j.Workers.Options.PeerLostGrace = 100 * time.Millisecond
	drains(&j)
	_, err := runner.RunAttempt(ctx, j)
	if errs.CodeOf(err) != errs.Transient || !strings.Contains(fmt.Sprint(err), "engine 2 at w1:7720 was lost") {
		t.Fatalf("the attempt ended with %v (%s)", err, errs.CodeOf(err))
	}
	lost := 0
	for i, err := range p.wait("a1") {
		switch {
		case errors.As(err, new(*attempt.PeerLost)):
			lost++
			if errs.CodeOf(err) != errs.Transient {
				t.Errorf("worker %d lost its peer with %v (%s)", i, err, errs.CodeOf(err))
			}
		case err == nil:
			t.Errorf("worker %d finished an attempt that failed", i)
		}
	}
	if lost != 1 {
		t.Errorf("%d workers failed with the lost peer", lost)
	}
}
