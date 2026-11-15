package runner_test

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/runner"
)

// statePlans is a dataflow that reports a snapshot of operator w at each
// epoch, as a stateful operator's lanes do, and keeps the plans it runs.
type statePlans struct {
	inner runner.Dataflow
	mu    *sync.Mutex
	plans *[]runner.Plan
}

func (f statePlans) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	f.mu.Lock()
	*f.plans = append(*f.plans, plan)
	f.mu.Unlock()
	mid := make(chan runner.Frame)
	errc := make(chan error, 1)
	go func() { errc <- f.inner.Run(ctx, plan, in, mid) }()
	for fr := range mid {
		if fr.Kind == runner.FrameReady && !plan.StateReadOnly {
			fr.Ready.Snapshots = []checkpoint.Snapshot{{Operator: "w", KeyGroups: checkpoint.KeyGroupRange{Last: 255}, Layout: 1,
				Data: fmt.Appendf(nil, "e%d", fr.Epoch), Runs: []string{fmt.Sprintf("w/g%d/e%d/kg0-255.run", plan.Generation, fr.Epoch)}}}
		}
		select {
		case out <- fr:
		case <-ctx.Done():
		}
	}
	return <-errc
}

// A redo of a writer lane's part of an epoch starts from the state of the
// record before the epoch, which it reads and doesn't write, on workers or
// on one runner.
func TestARedoStartsFromTheStateBeforeItsEpoch(t *testing.T) {
	for _, n := range []int{0, 2} {
		t.Run(fmt.Sprintf("workers=%d", n), func(t *testing.T) {
			var mu sync.Mutex
			var plans []runner.Plan
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			p.change = func(j *runner.Job) { j.Dataflow = statePlans{inner: j.Dataflow, mu: &mu, plans: &plans} }
			head := p.lose(t, s, 2, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			if head.Previous == nil {
				t.Fatalf("epoch %d is the log's first", head.Epoch)
			}
			p.settle(t, s, "a2", n, head)
			s.check(t, 600)

			mu.Lock()
			defer mu.Unlock()
			var redos []runner.Plan
			for _, pl := range plans {
				if pl.StateReadOnly {
					redos = append(redos, pl)
				}
			}
			if len(redos) == 0 {
				t.Fatal("no redo ran")
			}
			before := fmt.Sprintf("e%d", head.Previous.Epoch)
			for _, r := range redos {
				if r.FirstEpoch != head.Epoch || !slices.ContainsFunc(r.Snapshots, func(sn checkpoint.Snapshot) bool { return string(sn.Data) == before }) ||
					slices.ContainsFunc(r.Snapshots, func(sn checkpoint.Snapshot) bool { return string(sn.Data) != before }) {
					t.Errorf("a redo of epoch %d ran epoch %d from state %+v, want epoch %d's", head.Epoch, r.FirstEpoch, r.Snapshots, head.Previous.Epoch)
				}
			}
		})
	}
}
