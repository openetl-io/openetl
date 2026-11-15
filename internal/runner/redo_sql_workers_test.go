package runner_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// fanFlow stands in for an engine that runs row-wise SQL: each row of an
// input goes to every output written from it, its id raised by the
// output's shift, on the lane it came in on.
type fanFlow struct {
	shift map[string]int64
}

func (f fanFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	inner := plan
	inner.Streams, inner.Outputs, inner.SQL = plan.Outputs, nil, nil
	fanned := make(chan runner.Frame, 16)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(fanned)
		for fr := range in {
			var batch arrow.RecordBatch
			switch fr.Kind {
			case runner.FrameSplit:
				continue
			case runner.FrameRows:
				b, err := idRows(fr.Stream, fr.Rows)
				if err != nil {
					return err
				}
				batch = b
			}
			for _, o := range plan.Outputs {
				if o.From != fr.Stream {
					continue
				}
				to := runner.Frame{Kind: fr.Kind, Lane: fr.Lane, Stream: o.Name, Epoch: fr.Epoch}
				if batch != nil {
					to.Kind, to.Batch = runner.FrameBatch, shifted(batch, f.shift[o.Name])
				}
				select {
				case fanned <- to:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
			if batch != nil {
				batch.Release()
			}
		}
		return nil
	})
	g.Go(func() error { return runner.Passthrough{}.Run(gctx, inner, fanned, out) })
	return g.Wait()
}

// shifted is an id batch with by added to each id.
func shifted(b arrow.RecordBatch, by int64) arrow.RecordBatch {
	ids := b.Column(0).(*array.Int64).Int64Values()
	ib := array.NewInt64Builder(memory.DefaultAllocator)
	defer ib.Release()
	for _, id := range ids {
		ib.Append(id + by)
	}
	col := ib.NewArray()
	defer col.Release()
	return array.NewRecordBatch(idSchema, []arrow.Array{col}, int64(col.Len()))
}

// mixedLoss is a commit hook of s that fails writer lane 0's commit, by a
// writer of generation 1, and runs kill first, in the first epoch after
// its first with rows in which its transaction holds rows of names, which
// its own lanes wrote, and by_id's rows of an id without its names row,
// which the exchange brought from another lane.
func mixedLoss(s *txnStore, kill func()) func(uint64, laneMark) error {
	var mu sync.Mutex
	var once sync.Once
	var at uint64
	held := 0
	return func(gen uint64, m laneMark) error {
		if gen != 1 || m.Gen != 1 || m.Lane != 0 {
			return nil
		}
		mu.Lock()
		if at == 0 {
			s.mu.Lock()
			if t := s.open[txnID{m.Lane, int(m.N % txnSlots)}]; t != nil && t.mark == m && len(t.rows) > 0 {
				held++
				names := slices.ContainsFunc(t.rows, func(id int64) bool { return id > 1000 })
				brought := slices.ContainsFunc(t.rows, func(id int64) bool { return id <= 1000 && !slices.Contains(t.rows, id+1000) })
				if held >= 2 && names && brought {
					at = m.N
				}
			}
			s.mu.Unlock()
		}
		fail := at != 0 && m.N == at
		mu.Unlock()
		if !fail {
			return nil
		}
		once.Do(kill)
		return fmt.Errorf("lane 0's commit of epoch %d failed", at)
	}
}

// sqlLanesPool is a pool whose stream feeds by_id, keyed by id, which an
// exchange sends to the writer lane of each id's key group, and names, with
// no key, which each lane writes on its own writer lane; names' ids are
// raised by 1000. Its attempt a1, on three workers, is lost between the
// record and its commits once writer lane 0 holds rows of both outputs,
// by_id's of a split another lane read among them, which leaves the epoch
// HEAD names committed in part.
func sqlLanesPool(t *testing.T) (*txnStore, *pool, *checkpoint.Record) {
	t.Helper()
	s := newTxnStore(t)
	p := txnPool(t, s, ranges(6, 100, 5))
	p.change = func(j *runner.Job) {
		j.Streams[0].PrimaryKey = []string{"id"}
		j.Outputs = []runner.Stream{
			{Name: "by_id", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, From: "s", Transformed: true},
			{Name: "names", Replay: source.Exact, Mode: destination.Append, From: "s", Transformed: true},
		}
		j.SQL = &runner.SQLTransform{SQL: "INSERT INTO by_id SELECT id FROM s; INSERT INTO names SELECT id + 1000 AS id FROM s"}
		j.Dataflow = fanFlow{shift: map[string]int64{"names": 1000}}
	}
	head := p.lose(t, s, 3, mixedLoss(s, func() { p.kill("a1", 0) }))
	outputs := map[string]map[string]bool{}
	for _, c := range head.Coverage[0].Splits {
		if c.Output == "by_id" && c.KeyGroups == nil {
			t.Errorf("writer lane 0 holds all of by_id's rows of split %s", c.ID)
		}
		if outputs[c.ID] == nil {
			outputs[c.ID] = map[string]bool{}
		}
		outputs[c.ID][c.Output] = true
	}
	both, exchanged := 0, 0
	for _, o := range outputs {
		switch {
		case o["by_id"] && o["names"]:
			both++
		case o["by_id"] && len(o) == 1:
			exchanged++
		}
	}
	if both == 0 || exchanged == 0 {
		t.Fatalf("writer lane 0's coverage names %v; want splits of both outputs, and of by_id alone", outputs)
	}
	return s, p, head
}

// A row-wise SQL transform into a destination whose writer lanes commit on
// their own runs on workers. After sqlLanesPool's loss, the next attempt,
// on workers or on one runner, writes each lost lane's part again, as it
// was: of the splits its own lanes read, by_id's rows of its key groups and
// every row of names, and of the splits the exchange brought there, by_id's
// alone.
func TestASQLRedoFromTheSource(t *testing.T) {
	for name, after := range map[string]int{"onto two workers": 2, "onto one runner": 0} {
		t.Run(name, func(t *testing.T) {
			s, p, head := sqlLanesPool(t)
			p.settle(t, s, "a2", after, head)
			if after > 0 {
				p.readAgain(t, "a2", head, []int{0, 1})
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			got := slices.Sorted(slices.Values(s.rows))
			var want []int64
			for id := range int64(600) {
				want = append(want, id+1)
			}
			for id := range int64(600) {
				want = append(want, id+1001)
			}
			if !slices.Equal(got, want) {
				t.Errorf("%d rows landed, want ids 1 to 600 of by_id and 1001 to 1600 of names once each: %v", len(got), fmt.Sprint(got))
			}
		})
	}
}

// Statements edited since the epoch, though with the same outputs, keys and
// streams, would write other rows than the epoch's other lanes committed: a
// redo under them fails the attempt with CONFIG_INVALID, and commits
// nothing of the lost lanes.
func TestASQLRedoUnderOtherStatementsIsRefused(t *testing.T) {
	for name, after := range map[string]int{"on two workers": 2, "on one runner": 0} {
		t.Run(name, func(t *testing.T) {
			s, p, head := sqlLanesPool(t)
			edit := p.change
			p.change = func(j *runner.Job) {
				edit(j)
				j.SQL.SQL = strings.Replace(j.SQL.SQL, "id + 1000", "id + 2000", 1)
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			err := p.attempt(ctx, s, "a2", after)
			if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(fmt.Sprint(err), "other statements of transform.sql") {
				t.Fatalf("the attempt ended with %v (%s)", err, errs.CodeOf(err))
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, l := range []int{0, 1} {
				if s.marked[laneMark{head.Generation, head.Epoch, l}] {
					t.Errorf("lost lane %d of epoch %d was committed", l, head.Epoch)
				}
			}
		})
	}
}
