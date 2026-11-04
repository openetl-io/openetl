package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/internal/runner/flinktest"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var idSchema = arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)

// countingSource emits ids 1..rows on each stream, a page at a time with a
// resume point after each, from where its state says, then idles.
type countingSource struct {
	streams []string
	rows    int
	page    int
	pause   time.Duration
}

func (s countingSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	next := map[string]int{}
	for _, cs := range req.Streams {
		next[cs.Name] = 1
		if len(cs.State) > 0 {
			n, err := strconv.Atoi(string(cs.State))
			if err != nil {
				return err
			}
			next[cs.Name] = n + 1
		}
	}
	for {
		done := true
		for _, name := range s.streams {
			if next[name] > s.rows {
				continue
			}
			done = false
			var rows []json.RawMessage
			for i := 0; i < s.page && next[name] <= s.rows; i++ {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id":%d}`, next[name])))
				next[name]++
			}
			if err := out.Rows(name, rows); err != nil {
				return err
			}
			if err := out.ResumePoint(name, []byte(strconv.Itoa(next[name]-1))); err != nil {
				return err
			}
		}
		if done {
			<-ctx.Done()
			return ctx.Err()
		}
		select {
		case <-time.After(s.pause):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func idRows(_ string, rows []json.RawMessage) (arrow.RecordBatch, error) {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for _, r := range rows {
		var v struct{ ID int64 }
		if err := json.Unmarshal(r, &v); err != nil {
			return nil, err
		}
		b.Append(v.ID)
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(idSchema, []arrow.Array{col}, int64(col.Len())), nil
}

// exactDest stages each epoch's rows and applies them once, at Commit or
// at a Recover that finds them staged and not applied.
type exactDest struct {
	mu      sync.Mutex
	fence   uint64
	staged  map[destination.Epoch]map[string][]int64
	writing map[destination.Epoch]map[string][]int64
	applied map[destination.Epoch]bool
	rows    map[string][]int64
	lost    bool // Recover of an unapplied epoch reports it lost, as Kafka's does
}

func newExactDest() *exactDest {
	return &exactDest{staged: map[destination.Epoch]map[string][]int64{}, writing: map[destination.Epoch]map[string][]int64{},
		applied: map[destination.Epoch]bool{}, rows: map[string][]int64{}}
}

func (d *exactDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *exactDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *exactDest) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.writing[e] == nil {
		d.writing[e] = map[string][]int64{}
	}
	col := b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64)
	d.writing[e][stream] = append(d.writing[e][stream], col.Int64Values()...)
	return nil
}

func (d *exactDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e.Generation < d.fence {
		return nil, destination.ErrFenced
	}
	d.staged[e] = d.writing[e]
	delete(d.writing, e)
	return []byte("staged"), nil
}

func (d *exactDest) Commit(_ context.Context, e destination.Epoch, _ destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if e.Generation < d.fence {
		return destination.Committed{}, destination.ErrFenced
	}
	d.apply(e)
	return destination.Committed{}, nil
}

func (d *exactDest) apply(e destination.Epoch) {
	if d.applied[e] {
		return
	}
	d.applied[e] = true
	for stream, ids := range d.staged[e] {
		d.rows[stream] = append(d.rows[stream], ids...)
	}
	delete(d.staged, e)
}

func (d *exactDest) Recover(_ context.Context, gen uint64, e destination.Epoch, _ destination.Staged) (destination.Recovery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.fence = gen
	switch {
	case e.IsZero() || d.applied[e]:
		return destination.Applied, nil
	case d.staged[e] != nil && !d.lost:
		d.apply(e)
		return destination.Redone, nil
	}
	return destination.Lost, nil
}

func (d *exactDest) Close(context.Context) error { return nil }

// check fails unless every stream holds ids 1..rows once each.
func (d *exactDest) check(t *testing.T, streams []string, rows int) {
	t.Helper()
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range streams {
		got := slices.Clone(d.rows[s])
		slices.Sort(got)
		if len(got) != rows || slices.Compact(slices.Clone(got)) == nil || len(slices.Compact(got)) != rows || got[0] != 1 || got[rows-1] != int64(rows) {
			t.Errorf("stream %s: %d rows, %v", s, len(d.rows[s]), d.rows[s])
		}
	}
}

func (d *exactDest) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	for _, ids := range d.rows {
		n += len(ids)
	}
	return n
}

type flinkCase struct {
	streams  []string
	rows     int
	faults   flinktest.Faults
	lost     bool
	attempts int // runner attempts, each cut off partway, before the one that drains
}

func runFlinkCase(t *testing.T, c flinkCase) (*exactDest, *checkpoint.Log, *flinktest.Cluster) {
	t.Helper()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	bridge := flinktest.NewBridge()
	cluster := flinktest.NewCluster(bridge)
	cluster.Faults = c.faults
	cluster.Logf = t.Logf
	dest := newExactDest()
	dest.lost = c.lost
	var streams, outputs []runner.Stream
	for _, s := range c.streams {
		streams = append(streams, runner.Stream{Name: s, Replay: source.Exact, Read: source.Incremental, Mode: destination.Append, Schema: idSchema})
		outputs = append(outputs, runner.Stream{Name: s, Replay: source.Exact, Mode: destination.Append, Transformed: true})
	}
	job := func(owner string, drain <-chan struct{}) runner.Job {
		return runner.Job{
			Owner: owner, Pipeline: "p", Continuous: true, Log: log,
			Source:      countingSource{streams: c.streams, rows: c.rows, page: 7, pause: 3 * time.Millisecond},
			Streams:     streams,
			Outputs:     outputs,
			Destination: dest,
			Dataflow:    runner.Passthrough{Convert: idRows},
			Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline, CommitInterval: 10 * time.Millisecond},
			Flink:       cluster,
			Bridge:      bridge,
			Drain:       drain,
		}
	}
	total := c.rows * len(c.streams)
	for i := range c.attempts {
		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(time.Duration(40+30*i) * time.Millisecond)
			cancel()
		}()
		_, err := runner.RunAttempt(ctx, job(fmt.Sprintf("attempt-%d", i), nil))
		if !errors.Is(err, context.Canceled) && err != nil {
			t.Logf("attempt %d: %v", i, err)
		}
		cancel()
	}
	drain := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	go func() {
		for dest.count() < total {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				return
			}
		}
		close(drain)
	}()
	res, err := runner.RunAttempt(ctx, job("last", drain))
	if !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("attempt: %v (%+v)", err, res)
	}
	dest.check(t, c.streams, c.rows)
	return dest, log, cluster
}

func TestFlinkPassesEveryRowOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, log, _ := runFlinkCase(t, flinkCase{streams: []string{"a", "b"}, rows: 120})
		rec, err := log.Latest(context.Background())
		if err != nil || rec == nil {
			t.Fatalf("HEAD: %+v, %v", rec, err)
		}
		if rec.Flink == nil || rec.Flink.Savepoint == "" || rec.Flink.Job == "" {
			t.Errorf("the drain's record: %+v", rec.Flink)
		}
	})
}

func TestFlinkTaskFailuresRestoreInPlace(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, _, cluster := runFlinkCase(t, flinkCase{streams: []string{"a", "b"}, rows: 150, faults: flinktest.Faults{
			FailTasks: func(c int64) bool { return c == 3 || c == 7 },
		}})
		if n := cluster.Deploys(); n != 1 {
			t.Errorf("deployed %d times; a task failure restores in place", n)
		}
	})
}

func TestFlinkDeclinedCheckpointLosesItsEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runFlinkCase(t, flinkCase{streams: []string{"a"}, rows: 150, faults: flinktest.Faults{
			DeclineAfterRecord: func(c int64) bool { return c == 4 || c == 9 },
		}})
	})
}

func TestFlinkSavepointByHandEndsNoEpoch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runFlinkCase(t, flinkCase{streams: []string{"a", "b"}, rows: 100, faults: flinktest.Faults{
			SavepointByHand: func(c int64) bool { return c%3 == 0 },
		}})
	})
}

func TestFlinkAttemptsResumeWhereTheLastStopped(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runFlinkCase(t, flinkCase{streams: []string{"a", "b"}, rows: 200, attempts: 3})
	})
}

func TestFlinkAttemptsResumeWhenTheEpochIsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runFlinkCase(t, flinkCase{streams: []string{"a"}, rows: 200, attempts: 3, lost: true, faults: flinktest.Faults{
			FailTasks: func(c int64) bool { return c == 5 },
		}})
	})
}
