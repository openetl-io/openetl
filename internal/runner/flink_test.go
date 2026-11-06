package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/flinktest"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
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

func (d *exactDest) Commit(_ context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
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

func (d *exactDest) Recover(_ context.Context, gen uint64, e destination.Epoch, _ []destination.Staged) (destination.Recovery, error) {
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
	streams []string
	// Streams read with CDC, and the seq type each declares.
	cdc      map[string]arrow.DataType
	flow     runner.Dataflow
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
		st := runner.Stream{Name: s, Replay: source.Exact, Read: source.Incremental, Mode: destination.Append, Schema: idSchema}
		if seq, ok := c.cdc[s]; ok {
			st.Read, st.Seq = source.CDC, seq
		}
		streams = append(streams, st)
		outputs = append(outputs, runner.Stream{Name: s, Replay: source.Exact, Mode: destination.Append, Transformed: true})
	}
	var flow runner.Dataflow = runner.Passthrough{Convert: idRows}
	if c.flow != nil {
		flow = c.flow
	}
	job := func(owner string, drain <-chan struct{}) runner.Job {
		return runner.Job{
			Owner: owner, Pipeline: "p", Continuous: true, Log: log,
			Source:      countingSource{streams: c.streams, rows: c.rows, page: 7, pause: 3 * time.Millisecond},
			Streams:     streams,
			Outputs:     outputs,
			Destination: dest,
			Dataflow:    flow,
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

// Before any row, the first dataflow learns each stream's schema from an
// empty batch of its declared one. A changelog's batch carries op and seq,
// so a CDC stream that declares seq's type gets them, and one that doesn't
// is learned from its first batch. Outputs over changelogs are changelogs
// too, appended ones included, so the job may delete from them.
func TestFlinkPrimesChangelogsWithTheirControlColumns(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		flow := &primerFlow{Passthrough: runner.Passthrough{Convert: idRows}, first: map[string]runner.Frame{}}
		seq := &arrow.Decimal128Type{Precision: 38, Scale: 0}
		_, _, cluster := runFlinkCase(t, flinkCase{streams: []string{"a", "b"}, rows: 40, cdc: map[string]arrow.DataType{"a": seq, "b": nil}, flow: flow})
		for _, o := range cluster.Described().GetOutputs() {
			if !o.GetChangelog() {
				t.Errorf("appended output %s over changelogs isn't one", o.GetName())
			}
		}
		a, b := flow.first["a"], flow.first["b"]
		if a.Kind != runner.FrameBatch || a.Batch.NumRows() != 0 {
			t.Fatalf("a's first frame: %+v", a)
		}
		controls := map[string]arrow.DataType{}
		for _, f := range a.Batch.Schema().Fields() {
			if c, ok := f.Metadata.GetValue(source.ControlKey); ok {
				controls[c] = f.Type
			}
		}
		if len(controls) != 2 || !arrow.TypeEqual(controls["op"], arrow.BinaryTypes.String) || !arrow.TypeEqual(controls["seq"], seq) {
			t.Errorf("a's primer has control columns %v", controls)
		}
		if (b.Kind == runner.FrameBatch && b.Batch.NumRows() == 0) || (b.Kind == runner.FrameRows && len(b.Rows) == 0) {
			t.Errorf("b, without seq's type, was primed: %+v", b)
		}
	})
}

// primerFlow records each stream's first frame into the first dataflow, and
// passes primers on without their control columns, as the engine splits
// them off.
type primerFlow struct {
	runner.Passthrough
	mu    sync.Mutex
	first map[string]runner.Frame
}

func (p *primerFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	if !plan.ReportSchemas {
		return p.Passthrough.Run(ctx, plan, in, out)
	}
	mid := make(chan runner.Frame)
	go func() {
		defer close(mid)
		for f := range in {
			p.mu.Lock()
			if _, ok := p.first[f.Stream]; !ok && f.Stream != "" {
				p.first[f.Stream] = f
			}
			p.mu.Unlock()
			if f.Kind == runner.FrameBatch && f.Batch.NumRows() == 0 {
				f.Batch = array.NewRecordBatch(idSchema, []arrow.Array{array.NewInt64Builder(memory.DefaultAllocator).NewArray()}, 0)
			}
			select {
			case mid <- f:
			case <-ctx.Done():
				return
			}
		}
	}()
	return p.Passthrough.Run(ctx, plan, mid, out)
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

func TestFlinkStopAbortsTheCheckpointInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		runFlinkCase(t, flinkCase{streams: []string{"a", "b", "c", "d"}, rows: 80, faults: flinktest.Faults{AbortOnStop: true}})
	})
}

// failSecondStart fails a job's first run of tasks at its second
// checkpoint, after the first has completed, and calls then once.
func failSecondStart(then func()) func(context.Context, int, string, func() error) error {
	var mu sync.Mutex
	starts := 0
	return func(_ context.Context, run int, op string, apply func() error) error {
		mu.Lock()
		defer mu.Unlock()
		if run == 1 && op == "flink.start" {
			if starts++; starts == 2 {
				if then != nil {
					then()
				}
				return errors.New("a TaskManager was lost")
			}
		}
		return apply()
	}
}

func oneStreamJob(log *checkpoint.Log, dest *exactDest, f runner.Flink, bridge runner.Bridge, rows int, drain <-chan struct{}) runner.Job {
	return runner.Job{
		Owner: "a", Pipeline: "p", Continuous: true, Log: log,
		Source:      countingSource{streams: []string{"a"}, rows: rows, page: 3, pause: 2 * time.Millisecond},
		Streams:     []runner.Stream{{Name: "a", Replay: source.Exact, Read: source.Incremental, Mode: destination.Append, Schema: idSchema}},
		Outputs:     []runner.Stream{{Name: "a", Replay: source.Exact, Mode: destination.Append, Transformed: true}},
		Destination: dest,
		Dataflow:    runner.Passthrough{Convert: idRows},
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline, CommitInterval: 10 * time.Millisecond},
		Flink:       f, Bridge: bridge, Drain: drain,
	}
}

// Without high availability, a JobManager that restarts restores the
// checkpoint the job was deployed from. When HEAD is the job's first record
// and its checkpoint completed, that's before an epoch the destination may
// have made visible, so the runner doesn't move HEAD back: it deploys the job
// again from the checkpoint HEAD stands at, which Flink still retains.
func TestFlinkRestoreBeforeACompletedCheckpointDeploysAgain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		dest := newExactDest()
		bridge := flinktest.NewBridge()
		cluster := flinktest.NewCluster(bridge)
		cluster.Logf = t.Logf
		cluster.Faults.NoHA = func() bool { return true }
		cluster.Point = failSecondStart(nil)
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		const rows = 200
		drain := make(chan struct{})
		go func() {
			for dest.count() < rows {
				select {
				case <-time.After(5 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
			close(drain)
		}()
		_, err := runner.RunAttempt(ctx, oneStreamJob(log, dest, cluster, bridge, rows, drain))
		if !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("attempt: %v", err)
		}
		if n := cluster.Deploys(); n < 2 {
			t.Errorf("deployed %d times, want the job deployed again after its restore", n)
		}
		dest.check(t, []string{"a"}, rows)
	})
}

// hookStore runs before ahead of the next swap of HEAD, once armed.
type hookStore struct {
	checkpoint.Store
	mu     sync.Mutex
	armed  bool
	before func()
}

func (h *hookStore) arm() {
	h.mu.Lock()
	h.armed = true
	h.mu.Unlock()
}

func (h *hookStore) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	h.mu.Lock()
	run := h.armed && strings.HasSuffix(key, "/HEAD")
	h.armed = h.armed && !run
	h.mu.Unlock()
	if run {
		h.before()
	}
	return h.Store.Swap(ctx, key, data, prev)
}

// Another attempt claims the pipeline and writes a record between an
// attempt's check that it isn't fenced and its claim after its job's tasks
// restarted. The attempt must settle the HEAD it claimed, not the one its
// segment left, or pruning from that one deletes the other attempt's record
// from under HEAD.
func TestFlinkRestartAfterAnotherAttemptMovedHEAD(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := checkpoint.NewMemStore()
		hooked := &hookStore{Store: mem}
		hooked.before = func() {
			ctx := context.Background()
			sess, err := checkpoint.NewLog(mem, "org", "p").Claim(ctx, "other")
			if err != nil {
				t.Error(err)
				return
			}
			head, err := sess.Load(ctx)
			if err != nil || head == nil {
				t.Errorf("HEAD: %v, %v", head, err)
				return
			}
			if err := sess.Commit(ctx, &checkpoint.Record{Epoch: head.Epoch + 1, Streams: head.Streams, CreatedAt: time.Now().UTC()}); err != nil {
				t.Error(err)
			}
		}
		log := checkpoint.NewLog(hooked, "org", "p")
		dest := newExactDest()
		bridge := flinktest.NewBridge()
		cluster := flinktest.NewCluster(bridge)
		cluster.Logf = t.Logf
		cluster.Point = failSecondStart(hooked.arm)
		const rows = 80
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		drain := make(chan struct{})
		go func() {
			for dest.count() < rows {
				select {
				case <-time.After(5 * time.Millisecond):
				case <-ctx.Done():
					return
				}
			}
			close(drain)
		}()
		if _, err := runner.RunAttempt(ctx, oneStreamJob(log, dest, cluster, bridge, rows, drain)); !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("attempt: %v", err)
		}
		dest.check(t, []string{"a"}, rows)
		// Pruning keeps 10 records along the chain.
		rec, err := log.Latest(ctx)
		for i := 0; err == nil && rec != nil && rec.Previous != nil && i < 9; i++ {
			rec, err = log.Record(ctx, *rec.Previous)
		}
		if err != nil {
			t.Errorf("HEAD's chain: %v", err)
		}
	})
}
