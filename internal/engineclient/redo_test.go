package engineclient

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// chunkSource reads its splits a chunk of rows at a time, with a resume
// point after each, its state the last chunk read, and pause between
// chunks: a millisecond if zero.
type chunkSource struct {
	splits, chunks, rows int
	pause                time.Duration
}

func (s *chunkSource) Splits(_ context.Context, _ source.ConfiguredStream, _ []source.Split) ([]source.Split, error) {
	out := make([]source.Split, s.splits)
	for i := range out {
		out[i] = source.Split{ID: fmt.Sprint(i)}
	}
	return out, nil
}

func (s *chunkSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		split, err := strconv.Atoi(sp.ID)
		if err != nil {
			return err
		}
		c := 0
		if sp.State != nil {
			if c, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
			c++
		}
		for ; c < s.chunks; c++ {
			rows := make([]json.RawMessage, s.rows)
			for r := range rows {
				rows[r] = fmt.Appendf(nil, `{"id": %d}`, (split*s.chunks+c)*s.rows+r)
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(c))); err != nil {
				return err
			}
			select {
			case <-time.After(cmp.Or(s.pause, time.Millisecond)):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return e.Complete(nil)
	})
}

// txnLanes is a destination whose writer lanes commit on their own: a
// lane's part of an epoch is a transaction its writer holds, which opening
// the lane again aborts, and a commit makes visible with the lane's marker.
type txnLanes struct {
	t  *testing.T
	mu sync.Mutex
	// By lane, its producer and its open transactions.
	producers map[int]uint64
	open      map[int]map[txnMark][]int64
	aborted   map[txnMark][]int64
	marked    map[txnMark]bool
	rows      []int64
	redone    int
	// Called before a commit applies, by a writer of generation gen.
	before func(gen uint64, m txnMark, rows int) error
}

type txnMark struct {
	Gen, N uint64
	Lane   int
}

type txnCommit struct {
	Mark     txnMark `json:"mark"`
	Producer uint64  `json:"producer"`
}

func (d *txnLanes) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.TransactionalStream, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: 16, LaneCommits: true}
}

func (d *txnLanes) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.producers[cfg.Lane]++
	for m, rows := range d.open[cfg.Lane] {
		d.aborted[m] = rows
	}
	d.open[cfg.Lane] = map[txnMark][]int64{}
	return &txnLane{d: d, lane: cfg.Lane, gen: cfg.Generation, producer: d.producers[cfg.Lane], writing: map[destination.Epoch][]int64{}}, nil
}

type txnLane struct {
	d        *txnLanes
	lane     int
	gen      uint64
	producer uint64
	writing  map[destination.Epoch][]int64
}

func (w *txnLane) Write(_ context.Context, e destination.Epoch, _ string, b arrow.RecordBatch) error {
	w.writing[e] = append(w.writing[e], b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64).Int64Values()...)
	return nil
}

func (w *txnLane) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	d := w.d
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.producers[w.lane] != w.producer {
		return nil, destination.ErrFenced
	}
	m := txnMark{e.Generation, e.N, w.lane}
	d.open[w.lane][m] = w.writing[e]
	delete(w.writing, e)
	return json.Marshal(txnCommit{Mark: m, Producer: w.producer})
}

func (w *txnLane) Commit(_ context.Context, _ destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	d := w.d
	var c txnCommit
	if len(staged) != 1 || json.Unmarshal(staged[0], &c) != nil || c.Mark.Lane != w.lane {
		return destination.Committed{}, fmt.Errorf("lane %d asked to commit %d committables", w.lane, len(staged))
	}
	d.mu.Lock()
	rows, before := len(d.open[w.lane][c.Mark]), d.before
	d.mu.Unlock()
	if before != nil {
		if err := before(w.gen, c.Mark, rows); err != nil {
			return destination.Committed{}, err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.marked[c.Mark] {
		return destination.Committed{}, nil
	}
	got, ok := d.open[w.lane][c.Mark]
	switch {
	case d.producers[w.lane] != c.Producer:
		return destination.Committed{}, destination.ErrFenced
	case !ok:
		return destination.Committed{}, fmt.Errorf("lane %d has no open transaction of %v", w.lane, c.Mark)
	}
	if lost, ok := d.aborted[c.Mark]; ok {
		d.redone++
		if !slices.Equal(slices.Sorted(slices.Values(lost)), slices.Sorted(slices.Values(got))) {
			d.t.Errorf("%v was written again with %d rows, not its %d", c.Mark, len(got), len(lost))
		}
	}
	delete(d.open[w.lane], c.Mark)
	d.rows = append(d.rows, got...)
	d.marked[c.Mark] = true
	return destination.Committed{}, nil
}

func (w *txnLane) Recover(_ context.Context, _ uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	var c txnCommit
	if len(staged) != 1 || json.Unmarshal(staged[0], &c) != nil || c.Mark.Lane != w.lane {
		return 0, fmt.Errorf("lane %d asked to settle %d committables", w.lane, len(staged))
	}
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if w.d.marked[c.Mark] {
		return destination.Applied, nil
	}
	return destination.Lost, nil
}

func (w *txnLane) Close(context.Context) error { return nil }

// A keyed stream's epoch, its key groups spread over the workers' writer
// lanes through the exchange between their engines, committed in part when
// a worker is lost: the next attempt's engines write each lost lane's part
// again, the rows of its key groups alone of every split, whichever worker
// read it, and every row lands once.
func TestARedoThroughTheEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	var crossed atomic.Bool
	defer cancel()
	srv, err := attempt.Listen(ctx, attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := &txnLanes{t: t, producers: map[int]uint64{}, open: map[int]map[txnMark][]int64{}, aborted: map[txnMark][]int64{}, marked: map[txnMark]bool{}}
	opts := options(t, nil)
	timings := attempt.Options{Heartbeat: 100 * time.Millisecond, Timeout: 10 * time.Second}
	run := func(owner string, n int, src func() source.Reader) error {
		job := func(s source.Reader) runner.Job {
			return runner.Job{
				Pipeline: "p", Owner: owner, Sync: "s1", Log: log, Source: s,
				Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}}},
				Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2,
				Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 16 << 10, Scope: destination.ScopePipeline},
			}
		}
		var workers sync.WaitGroup
		cancels := make([]context.CancelFunc, n)
		j := job(src())
		j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
			for _, c := range rec.Coverage {
				readers := map[string]bool{}
				for _, s := range c.Splits {
					readers[s.ID] = true
				}
				crossed.Store(crossed.Load() || len(readers) > 1)
			}
		}
		j.Workers = &runner.Workers{Count: n, Server: srv, Options: timings, Launch: func(_ context.Context, targets []attempt.Target) error {
			for _, tg := range targets {
				wctx, stop := context.WithCancel(ctx)
				cancels[tg.Worker] = stop
				wj := job(src())
				wj.Log = nil
				workers.Go(func() { _ = runner.RunWorker(wctx, wj, tg, timings) })
			}
			return nil
		}}
		// Attempt 1's second epoch with rows on lane 0, worker 0's, fails
		// there, and loses the worker.
		var mu sync.Mutex
		var once sync.Once
		var failing *txnMark
		held := 0
		dest.mu.Lock()
		dest.before = func(gen uint64, m txnMark, rows int) error {
			if gen != 1 || m.Lane != 0 {
				return nil
			}
			mu.Lock()
			if failing == nil && rows > 0 {
				if held++; held == 2 {
					failing = &m
				}
			}
			fail := failing != nil && *failing == m
			mu.Unlock()
			if !fail {
				return nil
			}
			once.Do(cancels[0])
			return errors.New("worker 0 is gone")
		}
		dest.mu.Unlock()
		_, err := runner.RunAttempt(ctx, j)
		workers.Wait()
		return err
	}
	// Each worker reads a split, so enough chunks that the first attempt
	// fails mid-read.
	const chunks = 150
	src := func() source.Reader { return &chunkSource{splits: 3, chunks: chunks, rows: 50} }
	if err := run("a1", 3, src); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	// Each worker reads one split, and its engine's exchange sends rows to
	// every worker's writer lanes, so a committable holds others' splits.
	if !crossed.Load() {
		t.Error("no committable held the rows of splits two workers read")
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(head.Coverage) != len(head.Committables) || slices.ContainsFunc(head.Splits("s"), func(sp checkpoint.SplitState) bool { return sp.Complete }) {
		t.Fatalf("HEAD is epoch %d, with %d coverages of %d committables and splits %v", head.Epoch, len(head.Coverage), len(head.Committables), head.Splits("s"))
	}
	if err := run("a2", 2, src); err != nil {
		t.Fatal(err)
	}
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if dest.redone == 0 {
		t.Error("no lane was written again")
	}
	seen := map[int64]bool{}
	for _, id := range dest.rows {
		if seen[id] {
			t.Fatalf("row %d landed twice", id)
		}
		seen[id] = true
	}
	if want := 3 * chunks * 50; len(seen) != want {
		t.Errorf("landed %d rows, want %d", len(seen), want)
	}
}
