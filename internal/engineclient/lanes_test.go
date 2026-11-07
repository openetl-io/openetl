package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// laneSource reads one stream in splits of ids, a chunk of rows at a time,
// with a resume point after each.
type laneSource struct {
	splits, chunks, rows int
	// Set: the first split reads only its first chunk until the others end,
	// which holds every barrier back, so a lane far ahead of it has to wait.
	straggle bool
	others   sync.WaitGroup
}

func (s *laneSource) Splits(_ context.Context, _ source.ConfiguredStream, _ []source.Split) ([]source.Split, error) {
	out := make([]source.Split, s.splits)
	for i := range out {
		out[i] = source.Split{ID: fmt.Sprint(i)}
	}
	return out, nil
}

func (s *laneSource) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

func (s *laneSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	s.others.Add(s.splits - 1)
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		var split int
		if _, err := fmt.Sscan(sp.ID, &split); err != nil {
			return err
		}
		for c := range s.chunks {
			rows := make([]json.RawMessage, s.rows)
			for r := range rows {
				rows[r] = fmt.Appendf(nil, `{"id": %d}`, split*1_000_000_000+c*1_000_000+r)
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if s.straggle && split == 0 && c == 0 {
				done := make(chan struct{})
				go func() { s.others.Wait(); close(done) }()
				select {
				case <-done:
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			if err := e.ResumePoint(fmt.Appendf(nil, "%d", c)); err != nil {
				return err
			}
		}
		if s.straggle && split != 0 {
			s.others.Done()
		}
		return e.Complete(nil)
	})
}

// laneDest keeps what each writer lane staged in each epoch, and applies them
// together.
type laneDest struct {
	lanes int
	slow  time.Duration

	mu      sync.Mutex
	opened  []int
	staged  map[uint64]map[int][]int64
	applied []int64
	byLane  map[int64]int
	ends    map[string]uint64
}

func (d *laneDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: d.lanes, WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite}}
}

func (d *laneDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.opened = append(d.opened, cfg.Lane)
	return &laneWriter{d: d, lane: cfg.Lane}, nil
}

type laneWriter struct {
	d    *laneDest
	lane int
}

var _ destination.ReadTracker = (*laneWriter)(nil)

func (w *laneWriter) StartReads(context.Context, map[string]time.Time) error { return nil }

func (w *laneWriter) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	w.d.ends[stream] = e.N
	return nil
}

func (w *laneWriter) Write(_ context.Context, e destination.Epoch, _ string, b arrow.RecordBatch) error {
	if w.lane == 0 && w.d.slow > 0 {
		time.Sleep(w.d.slow)
	}
	ids, ok := b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64)
	if !ok {
		return fmt.Errorf("id is %v", b.Column(0).DataType())
	}
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if w.d.staged[e.N] == nil {
		w.d.staged[e.N] = map[int][]int64{}
	}
	w.d.staged[e.N][w.lane] = append(w.d.staged[e.N][w.lane], ids.Int64Values()...)
	return nil
}

func (w *laneWriter) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return fmt.Appendf(nil, "%d", w.lane), nil
}

func (w *laneWriter) Commit(_ context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if w.lane != 0 {
		return destination.Committed{}, fmt.Errorf("committed on lane %d", w.lane)
	}
	if len(staged) != max(len(w.d.opened), 1) {
		return destination.Committed{}, fmt.Errorf("%d committables on %d lanes", len(staged), len(w.d.opened))
	}
	for lane, ids := range w.d.staged[e.N] {
		for _, id := range ids {
			w.d.byLane[id] = lane
		}
		w.d.applied = append(w.d.applied, ids...)
	}
	delete(w.d.staged, e.N)
	return destination.Committed{}, nil
}

func (w *laneWriter) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (w *laneWriter) Close(context.Context) error { return nil }

func TestLanesThroughTheEngine(t *testing.T) {
	cases := []struct {
		name          string
		lanes, writer int
		keyed, byKey  bool
		straggle      bool
	}{
		{name: "in turn", lanes: 4, writer: 4},
		{name: "splits by key", lanes: 4, writer: 4, keyed: true, byKey: true},
		{name: "a straggler holds back barriers", lanes: 4, writer: 4, keyed: true, byKey: true, straggle: true},
		{name: "lanes meet at one writer lane", lanes: 4, writer: 1},
		{name: "lanes meet at one writer lane behind a straggler", lanes: 4, writer: 1, keyed: true, byKey: true, straggle: true},
		{name: "routed by key group", lanes: 4, writer: 4, keyed: true},
		{name: "routed to fewer writer lanes", lanes: 4, writer: 2, keyed: true},
		{name: "one lane", lanes: 1, writer: 1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			src := &laneSource{splits: 8, chunks: 20, rows: 200, straggle: c.straggle}
			dest := &laneDest{lanes: c.writer, slow: time.Millisecond, staged: map[uint64]map[int][]int64{}, byLane: map[int64]int{}, ends: map[string]uint64{}}
			stream := runner.Stream{Name: "s", Replay: source.Exact, Mode: destination.Overwrite}
			if c.keyed {
				stream.PrimaryKey, stream.SplitsByKey = []string{"id"}, c.byKey
			}
			opts := options(t, nil)
			res, err := runner.RunAttempt(ctx, runner.Job{
				Pipeline: "p", Owner: "o", Sync: "s1", Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
				Source: src, Streams: []runner.Stream{stream}, Destination: dest,
				Dataflow: &Dataflow{Options: opts}, Lanes: c.lanes,
				Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 64 << 10, Scope: destination.ScopePipeline},
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Committed < 2 {
				t.Errorf("committed %d epochs, want several", res.Committed)
			}
			want := 8 * 20 * 200
			if len(dest.applied) != want {
				t.Errorf("applied %d rows, want %d", len(dest.applied), want)
			}
			seen := map[int64]bool{}
			for _, id := range dest.applied {
				if seen[id] {
					t.Fatalf("row %d applied twice", id)
				}
				seen[id] = true
			}
			if len(dest.opened) != c.writer {
				t.Errorf("opened writer lanes %v, want %d", dest.opened, c.writer)
			}
			if got := dest.ends["s"]; got != res.Last {
				t.Errorf("the read ended in epoch %d, and the last epoch is %d", got, res.Last)
			}
			lanes := map[int]bool{}
			for id, lane := range dest.byLane {
				lanes[lane] = true
				// A split's rows stay on one writer lane.
				first := dest.byLane[id-id%1_000_000_000]
				if c.byKey && first != lane {
					t.Fatalf("row %d is on writer lane %d and its split's first on %d", id, lane, first)
				}
			}
			if c.writer > 1 && len(lanes) != c.writer {
				got := make([]int, 0, len(lanes))
				for l := range lanes {
					got = append(got, l)
				}
				slices.Sort(got)
				t.Errorf("rows reached writer lanes %v, want all %d", got, c.writer)
			}
		})
	}
}
