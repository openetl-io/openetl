package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// splitSource splits its one stream into a and b. A split read from its
// beginning emits a row and resumes at "<id>1", b adding c there, then
// waits for gate; every split then completes at "<id>N".
type splitSource struct {
	gate chan struct{}

	mu    sync.Mutex
	reads []source.ConfiguredStream
	ended [][]source.Split
}

func (s *splitSource) Splits(_ context.Context, _ source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ended = append(s.ended, ended)
	return []source.Split{{ID: "a"}, {ID: "b"}}, nil
}

func (s *splitSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	cs := req.Streams[0]
	s.mu.Lock()
	s.reads = append(s.reads, cs)
	s.mu.Unlock()
	for _, sp := range cs.Splits {
		if sp.State != nil {
			continue
		}
		e := out.Split("s", sp.ID)
		if err := e.Rows([]json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		var added []source.Split
		if sp.ID == "b" {
			added = []source.Split{{ID: "c", State: []byte("c0")}}
		}
		if err := e.ResumePoint([]byte(sp.ID+"1"), added...); err != nil {
			return err
		}
	}
	select {
	case <-s.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, id := range []string{"a", "b", "c"} {
		if err := out.Split("s", id).Complete([]byte(id + "N")); err != nil {
			return err
		}
	}
	return nil
}

// A stream's splits, and the one a resume point adds, are recorded; an
// attempt that continues the read reads its open splits from their states,
// and the next sync's read starts where they ended.
func TestSplitsAreRecordedAndContinued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		src := &splitSource{gate: make(chan struct{})}
		attempt := func(owner, syncID string, drain bool) error {
			job := runner.Job{
				Owner: owner, Sync: syncID, Pipeline: "p", Log: log, Source: src,
				Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
				Destination: &countingDest{},
				Dataflow:    runner.Passthrough{Convert: columnless},
				Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
			}
			if drain {
				ch := make(chan struct{})
				var once sync.Once
				job.Drain, job.OnCommit = ch, func(uint64) { once.Do(func() { close(ch) }) }
			}
			_, err := runner.RunAttempt(ctx, job)
			return err
		}
		head := func() *checkpoint.Record {
			rec, err := log.Latest(ctx)
			if err != nil {
				t.Fatal(err)
			}
			return rec
		}

		if err := attempt("a1", "s1", true); !errors.Is(err, runner.ErrDrained) {
			t.Fatalf("the first attempt: %v, want drained", err)
		}
		want := []checkpoint.SplitState{{ID: "a", State: []byte("a1")}, {ID: "b", State: []byte("b1")}, {ID: "c", State: []byte("c0")}}
		if rec := head(); !reflect.DeepEqual(rec.Splits("s"), want) || rec.Streams["s"].Complete {
			t.Fatalf("drained at %+v, complete %v; want %+v", rec.Splits("s"), rec.Streams["s"].Complete, want)
		}

		close(src.gate)
		if err := attempt("a2", "s1", false); err != nil {
			t.Fatal(err)
		}
		reread := []source.Split{{ID: "a", State: []byte("a1")}, {ID: "b", State: []byte("b1")}, {ID: "c", State: []byte("c0")}}
		if got := src.reads[1].Splits; !reflect.DeepEqual(got, reread) || len(src.ended) != 1 {
			t.Fatalf("the second attempt read %+v, asking for splits %d times; want %+v, once", got, len(src.ended), reread)
		}
		done := []checkpoint.SplitState{{ID: "a", State: []byte("aN"), Complete: true}, {ID: "b", State: []byte("bN"), Complete: true}, {ID: "c", State: []byte("cN"), Complete: true}}
		if rec := head(); !reflect.DeepEqual(rec.Splits("s"), done) || !rec.Streams["s"].Complete {
			t.Fatalf("ended at %+v, complete %v; want %+v", rec.Splits("s"), rec.Streams["s"].Complete, done)
		}

		if err := attempt("b1", "s2", false); err != nil {
			t.Fatal(err)
		}
		ended := []source.Split{{ID: "a", State: []byte("aN")}, {ID: "b", State: []byte("bN")}, {ID: "c", State: []byte("cN")}}
		if len(src.ended) != 2 || src.ended[0] != nil || !reflect.DeepEqual(src.ended[1], ended) {
			t.Fatalf("splits asked for with %+v, want nil, then %+v", src.ended, ended)
		}
	})
}

// laneSource reads every split of its one stream at once, emitting a chunk,
// a resume point and a completion each, unless told to wait.
type laneSource struct {
	ids []string
	// Set: split "a" emits and then waits for release, resuming at release.
	release chan struct{}
	// Set: each split emits and then waits for another to have.
	meet chan struct{}

	mu       sync.Mutex
	asked    []string
	accept   bool
	added    bool
	gotLanes int
}

func (s *laneSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, len(s.ids))
	for i, id := range s.ids {
		out[i] = source.Split{ID: id}
	}
	return out, nil
}

func (s *laneSource) Resplit(_ context.Context, _, split string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.asked = append(s.asked, split)
	return s.accept, nil
}

func (s *laneSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	s.mu.Lock()
	s.gotLanes = req.Lanes
	s.mu.Unlock()
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if err := e.Rows([]json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if s.meet != nil {
			select {
			case s.meet <- struct{}{}:
			case <-s.meet:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if sp.ID == "a" && s.release != nil {
			select {
			case <-s.release:
			case <-ctx.Done():
				return ctx.Err()
			}
			s.mu.Lock()
			add := s.accept && !s.added
			s.added = s.added || add
			s.mu.Unlock()
			if add {
				if err := e.ResumePoint([]byte("half"), source.Split{ID: "a2"}); err != nil {
					return err
				}
			}
		}
		return e.Complete(nil)
	})
}

// laneFlow records which lane each frame comes in on.
type laneFlow struct {
	runner.Passthrough
	mu     sync.Mutex
	frames []runner.Frame
	plan   runner.Plan
}

func (f *laneFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	f.mu.Lock()
	f.plan = plan
	f.mu.Unlock()
	through := make(chan runner.Frame)
	go func() {
		defer close(through)
		for fr := range in {
			f.mu.Lock()
			f.frames = append(f.frames, runner.Frame{Kind: fr.Kind, Lane: fr.Lane, Stream: fr.Stream, Epoch: fr.Epoch})
			f.mu.Unlock()
			select {
			case through <- fr:
			case <-ctx.Done():
				return
			}
		}
	}()
	return f.Passthrough.Run(ctx, plan, through, out)
}

func (f *laneFlow) dataLanes() map[int]int {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := map[int]int{}
	for _, fr := range f.frames {
		if fr.Kind == runner.FrameRows {
			out[fr.Lane]++
		}
	}
	return out
}

type laneDest struct{ countingDest }

func (d *laneDest) Capabilities() destination.Capabilities {
	c := d.countingDest.Capabilities()
	c.WriterLanes = 8
	return c
}

func laneJob(src source.Source, flow runner.Dataflow, lanes int, key []string) runner.Job {
	return runner.Job{
		Owner: "o", Sync: "s", Pipeline: "p", Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), Source: src,
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: key, SplitsByKey: true}},
		Destination: &laneDest{},
		Dataflow:    flow,
		Lanes:       lanes,
		Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
	}
}

type lanesOnly struct{ *laneSource }

func (l lanesOnly) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

// plainSource can't split a running split.
type plainSource struct {
	inner *laneSource
}

func (p plainSource) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }
func (p plainSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return p.inner.Read(ctx, req, out)
}
func (p plainSource) Splits(ctx context.Context, s source.ConfiguredStream, ended []source.Split) ([]source.Split, error) {
	return p.inner.Splits(ctx, s, ended)
}

// An attempt runs no more lanes than it has splits to read, and the source
// hears how many.
func TestAnAttemptsLanesFollowItsSplits(t *testing.T) {
	src := &laneSource{ids: []string{"a", "b"}, meet: make(chan struct{})}
	flow := &laneFlow{Passthrough: runner.Passthrough{Convert: columnless}}
	if _, err := runner.RunAttempt(t.Context(), laneJob(plainSource{src}, flow, 4, []string{"id"})); err != nil {
		t.Fatal(err)
	}
	if flow.plan.Lanes != 2 || flow.plan.WriterLanes != 2 || src.gotLanes != 2 {
		t.Errorf("%d lanes, %d writer lanes, the source told %d; want 2", flow.plan.Lanes, flow.plan.WriterLanes, src.gotLanes)
	}
	if got := flow.dataLanes(); len(got) != 2 {
		t.Errorf("rows on lanes %v, want a lane per split", got)
	}
}

// A split gets the lane emitting the least when it starts, so splits read at
// once keep to lanes of their own.
func TestSplitsTakeTheLaneEmittingTheLeast(t *testing.T) {
	src := &laneSource{ids: []string{"a", "b", "c", "d", "e", "f"}, meet: make(chan struct{})}
	flow := &laneFlow{Passthrough: runner.Passthrough{Convert: columnless}}
	if _, err := runner.RunAttempt(t.Context(), laneJob(plainSource{src}, flow, 2, []string{"id"})); err != nil {
		t.Fatal(err)
	}
	got := flow.dataLanes()
	if got[0] != 3 || got[1] != 3 {
		t.Errorf("rows on lanes %v", got)
	}
}

// A stream without a key has its frames sent to the lanes in turn, whatever
// its splits, where its destination puts rows in tables.
func TestFramesOfAStreamWithoutAKeyGoInTurn(t *testing.T) {
	src := &laneSource{ids: []string{"a", "b", "c", "d"}}
	flow := &laneFlow{Passthrough: runner.Passthrough{Convert: columnless}}
	if _, err := runner.RunAttempt(t.Context(), laneJob(plainSource{src}, flow, 2, nil)); err != nil {
		t.Fatal(err)
	}
	if got := flow.dataLanes(); got[0] != 2 || got[1] != 2 {
		t.Errorf("rows on lanes %v, want two each", got)
	}
	flow.mu.Lock()
	defer flow.mu.Unlock()
	barriers := map[int]int{}
	for _, f := range flow.frames {
		if f.Kind == runner.FrameBarrier {
			barriers[f.Lane]++
		}
	}
	if barriers[0] == 0 || barriers[0] != barriers[1] {
		t.Errorf("barriers on lanes %v, want as many on each", barriers)
	}
}

// A lane with nothing to do asks for the split that's been reading longest to
// be split, and asks about another when the source refuses.
func TestAnIdleLaneAsksForAStragglerToBeSplit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := &laneSource{ids: []string{"a", "b"}, release: make(chan struct{}), accept: true}
		flow := &laneFlow{Passthrough: runner.Passthrough{Convert: columnless}}
		done := make(chan error, 1)
		go func() {
			_, err := runner.RunAttempt(t.Context(), laneJob(lanesOnly{src}, flow, 2, []string{"id"}))
			done <- err
		}()
		time.Sleep(6 * time.Second)
		synctest.Wait()
		src.mu.Lock()
		asked := append([]string(nil), src.asked...)
		src.mu.Unlock()
		if !reflect.DeepEqual(asked, []string{"a"}) {
			t.Errorf("asked to split %v, want a", asked)
		}
		close(src.release)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if got := flow.dataLanes(); got[0]+got[1] != 3 {
			t.Errorf("rows on lanes %v, want the added split's too", got)
		}
	})
}

// slowCommits takes a long time to commit.
type slowCommits struct {
	countingDest
	commit time.Duration
}

type slowWriter struct {
	destination.Writer
	d *slowCommits
}

func (d *slowCommits) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w, err := d.countingDest.Open(ctx, cfg)
	return slowWriter{w, d}, err
}

func (w slowWriter) Commit(ctx context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	time.Sleep(w.d.commit)
	return w.Writer.Commit(ctx, e, staged)
}

// steadySource emits a row a second, with a resume point after each.
type steadySource struct{ rows int }

func (s steadySource) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{}, nil
}

func (s steadySource) Read(_ context.Context, _ source.ReadRequest, out source.Emitter) error {
	for range s.rows {
		time.Sleep(time.Second)
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{"pad":"0123456789"}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", []byte("x")); err != nil {
			return err
		}
	}
	return out.Complete("s", nil)
}

// While commits take more than a quarter of an epoch, epochs get longer;
// a pipeline that sets its bytes keeps them.
func TestEpochsGrowWhenCommitsCantKeepUp(t *testing.T) {
	for _, set := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			dest := &slowCommits{commit: 5 * time.Second}
			job := runner.Job{
				Owner: "o", Sync: "s", Pipeline: "p", Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
				Source:      steadySource{rows: 200},
				Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
				Destination: dest,
				Dataflow:    runner.Passthrough{Convert: columnless},
				Settings:    delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 16, CommitMaxBytesSet: set, Scope: destination.ScopePipeline},
			}
			res, err := runner.RunAttempt(t.Context(), job)
			if err != nil {
				t.Fatal(err)
			}
			// At 16 bytes, a commit in flight paces it to about one every 5 s.
			if set && res.Committed < 30 {
				t.Errorf("committed %d epochs with its bytes set, want about 40", res.Committed)
			}
			if !set && res.Committed > 20 {
				t.Errorf("committed %d epochs, want them to have grown", res.Committed)
			}
		})
	}
}
