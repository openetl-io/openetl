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
