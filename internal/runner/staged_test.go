package runner_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// calls is what the destination and the staging saw, in order.
type calls struct {
	mu  sync.Mutex
	log []string
}

func (c *calls) add(format string, args ...any) {
	c.mu.Lock()
	c.log = append(c.log, fmt.Sprintf(format, args...))
	c.mu.Unlock()
}

func (c *calls) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.log
	c.log = nil
	return out
}

type fakeStaging struct{ c *calls }

func (s fakeStaging) URL() string { return "mem://staging" }

func (s fakeStaging) Hand(e destination.Epoch, stream string, files []destination.StagedFile) error {
	s.c.add("hand %d/%d %s %d", e.Generation, e.N, stream, len(files))
	return nil
}

func (s fakeStaging) Flush(_ context.Context, e destination.Epoch) error {
	s.c.add("flush %d/%d", e.Generation, e.N)
	return nil
}

func (s fakeStaging) Drop(_ context.Context, e destination.Epoch) error {
	s.c.add("drop %d/%d", e.Generation, e.N)
	return nil
}

func (s fakeStaging) Sweep(_ context.Context, keep destination.Epoch, gen uint64) (int, error) {
	s.c.add("sweep %d/%d in %d", keep.Generation, keep.N, gen)
	return 0, nil
}

// stagedDest takes staged epochs, and fails Commit(failAt) as a fenced
// writer does, so the attempt stops there.
type stagedDest struct {
	c      *calls
	failAt uint64
}

func (d *stagedDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.DeliverAndAck, Scopes: []destination.Scope{destination.ScopePipeline}, Delivery: destination.DeliveryStaged}
}

func (d *stagedDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *stagedDest) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errors.New("a staged destination got a batch")
}

func (d *stagedDest) WriteStaged(_ context.Context, e destination.Epoch, stream string, files []destination.StagedFile) error {
	d.c.add("files %d/%d %s %s", e.Generation, e.N, stream, files[0].Path)
	return nil
}

func (d *stagedDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	d.c.add("precommit %d/%d", e.Generation, e.N)
	return []byte("mine"), nil
}

func (d *stagedDest) Commit(_ context.Context, e destination.Epoch, s destination.Staged) (destination.Committed, error) {
	if e.N == d.failAt {
		return destination.Committed{}, destination.ErrFenced
	}
	d.c.add("commit %d/%d %s", e.Generation, e.N, s)
	return destination.Committed{}, nil
}

func (d *stagedDest) Recover(_ context.Context, gen uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	d.c.add("recover %d/%d %s in %d", e.Generation, e.N, s, gen)
	return destination.Redone, nil
}

func (d *stagedDest) Close(context.Context) error { return nil }

func stageAll(_ context.Context, e destination.Epoch, stream string, batches []arrow.RecordBatch) ([]destination.StagedFile, error) {
	var rows int64
	for _, b := range batches {
		rows += b.NumRows()
	}
	if rows == 0 {
		return nil, nil
	}
	return []destination.StagedFile{{Path: fmt.Sprintf("%d/%d/%s/part-00000.parquet", e.Generation, e.N, stream), Rows: uint64(rows)}}, nil
}

// A staged destination gets each epoch's files before PreCommit, and the
// record keeps them. An epoch's files are dropped once the next record is
// written, and the cursor is written once Commit returns. The next attempt
// hands HEAD's files over again before Recover.
func TestStagedEpochs(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		c := &calls{}
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		attempt := func(owner string, failAt uint64) error {
			_, err := runner.RunAttempt(ctx, runner.Job{
				Owner:       owner,
				Pipeline:    "p",
				Log:         log,
				Source:      slowSource{pages: 3},
				Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
				Destination: &stagedDest{c: c, failAt: failAt},
				Dataflow:    runner.Passthrough{Convert: columnless, Stage: stageAll},
				Staging:     fakeStaging{c: c},
				Settings:    delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 1, Scope: destination.ScopePipeline},
			})
			return err
		}

		if err := attempt("a1", 2); !errors.Is(err, destination.ErrFenced) {
			t.Fatalf("the first attempt: %v, want fenced", err)
		}
		rec, err := log.Record(ctx, checkpoint.RecordRef{Epoch: 2, Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		if want := map[string][]checkpoint.StagedFile{"s": {{Path: "1/2/s/part-00000.parquet", Rows: 1}}}; rec.Epoch != 2 || !equalFiles(rec.Files, want) || string(rec.Staged) != "mine" {
			t.Fatalf("HEAD's record: epoch %d, files %v, staged %q", rec.Epoch, rec.Files, rec.Staged)
		}
		want := []string{
			"recover 0/0  in 1", "sweep 0/0 in 1",
			"hand 1/1 s 1", "files 1/1 s 1/1/s/part-00000.parquet", "precommit 1/1",
			"commit 1/1 mine", "flush 1/1",
			"hand 1/2 s 1", "files 1/2 s 1/2/s/part-00000.parquet", "precommit 1/2",
			"drop 1/1",
		}
		if got := c.take(); !slices.Equal(got, want) {
			t.Fatalf("the first attempt:\n got %s\nwant %s", strings.Join(got, "\n     "), strings.Join(want, "\n     "))
		}

		if err := attempt("a2", 0); err != nil {
			t.Fatal(err)
		}
		got := c.take()
		want = []string{
			"hand 1/2 s 1", "files 1/2 s 1/2/s/part-00000.parquet", "recover 1/2 mine in 2", "flush 1/2",
			"sweep 1/2 in 2",
			"hand 2/3 s 1", "files 2/3 s 2/3/s/part-00000.parquet", "precommit 2/3",
			"drop 1/2", "commit 2/3 mine", "flush 2/3",
		}
		if !slices.Equal(got, want) {
			t.Fatalf("the second attempt:\n got %s\nwant %s", strings.Join(got, "\n     "), strings.Join(want, "\n     "))
		}
	})
}

func equalFiles(a, b map[string][]checkpoint.StagedFile) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !slices.Equal(v, b[k]) {
			return false
		}
	}
	return true
}

// A staged destination needs somewhere to stage.
func TestStagedDestinationNeedsStaging(t *testing.T) {
	_, err := runner.RunAttempt(t.Context(), runner.Job{
		Owner:       "a1",
		Pipeline:    "p",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
		Source:      slowSource{pages: 1},
		Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
		Destination: &stagedDest{c: &calls{}},
		Dataflow:    runner.Passthrough{Convert: columnless, Stage: stageAll},
		Settings:    delivery.Settings{Require: delivery.AtLeastOnce, Scope: destination.ScopePipeline},
	})
	if err == nil || !strings.Contains(err.Error(), "nowhere to stage") {
		t.Fatalf("got %v", err)
	}
}
