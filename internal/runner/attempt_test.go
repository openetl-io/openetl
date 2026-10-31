package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// The source emits its second page only once Commit(1) has started, and
// Commit(1) returns only once PreCommit(2) has run, so barrier 2 must be
// requested and placed while epoch 1 is committing.
func TestBarrierRequestedWhileCommitting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		dest := &blockingDest{committing1: make(chan struct{}), preCommitted2: make(chan struct{})}
		res, err := runner.RunAttempt(ctx, runner.Job{
			Owner:       "a1",
			Pipeline:    "p",
			Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"),
			Source:      pagedSource{pages: 2, after1: dest.committing1},
			Streams:     []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append}},
			Destination: dest,
			Dataflow:    countingFlow{},
			Settings: runner.Settings{
				Require:        delivery.ExactlyOnce,
				CommitMaxBytes: 1,
				Scope:          destination.ScopePipeline,
				Dedupe:         delivery.DedupeNone,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if res.Committed < 2 {
			t.Fatalf("committed %d epochs, want at least 2", res.Committed)
		}
		if dest.rows != 2 {
			t.Fatalf("wrote %d rows, want 2", dest.rows)
		}
	})
}

// pagedSource emits one row per page, each followed by a resume point, and
// waits for after1 after the first.
type pagedSource struct {
	pages  int
	after1 <-chan struct{}
}

func (s pagedSource) Read(ctx context.Context, _ source.ReadRequest, out source.Emitter) error {
	for i := range s.pages {
		if i == 1 {
			select {
			case <-s.after1:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := out.Rows("s", []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
			return err
		}
		if err := out.ResumePoint("s", fmt.Appendf(nil, "%d", i+1)); err != nil {
			return err
		}
	}
	return out.Complete("s")
}

// countingFlow turns rows into column-less batches.
type countingFlow struct{}

var empty = arrow.NewSchema(nil, nil)

func (countingFlow) Run(ctx context.Context, _ runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	for f := range in {
		if f.Kind == runner.FrameRows {
			f = runner.Frame{Kind: runner.FrameBatch, Stream: f.Stream, Batch: array.NewRecordBatch(empty, nil, int64(len(f.Rows)))}
		}
		select {
		case out <- f:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

type blockingDest struct {
	committing1, preCommitted2 chan struct{}
	rows                       int64
}

func (d *blockingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}}
}

func (d *blockingDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *blockingDest) Write(_ context.Context, _ destination.Epoch, _ string, b arrow.RecordBatch) error {
	d.rows += b.NumRows()
	return nil
}

func (d *blockingDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	if e.N == 2 {
		close(d.preCommitted2)
	}
	return nil, nil
}

func (d *blockingDest) Commit(ctx context.Context, e destination.Epoch, _ destination.Staged) error {
	if e.N != 1 {
		return nil
	}
	close(d.committing1)
	select {
	case <-d.preCommitted2:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *blockingDest) Recover(context.Context, uint64, destination.Epoch, destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *blockingDest) Close(context.Context) error { return nil }
