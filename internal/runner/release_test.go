package runner_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// floodFlow sends batches of s from alloc until the attempt stops, and, as
// the engine client does, releases the batch it couldn't send.
type floodFlow struct {
	alloc memory.Allocator
	sent  *atomic.Int64
}

func (f floodFlow) Run(ctx context.Context, _ runner.Plan, _ <-chan runner.Frame, out chan<- runner.Frame) error {
	defer close(out)
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	for {
		b := array.NewRecordBuilder(f.alloc, schema)
		b.Field(0).(*array.Int64Builder).Append(1)
		rec := b.NewRecordBatch()
		b.Release()
		select {
		case out <- runner.Frame{Kind: runner.FrameBatch, Stream: "s", Batch: rec}:
			f.sent.Add(1)
		case <-ctx.Done():
			rec.Release()
			return ctx.Err()
		}
	}
}

// failingDest fails its first write once the flow has sent enough frames
// to leave some waiting in the attempt's channels.
type failingDest struct {
	countingDest
	sent *atomic.Int64
}

func (d *failingDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *failingDest) Write(ctx context.Context, _ destination.Epoch, _ string, _ arrow.RecordBatch) error {
	for d.sent.Load() < 20 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	return errors.New("the write failed")
}

// An attempt that stops releases the frames still waiting in its channels.
func TestAStoppedAttemptReleasesItsFrames(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	sent := new(atomic.Int64)
	j := job(checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"), "a1", encodedSource{pages: 3}, floodFlow{alloc: alloc, sent: sent}, make(chan uint64, 16))
	j.Destination = &failingDest{sent: sent}
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt ran, want the write's failure")
	}
	alloc.AssertSize(t, 0)
}
