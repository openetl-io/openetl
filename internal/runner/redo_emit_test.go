package runner

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// replayed feeds e a split's emits, rows in pages of five, each followed
// by a resume point at the last row's ID, then ends with end: a resume
// point or a completion at state at.
func replayed(t *testing.T, e *redoEmitter, rows int, complete bool, at string) {
	t.Helper()
	sp := e.Split("s", "s0")
	for first := 1; first <= rows; first += 5 {
		var page []json.RawMessage
		for id := first; id < first+5 && id <= rows; id++ {
			page = append(page, json.RawMessage(`{}`))
		}
		if err := sp.Rows(page); err != nil {
			t.Fatal(err)
		}
	}
	if complete {
		_ = sp.Complete([]byte(at))
	} else {
		_ = sp.ResumePoint([]byte(at))
	}
}

// draining is a redo emitter of one split, whose frames go nowhere.
func draining(t *testing.T, r redoRead) *redoEmitter {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	out := make(chan Frame)
	go func() {
		for {
			select {
			case <-out:
			case <-ctx.Done():
				return
			}
		}
	}()
	r.stream, r.id = "s", "s0"
	return &redoEmitter{ctx: ctx, stop: stop, read: r, out: out}
}

// A split read again ends where the record has it: a completed one only at
// a completion whose next read starts where the epoch's did, and no read
// with fewer rows than the epoch had, but where its barrier was forced.
func TestARedoEndsWhereTheEpochDid(t *testing.T) {
	for name, c := range map[string]struct {
		to       checkpoint.SplitState
		rows     uint64
		replay   int
		complete bool
		at       string
		lost     bool
	}{
		"at its resume point":                    {to: checkpoint.SplitState{State: []byte("20")}, rows: 20, replay: 20, at: "20"},
		"at its resume point, short":             {to: checkpoint.SplitState{State: []byte("20")}, rows: 20, replay: 10, at: "20", lost: true},
		"at its resume point, forced and short":  {to: checkpoint.SplitState{State: []byte("20"), Forced: true}, rows: 25, replay: 20, at: "20"},
		"at its resume point, rows not recorded": {to: checkpoint.SplitState{State: []byte("20")}, replay: 10, at: "20"},
		"completed":                              {to: checkpoint.SplitState{State: []byte("20"), Complete: true}, rows: 20, replay: 20, complete: true, at: "20"},
		"completed elsewhere":                    {to: checkpoint.SplitState{State: []byte("20"), Complete: true}, rows: 10, replay: 10, complete: true, at: "10", lost: true},
		"completed short":                        {to: checkpoint.SplitState{State: []byte("20"), Complete: true}, rows: 20, replay: 10, complete: true, at: "20", lost: true},
		"completed early":                        {to: checkpoint.SplitState{State: []byte("20")}, rows: 10, replay: 10, complete: true, at: "10", lost: true},
	} {
		t.Run(name, func(t *testing.T) {
			e := draining(t, redoRead{to: c.to, rows: c.rows})
			replayed(t, e, c.replay, c.complete, c.at)
			reached, err := e.result()
			if c.lost {
				if reached || errs.CodeOf(err) != errs.PositionLost {
					t.Fatalf("reached %v, with %v", reached, err)
				}
				return
			}
			if !reached || err != nil {
				t.Fatalf("reached %v, with %v", reached, err)
			}
		})
	}
}

// releases counts a batch's releases.
type releases struct {
	arrow.RecordBatch
	n int
}

func (r *releases) Release() { r.n++ }

// A batch the redo can't pass on, as the read stops, stays the source's,
// as the ordinary read's emitter leaves it: the redo doesn't release it.
func TestARedoLeavesABatchItCantPassOnToTheSource(t *testing.T) {
	ctx, stop := context.WithCancel(t.Context())
	stop()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	b.Append(1)
	col := b.NewArray()
	defer col.Release()
	rb := array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{col}, 1)
	defer rb.Release()
	batch := &releases{RecordBatch: rb}
	e := &redoEmitter{ctx: ctx, stop: stop, read: redoRead{stream: "s", id: source.WholeStream}, out: make(chan Frame)}
	if err := e.Records("s", batch); err == nil {
		t.Fatal("a batch went on with the read stopped")
	}
	if batch.n != 0 {
		t.Errorf("the redo released the source's batch %d times", batch.n)
	}
	// Another split's batch, which the redo takes and drops, it releases.
	if err := e.Split("s", "other").Records(batch); err != nil || batch.n != 1 {
		t.Errorf("another split's batch: %v, released %d times", err, batch.n)
	}
}
