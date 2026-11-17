package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// floorSource reads split s0 of stream s, ids 1 to 10, a page of five at a
// time, each page followed by a resume point at its last ID. It completes
// the split with a cursor floor, where its next sync starts: the last
// page's first ID less one, so the next sync reads that page again. It
// stops a split at its Until, as the last ID to read, with a resume point
// and its completion there, and can't tell from a completion's state where
// the read ended, so it ignores CompleteAt. reads has each split's Until,
// and completions its CompleteAt.
type floorSource struct {
	reads, completions chan []byte
}

func (floorSource) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

func (f floorSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		f.reads <- sp.Until
		f.completions <- sp.CompleteAt
		next, last := 1, 10
		if sp.State != nil {
			n, err := strconv.Atoi(string(sp.State))
			if err != nil {
				return err
			}
			next = n + 1
		}
		if sp.Until != nil {
			u, err := strconv.Atoi(string(sp.Until))
			if err != nil {
				return err
			}
			last = u
		}
		for next <= last {
			var rows []json.RawMessage
			for ; len(rows) < 5 && next <= last; next++ {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id":%d}`, next)))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(next - 1))); err != nil {
				return err
			}
		}
		if sp.Until != nil {
			return e.Complete(sp.Until)
		}
		return e.Complete([]byte(strconv.Itoa(last - 5)))
	})
}

// A split that completed in the epoch is asked to complete where it did:
// its record holds where its next sync starts, which needn't be where the
// read ended, so a source that can't tell where from it reads to its
// completion, and that is still the redo's end. A split the epoch left at a
// resume point is read to there.
func TestARedoAsksACompletedSplitToCompleteWhereItDid(t *testing.T) {
	for name, c := range map[string]struct {
		to                checkpoint.SplitState
		rows              int
		until, completeAt []byte
	}{
		"completed":          {to: checkpoint.SplitState{ID: "s0", State: []byte("5"), Complete: true}, rows: 10, completeAt: []byte("5")},
		"at a resume point":  {to: checkpoint.SplitState{ID: "s0", State: []byte("5")}, rows: 5, until: []byte("5")},
		"later resume point": {to: checkpoint.SplitState{ID: "s0", State: []byte("10")}, rows: 10, until: []byte("10")},
	} {
		t.Run(name, func(t *testing.T) {
			src := floorSource{reads: make(chan []byte, 1), completions: make(chan []byte, 1)}
			j := &Job{Pipeline: "p", Source: src, Streams: []Stream{{Name: "s"}}}
			out := make(chan Frame, 64)
			r := redoRead{stream: "s", id: "s0", to: c.to, rows: uint64(c.rows)}
			if err := j.replaySplit(t.Context(), 2, r, out); err != nil {
				t.Fatal(err)
			}
			close(out)
			rows := 0
			for f := range out {
				rows += len(f.Rows)
			}
			if rows != c.rows {
				t.Errorf("%d rows read again, want %d", rows, c.rows)
			}
			if until := <-src.reads; string(until) != string(c.until) {
				t.Errorf("the read's until is %q, want %q", until, c.until)
			}
			if at := <-src.completions; string(at) != string(c.completeAt) {
				t.Errorf("the read's complete_at is %q, want %q", at, c.completeAt)
			}
		})
	}
}
