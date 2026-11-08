package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A paused stream isn't read, and each record carries its state from the
// record the attempt restored, without its rows, so resuming it reads on
// from where it was.
func TestPausedStreamsCarryTheirState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		defer cancel()
		log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
		read := map[string]int{}
		a := runner.Stream{Name: "a", Replay: source.Exact, Mode: destination.Append}
		b := runner.Stream{Name: "b", Replay: source.Exact, Mode: destination.Append}
		var records []*checkpoint.Record
		attempt := func(sync string, pauseB bool) {
			t.Helper()
			committed := make(chan uint64, 16)
			j := job(log, "attempt-"+sync, incrementalSource{pages: 2, committed: committed, read: read}, runner.Passthrough{Convert: columnless}, committed)
			j.Sync = sync
			j.Streams = []runner.Stream{a, b}
			if pauseB {
				j.Streams, j.Paused = []runner.Stream{a}, []runner.Stream{b}
			}
			records = nil
			j.OnCheckpoint = func(rec *checkpoint.Record, _ map[string]uint64) { records = append(records, rec) }
			if _, err := runner.RunAttempt(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
		attempt("s1", false)
		before := records[len(records)-1].Streams["b"]
		if string(before.State) != "2" || !before.Complete {
			t.Fatalf("after the first sync, b is %+v, want state 2 and complete", before)
		}
		attempt("s2", true)
		if read["a"] != 4 || read["b"] != 2 {
			t.Fatalf("with b paused, read %v, want 4 rows of a and 2 of b", read)
		}
		for _, rec := range records {
			got, ok := rec.Streams["b"]
			switch {
			case !ok:
				t.Fatalf("record %d has no b", rec.Epoch)
			case got.Rows != 0 || !bytes.Equal(got.State, before.State) || got.Complete != before.Complete || !bytes.Equal(got.Schema, before.Schema):
				t.Fatalf("record %d carries b as %+v, want %+v without rows", rec.Epoch, got, before)
			}
		}
		attempt("s3", false)
		if read["b"] != 4 {
			t.Fatalf("after b was resumed, read %d rows of it, want 4", read["b"])
		}
		if got := records[len(records)-1].Streams["b"]; string(got.State) != "4" {
			t.Fatalf("after b was resumed, its state is %q, want 4", got.State)
		}
	})
}

// incrementalSource reads pages more pages of each stream each sync, one row and
// a resume point each, waiting for a commit after each, and completes it
// where it stopped, as an incremental read does.
type incrementalSource struct {
	pages     int
	committed <-chan uint64
	read      map[string]int
}

func (s incrementalSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	for _, cs := range req.Streams {
		from := 0
		if cs.State != nil {
			from, _ = strconv.Atoi(string(cs.State))
		}
		at := from
		for ; at < from+s.pages; at++ {
			if err := out.Rows(cs.Name, []json.RawMessage{json.RawMessage(`{}`)}); err != nil {
				return err
			}
			s.read[cs.Name]++
			if err := out.ResumePoint(cs.Name, strconv.AppendInt(nil, int64(at+1), 10)); err != nil {
				return err
			}
			select {
			case <-s.committed:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := out.Complete(cs.Name, strconv.AppendInt(nil, int64(at), 10)); err != nil {
			return err
		}
	}
	return nil
}
