package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// slotLog is a source's one log of changes for streams a and b, read with
// CDC, as a Postgres replication slot is: a read holds it from its start,
// which takes a while, to its end, and another read of it meanwhile fails.
// Stream a's rows are ids 1 to rows and b's the next rows, a page of each in
// turn, each page followed by a resume point at its last id. A read stops a
// stream at its Until.
type slotLog struct {
	rows, page int
	mu         sync.Mutex
	held       bool
	// Reads refused while another held the log.
	refused int
	// Each read's streams, as "stream@state" where it began.
	reads [][]string
}

type slotReader struct{ log *slotLog }

func (r slotReader) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	l := r.log
	l.mu.Lock()
	if l.held {
		l.refused++
		l.mu.Unlock()
		return errs.New(errs.Transient, "the slot is held by another read")
	}
	l.held = true
	var began []string
	for _, cs := range req.Streams {
		began = append(began, cs.Name+"@"+string(cs.State))
	}
	l.reads = append(l.reads, began)
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		l.held = false
		l.mu.Unlock()
	}()
	type read struct {
		stream    string
		next, end int
		done      bool
	}
	var reads []*read
	for _, cs := range req.Streams {
		first := 1
		if cs.Name == "b" {
			first += l.rows
		}
		rd := &read{stream: cs.Name, next: first, end: first + l.rows - 1}
		if cs.State != nil {
			last, err := strconv.Atoi(string(cs.State))
			if err != nil {
				return err
			}
			rd.next = last + 1
		}
		if cs.Until != nil {
			u, err := strconv.Atoi(string(cs.Until))
			if err != nil {
				return err
			}
			rd.end = min(rd.end, u)
		}
		reads = append(reads, rd)
	}
	// Taking the slot, as a replication connection's start does.
	pause := 10 * time.Millisecond
	for {
		select {
		case <-time.After(pause):
		case <-ctx.Done():
			return ctx.Err()
		}
		pause = 2 * time.Millisecond
		more := false
		for _, rd := range reads {
			if rd.done {
				continue
			}
			if rd.next > rd.end {
				if err := out.Complete(rd.stream, []byte(strconv.Itoa(rd.end))); err != nil {
					return err
				}
				rd.done = true
				continue
			}
			more = true
			var rows []json.RawMessage
			for ; len(rows) < l.page && rd.next <= rd.end; rd.next++ {
				rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id":%d}`, rd.next)))
			}
			if err := out.Rows(rd.stream, rows); err != nil {
				return err
			}
			if err := out.ResumePoint(rd.stream, []byte(strconv.Itoa(rd.next-1))); err != nil {
				return err
			}
		}
		if !more {
			return nil
		}
	}
}

// A lost writer lane that holds rows of streams read with CDC is read again
// from the log their changes come from, which one read holds: one stream
// after the other, whether one lane wrote both or a lane each, so that no
// read of the redo finds the log held by another and fails the attempt.
func TestARedoReadsTheChangeLogOneReadAtATime(t *testing.T) {
	for name, c := range map[string]struct {
		// Lanes of the attempt that loses the epoch, and of the one that
		// redoes it.
		before, after int
		workers       int
		lost          []int
	}{
		"one lane wrote both streams": {before: 1, after: 2, workers: 1, lost: []int{0}},
		"a lane each":                 {before: 2, after: 2, workers: 3, lost: []int{0, 1}},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(1, 1, 1))
			log := &slotLog{rows: 100, page: 5}
			p.reader = func() source.Reader { return slotReader{log} }
			lanes := c.before
			p.change = func(j *runner.Job) {
				j.Lanes = lanes
				j.Streams = []runner.Stream{
					{Name: "a", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
					{Name: "b", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
				}
			}
			head := p.loseEpoch(t, s, c.workers, failing(s, 1, 0, c.lost, func() { p.kill("a1", 0) }))
			for _, name := range []string{"a", "b"} {
				if head.Streams[name].Complete {
					t.Fatalf("epoch %d completed stream %s", head.Epoch, name)
				}
			}
			// The redo needs both streams' rows read again, by one lane's
			// read or by two lanes' on two workers.
			covered := map[string]bool{}
			for _, l := range c.lost {
				for _, sp := range head.Coverage[l].Splits {
					covered[sp.Stream] = true
				}
				if both := len(head.Coverage[l].Splits) == 2; both != (len(c.lost) == 1) {
					t.Fatalf("lane %d of epoch %d holds rows of %+v", l, head.Epoch, head.Coverage[l].Splits)
				}
			}
			if !covered["a"] || !covered["b"] {
				t.Fatalf("lanes %v of epoch %d hold rows of %v", c.lost, head.Epoch, covered)
			}
			log.mu.Lock()
			reads := len(log.reads)
			log.mu.Unlock()

			lanes = c.after
			p.settle(t, s, "a2", 2, head)
			s.check(t, 200)
			heldRows(t, s, head, c.lost)
			log.mu.Lock()
			defer log.mu.Unlock()
			if log.refused > 0 {
				t.Errorf("%d reads found the log held by another", log.refused)
			}
			// Each stream read again on its own, from where the epoch's part
			// of it began, then the attempt's read of both.
			var again []string
			for _, r := range log.reads[reads:] {
				if len(r) == 1 {
					again = append(again, r[0])
				}
			}
			for _, l := range c.lost {
				for _, sp := range head.Coverage[l].Splits {
					if want := sp.Stream + "@" + string(sp.From); !slices.Contains(again, want) {
						t.Errorf("stream %s wasn't read again from %q: %v", sp.Stream, sp.From, again)
					}
				}
			}
		})
	}
}
