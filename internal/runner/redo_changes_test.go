package runner_test

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// slotLog is a source's one log of changes for streams a and b, read with
// CDC, as a Postgres replication slot is: a read holds it from its start,
// which takes a while, to its end, and another read of it meanwhile fails.
// It gives the log in rounds: round r is a page of a's rows, then a page of
// b's, each followed by a resume point, its state the next round. Stream
// a's rows are ids 1 to rounds*page, and b's the next ones. A read stops a
// stream at its Until.
//
// Round r waits until a record committed, or restored, has both streams at
// round r or past it. b's page ends with a row padded to pad bytes, which
// takes the read past the job's CommitMaxBytes and asks for the epoch's
// barrier, placed at the end of the round. So each round is an epoch that
// holds both streams' rows.
type slotLog struct {
	rounds, page, pad int
	mu                sync.Mutex
	held              bool
	// Reads refused while another held the log.
	refused int
	// Each read's streams, as "stream@state" where it began.
	reads [][]string
	// The round both streams are at in the last record taken, and closed and
	// made again as it moves.
	at    int
	moved chan struct{}
}

// slotPad is the job's CommitMaxBytes, which only b's padded rows reach.
const slotPad = 4 << 10

func newSlotLog(rounds, page int) *slotLog {
	return &slotLog{rounds: rounds, page: page, pad: slotPad, moved: make(chan struct{})}
}

// committed takes a record an attempt committed, or restores.
func (l *slotLog) committed(rec *checkpoint.Record) {
	at := l.rounds
	for _, name := range []string{"a", "b"} {
		n := 0
		if st := rec.Streams[name].State; st != nil {
			var err error
			if n, err = strconv.Atoi(string(st)); err != nil {
				return
			}
		}
		at = min(at, n)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if at > l.at {
		l.at = at
		close(l.moved)
		l.moved = make(chan struct{})
	}
}

// wait waits until round r may be read.
func (l *slotLog) wait(ctx context.Context, r int) error {
	for {
		l.mu.Lock()
		ok, moved := l.at >= r, l.moved
		l.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-moved:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// rows are stream's page of round r.
func (l *slotLog) rows(stream string, r int) []json.RawMessage {
	first := r*l.page + 1
	if stream == "b" {
		first += l.rounds * l.page
	}
	rows := make([]json.RawMessage, l.page)
	for i := range rows {
		rows[i] = json.RawMessage(fmt.Sprintf(`{"id":%d}`, first+i))
	}
	if stream == "b" {
		last := string(rows[len(rows)-1])
		rows[len(rows)-1] = json.RawMessage(last[:len(last)-1] + strings.Repeat(" ", max(l.pad-len(last), 0)) + "}")
	}
	return rows
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
	}
	var reads []*read
	lo, hi := l.rounds, 0
	for _, cs := range req.Streams {
		rd := &read{stream: cs.Name, end: l.rounds}
		var err error
		if cs.State != nil {
			if rd.next, err = strconv.Atoi(string(cs.State)); err != nil {
				return err
			}
		}
		if cs.Until != nil {
			if rd.end, err = strconv.Atoi(string(cs.Until)); err != nil {
				return err
			}
		}
		reads = append(reads, rd)
		lo, hi = min(lo, rd.next), max(hi, rd.end)
	}
	slices.SortFunc(reads, func(x, y *read) int { return strings.Compare(x.stream, y.stream) })
	// Taking the slot, as a replication connection's start does, so that two
	// reads started together hold it at once.
	select {
	case <-time.After(10 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}
	for round := lo; round < hi; round++ {
		if err := l.wait(ctx, round); err != nil {
			return err
		}
		for _, rd := range reads {
			if rd.next != round || round >= rd.end {
				continue
			}
			if err := out.Rows(rd.stream, l.rows(rd.stream, round)); err != nil {
				return err
			}
			if err := out.ResumePoint(rd.stream, []byte(strconv.Itoa(round+1))); err != nil {
				return err
			}
			rd.next++
		}
	}
	for _, rd := range reads {
		if rd.end == l.rounds {
			if err := out.Complete(rd.stream, []byte(strconv.Itoa(l.rounds))); err != nil {
				return err
			}
		}
	}
	return nil
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
			log := newSlotLog(8, 5)
			p.reader = func() source.Reader { return slotReader{log} }
			lanes := c.before
			p.change = func(j *runner.Job) {
				j.Lanes = lanes
				j.Streams = []runner.Stream{
					{Name: "a", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
					{Name: "b", Replay: source.Exact, Read: source.CDC, Mode: destination.Append},
				}
				// Only b's padded rows ask for barriers.
				j.Settings.CommitMaxBytes, j.Settings.CommitMaxBytesSet = slotPad, true
				j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) { log.committed(rec) }
			}
			// The second epoch whose lanes hold rows, round 1's, is lost.
			head := p.loseEpoch(t, s, c.workers, failing(s, 1, 0, c.lost, func() { p.kill("a1", 0) }))
			for _, name := range []string{"a", "b"} {
				if st := head.Streams[name]; st.Complete || string(st.State) != "2" {
					t.Fatalf("epoch %d left stream %s at %q, complete %v", head.Epoch, name, st.State, st.Complete)
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
			log.committed(head)
			p.settle(t, s, "a2", 2, head)
			s.check(t, 2*8*5)
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
