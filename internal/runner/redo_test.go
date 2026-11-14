package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// txnStore is a destination whose writer lanes commit on their own, as
// Kafka's transactions do. A lane's part of an epoch is a transaction open
// in a producer of the lane's writer, under a transactional ID of the lane
// and a slot, which only that producer commits; a commit makes its rows
// visible with a marker of the epoch and lane. Opening a lane initializes
// its IDs, which fences the producers they had and aborts their open
// transactions. Its writers may keep a copy of each transaction, which
// recovery commits when the marker is missing.
type txnStore struct {
	t     *testing.T
	stage bool
	mu    sync.Mutex
	// By transactional ID: its producer's epoch, and its open transaction.
	producers map[txnID]uint64
	open      map[txnID]*txn
	marked    map[laneMark]bool
	staged    map[laneMark][]int64
	// The rows of the transactions an init aborted: a redo writes them
	// again.
	aborted map[laneMark][]int64
	rows    []int64
	redone  map[laneMark]bool
	// Called before a commit applies, by a writer of generation gen; an
	// error fails it. After: once it applied.
	before, after func(gen uint64, m laneMark) error
	// Called as a writer of generation gen and lane settles a committable,
	// and as it writes epoch e; an error fails it.
	settling func(ctx context.Context, gen uint64, lane int) error
	writing  func(ctx context.Context, gen uint64, lane int, e destination.Epoch) error
	// Set: no writer of a later generation writes before every lane of
	// this epoch is settled.
	settleFirst *destination.Epoch
	settleLanes int
}

type txnID struct{ Lane, Slot int }

type laneMark struct {
	Gen, N uint64
	Lane   int
}

type txn struct {
	mark laneMark
	rows []int64
}

// txnSlots are each lane's producers, so a lane writes epoch N+1 while N's
// transaction waits for its commit.
const txnSlots = 3

func newTxnStore(t *testing.T) *txnStore {
	return &txnStore{t: t, producers: map[txnID]uint64{}, open: map[txnID]*txn{}, marked: map[laneMark]bool{},
		staged: map[laneMark][]int64{}, aborted: map[laneMark][]int64{}, redone: map[laneMark]bool{}}
}

// initProducer starts a producer for id; s.mu is held.
func (s *txnStore) initProducer(id txnID) uint64 {
	s.producers[id]++
	if t := s.open[id]; t != nil {
		s.aborted[t.mark] = t.rows
		delete(s.open, id)
	}
	return s.producers[id]
}

type txnDest struct {
	s           *txnStore
	coordinator bool
}

func (d txnDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.TransactionalStream, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: 16, LaneCommits: true}
}

func (d txnDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	if d.coordinator {
		d.s.t.Error("the coordinator opened the destination")
		return nil, errors.New("the coordinator holds no credentials")
	}
	d.s.mu.Lock()
	defer d.s.mu.Unlock()
	w := &txnWriter{s: d.s, lane: cfg.Lane, gen: cfg.Generation, producers: map[int]uint64{}, writing: map[destination.Epoch][]int64{}}
	for slot := range txnSlots {
		w.producers[slot] = d.s.initProducer(txnID{cfg.Lane, slot})
	}
	return w, nil
}

type txnWriter struct {
	s         *txnStore
	lane      int
	gen       uint64
	producers map[int]uint64
	writing   map[destination.Epoch][]int64
}

// txnCommittable is a lane's: its transaction, the producer that holds it,
// and whether a copy was kept.
type txnCommittable struct {
	Lane     int    `json:"lane"`
	Slot     int    `json:"slot"`
	Producer uint64 `json:"producer"`
	Gen      uint64 `json:"gen"`
	N        uint64 `json:"n"`
	Staged   bool   `json:"staged,omitempty"`
}

func (c txnCommittable) mark() laneMark { return laneMark{c.Gen, c.N, c.Lane} }

func (w *txnWriter) Write(ctx context.Context, e destination.Epoch, _ string, b arrow.RecordBatch) error {
	s := w.s
	s.mu.Lock()
	writing := s.writing
	s.mu.Unlock()
	if writing != nil {
		if err := writing(ctx, w.gen, w.lane, e); err != nil {
			return err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if f := s.settleFirst; f != nil && e.Generation > f.Generation {
		for l := range s.settleLanes {
			if !s.marked[laneMark{f.Generation, f.N, l}] {
				s.t.Errorf("generation %d wrote epoch %d before lane %d of epoch %d was settled", e.Generation, e.N, l, f.N)
			}
		}
	}
	w.writing[e] = append(w.writing[e], b.Column(b.Schema().FieldIndices("id")[0]).(*array.Int64).Int64Values()...)
	return nil
}

func (w *txnWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	slot := int(e.N % txnSlots)
	id := txnID{w.lane, slot}
	c := txnCommittable{Lane: w.lane, Slot: slot, Producer: w.producers[slot], Gen: e.Generation, N: e.N, Staged: s.stage}
	if s.producers[id] != c.Producer {
		return nil, destination.ErrFenced
	}
	if t := s.open[id]; t != nil && t.mark != c.mark() {
		return nil, fmt.Errorf("%v has %v open, writing %v", id, t.mark, c.mark())
	}
	rows := w.writing[e]
	delete(w.writing, e)
	s.open[id] = &txn{mark: c.mark(), rows: rows}
	if s.stage {
		s.staged[c.mark()] = slices.Clone(rows)
	}
	return json.Marshal(c)
}

func (w *txnWriter) Commit(_ context.Context, e destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	s := w.s
	if len(staged) != 1 {
		return destination.Committed{}, fmt.Errorf("a commit of %d committables on lane %d", len(staged), w.lane)
	}
	var c txnCommittable
	if err := json.Unmarshal(staged[0], &c); err != nil {
		return destination.Committed{}, err
	}
	if c.Lane != w.lane || c.Gen != e.Generation || c.N != e.N {
		return destination.Committed{}, fmt.Errorf("lane %d's writer asked to commit %+v as epoch %d of generation %d", w.lane, c, e.N, e.Generation)
	}
	s.mu.Lock()
	before, after := s.before, s.after
	s.mu.Unlock()
	if before != nil {
		if err := before(w.gen, c.mark()); err != nil {
			return destination.Committed{}, err
		}
	}
	s.mu.Lock()
	err := s.commit(c, true)
	s.mu.Unlock()
	if err == nil && after != nil {
		err = after(w.gen, c.mark())
	}
	return destination.Committed{}, err
}

// commit commits c's transaction through its producer, a writer's or, from
// a copy, recovery's; s.mu is held.
func (s *txnStore) commit(c txnCommittable, writer bool) error {
	id, m := txnID{c.Lane, c.Slot}, c.mark()
	if s.producers[id] != c.Producer {
		return fmt.Errorf("%w: producer %v", destination.ErrFenced, id)
	}
	t := s.open[id]
	if t == nil || t.mark != m {
		if s.marked[m] {
			return nil
		}
		return fmt.Errorf("%v has no open transaction of %v", id, m)
	}
	delete(s.open, id)
	if lost, ok := s.aborted[m]; ok && writer {
		if !slices.Equal(slices.Sorted(slices.Values(lost)), slices.Sorted(slices.Values(t.rows))) {
			s.t.Errorf("%v's transaction was written again with rows %v, not %v", m, t.rows, lost)
		}
		s.redone[m] = true
	}
	s.rows = append(s.rows, t.rows...)
	s.marked[m] = true
	delete(s.staged, m)
	return nil
}

func (w *txnWriter) Recover(ctx context.Context, _ uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	if len(staged) != 1 {
		return w.recoverAll(staged)
	}
	w.s.mu.Lock()
	settling := w.s.settling
	w.s.mu.Unlock()
	if settling != nil {
		if err := settling(ctx, w.gen, w.lane); err != nil {
			return 0, err
		}
	}
	var c txnCommittable
	if err := json.Unmarshal(staged[0], &c); err != nil {
		return 0, err
	}
	if c.Lane != w.lane {
		return 0, fmt.Errorf("lane %d's committable settled on lane %d's writer", c.Lane, w.lane)
	}
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	pe := s.initProducer(txnID{c.Lane, c.Slot})
	w.producers[c.Slot] = pe
	m := c.mark()
	switch rows, ok := s.staged[m]; {
	case s.marked[m]:
		return destination.Applied, nil
	case ok:
		s.open[txnID{c.Lane, c.Slot}] = &txn{mark: m, rows: rows}
		c.Producer = pe
		return destination.Redone, s.commit(c, false)
	}
	return destination.Lost, nil
}

// recoverAll is a single runner's recovery of every lane's committable:
// applied if every lane's marker is visible, and otherwise lost, which
// rolls the epoch back.
func (w *txnWriter) recoverAll(staged []destination.Staged) (destination.Recovery, error) {
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	out := destination.Applied
	for _, b := range staged {
		var c txnCommittable
		if err := json.Unmarshal(b, &c); err != nil {
			return 0, err
		}
		s.initProducer(txnID{c.Lane, c.Slot})
		if !s.marked[c.mark()] {
			out = destination.Lost
		}
	}
	return out, nil
}

func (w *txnWriter) Close(context.Context) error { return nil }

// check fails unless the destination holds ids 1 to n once each.
func (s *txnStore) check(t *testing.T, n int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	got := slices.Sorted(slices.Values(s.rows))
	if len(got) != n {
		t.Fatalf("%d rows, want %d: %v", len(got), n, got)
	}
	for i, id := range got {
		if id != int64(i+1) {
			t.Fatalf("row %d is id %d: %v", i, id, got)
		}
	}
}

// txnPool is a pool of attempts writing s.
func txnPool(t *testing.T, s *txnStore, src func() *rangeSource) *pool {
	p := newPool(t, src)
	p.dest = func(coordinator bool) destination.Destination { return txnDest{s: s, coordinator: coordinator} }
	return p
}

// failing is a commit hook of s that fails the commits of epoch n of
// generation 1 on the lanes named, by writers of generation by, and runs
// first once before the first. With n 0, the epoch is the second in which a
// transaction of those lanes holds rows, so its splits start mid-read.
func failing(s *txnStore, by, n uint64, lanes []int, first func()) func(uint64, laneMark) error {
	var mu sync.Mutex
	var once sync.Once
	held := map[uint64]bool{}
	return func(gen uint64, m laneMark) error {
		if gen != by || m.Gen != 1 || !slices.Contains(lanes, m.Lane) {
			return nil
		}
		mu.Lock()
		if n == 0 {
			s.mu.Lock()
			t := s.open[txnID{m.Lane, int(m.N % txnSlots)}]
			if t != nil && t.mark == m && len(t.rows) > 0 {
				if held[m.N] = true; len(held) == 2 {
					n = m.N
				}
			}
			s.mu.Unlock()
		}
		at := n
		mu.Unlock()
		if m.N != at {
			return nil
		}
		once.Do(first)
		return fmt.Errorf("lane %d's commit of epoch %d failed", m.Lane, at)
	}
}

// lose runs attempt a1, of generation 1, on n1 workers until an epoch's
// commit fails as hook has it, and returns the record it leaves HEAD at. A
// hook that fails with errCoordinatorLost loses the coordinator too.
func (p *pool) lose(t *testing.T, s *txnStore, n1 int, hook func(uint64, laneMark) error) *checkpoint.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	j := p.workers("a1", n1)
	cctx, stop := context.WithCancel(ctx)
	defer stop()
	j.Workers.Server = listen(t)
	srv := j.Workers.Server
	var once sync.Once
	s.mu.Lock()
	s.before = func(gen uint64, m laneMark) error {
		err := hook(gen, m)
		if errors.Is(err, errCoordinatorLost) {
			once.Do(func() {
				go func() {
					srv.Close()
					stop()
				}()
			})
		}
		return err
	}
	s.mu.Unlock()
	if _, err := runner.RunAttempt(cctx, j); err == nil {
		t.Fatal("the attempt went on")
	}
	p.wait("a1")
	s.mu.Lock()
	s.before = nil
	s.mu.Unlock()
	head := p.head()
	if head.Generation != 1 || len(head.Coverage) != len(head.Committables) {
		t.Fatalf("HEAD is epoch %d of generation %d, with %d coverages of %d committables", head.Epoch, head.Generation, len(head.Coverage), len(head.Committables))
	}
	// A redo stops where the epoch did, short of the read's end.
	if slices.ContainsFunc(head.Splits("s"), func(sp checkpoint.SplitState) bool { return sp.Complete }) {
		t.Fatalf("epoch %d completed a split: %+v", head.Epoch, head.Splits("s"))
	}
	return head
}

var errCoordinatorLost = errors.New("the coordinator is gone")

// settle runs attempt owner on n workers, or on one runner with n 0, after
// head, and checks it settled head's epoch before anything else, redoing
// the lanes that weren't committed from the source, as they were.
func (p *pool) settle(t *testing.T, s *txnStore, owner string, n int, head *checkpoint.Record) runner.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	s.mu.Lock()
	missing := 0
	for l := range head.Committables {
		if !s.marked[laneMark{head.Generation, head.Epoch, l}] {
			missing++
		}
	}
	s.settleFirst, s.settleLanes = &destination.Epoch{Generation: head.Generation, N: head.Epoch}, len(head.Committables)
	s.mu.Unlock()
	j := p.job(owner, p.source(), txnDest{s: s})
	if n > 0 {
		j = p.workers(owner, n)
	}
	res, err := runner.RunAttempt(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if n > 0 {
		for i, err := range p.wait(owner) {
			if err != nil {
				t.Errorf("worker %d: %v", i, err)
			}
		}
	}
	if res.Restored != head.Epoch || missing > 0 && res.Recovery != destination.Redone {
		t.Errorf("recovered %s, restoring %d, with %d lanes of epoch %d uncommitted", res.Recovery, res.Restored, missing, head.Epoch)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	redone := 0
	for l := range head.Committables {
		m := laneMark{head.Generation, head.Epoch, l}
		if !s.marked[m] {
			t.Errorf("lane %d of epoch %d is uncommitted", l, head.Epoch)
		}
		if s.redone[m] {
			redone++
		}
	}
	if redone < missing {
		t.Errorf("%d of epoch %d's %d uncommitted lanes were redone", redone, head.Epoch, missing)
	}
	return res
}

// Rows of the splits and key groups a committable holds, read again from
// where its part of the epoch began: every lane's covered splits were read
// from there.
func (p *pool) readAgain(t *testing.T, owner string, head *checkpoint.Record, lanes []int) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	var from []string
	for _, src := range p.sources[owner] {
		src.mu.Lock()
		from = append(from, src.from...)
		src.mu.Unlock()
	}
	for _, l := range lanes {
		for _, c := range head.Coverage[l].Splits {
			if want := c.ID + "@" + string(c.From); !slices.Contains(from, want) {
				t.Errorf("lane %d's split %s wasn't read again from %q: reads %v", l, c.ID, c.From, from)
			}
		}
	}
}

// A writer lane lost between the record and its commit, or the
// coordinator, or both, leaves an epoch committed in part. The next
// attempt's workers settle each lane before anything is assigned: the
// committed by their markers, and the others by writing them again from the
// source, as they were.
func TestARedoFromTheSource(t *testing.T) {
	coordinatorToo := func(f func(uint64, laneMark) error) func(uint64, laneMark) error {
		return func(gen uint64, m laneMark) error {
			if err := f(gen, m); err != nil {
				return fmt.Errorf("%w: %w", errCoordinatorLost, err)
			}
			return nil
		}
	}
	for name, c := range map[string]struct {
		hook func(*pool, *txnStore) func(uint64, laneMark) error
		// Lanes not committed, if they hold rows: worker 0's are 0 and 1.
		lost []int
	}{
		"a worker lost": {hook: func(p *pool, s *txnStore) func(uint64, laneMark) error {
			return failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) })
		}, lost: []int{0, 1}},
		"the coordinator lost": {hook: func(_ *pool, s *txnStore) func(uint64, laneMark) error {
			return coordinatorToo(failing(s, 1, 0, []int{0, 1, 2, 3, 4, 5}, func() {}))
		}, lost: []int{0, 1, 2, 3, 4, 5}},
		"both": {hook: func(p *pool, s *txnStore) func(uint64, laneMark) error {
			return coordinatorToo(failing(s, 1, 0, []int{0, 1}, func() { p.kill("a1", 0) }))
		}, lost: []int{0, 1}},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			head := p.lose(t, s, 3, c.hook(p, s))
			p.settle(t, s, "a2", 2, head)
			p.readAgain(t, "a2", head, c.lost)
			s.check(t, 600)
		})
	}
}

// A redo that fails is done again by the next attempt, or found committed.
func TestARedoRepeated(t *testing.T) {
	for _, committed := range []bool{false, true} {
		t.Run(fmt.Sprintf("committed=%v", committed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			head := p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			// Attempt a2 redoes lane 0 on its worker 0, which is lost at
			// the redo's commit, before or after it applies.
			hook := failing(s, 2, head.Epoch, []int{0}, func() { p.kill("a2", 0) })
			s.mu.Lock()
			if committed {
				s.after = hook
			} else {
				s.before = hook
			}
			s.mu.Unlock()
			if _, err := runner.RunAttempt(ctx, p.workers("a2", 2)); err == nil {
				t.Fatal("the attempt went on without its worker")
			}
			p.wait("a2")
			s.mu.Lock()
			s.before, s.after = nil, nil
			marked := s.marked[laneMark{1, head.Epoch, 0}]
			s.mu.Unlock()
			if marked != committed {
				t.Fatalf("lane 0 of epoch %d committed: %v", head.Epoch, marked)
			}
			if h := p.head(); h.Epoch != head.Epoch || h.Generation != 1 {
				t.Fatalf("HEAD moved to epoch %d of generation %d", h.Epoch, h.Generation)
			}
			p.settle(t, s, "a3", 1, head)
			s.check(t, 600)
		})
	}
}

// A redo reads what the committable holds, whatever the layout that wrote
// it and the one that redoes it: one worker's lanes on several, several
// workers' on one, and on one runner; and of a keyed stream routed by key
// group, the rows of the lane's key groups.
func TestARedoOnAnyLayout(t *testing.T) {
	for name, c := range map[string]struct {
		before, after int
		keyed         bool
		lanes         []int
	}{
		"one worker onto three": {before: 1, after: 3, lanes: []int{1}},
		"three onto one":        {before: 3, after: 1, lanes: []int{0}},
		"three onto one runner": {before: 3, after: 0, lanes: []int{0}},
		"keyed, onto two":       {before: 3, after: 2, keyed: true, lanes: []int{0}},
		"keyed, onto one":       {before: 3, after: 0, keyed: true, lanes: []int{0}},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			if c.keyed {
				p.change = func(j *runner.Job) { j.Streams[0].PrimaryKey = []string{"id"} }
			}
			head := p.lose(t, s, c.before, failing(s, 1, 0, c.lanes, func() { p.kill("a1", 0) }))
			if c.keyed {
				for i, cov := range head.Coverage {
					for _, sp := range cov.Splits {
						if sp.KeyGroups == nil {
							t.Fatalf("committable %d holds all of split %s's rows", i, sp.ID)
						}
					}
				}
			}
			p.settle(t, s, "a2", c.after, head)
			if c.after > 0 {
				p.readAgain(t, "a2", head, c.lanes)
			}
			s.check(t, 600)
		})
	}
}

// A keyed stream's lane, its rows grouped by the key of the attempt that
// wrote it, isn't written again under another key: the attempt fails with
// CONFIG_INVALID, saying what changed, and commits nothing of the lane.
func TestARedoUnderAnotherKeyIsRefused(t *testing.T) {
	for _, n := range []int{0, 2} {
		t.Run(fmt.Sprintf("workers=%d", n), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			p.change = func(j *runner.Job) { j.Streams[0].PrimaryKey = []string{"id"} }
			head := p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			// A key of the same column hashes its rows to other groups.
			p.change = func(j *runner.Job) { j.Streams[0].PrimaryKey = []string{"id", "id"} }
			j := p.job("a2", p.source(), txnDest{s: s})
			if n > 0 {
				j = p.workers("a2", n)
			}
			_, err := runner.RunAttempt(ctx, j)
			if n > 0 {
				p.wait("a2")
			}
			if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(fmt.Sprint(err), "(id) until the epoch is settled") {
				t.Fatalf("the attempt ended with %v (%s)", err, errs.CodeOf(err))
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.marked[laneMark{head.Generation, head.Epoch, 0}] {
				t.Error("lane 0 was committed")
			}
		})
	}
}

// A worker settling when the attempt fails stops: its redo, which would
// otherwise go on to its end, ends with the attempt.
func TestASettlingWorkerStopsWithTheAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	s := newTxnStore(t)
	p := txnPool(t, s, ranges(6, 100, 5))
	p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
	// Worker 0's redo of lane 0 waits until it's stopped; worker 1 is
	// lost meanwhile, which fails the attempt.
	var once sync.Once
	s.mu.Lock()
	s.writing = func(ctx context.Context, gen uint64, lane int, e destination.Epoch) error {
		if gen != 2 || lane != 0 || e.Generation != 1 {
			return nil
		}
		once.Do(func() { p.kill("a2", 1) })
		<-ctx.Done()
		return ctx.Err()
	}
	s.mu.Unlock()
	if _, err := runner.RunAttempt(ctx, p.workers("a2", 2)); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.mu.Lock()
	done, n := p.done["a2"], len(p.cancels["a2"])
	p.mu.Unlock()
	for range n {
		select {
		case err := <-done:
			if err == nil {
				t.Error("a worker finished an attempt that failed")
			}
		case <-time.After(20 * time.Second):
			t.Fatal("the settling worker went on after the attempt failed")
		}
	}
}

// A source whose read ends before the position the record has fails the
// redo, and the attempt, with POSITION_LOST, and commits nothing of the
// lane.
func TestARedoShortOfItsPosition(t *testing.T) {
	for _, n := range []int{0, 2} {
		t.Run(fmt.Sprintf("workers=%d", n), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			head := p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			// The source now holds fewer rows than the epoch read.
			p.source = ranges(6, 1, 5)
			j := p.job("a2", p.source(), txnDest{s: s})
			if n > 0 {
				j = p.workers("a2", n)
			}
			_, err := runner.RunAttempt(ctx, j)
			if n > 0 {
				p.wait("a2")
			}
			if errs.CodeOf(err) != errs.PositionLost {
				t.Fatalf("the attempt ended with %v", err)
			}
			s.mu.Lock()
			defer s.mu.Unlock()
			if s.marked[laneMark{head.Generation, head.Epoch, 0}] {
				t.Error("lane 0 was committed")
			}
		})
	}
}

// A source whose read from a position doesn't meet the resume points of
// the read before it can still be read again exactly: the redo asks it to
// end at the epoch's position. One that ignores that fails the redo with
// POSITION_LOST.
func TestARedoAsksTheSourceWhereToEnd(t *testing.T) {
	for _, until := range []bool{true, false} {
		t.Run(fmt.Sprintf("until=%v", until), func(t *testing.T) {
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			head := p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			p.source = func() *rangeSource {
				src := ranges(6, 100, 5)()
				src.shift, src.until = true, until
				return src
			}
			if until {
				p.settle(t, s, "a2", 2, head)
				s.check(t, 600)
				return
			}
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			_, err := runner.RunAttempt(ctx, p.workers("a2", 2))
			p.wait("a2")
			if errs.CodeOf(err) != errs.PositionLost {
				t.Fatalf("the attempt ended with %v", err)
			}
		})
	}
}

// A worker stopped while it settles a committable, or redoes one, fails the
// attempt as transient, as one stopped in any other call does.
func TestAWorkerStoppedWhileItSettlesFailsTheAttemptAsTransient(t *testing.T) {
	for _, redoing := range []bool{false, true} {
		t.Run(fmt.Sprintf("redoing=%v", redoing), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			s := newTxnStore(t)
			p := txnPool(t, s, ranges(6, 100, 5))
			p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
			// Attempt a2's worker 0 settles lane 0 and is stopped there; the
			// other worker's lanes go on.
			var once sync.Once
			stop := func(ctx context.Context) error {
				once.Do(func() { p.kill("a2", 0) })
				<-ctx.Done()
				return ctx.Err()
			}
			s.mu.Lock()
			if redoing {
				s.writing = func(ctx context.Context, gen uint64, lane int, e destination.Epoch) error {
					if gen == 2 && lane == 0 && e.Generation == 1 {
						return stop(ctx)
					}
					return nil
				}
			} else {
				s.settling = func(ctx context.Context, gen uint64, lane int) error {
					if gen == 2 && lane == 0 {
						return stop(ctx)
					}
					return nil
				}
			}
			s.mu.Unlock()
			_, err := runner.RunAttempt(ctx, p.workers("a2", 2))
			if errs.CodeOf(err) != errs.Transient || !strings.Contains(fmt.Sprint(err), "stopped") {
				t.Fatalf("the attempt failed with %v (%s), want the stopped worker's transient", err, errs.CodeOf(err))
			}
			p.wait("a2")
		})
	}
}

// Writers that keep a copy of each lane's transaction have the lanes not
// committed settled from it, without reading the source again.
func TestALaneSettledFromItsStaging(t *testing.T) {
	s := newTxnStore(t)
	s.stage = true
	p := txnPool(t, s, ranges(6, 100, 5))
	head := p.lose(t, s, 3, failing(s, 1, 0, []int{0}, func() { p.kill("a1", 0) }))
	res, err := runner.RunAttempt(t.Context(), p.workers("a2", 2))
	if err != nil {
		t.Fatal(err)
	}
	p.wait("a2")
	if res.Recovery != destination.Redone || res.Restored != head.Epoch {
		t.Errorf("recovered %s, restoring %d", res.Recovery, res.Restored)
	}
	s.mu.Lock()
	redone := len(s.redone)
	s.mu.Unlock()
	if redone > 0 {
		t.Errorf("%d lanes were read again from the source", redone)
	}
	s.check(t, 600)
}

// Each worker commits its own writer lanes' transactions, which the
// coordinator never touches.
func TestLanesCommitOnTheirWorkers(t *testing.T) {
	s := newTxnStore(t)
	p := txnPool(t, s, ranges(6, 20, 5))
	res, err := runner.RunAttempt(t.Context(), p.workers("a1", 3))
	if err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait("a1") {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	if res.Committed < 2 {
		t.Errorf("%d epochs committed", res.Committed)
	}
	s.check(t, 120)
}
