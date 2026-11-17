package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	partition "github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// regions are what timedChunks' rows are keyed by, in turn: enough for
// every writer lane to own some.
var regions = func() []string {
	out := make([]string, 24)
	for i := range out {
		out[i] = fmt.Sprintf("r%d", i)
	}
	return out
}()

// timedChunks reads stream s in splits, a chunk of rows at a time with a
// resume point after each, its state the next chunk. Split k's chunk c
// holds rows ids (k*chunks+c)*rows+r at minutes+c seconds, a millisecond
// apart, of region regions[(k+r)%24], so no row is behind its split's
// watermark. With upTo, a split has only that many chunks so far, and keeps
// its read open after them until it's cancelled; it waits pause between
// chunks. With behind, the first row of every third chunk is 15 seconds
// behind its split's watermark, and late where its window has closed.
type timedChunks struct {
	splits, chunks, rows, upTo int
	pause                      time.Duration
	behind                     bool
}

func (s *timedChunks) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, s.splits)
	for i := range out {
		out[i] = source.Split{ID: strconv.Itoa(i)}
	}
	return out, nil
}

func (s *timedChunks) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		k, err := strconv.Atoi(sp.ID)
		if err != nil {
			return err
		}
		from := 0
		if sp.State != nil {
			if from, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
		}
		upTo := s.chunks
		if s.upTo > 0 {
			upTo = min(s.upTo, s.chunks)
		}
		for c := from; c < upTo; c++ {
			rows := make([]json.RawMessage, s.rows)
			for r := range rows {
				us := minutes + int64(c)*1_000_000 + int64(r)*1000
				if s.behind && r == 0 && c%3 == 2 {
					us -= 15_000_000
				}
				rows[r] = fmt.Appendf(nil, `{"id": %d, "region": %q, "at": %q}`, (k*s.chunks+c)*s.rows+r, regions[(k+r)%len(regions)], at(us))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(c + 1))); err != nil {
				return err
			}
			select {
			case <-time.After(s.pause):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if upTo < s.chunks {
			<-ctx.Done()
			return ctx.Err()
		}
		return e.Complete([]byte(strconv.Itoa(s.chunks)))
	})
}

// tens counts each region's rows ten seconds at a time.
const tens = "INSERT INTO tens SELECT window_start, region, count(*) AS n FROM s GROUP BY tumble(at, INTERVAL '10 seconds'), region"

// windows are the rows of tens a read of all of s's chunks gives: each
// window's count, by its start as the destination shows it and its region.
func (s *timedChunks) windows() map[string]int {
	out := map[string]int{}
	for k := range s.splits {
		for c := range s.chunks {
			for r := range s.rows {
				out[windowStart(int64(c/10*10))+"/"+regions[(k+r)%len(regions)]]++
			}
		}
	}
	return out
}

// tensJob is a job of tens over s, planned by the engine, its output keyed
// by its window's start and region and written in mode, with its state
// below stateURL.
func tensJob(ctx context.Context, t *testing.T, mode destination.WriteMode, stateURL string) runner.Job {
	t.Helper()
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	raw, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: tens, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: raw, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator
	if !slices.Equal(exp[0].Need, []string{"region"}) {
		t.Fatalf("the window is exchanged by %v", exp[0].Need)
	}
	// Exchanged by region before the window, which its key holds: no
	// exchange before the writer.
	out := runner.Stream{Name: "tens", Replay: source.Exact, Mode: mode, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s",
		Partitioning: partition.Place(partition.Partitioning{}, partition.StatementOperator(a.Statements[0])).Out()}
	return runner.Job{
		Pipeline: "p", Sync: "s1",
		Streams: []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, Schema: declared,
			EventTime: &runner.EventTime{Column: "at", Idle: time.Hour}}},
		Outputs:  []runner.Stream{out},
		SQL:      &runner.SQLTransform{SQL: tens, Expected: exp, Operators: []string{exp[0].Operator}},
		StateURL: stateURL,
		Lanes:    2,
	}
}

// landedOnce checks that each window landed once, with its count, of rows
// as "start/region/n".
func landedOnce(t *testing.T, rows []string, want map[string]int) {
	t.Helper()
	got := map[string]int{}
	for _, r := range rows {
		i := len(r) - 1
		for r[i] != '/' {
			i--
		}
		if _, ok := got[r[:i]]; ok {
			t.Fatalf("window %s landed twice", r[:i])
		}
		n, err := strconv.Atoi(r[i+1:])
		if err != nil {
			t.Fatal(err)
		}
		got[r[:i]] = n
	}
	for w, n := range want {
		if got[w] != n {
			t.Errorf("window %s counted %d rows, want %d", w, got[w], n)
		}
	}
	if len(got) != len(want) {
		t.Errorf("%d windows landed, want %d", len(got), len(want))
	}
}

// windowNumbers checks the window's numbers in each record as its
// coordinator commits it, its runs below dir.
type windowNumbers struct {
	t   *testing.T
	dir string
	mu  sync.Mutex
	ops []checkpoint.Operator
}

// watch checks each record j commits: the rows the window took in are
// those the source emitted in the epoch, on every worker; the windows it
// keeps never fall below 0; and its stored bytes are those of the runs the
// record names, each once, though lanes on several workers name it.
func (w *windowNumbers) watch(j *runner.Job) {
	then := j.OnCheckpoint
	j.OnCheckpoint = func(rec *checkpoint.Record, collapsed, held map[string]uint64) {
		w.check(rec)
		if then != nil {
			then(rec, collapsed, held)
		}
	}
}

func (w *windowNumbers) check(rec *checkpoint.Record) {
	if len(rec.Operators) != 1 {
		w.t.Errorf("epoch %d's record has the numbers of %d operators", rec.Epoch, len(rec.Operators))
		return
	}
	op := rec.Operators[0]
	if rows := rec.Streams["s"].Rows; op.RowsIn != rows {
		w.t.Errorf("epoch %d: the window took in %d rows, of %d the source emitted", rec.Epoch, op.RowsIn, rows)
	}
	if op.Windows < 0 {
		w.t.Errorf("epoch %d: the window keeps %d windows", rec.Epoch, op.Windows)
	}
	named := map[string]bool{}
	var stored uint64
	for _, sn := range rec.Snapshots {
		for _, r := range sn.Runs {
			if named[r] {
				continue
			}
			named[r] = true
			fi, err := os.Stat(filepath.Join(w.dir, r))
			if err != nil {
				w.t.Errorf("epoch %d: run %s: %v", rec.Epoch, r, err)
				continue
			}
			stored += uint64(fi.Size())
		}
	}
	if op.StoredBytes != stored {
		w.t.Errorf("epoch %d: the window stores %d bytes; its record's runs are %d", rec.Epoch, op.StoredBytes, stored)
	}
	w.mu.Lock()
	w.ops = append(w.ops, op)
	w.mu.Unlock()
}

// A group window runs on workers, each with an engine of its own: every
// worker's lanes send each row to the keyed lane owning its region's key
// group, on whichever worker, whose windows feed its own writer lane. A
// worker lost after the first commit fails the attempt; the next, on two
// workers, then one runner, restore the windows' state from the record on
// their own lanes, and each window lands once, with every row of it. Each
// record's numbers of the window add up every worker's: the rows it took
// in, the windows it wrote, once each, and the windows it keeps, counted on
// from the record each attempt restored, whatever its workers, to none at
// the end of input.
func TestAGroupWindowOnWorkersThroughTheEngine(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := newKeyDest(16)
	state := t.TempDir()
	numbers := &windowNumbers{t: t, dir: state}
	base := tensJob(ctx, t, destination.Merge, "file://"+state)
	all := &timedChunks{splits: 4, chunks: 60, rows: 24}
	job := func(owner string, upTo int) func() runner.Job {
		return func() runner.Job {
			j := base
			j.Owner, j.Log, j.Destination, j.Dataflow, j.Logger = owner, log, dest, &Dataflow{Options: opts}, opts.Logger.With("job", owner)
			j.Source = &timedChunks{splits: all.splits, chunks: all.chunks, rows: all.rows, upTo: upTo, pause: 2 * time.Millisecond}
			j.Settings = delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 4 << 10, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline}
			return j
		}
	}
	srv := listen(t)

	// Three workers, one of which is lost after the first commit.
	p := &inProcess{ctx: ctx, mk: job("a1", 20)}
	j := p.mk()
	p.on(&j, srv, 3)
	numbers.watch(&j)
	var once sync.Once
	j.OnCommit = func(uint64) { once.Do(func() { p.kill(1) }) }
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait()

	// Two workers, with more to read, drained once some windows landed.
	p.mk = job("a2", 40)
	j = p.mk()
	p.on(&j, srv, 2)
	drainOnceRecorded(&j, func(*checkpoint.Record) bool { return len(dest.applied("tens")) > 0 })
	numbers.watch(&j)
	res, err := runner.RunAttempt(ctx, j)
	if !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("two workers: %v", err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d of two: %v", i, err)
		}
	}
	if res.Restored == 0 || res.Committed == 0 {
		t.Fatalf("two workers restored %d and committed %d", res.Restored, res.Committed)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	st := head.Streams["s"]
	if st.Floor == nil || st.Closed == nil || *st.Closed < *st.Floor || len(head.Snapshots) == 0 {
		t.Fatalf("the record's window marks %v and %v, and %d snapshots", st.Floor, st.Closed, len(head.Snapshots))
	}

	// One runner, to the end.
	j = job("a3", 0)()
	numbers.watch(&j)
	last, err := runner.RunAttempt(ctx, j)
	if err != nil {
		t.Fatal(err)
	}
	if last.Restored != res.Last {
		t.Errorf("one runner restored %d, after %d", last.Restored, res.Last)
	}
	var rows []string
	for _, r := range dest.applied("tens") {
		rows = append(rows, r.cols["window_start"]+"/"+r.cols["region"]+"/"+r.cols["n"])
	}
	want := all.windows()
	landedOnce(t, rows, want)
	var written uint64
	for _, op := range numbers.ops {
		written += op.Inserts + op.Updates
	}
	if n := len(numbers.ops); n == 0 || written != uint64(len(want)) || numbers.ops[n-1].Windows != 0 {
		t.Errorf("%d records' numbers wrote %d windows' rows, of %d, and keep %v at the end", n, written, len(want), numbers.ops)
	}
}

// txnRows is txnLanes for rows of any shape, as render shows each: a
// lane's part of an epoch is a transaction its writer holds, which opening
// the lane again aborts, and a commit makes visible with the lane's marker.
type txnRows struct {
	t      *testing.T
	render func(b arrow.RecordBatch) []string
	mu     sync.Mutex
	// By lane, its producer and its open transactions.
	producers map[int]uint64
	open      map[int]map[txnMark][]string
	aborted   map[txnMark][]string
	marked    map[txnMark]bool
	rows      []string
	redone    int
	// Called before a commit applies, by a writer of generation gen.
	before func(gen uint64, m txnMark, rows int) error
}

func newTxnRows(t *testing.T, render func(b arrow.RecordBatch) []string) *txnRows {
	return &txnRows{t: t, render: render, producers: map[int]uint64{}, open: map[int]map[txnMark][]string{}, aborted: map[txnMark][]string{}, marked: map[txnMark]bool{}}
}

func (d *txnRows) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.TransactionalStream, Scopes: []destination.Scope{destination.ScopePipeline}, WriterLanes: 16, LaneCommits: true}
}

func (d *txnRows) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.producers[cfg.Lane]++
	for m, rows := range d.open[cfg.Lane] {
		d.aborted[m] = rows
	}
	d.open[cfg.Lane] = map[txnMark][]string{}
	return &txnRowsLane{d: d, lane: cfg.Lane, gen: cfg.Generation, producer: d.producers[cfg.Lane], writing: map[destination.Epoch][]string{}}, nil
}

type txnRowsLane struct {
	d        *txnRows
	lane     int
	gen      uint64
	producer uint64
	writing  map[destination.Epoch][]string
}

func (w *txnRowsLane) Write(_ context.Context, e destination.Epoch, _ string, b arrow.RecordBatch) error {
	w.writing[e] = append(w.writing[e], w.d.render(b)...)
	return nil
}

func (w *txnRowsLane) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	d := w.d
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.producers[w.lane] != w.producer {
		return nil, destination.ErrFenced
	}
	m := txnMark{e.Generation, e.N, w.lane}
	d.open[w.lane][m] = w.writing[e]
	delete(w.writing, e)
	return json.Marshal(txnCommit{Mark: m, Producer: w.producer})
}

func (w *txnRowsLane) Commit(_ context.Context, _ destination.Epoch, staged []destination.Staged) (destination.Committed, error) {
	d := w.d
	var c txnCommit
	if len(staged) != 1 || json.Unmarshal(staged[0], &c) != nil || c.Mark.Lane != w.lane {
		return destination.Committed{}, fmt.Errorf("lane %d asked to commit %d committables", w.lane, len(staged))
	}
	d.mu.Lock()
	rows, before := len(d.open[w.lane][c.Mark]), d.before
	d.mu.Unlock()
	if before != nil {
		if err := before(w.gen, c.Mark, rows); err != nil {
			return destination.Committed{}, err
		}
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.marked[c.Mark] {
		return destination.Committed{}, nil
	}
	got, ok := d.open[w.lane][c.Mark]
	switch {
	case d.producers[w.lane] != c.Producer:
		return destination.Committed{}, destination.ErrFenced
	case !ok:
		return destination.Committed{}, fmt.Errorf("lane %d has no open transaction of %v", w.lane, c.Mark)
	}
	if lost, ok := d.aborted[c.Mark]; ok {
		d.redone++
		if !slices.Equal(slices.Sorted(slices.Values(lost)), slices.Sorted(slices.Values(got))) {
			d.t.Errorf("%v was written again as %v, not its %v", c.Mark, got, lost)
		}
	}
	delete(d.open[w.lane], c.Mark)
	d.rows = append(d.rows, got...)
	d.marked[c.Mark] = true
	return destination.Committed{}, nil
}

func (w *txnRowsLane) Recover(_ context.Context, _ uint64, e destination.Epoch, staged []destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	var c txnCommit
	if len(staged) != 1 || json.Unmarshal(staged[0], &c) != nil || c.Mark.Lane != w.lane {
		return 0, fmt.Errorf("lane %d asked to settle %d committables", w.lane, len(staged))
	}
	w.d.mu.Lock()
	defer w.d.mu.Unlock()
	if w.d.marked[c.Mark] {
		return destination.Applied, nil
	}
	return destination.Lost, nil
}

func (w *txnRowsLane) Close(context.Context) error { return nil }

// windowRows renders a batch of tens' rows as "start/region/n".
func windowRows(b arrow.RecordBatch) []string {
	cols := map[string]arrow.Array{}
	for i, f := range b.Schema().Fields() {
		cols[f.Name] = b.Column(i)
	}
	out := make([]string, b.NumRows())
	for i := range out {
		out[i] = cols["window_start"].ValueStr(i) + "/" + cols["region"].ValueStr(i) + "/" + cols["n"].ValueStr(i)
	}
	return out
}

// A group window on workers into a destination whose writer lanes commit
// on their own: a worker lost as it commits writer lane 0's part of an
// epoch whose windows closed leaves the epoch committed in part. The next
// attempt's engine writes that part again from the state before the epoch,
// reading every worker's splits for the lane's key groups and closing the
// windows where the epoch did: what it writes is what was lost, and each
// window lands once, with every row of it.
func TestAGroupWindowRedoThroughTheEngine(t *testing.T) {
	dest, _ := windowRedo(t, false, func(*checkpoint.Record) bool { return true })
	landedOnce(t, dest.rows, (&timedChunks{splits: 3, chunks: 120, rows: 24}).windows())
}

// As TestAGroupWindowRedoThroughTheEngine, with rows behind their splits'
// watermark, which a window that closed in the epoch takes as late: the
// record keeps the steps of the window's watermark in the epoch, and the
// redo replays them, so what it writes is still what was lost, and no
// window lands twice. The epoch lost is one whose record has steps, and
// late rows.
func TestAGroupWindowRedoOfLateRowsThroughTheEngine(t *testing.T) {
	late := func(rec *checkpoint.Record) uint64 {
		var n uint64
		for _, q := range rec.Quarantine {
			for _, c := range q.Causes {
				if q.Output && c.Code == "late" {
					n += c.Rows
				}
			}
		}
		return n
	}
	dest, head := windowRedo(t, true, func(rec *checkpoint.Record) bool {
		return len(rec.Streams["s"].Steps) > 0 && late(rec) > 0
	})
	for op, b := range head.Streams["s"].Steps {
		t.Logf("the lost epoch's record keeps %d bytes of steps of %s, and %d late rows", len(b), op, late(head))
	}
	seen := map[string]bool{}
	for _, r := range dest.rows {
		w := r[:strings.LastIndex(r, "/")]
		if seen[w] {
			t.Fatalf("window %s landed twice", w)
		}
		seen[w] = true
	}
}

// windowRedo runs TestAGroupWindowRedoThroughTheEngine's attempts, with
// rows behind their splits' watermark where behind says, losing writer
// lane 0's part of an epoch whose record lose takes, and gives the
// destination and the record of the epoch that was lost in part.
func windowRedo(t *testing.T, behind bool, lose func(*checkpoint.Record) bool) (*txnRows, *checkpoint.Record) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := newTxnRows(t, windowRows)
	base := tensJob(ctx, t, destination.Append, "file://"+t.TempDir())
	all := &timedChunks{splits: 3, chunks: 120, rows: 24, pause: 4 * time.Millisecond, behind: behind}
	quarantine := "file://" + t.TempDir()
	job := func(owner string) func() runner.Job {
		return func() runner.Job {
			j := base
			j.Owner, j.Log, j.Destination, j.Dataflow, j.Logger = owner, log, dest, &Dataflow{Options: opts}, opts.Logger.With("job", owner)
			j.Source = &timedChunks{splits: all.splits, chunks: all.chunks, rows: all.rows, pause: all.pause, behind: all.behind}
			j.QuarantineURL = quarantine
			j.Settings = delivery.Settings{Require: delivery.AtLeastOnce, CommitMaxBytes: 8 << 10, Scope: destination.ScopePipeline}
			return j
		}
	}
	srv := listen(t)

	// Writer lane 0's commit fails, and its worker is lost, in its second
	// epoch with windows or later whose record lose takes.
	p := &inProcess{ctx: ctx, mk: job("a1")}
	j := p.mk()
	p.on(&j, srv, 3)
	var mu sync.Mutex
	var failing *txnMark
	held := 0
	dest.mu.Lock()
	dest.before = func(gen uint64, m txnMark, rows int) error {
		if gen != 1 || m.Lane != 0 || rows == 0 {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if held++; failing == nil && held >= 2 {
			if rec, err := log.Latest(ctx); err == nil && rec != nil && rec.Epoch == m.N && lose(rec) {
				failing = &m
			}
		}
		if failing == nil || *failing != m {
			return nil
		}
		p.kill(0)
		return errors.New("worker 0 is gone")
	}
	dest.mu.Unlock()
	if _, err := runner.RunAttempt(ctx, j); err == nil {
		t.Fatal("the attempt went on without its worker")
	}
	p.wait()
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if failing == nil || head == nil {
		t.Fatalf("HEAD is %v, failing %v", head, failing)
	}
	if head.Epoch != failing.N || len(head.Coverage) != len(head.Committables) {
		t.Fatalf("HEAD is epoch %d, failing %v, with %d coverages of %d committables", head.Epoch, failing, len(head.Coverage), len(head.Committables))
	}
	// Writer lane 0 holds the windows of its key groups, of every worker's
	// splits, from the floor the record has to where it closed them.
	c0 := head.Coverage[0]
	readers := map[string]bool{}
	for _, c := range c0.Splits {
		if c.Output != "tens" || c.KeyGroups == nil || *c.KeyGroups != *c0.KeyGroups || !slices.Equal(c.Key, []string{"region"}) {
			t.Errorf("writer lane 0 names split %s of %s with key groups %v by %v", c.ID, c.Output, c.KeyGroups, c.Key)
		}
		readers[c.ID] = true
	}
	st := head.Streams["s"]
	if c0.KeyGroups == nil || len(readers) < 2 || st.Floor == nil || st.Closed == nil || *st.Closed <= *st.Floor {
		t.Fatalf("writer lane 0's coverage %+v; the record's window marks %v and %v", c0, st.Floor, st.Closed)
	}

	p.mk = job("a2")
	j = p.mk()
	p.on(&j, srv, 2)
	if _, err := runner.RunAttempt(ctx, j); err != nil {
		t.Fatal(err)
	}
	for i, err := range p.wait() {
		if err != nil {
			t.Errorf("worker %d: %v", i, err)
		}
	}
	dest.mu.Lock()
	defer dest.mu.Unlock()
	if dest.redone == 0 {
		t.Error("no lane was written again")
	}
	return dest, head
}
