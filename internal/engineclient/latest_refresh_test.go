package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// profile is a row of stream s: its ID, region, amount and time in seconds.
type profile struct {
	id     int
	region string
	amount int
	sec    int64
}

// errReadBroke is the failure profileSource injects.
var errReadBroke = errors.New("the read broke")

// profileSource reads stream s's rows, one at a time with a resume point
// after each, from the one its state names. A bounded read then completes,
// so that the next starts over; a continuous one waits. With failAt, a
// read fails with errReadBroke once it has given that many rows and failed
// closes.
type profileSource struct {
	rows       []profile
	continuous bool
	failAt     int
	failed     <-chan struct{}
	// Where each read starts: the state it's given. Nil: not kept.
	starts *readStarts
}

type readStarts struct {
	mu sync.Mutex
	at []string
}

func (p profileSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if p.starts != nil {
			p.starts.mu.Lock()
			p.starts.at = append(p.starts.at, string(sp.State))
			p.starts.mu.Unlock()
		}
		n := 0
		if len(sp.State) > 0 {
			var err error
			if n, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
		}
		for ; n < len(p.rows); n++ {
			if p.failed != nil && n == p.failAt {
				select {
				case <-p.failed:
					return errReadBroke
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			r := p.rows[n]
			row := json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "amount": %d, "at": %q}`, r.id, r.region, r.amount, at(r.sec*1_000_000)))
			if err := e.Rows([]json.RawMessage{row}); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(n + 1))); err != nil {
				return err
			}
		}
		// A full read's next read starts over.
		if !p.continuous {
			return e.Complete([]byte("0"))
		}
		<-ctx.Done()
		return ctx.Err()
	})
}

// opDest is keyDest, keeping each row's op and sequence.
type opDest struct{ *keyDest }

func (d opDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	return &opWriter{&keyWriter{d: d.keyDest, lane: cfg.Lane}}, nil
}

type opWriter struct{ *keyWriter }

func (w *opWriter) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	rows := rowsOf(stream, false, b)
	for _, c := range []string{"_openetl_op", "_openetl_seq"} {
		if i := b.Schema().FieldIndices(c); len(i) == 1 {
			col := b.Column(i[0])
			for r := range rows {
				rows[r].cols[c] = col.ValueStr(r)
			}
		}
	}
	w.stage(e, rows)
	return nil
}

// merged is what a merge with hard deletes leaves of stream, by region,
// its amount, and every op applied, as op region: a row applies unless the
// table's row of its key has a greater sequence. Of the rows from from on,
// a key's come in the order of their sequences, or merged fails the test:
// the writes of one state's lifetime do.
func (d opDest) merged(t *testing.T, stream string, from int) (map[string]string, []string) {
	t.Helper()
	type row struct{ amount, seq string }
	out, seqs := map[string]row{}, map[string]string{}
	var ops []string
	for i, r := range d.applied(stream) {
		region, op, seq := r.cols["region"], r.cols["_openetl_op"], r.cols["_openetl_seq"]
		if i >= from {
			if seq < seqs[region] {
				t.Errorf("%s %s at %s after one at %s", op, region, seq, seqs[region])
			}
			seqs[region] = seq
		}
		ops = append(ops, op+" "+region)
		if have, ok := out[region]; ok && have.seq > seq {
			continue
		}
		if op == "delete" {
			delete(out, region)
			continue
		}
		out[region] = row{r.cols["amount"], seq}
	}
	amounts := map[string]string{}
	for k, r := range out {
		amounts[k] = r.amount
	}
	return amounts, ops
}

// latestJob runs a latest-row dedupe with a filter after it over stream s,
// merged by region, read with read: amounts under 50 pass.
type latestJob struct {
	t      *testing.T
	read   source.SyncMode
	cursor string
	sql    string
	a      sqlplan.Answer
	log    *checkpoint.Log
	state  string
	dest   opDest

	mu      sync.Mutex
	deletes uint64
}

func newLatestJob(ctx context.Context, t *testing.T, read source.SyncMode, sql string) *latestJob {
	t.Helper()
	s := profileStream(read)
	declared, err := arrowipc.EncodeSchema(s.Schema)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	return &latestJob{t: t, read: read, cursor: s.CursorField, sql: sql, a: a, log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "latest"),
		state: "file://" + t.TempDir(), dest: opDest{newKeyDest(1)}}
}

// profileStream is s as the runner has it read with read: held, and read
// whole each sync, with full_refresh; incremental by its time, or stream,
// otherwise.
func profileStream(read source.SyncMode) runner.Stream {
	cursor := ""
	if read == source.Incremental {
		cursor = "at"
	}
	return profileStreamBy(read, cursor)
}

// profileStreamBy is s read with read by cursor, held where that isn't in
// the order of its time.
func profileStreamBy(read source.SyncMode, cursor string) runner.Stream {
	held := read == source.FullRefresh || read == source.Incremental && cursor != "at"
	return runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append, Read: read, CursorField: cursor,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		EventTime: &runner.EventTime{Column: "at", Delay: time.Second, Idle: time.Hour, Held: held, FullRead: read == source.FullRefresh},
	}
}

func (l *latestJob) job(owner, sync string, src source.Reader, onRecord func(*checkpoint.Record)) runner.Job {
	opts := options(l.t, nil)
	exp := expected(l.t, l.a)
	exp[0].Operator = l.a.Statements[0].Operator
	return runner.Job{
		Pipeline: "latest", Owner: owner, Sync: sync, Continuous: l.read == source.Stream, Log: l.log, Source: src,
		Streams: []runner.Stream{profileStreamBy(l.read, l.cursor)},
		Outputs: []runner.Stream{{Name: "latest", Replay: source.Exact, Read: l.read, Mode: destination.Merge, PrimaryKey: []string{"region"},
			Transformed: true, From: "s", Deduped: true}},
		SQL:         &runner.SQLTransform{SQL: l.sql, Expected: exp, Operators: []string{l.a.Statements[0].Operator}},
		Destination: l.dest, Dataflow: &Dataflow{Options: opts}, Lanes: 1, Logger: opts.Logger, StateURL: l.state,
		Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
		OnCheckpoint: func(rec *checkpoint.Record, _, _ map[string]uint64) {
			l.mu.Lock()
			for _, o := range rec.Operators {
				l.deletes += o.Deletes
			}
			l.mu.Unlock()
			if onRecord != nil {
				onRecord(rec)
			}
		},
	}
}

// takeDeletes are the deletes the operator counted since the last call.
func (l *latestJob) takeDeletes() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := l.deletes
	l.deletes = 0
	return n
}

const latestSmall = "INSERT INTO latest SELECT region, amount, at FROM (SELECT region, amount, at, " +
	"row_number() OVER (PARTITION BY region ORDER BY at DESC) AS rn FROM s) AS t WHERE rn = 1 AND amount < 50"

// A latest-row dedupe with a filter after it, over a stream read with
// full_refresh and merged, through the runner and the engine. Each full
// read starts with no state, so it writes every key's latest row again, and
// a delete of each key whose latest row the filter drops: a row of the last
// read that now fails the filter goes. A key whose earlier row fails and
// latest passes keeps its latest; a key the read doesn't have keeps its
// row, as with any full_refresh merge. Within a read, a key whose latest
// row fails ends with no row. A retry within the second sync restores the
// state of the rows it read: it deletes a key it hasn't come to yet, and
// drops an older row of a key it has, which fails the filter.
func TestALatestRowDedupeAcrossFullReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	l := newLatestJob(ctx, t, source.FullRefresh, latestSmall)
	sync1 := []profile{
		{1, "eu", 10, 1}, {2, "eu", 20, 2}, {3, "us", 30, 1}, {5, "jp", 40, 1},
		// de passes, then fails within the read.
		{6, "de", 20, 1}, {7, "de", 90, 2},
		// fr fails, then passes.
		{8, "fr", 90, 1}, {9, "fr", 5, 2},
	}
	if _, err := runner.RunAttempt(ctx, l.job("a1", "s1", profileSource{rows: sync1}, nil)); err != nil {
		t.Fatal(err)
	}
	got, ops := l.dest.merged(t, "latest", 0)
	if want := map[string]string{"eu": "20", "us": "30", "jp": "40", "fr": "5"}; !maps.Equal(got, want) {
		t.Fatalf("after sync 1, %v, want %v (ops %v)", got, want, ops)
	}
	// de's, and fr's earlier row's unless it came with fr's latest.
	if n := l.takeDeletes(); n < 1 || n > 2 {
		t.Errorf("sync 1 counted %d deletes, want de's, and fr's at most", n)
	}

	// Sync 2: us and fr as they were, and a new key, it; then the attempt
	// fails. Its retry restores the state of those, and reads an older row
	// of fr that fails the filter, which its state drops, and eu, whose
	// latest row, first of eu's, now fails: the state hasn't eu. jp is gone
	// from the source.
	sync2 := []profile{{3, "us", 30, 1}, {9, "fr", 5, 2}, {10, "it", 10, 1}, {11, "fr", 95, 1}, {4, "eu", 90, 3}, {1, "eu", 10, 1}, {2, "eu", 20, 2}}
	failed := make(chan struct{})
	var once sync.Once
	read := func(rec *checkpoint.Record) {
		if slices.ContainsFunc(rec.Splits("s"), func(sp checkpoint.SplitState) bool { return string(sp.State) == "3" }) {
			once.Do(func() { close(failed) })
		}
	}
	before := len(ops)
	if _, err := runner.RunAttempt(ctx, l.job("a2", "s2", profileSource{rows: sync2, failAt: 3, failed: failed}, read)); !errors.Is(err, errReadBroke) {
		t.Fatalf("the first attempt of sync 2 ended with %v, want the read's failure", err)
	}
	starts := &readStarts{}
	if _, err := runner.RunAttempt(ctx, l.job("a3", "s2", profileSource{rows: sync2, starts: starts}, nil)); err != nil {
		t.Fatal(err)
	}
	// The retry reads on from the resume point after the third row.
	if !slices.Equal(starts.at, []string{"3"}) {
		t.Errorf("the retry's read started at %q, want the resume point after the third row", starts.at)
	}
	got, ops = l.dest.merged(t, "latest", before)
	if want := map[string]string{"us": "30", "fr": "5", "jp": "40", "it": "10"}; !maps.Equal(got, want) {
		t.Errorf("after sync 2, %v, want %v (ops %v)", got, want, ops)
	}
	if two := ops[before:]; !slices.Contains(two, "delete eu") || !slices.Contains(two, "insert us") || slices.Contains(two, "delete fr") {
		t.Errorf("sync 2 wrote %v, want eu deleted, us written again, and no delete of fr", two)
	}
	if n := l.takeDeletes(); n != 1 {
		t.Errorf("sync 2 counted %d deletes, want eu's", n)
	}
}

// Over a read that isn't whole, the state a dedupe restores holds every
// key it has seen, and a key it hasn't seen has no row of its writing: a
// filtered row of a new key writes no delete. A continuous pipeline's
// first start and restart, and an incremental pipeline's next sync,
// delete only a key whose row they wrote: also by a cursor other than its
// time, whose read is held to its end each sync, where the state goes on
// past that end. After a reset, which drops the dedupe's state, a key's
// row from before stays when its latest row now fails the filter: the
// documented gap.
func TestALatestRowDedupeOverOtherReadsDeletesOnlyWhatItWrote(t *testing.T) {
	for _, c := range []struct {
		name   string
		read   source.SyncMode
		cursor string
	}{{"continuous", source.Stream, ""}, {"incremental", source.Incremental, "at"}, {"incremental by another cursor", source.Incremental, "id"}} {
		read := c.read
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			l := newLatestJob(ctx, t, read, latestSmall)
			l.cursor = c.cursor
			// run reads rows to their end: a continuous pipeline drains once a
			// record holds them all, and a scheduled one's read completes.
			run := func(n int, resets []runner.Reset, rows ...profile) {
				t.Helper()
				owner, syncID := fmt.Sprintf("a%d", n), fmt.Sprintf("s%d", n)
				if read != source.Stream {
					j := l.job(owner, syncID, profileSource{rows: rows}, nil)
					j.Resets = resets
					if _, err := runner.RunAttempt(ctx, j); err != nil {
						t.Fatal(err)
					}
					return
				}
				drain := make(chan struct{})
				var once sync.Once
				all := strconv.Itoa(len(rows))
				j := l.job(owner, "c1", profileSource{rows: rows, continuous: true}, func(rec *checkpoint.Record) {
					if slices.ContainsFunc(rec.Splits("s"), func(sp checkpoint.SplitState) bool { return string(sp.State) == all }) {
						once.Do(func() { close(drain) })
					}
				})
				j.Drain, j.Resets = drain, resets
				if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
					t.Fatalf("%s ended with %v", owner, err)
				}
			}
			// The first start: us is new, and fails the filter.
			rows := []profile{{1, "eu", 5, 1}, {2, "us", 500, 1}}
			run(1, nil, rows...)
			_, ops := l.dest.merged(t, "latest", 0)
			if want := []string{"insert eu"}; !slices.Equal(ops, want) {
				t.Errorf("the first start wrote %v, want %v", ops, want)
			}
			// A restart, or the next sync, reads on: us again, eu now fails,
			// and it is new and fails.
			rows = append(rows, profile{3, "us", 600, 2}, profile{4, "eu", 700, 2}, profile{5, "it", 90, 2}, profile{6, "jp", 10, 3})
			run(2, nil, rows...)
			got, ops := l.dest.merged(t, "latest", 0)
			if want := []string{"insert eu", "delete eu", "insert jp"}; !slices.Equal(ops, want) || !maps.Equal(got, map[string]string{"jp": "10"}) {
				t.Errorf("after the restart, %v and ops %v, want jp's row and %v", got, ops, want)
			}
			if n := l.takeDeletes(); n != 1 {
				t.Errorf("%d deletes counted, want eu's alone", n)
			}

			// A reset drops the dedupe's state, and the read starts over. The
			// source has only jp's newer row now, which fails the filter.
			run(3, []runner.Reset{{ID: "r1"}}, profile{7, "jp", 90, 4})
			got, ops = l.dest.merged(t, "latest", 0)
			// The documented gap: jp's row from before stays, until a row of
			// it passes again or someone deletes it.
			if got["jp"] != "10" || slices.Contains(ops, "delete jp") {
				t.Errorf("after the reset, %v and ops %v, want jp's row from before kept", got, ops)
			}
		})
	}
}

// A first-row dedupe over a stream read with full_refresh, appended: each
// full read starts with no keys kept, so the next sync writes every key's
// first row again, with a retention and unbounded.
func TestAFirstRowDedupeAcrossFullReads(t *testing.T) {
	for _, retention := range []int64{time.Hour.Microseconds(), 1<<63 - 1} {
		t.Run(fmt.Sprint(retention), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			opts := options(t, nil)
			s := profileStream(source.FullRefresh)
			s.EventTime.Dedupe = &retention
			declared, err := arrowipc.EncodeSchema(s.Schema)
			if err != nil {
				t.Fatal(err)
			}
			const sql = "INSERT INTO firsts SELECT region, amount, at FROM s QUALIFY row_number() OVER (PARTITION BY region ORDER BY at) = 1"
			a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at", DedupeRetentionMicros: &retention}}})
			if err != nil || len(a.Errors) != 0 {
				t.Fatal(a.Errors, err)
			}
			exp := expected(t, a)
			exp[0].Operator = a.Statements[0].Operator
			log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "firsts")
			state, dest := "file://"+t.TempDir(), newKeyDest(1)
			sync := func(n int, rows ...profile) []string {
				t.Helper()
				before := len(dest.applied("firsts"))
				j := runner.Job{
					Pipeline: "firsts", Owner: fmt.Sprintf("a%d", n), Sync: fmt.Sprintf("s%d", n), Log: log, Source: profileSource{rows: rows},
					Streams: []runner.Stream{s},
					Outputs: []runner.Stream{{Name: "firsts", Replay: source.Exact, Read: source.FullRefresh, Mode: destination.Append, PrimaryKey: []string{"region"},
						Transformed: true, From: "s", Deduped: true}},
					SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
					Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 1, Logger: opts.Logger, StateURL: state,
					Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
				}
				if _, err := runner.RunAttempt(ctx, j); err != nil {
					t.Fatal(err)
				}
				var out []string
				for _, r := range dest.applied("firsts")[before:] {
					out = append(out, r.cols["region"])
				}
				slices.Sort(out)
				return out
			}
			if got := sync(1, profile{1, "eu", 10, 1}, profile{2, "us", 30, 1}); !slices.Equal(got, []string{"eu", "us"}) {
				t.Errorf("sync 1 wrote %v", got)
			}
			if got := sync(2, profile{1, "eu", 10, 1}, profile{2, "us", 30, 1}, profile{3, "fr", 5, 2}); !slices.Equal(got, []string{"eu", "fr", "us"}) {
				t.Errorf("sync 2 wrote %v, want every key again", got)
			}
		})
	}
}
