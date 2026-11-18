package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// profile is a row of stream s: its region, amount and time in seconds.
type profile struct {
	id     int
	region string
	amount int
	sec    int64
}

// wholeSource reads stream s whole, its rows then its end.
type wholeSource struct{ rows []profile }

func (w wholeSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, _ source.Split, e source.SplitEmitter) error {
		var rows []json.RawMessage
		for _, p := range w.rows {
			rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "amount": %d, "at": %q}`, p.id, p.region, p.amount, at(p.sec*1_000_000))))
		}
		if err := e.Rows(rows); err != nil {
			return err
		}
		return e.Complete([]byte("done"))
	})
}

// mergeDest is keyDest, merging each row it applies by region, a delete
// removing it, as a merge with hard deletes does.
type mergeDest struct {
	*keyDest
	ops []string
}

func (d *mergeDest) Open(ctx context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	w, err := d.keyDest.Open(ctx, cfg)
	return &opWriter{Writer: w, d: d}, err
}

type opWriter struct {
	destination.Writer
	d *mergeDest
}

func (w *opWriter) Write(ctx context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	if i := b.Schema().FieldIndices("_openetl_op"); len(i) == 1 {
		ops := b.Column(i[0]).(*array.String)
		regions := b.Column(b.Schema().FieldIndices("region")[0])
		for r := range int(b.NumRows()) {
			w.d.mu.Lock()
			w.d.ops = append(w.d.ops, ops.Value(r)+" "+regions.ValueStr(r))
			w.d.mu.Unlock()
		}
	}
	return w.Writer.Write(ctx, e, stream, b)
}

// table is what a merge leaves of the output, by region: its amount.
func (d *mergeDest) table() map[string]string {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := map[string]string{}
	for i, r := range d.rows {
		var op string
		if i < len(d.ops) {
			_, _ = fmt.Sscanf(d.ops[i], "%s", &op)
		}
		switch op {
		case "delete":
			delete(out, r.cols["region"])
		default:
			out[r.cols["region"]] = r.cols["amount"]
		}
	}
	return out
}

// A latest-row dedupe with a filter after it, over a stream read with
// full_refresh, merged: what each of two syncs writes, and what the output
// holds after them.
func TestALatestRowDedupeAcrossFullReads(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	opts := options(t, nil)
	s := runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append, Read: source.FullRefresh,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		EventTime: &runner.EventTime{Column: "at", Delay: time.Second, Idle: time.Hour, Held: true},
	}
	declared, err := arrowipc.EncodeSchema(s.Schema)
	if err != nil {
		t.Fatal(err)
	}
	const sql = "INSERT INTO latest SELECT region, amount, at FROM (SELECT region, amount, at, " +
		"row_number() OVER (PARTITION BY region ORDER BY at DESC) AS rn FROM s) AS t WHERE rn = 1 AND amount < 50"
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	if st := a.Statements[0].State; st == nil || st.Keep != "latest" {
		t.Fatalf("planned %+v, want a latest-row dedupe", a.Statements[0])
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "latest")
	state := "file://" + t.TempDir()
	dest := &mergeDest{keyDest: newKeyDest(1)}
	var snapshots []int
	sync := func(n int, rows ...profile) {
		t.Helper()
		snapshots = append(snapshots, -1)
		j := runner.Job{
			Pipeline: "latest", Owner: fmt.Sprintf("a%d", n), Sync: fmt.Sprintf("s%d", n), Log: log, Source: wholeSource{rows},
			Streams: []runner.Stream{s},
			Outputs: []runner.Stream{{Name: "latest", Replay: source.Exact, Read: source.FullRefresh, Mode: destination.Merge, PrimaryKey: []string{"region"},
				Transformed: true, From: "s", Deduped: true}},
			SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
			Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 1, Logger: opts.Logger, StateURL: state,
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
			OnCheckpoint: func(rec *checkpoint.Record, _, _ map[string]uint64) {
				snapshots[len(snapshots)-1] = len(rec.Snapshots)
			},
		}
		if _, err := runner.RunAttempt(ctx, j); err != nil {
			t.Fatal(err)
		}
	}

	// Sync 1: eu's latest passes the filter, and so does us's.
	sync(1, profile{1, "eu", 10, 1}, profile{2, "eu", 20, 2}, profile{3, "us", 30, 1}, profile{5, "jp", 40, 1})
	if got, want := dest.table(), map[string]string{"eu": "20", "us": "30", "jp": "40"}; !maps.Equal(got, want) {
		t.Fatalf("after sync 1, %v, want %v (ops %v)", got, want, dest.ops)
	}
	first := len(dest.ops)
	// Sync 2: eu's latest row now fails the filter, us is as it was, and jp
	// is gone from the source.
	sync(2, profile{1, "eu", 10, 1}, profile{2, "eu", 20, 2}, profile{4, "eu", 90, 3}, profile{3, "us", 30, 1})
	t.Logf("sync 1 wrote %v; sync 2 wrote %v; snapshots in each sync's last record %v", dest.ops[:first], dest.ops[first:], snapshots)
	t.Logf("after sync 2 the output holds %v", dest.table())
}

// A first-row dedupe over a stream read with full_refresh, appended: what
// each of two syncs writes, with a retention and unbounded.
func TestAFirstRowDedupeAcrossFullReads(t *testing.T) {
	for _, retention := range []int64{time.Hour.Microseconds(), 1<<63 - 1} {
		t.Run(fmt.Sprint(retention), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			opts := options(t, nil)
			s := runner.Stream{
				Name: "s", Replay: source.Exact, Mode: destination.Append, Read: source.FullRefresh,
				Schema: arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64},
					{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "amount", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
				}, nil),
				EventTime: &runner.EventTime{Column: "at", Delay: time.Second, Idle: time.Hour, Held: true, Dedupe: &retention},
			}
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
			state := "file://" + t.TempDir()
			dest := newKeyDest(1)
			sync := func(n int, rows ...profile) {
				t.Helper()
				j := runner.Job{
					Pipeline: "firsts", Owner: fmt.Sprintf("a%d", n), Sync: fmt.Sprintf("s%d", n), Log: log, Source: wholeSource{rows},
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
			}
			regions := func(rows []destRow) []string {
				var out []string
				for _, r := range rows {
					out = append(out, r.cols["region"])
				}
				return out
			}
			sync(1, profile{1, "eu", 10, 1}, profile{2, "us", 30, 1})
			first := len(dest.applied("firsts"))
			sync(2, profile{1, "eu", 10, 1}, profile{2, "us", 30, 1}, profile{3, "fr", 5, 2})
			all := dest.applied("firsts")
			t.Logf("sync 1 wrote %v; sync 2 wrote %v", regions(all[:first]), regions(all[first:]))
		})
	}
}
