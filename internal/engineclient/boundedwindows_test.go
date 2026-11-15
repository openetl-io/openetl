package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// timedSource reads each split's rows in order, in chunks, and then
// returns, or with continuous, waits for the end of the read.
type timedSource struct {
	splits     [][]json.RawMessage
	continuous bool
}

func (s *timedSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	out := make([]source.Split, len(s.splits))
	for i := range out {
		out[i] = source.Split{ID: strconv.Itoa(i)}
	}
	return out, nil
}

func (s *timedSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		i, err := strconv.Atoi(sp.ID)
		if err != nil {
			return err
		}
		rows := s.splits[i]
		for at := 0; at < len(rows); at += 7 {
			if err := e.Rows(rows[at:min(at+7, len(rows))]); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(at))); err != nil {
				return err
			}
		}
		if s.continuous {
			<-ctx.Done()
			return ctx.Err()
		}
		return e.Complete([]byte(strconv.Itoa(len(rows))))
	})
}

// startAt is how the destination shows a window's start at us.
func startAt(us int64) string {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer b.Release()
	b.Append(arrow.Timestamp(us))
	a := b.NewArray()
	defer a.Release()
	return a.ValueStr(0)
}

// A group window gives the same table read three ways: once whole by a
// held read, which ends at the end of input; once by a bounded read in the
// order of its time, whose splits complete at the clock less the delay;
// and continuously, in many epochs, until idle splits move on with the
// clock. Every window before the clock closes in each. A window after the
// clock closes only in the held read: an ordered read, bounded or not,
// leaves it open.
func TestAGroupWindowBoundedAndContinuous(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
	}, nil)
	declared, err := arrowipc.EncodeSchema(schema)
	if err != nil {
		t.Fatal(err)
	}
	const sql = "INSERT INTO minutes SELECT window_start, region, count(*) AS n, sum(id) AS total FROM s GROUP BY tumble(at, INTERVAL '1 minute'), region"
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator

	// Three splits of rows a few seconds apart over ten minutes, each in
	// order, and a last row of region later an hour past the clock.
	later := time.Now().Add(time.Hour).Truncate(time.Minute).UnixMicro()
	var splits [][]json.RawMessage
	want := map[string]string{}
	agg := map[string][2]int64{}
	id := int64(0)
	for s := range 3 {
		var rows []json.RawMessage
		add := func(us int64, region string) {
			id++
			rows = append(rows, json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "at": %q}`, id, region, at(us))))
			k := startAt(us-us%60_000_000) + "/" + region
			v := agg[k]
			agg[k] = [2]int64{v[0] + 1, v[1] + id}
		}
		for sec := int64(s); sec < 600; sec += 3 + int64(s) {
			add(minutes+sec*1_000_000, []string{"eu", "us", "apac"}[(sec/3)%3])
		}
		add(later+int64(s), "later")
		splits = append(splits, rows)
	}
	for k, v := range agg {
		want[k] = fmt.Sprintf("%d/%d", v[0], v[1])
	}
	past := maps.Clone(want)
	maps.DeleteFunc(past, func(k, _ string) bool { return strings.HasSuffix(k, "/later") })
	if len(past) != 30 || len(want) != 31 {
		t.Fatalf("%d windows before the clock and %d in all", len(past), len(want))
	}

	// table is what a merge of the window's rows leaves, by start/region.
	table := func(d *keyDest) map[string]string {
		out := map[string]string{}
		for _, r := range d.applied("minutes") {
			out[r.cols["window_start"]+"/"+r.cols["region"]] = r.cols["n"] + "/" + r.cols["total"]
		}
		return out
	}
	job := func(name string, src *timedSource, held bool, d *keyDest) runner.Job {
		read := source.FullRefresh
		if src.continuous {
			read = source.Stream
		}
		return runner.Job{
			Pipeline: name, Owner: "a1", Sync: "s1", Continuous: src.continuous,
			Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", name), Source: src,
			Streams: []runner.Stream{{
				Name: "s", Replay: source.Exact, Mode: destination.Append, Read: read, Schema: schema,
				EventTime: &runner.EventTime{Column: "at", Held: held, Idle: 300 * time.Millisecond},
			}},
			Outputs:     []runner.Stream{{Name: "minutes", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s"}},
			SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
			Destination: d, Dataflow: &Dataflow{Options: opts}, Lanes: 3, Logger: opts.Logger.With("job", name),
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
		}
	}

	held := newKeyDest(2)
	if _, err := runner.RunAttempt(ctx, job("held", &timedSource{splits: splits}, true, held)); err != nil {
		t.Fatal(err)
	}
	if got := table(held); !maps.Equal(got, want) {
		t.Errorf("the held read's table:\n got %v\nwant %v", got, want)
	}

	ordered := newKeyDest(2)
	if _, err := runner.RunAttempt(ctx, job("ordered", &timedSource{splits: splits}, false, ordered)); err != nil {
		t.Fatal(err)
	}
	if got := table(ordered); !maps.Equal(got, past) {
		t.Errorf("the ordered read's table:\n got %v\nwant %v", got, past)
	}

	continuous := newKeyDest(2)
	j := job("continuous", &timedSource{splits: splits, continuous: true}, false, continuous)
	drain := make(chan struct{})
	var once sync.Once
	var records atomic.Int64
	j.Drain = drain
	j.OnCheckpoint = func(*checkpoint.Record, map[string]uint64) {
		records.Add(1)
		if len(table(continuous)) >= len(past) {
			once.Do(func() { close(drain) })
		}
	}
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the continuous run ended with %v", err)
	}
	if got := table(continuous); !maps.Equal(got, past) {
		t.Errorf("the continuous read's table:\n got %v\nwant %v", got, past)
	}
	if n := records.Load(); n < 3 {
		t.Errorf("the continuous read committed %d epochs", n)
	}
}
