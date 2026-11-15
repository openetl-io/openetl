package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync"
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

// minutes is base at the start of its minute, where windows of a minute
// start.
const minutes = base - base%60_000_000

// windowSource reads stream s in two splits. Quiet reads five rows of
// region us, a second apart from minutes+70s, and then nothing. Busy reads a
// row of region hb at its latest time every few milliseconds, which keeps
// it from being idle, until quiet has moved on, and then its steps, each
// a chunk of rows of region eu once until says yes, with hb rows while it
// waits.
type windowSource struct {
	quietMoved <-chan struct{}
	steps      []windowStep
}

type windowStep struct {
	// Seconds past base.
	at    []int64
	until func() bool
}

func (s *windowSource) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "busy"}, {ID: "quiet"}}, nil
}

func (s *windowSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		n := 0
		emit := func(region string, seconds ...int64) error {
			rows := make([]json.RawMessage, len(seconds))
			for i, sec := range seconds {
				n++
				rows[i] = json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "at": %q}`, n, region, at(minutes+sec*1_000_000)))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			return e.ResumePoint([]byte(strconv.Itoa(n)))
		}
		if sp.ID == "quiet" {
			if err := emit("us", 70, 71, 72, 73, 74); err != nil {
				return err
			}
			<-ctx.Done()
			return ctx.Err()
		}
		latest := int64(0)
		wait := func(done func() bool) error {
			for !done() {
				if err := emit("hb", latest); err != nil {
					return err
				}
				select {
				case <-time.After(20 * time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		}
		moved := func() bool {
			select {
			case <-s.quietMoved:
				return true
			default:
				return false
			}
		}
		if err := wait(moved); err != nil {
			return err
		}
		for _, step := range s.steps {
			if err := emit("eu", step.at...); err != nil {
				return err
			}
			latest = max(latest, slices.Max(step.at))
			if err := wait(step.until); err != nil {
				return err
			}
		}
		<-ctx.Done()
		return ctx.Err()
	})
}

// windowStart is how the destination shows a window's start at
// minutes+sec.
func windowStart(sec int64) string {
	b := array.NewTimestampBuilder(memory.DefaultAllocator, &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"})
	defer b.Release()
	b.Append(arrow.Timestamp(minutes + sec*1_000_000))
	a := b.NewArray()
	defer a.Release()
	return a.ValueStr(0)
}

// A group window in a continuous run through the engine closes each window
// as the watermark passes it, while the read goes on. A row that comes
// late, within the lateness, writes its window's row again; one past it is
// quarantined. A split with nothing to read holds windows open only until
// it's idle.
func TestAGroupWindowClosesAsItsReadGoesOn(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	s := runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append, Read: source.Stream,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		EventTime: &runner.EventTime{Column: "at", Idle: 300 * time.Millisecond, Lateness: time.Minute},
	}
	declared, err := arrowipc.EncodeSchema(s.Schema)
	if err != nil {
		t.Fatal(err)
	}
	const sql = "INSERT INTO minutes SELECT window_start, region, count(*) AS n FROM s GROUP BY tumble(at, INTERVAL '1 minute'), region"
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator

	dest := newKeyDest(2)
	// The windows' rows of region eu and us the destination has, in the
	// order it applied them, as start/region/n.
	applied := func() []string {
		var out []string
		for _, r := range dest.applied("minutes") {
			if r.cols["region"] != "hb" {
				out = append(out, r.cols["window_start"]+"/"+r.cols["region"]+"/"+r.cols["n"])
			}
		}
		return out
	}
	has := func(want ...string) func() bool {
		return func() bool {
			got := applied()
			for _, w := range want {
				if !slices.Contains(got, w) {
					return false
				}
			}
			return true
		}
	}
	w0, w1, w2 := windowStart(0), windowStart(60), windowStart(120)
	quietMoved := make(chan struct{})
	src := &windowSource{quietMoved: quietMoved, steps: []windowStep{
		// Busy reaches 64 seconds, and the first minute closes.
		{at: []int64{0, 1, 2, 3, 4}},
		{at: []int64{60, 61, 62, 63, 64}, until: has(w0 + "/eu/5")},
		// 10 seconds is late, within the minute's lateness: the first
		// minute's row again. 124 closes the second minute, and ends the
		// first minute's lateness.
		{at: []int64{10, 121, 122, 123, 124}, until: has(w0+"/eu/6", w1+"/eu/5", w1+"/us/5")},
		// 5 seconds is past it; 184 closes the third minute.
		{at: []int64{5, 181, 182, 183, 184}, until: has(w2 + "/eu/4")},
	}}
	src.steps[0].until = func() bool { return true }

	drain := make(chan struct{})
	var once, moved sync.Once
	var mu sync.Mutex
	var quarantined uint64
	dir := t.TempDir()
	j := runner.Job{
		Pipeline: "windows", Owner: "a1", Sync: "s1", Continuous: true,
		Log: checkpoint.NewLog(checkpoint.NewMemStore(), "org", "windows"), Source: src,
		Streams:     []runner.Stream{s},
		Outputs:     []runner.Stream{{Name: "minutes", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s"}},
		SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
		Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger,
		QuarantineURL: "file://" + dir,
		StateURL:      "file://" + t.TempDir(),
		Settings:      delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
		Drain:         drain,
		OnCheckpoint: func(rec *checkpoint.Record, _ map[string]uint64) {
			mu.Lock()
			for _, q := range rec.Quarantine {
				if q.Stream == "minutes" {
					quarantined += q.Rows
				}
			}
			mu.Unlock()
			// Quiet's watermark moves on with the clock once it's idle.
			for _, sp := range rec.Splits("s") {
				if sp.ID == "quiet" && sp.Watermark != nil && *sp.Watermark > time.Now().Add(-time.Hour).UnixMicro() {
					moved.Do(func() { close(quietMoved) })
				}
			}
			if has(w2 + "/eu/4")() {
				once.Do(func() { close(drain) })
			}
		},
	}
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the run ended with %v", err)
	}
	// The windows close in order of time, each once, and the first again
	// for its late row; the fourth minute is still open.
	want := []string{w0 + "/eu/5", w0 + "/eu/6", w1 + "/eu/5", w1 + "/us/5", w2 + "/eu/4"}
	got := applied()
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if quarantined != 1 {
		t.Errorf("%d rows quarantined, want the one at 5 seconds", quarantined)
	}
}
