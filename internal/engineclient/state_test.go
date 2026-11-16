package engineclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
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

// spanRows are a minute's rows of region eu, a second apart from its
// start, that spanSource reads.
const spanRows = 20

// spanSource reads stream s's one split from its state: spanRows rows, a
// resume point after every four, up to pause. Then, past pause, a row of
// region zz closer seconds past the minute, which closes it, with a resume
// point: once in a bounded read, which then completes, and over and over
// in another.
type spanSource struct {
	// In microseconds since the Unix epoch.
	minute  int64
	pause   int
	closer  int64
	bounded bool
}

func (s *spanSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(ctx context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		n := 0
		if len(sp.State) > 0 {
			var err error
			if n, err = strconv.Atoi(string(sp.State)); err != nil {
				return err
			}
		}
		row := func(id int, region string, sec int64) json.RawMessage {
			return json.RawMessage(fmt.Sprintf(`{"id": %d, "region": %q, "at": %q}`, id, region, at(s.minute+sec*1_000_000)))
		}
		for n < min(s.pause, spanRows) {
			var rows []json.RawMessage
			for ; len(rows) < 4 && n < min(s.pause, spanRows); n++ {
				rows = append(rows, row(n, "eu", int64(n)))
			}
			if err := e.Rows(rows); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(n))); err != nil {
				return err
			}
		}
		// The closing row; a continuous read goes on reading it, so epochs
		// go on.
		for i := 0; s.pause >= spanRows; i++ {
			if err := e.Rows([]json.RawMessage{row(1000+i, "zz", s.closer)}); err != nil {
				return err
			}
			if err := e.ResumePoint([]byte(strconv.Itoa(n))); err != nil {
				return err
			}
			if s.bounded {
				break
			}
			select {
			case <-time.After(20 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if s.bounded {
			return e.Complete([]byte(strconv.Itoa(n)))
		}
		<-ctx.Done()
		return ctx.Err()
	})
}

// spanJob is a job of a count of each minute's rows by region, over stream
// s read in the order of its event time, with its state at stateURL.
func spanJob(ctx context.Context, t *testing.T, read source.SyncMode, delay time.Duration) func(owner, sync string, src source.Reader, stateURL string, dest destination.Destination) runner.Job {
	t.Helper()
	opts := options(t, nil)
	s := runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append, Read: read,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		// Never idle: only rows move the watermark.
		EventTime: &runner.EventTime{Column: "at", Delay: delay, Idle: time.Hour},
	}
	if read == source.Incremental {
		s.CursorField = "at"
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
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "windows")
	return func(owner, sync string, src source.Reader, stateURL string, dest destination.Destination) runner.Job {
		return runner.Job{
			Pipeline: "windows", Owner: owner, Sync: sync, Continuous: read == source.Stream, Log: log, Source: src,
			Streams:     []runner.Stream{s},
			Outputs:     []runner.Stream{{Name: "minutes", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s"}},
			SQL:         &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}},
			Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger,
			StateURL: stateURL,
			Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
		}
	}
}

// stateRuns are the runs of state rec names.
func stateRuns(rec *checkpoint.Record) []string {
	var out []string
	for _, sn := range rec.Snapshots {
		out = append(out, sn.Runs...)
	}
	return out
}

// minuteRows are the rows of region eu dest has of minutes, as
// start/region/n.
func minuteRows(dest *keyDest) []string {
	var out []string
	for _, r := range dest.applied("minutes") {
		if r.cols["region"] == "eu" {
			out = append(out, r.cols["window_start"]+"/"+r.cols["region"]+"/"+r.cols["n"])
		}
	}
	return out
}

// A group window over a stream read in the order of its event time in a
// scheduled pipeline: a minute open at the end of a sync, which its state
// holds, goes on in the next sync's engine, which closes it with every row
// of both syncs.
func TestAGroupWindowSpansScheduledSyncs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	// A minute ahead of the clock, which a bounded read's end moves the
	// watermark to, less the delay.
	minute := time.Now().Truncate(time.Minute).Add(2 * time.Minute).UnixMicro()
	job := spanJob(ctx, t, source.Incremental, time.Hour)
	state, dest := "file://"+t.TempDir(), newKeyDest(2)
	if _, err := runner.RunAttempt(ctx, job("a1", "s1", &spanSource{minute: minute, pause: 12, bounded: true}, state, dest)); err != nil {
		t.Fatal(err)
	}
	if got := minuteRows(dest); len(got) != 0 {
		t.Fatalf("the minute closed in the first sync: %v", got)
	}
	// Two hours on, less the hour's delay, closes the minute.
	if _, err := runner.RunAttempt(ctx, job("a2", "s2", &spanSource{minute: minute, pause: spanRows, closer: 2 * 3600, bounded: true}, state, dest)); err != nil {
		t.Fatal(err)
	}
	want := []string{windowStart((minute-minutes)/1_000_000) + "/eu/" + strconv.Itoa(spanRows)}
	if got := minuteRows(dest); !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v", got, want)
	}
}

// A group window's state outlives the engine that kept it. An attempt
// whose engine ends while the minute is open leaves its state in the
// state's storage, named by its records; the next attempt, in an engine
// of its own, opens it from the record it restores, reads on from the
// record's position, and closes the minute with every row of it, each
// once. Then the minute's state is dead, and the record names none of the
// first attempt's runs.
func TestAGroupWindowsStateOutlivesItsEngine(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	minute := minutes + 60_000_000
	job := spanJob(ctx, t, source.Stream, 0)
	state, dest := "file://"+t.TempDir(), newKeyDest(2)

	// The first attempt's engine ends once a record names state, with the
	// minute open.
	first, stop := context.WithCancel(ctx)
	var once sync.Once
	var named []string
	j := job("a1", "s1", &spanSource{minute: minute, pause: 12}, state, dest)
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		if r := stateRuns(rec); len(r) > 0 {
			once.Do(func() {
				named = r
				stop()
			})
		}
	}
	if _, err := runner.RunAttempt(first, j); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first attempt ended with %v", err)
	}
	if len(named) == 0 {
		t.Fatal("no record of the first attempt named state")
	}
	if got := minuteRows(dest); len(got) != 0 {
		t.Fatalf("the minute closed in the first attempt: %v", got)
	}

	// The second restores it, and goes on.
	drain := make(chan struct{})
	var mu sync.Mutex
	var last []string
	var done sync.Once
	j = job("a2", "s1", &spanSource{minute: minute, pause: spanRows, closer: 140}, state, dest)
	j.Drain = drain
	j.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		mu.Lock()
		last = stateRuns(rec)
		mu.Unlock()
		if len(minuteRows(dest)) > 0 {
			done.Do(func() { close(drain) })
		}
	}
	if _, err := runner.RunAttempt(ctx, j); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the second attempt ended with %v", err)
	}
	if got, want := minuteRows(dest), []string{windowStart(60) + "/eu/" + strconv.Itoa(spanRows)}; !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v", got, want)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(last) == 0 {
		t.Error("the second attempt's last record names no state, though the closing row's minute is open")
	}
	for _, r := range last {
		if slices.Contains(named, r) || !strings.Contains(r, "/g2/") {
			t.Errorf("the second attempt's last record names %s of the closed minute", r)
		}
	}
}
