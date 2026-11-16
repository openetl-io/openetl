package engineclient

import (
	"context"
	"errors"
	"io/fs"
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
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A window statement edited between two attempts: the pipeline's version
// drops the old operator, whose state the record holds on object storage.
// The next attempt runs, its new operator starts from the record's
// position with no state, its records stop naming the old operator's
// state, and retention deletes the old operator's runs.
func TestAnEditedWindowStartsFromNowAndItsOldStateGoes(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	o := leaveAWindowOpen(ctx, t)
	log, state, dir, dest, minute, oldOp, pos := o.log, o.state, o.dir, o.dest, o.minute, o.op, o.pos
	const after = "INSERT INTO minutes SELECT window_start, region, count(*) AS n, max(id) AS top FROM s GROUP BY tumble(at, INTERVAL '1 minute'), region"

	// The edited statement's version drops the old operator.
	edited := editJob(ctx, t, log, state, dest, after, []string{oldOp})
	newOp := edited.SQL.Operators[0]
	if newOp == oldOp {
		t.Fatal("the edit kept the operator")
	}
	drain := make(chan struct{})
	var mu sync.Mutex
	var named, wrote bool
	var done sync.Once
	edited.Owner, edited.Sync, edited.Source = "a2", "s1", &spanSource{minute: minute, pause: spanRows, closer: 140}
	edited.Drain, edited.KeepRecords = drain, 2
	edited.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		mu.Lock()
		for _, sn := range rec.Snapshots {
			named = named || sn.Operator == oldOp
			wrote = wrote || sn.Operator == newOp && len(sn.Runs) > 0
		}
		mu.Unlock()
		if len(minuteRows(dest)) > 0 && len(runsOf(t, dir, oldOp)) == 0 {
			done.Do(func() { close(drain) })
		}
	}
	if _, err := runner.RunAttempt(ctx, edited); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the edited attempt ended with %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if named {
		t.Errorf("a record of the edited attempt names %s's state", oldOp)
	}
	if !wrote {
		t.Errorf("no record of the edited attempt names state of %s", newOp)
	}
	// The minute holds only the rows read after the restored position.
	if got, want := minuteRows(dest), []string{windowStart(60) + "/eu/" + strconv.Itoa(spanRows-pos)}; !slices.Equal(got, want) {
		t.Errorf("applied %v, want %v", got, want)
	}
	if left := runsOf(t, dir, oldOp); len(left) > 0 {
		t.Errorf("retention left %s's runs %v", oldOp, left)
	}
}

// A version without transform.sql drops every operator the one before kept:
// its first attempt restores the record without their state, runs, and
// retention deletes their runs.
func TestRemovingTheTransformDropsItsState(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	o := leaveAWindowOpen(ctx, t)
	plain := editJob(ctx, t, o.log, o.state, o.dest, "", []string{o.op})
	drain := make(chan struct{})
	var mu sync.Mutex
	var named bool
	var done sync.Once
	plain.Owner, plain.Sync, plain.Source = "a2", "s1", &spanSource{minute: o.minute, pause: spanRows, closer: 140}
	plain.Drain, plain.KeepRecords = drain, 2
	plain.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		mu.Lock()
		named = named || len(rec.Snapshots) > 0
		mu.Unlock()
		if len(runsOf(t, o.dir, o.op)) == 0 {
			done.Do(func() { close(drain) })
		}
	}
	if _, err := runner.RunAttempt(ctx, plain); !errors.Is(err, runner.ErrDrained) {
		t.Fatalf("the attempt without SQL ended with %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if named {
		t.Error("a record of the attempt without SQL names state")
	}
}

// openWindow is a pipeline whose record holds the state of a minute's
// window that's open: operator op's, at position pos of stream s.
type openWindow struct {
	log    *checkpoint.Log
	state  string
	dir    string
	dest   *keyDest
	minute int64
	op     string
	pos    int
}

// leaveAWindowOpen runs a count of each minute's rows by region until a
// record names its state, with the minute open, on a checkpoint log and
// state that share one file store, as a pipeline's bucket.
func leaveAWindowOpen(ctx context.Context, t *testing.T) openWindow {
	t.Helper()
	dir := t.TempDir()
	store, err := checkpoint.NewFSStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	o := openWindow{
		log: checkpoint.NewLog(store, "org", "windows"), dir: dir, dest: newKeyDest(2), minute: minutes + 60_000_000,
		state: "file://" + filepath.Join(dir, "org", "windows", checkpoint.StateDir),
	}
	const before = "INSERT INTO minutes SELECT window_start, region, count(*) AS n FROM s GROUP BY tumble(at, INTERVAL '1 minute'), region"
	old := editJob(ctx, t, o.log, o.state, o.dest, before, nil)
	o.op = old.SQL.Operators[0]
	if o.op == "" {
		t.Fatal("the window has no operator")
	}
	first, stop := context.WithCancel(ctx)
	var once sync.Once
	old.Owner, old.Sync, old.Source = "a1", "s1", &spanSource{minute: o.minute, pause: 12}
	old.OnCheckpoint = func(rec *checkpoint.Record, _, _ map[string]uint64) {
		if len(stateRuns(rec)) > 0 {
			once.Do(stop)
		}
	}
	if _, err := runner.RunAttempt(first, old); !errors.Is(err, context.Canceled) {
		t.Fatalf("the first attempt ended with %v", err)
	}
	head, err := o.log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	o.pos, _ = strconv.Atoi(string(head.Splits("s")[0].State))
	if o.pos == 0 || o.pos >= spanRows || len(runsOf(t, dir, o.op)) == 0 {
		t.Fatalf("the first attempt left no state of %s, or read the minute whole: position %d", o.op, o.pos)
	}
	return o
}

// editJob is a job of sql, a group window over stream s read with stream,
// in a version that drops the operators dropped. Without sql, stream s
// passes through.
func editJob(ctx context.Context, t *testing.T, log *checkpoint.Log, stateURL string, dest destination.Destination, sql string, dropped []string) runner.Job {
	t.Helper()
	opts := options(t, nil)
	s := runner.Stream{
		Name: "s", Replay: source.Exact, Mode: destination.Append, Read: source.Stream,
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "region", Type: arrow.BinaryTypes.String, Nullable: true},
			{Name: "at", Type: &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, Nullable: true},
		}, nil),
		EventTime: &runner.EventTime{Column: "at", Idle: time.Hour},
	}
	declared, err := arrowipc.EncodeSchema(s.Schema)
	if err != nil {
		t.Fatal(err)
	}
	j := runner.Job{
		Pipeline: "windows", Continuous: true, Log: log,
		Streams: []runner.Stream{s}, DroppedOperators: dropped,
		Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger,
		StateURL: stateURL,
		Settings: delivery.Settings{Require: delivery.AtLeastOnce, CommitInterval: 20 * time.Millisecond, Scope: destination.ScopePipeline},
	}
	if sql == "" {
		j.Streams[0].EventTime = nil
		return j
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: declared, EventTime: "at"}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	exp := expected(t, a)
	exp[0].Operator = a.Statements[0].Operator
	j.Outputs = []runner.Stream{{Name: "minutes", Replay: source.Exact, Mode: destination.Merge, PrimaryKey: []string{"window_start", "region"}, Transformed: true, From: "s"}}
	j.SQL = &runner.SQLTransform{SQL: sql, Expected: exp, Operators: []string{a.Statements[0].Operator}}
	return j
}

// runsOf are the files of operator op's state below dir.
func runsOf(t *testing.T, dir, op string) []string {
	t.Helper()
	var out []string
	root := filepath.Join(dir, "org", "windows", checkpoint.StateDir, op)
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) {
			return fs.SkipAll
		}
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasSuffix(path, ".run") {
			out = append(out, path)
		}
		return nil
	})
	if err != nil {
		t.Error(err)
	}
	return out
}
