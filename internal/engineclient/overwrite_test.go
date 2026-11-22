package engineclient

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// splitRows reads one stream in splits, each emitting its rows, if it has
// any, and then completing.
type splitRows struct {
	rows map[string][]string
}

func (s *splitRows) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	var out []source.Split
	for _, id := range slices.Sorted(func(yield func(string) bool) {
		for id := range s.rows {
			if !yield(id) {
				return
			}
		}
	}) {
		out = append(out, source.Split{ID: id})
	}
	return out, nil
}

func (s *splitRows) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

func (s *splitRows) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if rs := s.rows[sp.ID]; len(rs) > 0 {
			if err := e.Rows(rows(rs...)); err != nil {
				return err
			}
		}
		return e.Complete(nil)
	})
}

// callDest logs what the runner calls on its writers, in order, and the
// schemas it applies.
type callDest struct {
	// Zero: one.
	lanes   int
	mu      sync.Mutex
	calls   []string
	applied []*arrow.Schema
}

func (d *callDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline},
		WriterLanes: d.lanes, WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite}}
}

func (d *callDest) Open(context.Context, destination.OpenConfig) (destination.Writer, error) {
	return d, nil
}

func (d *callDest) call(format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls = append(d.calls, fmt.Sprintf(format, args...))
}

func (d *callDest) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range streams {
		d.calls = append(d.calls, "apply "+s.Name)
		d.applied = append(d.applied, s.Schema)
	}
	return destination.SchemaApplied{}, nil
}

func (d *callDest) StartReads(context.Context, map[string]time.Time) error { return nil }

func (d *callDest) EndRead(_ context.Context, e destination.Epoch, stream string) error {
	d.call("end %s %d", stream, e.N)
	return nil
}

func (d *callDest) Write(_ context.Context, e destination.Epoch, stream string, _ arrow.RecordBatch) error {
	d.call("write %s %d", stream, e.N)
	return nil
}

func (d *callDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	return fmt.Appendf(nil, "%d", e.N), nil
}

func (d *callDest) Commit(_ context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
	d.call("commit %d", e.N)
	return destination.Committed{}, nil
}

func (d *callDest) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	return destination.Applied, nil
}

func (d *callDest) Close(context.Context) error { return nil }

var (
	_ destination.SchemaApplier = (*callDest)(nil)
	_ destination.ReadTracker   = (*callDest)(nil)
)

// overwriteJob reads stream users, declared as id and Name, with
// full_refresh, and overwrites it.
func overwriteJob(t *testing.T, log *checkpoint.Log, sync string, src source.Source, dest destination.Destination, lanes int) runner.Job {
	declared := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "Name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	return runner.Job{
		Owner: "o-" + sync, Pipeline: "p", Sync: sync, Log: log, Source: src, Destination: dest,
		Streams:  []runner.Stream{{Name: "users", Replay: source.Exact, Read: source.FullRefresh, Mode: destination.Overwrite, Schema: declared}},
		Dataflow: &Dataflow{Options: options(t, nil)}, Lanes: lanes,
		Settings: delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline},
	}
}

// The epoch the read of users ended in, from the calls.
func endedIn(t *testing.T, calls []string) uint64 {
	t.Helper()
	for _, c := range calls {
		var n uint64
		if _, err := fmt.Sscanf(c, "end users %d", &n); err == nil {
			return n
		}
	}
	t.Fatalf("the read of users never ended: %v", calls)
	return 0
}

func columnsOf(s *arrow.Schema) []string {
	var out []string
	for _, f := range s.Fields() {
		if !strings.HasPrefix(f.Name, "_openetl_") {
			out = append(out, f.Name)
		}
	}
	return out
}

func wrote(calls []string) bool {
	return slices.ContainsFunc(calls, func(c string) bool { return strings.HasPrefix(c, "write ") })
}

// sameSchema compares fields and metadata, which comes in no particular
// order.
func sameSchema(a, b *arrow.Schema) bool {
	meta := func(s *arrow.Schema) map[string]string { return s.Metadata().ToMap() }
	return slices.EqualFunc(a.Fields(), b.Fields(), func(x, y arrow.Field) bool { return x.Equal(y) }) && maps.Equal(meta(a), meta(b))
}

// An overwrite whose first read has no rows applies the schema its declared
// types shape to before the epoch its read ends in commits, so the
// destination replaces its table, or creates it, empty. A later read whose
// rows carry only declared columns changes nothing of it.
func TestAnEmptyFirstOverwriteAppliesItsDeclaredSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")

	first := &callDest{}
	empty := &splitRows{rows: map[string][]string{"0": nil}}
	if _, err := runner.RunAttempt(ctx, overwriteJob(t, log, "s1", empty, first, 1)); err != nil {
		t.Fatal(err)
	}
	n := endedIn(t, first.calls)
	apply := slices.Index(first.calls, "apply users")
	commit := slices.Index(first.calls, fmt.Sprintf("commit %d", n))
	if apply < 0 || commit < 0 || apply > commit {
		t.Fatalf("calls %v: want users' schema applied before epoch %d commits", first.calls, n)
	}
	if wrote(first.calls) {
		t.Errorf("calls %v: an empty read wrote", first.calls)
	}
	schema := first.applied[0]
	if got := columnsOf(schema); !slices.Equal(got, []string{"id", "name"}) {
		t.Errorf("applied columns %v, want id and name", got)
	}
	if table, _ := schema.Metadata().GetValue("openetl.table"); table != "users" {
		t.Errorf("applied table %q, want users", table)
	}

	second := &callDest{}
	var reported [][]byte
	full := &splitRows{rows: map[string][]string{"0": {`{"id": 1, "Name": "a"}`, `{"Name": "b", "id": 2}`}}}
	job := overwriteJob(t, log, "s2", full, second, 1)
	job.OnSchemas = func(_ destination.Epoch, schemas, _ map[string][]byte) {
		if s, ok := schemas["users"]; ok {
			reported = append(reported, s)
		}
	}
	if _, err := runner.RunAttempt(ctx, job); err != nil {
		t.Fatal(err)
	}
	if !wrote(second.calls) {
		t.Fatalf("calls %v: the second read wrote nothing", second.calls)
	}
	for _, s := range second.applied {
		if !sameSchema(s, schema) {
			t.Errorf("the second read applied %v, which isn't the first's %v", s, schema)
		}
	}
	for _, b := range reported {
		s, err := decodeSchema(b)
		if err != nil {
			t.Fatal(err)
		}
		if !sameSchema(s, schema) {
			t.Errorf("the second read reported %v, which isn't the first's %v", s, schema)
		}
	}
}

// Where one split has rows and another none, the stream's schema is its
// rows' alone, whichever lane's barrier ends the read.
func TestAnOverwriteWithRowsInOneSplitTakesNoDeclaredSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	dest := &callDest{}
	src := &splitRows{rows: map[string][]string{"0": {`{"id": 1}`}, "1": nil}}
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	if _, err := runner.RunAttempt(ctx, overwriteJob(t, log, "s1", src, dest, 2)); err != nil {
		t.Fatal(err)
	}
	if len(dest.applied) == 0 {
		t.Fatalf("calls %v: nothing applied", dest.calls)
	}
	for _, s := range dest.applied {
		if got := columnsOf(s); !slices.Equal(got, []string{"id"}) {
			t.Errorf("applied columns %v, want the rows' id alone", got)
		}
	}
}

// On workers, the declared schema of an empty overwrite is proposed to the
// coordinator, which owns the stream's schema, and is applied and recorded
// before the epoch its read ends in commits.
func TestAnEmptyOverwriteOnWorkersAppliesItsDeclaredSchema(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	srv, err := attempt.Listen(ctx, attempt.ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := &callDest{lanes: 4}
	src := &splitRows{rows: map[string][]string{"0": nil, "1": nil}}
	timings := attempt.Options{Heartbeat: 100 * time.Millisecond, Timeout: 10 * time.Second}
	var workers sync.WaitGroup
	j := overwriteJob(t, log, "s1", src, dest, 2)
	j.Workers = &runner.Workers{Count: 2, Server: srv, Options: timings, Launch: func(_ context.Context, targets []attempt.Target) error {
		for _, tg := range targets {
			wj := overwriteJob(t, nil, "s1", src, dest, 2)
			workers.Go(func() {
				if err := runner.RunWorker(ctx, wj, tg, timings); err != nil {
					t.Errorf("worker %d: %v", tg.Worker, err)
				}
			})
		}
		return nil
	}}
	_, err = runner.RunAttempt(ctx, j)
	workers.Wait()
	if err != nil {
		t.Fatal(err)
	}
	dest.mu.Lock()
	calls := slices.Clone(dest.calls)
	dest.mu.Unlock()
	n := endedIn(t, calls)
	apply := slices.Index(calls, "apply users")
	commit := slices.Index(calls, fmt.Sprintf("commit %d", n))
	if apply < 0 || commit < 0 || apply > commit {
		t.Fatalf("calls %v: want users' schema applied before epoch %d commits", calls, n)
	}
	head, err := log.Latest(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recorded, err := arrowipc.DecodeSchema(head.Streams["users"].Schema)
	if err != nil {
		t.Fatal(err)
	}
	if !sameSchema(recorded, dest.applied[0]) {
		t.Errorf("recorded %v, applied %v", recorded, dest.applied[0])
	}
}
