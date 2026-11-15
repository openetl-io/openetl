package engineclient

import (
	"context"
	"errors"
	"slices"
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

// Two row-wise statements read a keyed stream into a destination whose
// writer lanes commit on their own: by_id keeps the key, so the exchange
// between the workers' engines sends its rows to the writer lane of their
// key group, and shifted drops it, so each lane writes its rows on its own
// writer lane. A worker lost once writer lane 0 holds rows of both, by_id's
// from another worker's split among them, leaves the epoch committed in
// part. The next attempt's engines run the statements again on what each
// lost lane's committable names, keep by_id's rows of its key groups from
// every split and shifted's of its own lanes' splits alone, and commit the
// lane as it was: every row lands once.
func TestASQLRedoThroughTheEngine(t *testing.T) {
	opts := options(t, nil)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	const sql = "INSERT INTO by_id SELECT id FROM s; INSERT INTO shifted SELECT id + 1000000 AS id FROM s"
	declared := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	raw, err := arrowipc.EncodeSchema(declared)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: sql, Streams: []sqlplan.Stream{{Name: "s", DeclaredSchema: raw, Key: []string{"id"}}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	ex := expected(t, a)
	if len(ex) != 2 || !slices.Equal(ex[0].PrimaryKey, []string{"id"}) || len(ex[1].PrimaryKey) != 0 {
		t.Fatalf("the statements' outputs: %+v", ex)
	}
	srv := listen(t)
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	dest := &txnLanes{t: t, producers: map[int]uint64{}, open: map[int]map[txnMark][]int64{}, aborted: map[txnMark][]int64{}, marked: map[txnMark]bool{}}
	const chunks = 150
	job := func(owner string) func() runner.Job {
		return func() runner.Job {
			return runner.Job{
				Pipeline: "p", Owner: owner, Sync: "s1", Log: log, Source: &chunkSource{splits: 3, chunks: chunks, rows: 50, pause: 4 * time.Millisecond},
				Streams: []runner.Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Schema: declared}},
				Outputs: []runner.Stream{
					{Name: "by_id", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Transformed: true, From: "s"},
					{Name: "shifted", Replay: source.Exact, Mode: destination.Append, Transformed: true, From: "s"},
				},
				SQL:         &runner.SQLTransform{SQL: sql, Expected: ex},
				Destination: dest, Dataflow: &Dataflow{Options: opts}, Lanes: 2, Logger: opts.Logger.With("job", owner),
				Settings: delivery.Settings{Require: delivery.ExactlyOnce, CommitMaxBytes: 16 << 10, Scope: destination.ScopePipeline},
			}
		}
	}

	// Each of three workers reads a split, slowly enough for many epochs.
	// Writer lane 0's commit fails, and its worker is lost, in its second
	// epoch with rows or later that holds rows of both outputs and by_id's
	// of an id whose shifted row isn't there.
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
		if failing == nil {
			dest.mu.Lock()
			got := dest.open[0][m]
			dest.mu.Unlock()
			shifted := slices.ContainsFunc(got, func(id int64) bool { return id >= 1000000 })
			brought := slices.ContainsFunc(got, func(id int64) bool { return id < 1000000 && !slices.Contains(got, id+1000000) })
			if held++; held >= 2 && shifted && brought {
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
	if failing == nil || head.Epoch != failing.N || len(head.Coverage) != len(head.Committables) || slices.ContainsFunc(head.Splits("s"), func(sp checkpoint.SplitState) bool { return sp.Complete }) {
		t.Fatalf("HEAD is epoch %d, failing %v, with %d coverages of %d committables and splits %v", head.Epoch, failing, len(head.Coverage), len(head.Committables), head.Splits("s"))
	}
	// Writer lane 0 holds shifted's rows of its own worker's split, and
	// by_id's of another worker's.
	outputs := map[string][]string{}
	for _, c := range head.Coverage[0].Splits {
		outputs[c.ID] = append(outputs[c.ID], c.Output)
		if (c.Output == "by_id") != (c.KeyGroups != nil) {
			t.Errorf("writer lane 0 names split %s of %s with key groups %v", c.ID, c.Output, c.KeyGroups)
		}
	}
	both, byID := 0, 0
	for _, o := range outputs {
		switch {
		case slices.Equal(o, []string{"by_id", "shifted"}):
			both++
		case slices.Equal(o, []string{"by_id"}):
			byID++
		}
	}
	if both == 0 || byID == 0 {
		t.Fatalf("writer lane 0's coverage %v; want a split of both outputs, and one of by_id alone", outputs)
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
	seen := map[int64]bool{}
	for _, id := range dest.rows {
		if seen[id] {
			t.Fatalf("row %d landed twice", id)
		}
		seen[id] = true
	}
	for id := range int64(3 * chunks * 50) {
		if !seen[id] || !seen[id+1000000] {
			t.Fatalf("row %d of by_id landed: %v, of shifted: %v", id, seen[id], seen[id+1000000])
		}
	}
	if want := 2 * 3 * chunks * 50; len(seen) != want {
		t.Errorf("landed %d rows, want %d", len(seen), want)
	}
}
