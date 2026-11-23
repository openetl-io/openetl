package engineclient

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// versionRow is a row of versionDest's table.
type versionRow struct {
	id, name string
	key      int64
}

// versionDest applies each epoch's rows as a destination that dedupes at
// apply does: a stream with DedupeTarget gets one row per _openetl_id in
// the epoch, and only IDs its table doesn't have yet.
type versionDest struct {
	mu      sync.Mutex
	dedupe  map[string]destination.Dedupe
	pending map[uint64]map[string][]versionRow
	tables  map[string][]versionRow
	// It takes holds, and keeps each patch's IDs and prices.
	holds   bool
	patched []string
	writers int
}

func newVersionDest() *versionDest {
	return &versionDest{dedupe: map[string]destination.Dedupe{}, pending: map[uint64]map[string][]versionRow{}, tables: map[string][]versionRow{}}
}

func (d *versionDest) Capabilities() destination.Capabilities {
	c := destination.Capabilities{Class: destination.StageAndApply, Scopes: []destination.Scope{destination.ScopePipeline}, DedupeAtApply: true, WriterLanes: d.writers}
	if d.holds {
		c.Holds = destination.Holds{ByID: true, Guarded: true}
	}
	return c
}

func (d *versionDest) Patch(_ context.Context, _ destination.Epoch, _ string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	ids := batch.Column(batch.Schema().FieldIndices("_openetl_id")[0])
	prices := batch.Column(batch.Schema().FieldIndices("price")[0])
	for i := range int(batch.NumRows()) {
		d.patched = append(d.patched, ids.ValueStr(i)+" "+prices.ValueStr(i))
	}
	return nil
}

// ids are the IDs of the stream's rows of key, in the order applied.
func (d *versionDest) ids(stream string, key int64) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, r := range d.tables[stream] {
		if r.key == key {
			out = append(out, r.id)
		}
	}
	return out
}

func (d *versionDest) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range cfg.Streams {
		d.dedupe[s.Name] = s.Dedupe
	}
	return d, nil
}

func (d *versionDest) Write(_ context.Context, e destination.Epoch, stream string, batch arrow.RecordBatch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := batch.Schema()
	for _, f := range s.Fields() {
		if f.Name == "" || f.Name[0] == 0 {
			return fmt.Errorf("a hidden column %q reached the destination", f.Name)
		}
	}
	col := func(name string) (arrow.Array, error) {
		i := s.FieldIndices(name)
		if len(i) != 1 {
			return nil, fmt.Errorf("batch without %s: %v", name, s)
		}
		return batch.Column(i[0]), nil
	}
	ids, err := col("_openetl_id")
	if err != nil {
		return err
	}
	keys, err := col("id")
	if err != nil {
		return err
	}
	names, err := col("name")
	if err != nil {
		return err
	}
	if d.pending[e.N] == nil {
		d.pending[e.N] = map[string][]versionRow{}
	}
	for i := range int(batch.NumRows()) {
		d.pending[e.N][stream] = append(d.pending[e.N][stream], versionRow{id: ids.ValueStr(i), key: keys.(*array.Int64).Value(i), name: names.ValueStr(i)})
	}
	return nil
}

func (d *versionDest) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	return destination.Staged(strconv.FormatUint(e.N, 10)), nil
}

func (d *versionDest) Commit(_ context.Context, e destination.Epoch, _ []destination.Staged) (destination.Committed, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for stream, rows := range d.pending[e.N] {
		seen := map[string]bool{}
		if d.dedupe[stream] == destination.DedupeTarget {
			for _, r := range d.tables[stream] {
				seen[r.id] = true
			}
		}
		for _, r := range rows {
			if d.dedupe[stream] == destination.DedupeTarget && seen[r.id] {
				continue
			}
			seen[r.id] = true
			d.tables[stream] = append(d.tables[stream], r)
		}
	}
	delete(d.pending, e.N)
	return destination.Committed{}, nil
}

func (d *versionDest) Recover(context.Context, uint64, destination.Epoch, []destination.Staged) (destination.Recovery, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	clear(d.pending)
	return destination.Applied, nil
}

func (d *versionDest) Close(context.Context) error { return nil }

// names are the names of the stream's rows of key, in the order applied.
func (d *versionDest) names(stream string, key int64) []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, r := range d.tables[stream] {
		if r.key == key {
			out = append(out, r.name)
		}
	}
	return out
}

// readSource reads each stream whole, the rows of reads[n] on its n-th
// read, and completes it.
type readSource struct {
	reads [][]string
	n     int
}

func (s *readSource) Read(_ context.Context, req source.ReadRequest, out source.Emitter) error {
	rows := s.reads[s.n]
	s.n++
	for _, cs := range req.Streams {
		var page []json.RawMessage
		for _, r := range rows {
			page = append(page, json.RawMessage(r))
		}
		if len(page) > 0 {
			if err := out.Rows(cs.Name, page); err != nil {
				return err
			}
		}
		if err := out.ResumePoint(cs.Name, []byte(strconv.Itoa(s.n))); err != nil {
			return err
		}
		if err := out.Complete(cs.Name, nil); err != nil {
			return err
		}
	}
	return nil
}

// twoSplits is a readSource in two splits, the second empty, so that an
// attempt has two lanes, and its rows are on the first.
type twoSplits struct{ *readSource }

func (s twoSplits) Splits(context.Context, source.ConfiguredStream, []source.Split) ([]source.Split, error) {
	return []source.Split{{ID: "0"}, {ID: "1"}}, nil
}

func (s twoSplits) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	rows := s.reads[s.n]
	s.n++
	return source.ReadSplits(ctx, req, out, func(_ context.Context, _ source.ConfiguredStream, sp source.Split, e source.SplitEmitter) error {
		if sp.ID == "0" && len(rows) > 0 {
			page := make([]json.RawMessage, len(rows))
			for i, r := range rows {
				page[i] = json.RawMessage(r)
			}
			if err := e.Rows(page); err != nil {
				return err
			}
		}
		return e.Complete([]byte(strconv.Itoa(s.n)))
	})
}

// versionsPipeline runs syncs of stream users, keyed by id and read with
// cursor at, deduped at apply, on one checkpoint log and state.
type versionsPipeline struct {
	t     *testing.T
	opts  Options
	log   *checkpoint.Log
	root  string
	state string
	dest  *versionDest
	src   *readSource
	syncs int
	// Edits each sync's job.
	edit func(*runner.Job)
}

func newVersionsPipeline(t *testing.T, mem memory.Allocator, reads ...[]string) *versionsPipeline {
	t.Helper()
	root := t.TempDir()
	store, err := checkpoint.NewFSStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return &versionsPipeline{
		t: t, opts: options(t, mem), log: checkpoint.NewLog(store, "org", "pipe"), root: root,
		state: "file://" + filepath.Join(root, "org", "pipe", checkpoint.StateDir),
		dest:  newVersionDest(), src: &readSource{reads: reads},
	}
}

func (p *versionsPipeline) job() runner.Job {
	p.syncs++
	j := runner.Job{
		Owner: fmt.Sprintf("a%d", p.syncs), Sync: fmt.Sprintf("s%d", p.syncs), Pipeline: "pipe", Log: p.log,
		Source: p.src, Destination: p.dest, Dataflow: &Dataflow{Options: p.opts}, StateURL: p.state,
		Streams: []runner.Stream{{
			Name: "users", Replay: source.Superset, Mode: destination.Append, PrimaryKey: []string{"id"},
			Read: source.Incremental, CursorField: "at",
		}},
		Settings: delivery.Settings{Require: delivery.ExactlyOnce, Scope: destination.ScopePipeline},
	}
	if p.edit != nil {
		p.edit(&j)
	}
	return j
}

func (p *versionsPipeline) sync(ctx context.Context) {
	p.t.Helper()
	if _, err := runner.RunAttempt(ctx, p.job()); err != nil {
		p.t.Fatalf("sync %d: %v", p.syncs, err)
	}
}

// A row changed twice within one value of its cursor, and read between
// the changes, lands both versions: the second gets an _openetl_id of its
// own, which dedupe at apply doesn't skip. Read again unchanged, it lands
// once.
func TestASecondVersionAtOneCursorValueLands(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)
	p := newVersionsPipeline(t, mem,
		[]string{`{"id": 1, "name": "v1", "at": "2024-05-01"}`, `{"id": 2, "name": "x", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v2", "at": "2024-05-01"}`, `{"id": 2, "name": "x", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v2", "at": "2024-05-01"}`, `{"id": 2, "name": "x", "at": "2024-05-01"}`},
	)
	for range 3 {
		p.sync(ctx)
	}
	if p.dest.dedupe["users"] != destination.DedupeTarget {
		t.Fatalf("users is deduped with %v, not against the target", p.dest.dedupe["users"])
	}
	if got := p.dest.names("users", 1); !slices.Equal(got, []string{"v1", "v2"}) {
		t.Errorf("row 1's versions applied: %q, want v1 then v2", got)
	}
	if got := p.dest.names("users", 2); !slices.Equal(got, []string{"x"}) {
		t.Errorf("row 2, read three times unchanged, applied as %q", got)
	}
}

// A row read again whose volatile columns alone changed, such as the time
// the source fetched it, lands once.
func TestAVolatileColumnDoesntMakeAVersion(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	defer mem.AssertSize(t, 0)
	ctx := testContext(t)
	p := newVersionsPipeline(t, mem,
		[]string{`{"id": 1, "name": "v1", "fetched": 1, "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v1", "fetched": 2, "at": "2024-05-01"}`},
	)
	p.edit = func(j *runner.Job) { j.Streams[0].VolatileColumns = []string{"fetched"} }
	for range 2 {
		p.sync(ctx)
	}
	if got := p.dest.names("users", 1); !slices.Equal(got, []string{"v1"}) {
		t.Errorf("row 1 applied as %q, want v1 once", got)
	}
}

// A reset reads its streams again from the start, and keeps the versions
// of their records, as it keeps their schemas: a version that changed since
// the last read still lands.
func TestAResetKeepsTheVersions(t *testing.T) {
	ctx := testContext(t)
	p := newVersionsPipeline(t, nil,
		[]string{`{"id": 1, "name": "v1", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v2", "at": "2024-05-01"}`},
	)
	p.sync(ctx)
	p.edit = func(j *runner.Job) { j.Resets = []runner.Reset{{ID: "r1", Streams: []string{"users"}}} }
	p.sync(ctx)
	if got := p.dest.names("users", 1); !slices.Equal(got, []string{"v1", "v2"}) {
		t.Errorf("row 1's versions applied: %q, want v1 then v2", got)
	}
}

// A pipeline's checkpoint from before the versions were kept restores and
// runs: the first read finds no versions, so a version within the cursor
// unit that was in flight keeps its ID, and is skipped as before, but the
// next one lands. So does a record whose stream's versions are no longer
// kept, which drops them, and they start again as they did after the
// upgrade. A record keeps them apart from the other state, which a runner
// from before them restores without them.
func TestVersionsStartFromARecordWithoutThem(t *testing.T) {
	ctx := testContext(t)
	p := newVersionsPipeline(t, nil,
		[]string{`{"id": 1, "name": "v1", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v2", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v3", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v4", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v5", "at": "2024-05-01"}`},
	)
	named := func() bool {
		head, err := p.log.Latest(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(head.Snapshots) > 0 {
			t.Fatalf("the record's snapshots name %+v", head.Snapshots)
		}
		return slices.ContainsFunc(head.Versions, func(s checkpoint.Snapshot) bool { return s.Operator == runner.VersionsOperator("users") })
	}
	p.edit = func(j *runner.Job) { j.StateURL = "" }
	p.sync(ctx)
	if named() {
		t.Fatal("a job without state kept versions")
	}
	p.edit = nil
	p.sync(ctx)
	p.sync(ctx)
	if !named() {
		t.Fatal("the record names no versions")
	}
	// Under at_least_once the stream isn't deduped, so it keeps none.
	p.edit = func(j *runner.Job) { j.Streams[0].Require = delivery.AtLeastOnce }
	p.sync(ctx)
	if named() {
		t.Fatal("a stream that isn't deduped kept versions")
	}
	p.edit = nil
	p.sync(ctx)
	// v4 isn't deduped, and v2 and v5 are the first reads with versions.
	if got := p.dest.names("users", 1); !slices.Equal(got, []string{"v1", "v3", "v4"}) {
		t.Errorf("row 1's versions applied: %q, want v1, v3 and v4", got)
	}
}

// A version with a held cell is held under its own ID, so the release's
// patch fills its row, not the version before it; on several writer lanes
// too, where its rows are exchanged by key.
func TestAHeldCellOfASecondVersionIsPatchedIntoItsRow(t *testing.T) {
	for _, writers := range []int{1, 2} {
		t.Run(fmt.Sprint(writers), func(t *testing.T) {
			ctx := testContext(t)
			rows := func(name, price string) []string {
				var out []string
				for id := range 8 {
					out = append(out, fmt.Sprintf(`{"id": %d, "name": %q, "price": %q, "at": "2024-05-01"}`, id, name, price))
				}
				return out
			}
			p := newVersionsPipeline(t, nil, rows("v1", "1.5"), rows("v2", "12345678901.5"), nil, rows("v2", "12345678901.5"))
			p.dest.holds, p.dest.writers = true, writers
			declared := func(precision int32) *arrow.Schema {
				return arrow.NewSchema([]arrow.Field{
					{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
					{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
					{Name: "price", Type: &arrow.Decimal128Type{Precision: precision, Scale: 2}, Nullable: true},
					{Name: "at", Type: arrow.BinaryTypes.String, Nullable: true},
				}, nil)
			}
			precision := int32(10)
			var releases []runner.Release
			p.edit = func(j *runner.Job) {
				j.HeldURL = "file://" + filepath.Join(p.root, "org", "pipe", checkpoint.HeldDir)
				j.Streams[0].Schema = declared(precision)
				j.Streams[0].Holds = &runner.StreamHolds{Cells: true}
				j.Releases = releases
				if writers > 1 {
					j.Source, j.Lanes = twoSplits{p.src}, writers
				}
			}
			p.sync(ctx)
			p.sync(ctx)
			ids := map[int64][]string{}
			var want []string
			for id := range int64(8) {
				ids[id] = p.dest.ids("users", id)
				if len(ids[id]) != 2 || ids[id][0] == ids[id][1] {
					t.Fatalf("row %d applied with IDs %q, want two", id, ids[id])
				}
				want = append(want, ids[id][1]+" 12345678901.5")
			}
			head, err := p.log.Latest(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if n := len(head.Versions); n != writers {
				t.Fatalf("%d snapshots, want one for each writer lane", n)
			}
			h := head.Streams["users"].Holds
			if h == nil || h.Cells["price"].Cells != 8 {
				t.Fatalf("holds = %+v", h)
			}
			precision = 20
			releases = []runner.Release{{Stream: "users", Column: "price", Since: h.Cells["price"].Since}}
			p.sync(ctx)
			slices.Sort(want)
			slices.Sort(p.dest.patched)
			if !slices.Equal(p.dest.patched, want) {
				t.Errorf("patched %q, want %q", p.dest.patched, want)
			}
			// Read again once it fits, it's the version it was.
			p.sync(ctx)
			for id := range int64(8) {
				if got := p.dest.ids("users", id); !slices.Equal(got, ids[id]) {
					t.Errorf("row %d applied with IDs %q after the release, want %q", id, got, ids[id])
				}
			}
		})
	}
}

// A paused stream's record carries the versions of its records, as it
// carries its position.
func TestAPausedStreamKeepsItsVersions(t *testing.T) {
	ctx := testContext(t)
	p := newVersionsPipeline(t, nil,
		[]string{`{"id": 1, "name": "v1", "at": "2024-05-01"}`},
		[]string{`{"id": 7, "name": "o", "at": "2024-05-01"}`},
		[]string{`{"id": 1, "name": "v2", "at": "2024-05-01"}`},
	)
	orders := func(j *runner.Job) {
		o := j.Streams[0]
		o.Name = "orders"
		j.Streams = append(j.Streams, o)
	}
	p.edit = orders
	p.sync(ctx)
	p.edit = func(j *runner.Job) {
		orders(j)
		j.Paused, j.Streams = j.Streams[:1], j.Streams[1:]
	}
	p.sync(ctx)
	p.edit = orders
	p.sync(ctx)
	if got := p.dest.names("users", 1); !slices.Equal(got, []string{"v1", "v2"}) {
		t.Errorf("row 1's versions applied: %q, want v1 then v2", got)
	}
}
