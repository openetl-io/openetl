package conformance

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/sdk/go/arrowipc"
	"github.com/unsuitable001/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// SourceSuite checks a source connector against data the test put
// upstream. The data mustn't change while the suite runs, except through
// AddRecords.
type SourceSuite struct {
	Launch Launcher
	// A config that reads the test's data.
	Config []byte
	// A config with credentials the source rejects. Nil skips that part of
	// the errors check.
	BadCredentials []byte
	// A config whose upstream doesn't answer, such as a closed port. Nil
	// skips that part of the errors check.
	Unreachable []byte
	// The streams to read, and how. Nil: every stream in the catalog, read
	// the way the source reads it first.
	Streams []Stream
	// AddRecords adds n records upstream to stream, which an incremental
	// read from where the last one ended must return. Nil skips that part
	// of the incremental check.
	AddRecords func(ctx context.Context, stream string) (n int, err error)
	// How many of a stream's resume points the resume check restarts from:
	// 0 means 5.
	ResumePoints int
	// How long one read may take: 0 means 2 minutes.
	ReadTimeout time.Duration
}

// Stream is a stream to read.
type Stream struct {
	Name string
	Mode source.SyncMode
	// Empty: the catalog's default.
	Cursor string
}

// Test runs the suite's checks as subtests of t.
func (s SourceSuite) Test(t *testing.T) {
	r := newSourceRun(s)
	t.Cleanup(r.close)
	test(t, r.checks())
}

// Run runs the suite's checks outside go test, reporting each one's result.
func (s SourceSuite) Run(ctx context.Context, report func(Result)) {
	r := newSourceRun(s)
	defer r.close()
	run(ctx, r.checks(), report)
}

type sourceRun struct {
	*common
	s SourceSuite
	// The platform reads a pipeline's streams under one pipeline ID, which
	// names what the source keeps upstream for it, such as a slot.
	readAs string

	catalog    map[string]*protocolv1.CatalogStream
	order      []string
	catalogErr error
	full       *readResult
	fullReq    []*protocolv1.ConfiguredStream
	fullErr    error
}

func newSourceRun(s SourceSuite) *sourceRun {
	return &sourceRun{s: s, common: &common{
		launch: s.Launch, kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE,
		config: s.Config, badCredentials: s.BadCredentials, unreachable: s.Unreachable,
		unreachableCodes: []protocolv1.ErrorCode{protocolv1.ErrorCode_ERROR_CODE_TRANSIENT},
	}}
}

func (r *sourceRun) checks() []Check {
	return append(r.common.checks(),
		Check{"discover", r.checkDiscover},
		Check{"read/full", r.checkFull},
		Check{"read/resume", r.checkResume},
		Check{"read/incremental", r.checkIncremental},
	)
}

func (r *sourceRun) readTimeout() time.Duration {
	if r.s.ReadTimeout > 0 {
		return r.s.ReadTimeout
	}
	return 2 * time.Minute
}

func (r *sourceRun) discover(t T) map[string]*protocolv1.CatalogStream {
	t.Helper()
	p := r.process(t)
	if r.catalog == nil && r.catalogErr == nil {
		resp, err := protocolv1.NewSourceServiceClient(p.Conn()).Discover(t.Context(), &protocolv1.DiscoverRequest{Config: p.Config()})
		if err != nil {
			r.catalogErr = rpcerr.FromStatus(err)
		} else {
			r.catalog = map[string]*protocolv1.CatalogStream{}
			for _, s := range resp.GetCatalog().GetStreams() {
				if _, dup := r.catalog[s.GetName()]; dup {
					r.catalogErr = fmt.Errorf("the catalog lists stream %q twice", s.GetName())
				}
				r.catalog[s.GetName()] = s
				r.order = append(r.order, s.GetName())
			}
		}
	}
	if r.catalogErr != nil {
		t.Fatalf("Discover: %v", r.catalogErr)
	}
	return r.catalog
}

// Discover: schemas are valid, and keys and cursors name their fields.
func (r *sourceRun) checkDiscover(t T) {
	cat := r.discover(t)
	if len(cat) == 0 {
		t.Fatalf("the catalog is empty")
	}
	for _, name := range r.order {
		s := cat[name]
		if name == "" {
			t.Errorf("a stream has no name")
		}
		fields := map[string]bool{}
		if b := s.GetArrowSchema(); len(b) > 0 {
			sch, err := arrowipc.DecodeSchema(b)
			if err != nil {
				t.Errorf("%s: the Arrow schema doesn't decode: %v", name, err)
			} else if sch, _ = source.SplitDeclared(sch); sch != nil {
				for _, f := range sch.Fields() {
					fields[f.Name] = true
				}
			}
		}
		if b := s.GetJsonSchema(); len(b) > 0 {
			if _, err := compileSchema(b); err != nil {
				t.Errorf("%s: the JSON Schema doesn't compile: %v", name, err)
			}
		}
		if len(fields) > 0 {
			for _, k := range s.GetPrimaryKey() {
				if !fields[k] {
					t.Errorf("%s: primary key %q isn't a field of the schema", name, k)
				}
			}
			if c := s.GetDefaultCursor(); c != "" && !fields[c] {
				t.Errorf("%s: cursor %q isn't a field of the schema", name, c)
			}
		}
		if s.GetSourceDefinedCursor() && s.GetDefaultCursor() == "" {
			t.Errorf("%s: the cursor is source-defined, but there's no default cursor", name)
		}
		seen := map[protocolv1.SyncMode]bool{}
		for _, m := range s.GetSyncModes() {
			if m == protocolv1.SyncMode_SYNC_MODE_UNSPECIFIED || seen[m] {
				t.Errorf("%s: sync modes %v repeat one or name an unspecified one", name, s.GetSyncModes())
				break
			}
			seen[m] = true
		}
		if seen[protocolv1.SyncMode_SYNC_MODE_INCREMENTAL] && s.GetDefaultCursor() == "" {
			flagf(t, "%s: an incremental stream with no default cursor needs one configured", name)
		}
	}
	for _, st := range r.s.Streams {
		s, ok := cat[st.Name]
		if !ok {
			t.Errorf("the suite reads %q, which the catalog doesn't list", st.Name)
			continue
		}
		if m := protocolv1.SyncMode(st.Mode); len(s.GetSyncModes()) > 0 && !slices.Contains(s.GetSyncModes(), m) { //nolint:gosec // small
			t.Errorf("the suite reads %s with %s, which the catalog doesn't list for it", st.Name, m)
		}
	}
}

// streams are what the suite reads, as the platform configures them.
func (r *sourceRun) streams(t T) []*protocolv1.ConfiguredStream {
	t.Helper()
	cat := r.discover(t)
	var out []*protocolv1.ConfiguredStream
	if len(r.s.Streams) == 0 {
		for _, name := range r.order {
			s := cat[name]
			mode := protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH
			if len(s.GetSyncModes()) > 0 {
				mode = s.GetSyncModes()[0]
			}
			out = append(out, &protocolv1.ConfiguredStream{Name: name, SyncMode: mode, Cursor: s.GetDefaultCursor()})
		}
		return out
	}
	for _, st := range r.s.Streams {
		cursor := st.Cursor
		if cursor == "" {
			cursor = cat[st.Name].GetDefaultCursor()
		}
		out = append(out, &protocolv1.ConfiguredStream{Name: st.Name, SyncMode: protocolv1.SyncMode(st.Mode), Cursor: cursor}) //nolint:gosec // small
	}
	return out
}

func (r *sourceRun) read(t T, streams ...*protocolv1.ConfiguredStream) *readResult {
	t.Helper()
	p := r.process(t)
	ctx, cancel := context.WithTimeout(t.Context(), r.readTimeout())
	defer cancel()
	if r.readAs == "" {
		r.readAs = r.pipeline()
	}
	res, err := read(ctx, p.Conn(), &protocolv1.ReadRequest{Config: p.Config(), Streams: streams, Pipeline: r.readAs})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.err != nil {
		if ctx.Err() != nil {
			t.Fatalf("a bounded read didn't end within %s", r.readTimeout())
		}
		t.Fatalf("read: %s: %v", codeOf(res.err), res.err)
	}
	return res
}

func (r *sourceRun) fullRead(t T) (*readResult, []*protocolv1.ConfiguredStream) {
	t.Helper()
	if r.full == nil && r.fullErr == nil {
		r.fullReq = r.streams(t)
		done := false
		defer func() {
			if !done {
				r.fullErr = fmt.Errorf("the full read failed")
			}
		}()
		r.full = r.read(t, r.fullReq...)
		done = true
	}
	if r.fullErr != nil {
		t.Fatalf("%v", r.fullErr)
	}
	return r.full, r.fullReq
}

// Read: full: every stream completes once and nothing follows; records
// match the declared schema; primary keys are unique.
func (r *sourceRun) checkFull(t T) {
	res, streams := r.fullRead(t)
	cat := r.discover(t)
	asked := map[string]*protocolv1.ConfiguredStream{}
	for _, s := range streams {
		asked[s.GetName()] = s
	}
	for _, e := range res.events {
		if asked[e.stream] == nil && e.stream != "" {
			t.Errorf("the read sent a message for %q, which it wasn't asked for", e.stream)
			break
		}
	}
	for _, cs := range streams {
		name := cs.GetName()
		events := res.of(name)
		completes, batches, mid := 0, 0, 0
		for i, e := range events {
			if completes > 0 && (e.batch || e.resume) {
				t.Errorf("%s: a %s came after the stream completed", name, kindOf(e))
				break
			}
			switch {
			case e.complete:
				completes++
			case e.batch:
				batches++
			case e.resume && len(rowsAfter(events, i)) > 0:
				mid++
			}
		}
		if completes != 1 {
			t.Errorf("%s: completed %d times, not once", name, completes)
		}
		if mid == 0 && batches > 1 {
			flagf(t, "%s: %d batches and no resume point between them, so a retry reads it again from the start", name, batches)
		}
		decl, err := arrowipc.DecodeSchema(cat[name].GetArrowSchema())
		if err != nil {
			continue
		}
		decl, _ = source.SplitDeclared(decl)
		checkRecords(t, name, decl, events)
		checkKeys(t, name, cat[name].GetPrimaryKey(), cs.GetSyncMode(), events)
	}
}

func kindOf(e event) string {
	switch {
	case e.batch:
		return "batch"
	case e.resume:
		return "resume point"
	case e.complete:
		return "completion"
	}
	return "progress message"
}

// checkRecords: each field declared has its declared type, or carries text
// that the platform parses; non-nullable fields have no nulls; records have
// no fields the schema doesn't declare.
func checkRecords(t T, name string, decl *arrow.Schema, events []event) {
	t.Helper()
	if decl == nil {
		return
	}
	for _, e := range events {
		if e.schema == nil {
			continue
		}
		for _, f := range e.schema.Fields() {
			if _, ok := f.Metadata.GetValue("openetl.control"); ok {
				continue
			}
			i := decl.FieldIndices(f.Name)
			if len(i) == 0 {
				t.Errorf("%s: records have field %q, which the schema doesn't declare", name, f.Name)
				return
			}
			want := decl.Field(i[0])
			if !arrow.TypeEqual(f.Type, want.Type) && f.Type.ID() != arrow.STRING && f.Type.ID() != arrow.LARGE_STRING {
				t.Errorf("%s: field %q is %s, not its declared %s", name, f.Name, f.Type, want.Type)
				return
			}
			if !want.Nullable {
				for _, row := range e.rows {
					if row.fields[f.Name] == "null" {
						t.Errorf("%s: non-nullable field %q is null in %s", name, f.Name, row.text)
						return
					}
				}
			}
		}
	}
}

// checkKeys: every record has its key, and no two share one, except in a
// changelog.
func checkKeys(t T, name string, key []string, mode protocolv1.SyncMode, events []event) {
	t.Helper()
	if len(key) == 0 {
		return
	}
	changelog := mode == protocolv1.SyncMode_SYNC_MODE_CDC || mode == protocolv1.SyncMode_SYNC_MODE_STREAM
	seen := map[string]bool{}
	for _, row := range allRows(events) {
		k := ""
		for _, f := range key {
			v, ok := row.fields[f]
			if !ok || v == "null" {
				t.Errorf("%s: a record has no primary key %q: %s", name, f, row.text)
				return
			}
			k += v + "\x00"
		}
		if seen[k] && !changelog {
			t.Errorf("%s: two records share primary key %s", name, k)
			return
		}
		seen[k] = true
	}
}

// Read: resume: restarting a stream from a resume point reads every
// record that followed it, and with replay exact nothing else.
func (r *sourceRun) checkResume(t T) {
	res, streams := r.fullRead(t)
	cat := r.discover(t)
	n := r.s.ResumePoints
	if n <= 0 {
		n = 5
	}
	tried := 0
	for _, cs := range streams {
		events := res.of(cs.GetName())
		var points []int
		for i, e := range events {
			if e.resume {
				points = append(points, i)
			}
		}
		for _, i := range spread(points, n) {
			tried++
			remaining := countRows(rowsAfter(events, i))
			st := &protocolv1.ConfiguredStream{Name: cs.GetName(), SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor(), State: events[i].state}
			again := r.read(t, st).of(cs.GetName())
			got := countRows(allRows(again))
			if missing := remaining.minus(got); len(missing) > 0 {
				t.Errorf("%s: restarting from resume point %d skipped %d of the %d records that followed it: %s",
					cs.GetName(), slices.Index(points, i)+1, len(missing), len(rowsAfter(events, i)), sample(missing))
			}
			if cat[cs.GetName()].GetReplay() == protocolv1.Replay_REPLAY_EXACT {
				if extra := got.minus(remaining); len(extra) > 0 {
					t.Errorf("%s: the replay is exact, but restarting from resume point %d read %d records again: %s",
						cs.GetName(), slices.Index(points, i)+1, len(extra), sample(extra))
				}
			}
			if !completed(again) {
				t.Errorf("%s: the read from resume point %d didn't complete", cs.GetName(), slices.Index(points, i)+1)
			}
		}
	}
	if tried == 0 {
		t.Skipf("no stream sent a resume point")
	}
}

// spread picks up to n of points, the first and last among them.
func spread(points []int, n int) []int {
	if len(points) <= n {
		return points
	}
	out := make([]int, 0, n)
	for i := range n {
		out = append(out, points[i*(len(points)-1)/(n-1)])
	}
	return slices.Compact(out)
}

func completed(events []event) bool {
	return slices.ContainsFunc(events, func(e event) bool { return e.complete })
}

func completion(events []event) (event, bool) {
	for _, e := range events {
		if e.complete {
			return e, true
		}
	}
	return event{}, false
}

// Read: incremental: a stream says where its next sync starts, and a sync
// from there with nothing new upstream reads nothing new; records added
// upstream are read by the next sync.
func (r *sourceRun) checkIncremental(t T) {
	res, streams := r.fullRead(t)
	tried := 0
	for _, cs := range streams {
		if cs.GetSyncMode() != protocolv1.SyncMode_SYNC_MODE_INCREMENTAL {
			continue
		}
		tried++
		name := cs.GetName()
		first := res.of(name)
		end, _ := completion(first)
		if !end.hasNext {
			t.Errorf("%s: an incremental read completed without saying where the next one starts", name)
			continue
		}
		seen := countRows(allRows(first))
		next := &protocolv1.ConfiguredStream{Name: name, SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor(), State: end.next}
		second := r.read(t, next).of(name)
		if fresh := novel(allRows(second), seen); len(fresh) > 0 {
			t.Errorf("%s: with nothing new upstream, the next read returned %d records the first didn't: %s", name, len(fresh), sample(fresh))
		}
		end2, ok := completion(second)
		if !ok || !end2.hasNext {
			t.Errorf("%s: the second incremental read didn't say where the next one starts", name)
			continue
		}
		if r.s.AddRecords == nil {
			continue
		}
		added, err := r.s.AddRecords(t.Context(), name)
		if err != nil {
			t.Fatalf("AddRecords(%s): %v", name, err)
		}
		for k, v := range countRows(allRows(second)) {
			seen[k] += v
		}
		third := r.read(t, &protocolv1.ConfiguredStream{Name: name, SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor(), State: end2.next}).of(name)
		if fresh := novel(allRows(third), seen); len(fresh) < added {
			t.Errorf("%s: %d records were added upstream, but the next read returned %d new ones", name, added, len(fresh))
		}
	}
	if tried == 0 {
		t.Skipf("the suite reads no stream incrementally")
	}
}

// novel are rows not in seen.
func novel(rows []row, seen multiset) []string {
	var out []string
	for _, r := range rows {
		if seen[r.text] == 0 {
			out = append(out, r.text)
		}
	}
	return out
}
