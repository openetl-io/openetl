package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/conformance"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

var configSchema = []byte(`{"type": "object", "properties": {
	"token": {"type": "string", "x-secret": true}, "down": {"type": "boolean"}}, "required": ["token"]}`)

var (
	good           = []byte(`{"token": "good"}`)
	badCredentials = []byte(`{"token": "bad"}`)
	unreachable    = []byte(`{"token": "good", "down": true}`)
)

type memConfig struct {
	Token string `json:"token"`
	Down  bool   `json:"down"`
}

func bind(config []byte) (memConfig, error) {
	var c memConfig
	err := json.Unmarshal(config, &c)
	return c, err
}

func (c memConfig) check() error {
	switch {
	case c.Down:
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, errors.New("connection refused"))
	case c.Token != "good":
		return sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, errors.New("bad token"))
	}
	return nil
}

// upstream is the memory source's data: users read in full, events
// incrementally by ts.
type upstream struct {
	mu     sync.Mutex
	users  int64
	events int64
	// Users carry a control sequence that each read numbers anew, as a
	// changelog's snapshot does.
	seq   bool
	reads int64
	// Changes are a log read with stream: change i has id i. A resume can't
	// go back before trimmed, as a log discards what's acknowledged.
	changes, trimmed int64
}

type memSource struct {
	c   memConfig
	up  *upstream
	bug string
}

var usersSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
}, nil)

var usersSeqSchema = arrow.NewSchema(append(usersSchema.Fields(),
	arrow.Field{Name: "_seq", Type: arrow.PrimitiveTypes.Int64, Metadata: arrow.NewMetadata([]string{source.ControlKey}, []string{"seq"})}), nil)

var changesSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
}, nil)

var eventsSchema = arrow.NewSchema([]arrow.Field{
	{Name: "id", Type: arrow.PrimitiveTypes.Int64},
	{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
}, nil)

func (s *memSource) Check(context.Context) error {
	if s.bug == "check-internal" && s.c.Token != "good" {
		return errors.New("bad token")
	}
	return s.c.check()
}

func (s *memSource) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{
		{Name: "users", PrimaryKey: []string{"id"}, Replay: source.Exact, Schema: usersSchema, SyncModes: []source.SyncMode{source.FullRefresh}},
		{Name: "events", PrimaryKey: []string{"id"}, CursorField: "ts", Replay: source.Superset, Schema: eventsSchema,
			SyncModes: []source.SyncMode{source.Incremental, source.FullRefresh}},
		{Name: "changes", Replay: source.Exact, Schema: changesSchema, SyncModes: []source.SyncMode{source.Stream}},
	}}, nil
}

func (s *memSource) Read(ctx context.Context, req source.ReadRequest, out source.Emitter) error {
	if err := s.c.check(); err != nil {
		if s.bug == "check-internal" && s.c.Token != "good" {
			return errors.New("bad token")
		}
		return err
	}
	s.up.mu.Lock()
	users, events := s.up.users, s.up.events
	s.up.reads++
	seq := s.up.reads
	if !s.up.seq {
		seq = 0
	}
	s.up.mu.Unlock()
	for _, st := range req.Streams {
		var err error
		switch st.Name {
		case "users":
			err = s.readUsers(users, seq, st.State, out)
		case "events":
			err = s.readEvents(events, st.State, out)
		case "changes":
			err = s.readChanges(ctx, req.Continuous, st.State, out)
		default:
			err = fmt.Errorf("no stream %q", st.Name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// readUsers reads users from the state on; with seq above 0, they carry it.
func (s *memSource) readUsers(n, seq int64, state []byte, out source.Emitter) error {
	var from int64
	if state != nil {
		if err := json.Unmarshal(state, &from); err != nil {
			return err
		}
		if s.bug == "resume-gap" {
			from++
		}
	}
	for i := from; i < n; i += 5 {
		to := min(i+5, n)
		ids, names := []int64{}, []string{}
		for j := i; j < to; j++ {
			id := j
			if s.bug == "dup-key" && j == 7 {
				id = 6
			}
			ids, names = append(ids, id), append(names, fmt.Sprintf("u%d", j))
		}
		batch := usersBatch(ids, names)
		if seq > 0 {
			batch = withSeq(batch, seq)
		}
		if err := out.Records("users", batch); err != nil {
			return err
		}
		st, _ := json.Marshal(to)
		if err := out.ResumePoint("users", st); err != nil {
			return err
		}
	}
	if err := out.Complete("users", nil); err != nil {
		return err
	}
	if s.bug == "batch-after-complete" {
		return out.Records("users", usersBatch([]int64{n}, []string{"late"}))
	}
	return nil
}

// Event i has ts 10i; a state is the last ts read.
func (s *memSource) readEvents(n int64, state []byte, out source.Emitter) error {
	after := int64(-1)
	if state != nil {
		if err := json.Unmarshal(state, &after); err != nil {
			return err
		}
	}
	last := after
	for i := after/10 + 1; i < n; i += 4 {
		ids, ts := []int64{}, []int64{}
		for j := i; j < min(i+4, n); j++ {
			ids, ts = append(ids, j), append(ts, 10*j)
			last = 10 * j
		}
		if err := out.Records("events", eventsBatch(ids, ts)); err != nil {
			return err
		}
		st, _ := json.Marshal(last)
		if err := out.ResumePoint("events", st); err != nil {
			return err
		}
	}
	if s.bug == "incremental-no-next" {
		return out.Complete("events", nil)
	}
	if s.bug == "incremental-skips" {
		last += 100
	}
	next, _ := json.Marshal(last)
	return out.Complete("events", next)
}

// readChanges reads the log from the state, the next id, on: up to where it
// ended when the read started, or, read continuously, until cancelled.
func (s *memSource) readChanges(ctx context.Context, continuous bool, state []byte, out source.Emitter) error {
	var from int64
	if state != nil {
		if err := json.Unmarshal(state, &from); err != nil {
			return err
		}
	}
	s.up.mu.Lock()
	end := s.up.changes
	s.up.mu.Unlock()
	caughtUp := false
	for {
		s.up.mu.Lock()
		if continuous {
			end = s.up.changes
		}
		if state != nil {
			from = max(from, s.up.trimmed)
		}
		s.up.mu.Unlock()
		if from < end {
			to := min(from+3, end)
			b := array.NewInt64Builder(memory.DefaultAllocator)
			for i := from; i < to; i++ {
				b.Append(i)
			}
			col := b.NewArray()
			b.Release()
			if err := out.Records("changes", array.NewRecordBatch(changesSchema, []arrow.Array{col}, to-from)); err != nil {
				return err
			}
			from = to
			if s.bug == "continuous-no-resume" && caughtUp {
				continue
			}
			st, _ := json.Marshal(from)
			if err := out.ResumePoint("changes", st); err != nil {
				return err
			}
			continue
		}
		if !continuous || s.bug == "continuous-completes" {
			return out.Complete("changes", nil)
		}
		caughtUp = true
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// Committed discards the log before the changes' state.
func (s *memSource) Committed(_ context.Context, states map[string][]byte, _ map[string][]source.Split) error {
	if s.bug == "committed-fails" {
		return errors.New("no")
	}
	b, ok := states["changes"]
	if !ok {
		return nil
	}
	var at int64
	if err := json.Unmarshal(b, &at); err != nil {
		return err
	}
	if s.bug == "ack-trims-too-far" {
		at += 2
	}
	s.up.mu.Lock()
	s.up.trimmed = max(s.up.trimmed, at)
	s.up.mu.Unlock()
	return nil
}

func usersBatch(ids []int64, names []string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, usersSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	return b.NewRecordBatch()
}

func withSeq(rec arrow.RecordBatch, seq int64) arrow.RecordBatch {
	defer rec.Release()
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()
	for range rec.NumRows() {
		b.Append(seq)
	}
	col := b.NewArray()
	defer col.Release()
	return array.NewRecordBatch(usersSeqSchema, append(rec.Columns(), col), rec.NumRows())
}

func eventsBatch(ids, ts []int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, eventsSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.Int64Builder).AppendValues(ts, nil)
	return b.NewRecordBatch()
}

func sourceSuite(bug string) conformance.SourceSuite {
	return memSuite(&upstream{users: 23, events: 18, changes: 14}, bug)
}

func memSuite(up *upstream, bug string) conformance.SourceSuite {
	return conformance.SourceSuite{
		Launch: conformance.InProcess(sdk.Connector{
			Name: "memory", ConfigSchema: configSchema, Egress: sdk.Egress{Tier: sdk.EgressRaw}, Acknowledges: true,
			Source: func(config []byte, _ host.Host) (source.Source, error) {
				c, err := bind(config)
				return &memSource{c: c, up: up, bug: bug}, err
			},
		}),
		Config: good, BadCredentials: badCredentials, Unreachable: unreachable,
		Streams:     []conformance.Stream{{Name: "users", Mode: source.FullRefresh}, {Name: "events", Mode: source.Incremental}, {Name: "changes", Mode: source.Stream}},
		ReadTimeout: 5 * time.Second,
		AddRecords: func(_ context.Context, stream string) (int, error) {
			up.mu.Lock()
			defer up.mu.Unlock()
			switch stream {
			case "events":
				up.events += 3
			case "changes":
				up.changes += 4
				return 4, nil
			default:
				return 0, nil
			}
			return 3, nil
		},
	}
}

func TestSourcePasses(t *testing.T) {
	sourceSuite("").Test(t)
}

// Records read again after a resume point are the same records, though
// their control sequence is new.
func TestSourceSequencesAreNotRecords(t *testing.T) {
	memSuite(&upstream{users: 23, events: 18, seq: true}, "").Test(t)
}

func TestSourceBugsFail(t *testing.T) {
	cases := map[string][]string{
		"resume-gap":           {"read/resume", "read/committed"},
		"dup-key":              {"read/full"},
		"batch-after-complete": {"read/full"},
		"incremental-no-next":  {"read/incremental"},
		"incremental-skips":    {"read/incremental"},
		"check-internal":       {"errors"},
		"committed-fails":      {"read/committed"},
		"ack-trims-too-far":    {"read/committed"},
		"continuous-completes": {"read/continuous"},
		"continuous-no-resume": {"read/continuous"},
	}
	for bug, checks := range cases {
		t.Run(bug, func(t *testing.T) {
			failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) { sourceSuite(bug).Run(ctx, report) })
			if !slices.Equal(failed, checks) {
				t.Errorf("failed checks %v, want %v", failed, checks)
			}
		})
	}
}

func runFailed(t *testing.T, run func(context.Context, func(conformance.Result))) []string {
	var failed []string
	run(t.Context(), func(r conformance.Result) {
		if r.Failed {
			failed = append(failed, r.Check)
			t.Logf("%s: %s", r.Check, strings.Join(r.Log, "; "))
		}
	})
	return failed
}

// store is the memory destination's data: tables, fences, markers and
// staging, shared by its writers as a database is.
type store struct {
	mu      sync.Mutex
	tables  map[string][]map[string]any
	fences  map[string]uint64
	markers map[string]bool
	staging map[string]map[string][]map[string]any
	// Rows an overwrite read has committed, until it ends, and that read.
	reading map[string][]map[string]any
	reads   map[string]int64
	// By table, the columns that take no null.
	notNull map[string]map[string]bool
	// By table, each column's type.
	types map[string]map[string]arrow.DataType
}

func newStore() *store {
	return &store{tables: map[string][]map[string]any{}, fences: map[string]uint64{}, markers: map[string]bool{},
		staging: map[string]map[string][]map[string]any{}, reading: map[string][]map[string]any{}, reads: map[string]int64{}, notNull: map[string]map[string]bool{},
		types: map[string]map[string]arrow.DataType{}}
}

type memDestination struct {
	c     memConfig
	st    *store
	bug   string
	lanes int
}

var memCapabilities = destination.Capabilities{
	Class:      destination.StageAndApply,
	Scopes:     []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
	Holds:      destination.Holds{ByKey: true, Guarded: true},
}

func (d *memDestination) Capabilities() destination.Capabilities {
	c := memCapabilities
	c.WriterLanes = d.lanes
	return c
}
func (d *memDestination) Check(context.Context) error { return d.c.check() }

func (d *memDestination) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	modes := map[string]destination.WriteMode{}
	for _, s := range cfg.Streams {
		modes[s.Name] = s.Mode
	}
	return &memWriter{d: d, cfg: cfg, modes: modes, ended: map[string]bool{}, seen: map[string]*arrow.Schema{}}, nil
}

type memWriter struct {
	d     *memDestination
	cfg   destination.OpenConfig
	modes map[string]destination.WriteMode
	// Streams whose reads ended in the epoch being written.
	ended map[string]bool
	// Each read's start.
	starts map[string]int64
	// The last schema of each stream's staging.
	seen map[string]*arrow.Schema
}

// memStaged is a lane's committable: where it staged the epoch, and the reads
// it knows the ends of.
type memStaged struct {
	Key    string
	Ended  []string
	Starts map[string]int64
}

func memLanes(committables []destination.Staged) ([]memStaged, error) {
	lanes := make([]memStaged, len(committables))
	for i, c := range committables {
		if err := json.Unmarshal(c, &lanes[i]); err != nil {
			return nil, err
		}
	}
	return lanes, nil
}

func (w *memWriter) key(e destination.Epoch) string {
	return fmt.Sprintf("%s/%d/%d/%d", w.cfg.Pipeline, e.Generation, e.N, w.cfg.Lane)
}

func (w *memWriter) marker(stream string, e destination.Epoch) string {
	return fmt.Sprintf("%s/%s/%d/%d", w.cfg.Pipeline, stream, e.Generation, e.N)
}

func (w *memWriter) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	return w.stage(e, stream, b, false)
}

func (w *memWriter) Patch(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
	return w.stage(e, stream, b, true)
}

// A staged row that's a patch.
const patchKey = "_openetl_patch"

func (w *memWriter) stage(e destination.Epoch, stream string, b arrow.RecordBatch, patch bool) error {
	if err := w.d.c.check(); err != nil {
		return err
	}
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	k := w.key(e)
	if st.staging[k] == nil {
		st.staging[k] = map[string][]map[string]any{}
	}
	if s := w.seen[stream]; w.d.bug == "older-batch-fails" && !patch {
		if s != nil && b.Schema().NumFields() < s.NumFields() {
			return errors.New("the batch has fewer columns than the last")
		}
		w.seen[stream] = b.Schema()
	}
	for i := range int(b.NumRows()) {
		row := map[string]any{}
		if patch {
			row[patchKey] = true
		}
		for c, f := range b.Schema().Fields() {
			switch f.Name {
			case destination.HeldColumn:
				row[f.Name] = heldList(b.Column(c), i)
			case "_openetl_id":
				row[f.Name] = b.Column(c).ValueStr(i)
			case "_openetl_op", "_openetl_deleted":
				row[f.Name] = b.Column(c).GetOneForMarshal(i)
			default:
				if !strings.HasPrefix(f.Name, "_openetl") {
					row[f.Name] = w.d.mapType(b.Column(c), i)
				}
			}
		}
		st.staging[k][stream] = append(st.staging[k][stream], row)
	}
	return nil
}

// mapType is the memory destination's mapping table, with its bugs.
func (d *memDestination) mapType(a arrow.Array, i int) any {
	v := a.GetOneForMarshal(i)
	switch a := a.(type) {
	case *array.Float64:
		if d.bug == "types-nan-null" && math.IsNaN(a.Value(i)) {
			return nil
		}
	case *array.Decimal128:
		if d.bug == "types-decimal-float" && a.IsValid(i) {
			f, _ := strconv.ParseFloat(a.ValueStr(i), 64)
			return f
		}
	case *array.Timestamp:
		if d.bug == "types-millis" && a.IsValid(i) {
			unit := a.DataType().(*arrow.TimestampType).Unit
			return a.Value(i).ToTime(unit).Truncate(time.Millisecond)
		}
	}
	return v
}

func heldList(a arrow.Array, i int) []string {
	l := a.(*array.List)
	if l.IsNull(i) {
		return nil
	}
	vs := l.ListValues().(*array.String)
	var out []string
	start, end := l.ValueOffsets(i)
	for j := start; j < end; j++ {
		out = append(out, vs.Value(int(j)))
	}
	return out
}

// kept is a staged row as its table keeps it: its held columns by the
// version they're held for.
func kept(r map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range r {
		if k != "_openetl_op" && k != "_openetl_id" && k != destination.HeldColumn {
			out[k] = v
		}
	}
	if held, _ := r[destination.HeldColumn].([]string); len(held) > 0 {
		versions := map[string]any{}
		for _, c := range held {
			versions[c] = r["_openetl_id"]
		}
		out[destination.HeldColumn] = versions
	}
	return out
}

func (w *memWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if w.d.bug == "empty-lane-fails" && st.staging[w.key(e)] == nil {
		return nil, errors.New("the lane staged nothing")
	}
	lane := memStaged{Key: w.key(e), Ended: slices.Sorted(maps.Keys(w.ended)), Starts: w.starts}
	clear(w.ended)
	if w.d.bug == "visible-at-precommit" {
		w.apply(e, []memStaged{lane}, false)
	}
	return json.Marshal(lane)
}

func (w *memWriter) Commit(_ context.Context, e destination.Epoch, committables []destination.Staged) (destination.Committed, error) {
	lanes, err := memLanes(committables)
	if err != nil {
		return destination.Committed{}, err
	}
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.fences[w.cfg.Pipeline] > w.cfg.Generation {
		if w.d.bug == "fenced-internal" {
			return destination.Committed{}, errors.New("a newer generation owns the pipeline")
		}
		return destination.Committed{}, destination.ErrFenced
	}
	switch w.d.bug {
	case "commit-lane-0":
		lanes = lanes[:1]
	case "ended-lane-0":
		for i := range lanes[1:] {
			lanes[i+1].Ended = nil
		}
	}
	w.apply(e, lanes, w.d.bug != "double-apply")
	return destination.Committed{}, nil
}

// apply applies every lane's staging of e to each stream without its marker.
func (w *memWriter) apply(e destination.Epoch, lanes []memStaged, marked bool) {
	st := w.d.st
	staged := map[string][]map[string]any{}
	var ended []string
	starts := map[string]int64{}
	for _, l := range lanes {
		for stream, rows := range st.staging[l.Key] {
			staged[stream] = append(staged[stream], rows...)
		}
		ended = append(ended, l.Ended...)
		maps.Copy(starts, l.Starts)
	}
	for _, stream := range ended {
		if _, ok := staged[stream]; !ok {
			staged[stream] = nil
		}
	}
	for stream, rows := range staged {
		marker := w.marker(stream, e)
		if marked && st.markers[marker] {
			continue
		}
		table := w.cfg.Namespace + "." + stream
		var data, patches []map[string]any
		for _, r := range rows {
			if r[patchKey] == true {
				patches = append(patches, r)
			} else {
				data = append(data, kept(r))
			}
		}
		switch w.modes[stream] {
		case destination.Overwrite:
			if st.reads[table] != starts[stream] {
				st.reads[table] = starts[stream]
				delete(st.reading, table)
			}
			st.reading[table] = append(st.reading[table], data...)
			if slices.Contains(ended, stream) {
				st.tables[table] = st.reading[table]
				delete(st.reading, table)
			}
		case destination.Merge:
			for _, r := range data {
				i := slices.IndexFunc(st.tables[table], func(t map[string]any) bool { return t["id"] == r["id"] })
				switch {
				case i < 0:
					st.tables[table] = append(st.tables[table], r)
				case w.d.bug == "merge-keeps-first":
				case r["_openetl_deleted"] == true:
					// A soft delete keeps the row's other columns.
					t := maps.Clone(st.tables[table][i])
					t["_openetl_deleted"] = true
					st.tables[table][i] = t
				default:
					st.tables[table][i] = r
				}
			}
			w.patch(table, patches)
		default:
			st.tables[table] = append(st.tables[table], data...)
		}
		st.markers[marker] = true
	}
}

// patch fills each patch's held columns in the row of its key, where the
// row still holds them for the patch's version.
func (w *memWriter) patch(table string, patches []map[string]any) {
	st := w.d.st
	for _, p := range patches {
		i := slices.IndexFunc(st.tables[table], func(t map[string]any) bool { return t["id"] == p["id"] })
		if i < 0 {
			continue
		}
		t := maps.Clone(st.tables[table][i])
		held, _ := t[destination.HeldColumn].(map[string]any)
		held = maps.Clone(held)
		for _, c := range p[destination.HeldColumn].([]string) {
			if held[c] == p["_openetl_id"] || w.d.bug == "unguarded-patch" {
				t[c] = p[c]
				delete(held, c)
			}
		}
		t[destination.HeldColumn] = held
		if w.d.bug == "patch-undeletes" {
			delete(t, "_openetl_deleted")
		}
		st.tables[table][i] = t
	}
}

func (w *memWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, committables []destination.Staged) (destination.Recovery, error) {
	if err := w.d.c.check(); err != nil {
		return 0, err
	}
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	st.fences[w.cfg.Pipeline] = max(st.fences[w.cfg.Pipeline], gen)
	if e.IsZero() || w.d.bug == "recover-applied" {
		return destination.Applied, nil
	}
	lanes, err := memLanes(committables)
	if err != nil {
		return 0, err
	}
	if w.d.bug == "recover-lane-0" {
		lanes = lanes[:1]
	}
	missing := false
	for _, l := range lanes {
		for _, stream := range slices.Concat(slices.Collect(maps.Keys(st.staging[l.Key])), l.Ended) {
			missing = missing || !st.markers[w.marker(stream, e)]
		}
	}
	if !missing {
		return destination.Applied, nil
	}
	w.apply(e, lanes, true)
	return destination.Redone, nil
}

func (w *memWriter) StartReads(_ context.Context, starts map[string]time.Time) error {
	w.starts = map[string]int64{}
	for name, t := range starts {
		w.starts[name] = t.UnixMicro()
	}
	return nil
}

func (w *memWriter) EndRead(_ context.Context, _ destination.Epoch, stream string) error {
	w.ended[stream] = true
	return nil
}

func (w *memWriter) ApplySchema(_ context.Context, streams []destination.StreamSchema) (destination.SchemaApplied, error) {
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	out := destination.SchemaApplied{Limits: map[string]destination.TableLimits{}}
	for _, s := range streams {
		t := w.cfg.Namespace + "." + s.Name
		var l destination.TableLimits
		if w.d.bug != "limits-without-not-null" {
			l.NotNull = slices.Sorted(maps.Keys(st.notNull[t]))
		}
		out.Limits[s.Name] = l
		if st.types[t] == nil {
			st.types[t] = map[string]arrow.DataType{}
		}
		allow := slices.ContainsFunc(w.cfg.Streams, func(c destination.Stream) bool { return c.Name == s.Name && c.AllowTypeChanges })
		for _, f := range s.Schema.Fields() {
			have, ok := st.types[t][f.Name]
			switch {
			case !ok:
				st.types[t][f.Name] = f.Type
			case arrow.TypeEqual(have, f.Type) || narrower(f.Type, have):
			case w.d.bug == "type-changes-unasked" || allow && w.d.bug != "type-changes-refused":
				st.types[t][f.Name] = f.Type
				for _, row := range st.tables[t] {
					if v, ok := row[f.Name]; ok && v != nil && f.Type.ID() == arrow.STRING {
						row[f.Name] = fmt.Sprint(v)
					}
				}
			default:
				rc := destination.RefusedColumn{
					Stream: s.Name, Column: f.Name, Reason: "it doesn't change a column's type", FromType: have.String(), ToType: f.Type.String(),
				}
				if w.d.bug == "refusal-unexplained" {
					rc = destination.RefusedColumn{Stream: s.Name, Column: f.Name}
				}
				out.Refused = append(out.Refused, rc)
			}
		}
	}
	return out, nil
}

// narrower reports whether a is an integer narrower than b, which a table
// holding b takes as it is.
func narrower(a, b arrow.DataType) bool {
	wa, ok := a.(arrow.FixedWidthDataType)
	wb, okb := b.(arrow.FixedWidthDataType)
	return ok && okb && arrow.IsInteger(a.ID()) && arrow.IsInteger(b.ID()) && wa.BitWidth() < wb.BitWidth()
}
func (w *memWriter) Close(context.Context) error { return nil }

func destinationSuite(bug string) conformance.DestinationSuite { return lanesSuite(bug, 0) }

func lanesSuite(bug string, lanes int) conformance.DestinationSuite {
	st := newStore()
	caps := memCapabilities
	caps.WriterLanes = lanes
	return conformance.DestinationSuite{
		Launch: conformance.InProcess(sdk.Connector{
			Name: "memory", ConfigSchema: configSchema, Egress: sdk.Egress{Tier: sdk.EgressRaw},
			Destination: func(config []byte, _ host.Host) (destination.Destination, error) {
				c, err := bind(config)
				return &memDestination{c: c, st: st, bug: bug, lanes: lanes}, err
			},
			Capabilities: caps,
		}),
		Config: good, BadCredentials: badCredentials, Unreachable: unreachable,
		Read: func(_ context.Context, namespace, stream string) ([]map[string]any, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			return slices.Clone(st.tables[namespace+"."+stream]), nil
		},
		NotNull: func(_ context.Context, namespace, stream, column string) error {
			st.mu.Lock()
			defer st.mu.Unlock()
			t := namespace + "." + stream
			if st.notNull[t] == nil {
				st.notNull[t] = map[string]bool{}
			}
			st.notNull[t][column] = true
			return nil
		},
	}
}

func TestDestinationPasses(t *testing.T) {
	destinationSuite("").Test(t)
}

func TestDestinationOnLanesPasses(t *testing.T) {
	for _, lanes := range []int{2, 3, 16} {
		t.Run(fmt.Sprint(lanes), func(t *testing.T) { lanesSuite("", lanes).Test(t) })
	}
}

func TestDestinationLaneBugsFail(t *testing.T) {
	cases := map[string][]string{
		"commit-lane-0": {
			"lanes/commit", "lanes/empty", "lanes/overwrite", "lanes/merge", "lanes/crash-after-precommit", "lanes/crash-during-commit", "lanes/crash-after-commit",
			"lanes/fence", "lanes/overwrite-crash", "lanes/merge-crash", "lanes/schemas", "lanes/holds",
		},
		"ended-lane-0":      {"lanes/overwrite", "lanes/overwrite-crash", "lanes/schemas"},
		"recover-lane-0":    {"lanes/empty", "lanes/crash-after-record", "lanes/fence", "lanes/overwrite-crash", "lanes/merge-crash"},
		"older-batch-fails": {"lanes/schemas"},
		"empty-lane-fails":  {"lanes/empty", "lanes/overwrite", "lanes/overwrite-crash", "lanes/merge-crash", "lanes/schemas", "lanes/holds"},
		"merge-keeps-first": {"write/merge", "holds/patch", "holds/soft-delete", "lanes/merge", "lanes/merge-crash"},
	}
	// Checks that catch a bug only when a crash lands at the right moment.
	sometimes := map[string][]string{
		"recover-lane-0": {"lanes/crash-during-commit"},
	}
	for bug, checks := range cases {
		t.Run(bug, func(t *testing.T) {
			failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) { lanesSuite(bug, 3).Run(ctx, report) })
			failed = slices.DeleteFunc(failed, func(c string) bool { return slices.Contains(sometimes[bug], c) })
			if !slices.Equal(failed, checks) {
				t.Errorf("failed checks %v, want %v", failed, checks)
			}
		})
	}
}

func TestDestinationBugsFail(t *testing.T) {
	cases := map[string][]string{
		"visible-at-precommit":    {"write/commit", "write/crash-after-precommit", "write/crash-after-record"},
		"double-apply":            {"write/commit"},
		"recover-applied":         {"write/crash-after-record", "write/fence"},
		"fenced-internal":         {"write/fence"},
		"unguarded-patch":         {"holds/patch", "holds/soft-delete"},
		"patch-undeletes":         {"holds/soft-delete"},
		"limits-without-not-null": {"holds/not-null"},
		"type-changes-unasked":    {"write/type-changes"},
		"type-changes-refused":    {"write/type-changes"},
		"refusal-unexplained":     {"write/type-changes"},
		"types-nan-null":          {"types"},
		"types-decimal-float":     {"types"},
		"types-millis":            {"types"},
	}
	for bug, checks := range cases {
		t.Run(bug, func(t *testing.T) {
			failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) { destinationSuite(bug).Run(ctx, report) })
			if !slices.Equal(failed, checks) {
				t.Errorf("failed checks %v, want %v", failed, checks)
			}
		})
	}
}

// A type the mapping table says it doesn't keep exactly isn't checked, and
// is flagged.
func TestDestinationLossyTypes(t *testing.T) {
	s := destinationSuite("types-nan-null")
	s.LossyTypes = map[string]string{"float64": "NaN is stored as null"}
	var flagged bool
	failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) {
		s.Run(ctx, func(r conformance.Result) {
			flagged = flagged || r.Check == "types" && slices.ContainsFunc(r.Log, func(l string) bool { return strings.Contains(l, "NaN is stored as null") })
			report(r)
		})
	})
	if len(failed) > 0 || !flagged {
		t.Errorf("failed %v, flagged %v; want none failed, and float64 flagged", failed, flagged)
	}
}
