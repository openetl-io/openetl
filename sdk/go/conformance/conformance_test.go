package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/conformance"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
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
	}}, nil
}

func (s *memSource) Read(_ context.Context, req source.ReadRequest, out source.Emitter) error {
	if err := s.c.check(); err != nil {
		if s.bug == "check-internal" && s.c.Token != "good" {
			return errors.New("bad token")
		}
		return err
	}
	s.up.mu.Lock()
	users, events := s.up.users, s.up.events
	s.up.mu.Unlock()
	for _, st := range req.Streams {
		var err error
		switch st.Name {
		case "users":
			err = s.readUsers(users, st.State, out)
		case "events":
			err = s.readEvents(events, st.State, out)
		default:
			err = fmt.Errorf("no stream %q", st.Name)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *memSource) readUsers(n int64, state []byte, out source.Emitter) error {
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
		if err := out.Records("users", usersBatch(ids, names)); err != nil {
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

func usersBatch(ids []int64, names []string) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, usersSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.StringBuilder).AppendValues(names, nil)
	return b.NewRecordBatch()
}

func eventsBatch(ids, ts []int64) arrow.RecordBatch {
	b := array.NewRecordBuilder(memory.DefaultAllocator, eventsSchema)
	defer b.Release()
	b.Field(0).(*array.Int64Builder).AppendValues(ids, nil)
	b.Field(1).(*array.Int64Builder).AppendValues(ts, nil)
	return b.NewRecordBatch()
}

func sourceSuite(bug string) conformance.SourceSuite {
	up := &upstream{users: 23, events: 18}
	return conformance.SourceSuite{
		Launch: conformance.InProcess(sdk.Connector{
			Name: "memory", ConfigSchema: configSchema, Egress: sdk.Egress{Tier: sdk.EgressRaw},
			Source: func(config []byte, _ host.Host) (source.Source, error) {
				c, err := bind(config)
				return &memSource{c: c, up: up, bug: bug}, err
			},
		}),
		Config: good, BadCredentials: badCredentials, Unreachable: unreachable,
		Streams: []conformance.Stream{{Name: "users", Mode: source.FullRefresh}, {Name: "events", Mode: source.Incremental}},
		AddRecords: func(context.Context, string) (int, error) {
			up.mu.Lock()
			defer up.mu.Unlock()
			up.events += 3
			return 3, nil
		},
	}
}

func TestSourcePasses(t *testing.T) {
	sourceSuite("").Test(t)
}

func TestSourceBugsFail(t *testing.T) {
	cases := map[string]string{
		"resume-gap":           "read/resume",
		"dup-key":              "read/full",
		"batch-after-complete": "read/full",
		"incremental-no-next":  "read/incremental",
		"incremental-skips":    "read/incremental",
		"check-internal":       "errors",
	}
	for bug, check := range cases {
		t.Run(bug, func(t *testing.T) {
			failed := runFailed(t, func(ctx context.Context, report func(conformance.Result)) { sourceSuite(bug).Run(ctx, report) })
			if !slices.Equal(failed, []string{check}) {
				t.Errorf("failed checks %v, want [%s]", failed, check)
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
	// Rows an overwrite read has committed, until it ends.
	reading map[string][]map[string]any
}

func newStore() *store {
	return &store{tables: map[string][]map[string]any{}, fences: map[string]uint64{}, markers: map[string]bool{},
		staging: map[string]map[string][]map[string]any{}, reading: map[string][]map[string]any{}}
}

type memDestination struct {
	c   memConfig
	st  *store
	bug string
}

var memCapabilities = destination.Capabilities{
	Class:      destination.StageAndApply,
	Scopes:     []destination.Scope{destination.ScopePipeline, destination.ScopeStream},
	WriteModes: []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge},
}

func (d *memDestination) Capabilities() destination.Capabilities { return memCapabilities }
func (d *memDestination) Check(context.Context) error            { return d.c.check() }

func (d *memDestination) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	modes := map[string]destination.WriteMode{}
	for _, s := range cfg.Streams {
		modes[s.Name] = s.Mode
	}
	return &memWriter{d: d, cfg: cfg, modes: modes, ended: map[string]bool{}, endedIn: map[string][]string{}}, nil
}

type memWriter struct {
	d     *memDestination
	cfg   destination.OpenConfig
	modes map[string]destination.WriteMode
	// Streams whose reads ended in the epoch being written, then by epoch.
	ended   map[string]bool
	endedIn map[string][]string
}

func (w *memWriter) key(e destination.Epoch) string {
	return fmt.Sprintf("%s/%d/%d", w.cfg.Pipeline, e.Generation, e.N)
}

func (w *memWriter) Write(_ context.Context, e destination.Epoch, stream string, b arrow.RecordBatch) error {
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
	for i := range int(b.NumRows()) {
		row := map[string]any{}
		for c, f := range b.Schema().Fields() {
			if !strings.HasPrefix(f.Name, "_openetl") {
				row[f.Name] = b.Column(c).GetOneForMarshal(i)
			}
		}
		st.staging[k][stream] = append(st.staging[k][stream], row)
	}
	return nil
}

func (w *memWriter) PreCommit(_ context.Context, e destination.Epoch) (destination.Staged, error) {
	w.d.st.mu.Lock()
	w.endedIn[w.key(e)] = slices.Sorted(maps.Keys(w.ended))
	clear(w.ended)
	w.d.st.mu.Unlock()
	if w.d.bug == "visible-at-precommit" {
		w.d.st.mu.Lock()
		w.apply(e, false)
		w.d.st.mu.Unlock()
	}
	return destination.Staged(w.key(e)), nil
}

func (w *memWriter) Commit(_ context.Context, e destination.Epoch, _ destination.Staged) (destination.Committed, error) {
	st := w.d.st
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.fences[w.cfg.Pipeline] > w.cfg.Generation {
		if w.d.bug == "fenced-internal" {
			return destination.Committed{}, errors.New("a newer generation owns the pipeline")
		}
		return destination.Committed{}, destination.ErrFenced
	}
	w.apply(e, w.d.bug != "double-apply")
	return destination.Committed{}, nil
}

// apply applies e's staging to each stream without its marker.
func (w *memWriter) apply(e destination.Epoch, marked bool) {
	st := w.d.st
	staged := maps.Clone(st.staging[w.key(e)])
	if staged == nil {
		staged = map[string][]map[string]any{}
	}
	ended := w.endedIn[w.key(e)]
	for _, stream := range ended {
		if _, ok := staged[stream]; !ok {
			staged[stream] = nil
		}
	}
	for stream, rows := range staged {
		marker := fmt.Sprintf("%s/%s/%d/%d", w.cfg.Pipeline, stream, e.Generation, e.N)
		if marked && st.markers[marker] {
			continue
		}
		table := w.cfg.Namespace + "." + stream
		switch w.modes[stream] {
		case destination.Overwrite:
			st.reading[table] = append(st.reading[table], rows...)
			if slices.Contains(ended, stream) {
				st.tables[table] = st.reading[table]
				delete(st.reading, table)
			}
		case destination.Merge:
			for _, r := range rows {
				i := slices.IndexFunc(st.tables[table], func(t map[string]any) bool { return t["id"] == r["id"] })
				if i >= 0 {
					st.tables[table][i] = r
				} else {
					st.tables[table] = append(st.tables[table], r)
				}
			}
		default:
			st.tables[table] = append(st.tables[table], rows...)
		}
		st.markers[marker] = true
	}
}

func (w *memWriter) Recover(_ context.Context, gen uint64, e destination.Epoch, _ destination.Staged) (destination.Recovery, error) {
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
	staged, ok := st.staging[w.key(e)]
	if !ok {
		return destination.Lost, nil
	}
	missing := false
	for stream := range staged {
		if !st.markers[fmt.Sprintf("%s/%s/%d/%d", w.cfg.Pipeline, stream, e.Generation, e.N)] {
			missing = true
		}
	}
	if !missing {
		return destination.Applied, nil
	}
	w.apply(e, true)
	return destination.Redone, nil
}

func (w *memWriter) StartReads(context.Context, map[string]time.Time) error { return nil }

func (w *memWriter) EndRead(_ context.Context, _ destination.Epoch, stream string) error {
	w.ended[stream] = true
	return nil
}

func (w *memWriter) ApplySchema(context.Context, []destination.StreamSchema) error { return nil }
func (w *memWriter) Close(context.Context) error                                   { return nil }

func destinationSuite(bug string) conformance.DestinationSuite {
	st := newStore()
	return conformance.DestinationSuite{
		Launch: conformance.InProcess(sdk.Connector{
			Name: "memory", ConfigSchema: configSchema, Egress: sdk.Egress{Tier: sdk.EgressRaw},
			Destination: func(config []byte, _ host.Host) (destination.Destination, error) {
				c, err := bind(config)
				return &memDestination{c: c, st: st, bug: bug}, err
			},
			Capabilities: memCapabilities,
		}),
		Config: good, BadCredentials: badCredentials, Unreachable: unreachable,
		Read: func(_ context.Context, namespace, stream string) ([]map[string]any, error) {
			st.mu.Lock()
			defer st.mu.Unlock()
			return slices.Clone(st.tables[namespace+"."+stream]), nil
		},
	}
}

func TestDestinationPasses(t *testing.T) {
	destinationSuite("").Test(t)
}

func TestDestinationBugsFail(t *testing.T) {
	cases := map[string][]string{
		"visible-at-precommit": {"write/commit", "write/crash-after-precommit", "write/crash-after-record"},
		"double-apply":         {"write/commit"},
		"recover-applied":      {"write/crash-after-record", "write/fence"},
		"fenced-internal":      {"write/fence"},
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
