package engineclient

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func schemaBytes(t *testing.T, name string) []byte {
	t.Helper()
	b, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: name, Type: arrow.PrimitiveTypes.Int64}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fields are the numbers of a message's fields on the wire.
func fields(t *testing.T, m proto.Message) []protowire.Number {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var out []protowire.Number
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatal(protowire.ParseError(n))
		}
		b = b[n:]
		m := protowire.ConsumeFieldValue(num, typ, b)
		if m < 0 {
			t.Fatal(protowire.ParseError(m))
		}
		b = b[m:]
		out = append(out, num)
	}
	return out
}

// A plan without SQL makes the request it made before outputs: no field an
// engine that predates them doesn't know, and the streams on every edge.
func TestStartRequestWithoutSQL(t *testing.T) {
	p := plan("a", "b")
	p.Streams[0].Mode, p.Streams[0].PrimaryKey, p.Streams[0].Table = destination.Merge, []string{"id"}, "A"
	p.Schemas = map[string][]byte{"a": schemaBytes(t, "x")}
	p.Holds = map[string]*runner.HoldPlan{"a": {StreamHolds: runner.StreamHolds{Cells: true}}}
	p.KeyGrouped = map[string]bool{"a": true}
	req, names, sinkNames, err := startRequest(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range fields(t, req) {
		if n > 18 {
			t.Errorf("the request has field %d", n)
		}
	}
	if !slices.Equal(names, []string{"a", "b"}) || !slices.Equal(sinkNames, names) {
		t.Errorf("edges carry %v in and %v out", names, sinkNames)
	}
	for _, e := range req.GetEdges() {
		if !slices.Equal(e.GetStreams(), names) {
			t.Errorf("edge %s carries %v", e.GetId(), e.GetStreams())
		}
	}
	a := req.GetStreams()[0]
	if !a.GetSoftDeletes() || !a.GetHolds().GetCells() || a.GetTable() != "A" || a.GetRouting() != enginev1.Routing_ROUTING_KEY_GROUP || len(a.GetRestoredSchema()) == 0 {
		t.Errorf("stream a lost the destination's settings: %v", a)
	}
}

// With SQL, the streams are its inputs, without the destination's settings
// even where an output has the same name, and the outputs are what the
// edges from the engine carry.
func TestStartRequestCarriesSQLAndOutputs(t *testing.T) {
	p := plan("a", "b")
	p.Streams[0].CursorField, p.Streams[0].Schema = "at", arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	p.Streams[1].Read = source.CDC
	p.ReadStarts = map[string]int64{"a": 7}
	p.Schemas = map[string][]byte{"a": schemaBytes(t, "input")}
	p.Outputs = []runner.Stream{
		{Name: "a", Mode: destination.Merge, PrimaryKey: []string{"id"}, Table: "A", Transformed: true},
		{Name: "c", Mode: destination.Append, Read: source.CDC, Transformed: true},
	}
	p.OutputSchemas = map[string][]byte{"a": schemaBytes(t, "output")}
	p.Holds = map[string]*runner.HoldPlan{"a": {StreamHolds: runner.StreamHolds{Cells: true}}}
	p.KeyGrouped = map[string]bool{"a": true}
	expected := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	p.SQL = &runner.SQLTransform{
		SQL:       "INSERT INTO a SELECT id FROM a; INSERT INTO c SELECT * FROM b",
		Expected:  []runner.ExpectedOutput{{Name: "a", Schema: expected, PrimaryKey: []string{"id"}}, {Name: "c"}},
		Operators: []string{"op-1"},
	}
	req, names, sinkNames, err := startRequest(p)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, []string{"a", "b"}) || !slices.Equal(sinkNames, []string{"a", "c"}) {
		t.Fatalf("edges carry %v in and %v out", names, sinkNames)
	}
	for _, e := range req.GetEdges() {
		want := names
		if e.GetDirection() == enginev1.EdgeDirection_EDGE_DIRECTION_FROM_ENGINE {
			want = sinkNames
		}
		if !slices.Equal(e.GetStreams(), want) {
			t.Errorf("edge %s carries %v, want %v", e.GetId(), e.GetStreams(), want)
		}
	}

	in := req.GetStreams()[0]
	if in.GetSoftDeletes() || in.GetHolds() != nil || in.GetTable() != "" || in.GetRouting() != enginev1.Routing_ROUTING_UNSPECIFIED {
		t.Errorf("input a has the destination's settings: %v", in)
	}
	if in.GetCursorField() != "at" || in.GetReadStartMicros() != 7 || len(in.GetArrowSchema()) == 0 || !slices.Equal(in.GetRestoredSchema(), p.Schemas["a"]) {
		t.Errorf("input a lost what the engine reads it with: %v", in)
	}
	if !req.GetStreams()[1].GetChangelog() {
		t.Error("input b isn't a changelog")
	}

	outs := req.GetOutputs()
	if len(outs) != 2 {
		t.Fatalf("outputs: %v", outs)
	}
	a, c := outs[0], outs[1]
	if !a.GetSoftDeletes() || !a.GetHolds().GetCells() || a.GetTable() != "A" || a.GetRouting() != enginev1.Routing_ROUTING_KEY_GROUP ||
		!slices.Equal(a.GetPrimaryKey(), []string{"id"}) || !slices.Equal(a.GetRestoredSchema(), p.OutputSchemas["a"]) {
		t.Errorf("output a: %v", a)
	}
	if len(a.GetArrowSchema()) > 0 || a.GetCursorField() != "" || a.GetReadStartMicros() != 0 {
		t.Errorf("output a has what only a stream has: %v", a)
	}
	if c.GetName() != "c" || !c.GetChangelog() || c.GetHolds() != nil || len(c.GetRestoredSchema()) > 0 {
		t.Errorf("output c: %v", c)
	}

	tr := req.GetTransform()
	if tr.GetSql() != p.SQL.SQL || !slices.Equal(tr.GetOperators(), []string{"op-1"}) || len(tr.GetExpectedOutputs()) != 2 {
		t.Fatalf("transform: %v", tr)
	}
	e := tr.GetExpectedOutputs()[0]
	got, err := arrowipc.DecodeSchema(e.GetArrowSchema())
	if err != nil {
		t.Fatal(err)
	}
	if e.GetName() != "a" || !got.Equal(expected) || !slices.Equal(e.GetPrimaryKey(), []string{"id"}) {
		t.Errorf("expected output a: %v", e)
	}
	if e := tr.GetExpectedOutputs()[1]; e.GetName() != "c" || len(e.GetArrowSchema()) > 0 || len(e.GetPrimaryKey()) > 0 {
		t.Errorf("expected output c: %v", e)
	}
}

// The engine's report keeps a transform's outputs apart from its inputs.
// One from an engine that predates outputs has none.
func TestReadyKeepsOutputsApart(t *testing.T) {
	in, out := schemaBytes(t, "input"), schemaBytes(t, "output")
	r, err := readyOf(&enginev1.EpochReady{
		Schemas:       map[string][]byte{"a": in},
		OutputSchemas: map[string][]byte{"a": out},
		Quarantine: []*enginev1.QuarantineFile{
			{Stream: "a", Url: "file:///q/1/1/a.jsonl", Rows: 1},
			{Stream: "a", Url: "file:///q/_outputs/1/1/a.jsonl", Rows: 2, Output: true},
		},
	}, plan("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(r.Schemas["a"], in) || !slices.Equal(r.OutputSchemas["a"], out) || len(r.Schemas) != 1 || len(r.OutputSchemas) != 1 {
		t.Errorf("schemas %v, outputs' %v", r.Schemas, r.OutputSchemas)
	}
	if len(r.Quarantine) != 2 || r.Quarantine[0].Output || !r.Quarantine[1].Output {
		t.Errorf("quarantine: %+v", r.Quarantine)
	}

	r, err = readyOf(&enginev1.EpochReady{Schemas: map[string][]byte{"a": in}}, plan("a"))
	if err != nil {
		t.Fatal(err)
	}
	if r.OutputSchemas != nil || !slices.Equal(r.Schemas["a"], in) {
		t.Errorf("without outputs: schemas %v, outputs' %v", r.Schemas, r.OutputSchemas)
	}
}

// The engine checks a transform, and doesn't run one yet.
func TestEngineRefusesSQL(t *testing.T) {
	p := plan("a")
	p.Outputs = []runner.Stream{{Name: "b", Transformed: true}}
	p.SQL = &runner.SQLTransform{SQL: "INSERT INTO b SELECT * FROM a", Expected: []runner.ExpectedOutput{{Name: "b"}}}
	_, err := run(testContext(t), &Dataflow{Options: options(t, nil)}, p)
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "doesn't run") {
		t.Fatalf("err = %v", err)
	}
}
