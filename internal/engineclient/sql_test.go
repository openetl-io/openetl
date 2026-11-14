package engineclient

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
)

// users is a stream whose source declares its columns.
func users() runner.Stream {
	return runner.Stream{
		Name:       "users",
		PrimaryKey: []string{"id"},
		Schema: arrow.NewSchema([]arrow.Field{
			{Name: "id", Type: arrow.PrimitiveTypes.Int64},
			{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil),
	}
}

const contacts = "INSERT INTO contacts\nSELECT id AS uid, lower(email) AS address FROM users WHERE id > 1"

// A runner plans with the schemas a job starts with: the declared one, and
// the one a sync recorded, whose columns the source may no longer declare.
func TestPlanReadsTheSchemasAJobStartsWith(t *testing.T) {
	opts := planEngine(t)
	declared, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := arrowipc.EncodeSchema(users().Schema)
	if err != nil {
		t.Fatal(err)
	}
	in := sqlplan.Stream{Name: "users", Key: []string{"id"}, DeclaredSchema: declared}
	a, err := Plan(t.Context(), opts, sqlplan.Request{SQL: contacts, Streams: []sqlplan.Stream{in}})
	if err != nil || len(a.Errors) != 1 || !strings.Contains(a.Errors[0].Message, "email") {
		t.Fatalf("without the restored schema: %+v, %v", a, err)
	}
	in.RestoredSchema = restored
	a, err = Plan(t.Context(), opts, sqlplan.Request{SQL: contacts, Streams: []sqlplan.Stream{in}})
	if err != nil || len(a.Errors) != 0 || len(a.Statements) != 1 {
		t.Fatalf("%+v, %v", a, err)
	}
	if st := a.Statements[0]; st.Output != "contacts" || !slices.Equal(st.Key, []string{"uid"}) || !slices.Equal(st.Inputs, []string{"users"}) ||
		len(st.Columns) != 2 || st.Columns[1] != (sqlplan.Field{Name: "address", Type: "utf8", Nullable: true}) {
		t.Errorf("%+v", st)
	}
}

// expected is what planning gave, as a job's start takes it.
func expected(t *testing.T, a sqlplan.Answer) []runner.ExpectedOutput {
	t.Helper()
	var out []runner.ExpectedOutput
	for _, st := range a.Statements {
		schema, err := arrowipc.DecodeSchema(st.ArrowSchema)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, runner.ExpectedOutput{Name: st.Output, Schema: schema, PrimaryKey: st.Key, Input: st.Inputs[0]})
	}
	return out
}

// The engine runs a statement between the normalizer's halves: its output
// is named and given metadata columns, and the stream passes through too.
func TestEngineRunsSQL(t *testing.T) {
	ctx := testContext(t)
	df := &Dataflow{Options: options(t, nil)}
	in := users()
	declared, err := arrowipc.EncodeSchema(in.Schema)
	if err != nil {
		t.Fatal(err)
	}
	a, err := Plan(ctx, planEngine(t), sqlplan.Request{SQL: contacts, Streams: []sqlplan.Stream{{Name: "users", Key: in.PrimaryKey, DeclaredSchema: declared}}})
	if err != nil || len(a.Errors) != 0 {
		t.Fatal(a.Errors, err)
	}
	p := plan()
	p.Streams = []runner.Stream{in}
	p.Outputs = []runner.Stream{
		{Name: "contacts", PrimaryKey: []string{"uid"}, Transformed: true},
		{Name: "users", PrimaryKey: []string{"id"}, Transformed: true},
	}
	p.SQL = &runner.SQLTransform{SQL: contacts, Expected: expected(t, a)}
	got, err := run(ctx, df, p,
		runner.Frame{Kind: runner.FrameRows, Stream: "users", Rows: rows(`{"id": 1, "email": "A@x"}`, `{"id": 2, "email": "B@x"}`)},
		runner.Frame{Kind: runner.FrameBarrier, Stream: "users", Epoch: 5},
	)
	defer release(got)
	if err != nil {
		t.Fatal(err)
	}
	c := got["contacts"]
	if len(c) != 2 || c[0].Kind != runner.FrameBatch || c[1].Kind != runner.FrameBarrier {
		t.Fatalf("contacts: %+v", c)
	}
	b := c[0].Batch
	if b.NumRows() != 1 {
		t.Fatalf("contacts has %d rows, want 1", b.NumRows())
	}
	idx := b.Schema().FieldIndices("address")
	if len(idx) != 1 || b.Column(idx[0]).(*array.String).Value(0) != "b@x" {
		t.Errorf("address: %v", b)
	}
	if len(b.Schema().FieldIndices("_openetl_id")) != 1 {
		t.Errorf("no _openetl_id: %v", b.Schema())
	}
	if u := got["users"]; len(u) != 2 || u[0].Batch.NumRows() != 2 {
		t.Errorf("users: %+v", u)
	}
	ready := got[""]
	if len(ready) != 1 {
		t.Fatalf("ready: %+v", ready)
	}
	r := ready[0].Ready
	if len(r.Schemas) != 1 || r.Schemas["users"] == nil || len(r.OutputSchemas) != 2 || r.OutputSchemas["contacts"] == nil {
		t.Errorf("schemas %v, outputs' %v", r.Schemas, r.OutputSchemas)
	}
}

// What planning refuses, StartJob refuses too.
func TestEngineRefusesSQLItCantPlan(t *testing.T) {
	p := plan()
	p.Streams = []runner.Stream{users()}
	p.Outputs = []runner.Stream{{Name: "b", Transformed: true}}
	p.SQL = &runner.SQLTransform{SQL: "INSERT INTO b SELECT count(*) AS n FROM users"}
	_, err := run(testContext(t), &Dataflow{Options: options(t, nil)}, p)
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "transform.sql: 1:") {
		t.Fatalf("err = %v", err)
	}
}
