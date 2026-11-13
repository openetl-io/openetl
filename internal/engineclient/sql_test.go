package engineclient

import (
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
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

func TestPlanSQLGivesEachStatementsOutput(t *testing.T) {
	df := &Dataflow{Options: options(t, nil)}
	got, err := df.PlanSQL(testContext(t), contacts, []runner.Stream{users()})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != "contacts" || got[0].Input != "users" || !slices.Equal(got[0].PrimaryKey, []string{"uid"}) {
		t.Fatalf("outputs: %+v", got)
	}
	if f := got[0].Schema.Field(1); f.Name != "address" || !arrow.TypeEqual(f.Type, arrow.BinaryTypes.String) {
		t.Errorf("address: %v", f)
	}

	_, err = df.PlanSQL(testContext(t), "INSERT INTO n\nSELECT count(*) AS n FROM users", []runner.Stream{users()})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "transform.sql: 2:") {
		t.Errorf("an aggregate: %v", err)
	}
	inferred := users()
	inferred.Schema = nil
	_, err = df.PlanSQL(testContext(t), contacts, []runner.Stream{inferred})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "stream users has no known columns") {
		t.Errorf("a stream without declared columns: %v", err)
	}
}

// The engine runs a statement between the normalizer's halves: its output
// is named and given metadata columns, and the stream passes through too.
func TestEngineRunsSQL(t *testing.T) {
	ctx := testContext(t)
	df := &Dataflow{Options: options(t, nil)}
	in := users()
	expected, err := df.PlanSQL(ctx, contacts, []runner.Stream{in})
	if err != nil {
		t.Fatal(err)
	}
	p := plan()
	p.Streams = []runner.Stream{in}
	p.Outputs = []runner.Stream{
		{Name: "contacts", PrimaryKey: []string{"uid"}, Transformed: true},
		{Name: "users", PrimaryKey: []string{"id"}, Transformed: true},
	}
	p.SQL = &runner.SQLTransform{SQL: contacts, Expected: expected}
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
