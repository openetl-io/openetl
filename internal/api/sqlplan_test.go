package api

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// fakePlanner answers as a runner would, and keeps what it was sent.
type fakePlanner struct {
	answer sqlplan.Answer
	err    error
	sent   []contract.PlanSQLInput
}

func (f *fakePlanner) PlanSQL(_ context.Context, in contract.PlanSQLInput) (sqlplan.Answer, error) {
	f.sent = append(f.sent, in)
	return f.answer, f.err
}

func sqlPipeline(sql string) *planned {
	spec := &apiv1.PipelineSpec{
		Source:    &apiv1.PipelineSource{Ref: "shop"},
		Transform: &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql}}},
	}
	return &planned{name: "workspaces/default/pipelines/shop", action: apiv1.ApplyAction_APPLY_ACTION_CREATED, v: &pinned{spec: spec, dataPlane: "dp1"}}
}

var shopCatalog = &apiv1.Catalog{Streams: []*apiv1.CatalogStream{
	{Name: "customers", PrimaryKey: []string{"id"}, Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}, {Name: "email", Type: "utf8"}}},
	{Name: "events"},
	{Name: "orders", Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}}},
}}

// The plan sends the streams the SQL names to the pipeline's data plane, and
// shows what the engine answered: the statements, its notes, and its errors
// as problems that block the apply.
func TestPlanSQL(t *testing.T) {
	answer := sqlplan.Answer{
		Statements: []sqlplan.Statement{{At: sqlplan.Position{Line: 1, Column: 1}, Output: "contacts", Class: "row_wise", Inputs: []string{"customers"}, Key: []string{"id"},
			Columns: []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}}, ArrowSchema: []byte{1, 2}}},
		Notes: []sqlplan.Note{{At: sqlplan.Position{Line: 1, Column: 13}, Message: "destination. is a qualifier here"}},
	}
	f := &fakePlanner{answer: answer}
	s := &Server{Planner: f}
	p := sqlPipeline("INSERT INTO destination.contacts SELECT id FROM customers")
	out := &apiv1.PipelinePlan{}
	got, err := s.planSQLWith(t.Context(), p, shopCatalog, nil, out)
	if err != nil || got == nil {
		t.Fatal(got, err)
	}
	if len(f.sent) != 1 || f.sent[0].DataPlane != "dp1" || len(f.sent[0].Request.Streams) != 1 || f.sent[0].Request.Streams[0].Name != "customers" {
		t.Errorf("sent %+v", f.sent)
	}
	if st := out.GetTransform().GetStatements(); len(st) != 1 || st[0].GetOutput() != "contacts" || !slices.Equal(out.GetNotes(), []string{"transform.sql:1:13: destination. is a qualifier here"}) {
		t.Errorf("plan %v", out)
	}
	if len(p.sqlPlan.GetOutputs()) != 1 || string(p.sqlPlan.GetOutputs()[0].GetArrowSchema()) != "\x01\x02" {
		t.Errorf("kept %v", p.sqlPlan)
	}

	f.answer = sqlplan.Answer{Errors: []sqlplan.Error{{Kind: "later_tier", Class: "group_window", Tier: 2, At: &sqlplan.Position{Line: 3, Column: 10}, Message: "a group window: transforms don't run group windows yet (tier 2)"}}}
	p, out = sqlPipeline("…"), &apiv1.PipelinePlan{}
	if got, err := s.planSQLWith(t.Context(), p, shopCatalog, nil, out); err != nil || got != nil {
		t.Fatal(got, err)
	}
	if !slices.Equal(out.GetProblems(), []string{"transform.sql:3:10: a group window: transforms don't run group windows yet (tier 2)"}) || out.GetTransform() != nil || p.sqlPlan != nil {
		t.Errorf("plan %v", out)
	}
}

// Where the statements can't be planned, a note says why, and the plan
// goes on.
func TestPlanSQLNotPlanned(t *testing.T) {
	cases := []struct {
		name    string
		planner SQLPlanner
		sql     string
		history map[string][]sqlplan.Column
		want    string
	}{
		{"no runner", &fakePlanner{err: &orchestration.Failure{Code: contract.NoRunner, Message: "no runner took it"}}, "INSERT INTO x SELECT * FROM orders", nil,
			`no runner of data plane "dp1" took it within 30s`},
		{"refused", &fakePlanner{err: &orchestration.Failure{Code: "CONFIG_INVALID", Message: "stream orders column id: x isn't a type the engine knows"}}, "INSERT INTO x SELECT * FROM orders", nil,
			`a runner of data plane "dp1" failed to plan it: CONFIG_INVALID: stream orders column id`},
		{"unreachable", &fakePlanner{err: errors.New("connection refused")}, "INSERT INTO x SELECT * FROM orders", nil,
			"sending it to a runner of data plane \"dp1\" failed: connection refused"},
		{"no planner", nil, "INSERT INTO x SELECT * FROM orders", nil, "this server can't send it to a runner"},
		{"unknown schema", &fakePlanner{}, "INSERT INTO x SELECT * FROM events", nil,
			"the schemas of streams events aren't known: the source declares no columns for them, and no sync has read them yet"},
	}
	for _, c := range cases {
		s := &Server{Planner: c.planner}
		out := &apiv1.PipelinePlan{}
		if got, err := s.planSQLWith(t.Context(), sqlPipeline(c.sql), shopCatalog, c.history, out); err != nil || got != nil {
			t.Errorf("%s: %v, %v", c.name, got, err)
		}
		if len(out.GetNotes()) != 1 || !strings.HasPrefix(out.GetNotes()[0], "transform.sql wasn't planned: "+c.want) || len(out.GetProblems()) != 0 {
			t.Errorf("%s: notes %q", c.name, out.GetNotes())
		}
	}
	// The pipeline's schema history gives a stream the catalog lists no
	// columns for.
	f := &fakePlanner{}
	history := map[string][]sqlplan.Column{"events": {{Name: "at", Type: "utf8"}}}
	if _, err := (&Server{Planner: f}).planSQLWith(t.Context(), sqlPipeline("INSERT INTO x SELECT * FROM events"), shopCatalog, history, &apiv1.PipelinePlan{}); err != nil || len(f.sent) != 1 {
		t.Errorf("sent %v: %v", f.sent, err)
	}
}

// A version keeps its SQL plan as JSON, and gives it back.
func TestSQLPlanRoundTrip(t *testing.T) {
	want := &apiv1.SqlPlan{Outputs: []*apiv1.SqlOutput{{Name: "contacts", PrimaryKey: []string{"id"}, ArrowSchema: []byte{0xff, 0, 1},
		Columns: []*apiv1.SchemaColumn{{Name: "id", Type: "int64", Nullable: true}}}}, Operators: []string{"op1"}}
	b, err := protojson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := sqlPlanOf(b)
	if err != nil || !proto.Equal(got, want) {
		t.Errorf("%v, %v", got, err)
	}
	if got, err := sqlPlanOf(nil); got != nil || err != nil {
		t.Errorf("none: %v, %v", got, err)
	}
}
