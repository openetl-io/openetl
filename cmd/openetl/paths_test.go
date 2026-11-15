package main

import (
	"bytes"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// openetl plan shows each stream's way to the destination: what its source
// gives, each operator with what it needs, and where rows are exchanged. An
// aggregate exchanged on its group key reaches its writer with that one
// exchange.
func TestPlanShowsPartitioning(t *testing.T) {
	stream := func(name string, p plan.Path) *apiv1.StreamPlan {
		sp := &apiv1.StreamPlan{Name: name}
		sp.Partitioning, sp.Operators = p.API()
		return sp
	}
	key := []string{"id"}
	rename := sqlplan.Statement{At: sqlplan.Position{Line: 1, Column: 1}, Passes: map[string]string{"id": "customer_id"}}
	aggregate := plan.Operator{Kind: plan.OperatorStatement, At: sqlplan.Position{Line: 5, Column: 1},
		Need: plan.Need{Columns: []string{"k"}}, Passes: map[string]string{"k": "k"}}
	var out bytes.Buffer
	printPaths(&out, []*apiv1.StreamPlan{
		stream("orders", plan.Place(plan.SourcePartitioning(key, true, false), plan.WriterOperator(key, false))),
		stream("changes", plan.Place(plan.SourcePartitioning(key, true, true), plan.WriterOperator(key, false))),
		stream("logs", plan.Place(plan.SourcePartitioning(nil, false, false), plan.WriterOperator(nil, false))),
		stream("contacts", plan.StatementPath(plan.SourcePartitioning(key, true, false), rename, []string{"customer_id"}, false)),
		stream("hourly", plan.Place(plan.Partitioning{}, aggregate, plan.WriterOperator([]string{"window_start", "k"}, false))),
		stream("audit", plan.Place(plan.Partitioning{}, plan.WriterOperator(key, true))),
		{Name: "unplanned"},
	})
	want := `  partitioning:
    orders    split by id -> writer, needs id
    changes   unpartitioned -> exchange by key group on id -> writer, needs id
    logs      unpartitioned -> writer
    contacts  split by id -> statement 1:1 -> split by customer_id -> writer, needs customer_id
    hourly    unpartitioned -> exchange by key group on k -> statement 5:1, needs k -> writer, needs window_start, k
    audit     unpartitioned -> on one lane -> writer, needs id
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}
