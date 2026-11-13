package workflows_test

import (
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// Planning SQL is a light operation on the pipeline's data plane, which
// carries its request and gives the engine's answer.
func TestPlanSQLIsALightOperation(t *testing.T) {
	env := newHarness(t)
	env.RegisterWorkflows(workflows.Workflows{})
	p := newPlane(env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		req := input[contract.PlanSQLInput](t, in).Request
		return time.Second, result(t, sqlplan.Answer{Statements: []sqlplan.Statement{{Output: req.Streams[0].Name, Class: "row_wise"}}})
	})
	req := sqlplan.Request{SQL: "INSERT INTO orders SELECT * FROM orders", Streams: []sqlplan.Stream{{Name: "orders", Columns: []sqlplan.Column{{Name: "id", Type: "int64"}}}}}
	env.ExecuteWorkflow(contract.PlanSQLWorkflow, contract.PlanSQLInput{OrgID: "o", DataPlane: "dp1", Request: req})
	var res sqlplan.Answer
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.Statements) != 1 || res.Statements[0].Output != "orders" {
		t.Fatalf("got %+v", res)
	}
	w := p.sent[0]
	if len(p.sent) != 1 || w.Light != contract.LightOps || w.Kind != contract.PlanSQL || w.DataPlane != "dp1" || w.Pickup != contract.PlanPickup || w.Size.CPU != 0 {
		t.Errorf("sent %+v", p.sent)
	}
}

// No runner taking it fails it at once, in a way the API can tell; a
// request the engine refuses isn't tried again.
func TestPlanSQLFailures(t *testing.T) {
	for _, tc := range []struct {
		outcome contract.WorkOutcome
		sends   int
	}{
		{contract.WorkOutcome{Code: contract.NoRunner, Message: "no runner took it in time", NonRetryable: true}, 1},
		{fail(errs.ConfigInvalid), 1},
		{fail(errs.Internal), 3},
	} {
		code, sends := tc.outcome.Code, tc.sends
		env := newHarness(t)
		env.RegisterWorkflows(workflows.Workflows{})
		p := newPlane(env, func(contract.WorkInput) (time.Duration, contract.WorkOutcome) {
			return time.Second, tc.outcome
		})
		env.ExecuteWorkflow(contract.PlanSQLWorkflow, contract.PlanSQLInput{OrgID: "o", DataPlane: "dp1"})
		if err := env.GetWorkflowError(); orchestration.CodeOf(err) != code {
			t.Errorf("%s: got %v", code, err)
		}
		if len(p.sent) != sends {
			t.Errorf("%s: sent %d times", code, len(p.sent))
		}
	}
}
