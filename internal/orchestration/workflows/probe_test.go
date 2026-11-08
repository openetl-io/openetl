package workflows_test

import (
	"context"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
)

// Each probe runs on its data plane, one at a time, and a data plane no
// runner took one of is left until the next run.
func TestRetentionProbe(t *testing.T) {
	env := newHarness(t)
	env.RegisterWorkflows(workflows.Workflows{})
	env.RegisterStep(orchestration.NewStep(contract.ListRetentionProbes, func(_ context.Context, in contract.ListRetentionProbesInput) ([]contract.ConnectorOpInput, error) {
		if in.Max != contract.RetentionProbeMax {
			t.Errorf("listed with max %d", in.Max)
		}
		op := func(dp, p string) contract.ConnectorOpInput {
			return contract.ConnectorOpInput{OpID: "op-" + p, OrgID: "o", DataPlane: dp, Op: contract.OpRetention, PipelineID: p, SourceID: "s"}
		}
		return []contract.ConnectorOpInput{op("dp1", "p1"), op("gone", "p2"), op("gone", "p3"), op("dp1", "p4")}, nil
	}))
	p := newPlane(env, func(in contract.WorkInput) (time.Duration, contract.WorkOutcome) {
		if in.DataPlane == "gone" {
			return contract.OpPickup, contract.WorkOutcome{Code: contract.NoRunner, Message: "no runner took it in time", NonRetryable: true}
		}
		return time.Second, result(t, contract.ConnectorOpResult{})
	})
	env.ExecuteWorkflow(contract.RetentionProbeWorkflow, struct{}{})
	var res contract.RetentionProbeResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if res.Probed != 2 || res.Failed != 2 || len(p.sent) != 3 {
		t.Fatalf("result %+v, %d sent", res, len(p.sent))
	}
	for _, w := range p.sent {
		in := input[contract.ConnectorOpInput](t, w)
		if w.Kind != contract.ProbeRetention || w.Light != contract.LightOps || w.Pickup != contract.OpPickup || in.Op != contract.OpRetention {
			t.Errorf("sent %+v", w)
		}
	}
}
