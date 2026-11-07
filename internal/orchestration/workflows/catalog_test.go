package workflows_test

import (
	"context"
	"testing"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
)

func TestCatalogRefresh(t *testing.T) {
	env := newHarness(t)
	env.RegisterWorkflows(workflows.Workflows{})
	env.RegisterStep(orchestration.NewStep(contract.RefreshCatalog, func(context.Context, struct{}) ([]contract.ListedSource, error) {
		return []contract.ListedSource{{Org: "o", Source: "official", Packages: 3}, {Org: "o", Source: "acme", Error: "401 Unauthorized"}}, nil
	}))
	env.ExecuteWorkflow(contract.CatalogRefreshWorkflow, struct{}{})
	var res contract.CatalogRefreshResult
	if err := env.GetWorkflowResult(&res); err != nil {
		t.Fatal(err)
	}
	if len(res.Sources) != 2 || res.Sources[0].Packages != 3 || res.Sources[1].Error == "" {
		t.Errorf("result %+v", res)
	}
}
