package workflows

import (
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// CatalogRefresh lists every org's connected sources of connectors into the
// catalog. A source that fails to list keeps its last listing, and its
// error, which doesn't fail the workflow.
func (w Workflows) CatalogRefresh(ctx orchestration.Context, _ struct{}) (*contract.CatalogRefreshResult, error) {
	var res contract.CatalogRefreshResult
	step := orchestration.StepOptions{Timeout: 10 * time.Minute, Attempts: 3}
	if err := ctx.Step(contract.RefreshCatalog, step, struct{}{}, &res.Sources); err != nil {
		return nil, err
	}
	return &res, nil
}
