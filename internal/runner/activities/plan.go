package activities

import (
	"context"

	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// Plans plans SQL transforms for the control plane, which can't: the engine
// does, in one short call.
type Plans struct {
	Engine engineclient.Options
}

// PlanSQL plans the input's SQL against its streams.
func (a *Plans) PlanSQL(ctx context.Context, in contract.PlanSQLInput) (sqlplan.Answer, error) {
	return engineclient.Plan(ctx, a.Engine, in.Request)
}
