package workflows

import (
	"cmp"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// PlanSQL plans a SQL transform on a runner of the data plane, whose engine
// plans it. SQL with errors is an answer, not a failure.
func (w Workflows) PlanSQL(ctx orchestration.Context, in contract.PlanSQLInput) (*sqlplan.Answer, error) {
	var res sqlplan.Answer
	if err := (&inbox{ctx: ctx}).runWork(retryPolicy{wait: time.Second, maxWait: 10 * time.Second, attempts: 3}, &res, func(n int32) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: in.DataPlane, ID: workID(ctx, "plan", n), Light: contract.LightOps, Kind: contract.PlanSQL, Input: in,
			Heartbeat: cmp.Or(w.AttemptHeartbeat, time.Minute), Pickup: contract.PlanPickup, Timeout: time.Minute,
		}
	}); err != nil {
		return nil, err
	}
	return &res, nil
}
