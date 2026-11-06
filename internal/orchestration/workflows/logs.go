package workflows

import (
	"cmp"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// ReadLogs reads a page of a sync's log on a runner of the data plane that
// ran it, where the log is.
func (w Workflows) ReadLogs(ctx orchestration.Context, in contract.ReadLogsInput) (*contract.ReadLogsResult, error) {
	var res contract.ReadLogsResult
	if err := (&inbox{ctx: ctx}).runWork(retryPolicy{wait: time.Second, maxWait: 100 * time.Second, attempts: 3}, &res, func(n int32) contract.WorkInput {
		return contract.WorkInput{
			OrgID: in.OrgID, DataPlane: in.DataPlane, ID: workID(ctx, "logs", n), Light: contract.LightLogs, Kind: contract.ReadLogs, Input: in,
			Heartbeat: cmp.Or(w.AttemptHeartbeat, time.Minute), Pickup: contract.LogsPickup, Timeout: time.Minute,
		}
	}); err != nil {
		return nil, err
	}
	return &res, nil
}
