package workflows

import (
	"cmp"
	"time"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// teardownPickup is how long a teardown waits for a runner. The pipeline it
// cleans up after is gone, so nothing would ask again; a runner may have died
// with it, and its replacement may take a while.
const teardownPickup = 5 * time.Minute

// ConnectorOp runs Check, Discover or Teardown on a runner of the data
// plane, or cancels a deleted pipeline's Flink jobs. A teardown that a runner
// took retries until the operation times out, since the pipeline it cleans up
// after is gone; one no runner takes in teardownPickup ends there.
func (w Workflows) ConnectorOp(ctx orchestration.Context, in contract.ConnectorOpInput) (*contract.ConnectorOpResult, error) {
	work := contract.WorkInput{
		OrgID: in.OrgID, DataPlane: in.DataPlane, Light: contract.LightOps, Kind: contract.CheckConnector, Input: in,
		Heartbeat: cmp.Or(w.AttemptHeartbeat, time.Minute), Pickup: contract.OpPickup, Timeout: 2 * time.Minute,
	}
	policy := retryPolicy{wait: time.Second, maxWait: 100 * time.Second, attempts: 3}
	switch in.Op {
	case contract.OpDiscover:
		work.Kind, work.Timeout = contract.DiscoverSource, 10*time.Minute
	case contract.OpTeardown:
		work.Kind = contract.TeardownSource
		if in.SourceID == "" {
			work.Kind = contract.TeardownDestination
		}
		work.Pickup = teardownPickup
		policy = retryPolicy{wait: time.Second, maxWait: time.Minute}
	case contract.OpTeardownFlink:
		// The operation waits for the jobs' deployments to go.
		work.Kind, work.Pickup, work.Timeout = contract.TeardownFlink, teardownPickup, 10*time.Minute
		policy = retryPolicy{wait: time.Second, maxWait: time.Minute}
	}
	var res contract.ConnectorOpResult
	if err := (&inbox{ctx: ctx}).runWork(policy, &res, func(n int32) contract.WorkInput {
		w := work
		w.ID = workID(ctx, in.Op, n)
		return w
	}); err != nil {
		return nil, err
	}
	return &res, nil
}
