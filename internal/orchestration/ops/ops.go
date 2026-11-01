// Package ops runs connector operations through ConnectorOpWorkflow.
package ops

import (
	"cmp"
	"context"
	"fmt"

	"github.com/google/uuid"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

type Ops struct {
	Client client.Client
}

// Run starts the operation and waits for its result.
func (o *Ops) Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	run, err := o.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       contract.OpWorkflowID(cmp.Or(in.OpID, uuid.NewString())),
		TaskQueue:                contract.OrchestratorQueue,
		WorkflowExecutionTimeout: contract.OpTimeout,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			contract.OrgIDAttr.ValueSet(in.OrgID),
			contract.WorkspaceIDAttr.ValueSet(in.WorkspaceID),
			contract.DataPlaneIDAttr.ValueSet(in.DataPlane),
			contract.ConnectorDefAttr.ValueSet([]string{in.Connector}),
		),
	}, contract.ConnectorOpWorkflow, in)
	if err != nil {
		return res, fmt.Errorf("ops: start %s: %w", in.Op, err)
	}
	return res, run.Get(ctx, &res)
}
