// Package ops runs connector operations through ConnectorOpWorkflow, and
// reads syncs' logs through ReadLogsWorkflow.
package ops

import (
	"cmp"
	"context"
	"fmt"
	"time"

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
	run, err := o.start(ctx, in)
	if err != nil {
		return res, err
	}
	return res, run.Get(ctx, &res)
}

// Start starts the operation, without waiting for it.
func (o *Ops) Start(ctx context.Context, in contract.ConnectorOpInput) error {
	_, err := o.start(ctx, in)
	return err
}

func (o *Ops) start(ctx context.Context, in contract.ConnectorOpInput) (client.WorkflowRun, error) {
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
		return nil, fmt.Errorf("ops: start %s: %w", in.Op, err)
	}
	return run, nil
}

// ReadLogs reads a page of a sync's log on its data plane, and waits for it.
func (o *Ops) ReadLogs(ctx context.Context, in contract.ReadLogsInput) (contract.ReadLogsResult, error) {
	var res contract.ReadLogsResult
	run, err := o.Client.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:                       contract.LogsWorkflowID(uuid.NewString()),
		TaskQueue:                contract.OrchestratorQueue,
		WorkflowExecutionTimeout: 2 * time.Minute,
		TypedSearchAttributes: temporal.NewSearchAttributes(
			contract.OrgIDAttr.ValueSet(in.OrgID),
			contract.PipelineIDAttr.ValueSet(in.PipelineID),
			contract.DataPlaneIDAttr.ValueSet(in.DataPlane),
		),
	}, contract.ReadLogsWorkflow, in)
	if err != nil {
		return res, fmt.Errorf("ops: read a log: %w", err)
	}
	return res, run.Get(ctx, &res)
}
