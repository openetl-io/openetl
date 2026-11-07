package workflows_test

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/temporalbackend"
	"github.com/openetl-io/openetl/internal/orchestration/workflows"
)

// harness runs a workflow on a backend's test environment, on a clock that
// moves only while the workflow waits. Workflows' tests see only this.
type harness interface {
	RegisterWorkflows(workflows.Workflows)
	RegisterWorkflow(orchestration.Workflow)
	RegisterStep(orchestration.Step)
	// RegisterDelayedCallback calls f after d on the workflow's clock.
	RegisterDelayedCallback(f func(), d time.Duration)
	Send(orchestration.Message)
	CancelWorkflow()
	Now() time.Time
	SetStartTime(time.Time)
	// PastHistoryLimit makes the workflow's ShouldContinue say so.
	PastHistoryLimit()
	ExecuteWorkflow(name any, in ...any)
	IsWorkflowCompleted() bool
	// GetWorkflowError is how it ended: nil, orchestration.ErrStopped, a
	// *orchestration.Failure, a *continued, or whatever else it ended with.
	GetWorkflowError() error
	GetWorkflowResult(out any) error
}

func newHarness(t *testing.T) harness {
	t.Helper()
	if os.Getenv(onDBOS) != "" {
		return newDBOSHarness(t)
	}
	var s testsuite.WorkflowTestSuite
	return &temporalHarness{TestWorkflowEnvironment: s.NewTestWorkflowEnvironment()}
}

// continued is the error of a workflow that ended by continuing as new.
type continued struct {
	input json.RawMessage
}

func (c *continued) Error() string { return "continued as new" }

// continuedWith is the input err says the workflow continued with.
func continuedWith(t *testing.T, err error) contract.StreamInput {
	t.Helper()
	c, ok := errors.AsType[*continued](err)
	if !ok {
		t.Fatalf("got %v, want to continue as new", err)
	}
	var next contract.StreamInput
	if err := json.Unmarshal(c.input, &next); err != nil {
		t.Fatal(err)
	}
	return next
}

type temporalHarness struct {
	*testsuite.TestWorkflowEnvironment
}

func (h *temporalHarness) RegisterWorkflows(w workflows.Workflows) {
	for _, wf := range w.All() {
		temporalbackend.RegisterWorkflow(h, wf)
	}
}

func (h *temporalHarness) RegisterWorkflow(w orchestration.Workflow) {
	temporalbackend.RegisterWorkflow(h, w)
}

func (h *temporalHarness) RegisterStep(s orchestration.Step) { temporalbackend.RegisterStep(h, s) }

func (h *temporalHarness) Send(m orchestration.Message) {
	h.SignalWorkflow(temporalbackend.Mailbox, m)
}

func (h *temporalHarness) PastHistoryLimit() { h.SetCurrentHistoryLength(2001) }

func (h *temporalHarness) GetWorkflowError() error {
	err := h.TestWorkflowEnvironment.GetWorkflowError()
	if can, ok := errors.AsType[*workflow.ContinueAsNewError](err); ok {
		var input json.RawMessage
		if err := converter.GetDefaultDataConverter().FromPayloads(can.Input, &input); err != nil {
			return err
		}
		return &continued{input}
	}
	return temporalbackend.FailureOf(err)
}
