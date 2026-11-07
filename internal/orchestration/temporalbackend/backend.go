// Package temporalbackend runs workflows on Temporal, on a worker that polls
// its one task queue. Runners never touch it.
package temporalbackend

import (
	"context"
	"errors"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"

	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/platform/temporalx"
)

// Backend is a namespace on a Temporal server.
type Backend struct {
	client    client.Client
	namespace string
}

var _ orchestration.Backend = (*Backend)(nil)

// Open connects to Temporal, and adds the namespace's search attributes.
func Open(ctx context.Context, o temporalx.Options) (*Backend, error) {
	c, err := temporalx.Dial(ctx, o)
	if err != nil {
		return nil, err
	}
	if err := temporalx.EnsureSearchAttributes(ctx, c, o.Namespace, temporalx.SearchAttributes); err != nil {
		c.Close()
		return nil, err
	}
	return &Backend{client: c, namespace: o.Namespace}, nil
}

func (b *Backend) Close() { b.client.Close() }

func (b *Backend) Run(ctx context.Context, workflows []orchestration.Workflow, steps []orchestration.Step) error {
	w := worker.New(b.client, Queue, worker.Options{})
	for _, wf := range workflows {
		RegisterWorkflow(w, wf)
	}
	for _, s := range steps {
		RegisterStep(w, s)
	}
	if err := w.Start(); err != nil {
		return err
	}
	<-ctx.Done()
	w.Stop()
	return nil
}

func startOptions(o orchestration.StartOptions) client.StartWorkflowOptions {
	return client.StartWorkflowOptions{
		ID: o.ID, TaskQueue: Queue, WorkflowExecutionTimeout: o.Timeout,
		TypedSearchAttributes: temporal.NewSearchAttributes(searchAttributes(o.Attributes)...),
	}
}

func (b *Backend) Start(ctx context.Context, o orchestration.StartOptions) (orchestration.Run, error) {
	run, err := b.client.ExecuteWorkflow(ctx, startOptions(o), o.Workflow, o.Input)
	if err != nil {
		return nil, err
	}
	return started{run}, nil
}

type started struct{ client.WorkflowRun }

func (r started) Get(ctx context.Context, out any) error {
	return FailureOf(r.WorkflowRun.Get(ctx, out))
}

func (b *Backend) Send(ctx context.Context, workflowID string, m orchestration.Message) error {
	return ignoreNotFound(b.client.SignalWorkflow(ctx, workflowID, "", Mailbox, m))
}

func (b *Backend) SendWithStart(ctx context.Context, o orchestration.StartOptions, m orchestration.Message) error {
	_, err := b.client.SignalWithStartWorkflow(ctx, o.ID, Mailbox, m, startOptions(o), o.Workflow, o.Input)
	return err
}

func (b *Backend) Cancel(ctx context.Context, workflowID string) error {
	return ignoreNotFound(b.client.CancelWorkflow(ctx, workflowID, ""))
}

func ignoreNotFound(err error) error {
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

func (b *Backend) ListRunning(ctx context.Context, workflow string) ([]orchestration.RunningWorkflow, error) {
	req := &workflowservice.ListWorkflowExecutionsRequest{
		Namespace: b.namespace,
		Query:     fmt.Sprintf("WorkflowType = '%s' AND ExecutionStatus = 'Running'", workflow),
	}
	var out []orchestration.RunningWorkflow
	for {
		res, err := b.client.ListWorkflow(ctx, req)
		if err != nil {
			return nil, err
		}
		for _, w := range res.GetExecutions() {
			out = append(out, orchestration.RunningWorkflow{ID: w.GetExecution().GetWorkflowId(), Org: org(w.GetSearchAttributes())})
		}
		if req.NextPageToken = res.GetNextPageToken(); len(req.NextPageToken) == 0 {
			return out, nil
		}
	}
}

// org reads the org's search attribute, which is empty if it's missing.
func org(a *commonpb.SearchAttributes) string {
	var s string
	if p := a.GetIndexedFields()[temporalx.OrgIDAttr.GetName()]; p != nil {
		_ = converter.GetDefaultDataConverter().FromPayload(p, &s)
	}
	return s
}

func (b *Backend) UpsertSchedule(ctx context.Context, s orchestration.Schedule) error {
	spec := client.ScheduleSpec{Jitter: s.Jitter}
	if s.Every > 0 {
		spec.Intervals = []client.ScheduleIntervalSpec{{Every: s.Every}}
	} else {
		spec.CronExpressions = []string{s.Cron}
	}
	action := &client.ScheduleWorkflowAction{ID: s.WorkflowID, Workflow: s.Workflow, Args: []any{s.Input}, TaskQueue: Queue}
	const overlap = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
	schedules := b.client.ScheduleClient()
	_, err := schedules.Create(ctx, client.ScheduleOptions{
		ID: s.ID, Spec: spec, Action: action, Overlap: overlap, TriggerImmediately: s.StartNow && !s.Paused, Paused: s.Paused,
		TypedSearchAttributes: temporal.NewSearchAttributes(searchAttributes(orchestration.Attributes{Org: s.Org})...),
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("create schedule %s: %w", s.ID, err)
	}
	err = schedules.GetHandle(ctx, s.ID).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sc := in.Description.Schedule
			sc.Spec = &spec
			sc.Action = action
			if sc.Policy == nil {
				sc.Policy = &client.SchedulePolicies{}
			}
			sc.Policy.Overlap = overlap
			return &client.ScheduleUpdate{Schedule: &sc}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("update schedule %s: %w", s.ID, err)
	}
	return nil
}

func (b *Backend) DeleteSchedule(ctx context.Context, id string) error {
	return ignoreNotFound(b.client.ScheduleClient().GetHandle(ctx, id).Delete(ctx))
}

func (b *Backend) PauseSchedule(ctx context.Context, id string, paused bool) error {
	h := b.client.ScheduleClient().GetHandle(ctx, id)
	if paused {
		return h.Pause(ctx, client.SchedulePauseOptions{Note: "paused through the API"})
	}
	return h.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed through the API"})
}

func (b *Backend) TriggerSchedule(ctx context.Context, id string) error {
	h := b.client.ScheduleClient().GetHandle(ctx, id)
	desc, err := h.Describe(ctx)
	if err != nil {
		return fmt.Errorf("describe schedule %s: %w", id, err)
	}
	if len(desc.Info.RunningWorkflows) > 0 {
		return orchestration.ErrRunning
	}
	return h.Trigger(ctx, client.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP})
}

func (b *Backend) ListSchedules(ctx context.Context) ([]orchestration.ScheduleState, error) {
	it, err := b.client.ScheduleClient().List(ctx, client.ScheduleListOptions{})
	if err != nil {
		return nil, err
	}
	var out []orchestration.ScheduleState
	for it.HasNext() {
		s, err := it.Next()
		if err != nil {
			return nil, err
		}
		out = append(out, orchestration.ScheduleState{ID: s.ID, Org: org(s.SearchAttributes), Paused: s.Paused})
	}
	return out, nil
}
