// Package schedules keeps one Temporal schedule per scheduled pipeline.
package schedules

import (
	"context"
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

type Schedules struct {
	Client client.Client
}

// ErrSyncRunning: a sync of the pipeline is running.
var ErrSyncRunning = errors.New("schedules: a sync is running")

// Upsert creates or updates the pipeline's schedule. A new schedule is
// paused if paused is set, and otherwise starts its first sync right away.
// Updates keep the schedule's pause state, which only SetPaused changes.
func (s *Schedules) Upsert(ctx context.Context, orgID, pipelineID string, sched *apiv1.Schedule, paused bool) error {
	spec := client.ScheduleSpec{Jitter: sched.GetJitter().AsDuration()}
	switch t := sched.GetTrigger().(type) {
	case *apiv1.Schedule_Every:
		spec.Intervals = []client.ScheduleIntervalSpec{{Every: t.Every.AsDuration()}}
	case *apiv1.Schedule_Cron:
		spec.CronExpressions = []string{t.Cron}
	default:
		return errors.New("schedules: no trigger")
	}
	action := &client.ScheduleWorkflowAction{
		ID:        contract.SyncWorkflowID(pipelineID),
		Workflow:  contract.SyncWorkflow,
		Args:      []any{contract.SyncInput{OrgID: orgID, PipelineID: pipelineID}},
		TaskQueue: contract.OrchestratorQueue,
	}
	return s.upsert(ctx, client.ScheduleOptions{
		ID:                 contract.ScheduleID(pipelineID),
		Spec:               spec,
		Action:             action,
		Overlap:            enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		TriggerImmediately: !paused,
		Paused:             paused,
	})
}

// EnsureRetention creates or updates the daily RetentionWorkflow schedule.
func (s *Schedules) EnsureRetention(ctx context.Context, in contract.RetentionInput) error {
	return s.upsert(ctx, client.ScheduleOptions{
		ID:   contract.RetentionScheduleID,
		Spec: client.ScheduleSpec{Intervals: []client.ScheduleIntervalSpec{{Every: 24 * time.Hour}}, Jitter: time.Hour},
		Action: &client.ScheduleWorkflowAction{
			ID:        contract.RetentionWorkflowID,
			Workflow:  contract.RetentionWorkflow,
			Args:      []any{in},
			TaskQueue: contract.OrchestratorQueue,
		},
		Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
	})
}

// upsert creates the schedule, or updates its spec, action and overlap
// policy. Its other options apply only when it is created.
func (s *Schedules) upsert(ctx context.Context, opts client.ScheduleOptions) error {
	_, err := s.Client.ScheduleClient().Create(ctx, opts)
	if err == nil {
		return nil
	}
	if !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("schedules: create %s: %w", opts.ID, err)
	}
	err = s.Client.ScheduleClient().GetHandle(ctx, opts.ID).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sc := in.Description.Schedule
			sc.Spec = &opts.Spec
			sc.Action = opts.Action
			if sc.Policy == nil {
				sc.Policy = &client.SchedulePolicies{}
			}
			sc.Policy.Overlap = opts.Overlap
			return &client.ScheduleUpdate{Schedule: &sc}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("schedules: update %s: %w", opts.ID, err)
	}
	return nil
}

// Delete removes the pipeline's schedule, if it has one. A running sync
// isn't stopped.
func (s *Schedules) Delete(ctx context.Context, pipelineID string) error {
	err := s.Client.ScheduleClient().GetHandle(ctx, contract.ScheduleID(pipelineID)).Delete(ctx)
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return nil
	}
	return err
}

func (s *Schedules) SetPaused(ctx context.Context, pipelineID string, paused bool) error {
	h := s.Client.ScheduleClient().GetHandle(ctx, contract.ScheduleID(pipelineID))
	var err error
	if paused {
		err = h.Pause(ctx, client.SchedulePauseOptions{Note: "paused through the API"})
	} else {
		err = h.Unpause(ctx, client.ScheduleUnpauseOptions{Note: "resumed through the API"})
	}
	if err != nil {
		return fmt.Errorf("schedules: pause %s: %w", pipelineID, err)
	}
	return nil
}

// Trigger starts a sync now, under the schedule's overlap policy, or returns
// ErrSyncRunning.
func (s *Schedules) Trigger(ctx context.Context, pipelineID string) error {
	h := s.Client.ScheduleClient().GetHandle(ctx, contract.ScheduleID(pipelineID))
	desc, err := h.Describe(ctx)
	if err != nil {
		return fmt.Errorf("schedules: describe %s: %w", pipelineID, err)
	}
	if len(desc.Info.RunningWorkflows) > 0 {
		return ErrSyncRunning
	}
	if err := h.Trigger(ctx, client.ScheduleTriggerOptions{Overlap: enumspb.SCHEDULE_OVERLAP_POLICY_SKIP}); err != nil {
		return fmt.Errorf("schedules: trigger %s: %w", pipelineID, err)
	}
	return nil
}
