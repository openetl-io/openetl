// Package schedules keeps one Temporal schedule per scheduled pipeline.
package schedules

import (
	"context"
	"errors"
	"fmt"

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

// Upsert creates or updates the pipeline's schedule. A new schedule starts
// its first sync right away.
func (s *Schedules) Upsert(ctx context.Context, orgID, pipelineID string, sched *apiv1.Schedule) error {
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
	id := contract.ScheduleID(pipelineID)
	_, err := s.Client.ScheduleClient().Create(ctx, client.ScheduleOptions{
		ID:                 id,
		Spec:               spec,
		Action:             action,
		Overlap:            enumspb.SCHEDULE_OVERLAP_POLICY_SKIP,
		TriggerImmediately: true,
	})
	if err == nil {
		return nil
	}
	if !errors.Is(err, temporal.ErrScheduleAlreadyRunning) {
		return fmt.Errorf("schedules: create %s: %w", id, err)
	}
	err = s.Client.ScheduleClient().GetHandle(ctx, id).Update(ctx, client.ScheduleUpdateOptions{
		DoUpdate: func(in client.ScheduleUpdateInput) (*client.ScheduleUpdate, error) {
			sc := in.Description.Schedule
			sc.Spec = &spec
			sc.Action = action
			if sc.Policy == nil {
				sc.Policy = &client.SchedulePolicies{}
			}
			sc.Policy.Overlap = enumspb.SCHEDULE_OVERLAP_POLICY_SKIP
			return &client.ScheduleUpdate{Schedule: &sc}, nil
		},
	})
	if err != nil {
		return fmt.Errorf("schedules: update %s: %w", id, err)
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
