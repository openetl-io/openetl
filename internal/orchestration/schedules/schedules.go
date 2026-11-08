// Package schedules keeps what runs each pipeline: a schedule per scheduled
// pipeline, and a supervisor per continuous one.
package schedules

import (
	"context"
	"errors"
	"fmt"
	"time"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

type Schedules struct {
	Backend orchestration.Backend
}

// ErrSyncRunning: a sync of the pipeline is running.
var ErrSyncRunning = errors.New("schedules: a sync is running")

// Upsert creates or updates the pipeline's schedule. A new schedule is
// paused if paused is set, and otherwise starts its first sync right away.
// Updates keep the schedule's pause state, which only SetPaused changes.
func (s *Schedules) Upsert(ctx context.Context, orgID, pipelineID string, sched *apiv1.Schedule, paused bool) error {
	o := orchestration.Schedule{
		ID:         contract.ScheduleID(pipelineID),
		Jitter:     sched.GetJitter().AsDuration(),
		WorkflowID: contract.SyncWorkflowID(pipelineID),
		Workflow:   contract.SyncWorkflow,
		Input:      contract.SyncInput{OrgID: orgID, PipelineID: pipelineID},
		Org:        orgID,
		Paused:     paused,
		StartNow:   true,
	}
	switch t := sched.GetTrigger().(type) {
	case *apiv1.Schedule_Every:
		o.Every = t.Every.AsDuration()
	case *apiv1.Schedule_Cron:
		o.Cron = t.Cron
	default:
		return errors.New("schedules: no trigger")
	}
	if err := s.Backend.UpsertSchedule(ctx, o); err != nil {
		return fmt.Errorf("schedules: %w", err)
	}
	return nil
}

// EnsureRetention creates or updates the daily RetentionWorkflow schedule.
func (s *Schedules) EnsureRetention(ctx context.Context, in contract.RetentionInput) error {
	err := s.Backend.UpsertSchedule(ctx, orchestration.Schedule{
		ID:         contract.RetentionScheduleID,
		Every:      24 * time.Hour,
		Jitter:     time.Hour,
		WorkflowID: contract.RetentionWorkflowID,
		Workflow:   contract.RetentionWorkflow,
		Input:      in,
	})
	if err != nil {
		return fmt.Errorf("schedules: %w", err)
	}
	return nil
}

// EnsureCatalogRefresh creates or updates the CatalogRefreshWorkflow
// schedule, which lists connected sources of connectors.
func (s *Schedules) EnsureCatalogRefresh(ctx context.Context) error {
	err := s.Backend.UpsertSchedule(ctx, orchestration.Schedule{
		ID:         contract.CatalogRefreshScheduleID,
		Every:      contract.CatalogRefreshEvery,
		Jitter:     time.Minute,
		WorkflowID: contract.CatalogRefreshWorkflowID,
		Workflow:   contract.CatalogRefreshWorkflow,
		Input:      struct{}{},
		StartNow:   true,
	})
	if err != nil {
		return fmt.Errorf("schedules: %w", err)
	}
	return nil
}

// EnsureRetentionProbe creates or updates the RetentionProbeWorkflow
// schedule.
func (s *Schedules) EnsureRetentionProbe(ctx context.Context) error {
	err := s.Backend.UpsertSchedule(ctx, orchestration.Schedule{
		ID:         contract.RetentionProbeScheduleID,
		Every:      contract.RetentionProbeEvery,
		Jitter:     time.Minute,
		WorkflowID: contract.RetentionProbeWorkflowID,
		Workflow:   contract.RetentionProbeWorkflow,
		Input:      struct{}{},
	})
	if err != nil {
		return fmt.Errorf("schedules: %w", err)
	}
	return nil
}

// Delete removes the pipeline's schedule, if it has one. A running sync
// isn't stopped.
func (s *Schedules) Delete(ctx context.Context, pipelineID string) error {
	return s.Backend.DeleteSchedule(ctx, contract.ScheduleID(pipelineID))
}

func (s *Schedules) SetPaused(ctx context.Context, pipelineID string, paused bool) error {
	if err := s.Backend.PauseSchedule(ctx, contract.ScheduleID(pipelineID), paused); err != nil {
		return fmt.Errorf("schedules: pause %s: %w", pipelineID, err)
	}
	return nil
}

// Trigger starts a sync now, under the schedule's overlap policy, or returns
// ErrSyncRunning.
func (s *Schedules) Trigger(ctx context.Context, pipelineID string) error {
	err := s.Backend.TriggerSchedule(ctx, contract.ScheduleID(pipelineID))
	if errors.Is(err, orchestration.ErrRunning) {
		return ErrSyncRunning
	}
	if err != nil {
		return fmt.Errorf("schedules: trigger %s: %w", pipelineID, err)
	}
	return nil
}

func supervisor(orgID, pipelineID string) orchestration.StartOptions {
	return orchestration.StartOptions{
		ID:         contract.StreamWorkflowID(pipelineID),
		Workflow:   contract.StreamSupervisorWorkflow,
		Input:      contract.StreamInput{OrgID: orgID, PipelineID: pipelineID},
		Attributes: orchestration.Attributes{Org: orgID, Pipeline: pipelineID},
	}
}

// Refresh starts the continuous pipeline's supervisor, or tells the one
// running to look at the pipeline again.
func (s *Schedules) Refresh(ctx context.Context, orgID, pipelineID string) error {
	err := s.Backend.SendWithStart(ctx, supervisor(orgID, pipelineID), orchestration.Message{Kind: contract.RefreshMessage})
	if err != nil {
		return fmt.Errorf("schedules: refresh %s: %w", contract.StreamWorkflowID(pipelineID), err)
	}
	return nil
}

// StopStream cancels the pipeline's supervisor, if one is running, which
// stops its job at a checkpoint.
func (s *Schedules) StopStream(ctx context.Context, pipelineID string) error {
	if err := s.Backend.Cancel(ctx, contract.StreamWorkflowID(pipelineID)); err != nil {
		return fmt.Errorf("schedules: stop %s: %w", contract.StreamWorkflowID(pipelineID), err)
	}
	return nil
}
