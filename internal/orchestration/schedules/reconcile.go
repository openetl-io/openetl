package schedules

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/store"
)

// reconcileRetry is how soon a reconcile that failed tries again.
const reconcileRetry = 30 * time.Second

// Reconciler makes what runs pipelines match the database, when an
// orchestrator starts: each continuous pipeline gets a supervisor, and each
// scheduled one its schedule. A switch of backends or a workflow an operator
// lost needs nothing else. Every step is safe to repeat, so any number of
// orchestrators can run it.
type Reconciler struct {
	Store     *store.Store
	Schedules *Schedules
	Backend   orchestration.Backend
	Logger    *slog.Logger
}

// Run reconciles once, trying until it has or ctx ends.
func (r *Reconciler) Run(ctx context.Context) {
	for {
		err := r.Reconcile(ctx)
		if err == nil {
			return
		}
		if ctx.Err() != nil {
			return
		}
		r.Logger.Warn("reconciling failed; trying again", "in", reconcileRetry, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(reconcileRetry):
		}
	}
}

// pipelineToRun is what a pipeline wants running.
type pipelineToRun struct {
	org, id    string
	paused     bool
	continuous bool
	schedule   *apiv1.Schedule
	// It has a continuous run running, whatever it is now.
	runningRun bool
}

// Reconcile does it once. It lists what's running before it reads the
// database, so what it stops was there before what it read: a pipeline an
// apply changes meanwhile keeps what that apply started.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	supervisors, err := r.Backend.ListRunning(ctx, contract.StreamSupervisorWorkflow)
	if err != nil {
		return err
	}
	schedules, err := r.Backend.ListSchedules(ctx)
	if err != nil {
		return err
	}
	rows, err := r.Store.ListPipelinesToRun(ctx)
	if err != nil {
		return err
	}
	var pipelines []pipelineToRun
	for _, p := range rows {
		var spec apiv1.PipelineSpec
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(p.Spec, &spec); err != nil {
			r.Logger.Warn("a pipeline's spec doesn't parse; leaving what runs it", "pipeline", p.ID, "err", err)
			continue
		}
		pipelines = append(pipelines, pipelineToRun{
			org: p.OrgID.String(), id: p.ID.String(), paused: p.Paused,
			continuous: spec.GetMode() == apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS, schedule: spec.GetSchedule(), runningRun: p.RunningRun,
		})
	}
	orgs, err := r.Store.ListOrgs(ctx)
	if err != nil {
		return err
	}
	ours := map[string]bool{}
	for _, o := range orgs {
		ours[o.String()] = true
	}
	plan := planReconcile(pipelines, supervisors, schedules, ours)
	var failed error
	note := func(what, id string, err error) {
		if err != nil {
			r.Logger.Warn("reconciling: "+what+" failed", "id", id, "err", err)
			failed = err
		}
	}
	for _, p := range plan.startSupervisors {
		_, err := r.Backend.Start(ctx, supervisor(p.org, p.id))
		note("starting a supervisor", p.id, err)
	}
	for _, id := range plan.cancelSupervisors {
		note("cancelling a supervisor", id, r.Backend.Cancel(ctx, id))
	}
	for _, p := range plan.upsertSchedules {
		note("creating a schedule", p.id, r.Schedules.Upsert(ctx, p.org, p.id, p.schedule, p.paused))
	}
	for _, p := range plan.pauseSchedules {
		note("pausing a schedule", p.id, r.Schedules.SetPaused(ctx, p.id, p.paused))
	}
	for _, id := range plan.deleteSchedules {
		note("deleting a schedule", id, r.Backend.DeleteSchedule(ctx, id))
	}
	if failed == nil && plan.changes() > 0 {
		r.Logger.Info("reconciled", "changes", plan.changes())
	}
	return failed
}

type reconcilePlan struct {
	startSupervisors  []pipelineToRun
	cancelSupervisors []string
	upsertSchedules   []pipelineToRun
	pauseSchedules    []pipelineToRun
	deleteSchedules   []string
}

func (p reconcilePlan) changes() int {
	return len(p.startSupervisors) + len(p.cancelSupervisors) + len(p.upsertSchedules) + len(p.pauseSchedules) + len(p.deleteSchedules)
}

// planReconcile decides what to change. What runs and isn't wanted is stopped
// only if it's of an org in ours: a namespace may hold another install's
// schedules, and an empty database mustn't delete them.
func planReconcile(pipelines []pipelineToRun, supervisors []orchestration.RunningWorkflow, schedules []orchestration.ScheduleState, ours map[string]bool) reconcilePlan {
	wantSupervisor, wantSchedule := map[string]bool{}, map[string]bool{}
	running := map[string]bool{}
	for _, w := range supervisors {
		running[w.ID] = true
	}
	scheduled := map[string]orchestration.ScheduleState{}
	for _, s := range schedules {
		scheduled[s.ID] = s
	}
	var plan reconcilePlan
	for _, p := range pipelines {
		if p.continuous {
			id := contract.StreamWorkflowID(p.id)
			wantSupervisor[id] = true
			if !running[id] {
				plan.startSupervisors = append(plan.startSupervisors, p)
			}
			continue
		}
		// Only a supervisor can stop the job of a run left running, and record
		// its end. It then finds the pipeline scheduled, and ends.
		if p.runningRun {
			id := contract.StreamWorkflowID(p.id)
			wantSupervisor[id] = true
			if !running[id] {
				plan.startSupervisors = append(plan.startSupervisors, p)
			}
		}
		id := contract.ScheduleID(p.id)
		if p.schedule.GetTrigger() == nil {
			continue
		}
		wantSchedule[id] = true
		s, ok := scheduled[id]
		switch {
		case !ok:
			plan.upsertSchedules = append(plan.upsertSchedules, p)
		case s.Paused != p.paused:
			plan.pauseSchedules = append(plan.pauseSchedules, p)
		}
	}
	for _, w := range supervisors {
		if !wantSupervisor[w.ID] && ours[w.Org] {
			plan.cancelSupervisors = append(plan.cancelSupervisors, w.ID)
		}
	}
	for _, s := range schedules {
		if !wantSchedule[s.ID] && ours[s.Org] && s.ID != contract.RetentionScheduleID && strings.HasPrefix(s.ID, contract.ScheduleID("")) {
			plan.deleteSchedules = append(plan.deleteSchedules, s.ID)
		}
	}
	return plan
}
