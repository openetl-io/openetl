package schedules

import (
	"slices"
	"testing"

	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

var hourly = &apiv1.Schedule{Trigger: &apiv1.Schedule_Every{Every: durationpb.New(3600_000_000_000)}}

func ids(ps []pipelineToRun) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.id)
	}
	return out
}

// Each pipeline gets what runs it, and what's already there is left alone.
func TestReconcileStartsWhatIsMissing(t *testing.T) {
	plan := planReconcile(
		[]pipelineToRun{
			{org: "o", id: "c-lost", continuous: true},
			{org: "o", id: "c-running", continuous: true},
			{org: "o", id: "s-lost", schedule: hourly},
			{org: "o", id: "s-there", schedule: hourly},
		},
		[]orchestration.RunningWorkflow{{ID: contract.StreamWorkflowID("c-running"), Org: "o"}},
		[]orchestration.ScheduleState{{ID: contract.ScheduleID("s-there"), Org: "o"}},
		map[string]bool{"o": true},
	)
	if got := ids(plan.startSupervisors); !slices.Equal(got, []string{"c-lost"}) {
		t.Errorf("supervisors started: %v", got)
	}
	if got := ids(plan.upsertSchedules); !slices.Equal(got, []string{"s-lost"}) {
		t.Errorf("schedules created: %v", got)
	}
	if plan.changes() != 2 {
		t.Errorf("plan %+v", plan)
	}
}

// A schedule a failed pause or resume left as it was follows the pipeline.
func TestReconcileRepairsAPause(t *testing.T) {
	plan := planReconcile(
		[]pipelineToRun{
			{org: "o", id: "a", paused: true, schedule: hourly},
			{org: "o", id: "b", schedule: hourly},
			{org: "o", id: "c", paused: true, schedule: hourly},
		},
		nil,
		[]orchestration.ScheduleState{
			{ID: contract.ScheduleID("a"), Org: "o"},
			{ID: contract.ScheduleID("b"), Org: "o", Paused: true},
			{ID: contract.ScheduleID("c"), Org: "o", Paused: true},
		},
		map[string]bool{"o": true},
	)
	if got := ids(plan.pauseSchedules); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("schedules paused or resumed: %v", got)
	}
}

// Anything else of ours stops, and the deployment's retention schedule stays.
func TestReconcileCancelsWhatNothingWants(t *testing.T) {
	plan := planReconcile(
		[]pipelineToRun{
			{org: "o", id: "now-scheduled", schedule: hourly},
			{org: "o", id: "now-continuous", continuous: true},
		},
		[]orchestration.RunningWorkflow{
			{ID: contract.StreamWorkflowID("now-scheduled"), Org: "o"},
			{ID: contract.StreamWorkflowID("gone"), Org: "o"},
			{ID: contract.StreamWorkflowID("now-continuous"), Org: "o"},
		},
		[]orchestration.ScheduleState{
			{ID: contract.ScheduleID("now-scheduled"), Org: "o"},
			{ID: contract.ScheduleID("now-continuous"), Org: "o"},
			{ID: contract.ScheduleID("gone"), Org: "o"},
			{ID: contract.RetentionScheduleID},
		},
		map[string]bool{"o": true},
	)
	if !slices.Equal(plan.cancelSupervisors, []string{contract.StreamWorkflowID("now-scheduled"), contract.StreamWorkflowID("gone")}) {
		t.Errorf("supervisors cancelled: %v", plan.cancelSupervisors)
	}
	if !slices.Equal(plan.deleteSchedules, []string{contract.ScheduleID("now-continuous"), contract.ScheduleID("gone")}) {
		t.Errorf("schedules deleted: %v", plan.deleteSchedules)
	}
}

// A namespace may hold another install's workflows and schedules. An empty
// database, or one with other orgs, mustn't stop them.
func TestReconcileLeavesOtherInstallsAlone(t *testing.T) {
	plan := planReconcile(
		nil,
		[]orchestration.RunningWorkflow{{ID: contract.StreamWorkflowID("theirs"), Org: "other"}, {ID: contract.StreamWorkflowID("unknown")}},
		[]orchestration.ScheduleState{{ID: contract.ScheduleID("theirs"), Org: "other"}, {ID: contract.ScheduleID("unknown")}, {ID: "their-own-schedule", Org: "o"}},
		map[string]bool{"o": true},
	)
	if plan.changes() != 0 {
		t.Errorf("plan %+v", plan)
	}
}

// A run left running on a pipeline that's scheduled now needs a supervisor to
// stop its job and record its end. The supervisor ends by itself after.
func TestReconcileStopsARunLeftOnAScheduledPipeline(t *testing.T) {
	plan := planReconcile(
		[]pipelineToRun{{org: "o", id: "p", schedule: hourly, runningRun: true}},
		nil,
		[]orchestration.ScheduleState{{ID: contract.ScheduleID("p"), Org: "o"}},
		map[string]bool{"o": true},
	)
	if got := ids(plan.startSupervisors); !slices.Equal(got, []string{"p"}) {
		t.Errorf("supervisors started: %v", got)
	}
	// One that's already running needs nothing more.
	plan = planReconcile(
		[]pipelineToRun{{org: "o", id: "p", schedule: hourly, runningRun: true}},
		[]orchestration.RunningWorkflow{{ID: contract.StreamWorkflowID("p"), Org: "o"}},
		[]orchestration.ScheduleState{{ID: contract.ScheduleID("p"), Org: "o"}},
		map[string]bool{"o": true},
	)
	if plan.changes() != 0 {
		t.Errorf("plan %+v", plan)
	}
}
