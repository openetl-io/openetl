//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/events"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

func incidents() apiv1connect.IncidentServiceClient {
	return apiv1connect.NewIncidentServiceClient(env.http, env.apiURL)
}

// listIncidents is the pipeline's incidents of the class, newest first.
func listIncidents(t *testing.T, pipeline string, class apiv1.IncidentClass) []*apiv1.Incident {
	t.Helper()
	res, err := incidents().ListIncidents(t.Context(), connect.NewRequest(&apiv1.ListIncidentsRequest{Parent: "workspaces/default/pipelines/" + pipeline}))
	if err != nil {
		t.Fatal(err)
	}
	var out []*apiv1.Incident
	for _, i := range res.Msg.GetIncidents() {
		if i.GetClass() == class {
			out = append(out, i)
		}
	}
	return out
}

// waitIncident waits for the pipeline's latest incident of the class to be
// in the state.
func waitIncident(t *testing.T, pipeline string, class apiv1.IncidentClass, state apiv1.IncidentState) *apiv1.Incident {
	t.Helper()
	var got *apiv1.Incident
	eventually(t, 90*time.Second, fmt.Sprintf("a %s incident %s", class, state), func() bool {
		is := listIncidents(t, pipeline, class)
		if len(is) == 0 {
			return false
		}
		got = is[0]
		return got.GetState() == state
	})
	return got
}

// A schema change the block policy stops opens a schema_blocked incident for
// the stream, and its playbook pauses the pipeline; heal mode suggest only
// proposes it. Taking the change and resuming resolves it once a sync
// succeeds.
func TestIncidentSchemaBlocked(t *testing.T) {
	startRunner(t)
	for _, mode := range []string{"auto_safe", "suggest"} {
		t.Run(mode, func(t *testing.T) {
			r := newHookReceiver(t)
			name := unique("incblocked")
			apply(t, webhookDoc(unique("hook"), r.URL, []string{"incident.*", "pipeline.paused"}, []string{name}, r.secret))
			sch := pgx.Identifier{schema(name)}.Sanitize()
			pgExec(t,
				"CREATE SCHEMA "+sch,
				"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text)",
				"INSERT INTO "+sch+".items SELECT i, 'item ' || i FROM generate_series(1, 10) i",
			)
			src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, schema(name))
			ds := docs(name, src, parquetDest(t.TempDir()), `{}`)
			blocked := strings.Replace(ds[2], `"delivery"`, `"schemaPolicy": "block", "heal": {"mode": "`+mode+`"}, "delivery"`, 1)
			apply(t, ds[0], ds[1], blocked)
			if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			pgExec(t, "ALTER TABLE "+sch+".items ADD COLUMN price numeric(10, 2)")
			if status, _ := syncNow(t, name); status != "failed" {
				t.Fatalf("sync %s, want failed", status)
			}

			inc := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_SCHEMA_BLOCKED, apiv1.IncidentState_INCIDENT_STATE_OPEN)
			if inc.GetCode() != "SCHEMA_DRIFT" || !strings.Contains(inc.GetStream(), "items") || inc.GetSeverity() != apiv1.IncidentSeverity_INCIDENT_SEVERITY_CRITICAL ||
				!strings.Contains(inc.GetNextSteps(), "schemaPolicy") || !strings.Contains(inc.GetSync(), "/syncs/") || inc.GetOccurrences() != 1 {
				t.Errorf("incident %v", inc)
			}
			if strings.Contains(inc.GetMessage(), "activity error") {
				t.Errorf("the incident's message %q has Temporal's wrapping", inc.GetMessage())
			}
			p, err := pipelines().GetPipeline(t.Context(), connect.NewRequest(&apiv1.GetPipelineRequest{Name: "workspaces/default/pipelines/" + name}))
			if err != nil {
				t.Fatal(err)
			}
			if p.Msg.GetPipeline().GetStatus().GetOpenIncidents() != 1 {
				t.Errorf("the pipeline's status %v", p.Msg.GetPipeline().GetStatus())
			}
			wantPaused := mode == "auto_safe"
			if len(inc.GetActions()) != 1 || inc.GetActions()[0].GetKind() != apiv1.HealActionKind_HEAL_ACTION_KIND_PAUSE ||
				(inc.GetActions()[0].GetState() == apiv1.HealActionState_HEAL_ACTION_STATE_APPLIED) != wantPaused {
				t.Errorf("actions %v", inc.GetActions())
			}
			if got := p.Msg.GetPipeline().GetPaused(); got != wantPaused {
				t.Errorf("paused %v, want %v", got, wantPaused)
			}
			if got := schedulePaused(t, name); got != wantPaused {
				t.Errorf("schedule paused %v, want %v", got, wantPaused)
			}
			want := []string{events.IncidentOpened}
			if wantPaused {
				want = append(want, events.PipelinePaused)
			}
			got := r.wait(t, want...)
			if d := got[events.IncidentOpened].Data; d["incident"] != inc.GetName() || d["class"] != "schema_blocked" || d["code"] != "SCHEMA_DRIFT" {
				t.Errorf("incident.opened's data %v", d)
			}
			if wantPaused && got[events.PipelinePaused].Data["incident"] != inc.GetName() {
				t.Errorf("pipeline.paused's data %v", got[events.PipelinePaused].Data)
			}

			apply(t, ds...)
			if wantPaused {
				if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
					t.Fatal(err)
				}
			}
			if status, stats := syncNow(t, name); status != "succeeded" {
				t.Fatalf("sync %s: %s", status, stats)
			}
			done := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_SCHEMA_BLOCKED, apiv1.IncidentState_INCIDENT_STATE_RESOLVED)
			if done.GetName() != inc.GetName() || !strings.Contains(done.GetResolution(), "succeeded") || done.GetResolveTime() == nil {
				t.Errorf("resolved %v", done)
			}
			r.wait(t, events.IncidentResolved)
		})
	}
}

// A continuous run whose destination refuses every connection fails its
// attempts in a row: the API counts them on the run, and three without a
// commit are a crash loop. Once the destination is fixed, the run's next
// commit resolves it.
func TestIncidentCrashLoop(t *testing.T) {
	startRunner(t)
	name := unique("crashloop")
	dsn := setSecret(t, "postgres://nobody:nothing@127.0.0.1:1/none?connect_timeout=2")
	ds := continuousDocs(name, endlessFaker)
	ds[1] = strings.Replace(ds[1], `"e2e-dsn"`, `"`+dsn+`"`, 1)
	apply(t, ds...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	})
	inc := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_CRASH_LOOP, apiv1.IncidentState_INCIDENT_STATE_OPEN)
	if inc.GetMessage() == "" || inc.GetDetails()["failures"] == "" || len(inc.GetActions()) != 0 {
		t.Errorf("incident %v", inc)
	}
	res, err := apiv1connect.NewSyncServiceClient(env.http, env.apiURL).ListSyncs(t.Context(), connect.NewRequest(&apiv1.ListSyncsRequest{Parent: "workspaces/default/pipelines/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	if run := res.Msg.GetSyncs()[0]; run.GetState() != apiv1.SyncState_SYNC_STATE_RUNNING || run.GetFailedAttempts() < 3 ||
		!strings.Contains(run.GetLastAttemptError(), "connect") || run.GetLastAttemptTime() == nil {
		t.Errorf("the run %v", run)
	}

	_, err = apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(t.Context(), connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/" + dsn, Value: env.dbURL},
	}))
	if err != nil {
		t.Fatal(err)
	}
	done := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_CRASH_LOOP, apiv1.IncidentState_INCIDENT_STATE_RESOLVED)
	if !strings.Contains(done.GetResolution(), "committed epoch") {
		t.Errorf("resolved %v", done)
	}
}

// What a running read reports opens and resolves retention_at_risk and
// quarantine incidents: retention at half of the source's horizon, critical
// at 80%, resolved under 40%; a stream quarantining over 1% of its rows,
// resolved under half that.
func TestIncidentsFromReports(t *testing.T) {
	startRunner(t)
	r := newHookReceiver(t)
	name := unique("increports")
	apply(t, webhookDoc(unique("hook"), r.URL, []string{"incident.*"}, []string{name}, r.secret))
	startContinuous(t, name)
	waitRuns(t, name, "running")
	var id, run string
	eventually(t, 30*time.Second, "the run's first checkpoint", func() bool {
		v, err := query[string](t, `SELECT p.id::text || ' ' || s.run_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
			JOIN checkpoint_mirror m ON m.pipeline_id = p.id AND m.run_id = s.run_id
			WHERE p.name = $1 AND s.status = 'running' LIMIT 1`, name)
		if err != nil {
			return false
		}
		id, run, _ = strings.Cut(v, " ")
		return true
	})
	api := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL)
	epoch := uint64(1_000_000)
	report := func(progress []*protocolv1.Progress, streams ...*dataplanev1.CheckpointStream) {
		t.Helper()
		epoch++
		_, err := api.ReportCheckpoint(t.Context(), connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: id, RunId: run, Epoch: epoch, Generation: 1, CreateTime: timestamppb.Now(), Progress: progress, Streams: streams,
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	retention := func(lag, left uint64) []*protocolv1.Progress {
		return []*protocolv1.Progress{{LagBytes: &lag, RetentionLeftBytes: &left, Lag: durationpb.New(time.Second)}}
	}

	report(retention(55, 45))
	risk := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_RETENTION_AT_RISK, apiv1.IncidentState_INCIDENT_STATE_OPEN)
	if risk.GetSeverity() != apiv1.IncidentSeverity_INCIDENT_SEVERITY_WARNING || risk.GetDetails()["used"] != "55%" {
		t.Errorf("incident %v", risk)
	}
	report(retention(85, 15))
	r.wait(t, events.IncidentEscalated)
	if got := listIncidents(t, name, apiv1.IncidentClass_INCIDENT_CLASS_RETENTION_AT_RISK)[0]; got.GetSeverity() != apiv1.IncidentSeverity_INCIDENT_SEVERITY_CRITICAL {
		t.Errorf("at 85%%: %v", got)
	}
	report(retention(30, 70))
	waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_RETENTION_AT_RISK, apiv1.IncidentState_INCIDENT_STATE_RESOLVED)

	report(nil, &dataplanev1.CheckpointStream{Name: "e2e_fake", Rows: 10_000, QuarantinedRows: 500})
	bad := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_QUARANTINE, apiv1.IncidentState_INCIDENT_STATE_OPEN)
	if bad.GetStream() != "e2e_fake" || bad.GetDetails()["share"] != "5.0%" || bad.GetSeverity() != apiv1.IncidentSeverity_INCIDENT_SEVERITY_WARNING {
		t.Errorf("incident %v", bad)
	}
	report(nil, &dataplanev1.CheckpointStream{Name: "e2e_fake", Rows: 200_000})
	waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_QUARANTINE, apiv1.IncidentState_INCIDENT_STATE_RESOLVED)
}
