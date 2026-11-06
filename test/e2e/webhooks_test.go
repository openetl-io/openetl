//go:build e2e

package e2e

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/webhooks"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type hookEvent struct {
	ID   string
	Type string
	Data map[string]any
}

// hookReceiver is a webhook's endpoint. It fails the test on a delivery
// its key didn't sign.
type hookReceiver struct {
	*httptest.Server
	secret string
	mu     sync.Mutex
	got    []hookEvent
}

func newHookReceiver(t *testing.T) *hookReceiver {
	t.Helper()
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	r := &hookReceiver{secret: setSecret(t, "whsec_"+base64.StdEncoding.EncodeToString(key))}
	r.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if err := webhooks.Verify(key, req.Header, body, time.Now(), time.Minute); err != nil {
			t.Errorf("a delivery: %v", err)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		var p struct {
			Type string         `json:"type"`
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(body, &p); err != nil {
			t.Errorf("a delivery's body %s: %v", body, err)
		}
		r.mu.Lock()
		r.got = append(r.got, hookEvent{ID: req.Header.Get("webhook-id"), Type: p.Type, Data: p.Data})
		r.mu.Unlock()
	}))
	t.Cleanup(r.Close)
	return r
}

// wait waits for an event of each type, and returns the first of each.
func (r *hookReceiver) wait(t *testing.T, types ...string) map[string]hookEvent {
	t.Helper()
	out := map[string]hookEvent{}
	eventually(t, 30*time.Second, strings.Join(types, ", "), func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, e := range r.got {
			if _, ok := out[e.Type]; !ok && slices.Contains(types, e.Type) {
				out[e.Type] = e
			}
		}
		return len(out) == len(types)
	})
	return out
}

func (r *hookReceiver) events() []hookEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.got)
}

func webhookDoc(name, url string, events, pipelines []string, secret string) string {
	b, _ := json.Marshal(map[string]any{
		"apiVersion": "openetl.io/v1alpha1", "kind": "Webhook", "metadata": map[string]any{"name": name},
		"spec": map[string]any{"url": url, "events": events, "pipelines": pipelines, "secrets": []string{secret}},
	})
	return string(b)
}

func webhookClient() apiv1connect.WebhookServiceClient {
	return apiv1connect.NewWebhookServiceClient(env.http, env.apiURL)
}

// A pipeline's events reach a webhook subscribed to them, signed with its
// secret, and the API lists the deliveries.
func TestWebhooks(t *testing.T) {
	startRunner(t)
	r := newHookReceiver(t)
	name, other := unique("hooked"), unique("unhooked")
	hook := unique("hook")
	hookName := "workspaces/default/webhooks/" + hook
	apply(t, webhookDoc(hook, r.URL, []string{"sync.*", "pipeline.*", "schema.changed"}, []string{name}, r.secret))

	secretsAPI := apiv1connect.NewSecretServiceClient(env.http, env.apiURL)
	_, err := secretsAPI.DeleteSecret(t.Context(), connect.NewRequest(&apiv1.DeleteSecretRequest{Name: "workspaces/default/secrets/" + r.secret}))
	wantCode(t, err, connect.CodeFailedPrecondition, "deleting a webhook's secret")

	apply(t, pipelineDocs(name, `{"seed": 4, "count": 20}`, `{}`)...)
	apply(t, pipelineDocs(other, `{"seed": 4, "count": 20}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	got := r.wait(t, events.PipelineCreated, events.SyncStarted, events.SyncSucceeded)
	sync := latestSync(t, name)
	pipeline := "workspaces/default/pipelines/" + name
	if d := got[events.SyncSucceeded].Data; d["pipeline"] != pipeline || d["sync"] != sync.GetName() || d["version"] != sync.GetVersion() || d["endTime"] == nil {
		t.Errorf("sync.succeeded's data %v; the sync is %v", d, sync)
	}
	if got[events.SyncStarted].ID == got[events.SyncSucceeded].ID {
		t.Error("two events share an ID")
	}

	// A stream's new schema, as a runner reports it.
	var id string
	var raw []byte
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	err = conn.QueryRow(t.Context(), `SELECT p.id::text, s.arrow_schema FROM stream_schemas s JOIN pipelines p ON p.org_id = s.org_id AND p.id = s.pipeline_id
		WHERE p.name = $1 AND s.stream = 'users'`, name).Scan(&id, &raw)
	_ = conn.Close(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL).ReportSchemas(t.Context(), connect.NewRequest(&dataplanev1.ReportSchemasRequest{
		PipelineId: id, Generation: 9, Epoch: 9, Schemas: map[string][]byte{"users": withColumn(t, raw, "extra")},
	})); err != nil {
		t.Fatal(err)
	}
	changed := r.wait(t, events.SchemaChanged)[events.SchemaChanged].Data
	if changed["stream"] != "users" || changed["version"] != 2.0 || fmt.Sprint(changed["added"]) != "[extra]" {
		t.Errorf("schema.changed's data %v", changed)
	}

	if _, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: pipeline})); err != nil {
		t.Fatal(err)
	}
	r.wait(t, events.PipelinePaused)

	sent, err := webhookClient().TestWebhook(t.Context(), connect.NewRequest(&apiv1.TestWebhookRequest{Name: hookName}))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(sent.Msg.GetDelivery(), hookName+"/deliveries/") {
		t.Errorf("test delivery %q", sent.Msg.GetDelivery())
	}
	r.wait(t, events.WebhookTest)

	var deliveries []*apiv1.WebhookDelivery
	eventually(t, 10*time.Second, "every delivery to succeed", func() bool {
		res, err := webhookClient().ListWebhookDeliveries(t.Context(), connect.NewRequest(&apiv1.ListWebhookDeliveriesRequest{Parent: hookName}))
		if err != nil {
			t.Fatal(err)
		}
		deliveries = res.Msg.GetDeliveries()
		return !slices.ContainsFunc(deliveries, func(d *apiv1.WebhookDelivery) bool {
			return d.GetState() != apiv1.DeliveryState_DELIVERY_STATE_SUCCEEDED
		})
	})
	if len(deliveries) != len(r.events()) || deliveries[0].GetName() != sent.Msg.GetDelivery() || deliveries[0].GetResponseStatus() != 200 || deliveries[0].GetAttempts() != 1 {
		t.Errorf("deliveries %v; the endpoint got %v", deliveries, r.events())
	}
	if status, _ := restCall(t, env.http, "GET", "/v1/"+hookName+"/deliveries", nil); status != 200 {
		t.Errorf("REST deliveries: %d", status)
	}

	if _, err := webhookClient().DisableWebhook(t.Context(), connect.NewRequest(&apiv1.DisableWebhookRequest{Name: hookName})); err != nil {
		t.Fatal(err)
	}
	_, err = webhookClient().TestWebhook(t.Context(), connect.NewRequest(&apiv1.TestWebhookRequest{Name: hookName}))
	wantCode(t, err, connect.CodeFailedPrecondition, "testing a disabled webhook")
	res, err := webhookClient().EnableWebhook(t.Context(), connect.NewRequest(&apiv1.EnableWebhookRequest{Name: hookName}))
	if err != nil || res.Msg.GetWebhook().GetState() != apiv1.WebhookState_WEBHOOK_STATE_ENABLED {
		t.Fatalf("enable: %v (%v)", res, err)
	}

	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipeline})); err != nil {
		t.Fatal(err)
	}
	r.wait(t, events.PipelineDeleted)
	for _, e := range r.events() {
		if p, _ := e.Data["pipeline"].(string); p != "" && p != pipeline {
			t.Errorf("%s of %s, which the webhook doesn't follow", e.Type, p)
		}
	}

	if _, err := webhookClient().DeleteWebhook(t.Context(), connect.NewRequest(&apiv1.DeleteWebhookRequest{Name: hookName})); err != nil {
		t.Fatal(err)
	}
	if _, err := secretsAPI.DeleteSecret(t.Context(), connect.NewRequest(&apiv1.DeleteSecretRequest{Name: "workspaces/default/secrets/" + r.secret})); err != nil {
		t.Errorf("deleting the secret once its webhook is gone: %v", err)
	}
}

// A sync the block policy stops is sync.failed with its code, and
// schema.blocked.
func TestWebhookSchemaBlocked(t *testing.T) {
	startRunner(t)
	r := newHookReceiver(t)
	name := unique("hookblocked")
	apply(t, webhookDoc(unique("hook"), r.URL, []string{"sync.failed", "schema.blocked"}, []string{name}, r.secret))
	sch := pgx.Identifier{schema(name)}.Sanitize()
	pgExec(t,
		"CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i FROM generate_series(1, 10) i",
	)
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q}}`, schema(name))
	ds := docs(name, src, parquetDest(t.TempDir()), `{}`)
	ds[2] = strings.Replace(ds[2], `"delivery"`, `"schemaPolicy": "block", "delivery"`, 1)
	apply(t, ds...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	pgExec(t, "ALTER TABLE "+sch+".items ADD COLUMN price numeric(10, 2)")
	if status, _ := syncNow(t, name); status != "failed" {
		t.Fatalf("sync %s, want failed", status)
	}
	got := r.wait(t, events.SyncFailed, events.SchemaBlocked)
	for _, typ := range []string{events.SyncFailed, events.SchemaBlocked} {
		if d := got[typ].Data; d["code"] != "SCHEMA_DRIFT" || d["error"] == nil || d["pipeline"] != "workspaces/default/pipelines/"+name {
			t.Errorf("%s's data %v", typ, d)
		}
	}
}

// A continuous pipeline whose source reports it's further behind than the
// server's 5 minutes is lagging, once, until it's back within half of it,
// with an incident while it is.
func TestWebhookLagging(t *testing.T) {
	startRunner(t)
	r := newHookReceiver(t)
	name := unique("hooklag")
	apply(t, webhookDoc(unique("hook"), r.URL, []string{"pipeline.lagging", "pipeline.recovered"}, []string{name}, r.secret))
	startContinuous(t, name)
	waitRuns(t, name, "running")
	var id, run, sync string
	eventually(t, 30*time.Second, "the run's first checkpoint", func() bool {
		conn, err := pgx.Connect(t.Context(), env.dbURL)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = conn.Close(t.Context()) }()
		return conn.QueryRow(t.Context(), `SELECT p.id::text, s.run_id, s.id::text FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
			JOIN checkpoint_mirror m ON m.pipeline_id = p.id AND m.run_id = s.run_id
			WHERE p.name = $1 AND s.status = 'running' LIMIT 1`, name).Scan(&id, &run, &sync) == nil
	})
	api := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL)
	report := func(epoch uint64, lag time.Duration) {
		t.Helper()
		_, err := api.ReportCheckpoint(t.Context(), connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: id, RunId: run, Epoch: epoch, Generation: 1, CreateTime: timestamppb.Now(),
			Progress: []*protocolv1.Progress{{Stream: "users", Lag: durationpb.New(lag)}},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	report(1_000_001, 10*time.Minute)
	report(1_000_002, 12*time.Minute)
	report(1_000_003, 4*time.Minute)
	lagging := r.wait(t, events.PipelineLagging)[events.PipelineLagging].Data
	if lagging["lagSeconds"] != 600.0 || lagging["thresholdSeconds"] != 300.0 || lagging["stream"] != "users" || lagging["sync"] != "workspaces/default/pipelines/"+name+"/syncs/"+sync {
		t.Errorf("pipeline.lagging's data %v", lagging)
	}
	if inc := waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_LAGGING, apiv1.IncidentState_INCIDENT_STATE_OPEN); inc.GetDetails()["lag_seconds"] != "600" {
		t.Errorf("lagging incident %v", inc)
	}
	report(1_000_004, time.Minute)
	report(1_000_005, 0)
	r.wait(t, events.PipelineRecovered)
	waitIncident(t, name, apiv1.IncidentClass_INCIDENT_CLASS_LAGGING, apiv1.IncidentState_INCIDENT_STATE_RESOLVED)
	time.Sleep(2 * time.Second)
	var n int
	for _, e := range r.events() {
		if e.Type == events.PipelineLagging || e.Type == events.PipelineRecovered {
			n++
		}
	}
	if n != 2 {
		t.Errorf("%d lagging and recovered events, want one of each", n)
	}
}
