//go:build integration

package api_test

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/api"
	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

// refreshes counts Refresh calls; the rest do nothing.
type refreshes struct {
	api.Scheduler
	n int
}

func (r *refreshes) Refresh(context.Context, string, string) error {
	r.n++
	return nil
}

type holdsEnv struct {
	conn     *pgx.Conn
	owner    apiv1connect.PipelineServiceClient
	viewer   apiv1connect.PipelineServiceClient
	runner   dataplanev1connect.DataPlaneServiceClient
	sched    *refreshes
	pipeline uuid.UUID
	parent   string
}

func newHoldsEnv(t *testing.T) *holdsEnv {
	t.Helper()
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.Bootstrap(ctx, "dp1", nil)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close(context.Background()) })
	e := &holdsEnv{conn: conn, sched: &refreshes{}, pipeline: uuid.New(), parent: "workspaces/default/pipelines/orders"}
	args := pgx.NamedArgs{"org": tenant.Org, "ws": tenant.Workspace, "src": uuid.New(), "dst": uuid.New(), "p": e.pipeline}
	for _, q := range []string{
		`INSERT INTO sources (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @src, 'src', '{"connector": "postgres"}', 's1', 1, 'sha256:s', '{}')`,
		`INSERT INTO destinations (org_id, workspace_id, id, name, spec, version, revision, connector_digest, secrets)
			VALUES (@org, @ws, @dst, 'dst', '{"connector": "postgres"}', 'd1', 1, 'sha256:d', '{}')`,
		`INSERT INTO source_versions (org_id, source_id, version, spec, connector_digest, secrets, provenance)
			VALUES (@org, @src, 's1', '{"connector": "postgres"}', 'sha256:s', '{}', 'test')`,
		`INSERT INTO destination_versions (org_id, destination_id, version, spec, connector_digest, secrets, provenance)
			VALUES (@org, @dst, 'd1', '{"connector": "postgres"}', 'sha256:d', '{}', 'test')`,
		`INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
			VALUES (@org, @ws, @p, 'orders', @src, @dst, 'dp1', 'v1', 1)`,
		`INSERT INTO pipeline_versions (org_id, pipeline_id, version, spec, source_id, destination_id, data_plane_id,
			source_version, destination_version, dbt, provenance)
			VALUES (@org, @p, 'v1', '{"mode": "PIPELINE_MODE_CONTINUOUS"}', @src, @dst, 'dp1', 's1', 'd1', '{}', 'test')`,
		`INSERT INTO syncs (org_id, id, pipeline_id, version, workflow_id, run_id, status, continuous)
			VALUES (@org, gen_random_uuid(), @p, 'v1', 'wf', 'run1', 'running', true)`,
	} {
		if _, err := conn.Exec(ctx, q, args); err != nil {
			t.Fatal(err)
		}
	}
	var token auth.Credential
	err = s.Tx(ctx, func(q *db.Queries) error {
		token, _, err = store.CreateRunnerToken(ctx, q, tenant.Org, "dp1")
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	srv := &api.Server{Store: s, Tenant: tenant, Schedules: e.sched, DefaultDataPlane: "dp1", Authless: true, Logger: slog.New(slog.DiscardHandler)}
	h, err := srv.Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	e.owner = apiv1connect.NewPipelineServiceClient(http.DefaultClient, ts.URL)
	e.runner = dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, ts.URL, auth.WithBearer(token.Secret))

	access := apiv1connect.NewAccessServiceClient(http.DefaultClient, ts.URL)
	p, err := access.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: "watcher", Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT, DisplayName: "watcher"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	name := p.Msg.GetPrincipal().GetName()
	if _, err := access.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: name, Binding: &apiv1.RoleBinding{Role: apiv1.Role_ROLE_VIEWER}})); err != nil {
		t.Fatal(err)
	}
	key, err := access.CreateKey(ctx, connect.NewRequest(&apiv1.CreateKeyRequest{Parent: name}))
	if err != nil {
		t.Fatal(err)
	}
	e.viewer = apiv1connect.NewPipelineServiceClient(http.DefaultClient, ts.URL, auth.WithBearer(key.Msg.GetSecret()))
	return e
}

func (e *holdsEnv) report(t *testing.T, gen, epoch uint64, holds ...*dataplanev1.CheckpointHold) {
	t.Helper()
	_, err := e.runner.ReportCheckpoint(t.Context(), connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: gen, Epoch: epoch, CreateTime: timestamppb.Now(),
		Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: 10, Holds: holds}, {Name: "users", Rows: 1}},
	}))
	if err != nil {
		t.Fatal(err)
	}
}

func (e *holdsEnv) holds(t *testing.T) map[string]*apiv1.Hold {
	t.Helper()
	res, err := e.viewer.ListHolds(t.Context(), connect.NewRequest(&apiv1.ListHoldsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*apiv1.Hold{}
	for _, h := range res.Msg.GetHolds() {
		out[h.GetColumn()] = h
	}
	return out
}

func (e *holdsEnv) releases(t *testing.T) []string {
	t.Helper()
	spec, err := e.runner.GetSyncSpec(t.Context(), connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: e.pipeline.String(), Version: "v1"}))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, r := range spec.Msg.GetReleases() {
		s := r.GetStream() + "." + r.GetColumn()
		if r.GetDiscard() {
			s += " discard"
		}
		out = append(out, s)
	}
	return out
}

func (e *holdsEnv) incident(t *testing.T) (status, severity string) {
	t.Helper()
	err := e.conn.QueryRow(t.Context(), `SELECT status, severity FROM incidents WHERE class = 'held' AND stream = 'orders'
		ORDER BY first_seen_time DESC LIMIT 1`).Scan(&status, &severity)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return status, severity
}

func code(err error) connect.Code { return connect.CodeOf(err) }

func TestHoldsReleasesAndTheirIncident(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	since := func(g, n uint64) *dataplanev1.CheckpointRef {
		return &dataplanev1.CheckpointRef{Generation: g, Epoch: n}
	}
	amount := &dataplanev1.CheckpointHold{Column: "amount", Whole: true, Reason: "a view depends on it", Since: since(1, 2), StartTime: timestamppb.Now()}
	note := &dataplanev1.CheckpointHold{Column: "note", Cells: 7, Reason: "nan", Since: since(1, 3), StartTime: timestamppb.Now()}
	e.report(t, 1, 3, amount, note)
	h := e.holds(t)
	if len(h) != 2 || !h["amount"].GetWhole() || h["note"].GetCells() != 7 || h["amount"].GetSinceEpoch() != 2 || h["amount"].GetPending() {
		t.Fatalf("holds %v", h)
	}
	if st, sev := e.incident(t); st != "open" || sev != "warning" {
		t.Fatalf("incident %s, %s", st, sev)
	}

	// A day on, the same hold makes it critical.
	amount.StartTime = timestamppb.New(time.Now().Add(-25 * time.Hour))
	e.report(t, 1, 4, amount, note)
	if st, sev := e.incident(t); st != "open" || sev != "critical" {
		t.Fatalf("incident %s, %s, a day on", st, sev)
	}

	_, err := e.viewer.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount"}))
	if code(err) != connect.CodePermissionDenied {
		t.Fatalf("a viewer released a hold: %v", err)
	}
	res, err := e.owner.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount"}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Msg.GetHold().GetPending() || e.sched.n != 1 {
		t.Errorf("released %v, %d refreshes", res.Msg.GetHold(), e.sched.n)
	}
	if _, err := e.owner.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount"})); err != nil {
		t.Errorf("releasing again: %v", err)
	}
	_, err = e.owner.DiscardHold(ctx, connect.NewRequest(&apiv1.DiscardHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount"}))
	if code(err) != connect.CodeFailedPrecondition {
		t.Errorf("discarding a hold with a release pending: %v", err)
	}
	_, err = e.owner.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "missing"}))
	if code(err) != connect.CodeNotFound {
		t.Errorf("releasing a column that isn't held: %v", err)
	}
	if !e.holds(t)["amount"].GetPending() {
		t.Error("the release isn't pending")
	}
	if got := e.releases(t); len(got) != 1 || got[0] != "orders.amount" {
		t.Errorf("releases %v", got)
	}
	var details string
	err = e.conn.QueryRow(ctx, `SELECT details::text FROM audit_log WHERE method LIKE '%/ReleaseHold' AND actor NOT LIKE '%watcher%'
		ORDER BY id LIMIT 1`).Scan(&details)
	if err != nil || !strings.Contains(details, `"column": "amount"`) || !strings.Contains(details, `"action": "release"`) {
		t.Errorf("audit details %s, %v", details, err)
	}

	// An older checkpoint changes nothing.
	e.report(t, 1, 2)
	if len(e.holds(t)) != 2 {
		t.Fatal("an older checkpoint replaced the holds")
	}

	// The run takes the release: the column's hold replays, and the release
	// isn't sent again.
	e.report(t, 1, 5, &dataplanev1.CheckpointHold{Column: "amount", Whole: true, Reason: "released", Releasing: true}, note)
	if got := e.releases(t); len(got) != 0 {
		t.Errorf("releases %v, once taken", got)
	}
	if h := e.holds(t); !h["amount"].GetReleasing() || h["amount"].GetPending() {
		t.Errorf("amount %v", h["amount"])
	}
	if _, err := e.owner.DiscardHold(ctx, connect.NewRequest(&apiv1.DiscardHoldRequest{Parent: e.parent, Stream: "orders", Column: "note"})); err != nil {
		t.Fatal(err)
	}
	if got := e.releases(t); len(got) != 1 || got[0] != "orders.note discard" {
		t.Errorf("releases %v", got)
	}

	// Nothing held: the incident resolves.
	e.report(t, 2, 6)
	if len(e.holds(t)) != 0 || len(e.releases(t)) != 0 {
		t.Errorf("holds %v, releases %v", e.holds(t), e.releases(t))
	}
	if st, _ := e.incident(t); st != "resolved" {
		t.Errorf("incident %s", st)
	}
}
