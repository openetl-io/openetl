//go:build integration

package api_test

import (
	"context"
	"errors"
	"fmt"
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

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
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

func (r *refreshes) Delete(context.Context, string) error { return nil }

type holdsEnv struct {
	conn     *pgx.Conn
	owner    apiv1connect.PipelineServiceClient
	viewer   apiv1connect.PipelineServiceClient
	runner   dataplanev1connect.DataPlaneServiceClient
	sched    *refreshes
	pipeline uuid.UUID
	store    *store.Store
	parent   string
	url      string
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
	e := &holdsEnv{conn: conn, sched: &refreshes{}, pipeline: uuid.New(), parent: "workspaces/default/pipelines/orders", store: s}
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
	e.url = ts.URL
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

// Syncs and checkpoints count each stream's held cells, and name the columns
// it held at their latest commit, discarded ones left out.
func TestHeldCellsBySyncAndCheckpoint(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	report := func(epoch, cells uint64, holds ...*dataplanev1.CheckpointHold) {
		t.Helper()
		_, err := e.runner.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
			PipelineId: e.pipeline.String(), RunId: "run1", Generation: 1, Epoch: epoch, CreateTime: timestamppb.Now(),
			Streams: []*dataplanev1.CheckpointStream{{Name: "orders", Rows: 10, HeldCells: cells, Holds: holds}, {Name: "users", Rows: 1}},
		}))
		if err != nil {
			t.Fatal(err)
		}
	}
	sc := apiv1connect.NewSyncServiceClient(http.DefaultClient, e.url)
	since := &dataplanev1.CheckpointRef{Generation: 1, Epoch: 1}
	report(1, 3, &dataplanev1.CheckpointHold{Column: "note", Cells: 3, Reason: "nan", Since: since})
	report(2, 4,
		&dataplanev1.CheckpointHold{Column: "note", Cells: 7, Reason: "nan", Since: since},
		&dataplanev1.CheckpointHold{Column: "note", Whole: true, Reason: "a view depends on it", Since: since},
		&dataplanev1.CheckpointHold{Column: "amount", Whole: true, Reason: "a view depends on it", Since: since},
		&dataplanev1.CheckpointHold{Column: "old", Whole: true, Reason: "released", Since: since, Discarded: true})

	syncs, err := sc.ListSyncs(ctx, connect.NewRequest(&apiv1.ListSyncsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(syncs.Msg.GetSyncs()); n != 1 {
		t.Fatalf("%d syncs", n)
	}
	got := map[string]string{}
	for _, st := range syncs.Msg.GetSyncs()[0].GetStreams() {
		got[st.GetName()] = fmt.Sprint(st.GetHeldCells(), st.GetHeldColumns())
	}
	if got["orders"] != "7 [amount note]" || got["users"] != "0 []" {
		t.Errorf("the sync's streams hold %v", got)
	}

	cps, err := e.viewer.ListCheckpoints(ctx, connect.NewRequest(&apiv1.ListCheckpointsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	var held []string
	for _, c := range cps.Msg.GetCheckpoints() {
		for _, st := range c.GetStreams() {
			if st.GetName() == "orders" {
				held = append(held, fmt.Sprint(c.GetEpoch(), " ", st.GetHeldCells(), st.GetHeldColumns()))
			}
		}
	}
	if fmt.Sprint(held) != "[2 4 [amount note] 1 3 [note]]" {
		t.Errorf("checkpoints hold %v", held)
	}

	// Patched in: the sync holds nothing at its latest commit.
	report(3, 0)
	syncs, err = sc.ListSyncs(ctx, connect.NewRequest(&apiv1.ListSyncsRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	if st := syncs.Msg.GetSyncs()[0].GetStreams()[0]; st.GetHeldCells() != 7 || len(st.GetHeldColumns()) != 0 {
		t.Errorf("orders holds %d cells and %v, once patched in", st.GetHeldCells(), st.GetHeldColumns())
	}
}

// A release the destination refused shows on its hold, with what to do, and
// isn't sent again until the hold is released again.
func TestARefusedReleaseIsntSentAgain(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	cause := &apiv1.HoldCause{
		Kind: apiv1.HoldCause_KIND_REFUSED, SourceColumn: "Amount", From: "int32", To: "int64", FromType: "integer", ToType: "bigint",
		Refusal: "it doesn't change a column's type", Fix: `ALTER TABLE "orders" ALTER COLUMN "amount" TYPE bigint`,
	}
	amount := &dataplanev1.CheckpointHold{
		Column: "amount", Whole: true, Reason: "refused", Since: &dataplanev1.CheckpointRef{Generation: 1, Epoch: 2}, StartTime: timestamppb.Now(), Cause: cause,
	}
	e.report(t, 1, 2, amount)
	h := e.holds(t)["amount"]
	if want := "amount changed from int32 to int64 (integer to bigint in the destination), and the destination refused it: " +
		"it doesn't change a column's type."; h.GetReason() != want {
		t.Errorf("reason %q, want %q", h.GetReason(), want)
	}
	var ways []string
	for _, w := range h.GetWaysOut() {
		ways = append(ways, w.GetAction().String()+" "+w.GetStatement())
	}
	if fmt.Sprint(ways) != `[ACTION_RELEASE ALTER TABLE "orders" ALTER COLUMN "amount" TYPE bigint ACTION_RELEASE_NEW_VERSION  ACTION_DISCARD ]` {
		t.Errorf("ways out %v", ways)
	}
	var next string
	if err := e.conn.QueryRow(ctx, `SELECT next_steps FROM incidents WHERE class = 'held'`).Scan(&next); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(next, cause.GetFix()+". Then openetl holds release orders orders amount.") ||
		!strings.Contains(next, "Or openetl holds release --new-version orders orders amount: sends its held values") {
		t.Errorf("next steps %q", next)
	}

	released := func() []*dataplanev1.HoldRelease {
		t.Helper()
		spec, err := e.runner.GetSyncSpec(ctx, connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: e.pipeline.String(), Version: "v1"}))
		if err != nil {
			t.Fatal(err)
		}
		return spec.Msg.GetReleases()
	}
	if _, err := e.owner.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount"})); err != nil {
		t.Fatal(err)
	}
	rs := released()
	if len(rs) != 1 || rs[0].GetId() == "" {
		t.Fatalf("releases %v", rs)
	}

	// The run refuses it again.
	amount.RefusedRelease, amount.Refused = rs[0].GetId(), &apiv1.HoldRefusal{Reason: "it doesn't change a column's type"}
	e.report(t, 1, 3, amount)
	if h := e.holds(t)["amount"]; h.GetPending() || h.GetRefused().GetReason() != "it doesn't change a column's type" || len(h.GetWaysOut()) != 3 {
		t.Errorf("refused hold %v", h)
	}
	if rs := released(); len(rs) != 0 {
		t.Errorf("a refused release is sent again: %v", rs)
	}

	// Released again, it's pending again, as a new release.
	res, err := e.owner.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: e.parent, Stream: "orders", Column: "amount", NewVersion: true}))
	if err != nil {
		t.Fatal(err)
	}
	if h := res.Msg.GetHold(); !h.GetPending() || h.GetRefused() != nil {
		t.Errorf("released again %v", h)
	}
	again := released()
	if len(again) != 1 || again[0].GetId() == rs[0].GetId() || !again[0].GetNewVersion() {
		t.Errorf("releases %v, after %v", again, rs)
	}
}

// A deleted pipeline's teardowns are recorded with the deletion, and hold what
// a runner reads, so deleting the source afterwards doesn't lose them.
func TestDeletingAPipelineRecordsItsTeardowns(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	pc := apiv1connect.NewPipelineServiceClient(http.DefaultClient, e.url)
	if _, err := pc.DeletePipeline(ctx, connect.NewRequest(&apiv1.DeletePipelineRequest{Name: e.parent})); err != nil {
		t.Fatal(err)
	}
	rows, err := e.conn.Query(ctx, `SELECT id, op, source_id, destination_id, spec::text FROM teardowns WHERE pipeline = 'orders'`)
	if err != nil {
		t.Fatal(err)
	}
	type row struct {
		id       uuid.UUID
		op, spec string
		src, dst *uuid.UUID
	}
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.op, &r.src, &r.dst, &r.spec); err != nil {
			t.Fatal(err)
		}
		got = append(got, r)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("%d teardowns, want one for the source and one for the destination", len(got))
	}
	var src row
	for _, r := range got {
		if r.op != "teardown" || !strings.Contains(r.spec, "postgres") {
			t.Errorf("teardown %+v", r)
		}
		if r.src != nil {
			src = r
		}
	}
	if src.src == nil {
		t.Fatalf("no teardown of the source: %+v", got)
	}

	if _, err := e.conn.Exec(ctx, `DELETE FROM sources WHERE id = $1`, *src.src); err != nil {
		t.Fatal(err)
	}
	dpc := e.runner
	req := &dataplanev1.GetConnectorConfigRequest{OperationId: src.id.String(), Connector: &dataplanev1.GetConnectorConfigRequest_SourceId{SourceId: src.src.String()}}
	res, err := dpc.GetConnectorConfig(ctx, connect.NewRequest(req))
	if err != nil {
		t.Fatalf("reading a deleted source's config for its teardown: %v", err)
	}
	if res.Msg.GetConfig().GetConnector() != "postgres" {
		t.Errorf("config %v", res.Msg.GetConfig())
	}
	// It names another source, so it's not this teardown's.
	req = &dataplanev1.GetConnectorConfigRequest{OperationId: src.id.String(), Connector: &dataplanev1.GetConnectorConfigRequest_SourceId{SourceId: uuid.NewString()}}
	if _, err := dpc.GetConnectorConfig(ctx, connect.NewRequest(req)); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("another source's config: %v, want permission denied", err)
	}
}
