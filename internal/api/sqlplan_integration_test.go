//go:build integration

package api_test

import (
	"context"
	"net/http"
	"slices"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/internal/store/db"
)

// answers as a runner would: a statement per stream sent, writing the stream
// it reads.
type echoPlanner struct{ sent []contract.PlanSQLInput }

func (p *echoPlanner) PlanSQL(_ context.Context, in contract.PlanSQLInput) (sqlplan.Answer, error) {
	p.sent = append(p.sent, in)
	var a sqlplan.Answer
	for _, s := range in.Request.Streams {
		a.Statements = append(a.Statements, sqlplan.Statement{At: sqlplan.Position{Line: 1, Column: 1}, Output: s.Name, Replaces: true, Class: "row_wise", Tier: 1,
			Inputs: []string{s.Name}, Key: s.Key, Columns: []sqlplan.Field{{Name: "id", Type: "int64", Nullable: true}}, ArrowSchema: []byte{1, 2, 3}})
	}
	return a, nil
}

// A new version keeps what planning its transform.sql gave, and the API
// gives it back with the version.
func TestApplyKeepsSQLPlan(t *testing.T) {
	ctx := t.Context()
	planner := &echoPlanner{}
	e := newApplyEnvPlanning(t, connectorhost.Env{}, planner)
	if _, err := e.applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{fakerSource("shop", 1)}})); err != nil {
		t.Fatal(err)
	}
	q := db.New(e.conn)
	src, err := q.GetSource(ctx, db.GetSourceParams{OrgID: e.tenant.Org, WorkspaceID: e.tenant.Workspace, Name: "shop"})
	if err != nil {
		t.Fatal(err)
	}
	cat, err := protojson.Marshal(&apiv1.Catalog{Streams: []*apiv1.CatalogStream{
		{Name: "users", PrimaryKey: []string{"id"}, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}, Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}}},
		{Name: "purchases", PrimaryKey: []string{"id"}, SyncModes: []apiv1.SyncMode{apiv1.SyncMode_SYNC_MODE_FULL_REFRESH}, Columns: []*apiv1.CatalogColumn{{Name: "id", Type: "int64"}}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := q.UpsertSourceCatalog(ctx, db.UpsertSourceCatalogParams{OrgID: e.tenant.Org, SourceID: src.ID, Digest: "d1", SourceVersion: src.Version, Catalog: cat}); err != nil {
		t.Fatal(err)
	}
	docs := []*apiv1.Document{{Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata: { name: files }
spec: { connector: parquet, config: { url: "file:///tmp/openetl-sql-plan" } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Pipeline
metadata: { name: shop }
spec:
  source: { ref: shop }
  destination: { ref: files }
  transform:
    sql: INSERT INTO users SELECT id FROM users
`)}}
	dry, err := e.applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: docs, ValidateOnly: true}))
	if err != nil {
		t.Fatal(err)
	}
	if len(planner.sent) != 1 || planner.sent[0].DataPlane != "dp1" || len(planner.sent[0].Request.Streams) != 1 || planner.sent[0].Request.Streams[0].Name != "users" {
		t.Fatalf("sent %+v", planner.sent)
	}
	plan := dry.Msg.GetPlans()[0]
	if tp := plan.GetTransform(); len(tp.GetStatements()) != 1 || !slices.Equal(tp.GetPassedThrough(), []string{"purchases"}) || !slices.Equal(tp.GetWithheld(), []string{"users"}) {
		t.Errorf("plan %v", plan)
	}

	res, err := e.applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: docs}))
	if err != nil {
		t.Fatal(err)
	}
	var name, version string
	for _, r := range res.Msg.GetResources() {
		if r.GetKind() == "Pipeline" {
			name, version = r.GetName(), r.GetVersion()
		}
	}
	pc := apiv1connect.NewPipelineServiceClient(http.DefaultClient, e.api)
	v, err := pc.GetPipelineVersion(ctx, connect.NewRequest(&apiv1.GetPipelineVersionRequest{Name: name + "/versions/" + version}))
	if err != nil {
		t.Fatal(err)
	}
	outs := v.Msg.GetPipelineVersion().GetSqlPlan().GetOutputs()
	if len(outs) != 1 || outs[0].GetName() != "users" || !slices.Equal(outs[0].GetPrimaryKey(), []string{"id"}) || string(outs[0].GetArrowSchema()) != "\x01\x02\x03" {
		t.Errorf("kept %v", v.Msg.GetPipelineVersion().GetSqlPlan())
	}
	var raw []byte
	if err := e.conn.QueryRow(ctx, "SELECT sql_plan FROM pipeline_versions WHERE version = $1", version).Scan(&raw); err != nil || raw == nil {
		t.Errorf("stored %s: %v", raw, err)
	}
}
