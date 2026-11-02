//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
)

// A sync reports its schemas to the control plane, and the history keeps a
// new version only when a stream's schema changes.
func TestSchemaHistory(t *testing.T) {
	startRunner(t)
	name := unique("schemas")
	apply(t, pipelineDocs(name, `{"seed": 3, "count": 50}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	const history = `SELECT string_agg(s.stream || ':' || s.version, ',' ORDER BY s.stream, s.version)
		FROM stream_schemas s JOIN pipelines p ON p.org_id = s.org_id AND p.id = s.pipeline_id WHERE p.name = $1`
	if got, err := query[string](t, history, name); err != nil || got != "purchases:1,users:1" {
		t.Fatalf("history is %q (%v)", got, err)
	}
	var id string
	var raw []byte
	var hasID bool
	conn, err := pgx.Connect(t.Context(), env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	err = conn.QueryRow(t.Context(), `SELECT p.id::text, s.arrow_schema, s.columns @> '[{"name": "_openetl_id"}]'
		FROM stream_schemas s JOIN pipelines p ON p.org_id = s.org_id AND p.id = s.pipeline_id
		WHERE p.name = $1 AND s.stream = 'users'`, name).Scan(&id, &raw, &hasID)
	if err != nil {
		t.Fatal(err)
	}
	if !hasID {
		t.Error("the users columns don't list _openetl_id")
	}

	api := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL)
	report := func(schema []byte) []string {
		t.Helper()
		res, err := api.ReportSchemas(t.Context(), connect.NewRequest(&dataplanev1.ReportSchemasRequest{
			PipelineId: id, Generation: 9, Epoch: 9, Schemas: map[string][]byte{"users": schema},
		}))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetAdded()
	}
	if added := report(raw); len(added) != 0 {
		t.Errorf("the same schema was added again: %v", added)
	}
	if added := report(withColumn(t, raw, "extra")); len(added) != 1 {
		t.Errorf("a new column added %v", added)
	}
	if got, err := query[string](t, history, name); err != nil || got != "purchases:1,users:1,users:2" {
		t.Errorf("history is %q (%v)", got, err)
	}

	pipelines := apiv1connect.NewPipelineServiceClient(env.http, env.apiURL)
	list := func(stream string) []string {
		t.Helper()
		var got []string
		for token := ""; ; {
			res, err := pipelines.ListStreamSchemas(t.Context(), connect.NewRequest(&apiv1.ListStreamSchemasRequest{
				Parent: "workspaces/default/pipelines/" + name, Stream: stream, PageSize: 1, PageToken: token,
			}))
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range res.Msg.GetStreamSchemas() {
				got = append(got, fmt.Sprintf("%s:%d:%d", s.GetStream(), s.GetVersion(), len(s.GetColumns())))
			}
			if token = res.Msg.GetNextPageToken(); token == "" {
				return got
			}
		}
	}
	all := list("")
	if len(all) != 3 || !strings.HasPrefix(all[0], "purchases:1:") || !strings.HasPrefix(all[1], "users:1:") || !strings.HasPrefix(all[2], "users:2:") {
		t.Errorf("listed %v", all)
	}
	if users := list("users"); !slices.Equal(users, all[1:]) {
		t.Errorf("users' history %v; want %v", users, all[1:])
	}
}

func withColumn(t *testing.T, raw []byte, name string) []byte {
	t.Helper()
	r, err := ipc.NewReader(bytes.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Release()
	s := r.Schema()
	md := s.Metadata()
	fields := append(s.Fields(), arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true})
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(arrow.NewSchema(fields, &md)))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}
