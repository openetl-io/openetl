package activities

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// planner answers as the engine's planning would, and keeps what it was
// asked.
type planner struct {
	answer  sqlplan.Answer
	err     error
	sql     string
	streams []string
	req     sqlplan.Request
}

// engine is p as engineclient.Plan.
func (p *planner) engine(_ context.Context, req sqlplan.Request) (sqlplan.Answer, error) {
	p.req = req
	return p.answer, p.err
}

// plan is p planning a job's streams as they are.
func (p *planner) plan() planSQL {
	if p == nil {
		return nil
	}
	return func(_ context.Context, sql string, streams []runner.Stream) (sqlplan.Answer, error) {
		p.sql = sql
		for _, s := range streams {
			p.streams = append(p.streams, s.Name)
		}
		return p.answer, p.err
	}
}

// A resumed pipeline's planning reads the streams' schemas HEAD's record
// has, without claiming the pipeline, or where it can't be read, those the
// record before it has, which the attempt rewinds to.
func TestPlanningReadsTheRecordedSchemas(t *testing.T) {
	users := []runner.Stream{{Name: "users", PrimaryKey: []string{"id"}}, {Name: "purchases"}}
	for _, head := range []string{
		`{"format":1,"generation":1,"owner":"old","record":{"epoch":2,"generation":1},"first":true}`,
		`{"format":1,"generation":1,"owner":"old","record":{"epoch":3,"generation":1},"previous":{"epoch":2,"generation":1}}`,
	} {
		store := checkpoint.NewMemStore()
		put := func(key, data string) {
			if _, err := store.Create(t.Context(), "org/p/_checkpoints/"+key, []byte(data)); err != nil {
				t.Fatal(err)
			}
		}
		put("HEAD", head)
		put("00000000000000000002-g00000000000000000001.json",
			`{"epoch":2,"generation":1,"streams":{"users":{"schema":"c2NoZW1h","rows":1},"purchases":{"rows":1}},"key_groups":256,"committables":[null],"created_at":"2026-11-01T00:00:00Z"}`)
		pl := plannedEmails(t)
		a, err := withRestored(pl.engine, checkpoint.NewLog(store, "org", "p"))(t.Context(), emails, users)
		if err != nil || len(a.Errors) > 0 {
			t.Fatal(err, a.Errors)
		}
		if r := pl.req.Streams; len(r) != 1 || r[0].Name != "users" || string(r[0].RestoredSchema) != "schema" || !slices.Equal(r[0].Key, []string{"id"}) {
			t.Errorf("HEAD %s: request: %+v", head, r)
		}
	}
	pl := plannedEmails(t)
	if _, err := withRestored(pl.engine, checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"))(t.Context(), emails, nil); err != nil || len(pl.req.Streams) != 0 {
		t.Errorf("without a checkpoint: %v, %+v", err, pl.req)
	}
}

// A statement can't read a stream whose columns neither the source declares
// nor a sync recorded, as when the job starts; the stream is still in the
// request, so a statement can write its name.
func TestAStreamWithoutKnownColumnsCantBeRead(t *testing.T) {
	pl := plannedEmails(t)
	log := checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p")
	a, err := withRestored(pl.engine, log)(t.Context(), emails, []runner.Stream{{Name: "users", PrimaryKey: []string{"id"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(pl.req.Streams) != 1 || pl.req.Streams[0].Key != nil {
		t.Errorf("request: %+v", pl.req)
	}
	if len(a.Statements) != 0 || len(a.Errors) != 1 || !strings.Contains(a.Errors[0].Error(), "transform.sql:1:1: stream users has no known columns") {
		t.Errorf("answer: %+v", a)
	}
	pl.answer = sqlplan.Answer{Errors: []sqlplan.Error{{Kind: "name", Message: "there's no stream users"}}}
	a, err = withRestored(pl.engine, log)(t.Context(), emails, []runner.Stream{{Name: "users"}})
	if err != nil || len(a.Errors) != 2 || !strings.Contains(a.Errors[1].Message, "stream users has no known columns") {
		t.Errorf("SQL that didn't plan: %+v, %v", a, err)
	}
}

// sqlSpec reads faker's users and purchases into stdout through sql.
func sqlSpec(t *testing.T, sql string, passthrough apiv1.Passthrough, dst *apiv1.PipelineDestination) *dataplanev1.GetSyncSpecResponse {
	t.Helper()
	cfg, err := structpb.NewStruct(map[string]any{"count": 1})
	if err != nil {
		t.Fatal(err)
	}
	return &dataplanev1.GetSyncSpecResponse{
		PipelineId:  "p",
		Source:      &apiv1.ConnectorConfig{Connector: "faker", Config: cfg},
		Destination: &apiv1.ConnectorConfig{Connector: "stdout"},
		Pipeline: &apiv1.PipelineSpec{
			Destination: dst,
			Transform:   &apiv1.Transform{Engine: &apiv1.Transform_Sql{Sql: &apiv1.SqlTransform{Sql: sql, Passthrough: passthrough}}},
		},
	}
}

const emails = "INSERT INTO emails SELECT id AS uid, email FROM users"

// planned is an answer with one statement, reading users.
func planned(t *testing.T, output string, key ...string) sqlplan.Answer {
	t.Helper()
	schema, err := arrowipc.EncodeSchema(arrow.NewSchema([]arrow.Field{{Name: "uid", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil))
	if err != nil {
		t.Fatal(err)
	}
	return sqlplan.Answer{Statements: []sqlplan.Statement{{At: sqlplan.Position{Line: 1, Column: 1}, Output: output, Class: "row_wise", Tier: 1,
		Inputs: []string{"users"}, ArrowSchema: schema, Key: key}}}
}

func plannedEmails(t *testing.T) *planner {
	return &planner{answer: planned(t, "emails", "uid")}
}

// A SQL job's outputs are its statements', then the streams no statement
// writes; each is written as a stream read like its input, with its key,
// and only a statement's is transformed.
func TestBuildJobPlansSQL(t *testing.T) {
	pl := plannedEmails(t)
	job, err := buildJob(t.Context(), sqlSpec(t, emails, apiv1.Passthrough_PASSTHROUGH_UNSPECIFIED,
		&apiv1.PipelineDestination{Tables: map[string]string{"emails": "user_emails"}}), connectorhost.Env{}, pl.plan())
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if err != nil {
		t.Fatal(err)
	}
	if pl.sql != emails || !slices.Equal(pl.streams, []string{"users", "purchases"}) {
		t.Errorf("planned %q against %v", pl.sql, pl.streams)
	}
	if job.SQL == nil || job.SQL.SQL != emails || len(job.SQL.Expected) != 1 || job.Transform != nil {
		t.Fatalf("the job's SQL: %+v", job.SQL)
	}
	var names []string
	for _, o := range job.Outputs {
		names = append(names, o.Name)
		if o.Transformed != (o.Name == "emails") || o.Mode == 0 || o.From == "" {
			t.Errorf("output %s: %+v", o.Name, o)
		}
	}
	if !slices.Equal(names, []string{"emails", "users", "purchases"}) {
		t.Errorf("outputs %v", names)
	}
	if o := job.Outputs[0]; !slices.Equal(o.PrimaryKey, []string{"uid"}) || o.Table != "user_emails" {
		t.Errorf("emails: %+v", o)
	}
	if o := job.Outputs[1]; !slices.Equal(o.PrimaryKey, []string{"id"}) {
		t.Errorf("users: %+v", o)
	}
	if len(job.Streams) != 2 || job.Streams[0].Mode != destination.Append || job.Streams[0].Transformed {
		t.Errorf("streams: %+v", job.Streams)
	}
}

func TestBuildJobPassesNothingThroughWithPassthroughNone(t *testing.T) {
	job, err := buildJob(t.Context(), sqlSpec(t, emails, apiv1.Passthrough_PASSTHROUGH_NONE, nil), connectorhost.Env{}, plannedEmails(t).plan())
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if err != nil {
		t.Fatal(err)
	}
	if len(job.Outputs) != 1 || job.Outputs[0].Name != "emails" {
		t.Errorf("outputs: %+v", job.Outputs)
	}
}

func TestBuildJobRefusesSQLThatDoesntPlan(t *testing.T) {
	cases := []struct {
		name string
		pl   *planner
		dst  *apiv1.PipelineDestination
		want string
	}{
		{"planning", &planner{answer: sqlplan.Answer{Errors: []sqlplan.Error{{Kind: "bind", At: &sqlplan.Position{Line: 1, Column: 8}, Message: "there's no column nope"}}}},
			nil, "transform.sql:1:8: there's no column nope"},
		{"the engine", &planner{err: errs.New(errs.ConfigInvalid, "stream users column a: struct isn't a type the engine knows")}, nil, "struct"},
		{"merge without a key", &planner{answer: planned(t, "e")},
			&apiv1.PipelineDestination{Write: apiv1.WriteMode_WRITE_MODE_MERGE}, "needs a primary key"},
		{"a table for no output", plannedEmails(t), &apiv1.PipelineDestination{Tables: map[string]string{"nope": "t"}}, `names "nope"`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job, err := buildJob(t.Context(), sqlSpec(t, emails, apiv1.Passthrough_PASSTHROUGH_ALL, c.dst), connectorhost.Env{}, c.pl.plan())
			defer connectorhost.Close(job.Source)
			defer connectorhost.Close(job.Destination)
			if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want %q", err, c.want)
			}
		})
	}
	job, err := buildJob(t.Context(), sqlSpec(t, emails, 0, nil), connectorhost.Env{}, nil)
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)
	if errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("without a planner: %v", err)
	}
}
