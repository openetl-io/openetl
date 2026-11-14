package activities

import (
	"context"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// planner gives outputs, or an error, as the engine's planning would, and
// keeps what it was asked.
type planner struct {
	outputs  []runner.ExpectedOutput
	err      error
	sql      string
	streams  []string
	restored map[string][]byte
}

func (p *planner) PlanSQL(_ context.Context, sql string, streams []runner.Stream, restored map[string][]byte) ([]runner.ExpectedOutput, error) {
	p.sql, p.restored = sql, restored
	for _, s := range streams {
		p.streams = append(p.streams, s.Name)
	}
	return p.outputs, p.err
}

// plan is p planning with a pipeline that has no checkpoint yet.
func (p *planner) plan() planSQL {
	if p == nil {
		return nil
	}
	return withRestored(p, checkpoint.NewLog(checkpoint.NewMemStore(), "org", "p"))
}

// A resumed pipeline's planning reads the streams' schemas HEAD's record
// has, without claiming the pipeline, or where it can't be read, those the
// record before it has, which the attempt rewinds to.
func TestPlanningReadsTheRecordedSchemas(t *testing.T) {
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
		pl := plannedEmails()
		if _, err := withRestored(pl, checkpoint.NewLog(store, "org", "p"))(t.Context(), emails, nil); err != nil {
			t.Fatal(err)
		}
		if len(pl.restored) != 1 || string(pl.restored["users"]) != "schema" {
			t.Errorf("HEAD %s: restored: %q", head, pl.restored)
		}
	}
	pl := plannedEmails()
	if _, err := pl.plan()(t.Context(), emails, nil); err != nil || pl.restored != nil {
		t.Errorf("without a checkpoint: %v, %q", err, pl.restored)
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

func plannedEmails() *planner {
	return &planner{outputs: []runner.ExpectedOutput{{Name: "emails", PrimaryKey: []string{"uid"}, Input: "users"}}}
}

// A SQL job's outputs are its statements', then the streams no statement
// writes; each is written as a stream read like its input, with its key,
// and only a statement's is transformed.
func TestBuildJobPlansSQL(t *testing.T) {
	pl := plannedEmails()
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
	job, err := buildJob(t.Context(), sqlSpec(t, emails, apiv1.Passthrough_PASSTHROUGH_NONE, nil), connectorhost.Env{}, plannedEmails().plan())
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
		{"planning", &planner{err: errs.New(errs.ConfigInvalid, "transform.sql: 1:8: there's no column nope")}, nil, "1:8"},
		{"merge without a key", &planner{outputs: []runner.ExpectedOutput{{Name: "e", Input: "users"}}},
			&apiv1.PipelineDestination{Write: apiv1.WriteMode_WRITE_MODE_MERGE}, "needs a primary key"},
		{"a table for no output", plannedEmails(), &apiv1.PipelineDestination{Tables: map[string]string{"nope": "t"}}, `names "nope"`},
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
