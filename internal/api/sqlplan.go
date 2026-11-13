package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/streamschema"
)

// SQLPlanner is implemented by ops.Ops.
type SQLPlanner interface {
	PlanSQL(ctx context.Context, in contract.PlanSQLInput) (sqlplan.Answer, error)
}

// planSQL plans the pipeline's transform.sql on a runner of its data plane,
// against the source's catalog and the pipeline's schema history, and adds
// what that gave to the plan: the transform's plan, its errors as problems,
// and its notes. It returns nil where the statements have errors, or
// couldn't be planned, which a note says.
func (s *Server) planSQL(ctx context.Context, q *db.Queries, p *planned, cat *apiv1.Catalog, out *apiv1.PipelinePlan) (*sqlplan.Answer, error) {
	if p.v.spec.GetTransform().GetSql() == nil {
		return nil, nil
	}
	if cat == nil {
		out.Notes = append(out.Notes, fmt.Sprintf("transform.sql wasn't planned: source %q hasn't been discovered, so its streams' schemas aren't known", p.v.spec.GetSource().GetRef()))
		return nil, nil
	}
	history, err := s.inputSchemas(ctx, q, p.id)
	if err != nil {
		return nil, err
	}
	return s.planSQLWith(ctx, p, cat, history, out)
}

// planSQLWith is planSQL given the streams' schemas from the pipeline's
// schema history.
func (s *Server) planSQLWith(ctx context.Context, p *planned, cat *apiv1.Catalog, history map[string][]sqlplan.Column, out *apiv1.PipelinePlan) (*sqlplan.Answer, error) {
	spec := p.v.spec
	notPlanned := func(format string, args ...any) {
		out.Notes = append(out.Notes, "transform.sql wasn't planned: "+fmt.Sprintf(format, args...))
	}
	req, unknown := plan.SQLRequest(spec, cat, history)
	if len(unknown) > 0 {
		notPlanned("the schemas of streams %s aren't known: the source declares no columns for them, and no sync has read them yet", strings.Join(unknown, ", "))
		return nil, nil
	}
	if s.Planner == nil {
		notPlanned("this server can't send it to a runner")
		return nil, nil
	}
	answer, err := s.Planner.PlanSQL(ctx, contract.PlanSQLInput{OrgID: s.Tenant.Org.String(), DataPlane: p.v.dataPlane, Request: req})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		f, ok := errors.AsType[*orchestration.Failure](err)
		switch {
		case ok && f.Code == contract.NoRunner:
			notPlanned("no runner of data plane %q took it within %s", p.v.dataPlane, contract.PlanPickup)
		case ok:
			notPlanned("a runner of data plane %q failed to plan it: %s: %s", p.v.dataPlane, f.Code, f.Message)
		default:
			s.log().Error("planning transform.sql failed", "pipeline", p.name, "err", err)
			notPlanned("sending it to a runner of data plane %q failed: %v", p.v.dataPlane, err)
		}
		return nil, nil
	}
	for _, e := range answer.Errors {
		out.Problems = append(out.Problems, e.Error())
	}
	for _, n := range answer.Notes {
		out.Notes = append(out.Notes, "transform.sql:"+n.At.String()+": "+n.Message)
	}
	if len(answer.Errors) > 0 {
		return nil, nil
	}
	out.Transform = plan.Transform(spec, cat, &answer)
	p.sqlPlan = plan.SQLPlan(&answer)
	return &answer, nil
}

// inputSchemas are the newest schemas of the pipeline's source streams in its
// schema history, under the source's column names and without metadata
// columns, by stream.
func (s *Server) inputSchemas(ctx context.Context, q *db.Queries, pipeline uuid.UUID) (map[string][]sqlplan.Column, error) {
	rows, err := q.LatestStreamSchemas(ctx, db.LatestStreamSchemasParams{OrgID: s.Tenant.Org, PipelineID: pipeline})
	if err != nil {
		return nil, err
	}
	out := map[string][]sqlplan.Column{}
	for _, r := range rows {
		var cols []streamschema.Column
		if err := json.Unmarshal(r.Columns, &cols); err != nil {
			return nil, err
		}
		for _, c := range cols {
			name := cmp.Or(c.Source, c.Name)
			if strings.HasPrefix(name, "_openetl_") {
				continue
			}
			out[r.Stream] = append(out[r.Stream], sqlplan.Column{Name: name, Type: c.Type})
		}
	}
	return out, nil
}

// storeSQLPlan keeps what planning a new version's transform.sql gave with
// the version.
func (s *Server) storeSQLPlan(ctx context.Context, q *db.Queries, p *planned) error {
	if p.sqlPlan == nil || p.action == apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
		return nil
	}
	b, err := protojson.Marshal(p.sqlPlan)
	if err != nil {
		return err
	}
	return q.SetPipelineVersionSQLPlan(ctx, db.SetPipelineVersionSQLPlanParams{OrgID: s.Tenant.Org, PipelineID: p.id, Version: p.version, SqlPlan: b})
}

// sqlPlanOf is a version's stored SqlPlan; nil if it has none.
func sqlPlanOf(raw []byte) (*apiv1.SqlPlan, error) {
	if raw == nil {
		return nil, nil
	}
	out := &apiv1.SqlPlan{}
	return out, protojson.Unmarshal(raw, out)
}
