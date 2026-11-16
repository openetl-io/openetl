package activities

import (
	"context"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/robfig/cron/v3"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/engineclient"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The preflight's roles and stages, as contract.CheckGroup names them.
const (
	roleSource      = "source"
	roleDestination = "destination"
	roleSync        = "sync"
	stageCheck      = "check"
	stagePreflight  = "preflight"
	stagePlan       = "plan"
	// A stream's table, in its schema's metadata.
	tableKey = "openetl.table"
)

// PreflightPipeline checks a pipeline's source and destination, settles
// what a sync of its version would read and write as an attempt does, and
// runs the source's and the destination's preflights for that. What fails
// is a result, not an error.
func (a *Ops) PreflightPipeline(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	spec, err := syncSpec(ctx, a.API, in.OrgID, in.PipelineID, in.Version)
	if err != nil {
		return res, err
	}
	src := contract.CheckGroup{Role: roleSource, Stage: stageCheck, Version: spec.GetSourceConnector()}
	dst := contract.CheckGroup{Role: roleDestination, Stage: stageCheck, Version: spec.GetDestinationConnector()}
	failed := func(g *contract.CheckGroup, err error) {
		g.Code, g.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetSource(), spec.GetSourceSecrets()); err != nil {
		return res, err
	}
	if err := readSecrets(ctx, a.Secrets, spec.GetDestination(), spec.GetDestinationSecrets()); err != nil {
		return res, err
	}
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, spec.GetSourceConnector(), spec.GetDestinationConnector())
	if err != nil {
		return res, err
	}
	release, err := leaseAWS(ctx, a.AWSRoles, &env, spec.GetWorkspace(), spec.GetPipelineName(),
		awsConnection{"source", spec.GetSourceName(), spec.GetSource().GetAws()}, awsConnection{"destination", spec.GetDestinationName(), spec.GetDestination().GetAws()})
	defer release()
	if err != nil {
		return res, err
	}
	planner := a.Engine
	engine := func(ctx context.Context, req sqlplan.Request) (sqlplan.Answer, error) {
		return engineclient.Plan(ctx, planner, req)
	}
	plans := func(ctx context.Context, sql string, streams []runner.Stream) (sqlplan.Answer, error) {
		req, unknown, err := sqlRequest(sql, streams, nil)
		if err != nil {
			return sqlplan.Answer{}, err
		}
		a, err := engine(ctx, req)
		if err != nil {
			return a, err
		}
		return withUnknown(a, unknown), nil
	}
	if a.Checkpoints != nil {
		plans = withRestored(engine, checkpoint.NewLog(a.Checkpoints, spec.GetOrgId(), spec.GetPipelineId()))
	}
	job, jobErr := bindJob(ctx, spec, env, plans, false)
	defer connectorhost.Close(job.Source)
	defer connectorhost.Close(job.Destination)

	var srcOK, dstOK bool
	for _, side := range []struct {
		g  *contract.CheckGroup
		c  any
		ok *bool
	}{{&src, job.Source, &srcOK}, {&dst, job.Destination, &dstOK}} {
		if side.c == nil {
			side.g.Skipped = "the connector didn't start"
			continue
		}
		items, err := runCheck(ctx, side.c)
		side.g.Items = checkItems(items)
		if err != nil {
			failed(side.g, err)
			continue
		}
		*side.ok = true
	}
	res.Preflight = append(res.Preflight, src, dst)

	plan := contract.CheckGroup{Role: roleSync, Stage: stagePlan}
	if jobErr != nil {
		failed(&plan, jobErr)
		var r check.Report
		r.Fail("sync.plan", "Settle what a sync reads and writes", jobErr)
		plan.Items = checkItems(r.Items())
		res.Preflight = append(res.Preflight, plan)
		why := "a sync can't be planned"
		res.Preflight = append(res.Preflight,
			contract.CheckGroup{Role: roleSource, Stage: stagePreflight, Version: src.Version, Skipped: why},
			contract.CheckGroup{Role: roleDestination, Stage: stagePreflight, Version: dst.Version, Skipped: why})
		return res, nil
	}

	srcPre := contract.CheckGroup{Role: roleSource, Stage: stagePreflight, Version: src.Version}
	if !srcOK {
		srcPre.Skipped = "the source's check failed"
	} else {
		items, err := runPreflight(ctx, job.Source, sourcePreflight(spec.GetPipelineId(), spec.GetPipeline(), job))
		srcPre.Items = checkItems(items)
		if err != nil {
			failed(&srcPre, err)
		}
	}
	dstPre := contract.CheckGroup{Role: roleDestination, Stage: stagePreflight, Version: dst.Version}
	if !dstOK {
		dstPre.Skipped = "the destination's check failed"
	} else {
		items, err := runPreflight(ctx, job.Destination, destinationPreflight(spec.GetPipelineId(), job))
		dstPre.Items = checkItems(items)
		if err != nil {
			failed(&dstPre, err)
		}
	}
	res.Preflight = append(res.Preflight, srcPre, dstPre)
	return res, nil
}

// runPreflight runs c's preflight for req: an external connector's items, or
// a built-in one's report; none without one.
func runPreflight(ctx context.Context, c any, req any) ([]check.Item, error) {
	switch req := req.(type) {
	case source.PreflightRequest:
		switch c := c.(type) {
		case interface {
			PreflightItems(context.Context, source.PreflightRequest) ([]check.Item, error)
		}:
			return c.PreflightItems(ctx, req)
		case source.Preflighter:
			var r check.Report
			c.Preflight(ctx, req, &r)
			return r.Items(), r.Err()
		}
	case destination.PreflightRequest:
		switch c := c.(type) {
		case interface {
			PreflightItems(context.Context, destination.PreflightRequest) ([]check.Item, error)
		}:
			return c.PreflightItems(ctx, req)
		case destination.Preflighter:
			var r check.Report
			c.Preflight(ctx, req, &r)
			return r.Items(), r.Err()
		}
	}
	return nil, nil
}

// sourcePreflight is what a sync of the job would read.
func sourcePreflight(pipeline string, p *apiv1.PipelineSpec, job runner.Job) source.PreflightRequest {
	req := source.PreflightRequest{Pipeline: pipeline, Continuous: job.Continuous}
	if !req.Continuous {
		req.Interval = interval(p.GetSchedule())
	}
	for _, s := range job.Streams {
		req.Streams = append(req.Streams, source.ConfiguredStream{Name: s.Name, SyncMode: s.Read, Cursor: s.CursorField})
	}
	return req
}

// destinationPreflight is what a sync of the job would write: its streams,
// and their schemas as the source declares them, under the table a sync
// names, before it resolves collisions and fits identifier limits.
func destinationPreflight(pipeline string, job runner.Job) destination.PreflightRequest {
	req := destination.PreflightRequest{Pipeline: pipeline, Namespace: job.Namespace, Schemas: map[string]*arrow.Schema{}}
	streams := job.Streams
	if len(job.Outputs) > 0 {
		streams = job.Outputs
	}
	for _, s := range streams {
		req.Streams = append(req.Streams, destination.Stream{Name: s.Name, Mode: s.Mode, PrimaryKey: s.PrimaryKey, Deletes: s.Deletes, AllowTypeChanges: s.AllowTypeChanges})
		table := s.Table
		if table == "" {
			table = tableName(s.Name, job.KeepSourceNames)
		}
		var fields []arrow.Field
		if d := s.Declared(); d != nil {
			fields = d.Fields()
		}
		md := arrow.NewMetadata([]string{tableKey}, []string{table})
		req.Schemas[s.Name] = arrow.NewSchema(fields, &md)
	}
	return req
}

// tableName is a stream's table as the engine names it, before collisions
// and identifier limits: as it is, or lowercase with _ for anything outside
// [a-z0-9_].
func tableName(stream string, keep bool) string {
	if keep {
		return stream
	}
	name := strings.Map(func(r rune) rune {
		r = []rune(strings.ToLower(string(r)))[0]
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
			return r
		}
		return '_'
	}, stream)
	if name == "" {
		return "_"
	}
	return name
}

// interval is the longest a schedule goes between syncs, over its next
// month; 0 without a regular one.
func interval(s *apiv1.Schedule) time.Duration {
	if d := s.GetEvery(); d != nil {
		return d.AsDuration() + s.GetJitter().AsDuration()
	}
	if s.GetCron() == "" {
		return 0
	}
	sched, err := cron.ParseStandard(s.GetCron())
	if err != nil {
		return 0
	}
	var longest time.Duration
	t := sched.Next(time.Now())
	for end := t.AddDate(0, 1, 0); t.Before(end); {
		next := sched.Next(t)
		longest = max(longest, next.Sub(t))
		t = next
	}
	return longest + s.GetJitter().AsDuration()
}
