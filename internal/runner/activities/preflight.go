package activities

import (
	"context"
	"math"
	"strings"
	"time"
	"unicode"

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
// runs the source's and the destination's preflights for that. What fails,
// a connector that doesn't start or answer included, is a result, not an
// error.
func (a *Ops) PreflightPipeline(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	spec, err := syncSpec(ctx, a.API, in.OrgID, in.PipelineID, in.Version)
	if err != nil {
		return res, err
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

	var (
		src source.Source
		dst destination.Destination
		job runner.Job
	)
	defer func() { connectorhost.Close(src) }()
	defer func() { connectorhost.Close(dst) }()
	p := preflight{
		startSource: func(ctx context.Context) (err error) {
			if src, err = bindSource(spec, env); err != nil {
				return err
			}
			return connectorhost.Start(ctx, src)
		},
		startDestination: func(ctx context.Context) (err error) {
			if dst, err = bindDestination(spec, env); err != nil {
				return err
			}
			return connectorhost.Start(ctx, dst)
		},
		checkSource:      func(ctx context.Context) (check.Report, error) { return runCheck(ctx, src) },
		checkDestination: func(ctx context.Context) (check.Report, error) { return runCheck(ctx, dst) },
		settle: func(ctx context.Context) (err error) {
			job, err = settleJob(ctx, spec, src, dst, plans)
			return err
		},
		preflightSource: func(ctx context.Context) (check.Report, error) {
			return runPreflight(ctx, src, sourcePreflight(spec.GetPipelineId(), spec.GetPipeline(), job))
		},
		preflightDestination: func(ctx context.Context) (check.Report, error) {
			return runPreflight(ctx, dst, destinationPreflight(spec.GetPipelineId(), job))
		},
		checkTime: checkTimeout, preflightTime: preflightTimeout,
	}
	if res.Preflight, err = p.run(ctx, spec.GetSourceConnector(), spec.GetDestinationConnector()); err != nil {
		return res, err
	}
	return fitResult("preflight", res)
}

// preflight is a pipeline's preflight in parts: starting each connector and
// its check, settling what a sync reads and writes, and each connector's
// preflight for that.
type preflight struct {
	startSource, startDestination         func(context.Context) error
	checkSource, checkDestination         func(context.Context) (check.Report, error)
	settle                                func(context.Context) error
	preflightSource, preflightDestination func(context.Context) (check.Report, error)
	// What each check and each preflight has. Settling has what's left of
	// ctx's time after both preflights'; starting, what it takes.
	checkTime, preflightTime time.Duration
}

// run runs the parts in order and gives them as the source's and the
// destination's checks, the plan, and the source's and the destination's
// preflights. A part fails with what its connector answered, or with why
// the connector didn't start or answer; it's skipped, saying why, only when
// a part it needs failed or was skipped. The error is ctx's.
func (p preflight) run(ctx context.Context, srcVersion, dstVersion string) ([]contract.CheckGroup, error) {
	parts := []contract.CheckGroup{
		{Role: roleSource, Stage: stageCheck, Version: srcVersion},
		{Role: roleDestination, Stage: stageCheck, Version: dstVersion},
		{Role: roleSync, Stage: stagePlan},
		{Role: roleSource, Stage: stagePreflight, Version: srcVersion},
		{Role: roleDestination, Stage: stagePreflight, Version: dstVersion},
	}
	srcCheck, dstCheck, plan, srcPre, dstPre := &parts[0], &parts[1], &parts[2], &parts[3], &parts[4]

	srcUp := started(ctx, srcCheck, p.startSource)
	srcOK := srcUp && runPart(ctx, srcCheck, p.checkTime, "the source's check", p.checkSource)
	dstUp := started(ctx, dstCheck, p.startDestination)
	dstOK := dstUp && runPart(ctx, dstCheck, p.checkTime, "the destination's check", p.checkDestination)

	// The plan needs the destination's capabilities, which don't depend on
	// its check.
	switch {
	case !srcUp:
		plan.Skipped = "the source didn't start"
	case !srcOK:
		plan.Skipped = "the source's check failed"
	case !dstUp:
		plan.Skipped = "the destination didn't start"
	default:
		left := time.Duration(math.MaxInt64)
		if d, ok := ctx.Deadline(); ok {
			left = time.Until(d) - 2*p.preflightTime
		}
		runPart(ctx, plan, left, "settling what a sync reads and writes", func(ctx context.Context) (check.Report, error) {
			return check.Report{}, p.settle(ctx)
		})
	}
	planned := plan.Skipped == "" && plan.Code == ""

	for _, pre := range []struct {
		g      *contract.CheckGroup
		role   string
		up, ok bool
		run    func(context.Context) (check.Report, error)
	}{{srcPre, "source", srcUp, srcOK, p.preflightSource}, {dstPre, "destination", dstUp, dstOK, p.preflightDestination}} {
		switch {
		case !pre.up:
			pre.g.Skipped = "the " + pre.role + " didn't start"
		case !pre.ok:
			pre.g.Skipped = "the " + pre.role + "'s check failed"
		case !planned:
			pre.g.Skipped = "a sync can't be planned"
		default:
			runPart(ctx, pre.g, p.preflightTime, "the "+pre.role+"'s preflight", pre.run)
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return parts, nil
}

// started starts a part's connector, and is whether it did; why it didn't
// is the part's failure.
func started(ctx context.Context, g *contract.CheckGroup, start func(context.Context) error) bool {
	if err := start(ctx); err != nil {
		g.Code, g.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
		return false
	}
	return true
}

// answerSlack is how long before its deadline a part that answered may have
// run out of time: the SDK answers up to a second before the RPC's.
const answerSlack = 2 * time.Second

// runPart runs a part, what, with at most budget of ctx's time, and is
// whether it passed. The part has what the connector answered, or, without
// items, why it didn't answer; one that fails as it runs out of time says
// how long it had.
func runPart(ctx context.Context, g *contract.CheckGroup, budget time.Duration, what string, run func(context.Context) (check.Report, error)) bool {
	pctx, cancel, had := within(ctx, budget)
	defer cancel()
	r, err := run(pctx)
	if err == nil {
		g.Items, g.Code, g.Message = checkResult(r)
		if r.OK() {
			return true
		}
	}
	if d, _ := pctx.Deadline(); time.Until(d) < answerSlack && ctx.Err() == nil {
		err = errs.New(errs.Transient, "%s didn't finish within %v", what, had.Round(time.Second))
	}
	if err != nil {
		g.Code, g.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
	}
	return false
}

// runPreflight runs c's preflight for req, a source.PreflightRequest or a
// destination.PreflightRequest: an external connector's report of its
// answer, or a built-in one's checks run here; an empty report without a
// preflight. The error is an external connector's that didn't answer.
func runPreflight(ctx context.Context, c any, req any) (check.Report, error) {
	switch req := req.(type) {
	case source.PreflightRequest:
		switch c := c.(type) {
		case interface {
			PreflightReport(context.Context, source.PreflightRequest) (check.Report, error)
		}:
			return c.PreflightReport(ctx, req)
		case source.Preflighter:
			return check.RunList(ctx, func() []check.Check { return c.Preflight(req) }), nil
		}
	case destination.PreflightRequest:
		switch c := c.(type) {
		case interface {
			PreflightReport(context.Context, destination.PreflightRequest) (check.Report, error)
		}:
			return c.PreflightReport(ctx, req)
		case destination.Preflighter:
			return check.RunList(ctx, func() []check.Check { return c.Preflight(req) }), nil
		}
	}
	return check.Report{}, nil
}

// sourcePreflight is what a sync of the job would read.
func sourcePreflight(pipeline string, p *apiv1.PipelineSpec, job runner.Job) source.PreflightRequest {
	req := source.PreflightRequest{Pipeline: pipeline, Continuous: job.Continuous}
	if !req.Continuous {
		req.Interval = interval(p.GetSchedule(), time.Now())
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
// [a-z0-9_] and before a leading digit; _ for none.
func tableName(stream string, keep bool) string {
	name := stream
	if !keep {
		var b strings.Builder
		for _, r := range stream {
			// İ lowercases to i and a combining dot, which isn't in [a-z0-9_].
			if r == 'İ' {
				b.WriteString("i_")
				continue
			}
			if r = unicode.ToLower(r); r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' {
				b.WriteRune(r)
			} else {
				b.WriteByte('_')
			}
		}
		name = b.String()
		if name != "" && name[0] >= '0' && name[0] <= '9' {
			name = "_" + name
		}
	}
	if name == "" {
		return "_"
	}
	return name
}

// The firings of a cron schedule that interval looks at: at most
// cronFirings, until one past cronHorizon from the first.
const (
	cronFirings = 10000
	cronHorizon = 366 * 24 * time.Hour
)

// interval is the longest a schedule goes between syncs after now, jitter
// included: its every, or the longest gap between its cron's firings over
// about a year, as the scheduler reads them: in UTC unless the cron starts
// with CRON_TZ=. It's 0 without a regular schedule, and for a cron that
// never fires.
func interval(s *apiv1.Schedule, now time.Time) time.Duration {
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
	t := sched.Next(now.UTC())
	if t.IsZero() {
		return 0
	}
	var longest time.Duration
	for end, i := t.Add(cronHorizon), 0; i < cronFirings && !t.After(end); i++ {
		next := sched.Next(t)
		if next.IsZero() {
			break
		}
		longest = max(longest, next.Sub(t))
		t = next
	}
	if longest == 0 {
		return 0
	}
	return longest + s.GetJitter().AsDuration()
}
