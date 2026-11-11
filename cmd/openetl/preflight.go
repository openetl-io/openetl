package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"golang.org/x/sync/errgroup"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// applyCheckLimit bounds the checks after an apply; one operation can wait a
// minute for a runner alone.
const applyCheckLimit = 2 * time.Minute

// checksAtOnce bounds the connector operations that run together.
const checksAtOnce = 4

const (
	checkSource      = "source"
	checkDestination = "destination"
)

// checkTarget is a source or destination for a runner to reach.
type checkTarget struct {
	kind string
	// workspaces/{workspace}/sources/{source}, or destinations.
	name string
	// Empty: the server's.
	dataPlane string
}

func (t checkTarget) label() string { return t.kind + " " + t.name[strings.LastIndex(t.name, "/")+1:] }

type checkOutcome struct {
	target  checkTarget
	streams int
	err     error
}

type checkClients struct {
	sources      apiv1connect.SourceServiceClient
	destinations apiv1connect.DestinationServiceClient
	pipelines    apiv1connect.PipelineServiceClient
}

func newCheckClients(conn *api) (c checkClients, err error) {
	if c.sources, err = client(conn, apiv1connect.NewSourceServiceClient); err != nil {
		return c, err
	}
	if c.destinations, err = client(conn, apiv1connect.NewDestinationServiceClient); err != nil {
		return c, err
	}
	c.pipelines, err = client(conn, apiv1connect.NewPipelineServiceClient)
	return c, err
}

// run checks each target on a runner, and discovers each source that passes,
// which refreshes the catalog plans read.
func (c checkClients) run(ctx context.Context, targets []checkTarget) []checkOutcome {
	out := make([]checkOutcome, len(targets))
	var g errgroup.Group
	g.SetLimit(checksAtOnce)
	for i, t := range targets {
		g.Go(func() error {
			out[i] = c.check(ctx, t)
			return nil
		})
	}
	_ = g.Wait()
	return out
}

func (c checkClients) check(ctx context.Context, t checkTarget) checkOutcome {
	o := checkOutcome{target: t}
	if t.kind == checkDestination {
		r, err := c.destinations.CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: t.name, DataPlane: t.dataPlane}))
		if err != nil {
			o.err = err
			return o
		}
		o.err = checkFailure(r.Msg.GetResult())
		return o
	}
	r, err := c.sources.CheckSource(ctx, connect.NewRequest(&apiv1.CheckSourceRequest{Name: t.name, DataPlane: t.dataPlane}))
	if err != nil {
		o.err = err
		return o
	}
	if o.err = checkFailure(r.Msg.GetResult()); o.err != nil {
		return o
	}
	d, err := c.sources.DiscoverSource(ctx, connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: t.name, DataPlane: t.dataPlane}))
	if err != nil {
		o.err = err
		return o
	}
	o.streams = len(d.Msg.GetCatalog().GetStreams())
	return o
}

func checkFailure(r *apiv1.CheckResult) error {
	if r.GetOk() {
		return nil
	}
	return fmt.Errorf("%s: %s", r.GetCode(), r.GetMessage())
}

// pipelineTargets are p's source and destination, on dataPlane or else on
// p's own.
func pipelineTargets(p *apiv1.Pipeline, dataPlane string) []checkTarget {
	workspace, _, _ := strings.Cut(p.GetName(), "/pipelines/")
	dp := cmp.Or(dataPlane, p.GetSpec().GetDataPlane())
	var out []checkTarget
	if ref := p.GetSpec().GetSource().GetRef(); ref != "" {
		out = append(out, checkTarget{kind: checkSource, name: workspace + "/sources/" + ref, dataPlane: dp})
	}
	if ref := p.GetSpec().GetDestination().GetRef(); ref != "" {
		out = append(out, checkTarget{kind: checkDestination, name: workspace + "/destinations/" + ref, dataPlane: dp})
	}
	return out
}

// checkPipeline checks a pipeline's connections, refreshes its source's
// catalog and prints the plan that gives, and fails if any of that does.
func checkPipeline(ctx context.Context, conn *api, workspace, dataPlane, pipeline string, out io.Writer) error {
	c, err := newCheckClients(conn)
	if err != nil {
		return err
	}
	name := "workspaces/" + workspace + "/pipelines/" + pipeline
	p, err := c.pipelines.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: name}))
	if err != nil {
		return err
	}
	failed := false
	for _, o := range c.run(ctx, pipelineTargets(p.Msg.GetPipeline(), dataPlane)) {
		switch {
		case o.err != nil:
			failed = true
			fmt.Fprintf(out, "%s failed its check: %v\n", o.target.label(), o.err)
		case o.target.kind == checkSource:
			fmt.Fprintf(out, "%s passed its check, and offers %s\n", o.target.label(), streamCount(o.streams))
		default:
			fmt.Fprintf(out, "%s passed its check\n", o.target.label())
		}
	}
	plan, err := c.pipelines.GetPipelinePlan(ctx, connect.NewRequest(&apiv1.GetPipelinePlanRequest{Name: name}))
	if err != nil {
		return err
	}
	printPlan(out, plan.Msg.GetPlan())
	if failed || len(plan.Msg.GetPlan().GetProblems()) > 0 {
		return errors.New(name + " failed its checks")
	}
	return nil
}

func streamCount(n int) string {
	if n == 1 {
		return "1 stream"
	}
	return fmt.Sprintf("%d streams", n)
}

// resourceCollection is "sources" for workspaces/w/sources/s, and empty for
// a name of another shape.
func resourceCollection(name string) string {
	if parts := strings.Split(name, "/"); len(parts) == 4 && parts[0] == "workspaces" {
		return parts[2]
	}
	return ""
}

// checkApplied asks a runner to check what an apply changed: each source and
// destination it created or updated, and each one a pipeline it created or
// updated uses. What's applied stands, so a failure only warns.
func checkApplied(ctx context.Context, conn *api, res *apiv1.ApplyResponse, out io.Writer) {
	ctx, cancel := context.WithTimeout(ctx, applyCheckLimit)
	defer cancel()
	c, err := newCheckClients(conn)
	if err != nil {
		fmt.Fprintf(out, "\nthe apply stands, but its checks couldn't run: %v\n", err)
		return
	}
	var targets []checkTarget
	var warnings []string
	add := func(t checkTarget) {
		if !slices.ContainsFunc(targets, func(have checkTarget) bool { return have.name == t.name }) {
			targets = append(targets, t)
		}
	}
	changed := func(r *apiv1.AppliedResource) bool { return r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED }
	// A pipeline's connections are checked on its data plane, so they go first.
	for _, r := range res.GetResources() {
		if !changed(r) || resourceCollection(r.GetName()) != "pipelines" {
			continue
		}
		p, err := c.pipelines.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: r.GetName()}))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", r.GetName(), err))
			continue
		}
		for _, t := range pipelineTargets(p.Msg.GetPipeline(), "") {
			add(t)
		}
	}
	for _, r := range res.GetResources() {
		if !changed(r) {
			continue
		}
		switch resourceCollection(r.GetName()) {
		case "sources":
			add(checkTarget{kind: checkSource, name: r.GetName()})
		case "destinations":
			add(checkTarget{kind: checkDestination, name: r.GetName()})
		}
	}
	if len(targets) == 0 {
		return
	}
	var passed []string
	for _, o := range c.run(ctx, targets) {
		if o.err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", o.target.label(), o.err))
			continue
		}
		passed = append(passed, o.target.label())
	}
	// With its source's catalog fresh, a plan can see what the apply's couldn't.
	for _, r := range slices.Concat(res.GetResources(), res.GetDependents()) {
		if !changed(r) || resourceCollection(r.GetName()) != "pipelines" || ctx.Err() != nil {
			continue
		}
		plan, err := c.pipelines.GetPipelinePlan(ctx, connect.NewRequest(&apiv1.GetPipelinePlanRequest{Name: r.GetName()}))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", r.GetName(), err))
			continue
		}
		for _, problem := range plan.Msg.GetPlan().GetProblems() {
			warnings = append(warnings, fmt.Sprintf("%s: %s", r.GetName(), problem))
		}
	}
	if len(passed) > 0 {
		fmt.Fprintf(out, "checked %s\n", strings.Join(passed, ", "))
	}
	if len(warnings) > 0 {
		fmt.Fprintln(out, "\nthe apply stands, but its checks found:")
		for _, w := range warnings {
			fmt.Fprintf(out, "  %s\n", w)
		}
	}
}
