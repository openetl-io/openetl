package main

import (
	"context"
	"errors"
	"flag"
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
	checkPipeline    = "pipeline"
)

// checkTarget is a source or destination to check, or a pipeline to
// preflight.
type checkTarget struct {
	kind string
	// workspaces/{workspace}/sources/{source}, destinations or pipelines.
	name string
}

func (t checkTarget) label() string { return t.kind + " " + shortName(t.name) }

func shortName(name string) string { return name[strings.LastIndex(name, "/")+1:] }

type checkOutcome struct {
	target checkTarget
	err    error
	// A check's items.
	items []checkItem
	// A preflight's result.
	preflight *apiv1.PreflightResult
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

// run checks or preflights each target on a runner. A source that passes
// is discovered too.
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
	var res *apiv1.CheckResult
	switch t.kind {
	case checkPipeline:
		r, err := c.pipelines.PreflightPipeline(ctx, connect.NewRequest(&apiv1.PreflightPipelineRequest{Name: t.name}))
		if err != nil {
			o.err = err
			return o
		}
		o.preflight = r.Msg.GetResult()
		if !o.preflight.GetOk() {
			o.err = errors.New(preflightFailure(o.preflight))
		}
		return o
	case checkDestination:
		r, err := c.destinations.CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: t.name}))
		if err != nil {
			o.err = err
			return o
		}
		res = r.Msg.GetResult()
	default:
		r, err := c.sources.CheckSource(ctx, connect.NewRequest(&apiv1.CheckSourceRequest{Name: t.name}))
		if err != nil {
			o.err = err
			return o
		}
		res = r.Msg.GetResult()
	}
	o.items = apiCheckItems(res.GetItems())
	if !res.GetOk() {
		o.err = fmt.Errorf("%s: %s", res.GetCode(), res.GetMessage())
		return o
	}
	// Discovering refreshes the catalog that plans read.
	if t.kind == checkSource {
		_, o.err = c.sources.DiscoverSource(ctx, connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: t.name}))
	}
	return o
}

// partLabel names a preflight's part: "source shop's check".
func partLabel(p *apiv1.PreflightPart) string {
	if p.GetRole() == "sync" {
		return "the sync's plan"
	}
	return fmt.Sprintf("%s %s's %s", p.GetRole(), shortName(p.GetResource()), p.GetStage())
}

// preflightFailure is the first part that failed.
func preflightFailure(r *apiv1.PreflightResult) string {
	for _, p := range r.GetParts() {
		if !p.GetOk() && p.GetSkipped() == "" {
			if p.GetCode() == "" {
				return partLabel(p) + " failed"
			}
			return fmt.Sprintf("%s failed: %s: %s", partLabel(p), p.GetCode(), p.GetMessage())
		}
	}
	return "it failed"
}

// printPreflight prints each part of a preflight, and its items.
func printPreflight(out io.Writer, r *apiv1.PreflightResult) {
	for _, p := range r.GetParts() {
		switch {
		case p.GetSkipped() != "":
			fmt.Fprintf(out, "%s: skipped, %s\n", partLabel(p), p.GetSkipped())
		case p.GetOk():
			fmt.Fprintf(out, "%s: passed\n", partLabel(p))
		case p.GetCode() != "":
			fmt.Fprintf(out, "%s: failed: %s: %s\n", partLabel(p), p.GetCode(), p.GetMessage())
		default:
			fmt.Fprintf(out, "%s: failed\n", partLabel(p))
		}
		printCheckItems(out, "  ", apiCheckItems(p.GetItems()))
	}
}

func runPreflight(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl preflight", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl preflight [flags] PIPELINE")
		fmt.Fprintln(fs.Output(), "\nRuns on a runner of the pipeline's own data plane.")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("preflight takes one pipeline name, after its flags")
	}
	c, err := newCheckClients(conn)
	if err != nil {
		return err
	}
	name := "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)
	o := c.check(ctx, checkTarget{kind: checkPipeline, name: name})
	if o.preflight == nil {
		return o.err
	}
	printPreflight(out, o.preflight)
	plan, err := c.pipelines.GetPipelinePlan(ctx, connect.NewRequest(&apiv1.GetPipelinePlanRequest{Name: name}))
	if err != nil {
		return err
	}
	printPlan(out, plan.Msg.GetPlan())
	if o.err != nil {
		return fmt.Errorf("%s failed its preflight: %w", name, o.err)
	}
	return nil
}

// resourceCollection is "sources" for workspaces/w/sources/s, and empty for
// a name of another shape.
func resourceCollection(name string) string {
	if parts := strings.Split(name, "/"); len(parts) == 4 && parts[0] == "workspaces" {
		return parts[2]
	}
	return ""
}

// checkApplied asks a runner about what an apply changed: a preflight of
// each pipeline it created or updated, which checks its source and
// destination, and a check of each other source and destination it created
// or updated. What's applied stands, so a failure only warns.
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
	covered := map[string]bool{}
	changed := func(r *apiv1.AppliedResource) bool { return r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED }
	for _, r := range res.GetResources() {
		if !changed(r) || resourceCollection(r.GetName()) != "pipelines" {
			continue
		}
		targets = append(targets, checkTarget{kind: checkPipeline, name: r.GetName()})
		p, err := c.pipelines.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: r.GetName()}))
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("%s: %v", r.GetName(), err))
			continue
		}
		workspace, _, _ := strings.Cut(p.Msg.GetPipeline().GetName(), "/pipelines/")
		covered[workspace+"/sources/"+p.Msg.GetPipeline().GetSpec().GetSource().GetRef()] = true
		covered[workspace+"/destinations/"+p.Msg.GetPipeline().GetSpec().GetDestination().GetRef()] = true
	}
	for _, r := range res.GetResources() {
		if !changed(r) || covered[r.GetName()] {
			continue
		}
		switch resourceCollection(r.GetName()) {
		case "sources":
			targets = append(targets, checkTarget{kind: checkSource, name: r.GetName()})
		case "destinations":
			targets = append(targets, checkTarget{kind: checkDestination, name: r.GetName()})
		}
	}
	var passed []string
	for _, o := range c.run(ctx, targets) {
		if o.err == nil {
			passed = append(passed, o.target.label())
			continue
		}
		warnings = append(warnings, fmt.Sprintf("%s: %v", o.target.label(), o.err))
		items := o.items
		for _, p := range o.preflight.GetParts() {
			items = append(items, apiCheckItems(p.GetItems())...)
		}
		for _, it := range items {
			if it.status == "failed" && it.fix != "" {
				warnings = append(warnings, fmt.Sprintf("  %s: %s", it.title, it.fix))
			}
		}
	}
	// A pipeline that depends on what changed, without changing itself, gets
	// its plan looked at, with its source's catalog refreshed above.
	for _, r := range res.GetDependents() {
		if !changed(r) || resourceCollection(r.GetName()) != "pipelines" || slices.ContainsFunc(targets, func(t checkTarget) bool { return t.name == r.GetName() }) {
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
