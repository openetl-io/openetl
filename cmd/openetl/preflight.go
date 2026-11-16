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
		if o.preflight = r.Msg.GetResult(); o.preflight == nil {
			o.err = errNoResult
		} else if why, failed := preflightFailure(o.preflight); failed {
			o.err = checkFailed("%s", why)
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
	if res == nil {
		o.err = errNoResult
		return o
	}
	o.items = apiCheckItems(res.GetItems())
	if why, failed := resultFailure(res, o.items); failed {
		o.err = checkFailed("%s", why)
		return o
	}
	// Discovering refreshes the catalog that plans read.
	if t.kind == checkSource {
		_, o.err = c.sources.DiscoverSource(ctx, connect.NewRequest(&apiv1.DiscoverSourceRequest{Name: t.name}))
	}
	return o
}

var errNoResult = errors.New("the API answered without a result")

// resultFailure is why a check's result failed. It failed when it isn't ok
// or when an item failed, whatever its code says.
func resultFailure(res *apiv1.CheckResult, items []checkItem) (why string, failed bool) {
	it, failing := firstFailing(items)
	switch {
	case res.GetOk() && !failing:
		return "", false
	case res.GetCode() != "" || res.GetMessage() != "":
		return codeText(res.GetCode(), res.GetMessage()), true
	case failing:
		return it.text(), true
	}
	return "it failed", true
}

func codeText(code, message string) string {
	return printable(code) + ": " + printable(message)
}

// partLabel names a preflight's part: "source shop's check".
func partLabel(p *apiv1.PreflightPart) string {
	if p.GetRole() == "sync" {
		return "the sync's plan"
	}
	return printable(fmt.Sprintf("%s %s's %s", p.GetRole(), shortName(p.GetResource()), p.GetStage()))
}

// partFailure is why a preflight's part failed. It failed when it neither
// passed nor was skipped, or when an item failed, whatever its ok says.
func partFailure(p *apiv1.PreflightPart) (why string, failed bool) {
	it, failing := firstFailing(apiCheckItems(p.GetItems()))
	switch {
	case !failing && (p.GetOk() || p.GetSkipped() != ""):
		return "", false
	case p.GetCode() != "":
		return codeText(p.GetCode(), p.GetMessage()), true
	case failing:
		return it.text(), true
	}
	return "", true
}

// preflightFailure is the first part that failed; or else, as a skipped
// part leaves the preflight unfinished, the first part skipped; or else why
// a result that isn't ok failed without either.
func preflightFailure(r *apiv1.PreflightResult) (why string, failed bool) {
	for _, p := range r.GetParts() {
		if why, failed := partFailure(p); failed {
			if why == "" {
				return partLabel(p) + " failed", true
			}
			return partLabel(p) + " failed: " + why, true
		}
	}
	for _, p := range r.GetParts() {
		if p.GetSkipped() != "" {
			return partLabel(p) + " was skipped: " + printable(p.GetSkipped()), true
		}
	}
	if !r.GetOk() {
		return "it failed, though none of its parts did; run it again", true
	}
	return "", false
}

// printPreflight prints each part of a preflight, and its items.
func printPreflight(out io.Writer, r *apiv1.PreflightResult) {
	for _, p := range r.GetParts() {
		_, failed := partFailure(p)
		switch {
		case !failed && p.GetSkipped() != "":
			fmt.Fprintf(out, "%s: skipped, %s\n", partLabel(p), printable(p.GetSkipped()))
		case !failed:
			fmt.Fprintf(out, "%s: passed\n", partLabel(p))
		case p.GetCode() != "":
			fmt.Fprintf(out, "%s: failed: %s\n", partLabel(p), codeText(p.GetCode(), p.GetMessage()))
		default:
			fmt.Fprintf(out, "%s: failed\n", partLabel(p))
		}
		printCheckItems(out, "  ", apiCheckItems(p.GetItems()))
	}
}

func runPreflight(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl preflight", flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl preflight [flags] PIPELINE")
		fmt.Fprint(fs.Output(), "\nChecks that a pipeline would run, on a runner of its own data plane: both\n"+
			"connections, then both connectors for the streams it reads and writes. As\n"+
			"text, it then prints the pipeline's plan, or, if an apply has changed the\n"+
			"pipeline since, the version it checked and the current one.\n"+exitStatusUsage+"\n")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	format := formatFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if err := checkFormat(*format); err != nil {
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
	if *format == formatJSON {
		if err := printJSON(out, o.preflight); err != nil {
			return err
		}
	} else {
		printPreflight(out, o.preflight)
		plan, err := c.pipelines.GetPipelinePlan(ctx, connect.NewRequest(&apiv1.GetPipelinePlanRequest{Name: name}))
		switch {
		case err != nil && o.err != nil:
			return checkFailed("%s failed its preflight: %w; and its plan couldn't be fetched: %w", name, o.err, err)
		case err != nil:
			return fmt.Errorf("%s passed its preflight, but its plan couldn't be fetched: %w", name, err)
		}
		// The plan is the current version's, which an apply since the
		// preflight may have replaced.
		if checked, current := o.preflight.GetVersion(), plan.Msg.GetPlan().GetVersion(); checked != current {
			fmt.Fprintf(out, "\n%s changed after its preflight: version %.12s was checked, and version %.12s is current\n",
				name, printable(checked), printable(current))
		} else {
			printPlan(out, plan.Msg.GetPlan())
		}
	}
	if o.err != nil {
		return checkFailed("%s failed its preflight: %w", name, o.err)
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
		items := o.items
		for _, p := range o.preflight.GetParts() {
			items = append(items, apiCheckItems(p.GetItems())...)
		}
		// Each failed or warning item, with its message and its fix.
		var found []string
		for _, it := range items {
			if it.status != statusFailed && it.status != statusWarning {
				continue
			}
			line := fmt.Sprintf("  %-8s %s", it.status, it.title)
			if it.message != "" {
				line += ": " + it.message
			}
			found = append(found, line)
			if it.fix != "" {
				found = append(found, strings.Repeat(" ", 11)+"fix: "+it.fix)
			}
		}
		switch {
		case o.err != nil:
			warnings = append(warnings, fmt.Sprintf("%s: %v", o.target.label(), o.err))
		case len(found) > 0:
			passed = append(passed, o.target.label())
			warnings = append(warnings, o.target.label()+": passed, with warnings")
		default:
			passed = append(passed, o.target.label())
		}
		warnings = append(warnings, found...)
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
			fmt.Fprintf(out, "  %s\n", printable(w))
		}
	}
}
