package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// runOverrides lists a pipeline's paused streams and heal's overrides, or
// removes an override.
func runOverrides(ctx context.Context, args []string, out io.Writer) error {
	sub := "list"
	if len(args) > 0 && (args[0] == "list" || args[0] == "remove") {
		sub, args = args[0], args[1:]
	}
	var workspace *string
	flags := func(fs *flag.FlagSet) { workspace = fs.String("workspace", "default", "the pipeline's workspace") }
	c := func(conn *api) (apiv1connect.PipelineServiceClient, error) {
		return client(conn, apiv1connect.NewPipelineServiceClient)
	}
	if sub == "remove" {
		conn, rest, err := subcommand("overrides remove", args, 2, "PIPELINE FIELD", flags)
		if err != nil {
			return err
		}
		pc, err := c(conn)
		if err != nil {
			return err
		}
		name := "workspaces/" + *workspace + "/pipelines/" + rest[0]
		if _, err := pc.RemovePipelineOverride(ctx, connect.NewRequest(&apiv1.RemovePipelineOverrideRequest{Name: name, Field: rest[1]})); err != nil {
			return err
		}
		fmt.Fprintf(out, "removed heal's override of %s from %s; the spec has its say again\n", rest[1], name)
		return nil
	}
	conn, rest, err := subcommand("overrides", args, 1, "PIPELINE", flags)
	if err != nil {
		return err
	}
	pc, err := c(conn)
	if err != nil {
		return err
	}
	res, err := pc.GetPipeline(ctx, connect.NewRequest(&apiv1.GetPipelineRequest{Name: "workspaces/" + *workspace + "/pipelines/" + rest[0]}))
	if err != nil {
		return err
	}
	p := res.Msg.GetPipeline()
	if len(p.GetPausedStreams()) == 0 && len(p.GetOverrides()) == 0 {
		fmt.Fprintln(out, "no paused streams, and nothing overridden")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if len(p.GetPausedStreams()) > 0 {
		fmt.Fprintln(tw, "PAUSED STREAM\tSINCE\tWHY")
		for _, s := range p.GetPausedStreams() {
			fmt.Fprintf(tw, "%s\t%s\t%s\n", s.GetStream(), s.GetCreateTime().AsTime().Local().Format(time.DateTime), reasonWithIncident(s.GetReason(), s.GetIncident()))
		}
	}
	if len(p.GetOverrides()) > 0 {
		if len(p.GetPausedStreams()) > 0 {
			fmt.Fprintln(tw)
		}
		fmt.Fprintln(tw, "FIELD\tVALUE\tSPEC\tWHY")
		for _, o := range p.GetOverrides() {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", o.GetField(), o.GetValue(), orUnset(o.GetSpecValue()), reasonWithIncident(o.GetReason(), o.GetIncident()))
		}
	}
	return tw.Flush()
}

func orUnset(s string) string {
	if s == "" {
		return "(unset)"
	}
	return s
}

// reasonWithIncident is why, and the incident's ID if it's set.
func reasonWithIncident(why, incident string) string {
	if incident == "" {
		return why
	}
	return why + " (incident " + incident[strings.LastIndex(incident, "/")+1:] + ")"
}

// overrideText is an override as the plan prints it.
func overrideText(o *apiv1.PipelineOverride) string {
	return fmt.Sprintf("%s: %s, where the spec has %s; %s", o.GetField(), o.GetValue(), orUnset(o.GetSpecValue()), reasonWithIncident(o.GetReason(), o.GetIncident()))
}
