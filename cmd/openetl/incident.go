package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

const incidentUsage = `usage: openetl incident <command> [flags]

Commands:
  list [PIPELINE]             list open incidents, newest first: a
                              pipeline's, or the workspace's
  get PIPELINE INCIDENT       show what happened, what heal did about it,
                              and what to do
  resolve PIPELINE INCIDENT   resolve an incident by hand

Most incidents resolve themselves once their pipeline makes progress again.
`

func runIncident(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		fmt.Fprint(os.Stderr, incidentUsage)
		return errors.New("no incident command")
	}
	cmd, args := args[0], args[1:]
	fs := flag.NewFlagSet("openetl incident "+cmd, flag.ContinueOnError)
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	var all, asJSON *bool
	var comment *string
	switch cmd {
	case "list":
		all = fs.Bool("all", false, "list resolved incidents too")
		asJSON = fs.Bool("json", false, "print each incident as a line of JSON")
	case "get":
		asJSON = fs.Bool("json", false, "print the incident as JSON")
	case "resolve":
		comment = fs.String("comment", "", "why, for the incident's resolution")
	default:
		fmt.Fprint(os.Stderr, incidentUsage)
		return fmt.Errorf("unknown incident command %q", cmd)
	}
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case cmd == "list" && fs.NArg() > 1:
		return errors.New("incident list takes at most a pipeline, after its flags")
	case cmd != "list" && fs.NArg() != 2:
		return fmt.Errorf("incident %s takes a pipeline and an incident, after its flags", cmd)
	}
	c, err := client(conn, apiv1connect.NewIncidentServiceClient)
	if err != nil {
		return err
	}
	pipelines := "workspaces/" + *workspace + "/pipelines/"
	switch cmd {
	case "list":
		parent := pipelines + "-"
		if fs.NArg() == 1 {
			parent = pipelines + fs.Arg(0)
		}
		return listIncidents(ctx, c, parent, *all, *asJSON, out)
	case "get":
		res, err := c.GetIncident(ctx, connect.NewRequest(&apiv1.GetIncidentRequest{Name: pipelines + fs.Arg(0) + "/incidents/" + fs.Arg(1)}))
		if err != nil {
			return err
		}
		if *asJSON {
			b, err := protojson.MarshalOptions{Multiline: true}.Marshal(res.Msg.GetIncident())
			if err != nil {
				return err
			}
			fmt.Fprintln(out, string(b))
			return nil
		}
		return printIncident(res.Msg.GetIncident(), out)
	}
	name := pipelines + fs.Arg(0) + "/incidents/" + fs.Arg(1)
	if _, err := c.ResolveIncident(ctx, connect.NewRequest(&apiv1.ResolveIncidentRequest{Name: name, Comment: *comment})); err != nil {
		return err
	}
	fmt.Fprintln(out, "resolved", name)
	return nil
}

func incidentClass(i *apiv1.Incident) string {
	class := strings.ToLower(strings.TrimPrefix(i.GetClass().String(), "INCIDENT_CLASS_"))
	if i.GetCode() != "" {
		class += " (" + i.GetCode() + ")"
	}
	return class
}

func lower(e fmt.Stringer, prefix string) string {
	return strings.ToLower(strings.TrimPrefix(e.String(), prefix))
}

func listIncidents(ctx context.Context, c apiv1connect.IncidentServiceClient, parent string, all, asJSON bool, out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if !asJSON {
		fmt.Fprintln(tw, "INCIDENT\tPIPELINE\tCLASS\tSEVERITY\tSTATE\tSEEN\tLAST SEEN\tSUMMARY")
	}
	req := &apiv1.ListIncidentsRequest{Parent: parent}
	if !all {
		req.State = apiv1.IncidentState_INCIDENT_STATE_OPEN
	}
	for {
		res, err := c.ListIncidents(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, i := range res.Msg.GetIncidents() {
			if asJSON {
				b, err := protojson.Marshal(i)
				if err != nil {
					return err
				}
				fmt.Fprintln(out, string(b))
				continue
			}
			pipeline, id, _ := strings.Cut(i.GetName(), "/incidents/")
			pipeline = pipeline[strings.LastIndex(pipeline, "/")+1:]
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%d\t%s\t%s\n", id, pipeline, incidentClass(i), lower(i.GetSeverity(), "INCIDENT_SEVERITY_"),
				lower(i.GetState(), "INCIDENT_STATE_"), i.GetOccurrences(), i.GetLastSeenTime().AsTime().Local().Format(time.DateTime), i.GetSummary())
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return tw.Flush()
		}
	}
}

func printIncident(i *apiv1.Incident, out io.Writer) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	row := func(k, v string) {
		if v != "" {
			fmt.Fprintf(tw, "%s\t%s\n", k, v)
		}
	}
	row("NAME", i.GetName())
	row("CLASS", incidentClass(i))
	row("SEVERITY", lower(i.GetSeverity(), "INCIDENT_SEVERITY_"))
	state := lower(i.GetState(), "INCIDENT_STATE_")
	if i.GetResolveTime() != nil {
		state += " " + i.GetResolveTime().AsTime().Local().Format(time.DateTime) + ": " + i.GetResolution()
	}
	row("STATE", state)
	row("STREAM", i.GetStream())
	row("SUMMARY", i.GetSummary())
	row("MESSAGE", i.GetMessage())
	row("SEEN", fmt.Sprintf("%d times, first %s, last %s", i.GetOccurrences(),
		i.GetFirstSeenTime().AsTime().Local().Format(time.DateTime), i.GetLastSeenTime().AsTime().Local().Format(time.DateTime)))
	row("SYNC", i.GetSync())
	for _, a := range i.GetActions() {
		row("ACTION", fmt.Sprintf("%s %s, %s: %s", lower(a.GetState(), "HEAL_ACTION_STATE_"), lower(a.GetKind(), "HEAL_ACTION_KIND_"),
			a.GetTime().AsTime().Local().Format(time.DateTime), a.GetDescription()))
	}
	for _, k := range slices.Sorted(maps.Keys(i.GetDetails())) {
		row(strings.ToUpper(k), i.GetDetails()[k])
	}
	row("NEXT STEPS", i.GetNextSteps())
	row("FINGERPRINT", i.GetFingerprint())
	return tw.Flush()
}
