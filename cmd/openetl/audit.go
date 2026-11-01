package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

const auditUsage = `usage: openetl audit list [flags]

Lists the audit log, newest first: every call that changed something or
read secrets' values, who made it, and how it ended. It takes admin.
`

func runAudit(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 || args[0] != "list" {
		fmt.Fprint(os.Stderr, auditUsage)
		if len(args) == 0 {
			return errors.New("no audit command")
		}
		return fmt.Errorf("unknown audit command %q", args[0])
	}
	var actor, resource, method, since *string
	var limit *int
	var asJSON *bool
	conn, _, err := subcommand("audit list", args[1:], 0, "", func(fs *flag.FlagSet) {
		actor = fs.String("actor", "", "only this principal's calls, or a data plane's, such as dataPlanes/default")
		resource = fs.String("resource", "", "only calls that named or touched this resource, such as workspaces/default/secrets/pg")
		method = fs.String("method", "", "only this method's calls, such as SyncPipeline")
		since = fs.String("since", "", "only calls in the last 24h, 7d, …")
		limit = fs.Int("limit", 50, "at most this many entries")
		asJSON = fs.Bool("json", false, "print each entry as a line of JSON, with its details")
	})
	if err != nil {
		return err
	}
	req := &apiv1.ListAuditEntriesRequest{Actor: *actor, Resource: *resource, Method: *method}
	if req.Actor != "" && !strings.Contains(req.Actor, "/") {
		req.Actor = "principals/" + req.Actor
	}
	if *since != "" {
		d, err := parseDuration(*since)
		if err != nil || d <= 0 {
			return fmt.Errorf("--since %q isn't a positive duration", *since)
		}
		req.StartTime = timestamppb.New(time.Now().Add(-d))
	}
	c, err := client(conn, apiv1connect.NewAuditServiceClient)
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	if !*asJSON {
		fmt.Fprintln(tw, "TIME\tACTOR\tMETHOD\tCODE\tRESOURCES")
	}
	for left := *limit; left > 0; {
		req.PageSize = int32(min(left, 500))
		res, err := c.ListAuditEntries(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, e := range res.Msg.GetEntries() {
			if *asJSON {
				b, err := protojson.Marshal(e)
				if err != nil {
					return err
				}
				fmt.Fprintf(out, "%s\n", b)
				continue
			}
			code := e.GetCode()
			if code == "" {
				code = "unfinished"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", e.GetTime().AsTime().Local().Format(time.DateTime), e.GetActor(),
				shortMethod(e.GetMethod()), code, strings.Join(e.GetResources(), ", "))
		}
		left -= len(res.Msg.GetEntries())
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	return tw.Flush()
}

// shortMethod drops a procedure's package: PipelineService/SyncPipeline.
func shortMethod(m string) string {
	slash := strings.LastIndex(m, "/")
	if !strings.HasPrefix(m, "/") || slash <= 0 {
		return m
	}
	return m[strings.LastIndex(m[:slash], ".")+1:]
}
