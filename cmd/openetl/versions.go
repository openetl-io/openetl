package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"path"
	"text/tabwriter"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

func runVersions(ctx context.Context, args []string, out io.Writer) error {
	var workspace *string
	var limit *int
	conn, rest, err := subcommand("versions", args, 1, "PIPELINE", func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		limit = fs.Int("limit", 20, "at most this many versions")
	})
	if err != nil {
		return err
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	req := &apiv1.ListPipelineVersionsRequest{Parent: "workspaces/" + *workspace + "/pipelines/" + rest[0]}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "VERSION\tCURRENT\tCREATED\tPROVENANCE")
	for left := *limit; left > 0; {
		req.PageSize = int32(min(left, 500))
		res, err := c.ListPipelineVersions(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, v := range res.Msg.GetPipelineVersions() {
			current := ""
			if v.GetCurrent() {
				current = "*"
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", path.Base(v.GetName()), current, when(v.GetCreateTime()), v.GetProvenance())
		}
		left -= len(res.Msg.GetPipelineVersions())
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	return tw.Flush()
}
