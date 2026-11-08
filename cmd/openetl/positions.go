package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/plan"
)

func runReset(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl reset", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl reset [flags] NAME")
		fmt.Fprintln(fs.Output(), "Reads a pipeline's streams again from the start on its next sync or run: every stream, or each --stream.")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	var streams files
	fs.Var(&streams, "stream", "a stream to read again; repeat for more. None: every stream")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("reset takes one pipeline name, after its flags")
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	name := "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)
	resp, err := c.ResetPipeline(ctx, connect.NewRequest(&apiv1.ResetPipelineRequest{Name: name, Streams: streams}))
	if err != nil {
		return err
	}
	what := "every stream of " + name
	if len(streams) > 0 {
		what = "stream " + strings.Join(streams, ", ") + " of " + name
	}
	p := resp.Msg.GetPipeline()
	next := "its next sync"
	if plan.Continuous(p.GetSpec()) {
		next = "its run, which restarts at a checkpoint"
	}
	fmt.Fprintf(out, "%s is read again from the start by %s. Merged streams update rows in place; appended streams get every row again.\n", what, next)
	if p.GetPaused() {
		fmt.Fprintf(out, "The pipeline is paused: resume it to start (openetl resume %s%s).\n", workspaceFlag(*workspace), fs.Arg(0))
	}
	return nil
}

func runRewind(ctx context.Context, args []string, out io.Writer) error {
	fs := flag.NewFlagSet("openetl rewind", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprintln(fs.Output(), "usage: openetl rewind --yes [flags] NAME")
		fmt.Fprintln(fs.Output(), "Lets a pipeline whose latest checkpoint can't be read restart from the one before it on its next sync or run.")
		fs.PrintDefaults()
	}
	conn := apiFlags(fs)
	workspace := fs.String("workspace", "default", "the pipeline's workspace")
	yes := fs.Bool("yes", false, "restart even where streams get the latest epoch's rows twice")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return errors.New("rewind takes one pipeline name, after its flags")
	}
	if !*yes {
		return errors.New("restarting from the checkpoint before the latest delivers the latest epoch again: streams written with " +
			"append and not deduped get its rows twice. Run it again with --yes to go ahead")
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	name := "workspaces/" + *workspace + "/pipelines/" + fs.Arg(0)
	resp, err := c.RewindPipeline(ctx, connect.NewRequest(&apiv1.RewindPipelineRequest{Name: name}))
	if err != nil {
		return err
	}
	fmt.Fprintf(out, "%s restarts from the checkpoint before its latest if the latest can't be read.\n", name)
	if resp.Msg.GetPipeline().GetPaused() {
		fmt.Fprintf(out, "The pipeline is paused: resume it to start (openetl resume %s%s).\n", workspaceFlag(*workspace), fs.Arg(0))
	}
	return nil
}

func workspaceFlag(ws string) string {
	if ws == "default" {
		return ""
	}
	return "--workspace " + ws + " "
}
