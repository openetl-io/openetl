package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// runHolds lists a pipeline's holds (ADR-0025), or releases or discards one.
func runHolds(ctx context.Context, args []string, out io.Writer) error {
	sub := "list"
	if len(args) > 0 && (args[0] == "list" || args[0] == "release" || args[0] == "discard") {
		sub, args = args[0], args[1:]
	}
	var workspace *string
	var newVersion *bool
	flags := func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		if sub == "release" {
			newVersion = fs.Bool("new-version", false, "send a column held whole to a new version of it, COLUMN__v2, instead of the column, which keeps its type")
		}
	}
	if sub == "list" {
		conn, rest, err := subcommand("holds", args, 1, "PIPELINE", flags)
		if err != nil {
			return err
		}
		return listHolds(ctx, conn, "workspaces/"+*workspace+"/pipelines/"+rest[0], out)
	}
	conn, rest, err := subcommand("holds "+sub, args, 3, "PIPELINE STREAM COLUMN", flags)
	if err != nil {
		return err
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	parent := "workspaces/" + *workspace + "/pipelines/" + rest[0]
	var h *apiv1.Hold
	if sub == "release" {
		res, err := c.ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: parent, Stream: rest[1], Column: rest[2], NewVersion: *newVersion}))
		if err != nil {
			return err
		}
		h = res.Msg.GetHold()
		into := ""
		if *newVersion {
			into = " into a new version of the column"
		}
		fmt.Fprintf(out, "released %s.%s of %s; the pipeline's next run patches its held values in%s\n", h.GetStream(), h.GetColumn(), parent, into)
		return nil
	}
	res, err := c.DiscardHold(ctx, connect.NewRequest(&apiv1.DiscardHoldRequest{Parent: parent, Stream: rest[1], Column: rest[2]}))
	if err != nil {
		return err
	}
	h = res.Msg.GetHold()
	fmt.Fprintf(out, "discarded %s.%s of %s; the pipeline's next run drops its held values\n", h.GetStream(), h.GetColumn(), parent)
	return nil
}

func listHolds(ctx context.Context, conn *api, parent string, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	res, err := c.ListHolds(ctx, connect.NewRequest(&apiv1.ListHoldsRequest{Parent: parent}))
	if err != nil {
		return err
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "STREAM\tCOLUMN\tHELD\tCELLS\tSINCE\tAGE\tSTATE\tREASON")
	for _, h := range res.Msg.GetHolds() {
		held, cells := "cells", strconv.FormatUint(h.GetCells(), 10)
		if h.GetWhole() {
			held, cells = "column", "-"
		}
		age := "-"
		if h.GetStartTime() != nil {
			age = time.Since(h.GetStartTime().AsTime()).Round(time.Second).String()
		}
		state := "held"
		switch {
		case h.GetDiscarded():
			state = "discarded"
		case h.GetReleasing():
			state = "releasing"
		case h.GetPending():
			state = "pending"
		}
		since := fmt.Sprintf("%d/%d", h.GetSinceGeneration(), h.GetSinceEpoch())
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", h.GetStream(), h.GetColumn(), held, cells, since, age, state, h.GetReason())
	}
	return tw.Flush()
}
