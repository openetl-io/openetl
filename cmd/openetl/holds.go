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

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// runHolds lists a pipeline's holds, or releases or discards one.
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
		return listHolds(ctx, conn, *workspace, rest[0], out)
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

// listHolds prints a table of the pipeline's holds, then each one's reason
// and the ways to clear it.
func listHolds(ctx context.Context, conn *api, workspace, pipeline string, out io.Writer) error {
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	res, err := c.ListHolds(ctx, connect.NewRequest(&apiv1.ListHoldsRequest{Parent: "workspaces/" + workspace + "/pipelines/" + pipeline}))
	if err != nil {
		return err
	}
	holds := res.Msg.GetHolds()
	if len(holds) == 0 {
		fmt.Fprintln(out, "nothing held")
		return nil
	}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "STREAM\tCOLUMN\tHELD\tCELLS\tSINCE\tAGE\tSTATE")
	for _, h := range holds {
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
		case h.GetRefused() != nil:
			state = "refused"
		}
		since := fmt.Sprintf("%d/%d", h.GetSinceGeneration(), h.GetSinceEpoch())
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n", h.GetStream(), h.GetColumn(), held, cells, since, age, state)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	ws := ""
	if workspace != "default" {
		ws = "--workspace " + workspace + " "
	}
	for _, h := range holds {
		what := h.GetStream() + "." + h.GetColumn()
		if !h.GetWhole() {
			what += " (cells)"
		}
		fmt.Fprintf(out, "\n%s\n  %s\n", what, h.GetReason())
		if r := h.GetRefused(); r != nil {
			fmt.Fprintf(out, "  Its last release didn't clear it: %s. Release it again once that's fixed.\n", r.GetReason())
		}
		if len(h.GetWaysOut()) > 0 {
			fmt.Fprintln(out, "  To clear it:")
		}
		for _, w := range h.GetWaysOut() {
			cmd := holdCommand(w.GetAction(), ws, pipeline, h)
			if cmd == "" {
				continue
			}
			if w.GetFirst() == "" {
				fmt.Fprintf(out, "  - %s\n    %s\n", cmd, w.GetEffect())
				continue
			}
			fmt.Fprintf(out, "  - %s\n", w.GetFirst())
			if w.GetStatement() != "" {
				fmt.Fprintf(out, "      %s\n", w.GetStatement())
			}
			fmt.Fprintf(out, "    then: %s\n", cmd)
		}
	}
	return nil
}

func holdCommand(a apiv1.HoldWayOut_Action, ws, pipeline string, h *apiv1.Hold) string {
	var sub string
	switch a {
	case apiv1.HoldWayOut_ACTION_RELEASE:
		sub = "release"
	case apiv1.HoldWayOut_ACTION_RELEASE_NEW_VERSION:
		sub = "release --new-version"
	case apiv1.HoldWayOut_ACTION_DISCARD:
		sub = "discard"
	default:
		return ""
	}
	return fmt.Sprintf("openetl holds %s %s%s %s %s", sub, ws, pipeline, h.GetStream(), h.GetColumn())
}
