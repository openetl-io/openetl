package main

import (
	"cmp"
	"context"
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
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/platform/ints"
)

// runQuarantine lists the files a pipeline's committed epochs quarantined
// rows to, newest first, then sums their rows by cause; or, with show,
// prints a file's first lines.
func runQuarantine(ctx context.Context, args []string, out io.Writer) error {
	if len(args) > 0 && args[0] == "show" {
		return showQuarantine(ctx, args[1:], out, os.Stderr)
	}
	if len(args) > 0 && args[0] == "list" {
		args = args[1:]
	}
	var workspace, stream, since *string
	var limit *int
	var asJSON *bool
	conn, rest, err := subcommand("quarantine", args, 1, "PIPELINE", func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		stream = fs.String("stream", "", "only this stream's files, or this transform output's")
		since = fs.String("since", "", "only files committed in the last 24h, 7d, …")
		limit = fs.Int("limit", 100, "at most this many files, the newest")
		asJSON = fs.Bool("json", false, "print each file as a line of JSON, with its causes")
	})
	if err != nil {
		return err
	}
	if *limit <= 0 {
		return fmt.Errorf("--limit %d isn't positive", *limit)
	}
	req := &apiv1.ListQuarantineRequest{Parent: "workspaces/" + *workspace + "/pipelines/" + rest[0], Stream: *stream}
	if *since != "" {
		d, err := parseDuration(*since)
		if err != nil || d <= 0 {
			return fmt.Errorf("--since %q isn't a positive duration", *since)
		}
		req.Since = timestamppb.New(time.Now().Add(-d))
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	var files []*apiv1.QuarantineFile
	more := false
	for {
		req.PageSize = ints.Int32(min(*limit-len(files), 500))
		res, err := c.ListQuarantine(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		files = append(files, res.Msg.GetFiles()...)
		req.PageToken = res.Msg.GetNextPageToken()
		if req.PageToken == "" {
			break
		}
		if len(files) >= *limit {
			more = true
			break
		}
	}
	if *asJSON {
		for _, f := range files {
			b, err := protojson.Marshal(f)
			if err != nil {
				return err
			}
			if _, err := fmt.Fprintf(out, "%s\n", b); err != nil {
				return err
			}
		}
		return nil
	}
	if len(files) == 0 {
		fmt.Fprintln(out, "nothing quarantined")
		return nil
	}
	return printQuarantine(out, files, more)
}

// printQuarantine prints a table of the files, then their rows by stream
// and cause, the most first.
func printQuarantine(out io.Writer, files []*apiv1.QuarantineFile, more bool) error {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "COMMITTED\tSTREAM\tEPOCH\tROWS\tPATH")
	type key struct{ stream, code, column, notHeld string }
	rows := map[key]uint64{}
	for _, f := range files {
		name := f.GetStream()
		if f.GetOutput() {
			name += " (output)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%d/%d\t%d\t%s\n", f.GetCommitTime().AsTime().Local().Format(time.DateTime), name,
			f.GetGeneration(), f.GetEpoch(), f.GetRows(), f.GetPath())
		for _, c := range f.GetCauses() {
			rows[key{name, c.GetCode(), c.GetColumn(), c.GetNotHeld()}] += c.GetRows()
		}
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if more {
		fmt.Fprintf(out, "\nThe newest %d files; --limit or --since lists others.\n", len(files))
	}
	if base := quarantineBase(files[0]); base != "" {
		fmt.Fprintf(out, "\nPaths are below %s in the data plane's storage.\n", base)
	}
	keys := slices.Collect(maps.Keys(rows))
	slices.SortFunc(keys, func(a, b key) int {
		return cmp.Or(strings.Compare(a.stream, b.stream), cmp.Compare(rows[b], rows[a]),
			strings.Compare(a.code, b.code), strings.Compare(a.column, b.column), strings.Compare(a.notHeld, b.notHeld))
	})
	if len(keys) == 0 {
		return nil
	}
	fmt.Fprintln(out, "\nBy cause:")
	tw = tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "STREAM\tROWS\tCAUSE")
	for _, k := range keys {
		fmt.Fprintf(tw, "%s\t%d\t%s\n", k.stream, rows[k], causeText(&apiv1.QuarantineCause{Code: k.code, Column: k.column, NotHeld: k.notHeld}))
	}
	return tw.Flush()
}

// causeText is a cause as a phrase: its code, the column, and why the rows
// weren't held, such as "type on amount" or "object, not held: record".
func causeText(c *apiv1.QuarantineCause) string {
	s := c.GetCode()
	if c.GetColumn() != "" {
		s += " on " + c.GetColumn()
	}
	if c.GetNotHeld() != "" {
		s += ", not held: " + c.GetNotHeld()
	}
	return s
}

// quarantineBase is the pipeline's _quarantine directory, from a file's URL
// and path.
func quarantineBase(f *apiv1.QuarantineFile) string {
	base, ok := strings.CutSuffix(f.GetUrl(), "/"+f.GetPath())
	if !ok {
		return ""
	}
	return base + "/"
}

// showQuarantine prints a quarantine file's first lines to out, as the file
// has them, so they pipe to jq, and says on notes when the file has more.
func showQuarantine(ctx context.Context, args []string, out, notes io.Writer) error {
	var workspace *string
	var limit *int
	conn, rest, err := subcommand("quarantine show", args, 2, "PIPELINE PATH", func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		limit = fs.Int("limit", 20, "at most this many lines, from the first; at most 100")
	})
	if err != nil {
		return err
	}
	if *limit <= 0 || *limit > 100 {
		return fmt.Errorf("--limit %d isn't from 1 to 100", *limit)
	}
	c, err := client(conn, apiv1connect.NewPipelineServiceClient)
	if err != nil {
		return err
	}
	res, err := c.ReadQuarantineFile(ctx, connect.NewRequest(&apiv1.ReadQuarantineFileRequest{
		Parent: "workspaces/" + *workspace + "/pipelines/" + rest[0], Path: rest[1], Limit: ints.Int32(*limit),
	}))
	if err != nil {
		return err
	}
	for _, l := range res.Msg.GetLines() {
		if _, err := fmt.Fprintln(out, l); err != nil {
			return err
		}
	}
	if res.Msg.GetMore() {
		fmt.Fprintf(notes, "the file has more lines than these %d; read it from the data plane's storage for the rest\n", len(res.Msg.GetLines()))
	}
	return nil
}
