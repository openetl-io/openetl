package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"path"
	"slices"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// How often logs -f asks for more.
var followEvery = 2 * time.Second

func runSyncs(ctx context.Context, args []string, out io.Writer) error {
	var workspace *string
	var limit *int
	conn, rest, err := subcommand("syncs", args, 1, "PIPELINE", func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		limit = fs.Int("limit", 20, "at most this many syncs")
	})
	if err != nil {
		return err
	}
	c, err := client(conn, apiv1connect.NewSyncServiceClient)
	if err != nil {
		return err
	}
	req := &apiv1.ListSyncsRequest{Parent: "workspaces/" + *workspace + "/pipelines/" + rest[0]}
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "SYNC\tSTATE\tSTARTED\tTOOK\tROWS\tVERSION\tLAST COMMIT\tLAG\tERROR")
	for left := *limit; left > 0; {
		req.PageSize = int32(min(left, 500))
		res, err := c.ListSyncs(ctx, connect.NewRequest(req))
		if err != nil {
			return err
		}
		for _, s := range res.Msg.GetSyncs() {
			took := "-"
			if s.GetEndTime() != nil {
				took = s.GetEndTime().AsTime().Sub(s.GetStartTime().AsTime()).Round(time.Second).String()
			}
			state := strings.ToLower(strings.TrimPrefix(s.GetState().String(), "SYNC_STATE_"))
			msg := s.GetError()
			if n := s.GetFailedAttempts(); n > 0 && s.GetState() == apiv1.SyncState_SYNC_STATE_RUNNING {
				state += fmt.Sprintf(", %d failed", n)
				msg = s.GetLastAttemptError()
			}
			msg, _, _ = strings.Cut(msg, "\n")
			if len(msg) > 80 {
				msg = msg[:79] + "…"
			}
			var rows uint64
			for _, st := range s.GetStreams() {
				rows += st.GetRows()
			}
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\t%s\t%s\t%s\n", path.Base(s.GetName()), state, when(s.GetStartTime()), took, rows, s.GetVersion(), when(s.GetLastCommitTime()), lag(s.GetProgress()), msg)
		}
		left -= len(res.Msg.GetSyncs())
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	return tw.Flush()
}

// lag is how far behind its source a sync's read was at its latest commit,
// the most of its streams'; "-" if the source doesn't say.
func lag(progress []*apiv1.ReadProgress) string {
	var d time.Duration
	var bytes, records uint64
	var hasD, hasBytes, hasRecords bool
	for _, p := range progress {
		if p.GetLag() != nil {
			d, hasD = max(d, p.GetLag().AsDuration()), true
		}
		if p.LagBytes != nil {
			bytes, hasBytes = max(bytes, p.GetLagBytes()), true
		}
		if p.LagRecords != nil {
			records, hasRecords = max(records, p.GetLagRecords()), true
		}
	}
	var parts []string
	if hasD {
		parts = append(parts, d.Round(100*time.Millisecond).String())
	}
	if hasBytes {
		parts = append(parts, size(bytes))
	}
	if hasRecords {
		parts = append(parts, strconv.FormatUint(records, 10)+" records")
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ", ")
}

func size(n uint64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := uint64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func runLogs(ctx context.Context, args []string, out io.Writer) error {
	var workspace, sync *string
	var follow, asJSON *bool
	conn, rest, err := subcommand("logs", args, 1, "PIPELINE", func(fs *flag.FlagSet) {
		workspace = fs.String("workspace", "default", "the pipeline's workspace")
		sync = fs.String("sync", "", "the sync's ID, from openetl syncs (default: the latest)")
		follow = fs.Bool("f", false, "keep printing the sync's log until it ends")
		asJSON = fs.Bool("json", false, "print each entry as a line of JSON")
	})
	if err != nil {
		return err
	}
	c, err := client(conn, apiv1connect.NewSyncServiceClient)
	if err != nil {
		return err
	}
	pipeline := "workspaces/" + *workspace + "/pipelines/" + rest[0]
	name := pipeline + "/syncs/" + *sync
	if *sync == "" {
		res, err := c.ListSyncs(ctx, connect.NewRequest(&apiv1.ListSyncsRequest{Parent: pipeline, PageSize: 1}))
		if err != nil {
			return err
		}
		if len(res.Msg.GetSyncs()) == 0 {
			return fmt.Errorf("%s hasn't synced yet", pipeline)
		}
		name = res.Msg.GetSyncs()[0].GetName()
	}
	req := &apiv1.ReadSyncLogRequest{Name: name}
	for {
		res, err := c.ReadSyncLog(ctx, connect.NewRequest(req))
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
			fmt.Fprintln(out, formatEntry(e))
		}
		if req.PageToken = res.Msg.GetNextPageToken(); req.PageToken == "" {
			return nil
		}
		if len(res.Msg.GetEntries()) > 0 {
			continue
		}
		if !*follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(followEvery):
		}
	}
}

// formatEntry prints an entry as its time, level, attempt, component,
// message and other fields.
func formatEntry(e *apiv1.LogEntry) string {
	var b strings.Builder
	b.WriteString(e.GetTime().AsTime().Local().Format("2006-01-02 15:04:05.000"))
	fmt.Fprintf(&b, " %-5s ", e.GetLevel())
	if e.GetPostSyncStep() > 0 {
		fmt.Fprintf(&b, "step %d ", e.GetPostSyncStep())
	}
	fmt.Fprintf(&b, "attempt %d ", e.GetAttempt())
	attrs := e.GetAttrs().AsMap()
	if c, ok := attrs["component"].(string); ok {
		b.WriteString(c + ": ")
		delete(attrs, "component")
	}
	b.WriteString(e.GetMessage())
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		b.WriteString(" " + k + "=" + formatValue(attrs[k]))
	}
	return b.String()
}

func formatValue(v any) string {
	if s, ok := v.(string); ok {
		if s == "" || strings.ContainsAny(s, " \t\n\"=") {
			return strconv.Quote(s)
		}
		return s
	}
	j, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(j)
}
