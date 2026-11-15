//go:build integration

package api_test

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
)

const quarantineBase = "s3://lake/org/pipe/_quarantine/"

func quarantineFile(path string, rows uint64, causes ...*apiv1.QuarantineCause) *dataplanev1.CheckpointQuarantineFile {
	return &dataplanev1.CheckpointQuarantineFile{Path: path, Url: quarantineBase + path, Rows: rows, Causes: causes}
}

func (e *holdsEnv) reportQuarantine(t *testing.T, epoch uint64, at time.Time, rows uint64, streams []*dataplanev1.CheckpointStream, outputs ...*dataplanev1.CheckpointOutput) {
	t.Helper()
	for _, st := range streams {
		if st.Rows == 0 {
			st.Rows = rows
		}
		for _, f := range st.GetQuarantineFiles() {
			st.QuarantinedRows += f.GetRows()
		}
	}
	_, err := e.runner.ReportCheckpoint(t.Context(), connect.NewRequest(&dataplanev1.ReportCheckpointRequest{
		PipelineId: e.pipeline.String(), RunId: "run1", Generation: 2, Epoch: epoch, CreateTime: timestamppb.New(at),
		Streams: streams, Outputs: outputs,
	}))
	if err != nil {
		t.Fatal(err)
	}
}

func (e *holdsEnv) quarantineEvents(t *testing.T) []map[string]any {
	t.Helper()
	rows, err := e.conn.Query(t.Context(), `SELECT data FROM outbox WHERE type = 'stream.quarantined' ORDER BY id`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		out = append(out, m)
	}
	return out
}

// A committed checkpoint's quarantine files are indexed, listed newest
// first with their causes, and sent once each in a stream.quarantined
// event, however often the checkpoint is reported.
func TestQuarantineIsIndexed(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	e.reportQuarantine(t, 4, now.Add(-time.Minute), 1000, []*dataplanev1.CheckpointStream{
		{Name: "orders", QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{quarantineFile("2/4/orders.jsonl", 2, &apiv1.QuarantineCause{Code: "type", Column: "amount", Rows: 2})}},
	})
	report := func() {
		e.reportQuarantine(t, 5, now, 1000, []*dataplanev1.CheckpointStream{
			{Name: "orders", QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{
				quarantineFile("2/5/orders.jsonl", 3,
					&apiv1.QuarantineCause{Code: "type", Column: "id", NotHeld: "key", Rows: 1},
					&apiv1.QuarantineCause{Code: "type", Column: "amount", Rows: 2}),
				quarantineFile("2/5/orders.1.jsonl", 1, &apiv1.QuarantineCause{Code: "json", Rows: 1}),
			}},
			{Name: "users"},
		}, &dataplanev1.CheckpointOutput{Name: "daily", QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{
			quarantineFile("_outputs/2/5/daily.jsonl", 4, &apiv1.QuarantineCause{Code: "statement", Rows: 4}),
		}})
	}
	report()
	report()

	res, err := e.viewer.ListQuarantine(ctx, connect.NewRequest(&apiv1.ListQuarantineRequest{Parent: e.parent}))
	if err != nil {
		t.Fatal(err)
	}
	files := res.Msg.GetFiles()
	var paths []string
	for _, f := range files {
		paths = append(paths, f.GetPath())
	}
	if got, want := strings.Join(paths, " "), "_outputs/2/5/daily.jsonl 2/5/orders.jsonl 2/5/orders.1.jsonl 2/4/orders.jsonl"; got != want {
		t.Fatalf("files %s, want %s", got, want)
	}
	f := files[1]
	switch {
	case f.GetStream() != "orders" || f.GetOutput() || f.GetUrl() != quarantineBase+"2/5/orders.jsonl" || f.GetRows() != 3 ||
		f.GetGeneration() != 2 || f.GetEpoch() != 5 || !f.GetCommitTime().AsTime().Equal(now) || !strings.HasPrefix(f.GetSync(), e.parent+"/syncs/"):
		t.Errorf("file %v", f)
	case len(f.GetCauses()) != 2 || f.GetCauses()[0].GetColumn() != "amount" || f.GetCauses()[1].GetNotHeld() != "key":
		t.Errorf("causes %v, want the most rows first", f.GetCauses())
	}
	if d := files[0]; d.GetStream() != "daily" || !d.GetOutput() {
		t.Errorf("output's file %v", d)
	}

	// Paged, filtered by stream and by time.
	var paged []string
	req := &apiv1.ListQuarantineRequest{Parent: e.parent, PageSize: 1}
	for {
		r, err := e.viewer.ListQuarantine(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range r.Msg.GetFiles() {
			paged = append(paged, f.GetPath())
		}
		if req.PageToken = r.Msg.GetNextPageToken(); req.PageToken == "" {
			break
		}
	}
	if strings.Join(paged, " ") != strings.Join(paths, " ") {
		t.Errorf("pages %v, want %v", paged, paths)
	}
	r, err := e.viewer.ListQuarantine(ctx, connect.NewRequest(&apiv1.ListQuarantineRequest{Parent: e.parent, Stream: "orders", Since: timestamppb.New(now.Add(-time.Second))}))
	if err != nil {
		t.Fatal(err)
	}
	if n := len(r.Msg.GetFiles()); n != 2 {
		t.Errorf("orders' files since a second ago: %d, want 2", n)
	}

	events := e.quarantineEvents(t)
	if len(events) != 3 {
		t.Fatalf("%d stream.quarantined events, want one per stream and epoch: %v", len(events), events)
	}
	ev := events[1]
	files5, _ := ev["files"].([]any)
	if ev["pipeline"] != e.parent || ev["stream"] != "orders" || ev["epoch"] != float64(5) || ev["rows"] != float64(4) || len(files5) != 2 ||
		!strings.HasPrefix(ev["sync"].(string), e.parent+"/syncs/") {
		t.Errorf("event %v", ev)
	}
	if out := events[2]; out["stream"] != "daily" || out["output"] != true {
		t.Errorf("output's event %v", out)
	}
}

// Files older than the pipeline's quarantine.retention leave the index;
// without one, they stay.
func TestQuarantineRetentionDropsOldFiles(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	now := time.Now().UTC()
	old := []*dataplanev1.CheckpointStream{{Name: "orders", QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{quarantineFile("2/1/orders.jsonl", 1)}}}
	e.reportQuarantine(t, 1, now.Add(-3*time.Hour), 1000, old)
	e.reportQuarantine(t, 2, now, 1000, []*dataplanev1.CheckpointStream{{Name: "orders"}})
	count := func() int {
		t.Helper()
		res, err := e.viewer.ListQuarantine(ctx, connect.NewRequest(&apiv1.ListQuarantineRequest{Parent: e.parent}))
		if err != nil {
			t.Fatal(err)
		}
		return len(res.Msg.GetFiles())
	}
	if n := count(); n != 1 {
		t.Fatalf("%d files without a retention, want 1", n)
	}
	if _, err := e.conn.Exec(ctx, `UPDATE pipeline_versions SET spec = spec || '{"quarantine": {"retention": "3600s"}}'`); err != nil {
		t.Fatal(err)
	}
	e.reportQuarantine(t, 3, now, 1000, []*dataplanev1.CheckpointStream{{Name: "orders"}})
	if n := count(); n != 0 {
		t.Errorf("%d files past an hour's retention are listed", n)
	}
}

// The quarantine incident names the stream's files in its window.
func TestQuarantineIncidentNamesItsFiles(t *testing.T) {
	ctx := t.Context()
	e := newHoldsEnv(t)
	now := time.Now().UTC()
	for epoch := uint64(1); epoch <= 12; epoch++ {
		e.reportQuarantine(t, epoch, now.Add(time.Duration(epoch)*time.Second), 1000, []*dataplanev1.CheckpointStream{
			{Name: "orders", QuarantineFiles: []*dataplanev1.CheckpointQuarantineFile{quarantineFile("2/"+strconv.FormatUint(epoch, 10)+"/orders.jsonl", 20)}},
		})
	}
	var details map[string]string
	var next string
	err := e.conn.QueryRow(ctx, `SELECT details, next_steps FROM incidents WHERE class = 'quarantine' AND stream = 'orders'`).Scan(&details, &next)
	if err != nil {
		t.Fatal(err)
	}
	if details["quarantine_files"] != "12" || !strings.HasPrefix(details["quarantine_paths"], "2/12/orders.jsonl, 2/11/orders.jsonl") ||
		strings.Count(details["quarantine_paths"], ", ") != 9 {
		t.Errorf("details %v", details)
	}
	if !strings.Contains(next, "openetl quarantine --stream orders orders") {
		t.Errorf("next steps %q", next)
	}
}
