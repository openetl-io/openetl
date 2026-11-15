package activities

import (
	"bytes"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/delivery"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/streamschema"
	"github.com/openetl-io/openetl/sdk/go/source"
)

func TestMirroredCheckpoint(t *testing.T) {
	var buf bytes.Buffer
	md := arrow.NewMetadata([]string{"openetl.table"}, []string{"users"})
	w := ipc.NewWriter(&buf, ipc.WithSchema(arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, &md)))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	schema := buf.Bytes()
	want, err := streamschema.Describe(schema)
	if err != nil {
		t.Fatal(err)
	}

	r := &reporter{pipeline: "p", runID: "run", log: slog.New(slog.DiscardHandler), digests: map[string]string{}, lowered: map[string]runner.Lowered{
		"orders": {Level: delivery.AtLeastOnce, Planned: delivery.ExactlyOnce},
		"users":  {Level: delivery.AtLeastOnce, Planned: delivery.ExactlyOnce},
	}}
	created := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	req := r.mirrored(&checkpoint.Record{
		Epoch: 7, Generation: 3, Previous: &checkpoint.RecordRef{Epoch: 6, Generation: 2}, CreatedAt: created,
		Streams: map[string]checkpoint.StreamState{
			"users":  {Schema: schema, Rows: 10, Complete: true},
			"orders": {Rows: 4},
		},
		Quarantine: []checkpoint.QuarantineFile{{Stream: "users", Rows: 2}, {Stream: "users", Rows: 1}},
		Forced:     []string{"orders"},
		Reread:     []string{"orders"},
		SyncRows:   map[string]uint64{"users": 25},
	}, map[string]uint64{"users": 5}, nil, []source.Progress{{LagBytes: new(uint64(4096)), Lag: new(1500 * time.Millisecond)}})
	if req.GetPipelineId() != "p" || req.GetRunId() != "run" || req.GetEpoch() != 7 || req.GetGeneration() != 3 ||
		req.GetPrevious().GetEpoch() != 6 || req.GetPrevious().GetGeneration() != 2 || !req.GetCreateTime().AsTime().Equal(created) {
		t.Errorf("request %v", req)
	}
	streams := req.GetStreams()
	slices.SortFunc(streams, func(a, b *dataplanev1.CheckpointStream) int { return strings.Compare(a.GetName(), b.GetName()) })
	if len(streams) != 2 {
		t.Fatalf("streams %v", streams)
	}
	if o := streams[0]; o.GetName() != "orders" || o.GetRows() != 4 || !o.GetForced() || o.GetComplete() || o.GetQuarantinedRows() != 0 || o.GetSchemaDigest() != "" || o.GetCollapsedRows() != 0 ||
		o.GetLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE || o.GetPlannedLevel() != apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE {
		t.Errorf("orders: %v", o)
	}
	// It would be lowered, but read nothing again.
	if u := streams[1]; u.GetName() != "users" || u.GetRows() != 10 || u.GetForced() || !u.GetComplete() || u.GetQuarantinedRows() != 3 || u.GetSchemaDigest() != want.Digest || u.GetCollapsedRows() != 5 ||
		u.GetLevel() != 0 || u.GetPlannedLevel() != 0 {
		t.Errorf("users: %v, want digest %s", u, want.Digest)
	}
	if streams[0].SyncRows != nil || streams[1].GetSyncRows() != 25 {
		t.Errorf("sync rows: orders %v, users %v", streams[0].SyncRows, streams[1].SyncRows)
	}
	if p := req.GetProgress(); len(p) != 1 || p[0].GetStream() != "" || p[0].GetLagBytes() != 4096 || p[0].LagRecords != nil ||
		p[0].GetLag().AsDuration() != 1500*time.Millisecond || p[0].RetentionLeft != nil {
		t.Errorf("progress %v", p)
	}
}

// Every quarantine file of the epoch is reported, a stream's on the stream
// and an output's on the output, by its path below the pipeline's
// _quarantine directory, with its causes.
func TestMirroredQuarantineFiles(t *testing.T) {
	const q = "s3://lake/org/pipe/_quarantine"
	r := &reporter{pipeline: "p", runID: "run", log: slog.New(slog.DiscardHandler), digests: map[string]string{}}
	req := r.mirrored(&checkpoint.Record{
		Epoch: 7, Generation: 3,
		Streams: map[string]checkpoint.StreamState{"users": {Rows: 10}, "orders": {Rows: 4}},
		Outputs: map[string]checkpoint.OutputState{"daily": {}},
		Quarantine: []checkpoint.QuarantineFile{
			{Stream: "users", URL: q + "/3/7/users.jsonl?region=x", Rows: 2, Causes: []checkpoint.QuarantineCause{{Code: "type", Column: "age", Rows: 2}}},
			{Stream: "users", URL: q + "/3/7/users.1.jsonl", Rows: 1, Causes: []checkpoint.QuarantineCause{{Code: "object", NotHeld: "record", Rows: 1}}},
			{Stream: "daily", Output: true, URL: q + "/_outputs/3/7/daily.jsonl", Rows: 4, Causes: []checkpoint.QuarantineCause{{Code: "statement", Rows: 4}}},
		},
	}, nil, nil, nil)
	var users *dataplanev1.CheckpointStream
	for _, st := range req.GetStreams() {
		if st.GetName() == "users" {
			users = st
		} else if len(st.GetQuarantineFiles()) > 0 {
			t.Errorf("%s has files: %v", st.GetName(), st.GetQuarantineFiles())
		}
	}
	files := users.GetQuarantineFiles()
	if len(files) != 2 || users.GetQuarantinedRows() != 3 {
		t.Fatalf("users: %v", users)
	}
	if f := files[0]; f.GetPath() != "3/7/users.jsonl" || f.GetUrl() != q+"/3/7/users.jsonl" || f.GetRows() != 2 ||
		len(f.GetCauses()) != 1 || f.GetCauses()[0].GetCode() != "type" || f.GetCauses()[0].GetColumn() != "age" || f.GetCauses()[0].GetRows() != 2 {
		t.Errorf("first file %v", f)
	}
	if c := files[1].GetCauses(); files[1].GetPath() != "3/7/users.1.jsonl" || len(c) != 1 || c[0].GetNotHeld() != "record" {
		t.Errorf("second file %v", files[1])
	}
	outputs := req.GetOutputs()
	if len(outputs) != 1 || outputs[0].GetName() != "daily" || outputs[0].GetHolds() != nil {
		t.Fatalf("outputs %v", outputs)
	}
	if f := outputs[0].GetQuarantineFiles(); len(f) != 1 || f[0].GetPath() != "_outputs/3/7/daily.jsonl" || f[0].GetCauses()[0].GetCode() != "statement" {
		t.Errorf("daily's files %v", f)
	}
}

func TestMirroredHolds(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	rec := func(cells uint64, replay bool) *checkpoint.Record {
		h := &checkpoint.Holds{
			Columns: []checkpoint.ColumnHold{{Column: "y", Reason: "new", Since: since, Discarded: true}},
			Cells:   map[string]checkpoint.CellHold{"x": {Cells: cells, Reason: "nan", Since: since}},
		}
		if replay {
			h.Replay = &checkpoint.Replay{Columns: []string{"x", "z"}}
		}
		return &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: h}, "t": {}}}
	}
	r := &reporter{}
	if got := r.heldCells(rec(3, false)); got["s"] != 3 || got["t"] != 0 {
		t.Errorf("held cells %v", got)
	}
	if got := r.heldCells(rec(5, true)); got["s"] != 2 {
		t.Errorf("held cells %v, want the epoch's 2", got)
	}
	holds := mirroredHolds(rec(5, true).Streams["s"].Holds)
	if len(holds) != 3 {
		t.Fatalf("holds %v", holds)
	}
	y, x, z := holds[0], holds[1], holds[2]
	switch {
	case y.GetColumn() != "y" || !y.GetWhole() || !y.GetDiscarded() || y.GetSince().GetEpoch() != 2:
		t.Errorf("y %v", y)
	case x.GetColumn() != "x" || x.GetWhole() || x.GetCells() != 5 || !x.GetReleasing():
		t.Errorf("x %v", x)
	case z.GetColumn() != "z" || !z.GetWhole() || !z.GetReleasing():
		t.Errorf("z %v: a released column held whole", z)
	}
	if mirroredHolds(nil) != nil {
		t.Error("holds for a stream that holds nothing")
	}
}

// A SQL transform's outputs hold, and the source's streams don't: the
// mirror has an output's holds apart, though it shares a stream's name.
func TestMirroredOutputHolds(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	rec := func(cells uint64) *checkpoint.Record {
		h := &checkpoint.Holds{
			Columns: []checkpoint.ColumnHold{{Column: "n", Reason: "changed", Since: since}},
			Cells:   map[string]checkpoint.CellHold{"x": {Cells: cells, Reason: "nan", Since: since}},
		}
		return &checkpoint.Record{
			Streams: map[string]checkpoint.StreamState{"s": {Rows: 4}, "t": {}},
			Outputs: map[string]checkpoint.OutputState{"s": {Holds: h}, "out": {}},
		}
	}
	r := &reporter{}
	if got := r.heldCells(rec(3)); got["s"] != 3 {
		t.Errorf("held cells %v", got)
	}
	req := r.mirrored(rec(5), nil, r.heldCells(rec(5)), nil)
	for _, st := range req.GetStreams() {
		if len(st.GetHolds()) > 0 || st.GetHeldCells() > 0 {
			t.Errorf("stream %s holds %v", st.GetName(), st.GetHolds())
		}
	}
	outs := req.GetOutputs()
	if len(outs) != 1 || outs[0].GetName() != "s" || len(outs[0].GetHolds()) != 2 || outs[0].GetHeldCells() != 2 {
		t.Errorf("outputs %v", outs)
	}
}
