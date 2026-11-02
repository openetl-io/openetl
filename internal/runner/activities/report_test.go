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

	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/streamschema"
	"github.com/unsuitable001/openetl/sdk/go/source"
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

	r := &reporter{pipeline: "p", runID: "run", log: slog.New(slog.DiscardHandler), digests: map[string]string{}}
	created := time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)
	req := r.mirrored(&checkpoint.Record{
		Epoch: 7, Generation: 3, Previous: &checkpoint.RecordRef{Epoch: 6, Generation: 2}, CreatedAt: created,
		Streams: map[string]checkpoint.StreamState{
			"users":  {Schema: schema, Rows: 10, Complete: true},
			"orders": {Rows: 4},
		},
		Quarantine: []checkpoint.QuarantineFile{{Stream: "users", Rows: 2}, {Stream: "users", Rows: 1}},
		Forced:     []string{"orders"},
	}, map[string]uint64{"users": 5}, []source.Progress{{LagBytes: new(uint64(4096)), Lag: new(1500 * time.Millisecond)}})
	if req.GetPipelineId() != "p" || req.GetRunId() != "run" || req.GetEpoch() != 7 || req.GetGeneration() != 3 ||
		req.GetPrevious().GetEpoch() != 6 || req.GetPrevious().GetGeneration() != 2 || !req.GetCreateTime().AsTime().Equal(created) {
		t.Errorf("request %v", req)
	}
	streams := req.GetStreams()
	slices.SortFunc(streams, func(a, b *dataplanev1.CheckpointStream) int { return strings.Compare(a.GetName(), b.GetName()) })
	if len(streams) != 2 {
		t.Fatalf("streams %v", streams)
	}
	if o := streams[0]; o.GetName() != "orders" || o.GetRows() != 4 || !o.GetForced() || o.GetComplete() || o.GetQuarantinedRows() != 0 || o.GetSchemaDigest() != "" || o.GetCollapsedRows() != 0 {
		t.Errorf("orders: %v", o)
	}
	if u := streams[1]; u.GetName() != "users" || u.GetRows() != 10 || u.GetForced() || !u.GetComplete() || u.GetQuarantinedRows() != 3 || u.GetSchemaDigest() != want.Digest || u.GetCollapsedRows() != 5 {
		t.Errorf("users: %v, want digest %s", u, want.Digest)
	}
	if p := req.GetProgress(); len(p) != 1 || p[0].GetStream() != "" || p[0].GetLagBytes() != 4096 || p[0].LagRecords != nil ||
		p[0].GetLag().AsDuration() != 1500*time.Millisecond || p[0].RetentionLeft != nil {
		t.Errorf("progress %v", p)
	}
}
