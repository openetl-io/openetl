package api

import (
	"testing"

	"github.com/google/uuid"

	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
)

// An output's schema versions are its own: each is recorded once, apart
// from those of the stream that shares its name.
func TestSchemaEventKeysKeepOutputsApart(t *testing.T) {
	p := uuid.New()
	in := schemaEventKey(p, streamSchema{stream: "customers"}, 2)
	out := schemaEventKey(p, streamSchema{stream: "customers", output: true}, 2)
	if in == out || in != "schema.changed/"+p.String()+"/customers/2" {
		t.Errorf("keys %q and %q", in, out)
	}
}

// A SQL transform's outputs hold, and the source's streams don't: the
// pipeline's holds are the streams' and the outputs', each by its name.
func TestHoldRowsTakeOutputs(t *testing.T) {
	m := &dataplanev1.ReportCheckpointRequest{
		Streams: []*dataplanev1.CheckpointStream{
			{Name: "orders", Holds: []*dataplanev1.CheckpointHold{{Column: "x", Cells: 2}}},
			{Name: "customers"},
		},
		Outputs: []*dataplanev1.CheckpointOutput{{Name: "customers", Holds: []*dataplanev1.CheckpointHold{{Column: "n", Whole: true}}}},
	}
	var b bigints
	rows, err := (&Server{}).holdRows(uuid.New(), m, &b)
	if err != nil || b.err != nil {
		t.Fatal(err, b.err)
	}
	if len(rows) != 2 || rows[0].Stream != "orders" || rows[0].ColumnName != "x" ||
		rows[1].Stream != "customers" || rows[1].ColumnName != "n" || !rows[1].Whole {
		t.Errorf("rows %+v", rows)
	}
}
