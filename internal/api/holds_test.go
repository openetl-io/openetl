package api

import (
	"strings"
	"testing"

	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/store/db"
)

// An output's schema versions are its own: each is recorded once, apart
// from those of the stream that shares its name, and of a stream whose
// name looks like an output's key.
func TestSchemaEventKeysKeepOutputsApart(t *testing.T) {
	p := uuid.New()
	key := func(stream string, output bool, version int32) string {
		return schemaEventKey(p, streamSchema{stream: stream, output: output}, version)
	}
	out := key("customers", true, 2)
	if out != "schema.changed/"+p.String()+"/output/customers/2" {
		t.Errorf("output key %q", out)
	}
	for _, other := range []string{
		key("customers", false, 2),
		key("output/customers", false, 2),
		key("input/output/customers", false, 2),
		key("output/customers", true, 2),
		key("customers/2", true, 2),
	} {
		if other == out {
			t.Errorf("%q is also an output's key", other)
		}
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

// A discarded column hold drops the column's values until it's released:
// a release is its way out, and pends until the pipeline's next run, while
// the discard it took doesn't.
func TestADiscardedColumnHoldsWayOutIsARelease(t *testing.T) {
	cause, err := protojson.Marshal(&apiv1.HoldCause{Kind: apiv1.HoldCause_KIND_REFUSED, From: "int64", To: "utf8", Refusal: "it doesn't change a column's type", Fix: "ALTER v"})
	if err != nil {
		t.Fatal(err)
	}
	id, discards, releases := uuid.New(), true, false
	row := db.ListHoldsRow{Stream: "s", ColumnName: "v", Whole: true, Discarded: true, Cause: cause, ReleaseID: &id, ReleaseDiscards: &discards}
	h, err := hold(row)
	if err != nil {
		t.Fatal(err)
	}
	if h.GetPending() || len(h.GetWaysOut()) != 1 {
		t.Fatalf("discarded: pending %v, ways out %v", h.GetPending(), h.GetWaysOut())
	}
	w := h.GetWaysOut()[0]
	if w.GetAction() != apiv1.HoldWayOut_ACTION_RELEASE || w.GetStatement() != "ALTER v" || !strings.Contains(w.GetEffect(), "Delivers the column again") {
		t.Errorf("way out %v", w)
	}
	row.ReleaseDiscards = &releases
	if h, err = hold(row); err != nil || !h.GetPending() {
		t.Errorf("released after its discard, pending %v, %v", h.GetPending(), err)
	}
}

// A discard replaces a release of a hold not discarded that waits for the
// next run, and a release replaces the discard a discarded hold took.
// Neither replaces one of its own kind, nor a discard the release that ends
// a discarded hold, or one being patched in, which it ends instead.
func TestWhatAReleaseReplaces(t *testing.T) {
	id, yes, no := uuid.New(), true, false
	for _, tc := range []struct {
		name    string
		row     db.ListHoldsRow
		discard bool
		want    bool
	}{
		{"a discard, a release pending", db.ListHoldsRow{ReleaseID: &id, ReleaseDiscards: &no}, true, true},
		{"a discard, a release being patched in", db.ListHoldsRow{Releasing: true, ReleaseID: &id, ReleaseDiscards: &no}, true, false},
		{"a discard, the release that ends a discarded hold", db.ListHoldsRow{Discarded: true, ReleaseID: &id, ReleaseDiscards: &no}, true, false},
		{"a discard, a discard pending", db.ListHoldsRow{ReleaseID: &id, ReleaseDiscards: &yes}, true, false},
		{"a discard, nothing", db.ListHoldsRow{}, true, false},
		{"a release, the discard a discarded hold took", db.ListHoldsRow{Discarded: true, ReleaseID: &id, ReleaseDiscards: &yes}, false, true},
		{"a release, a discard pending", db.ListHoldsRow{ReleaseID: &id, ReleaseDiscards: &yes}, false, false},
		{"a release, a release pending", db.ListHoldsRow{ReleaseID: &id, ReleaseDiscards: &no}, false, false},
	} {
		if got := replacedBy(tc.row, tc.discard); got != tc.want {
			t.Errorf("%s: replaced %v, want %v", tc.name, got, tc.want)
		}
	}
}

// A release a person discarded while it was being patched in, which ended
// with values left held, says that the discard dropped nothing, and the
// hold isn't pending: the discard lapsed with the release.
func TestARefusedReleaseSaysItsDiscardLapsed(t *testing.T) {
	id, no, yes := uuid.New(), false, true
	row := db.ListHoldsRow{Stream: "s", ColumnName: "x", Cells: 2, Reason: "nan", ReleaseID: &id, ReleaseDiscards: &no, ReleaseEnds: &yes,
		RefusedRelease: &id, Refusal: "2 released values still don't fit"}
	h, err := hold(row)
	if err != nil {
		t.Fatal(err)
	}
	if h.GetPending() || !strings.HasPrefix(h.GetRefused().GetReason(), "2 released values still don't fit; ") ||
		!strings.Contains(h.GetRefused().GetReason(), "so the discard dropped nothing") || len(h.GetWaysOut()) == 0 {
		t.Errorf("hold %v", h)
	}
	row.ReleaseEnds = &no
	if h, err = hold(row); err != nil || h.GetRefused().GetReason() != "2 released values still don't fit" {
		t.Errorf("refused %v, %v", h.GetRefused(), err)
	}
}
