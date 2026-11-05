package runner

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

func TestMergeHoldsKeepsWhenEachHoldStarted(t *testing.T) {
	now := time.Unix(100, 0)
	at := func(g, n uint64) checkpoint.RecordRef { return checkpoint.RecordRef{Generation: g, Epoch: n} }
	files := []destination.StagedFile{{Path: "f", Bytes: 1}}
	h := mergeHolds(nil, &ReadyHolds{
		Holding: []checkpoint.ColumnHold{{Column: "y", Reason: "new"}},
		Cells:   map[string]checkpoint.CellHold{"x": {Cells: 2, Reason: "nan"}},
		Files:   files,
	}, at(1, 3), now)
	h = mergeHolds(h, &ReadyHolds{
		Holding: []checkpoint.ColumnHold{{Column: "y", Reason: "new"}},
		Cells:   map[string]checkpoint.CellHold{"x": {Cells: 1, Reason: "range"}},
		Files:   files,
	}, at(1, 5), now.Add(time.Hour))
	h = mergeHolds(h, &ReadyHolds{Holding: h.Columns, Files: files}, at(2, 6), now)
	if c := h.Columns[0]; c.Since != at(1, 3) || !c.Started.Equal(now) {
		t.Errorf("column hold %+v", c)
	}
	if c := h.Cells["x"]; c.Cells != 3 || c.Reason != "nan" || c.Since != at(1, 3) {
		t.Errorf("cell hold %+v", c)
	}
	want := []checkpoint.HeldEpochs{{Generation: 1, From: 3, To: 5}, {Generation: 2, From: 6, To: 6}}
	if !reflect.DeepEqual(h.Epochs, want) {
		t.Errorf("epochs %+v, want %+v", h.Epochs, want)
	}
	if got := mergeHolds(h, nil, at(2, 7), now); !reflect.DeepEqual(got, h) {
		t.Errorf("a stream that reported nothing changed: %+v", got)
	}

	// A finished replay ends the released columns' cell holds; a discarded
	// column needs no values.
	h.Replay = &checkpoint.Replay{Columns: []string{"x"}}
	h.Columns[0].Discarded = true
	got := mergeHolds(h, &ReadyHolds{Holding: h.Columns, Replayed: &ReplayProgress{Done: true}}, at(2, 8), now)
	if len(got.Cells) != 0 || got.Replay != nil || got.Epochs != nil || len(got.Columns) != 1 {
		t.Errorf("after the replay: %+v", got)
	}
	// Cells held while a replay runs aren't in its files, so they stay
	// held once it ends, as a hold of their own.
	h = &checkpoint.Holds{
		Cells:  map[string]checkpoint.CellHold{"x": {Cells: 2, Since: at(1, 3)}},
		Epochs: []checkpoint.HeldEpochs{{Generation: 1, From: 3, To: 3}},
		Replay: &checkpoint.Replay{Columns: []string{"x"}, Files: []string{"1/3/s/part-00000.parquet"}},
	}
	h = mergeHolds(h, &ReadyHolds{Cells: map[string]checkpoint.CellHold{"x": {Cells: 1, Reason: "type"}}, Files: files, Replayed: &ReplayProgress{}}, at(3, 4), now)
	got = mergeHolds(h, &ReadyHolds{Replayed: &ReplayProgress{File: 1, Done: true}}, at(3, 5), now)
	if c := got.Cells["x"]; c.Cells != 1 || c.Since != at(3, 4) || got.Replay != nil || len(got.Epochs) != 2 {
		t.Errorf("after a replay with a cell held during it: %+v", got)
	}
	if mergeHolds(&checkpoint.Holds{}, &ReadyHolds{}, at(2, 9), now) != nil {
		t.Error("nothing held, and still holds")
	}
}

func TestStartHoldsAppliesReleasesToTheHoldsTheyName(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	restored := &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: &checkpoint.Holds{
		Columns: []checkpoint.ColumnHold{{Column: "y", Since: since}, {Column: "z", Since: since}},
		Cells:   map[string]checkpoint.CellHold{"x": {Cells: 1, Since: since}},
	}}}}
	j := &Job{
		HeldURL:     "file:///held",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Destination: holdingDest{},
		Streams:     []Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: &StreamHolds{}}},
		Releases: []Release{
			{Stream: "s", Column: "y", Since: since, Discard: true},
			{Stream: "s", Column: "x", Since: since},
			{Stream: "s", Column: "z", Since: checkpoint.RecordRef{Generation: 9, Epoch: 9}},
		},
		refused: []RefusedColumn{{RefusedColumn: destination.RefusedColumn{Stream: "s", Column: "w", Reason: "view"}}},
	}
	holds, plans, err := j.startHolds(t.Context(), restored, map[string]destination.TableLimits{"s": {MaxColumns: 1600}})
	if err != nil {
		t.Fatal(err)
	}
	h := holds["s"]
	if len(h.Columns) != 3 || !h.Columns[0].Discarded || h.Columns[1].Discarded || h.Columns[2].Column != "w" {
		t.Errorf("columns %+v", h.Columns)
	}
	if h.Replay == nil || !reflect.DeepEqual(h.Replay.Columns, []string{"x"}) {
		t.Errorf("replay %+v", h.Replay)
	}
	if p := plans["s"]; p.Limits.MaxColumns != 1600 || len(p.Holding) != 3 || p.Replay != h.Replay {
		t.Errorf("plan %+v", p)
	}
	if restored.Streams["s"].Holds.Columns[0].Discarded {
		t.Error("the restored record changed")
	}

	// The destination refuses a released column again: it stays held as it
	// was, and isn't patched in.
	j.Releases = []Release{{Stream: "s", Column: "z", Since: since}, {Stream: "s", Column: "x", Since: since}}
	j.refused = []RefusedColumn{{RefusedColumn: destination.RefusedColumn{Stream: "s", Column: "z", Reason: "view"}}}
	holds, _, err = j.startHolds(t.Context(), restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	h = holds["s"]
	if i := slices.IndexFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return c.Column == "z" }); i < 0 || h.Columns[i].Since != since {
		t.Errorf("columns %+v", h.Columns)
	}
	if h.Replay == nil || !reflect.DeepEqual(h.Replay.Columns, []string{"x"}) {
		t.Errorf("replay %+v", h.Replay)
	}
}

type holdingDest struct{ destination.Destination }

func (holdingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Holds: destination.Holds{ByID: true, Guarded: true}}
}
