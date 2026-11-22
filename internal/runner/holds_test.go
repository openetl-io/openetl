package runner

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
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
	// A replay that ends with cells that still don't fit leaves them held,
	// and records the release that didn't clear them.
	h = &checkpoint.Holds{
		Cells:  map[string]checkpoint.CellHold{"x": {Cells: 2, Since: at(1, 3)}},
		Epochs: []checkpoint.HeldEpochs{{Generation: 1, From: 3, To: 3}},
		Replay: &checkpoint.Replay{Columns: []string{"x"}, CellReleases: map[string]string{"x": "r"}},
	}
	got = mergeHolds(h, &ReadyHolds{Replayed: &ReplayProgress{File: 1, StillHeld: 2, Done: true}}, at(3, 5), now)
	if c := got.Cells["x"]; c.Since != at(1, 3) || c.Refused == nil || *c.Refused != (checkpoint.Refused{Release: "r", Reason: "2 released values still don't fit"}) {
		t.Errorf("after a replay with cells that still don't fit: %+v", c)
	}
	if mergeHolds(&checkpoint.Holds{}, &ReadyHolds{}, at(2, 9), now) != nil {
		t.Error("nothing held, and still holds")
	}
}

func TestStartHoldsAppliesReleasesToTheHoldsTheyName(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	restored := &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: &checkpoint.Holds{
		Columns: []checkpoint.ColumnHold{{Column: "y", Since: since}, {Column: "z", Delivered: []byte("z int"), Since: since}},
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

	// The destination refuses a released column again: it stays held from
	// when it was, with the destination's cause, records the release it
	// refused, and isn't patched in.
	j.Releases = []Release{{ID: "r1", Stream: "s", Column: "z", Since: since}, {ID: "r2", Stream: "s", Column: "x", Since: since}}
	refusal := destination.RefusedColumn{Stream: "s", Column: "z", Reason: "it doesn't widen", FromType: "integer", ToType: "bigint", Fix: "ALTER z"}
	j.refused = []RefusedColumn{{RefusedColumn: refusal}}
	holds, _, err = j.startHolds(t.Context(), restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	h = holds["s"]
	i := slices.IndexFunc(h.Columns, func(c checkpoint.ColumnHold) bool { return c.Column == "z" })
	if i < 0 || h.Columns[i].Since != since {
		t.Fatalf("columns %+v", h.Columns)
	}
	z := h.Columns[i]
	wantCause := &checkpoint.HoldCause{Kind: checkpoint.HoldRefused, Refusal: "it doesn't widen", FromType: "integer", ToType: "bigint", Fix: "ALTER z"}
	if !reflect.DeepEqual(z.Cause, wantCause) || z.Refused == nil || *z.Refused != (checkpoint.Refused{Release: "r1", Reason: "the destination refused it again"}) {
		t.Errorf("refused column %+v, cause %+v, refused %+v", z, z.Cause, z.Refused)
	}
	if h.Replay == nil || !reflect.DeepEqual(h.Replay.Columns, []string{"x"}) || h.Replay.CellReleases["x"] != "r2" {
		t.Errorf("replay %+v", h.Replay)
	}

	// The engine's next report keeps the refusal, which it doesn't carry.
	engine := z
	engine.Since, engine.Refused = checkpoint.RecordRef{}, nil
	engine.Cause = &checkpoint.HoldCause{Kind: checkpoint.HoldRefused, Source: "Z", From: "int32", To: "int64"}
	merged := mergeHolds(h, &ReadyHolds{Holding: []checkpoint.ColumnHold{engine}}, checkpoint.RecordRef{Generation: 2, Epoch: 1}, time.Now())
	got := merged.Columns[0]
	if got.Since != since || got.Refused == nil || got.Refused.Release != "r1" || got.Cause.Fix != "ALTER z" || got.Cause.From != "int32" || got.Cause.Source != "Z" {
		t.Errorf("merged %+v, cause %+v", got, got.Cause)
	}

	// A release already refused isn't taken again, unless it's a new one.
	merged.Replay, merged.Cells = nil, nil
	refused := &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: merged}}}
	j.Releases, j.refused = []Release{{ID: "r1", Stream: "s", Column: "z", Since: since}}, nil
	holds, _, err = j.startHolds(t.Context(), refused, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h := holds["s"]; len(h.Columns) != 1 || h.Replay != nil {
		t.Errorf("a refused release was taken again: %+v", h)
	}
	j.Releases[0].ID = "r3"
	holds, _, err = j.startHolds(t.Context(), refused, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h := holds["s"]; len(h.Columns) != 0 || h.Replay == nil {
		t.Errorf("a new release wasn't taken: %+v", h)
	}

	// Released to a new version, a column keeps the type the destination
	// has; one the destination doesn't have is released as it is.
	j.Releases = []Release{{Stream: "s", Column: "z", Since: since, NewVersion: true}, {Stream: "s", Column: "y", Since: since, NewVersion: true}}
	j.refused = nil
	holds, _, err = j.startHolds(t.Context(), restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	r := holds["s"].Replay
	if r == nil || !reflect.DeepEqual(r.Columns, []string{"y", "z"}) || len(r.Versioned) != 1 || r.Versioned[0].Column != "z" || string(r.Versioned[0].Delivered) != "z int" {
		t.Errorf("replay %+v", r)
	}
	if len(holds["s"].Columns) != 0 {
		t.Errorf("columns %+v", holds["s"].Columns)
	}
}

// A hold on a column that can't be held, recorded before the runner checked
// what it holds, stops each attempt with how to release it; released or
// discarded, the attempt holds it no longer. A refusal of it fails with the
// fix.
func TestAHoldOnAColumnThatCantBeHeldStopsUntilReleased(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	fix := "aws dynamodb delete-table --table-name s"
	key := checkpoint.ColumnHold{
		Column: "id", Since: since, Delivered: []byte("id int"), Reason: "the destination refused it: the table is keyed on it",
		Cause: &checkpoint.HoldCause{Kind: checkpoint.HoldRefused, Refusal: "the table is keyed on it", Fix: fix},
	}
	record := func(c checkpoint.ColumnHold) *checkpoint.Record {
		return &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: &checkpoint.Holds{Columns: []checkpoint.ColumnHold{c}}}}}
	}
	j := &Job{
		HeldURL:     "file:///held",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Destination: holdingDest{},
		Streams:     []Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: &StreamHolds{}}},
	}
	stops := func(err error, says ...string) {
		t.Helper()
		if errs.CodeOf(err) != errs.SchemaDrift {
			t.Fatalf("got %v, want SCHEMA_DRIFT", err)
		}
		for _, s := range says {
			if !strings.Contains(err.Error(), s) {
				t.Errorf("%v doesn't say %q", err, s)
			}
		}
	}
	_, _, err := j.startHolds(t.Context(), record(key), nil)
	stops(err, "it's part of the stream's key", fix, "openetl holds release PIPELINE s id")

	// Discarded, in this attempt or before it, the hold goes without a
	// replay: its values are gone.
	gone := key
	gone.Discarded = true
	for _, c := range []struct {
		releases []Release
		rec      *checkpoint.Record
	}{
		{[]Release{{Stream: "s", Column: "id", Since: since, Discard: true}}, record(key)},
		{nil, record(gone)},
	} {
		j.Releases = c.releases
		holds, plans, err := j.startHolds(t.Context(), c.rec, nil)
		if err != nil {
			t.Fatal(err)
		}
		if h := holds["s"]; len(h.Columns) != 0 || h.Replay != nil || len(plans["s"].Holding) != 0 {
			t.Errorf("discarded, holds %+v, plan %+v", h, plans["s"])
		}
	}

	j.Releases = []Release{{ID: "r1", Stream: "s", Column: "id", Since: since}}
	holds, plans, err := j.startHolds(t.Context(), record(key), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans["s"].Holding) != 0 || holds["s"].Replay == nil || !reflect.DeepEqual(holds["s"].Replay.Columns, []string{"id"}) {
		t.Errorf("released, holds %+v, plan %+v", holds["s"], plans["s"])
	}

	// The destination refuses the released column again, and lists it as
	// one it can't hold.
	j.refused = []RefusedColumn{{RefusedColumn: destination.RefusedColumn{Stream: "s", Column: "id", Reason: "the table is keyed on it", Fix: fix}}}
	_, _, err = j.startHolds(t.Context(), record(key), map[string]destination.TableLimits{"s": {Unholdable: []string{"id"}}})
	stops(err, "the destination can't hold it", fix)

	// A column the destination's limits say it can't hold.
	j.Releases, j.refused = nil, nil
	part := checkpoint.ColumnHold{Column: "p", Since: since, Reason: "the destination refused it: it partitions by it"}
	_, _, err = j.startHolds(t.Context(), record(part), map[string]destination.TableLimits{"s": {Unholdable: []string{"p"}}})
	stops(err, "the destination can't hold it", "once the destination takes the column as the stream has it", "openetl holds release PIPELINE s p")
	if _, _, err := j.startHolds(t.Context(), record(part), nil); err != nil {
		t.Errorf("held where the destination can hold it: %v", err)
	}
}

// A SQL transform's outputs hold, as the destination's streams, and its
// inputs don't; the user's Spark or Flink code's outputs don't hold. An
// output's holds restore from the record's outputs, released under its
// name.
func TestASQLTransformsOutputsHold(t *testing.T) {
	since := checkpoint.RecordRef{Generation: 1, Epoch: 2}
	held := &checkpoint.Holds{Columns: []checkpoint.ColumnHold{{Column: "n", Since: since}}, Cells: map[string]checkpoint.CellHold{"x": {Cells: 1, Since: since}}}
	restored := &checkpoint.Record{
		Streams: map[string]checkpoint.StreamState{"a": {}},
		Outputs: map[string]checkpoint.OutputState{"a": {Holds: held}},
	}
	output := func(name string, key ...string) Stream {
		return Stream{Name: name, Replay: source.Exact, Mode: destination.Append, PrimaryKey: key, Holds: &StreamHolds{}, Transformed: true}
	}
	j := &Job{
		HeldURL:     "file:///held",
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "org", "pipe"),
		Destination: holdingDest{},
		Streams:     []Stream{{Name: "a", Replay: source.Exact, PrimaryKey: []string{"id"}}},
		Outputs:     []Stream{output("a", "id"), output("c")},
		SQL:         &SQLTransform{SQL: "INSERT INTO a SELECT * FROM a; INSERT INTO c SELECT * FROM a"},
		Releases:    []Release{{Stream: "a", Column: "x", Since: since}},
	}
	if err := j.checkHolds(); err == nil || !strings.Contains(err.Error(), `stream "c" holds columns, and only a keyed stream can`) {
		t.Errorf("an output without a key: %v", err)
	}
	j.Outputs[1].Holds = nil
	if err := j.checkHolds(); err != nil {
		t.Error(err)
	}
	holds, plans, err := j.startHolds(t.Context(), restored, nil)
	if err != nil {
		t.Fatal(err)
	}
	if h := holds["a"]; len(holds) != 1 || len(h.Columns) != 1 || h.Replay == nil || !reflect.DeepEqual(h.Replay.Columns, []string{"x"}) {
		t.Errorf("holds %+v", holds)
	}
	if p := plans["a"]; p == nil || len(p.Holding) != 1 {
		t.Errorf("plans %+v", plans)
	}

	j.Streams[0].Holds = &StreamHolds{}
	if err := j.checkHolds(); err == nil || !strings.Contains(err.Error(), "a transform's inputs can't be held") {
		t.Errorf("an input that holds: %v", err)
	}
	j.Streams[0].Holds, j.SQL, j.Transform = nil, nil, sparkCode{}
	if j.holdsOn(j.Outputs[0]) {
		t.Error("a Spark transform's output holds")
	}
}

// sparkCode stands for the user's Spark code.
type sparkCode struct{ Transform }

type holdingDest struct{ destination.Destination }

func (holdingDest) Capabilities() destination.Capabilities {
	return destination.Capabilities{Holds: destination.Holds{ByID: true, Guarded: true}}
}

// heldEpochs are the held files of stream s in epochs 1 to 6 of generation
// 1, one each, and start, which is the replay that j's releases start on s
// with holds h.
func heldEpochs(t *testing.T) (files []string, start func(h *checkpoint.Holds, j *Job) *checkpoint.Replay) {
	t.Helper()
	store := checkpoint.NewMemStore()
	for n := uint64(1); n <= 6; n++ {
		rel := fmt.Sprintf("1/%d/s/part-00000.parquet", n)
		if _, err := store.Create(t.Context(), "org/pipe/"+checkpoint.HeldDir+"/"+rel, []byte("PAR1")); err != nil {
			t.Fatal(err)
		}
		files = append(files, rel)
	}
	return files, func(h *checkpoint.Holds, j *Job) *checkpoint.Replay {
		t.Helper()
		j.HeldURL, j.Log, j.Destination = "file:///held", checkpoint.NewLog(store, "org", "pipe"), holdingDest{}
		j.Streams = []Stream{{Name: "s", Replay: source.Exact, Mode: destination.Append, PrimaryKey: []string{"id"}, Holds: &StreamHolds{Cells: true}}}
		holds, _, err := j.startHolds(t.Context(), &checkpoint.Record{Streams: map[string]checkpoint.StreamState{"s": {Holds: h}}}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return holds["s"].Replay
	}
}

func epoch(n uint64) checkpoint.RecordRef { return checkpoint.RecordRef{Generation: 1, Epoch: n} }

// A release reads the held files from the oldest start of the holds it
// releases. Epochs is the stream's, and keeps the files of cells patched in
// by an earlier release while another column holds: reading those again
// would patch back cells since discarded, or changed.
func TestAReleaseReadsOnlyTheFilesOfTheHoldsItReleases(t *testing.T) {
	files, start := heldEpochs(t)
	// b has held cells since epoch 1. A release patched in a's cells of
	// epochs 1-2, and a holds cells again from epoch 5 on.
	h := &checkpoint.Holds{
		Cells: map[string]checkpoint.CellHold{
			"a": {Cells: 1, Since: epoch(1)},
			"b": {Cells: 9, Since: epoch(1)},
		},
		Epochs: []checkpoint.HeldEpochs{{Generation: 1, From: 1, To: 2}},
		Replay: &checkpoint.Replay{Columns: []string{"a"}, Files: files[:2], CellReleases: map[string]string{"a": "r1"}},
	}
	h = mergeHolds(h, &ReadyHolds{Replayed: &ReplayProgress{File: 2, Done: true}}, epoch(3), time.Now())
	if _, ok := h.Cells["a"]; ok {
		t.Fatalf("a still held after its replay: %+v", h)
	}
	for n := uint64(4); n <= 6; n++ {
		r := &ReadyHolds{Files: []destination.StagedFile{{Path: "f"}}}
		if n >= 5 {
			r.Cells = map[string]checkpoint.CellHold{"a": {Cells: 1}}
		}
		h = mergeHolds(h, r, epoch(n), time.Now())
	}
	if h.Cells["a"].Since != epoch(5) {
		t.Fatalf("a's new hold %+v", h.Cells["a"])
	}
	r := start(h, &Job{
		Releases: []Release{{ID: "r2", Stream: "s", Column: "a", Since: epoch(5)}},
	})
	if got, want := r.Files, files[4:]; !slices.Equal(got, want) {
		t.Errorf("the release of a's hold since epoch 5 reads %v, want %v", got, want)
	}
}

// A replay that ends with every cell fitting clears the cell holds of the
// columns it read, released or not, so it reads from the oldest of them: x
// holds cells since epoch 2, and the release names only its hold held whole
// since epoch 5. A column the destination refuses again isn't read for.
func TestAReleaseReadsFromTheCellHoldsItClears(t *testing.T) {
	files, start := heldEpochs(t)
	h := &checkpoint.Holds{
		Columns: []checkpoint.ColumnHold{{Column: "x", Reason: "refused", Since: epoch(5)}, {Column: "y", Reason: "refused", Since: epoch(1)}},
		Cells:   map[string]checkpoint.CellHold{"x": {Cells: 3, Since: epoch(2)}},
		Epochs:  []checkpoint.HeldEpochs{{Generation: 1, From: 1, To: 6}},
	}
	r := start(h, &Job{
		Releases: []Release{
			{ID: "r1", Stream: "s", Column: "x", Since: epoch(5)},
			{ID: "r2", Stream: "s", Column: "y", Since: epoch(1)},
		},
		refused: []RefusedColumn{{RefusedColumn: destination.RefusedColumn{Stream: "s", Column: "y", Reason: "still too narrow"}}},
	})
	if !slices.Equal(r.Columns, []string{"x"}) {
		t.Fatalf("the replay releases %v", r.Columns)
	}
	if want := files[1:]; !slices.Equal(r.Files, want) {
		t.Errorf("the release of x's hold since epoch 5 reads %v, want %v, from x's cell hold", r.Files, want)
	}
}

// Columns released together are read from one list of files, each from its
// own holds' start: a held cells in epochs 1-2, which a release patched in
// or the user discarded, and holds cells again since epoch 5; b holds since
// epoch 1. The files before epoch 5 aren't read for a.
func TestColumnsReleasedTogetherAreEachReadFromTheirStart(t *testing.T) {
	files, start := heldEpochs(t)
	h := &checkpoint.Holds{
		Cells: map[string]checkpoint.CellHold{
			"a": {Cells: 1, Since: epoch(5)},
			"b": {Cells: 9, Since: epoch(1)},
		},
		Epochs: []checkpoint.HeldEpochs{{Generation: 1, From: 1, To: 6}},
	}
	r := start(h, &Job{
		Releases: []Release{
			{ID: "r1", Stream: "s", Column: "a", Since: epoch(5)},
			{ID: "r2", Stream: "s", Column: "b", Since: epoch(1)},
		},
	})
	if !slices.Equal(r.Files, files) {
		t.Fatalf("the replay reads %v, want %v", r.Files, files)
	}
	// Neither is held whole, so no file is read for values held whole.
	if want := map[string]int{"a": 6, "b": 6}; !reflect.DeepEqual(r.First, want) {
		t.Errorf("the first files of values held whole are %v, want %v", r.First, want)
	}
	if want := map[string]int{"a": 4}; !reflect.DeepEqual(r.FirstCells, want) {
		t.Errorf("the first files of held cells are %v, want %v", r.FirstCells, want)
	}
}

// A column's cells and its values held whole are each read from their own
// hold's start: x is held whole since epoch 2, and its cells of epochs 1-4
// were discarded, so the release of the hold held whole reads cells from
// epoch 5 on, where x's cell hold now starts.
func TestAColumnsCellsAreReadFromItsCellHoldsStart(t *testing.T) {
	files, start := heldEpochs(t)
	h := &checkpoint.Holds{
		Columns: []checkpoint.ColumnHold{{Column: "x", Reason: "refused", Since: epoch(2)}},
		Cells:   map[string]checkpoint.CellHold{"x": {Cells: 3, Since: epoch(5)}},
		Epochs:  []checkpoint.HeldEpochs{{Generation: 1, From: 1, To: 6}},
	}
	r := start(h, &Job{
		Releases: []Release{{ID: "r1", Stream: "s", Column: "x", Since: epoch(2)}},
	})
	if want := files[1:]; !slices.Equal(r.Files, want) {
		t.Fatalf("the replay reads %v, want %v", r.Files, want)
	}
	if len(r.First) != 0 {
		t.Errorf("the first files of values held whole are %v, want the first", r.First)
	}
	if want := map[string]int{"x": 3}; !reflect.DeepEqual(r.FirstCells, want) {
		t.Errorf("the first files of held cells are %v, want %v", r.FirstCells, want)
	}
}
