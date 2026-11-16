package runner

import (
	"maps"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

// A redo's lanes start at the floors the epoch started at, which an
// attempt's start gives them: a read that started in the epoch's attempt,
// after one that completed, carries that read's watermarks, completed
// splits' among them, and a split it didn't have starts at their least; a
// read that goes on keeps its splits'; one that completed earlier in the
// sync, or a held stream's, has none.
func TestARedoStartsAtTheFloorsItsEpochStartedAt(t *testing.T) {
	mark := func(n int64) *int64 { return &n }
	done := func(sync string, splits ...checkpoint.SplitState) *checkpoint.Record {
		r := &checkpoint.Record{Epoch: 4, Generation: 1, Sync: sync, Streams: map[string]checkpoint.StreamState{"s": {}}}
		r.SetSplits("s", splits)
		return r
	}
	at := func(sync string, ids ...string) *checkpoint.Record {
		r := &checkpoint.Record{Epoch: 5, Generation: 2, Sync: sync, Streams: map[string]checkpoint.StreamState{"s": {}}}
		var splits []checkpoint.SplitState
		for _, id := range ids {
			splits = append(splits, checkpoint.SplitState{ID: id, State: []byte("x")})
		}
		r.SetSplits("s", splits)
		return r
	}
	completed := []checkpoint.SplitState{
		{ID: "p0", State: []byte("9"), Complete: true, Watermark: mark(100)},
		{ID: "p1", State: []byte("9"), Complete: true, Watermark: mark(50)},
	}
	for name, c := range map[string]struct {
		held        bool
		rec, before *checkpoint.Record
		want        map[string]int64
	}{
		"a new read after a completed one": {rec: at("s2", "p0", "p1"), before: done("s1", completed...), want: map[string]int64{"s": 50}},
		"a new read's own splits":          {rec: at("s2", "p0"), before: done("s1", completed...), want: map[string]int64{"s": 100}},
		"a split the last read hadn't":     {rec: at("s2", "p0", "p2"), before: done("s1", completed...), want: map[string]int64{"s": 50}},
		"a read completed in the sync":     {rec: at("s1", "p0", "p1"), before: done("s1", completed...), want: map[string]int64{}},
		"a read that goes on": {rec: at("s1", "p0", "p1"), before: done("s1",
			checkpoint.SplitState{ID: "p0", State: []byte("4"), Watermark: mark(100)}, checkpoint.SplitState{ID: "p1", State: []byte("4"), Watermark: mark(70)}),
			want: map[string]int64{"s": 70}},
		"a held stream": {held: true, rec: at("s2", "p0", "p1"), before: done("s1", completed...), want: map[string]int64{}},
	} {
		t.Run(name, func(t *testing.T) {
			j := &Job{Streams: []Stream{{Name: "s", EventTime: &EventTime{Column: "at", Held: c.held}}}}
			if got := j.redoFloors(c.rec, c.before); !maps.Equal(got, c.want) {
				t.Errorf("floors %v, want %v", got, c.want)
			}
		})
	}
	if got := (&Job{Streams: []Stream{{Name: "s", EventTime: &EventTime{Column: "at"}}}}).redoFloors(at("s2", "p0"), nil); got != nil {
		t.Errorf("floors %v from no record before, want none", got)
	}
}

// A split the source added mid-read gives the floors an attempt's start
// would the least watermark carried from the read before, which the
// epoch's attempt never started at: where a group window reads the stream,
// the record keeps where the epoch started it and where it closed its
// windows at the barrier, and a redo goes by those.
func TestARedoStartsItsWindowsWhereTheRecordSays(t *testing.T) {
	mark := func(n int64) *int64 { return &n }
	before := &checkpoint.Record{Epoch: 4, Generation: 1, Sync: "s1", Streams: map[string]checkpoint.StreamState{"s": {}}}
	before.SetSplits("s", []checkpoint.SplitState{
		{ID: "p0", State: []byte("9"), Complete: true, Watermark: mark(100)},
		{ID: "p1", State: []byte("9"), Complete: true, Watermark: mark(50)},
	})
	rec := &checkpoint.Record{Epoch: 5, Generation: 2, Sync: "s2", Streams: map[string]checkpoint.StreamState{"s": {}}}
	// p2 joined the read in the epoch, after it started with p0.
	rec.SetSplits("s", []checkpoint.SplitState{{ID: "p0", State: []byte("x")}, {ID: "p2", State: []byte("x")}})
	j := &Job{Streams: []Stream{{Name: "s", EventTime: &EventTime{Column: "at"}}}}
	floors, closes := j.redoMarks(rec, before)
	if want := map[string]int64{"s": 50}; !maps.Equal(floors, want) || len(closes) != 0 {
		t.Errorf("without the window's marks, floors %v and closes %v, want %v and none", floors, closes, want)
	}
	st := rec.Streams["s"]
	st.Floor, st.Closed = mark(100), mark(130)
	rec.Streams["s"] = st
	floors, closes = j.redoMarks(rec, before)
	if !maps.Equal(floors, map[string]int64{"s": 100}) || !maps.Equal(closes, map[string]int64{"s": 130}) {
		t.Errorf("floors %v and closes %v, want the record's 100 and 130", floors, closes)
	}
}
