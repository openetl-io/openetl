package checkpoint

import (
	"math"
	"reflect"
	"testing"
)

func TestOperatorsAddUpEveryEnginesPart(t *testing.T) {
	at := func(n int64) *int64 { return &n }
	parts := []OperatorPart{
		{Operator: "window-b", RowsIn: 7, Opened: 2, Watermark: at(math.MaxInt64)},
		{Operator: "window-a", RowsIn: 10, Inserts: 3, Updates: 1, Late: 2, Opened: 5, Dropped: 1, Watermark: at(90), MemoryBytes: 100,
			Runs:    map[string]uint64{"window-a/g1/e1/kg0-127.run": 40, "window-a/g1/e2/kg0-127.run": 10},
			Barrier: Requests{Count: 1, Bytes: 10}, Lookup: Requests{Count: 3, Bytes: 300}, MergeWrite: Requests{Count: 1, Bytes: 30}},
		{Operator: "window-a", RowsIn: 5, Deletes: 1, Opened: 1, Dropped: 2, Watermark: at(60), MemoryBytes: 50,
			Runs:    map[string]uint64{"window-a/g1/e1/kg0-127.run": 40, "window-a/g1/e2/kg128-255.run": 7},
			Barrier: Requests{Count: 1, Bytes: 7}, Restore: Requests{Count: 2, Bytes: 64}, MergeWrite: Requests{Count: 1, Bytes: 5},
			MergeRead: Requests{Count: 4, Bytes: 90}, Disk: Requests{Count: 6, Bytes: 600}},
	}
	got := Operators(parts, map[string]int64{"window-a": 4, "window-c": 9})
	want := []Operator{
		{Operator: "window-a", RowsIn: 15, Inserts: 3, Updates: 1, Deletes: 1, Late: 2, Windows: 4 + 6 - 3, Watermark: at(60), MemoryBytes: 150,
			StoredBytes: 40 + 10 + 7, Barrier: Requests{Count: 2, Bytes: 17}, Lookup: Requests{Count: 3, Bytes: 300}, Restore: Requests{Count: 2, Bytes: 64},
			MergeWrite: Requests{Count: 2, Bytes: 35}, MergeRead: Requests{Count: 4, Bytes: 90}, Disk: Requests{Count: 6, Bytes: 600}},
		{Operator: "window-b", RowsIn: 7, Windows: 2, Watermark: at(math.MaxInt64)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v\nwant %+v", got, want)
	}
	// A lane with no watermark yet holds the operator's back, whichever
	// engine's it is.
	none := Operators([]OperatorPart{{Operator: "w", Watermark: at(5)}, {Operator: "w"}, {Operator: "w", Watermark: at(3)}}, nil)
	if none[0].Watermark != nil {
		t.Errorf("watermark %d with a lane that has none", *none[0].Watermark)
	}
	if Operators(nil, nil) != nil {
		t.Error("operators without parts")
	}
}

func TestWindowsCountOnOnlyWhereTheRecordKeepsTheState(t *testing.T) {
	rec := &Record{
		Snapshots: []Snapshot{{Operator: "kept"}},
		Operators: []Operator{{Operator: "kept", Windows: 12}, {Operator: "reset", Windows: 30}},
	}
	if got := KeptWindows(rec); !reflect.DeepEqual(got, map[string]int64{"kept": 12}) {
		t.Errorf("%v", got)
	}
	if got := KeptWindows(nil); len(got) != 0 {
		t.Errorf("%v", got)
	}
}
