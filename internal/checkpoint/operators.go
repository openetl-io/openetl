package checkpoint

import (
	"maps"
	"slices"

	"github.com/openetl-io/openetl/internal/platform/ints"
)

// Requests are requests of one kind to object storage, and their bytes.
type Requests struct {
	Count uint64 `json:"count,omitempty"`
	Bytes uint64 `json:"bytes,omitempty"`
}

func (r Requests) add(o Requests) Requests {
	return Requests{Count: r.Count + o.Count, Bytes: r.Bytes + o.Bytes}
}

// OperatorPart is what one engine's lanes of a stateful operator did in an
// epoch, and held at their barriers. An attempt's engines each report their
// own.
type OperatorPart struct {
	Operator                                string
	RowsIn, Inserts, Updates, Deletes, Late uint64
	// Windows, or sessions, whose state it began keeping, and stopped
	// keeping; a dedupe's keys.
	Opened, Dropped uint64
	// A dedupe's rows it didn't write: repeats of a key it keeps, and for
	// one that keeps each key's latest row, rows no later than the key's.
	Repeats uint64
	// The least of its lanes' watermarks, in microseconds since the Unix
	// epoch; nil while a lane has none.
	Watermark   *int64
	MemoryBytes uint64
	// The runs its lanes' snapshots name, with their bytes.
	Runs                     map[string]uint64
	Barrier, Lookup, Restore Requests
	// Merges' outputs written at the barriers, and merges' reads.
	MergeWrite, MergeRead Requests
	// A group window's, where its steps are recorded: those of its
	// watermark that each of its keyed lanes' rows met.
	Steps []LaneSteps
}

// Operator is what a stateful operator did in a record's epoch, on every
// lane of every engine, and held at its barrier.
type Operator struct {
	Operator string `json:"operator"`
	// Rows of its stream that reached it.
	RowsIn uint64 `json:"rows_in,omitempty"`
	// Its output's rows it wrote, by op.
	Inserts uint64 `json:"inserts,omitempty"`
	Updates uint64 `json:"updates,omitempty"`
	Deletes uint64 `json:"deletes,omitempty"`
	// Rows it quarantined as late.
	Late uint64 `json:"late,omitempty"`
	// A dedupe's rows it didn't write, as OperatorPart's.
	Repeats uint64 `json:"repeats,omitempty"`
	// The windows, or sessions, whose state it keeps: open, or closed and
	// kept for their lateness; a dedupe's keys. It counts from the record
	// its attempt restored, and from 0 where that holds none of its state.
	Windows int64 `json:"windows"`
	// The least of its lanes' watermarks, in microseconds since the Unix
	// epoch, or the largest int64 at the end of input; nil while a lane has
	// none.
	Watermark *int64 `json:"watermark,omitempty"`
	// What its lanes' state and block caches held in memory.
	MemoryBytes uint64 `json:"memory_bytes,omitempty"`
	// The bytes of the runs of its that the record names.
	StoredBytes uint64 `json:"stored_bytes,omitempty"`
	// The bytes of its state that the record carries itself: its lanes'
	// changes since each last wrote a run, in their snapshots' Pending.
	CheckpointBytes uint64 `json:"checkpoint_bytes,omitempty"`
	// Its requests to object storage in the epoch: runs written at
	// barriers, blocks read, and runs opened as a record names them.
	Barrier Requests `json:"barrier"`
	Lookup  Requests `json:"lookup"`
	Restore Requests `json:"restore"`
	// Its merges': outputs written at barriers, and reads of what they
	// merge.
	MergeWrite Requests `json:"merge_write"`
	MergeRead  Requests `json:"merge_read"`
}

// Operators adds up an epoch's parts by operator, sorted. Each counts its
// windows on from kept's, which are the operators' as the record before
// had them, and the bytes of its pending sets that snapshots, the record's,
// carry.
func Operators(parts []OperatorPart, kept map[string]int64, snapshots []Snapshot) []Operator {
	if len(parts) == 0 {
		return nil
	}
	carried := map[string]uint64{}
	for _, s := range snapshots {
		carried[s.Operator] += uint64(len(s.Pending))
	}
	by := map[string]*Operator{}
	runs := map[string]map[string]uint64{}
	none := map[string]bool{}
	for _, p := range parts {
		o := by[p.Operator]
		if o == nil {
			o = &Operator{Operator: p.Operator, Windows: kept[p.Operator], Watermark: p.Watermark}
			by[p.Operator], runs[p.Operator] = o, map[string]uint64{}
		}
		o.RowsIn += p.RowsIn
		o.Inserts += p.Inserts
		o.Updates += p.Updates
		o.Deletes += p.Deletes
		o.Late += p.Late
		o.Repeats += p.Repeats
		o.Windows += ints.Int64Clamped(p.Opened) - ints.Int64Clamped(p.Dropped)
		switch {
		case p.Watermark == nil:
			none[p.Operator] = true
		case o.Watermark == nil || *p.Watermark < *o.Watermark:
			o.Watermark = p.Watermark
		}
		o.MemoryBytes += p.MemoryBytes
		maps.Copy(runs[p.Operator], p.Runs)
		o.Barrier, o.Lookup, o.Restore = o.Barrier.add(p.Barrier), o.Lookup.add(p.Lookup), o.Restore.add(p.Restore)
		o.MergeWrite, o.MergeRead = o.MergeWrite.add(p.MergeWrite), o.MergeRead.add(p.MergeRead)
	}
	out := make([]Operator, 0, len(by))
	for _, name := range slices.Sorted(maps.Keys(by)) {
		o := by[name]
		if none[name] {
			o.Watermark = nil
		}
		for _, b := range runs[name] {
			o.StoredBytes += b
		}
		o.CheckpointBytes = carried[name]
		out = append(out, *o)
	}
	return out
}

// KeptWindows are the windows each operator kept as of rec, for those whose
// state rec names: an attempt that restores it counts on from them. Nil rec
// has none.
func KeptWindows(rec *Record) map[string]int64 {
	out := map[string]int64{}
	if rec == nil {
		return out
	}
	for _, o := range rec.Operators {
		if slices.ContainsFunc(rec.Snapshots, func(s Snapshot) bool { return s.Operator == o.Operator }) {
			out[o.Operator] = o.Windows
		}
	}
	return out
}
