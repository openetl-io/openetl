package attempt

import (
	"errors"
	"maps"
	"slices"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// Ready is what a share carries of its worker's dataflow's part of an
// epoch.
type Ready struct {
	// As openetl.engine.v1.EpochReady's.
	Schemas    map[string][]byte
	Snapshots  []checkpoint.Snapshot
	Quarantine []checkpoint.QuarantineFile
	// As openetl.engine.v1.EpochReady's: a SQL transform's outputs'
	// schemas, the schemas the destination gets where columns are held
	// whole, and what the destination's streams hold.
	OutputSchemas map[string][]byte
	Delivered     map[string][]byte
	Holds         map[string]*Holds
	// Each split's watermark at its lane's barrier, by stream and split.
	Watermarks map[string]map[string]int64
	// By stream a group window reads: its watermark past the exchange at
	// the keyed lanes' barrier.
	WindowMarks map[string]int64
	// As openetl.engine.v1.EpochReady's.
	Operators []checkpoint.OperatorPart
}

// Holds are a stream's holds at its barrier.
type Holds struct {
	// Every column held whole, without Since.
	Holding []checkpoint.ColumnHold
	// The epoch's held cells by column, without Since.
	Cells map[string]checkpoint.CellHold
	// Below the held values' URL.
	Files []destination.StagedFile
	// Nil without a replay.
	Replayed *ReplayProgress
}

type ReplayProgress struct {
	File           int
	Row, StillHeld uint64
	Done           bool
}

// EpochReady is r as a share of epoch e carries it.
func (r Ready) EpochReady(e *protocolv1.Epoch) *enginev1.EpochReady {
	p := &enginev1.EpochReady{Epoch: e, Schemas: r.Schemas, OutputSchemas: r.OutputSchemas, Delivered: r.Delivered, Holds: holdsProto(r.Holds), Watermarks: watermarksProto(r.Watermarks),
		WindowWatermarks: r.WindowMarks}
	for _, s := range r.Snapshots {
		p.Snapshots = append(p.Snapshots, SnapshotProto(s))
	}
	for _, q := range r.Quarantine {
		p.Quarantine = append(p.Quarantine, QuarantineProto(q))
	}
	for _, o := range r.Operators {
		p.Operators = append(p.Operators, OperatorProto(o))
	}
	return p
}

// OperatorOf is an operator's part of an epoch as the engine reports it.
func OperatorOf(p *enginev1.OperatorEpoch) checkpoint.OperatorPart {
	req := func(r *enginev1.StateRequests) checkpoint.Requests {
		return checkpoint.Requests{Count: r.GetCount(), Bytes: r.GetBytes()}
	}
	return checkpoint.OperatorPart{
		Operator: p.GetOperator(), RowsIn: p.GetRowsIn(), Inserts: p.GetInserts(), Updates: p.GetUpdates(), Deletes: p.GetDeletes(),
		Late: p.GetLate(), Opened: p.GetWindowsOpened(), Dropped: p.GetWindowsDropped(), Watermark: p.Watermark,
		MemoryBytes: p.GetMemoryBytes(), Runs: p.GetRuns(), Barrier: req(p.GetBarrier()), Lookup: req(p.GetLookup()), Restore: req(p.GetRestore()),
		MergeWrite: req(p.GetMergeWrite()), MergeRead: req(p.GetMergeRead()), Disk: req(p.GetDisk()),
		Steps: StepsOf(p.GetSteps()),
	}
}

// StepsOf are keyed lanes' steps of a window's watermark as the engine
// reports them; nil for none.
func StepsOf(ps []*enginev1.LaneSteps) []checkpoint.LaneSteps {
	var out []checkpoint.LaneSteps
	for _, p := range ps {
		kg := p.GetKeyGroups()
		l := checkpoint.LaneSteps{KeyGroups: checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())}}
		for _, s := range p.GetSplits() {
			steps := make([]checkpoint.Step, 0, len(s.GetPlaces()))
			for i, place := range s.GetPlaces() {
				if i < len(s.GetWatermarks()) {
					steps = append(steps, checkpoint.Step{Place: place, Watermark: s.GetWatermarks()[i]})
				}
			}
			l.Splits = append(l.Splits, checkpoint.SplitSteps{Split: s.GetSplit(), Steps: steps})
		}
		out = append(out, l)
	}
	return out
}

// StepsProto are keyed lanes' steps as the engine reports them.
func StepsProto(ls []checkpoint.LaneSteps) []*enginev1.LaneSteps {
	var out []*enginev1.LaneSteps
	for _, l := range ls {
		p := &enginev1.LaneSteps{KeyGroups: KeyGroups(l.KeyGroups.First, l.KeyGroups.Last)}
		for _, s := range l.Splits {
			sp := &enginev1.SplitSteps{Split: s.Split}
			for _, st := range s.Steps {
				sp.Places, sp.Watermarks = append(sp.Places, st.Place), append(sp.Watermarks, st.Watermark)
			}
			p.Splits = append(p.Splits, sp)
		}
		out = append(out, p)
	}
	return out
}

// OperatorProto is o as the engine reports it.
func OperatorProto(o checkpoint.OperatorPart) *enginev1.OperatorEpoch {
	req := func(r checkpoint.Requests) *enginev1.StateRequests {
		return &enginev1.StateRequests{Count: r.Count, Bytes: r.Bytes}
	}
	return &enginev1.OperatorEpoch{
		Operator: o.Operator, RowsIn: o.RowsIn, Inserts: o.Inserts, Updates: o.Updates, Deletes: o.Deletes,
		Late: o.Late, WindowsOpened: o.Opened, WindowsDropped: o.Dropped, Watermark: o.Watermark,
		MemoryBytes: o.MemoryBytes, Runs: o.Runs, Barrier: req(o.Barrier), Lookup: req(o.Lookup), Restore: req(o.Restore),
		MergeWrite: req(o.MergeWrite), MergeRead: req(o.MergeRead), Disk: req(o.Disk),
		Steps: StepsProto(o.Steps),
	}
}

// ReadyOf is what a share's EpochReady carries. It fails on what a worker
// doesn't run: staged files.
func ReadyOf(p *enginev1.EpochReady) (Ready, error) {
	if len(p.GetFiles()) > 0 || len(p.GetStaged()) > 0 {
		return Ready{}, errors.New("a worker's share holds what runs on one pod: staged files")
	}
	r := Ready{Schemas: p.GetSchemas(), OutputSchemas: p.GetOutputSchemas(), Delivered: p.GetDelivered(), Holds: HoldsOf(p.GetHolds()), Watermarks: WatermarksOf(p.GetWatermarks()),
		WindowMarks: WindowMarksOf(p.GetWindowWatermarks())}
	for _, s := range p.GetSnapshots() {
		r.Snapshots = append(r.Snapshots, SnapshotOf(s))
	}
	for _, q := range p.GetQuarantine() {
		r.Quarantine = append(r.Quarantine, QuarantineOf(q))
	}
	for _, o := range p.GetOperators() {
		r.Operators = append(r.Operators, OperatorOf(o))
	}
	return r, nil
}

// QuarantineOf is a quarantine file as the engine reports it.
func QuarantineOf(q *enginev1.QuarantineFile) checkpoint.QuarantineFile {
	f := checkpoint.QuarantineFile{Stream: q.GetStream(), Output: q.GetOutput(), URL: q.GetUrl(), Rows: q.GetRows()}
	for _, c := range q.GetCauses() {
		f.Causes = append(f.Causes, checkpoint.QuarantineCause{Code: c.GetCode(), Column: c.GetColumn(), NotHeld: c.GetNotHeld(), Rows: c.GetRows()})
	}
	return f
}

// QuarantineProto is q as the engine reports it.
func QuarantineProto(q checkpoint.QuarantineFile) *enginev1.QuarantineFile {
	p := &enginev1.QuarantineFile{Stream: q.Stream, Url: q.URL, Rows: q.Rows, Output: q.Output}
	for _, c := range q.Causes {
		p.Causes = append(p.Causes, &enginev1.QuarantineCause{Code: c.Code, Column: c.Column, NotHeld: c.NotHeld, Rows: c.Rows})
	}
	return p
}

// KeyGroups is key groups first to last as the protocol carries them.
func KeyGroups(first, last int) *enginev1.KeyGroupRange {
	return &enginev1.KeyGroupRange{First: ints.Uint32(first), Last: ints.Uint32(last)}
}

// SnapshotProto is a lane's snapshot as the engine and a share carry it.
func SnapshotProto(s checkpoint.Snapshot) *enginev1.StateSnapshot {
	return &enginev1.StateSnapshot{Operator: s.Operator, KeyGroups: KeyGroups(s.KeyGroups.First, s.KeyGroups.Last),
		Layout: s.Layout, Data: s.Data, Runs: s.Runs}
}

// SnapshotOf is a lane's snapshot as the record keeps it.
func SnapshotOf(p *enginev1.StateSnapshot) checkpoint.Snapshot {
	kg := p.GetKeyGroups()
	return checkpoint.Snapshot{Operator: p.GetOperator(), KeyGroups: checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())},
		Layout: p.GetLayout(), Data: p.GetData(), Runs: p.GetRuns()}
}

// WindowMarksOf are the windows' watermarks as the engine reports them at
// their keyed lanes' barriers, by stream; nil for none.
func WindowMarksOf(m map[string]int64) map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	return maps.Clone(m)
}

// WatermarksOf are the splits' watermarks as the engine reports them at
// their lanes' barriers, by stream and split; nil for none.
func WatermarksOf(m map[string]*enginev1.SplitWatermarks) map[string]map[string]int64 {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]map[string]int64, len(m))
	for name, w := range m {
		out[name] = maps.Clone(w.GetSplits())
	}
	return out
}

func watermarksProto(m map[string]map[string]int64) map[string]*enginev1.SplitWatermarks {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*enginev1.SplitWatermarks, len(m))
	for name, w := range m {
		out[name] = &enginev1.SplitWatermarks{Splits: maps.Clone(w)}
	}
	return out
}

// HoldsOf are holds as the engine reports them at its streams' barriers;
// nil for none.
func HoldsOf(m map[string]*enginev1.StreamHolds) map[string]*Holds {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*Holds, len(m))
	for name, h := range m {
		rh := &Holds{Holding: ColumnHoldsOf(h.GetHolding())}
		for col, c := range h.GetCells() {
			if rh.Cells == nil {
				rh.Cells = map[string]checkpoint.CellHold{}
			}
			rh.Cells[col] = checkpoint.CellHold{Cells: c.GetCells(), Reason: c.GetReason()}
		}
		for _, f := range h.GetFiles() {
			rh.Files = append(rh.Files, destination.StagedFile{Path: f.GetPath(), Rows: f.GetRows(), Bytes: f.GetBytes()})
		}
		if p := h.GetReplayed(); p != nil {
			rh.Replayed = &ReplayProgress{File: int(p.GetFile()), Row: p.GetRow(), StillHeld: p.GetStillHeld(), Done: p.GetDone()}
		}
		out[name] = rh
	}
	return out
}

func holdsProto(m map[string]*Holds) map[string]*enginev1.StreamHolds {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]*enginev1.StreamHolds, len(m))
	for _, name := range slices.Sorted(maps.Keys(m)) {
		h := m[name]
		sh := &enginev1.StreamHolds{Holding: ColumnHolds(h.Holding)}
		for col, c := range h.Cells {
			if sh.Cells == nil {
				sh.Cells = map[string]*enginev1.CellHolds{}
			}
			sh.Cells[col] = &enginev1.CellHolds{Cells: c.Cells, Reason: c.Reason}
		}
		for _, f := range h.Files {
			sh.Files = append(sh.Files, &protocolv1.StagedFile{Path: f.Path, Rows: f.Rows, Bytes: f.Bytes})
		}
		if p := h.Replayed; p != nil {
			sh.Replayed = &enginev1.ReplayProgress{File: ints.Uint32(p.File), Row: p.Row, StillHeld: p.StillHeld, Done: p.Done}
		}
		out[name] = sh
	}
	return out
}

var holdKinds = map[checkpoint.HoldKind]enginev1.HoldCause_Kind{
	checkpoint.HoldPolicy:      enginev1.HoldCause_KIND_POLICY,
	checkpoint.HoldRefused:     enginev1.HoldCause_KIND_REFUSED,
	checkpoint.HoldFixed:       enginev1.HoldCause_KIND_FIXED,
	checkpoint.HoldColumnLimit: enginev1.HoldCause_KIND_COLUMN_LIMIT,
	checkpoint.HoldGroup:       enginev1.HoldCause_KIND_GROUP,
}

// ColumnHold is a column held whole as the engine's protocol carries it.
// The destination's refusal isn't in it.
func ColumnHold(c checkpoint.ColumnHold) *enginev1.ColumnHold {
	h := &enginev1.ColumnHold{Column: c.Column, Delivered: c.Delivered, Reason: c.Reason}
	if c.Cause != nil {
		h.Cause = &enginev1.HoldCause{Kind: holdKinds[c.Cause.Kind], Policy: c.Cause.Policy, With: c.Cause.With, Source: c.Cause.Source, From: c.Cause.From, To: c.Cause.To}
	}
	return h
}

// ColumnHoldOf is a column held whole as the engine reported it.
func ColumnHoldOf(c *enginev1.ColumnHold) checkpoint.ColumnHold {
	return checkpoint.ColumnHold{Column: c.GetColumn(), Delivered: c.GetDelivered(), Reason: c.GetReason(), Cause: holdCause(c.GetCause())}
}

// ColumnHolds are columns held whole as the engine's protocol carries them.
func ColumnHolds(cs []checkpoint.ColumnHold) []*enginev1.ColumnHold {
	if len(cs) == 0 {
		return nil
	}
	out := make([]*enginev1.ColumnHold, 0, len(cs))
	for _, c := range cs {
		out = append(out, ColumnHold(c))
	}
	return out
}

// ColumnHoldsOf are columns held whole as the engine reported them; nil
// for none.
func ColumnHoldsOf(cs []*enginev1.ColumnHold) []checkpoint.ColumnHold {
	if len(cs) == 0 {
		return nil
	}
	out := make([]checkpoint.ColumnHold, 0, len(cs))
	for _, c := range cs {
		out = append(out, ColumnHoldOf(c))
	}
	return out
}

// holdCause is the engine's cause; the destination's refusal isn't in it.
func holdCause(c *enginev1.HoldCause) *checkpoint.HoldCause {
	if c == nil {
		return nil
	}
	out := &checkpoint.HoldCause{Policy: c.GetPolicy(), With: c.GetWith(), Source: c.GetSource(), From: c.GetFrom(), To: c.GetTo()}
	for k, v := range holdKinds {
		if v == c.GetKind() {
			out.Kind = k
		}
	}
	return out
}
