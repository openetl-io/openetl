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
	p := &enginev1.EpochReady{Epoch: e, Schemas: r.Schemas, OutputSchemas: r.OutputSchemas, Delivered: r.Delivered, Holds: holdsProto(r.Holds), Watermarks: watermarksProto(r.Watermarks)}
	for _, s := range r.Snapshots {
		snap := &enginev1.StateSnapshot{Operator: s.Operator, Url: s.URL}
		if kg := s.KeyGroups; kg != nil {
			snap.KeyGroups = KeyGroups(kg.First, kg.Last)
		}
		p.Snapshots = append(p.Snapshots, snap)
	}
	for _, q := range r.Quarantine {
		p.Quarantine = append(p.Quarantine, &enginev1.QuarantineFile{Stream: q.Stream, Url: q.URL, Rows: q.Rows, Output: q.Output})
	}
	return p
}

// ReadyOf is what a share's EpochReady carries. It fails on what a worker
// doesn't run: staged files.
func ReadyOf(p *enginev1.EpochReady) (Ready, error) {
	if len(p.GetFiles()) > 0 || len(p.GetStaged()) > 0 {
		return Ready{}, errors.New("a worker's share holds what runs on one pod: staged files")
	}
	r := Ready{Schemas: p.GetSchemas(), OutputSchemas: p.GetOutputSchemas(), Delivered: p.GetDelivered(), Holds: HoldsOf(p.GetHolds()), Watermarks: WatermarksOf(p.GetWatermarks())}
	for _, s := range p.GetSnapshots() {
		snap := checkpoint.Snapshot{Operator: s.GetOperator(), URL: s.GetUrl()}
		if kg := s.GetKeyGroups(); kg != nil {
			snap.KeyGroups = &checkpoint.KeyGroupRange{First: int(kg.GetFirst()), Last: int(kg.GetLast())}
		}
		r.Snapshots = append(r.Snapshots, snap)
	}
	for _, q := range p.GetQuarantine() {
		r.Quarantine = append(r.Quarantine, checkpoint.QuarantineFile{Stream: q.GetStream(), Output: q.GetOutput(), URL: q.GetUrl(), Rows: q.GetRows()})
	}
	return r, nil
}

// KeyGroups is key groups first to last as the protocol carries them.
func KeyGroups(first, last int) *enginev1.KeyGroupRange {
	return &enginev1.KeyGroupRange{First: ints.Uint32(first), Last: ints.Uint32(last)}
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
