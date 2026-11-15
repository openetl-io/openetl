package plan

import (
	"fmt"
	"slices"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
)

// Every edge of a job's dataflow has a partitioning: how its rows are spread
// over the lanes of the operator it feeds. Each operator states what it needs
// of that edge, and an exchange by key group sits only where an edge's
// partitioning doesn't meet its operator's need. A partitioning by columns K
// meets a need for columns J when K is a subset of J: rows with equal values
// of J have equal values of K, so they're on one lane. An operator that
// exchanged rows by its group keys k therefore feeds a writer that needs
// (window_start, k) with no second exchange.

// PartitionKind is how an edge spreads rows with equal values of its
// columns over lanes.
type PartitionKind int

const (
	// Unpartitioned: rows with equal values may be on any lane.
	Unpartitioned PartitionKind = iota
	// BySplits: the source's splits partition the stream by the columns, and
	// each split's rows stay on one lane.
	BySplits
	// ByKeyGroup: each lane owns a contiguous range of the key groups of the
	// columns' values (openetl.kg.v1), and an exchange sent it their rows.
	ByKeyGroup
	// OneLane: every row is on one lane, as where lanes merge into an
	// operator that runs on one.
	OneLane
)

// Partitioning is an edge's.
type Partitioning struct {
	Kind PartitionKind
	// With BySplits and ByKeyGroup: rows with equal values of these are on
	// one lane. Never empty for them.
	Columns []string
}

// Need is what an operator needs of the edge that feeds it: that rows with
// equal values of Columns are on one of its lanes. Without Columns it needs
// nothing, unless KeyGroups.
type Need struct {
	Columns []string
	// The operator keeps state by key group, so only lanes that own key
	// groups of the columns' values meet it, not splits; without Columns,
	// every row on one lane.
	KeyGroups bool
}

// Meets reports whether p meets n: n needs nothing, every row is on one lane,
// or p partitions by a subset of the columns n needs, by key group where n
// keeps state by key group.
func (p Partitioning) Meets(n Need) bool {
	switch {
	case p.Kind == OneLane, len(n.Columns) == 0 && !n.KeyGroups:
		return true
	case p.Kind == Unpartitioned, len(p.Columns) == 0, len(n.Columns) == 0:
		return false
	case n.KeyGroups && p.Kind != ByKeyGroup:
		return false
	}
	for _, c := range p.Columns {
		if !slices.Contains(n.Columns, c) {
			return false
		}
	}
	return true
}

func (p Partitioning) String() string {
	switch p.Kind {
	case BySplits:
		return fmt.Sprintf("split by %v", p.Columns)
	case ByKeyGroup:
		return fmt.Sprintf("by key group on %v", p.Columns)
	case OneLane:
		return "on one lane"
	}
	return "unpartitioned"
}

// SourcePartitioning is how a stream's source spreads its rows over lanes:
// by its primary key where its splits partition it by key, since a split
// stays on its lane, and otherwise not at all. A changelog's (cdc) never is:
// its changes are one log, whose rows spread over the writer lanes by key
// group.
func SourcePartitioning(key []string, splitsByKey, changelog bool) Partitioning {
	if len(key) == 0 || !splitsByKey || changelog {
		return Partitioning{}
	}
	return Partitioning{Kind: BySplits, Columns: slices.Clone(key)}
}

// OperatorKind is what an operator on a stream's way to the destination is.
type OperatorKind int

const (
	OperatorStatement OperatorKind = iota + 1
	OperatorWriter
)

// Operator is a step on a stream's way from its source to the destination.
type Operator struct {
	Kind OperatorKind
	// A statement's place in transform.sql.
	At   sqlplan.Position
	Need Need
	// It runs on one lane, so every lane before it merges into it: the
	// writer of a destination with one writer lane.
	OneLane bool
	// The columns of its input that reach its output unchanged, each with
	// its name there. A partitioning by any other column ends at it.
	Passes map[string]string
}

// WriterOperator is the destination's writer of a stream with primary key
// key: it needs every row of a key on one writer lane. With oneLane, the
// destination writes on one writer lane, which every lane merges into.
func WriterOperator(key []string, oneLane bool) Operator {
	return Operator{Kind: OperatorWriter, Need: Need{Columns: slices.Clone(key)}, OneLane: oneLane}
}

// StatementOperator is a SQL statement's operator, with the need and the
// columns planning gave it.
func StatementOperator(st sqlplan.Statement) Operator {
	return Operator{Kind: OperatorStatement, At: st.At, Need: Need{Columns: slices.Clone(st.Need), KeyGroups: st.Operator != ""}, Passes: st.Passes}
}

// Path is a stream's way from its source to the destination, its exchanges
// placed.
type Path struct {
	// What the source gives.
	Source Partitioning
	Steps  []Step
}

// Step is an operator on a path, and the edge that feeds it.
type Step struct {
	Operator
	// The edge's partitioning, after the exchange before the operator, if
	// there is one.
	In Partitioning
	// The rows are exchanged by key group on the operator's need before it,
	// since the edge's partitioning didn't meet it.
	Exchange bool
}

// Place places a stream's exchanges: one before each operator whose need its
// input's partitioning doesn't meet, and none elsewhere. Lanes that merge
// into an operator on one lane meet every need without one.
func Place(source Partitioning, ops ...Operator) Path {
	path := Path{Source: source}
	p := source
	for i, op := range ops {
		st := Step{Operator: op}
		switch {
		case op.OneLane:
			p = Partitioning{Kind: OneLane}
		case !p.Meets(op.Need) && len(op.Need.Columns) == 0:
			// Rows exchanged by no columns all go to one key group's lane.
			st.Exchange = true
			p = Partitioning{Kind: OneLane}
		case !p.Meets(op.Need):
			st.Exchange = true
			p = Partitioning{Kind: ByKeyGroup, Columns: slices.Clone(op.Need.Columns)}
		}
		st.In = p
		path.Steps = append(path.Steps, st)
		if i < len(ops)-1 {
			p = through(p, op.Passes)
		}
	}
	return path
}

// through is p past an operator that passes the columns in passes: the same,
// with its columns renamed, while each of them passes; otherwise
// unpartitioned. Rows on one lane stay there.
func through(p Partitioning, passes map[string]string) Partitioning {
	if p.Kind == OneLane || p.Kind == Unpartitioned {
		return p
	}
	out := Partitioning{Kind: p.Kind}
	for _, c := range p.Columns {
		to, ok := passes[c]
		if !ok {
			return Partitioning{}
		}
		out.Columns = append(out.Columns, to)
	}
	return out
}

// Out is the partitioning of the last operator's output.
func (p Path) Out() Partitioning {
	if len(p.Steps) == 0 {
		return p.Source
	}
	last := p.Steps[len(p.Steps)-1]
	return through(last.In, last.Passes)
}

// Exchanges are the path's steps that an exchange precedes, by index.
func (p Path) Exchanges() []int {
	var out []int
	for i, s := range p.Steps {
		if s.Exchange {
			out = append(out, i)
		}
	}
	return out
}

// API is the path as the API's plan of a stream has it.
func (p Path) API() (*apiv1.Partitioning, []*apiv1.PlanOperator) {
	var ops []*apiv1.PlanOperator
	for _, s := range p.Steps {
		op := &apiv1.PlanOperator{Needs: s.Need.Columns, Exchange: s.Exchange, Partitioning: s.In.API()}
		switch s.Kind {
		case OperatorStatement:
			op.Kind, op.Line, op.Column = apiv1.PlanOperatorKind_PLAN_OPERATOR_KIND_STATEMENT, s.At.Line, s.At.Column
		case OperatorWriter:
			op.Kind = apiv1.PlanOperatorKind_PLAN_OPERATOR_KIND_WRITER
		}
		ops = append(ops, op)
	}
	return p.Source.API(), ops
}

// API is p as the API has it.
func (p Partitioning) API() *apiv1.Partitioning {
	kinds := map[PartitionKind]apiv1.PartitioningKind{
		Unpartitioned: apiv1.PartitioningKind_PARTITIONING_KIND_NONE,
		BySplits:      apiv1.PartitioningKind_PARTITIONING_KIND_SPLITS,
		ByKeyGroup:    apiv1.PartitioningKind_PARTITIONING_KIND_KEY_GROUP,
		OneLane:       apiv1.PartitioningKind_PARTITIONING_KIND_ONE_LANE,
	}
	return &apiv1.Partitioning{Kind: kinds[p.Kind], Columns: p.Columns}
}
