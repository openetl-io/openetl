package plan

import (
	"fmt"
	"slices"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// selected are the catalog's streams the pipeline reads, with the spec's
// settings for each it names: those source.streams lists, or else every
// one. A listed stream the catalog lacks is left out.
func selected(spec *apiv1.PipelineSpec, cat *apiv1.Catalog) []readStream {
	var out []readStream
	if len(spec.GetSource().GetStreams()) == 0 {
		for _, cs := range cat.GetStreams() {
			out = append(out, readStream{cs: cs})
		}
		return out
	}
	for _, ps := range spec.GetSource().GetStreams() {
		if i := slices.IndexFunc(cat.GetStreams(), func(cs *apiv1.CatalogStream) bool { return cs.GetName() == ps.GetName() }); i >= 0 {
			out = append(out, readStream{cs: cat.GetStreams()[i], ps: ps})
		}
	}
	return out
}

type readStream struct {
	cs *apiv1.CatalogStream
	ps *apiv1.PipelineStream
}

// SQLRequest is what planning the spec's transform.sql needs: the SQL, and
// each stream the pipeline reads that the SQL may name. A stream's columns
// are those the catalog declares, or else its newest schema in the
// pipeline's schema history, by stream; unknown names the streams neither
// has, which a sync's inference gives.
func SQLRequest(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, history map[string][]sqlplan.Column) (req sqlplan.Request, unknown []string) {
	sql := spec.GetTransform().GetSql().GetSql()
	req.SQL = sql
	for _, s := range selected(spec, cat) {
		name := s.cs.GetName()
		if !sqlplan.Mentions(sql, name) {
			continue
		}
		st := sqlplan.Stream{
			Name:        name,
			Key:         s.cs.GetPrimaryKey(),
			Changelog:   ReadMode(spec, s.cs.GetSyncModes(), s.ps) == apiv1.SyncMode_SYNC_MODE_CDC,
			Nanoseconds: s.ps.GetTimestamps() == apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND,
			EventTime:   s.ps.GetEventTime(),
		}
		for _, c := range s.cs.GetColumns() {
			st.Columns = append(st.Columns, sqlplan.Column{Name: c.GetName(), Type: c.GetType()})
		}
		if len(st.Columns) == 0 {
			st.Columns = history[name]
		}
		if len(st.Columns) == 0 {
			unknown = append(unknown, name)
			continue
		}
		req.Streams = append(req.Streams, st)
	}
	return req, unknown
}

// Transform is the plan of a SQL transform whose statements planned: each
// statement and view, and which of the streams the pipeline reads reach the
// destination.
func Transform(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, a *sqlplan.Answer) *apiv1.TransformPlan {
	out := &apiv1.TransformPlan{}
	for _, st := range a.Statements {
		sp := &apiv1.StatementPlan{
			Line: st.At.Line, Column: st.At.Column, Output: st.Output, Replaces: st.Replaces, Class: st.Class,
			Inputs: st.Inputs, Views: st.Views, Columns: schemaColumns(st.Columns), PrimaryKey: st.Key, Operator: st.Operator,
		}
		if p := Place(statementSource(spec, cat, st), StatementOperator(st)); p.Steps[0].Exchange {
			sp.Exchange = st.Need
		}
		out.Statements = append(out.Statements, sp)
	}
	for _, v := range a.Views {
		out.Views = append(out.Views, &apiv1.ViewPlan{
			Name: v.Name, Line: v.At.Line, Column: v.At.Column, Inputs: v.Inputs, Columns: schemaColumns(v.Columns), PrimaryKey: v.Key,
		})
	}
	for _, s := range selected(spec, cat) {
		if Lands(spec, a, s.cs.GetName()) {
			out.PassedThrough = append(out.PassedThrough, s.cs.GetName())
		} else {
			out.Withheld = append(out.Withheld, s.cs.GetName())
		}
	}
	return out
}

// sourceOf is how the source of a stream the pipeline reads spreads its rows
// over lanes.
func sourceOf(spec *apiv1.PipelineSpec, cs *apiv1.CatalogStream, ps *apiv1.PipelineStream) Partitioning {
	return SourcePartitioning(cs.GetPrimaryKey(), cs.GetSplitsByKey(), ReadMode(spec, cs.GetSyncModes(), ps) == apiv1.SyncMode_SYNC_MODE_CDC)
}

// statementSource is how the stream a statement reads arrives at it:
// unpartitioned where it reads several, or one the catalog lacks.
func statementSource(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, st sqlplan.Statement) Partitioning {
	if len(st.Inputs) != 1 {
		return Partitioning{}
	}
	for _, s := range selected(spec, cat) {
		if s.cs.GetName() == st.Inputs[0] {
			return sourceOf(spec, s.cs, s.ps)
		}
	}
	return Partitioning{}
}

// StatementPath is the way of a statement's output, keyed by key, from the
// source of the stream it reads to the destination's writer, which writes
// on one writer lane with oneLane.
func StatementPath(source Partitioning, st sqlplan.Statement, key []string, oneLane bool) Path {
	return Place(source, StatementOperator(st), WriterOperator(key, oneLane))
}

// Lands reports whether the source's stream reaches the destination as it
// is: no statement writes its name, and passthrough isn't none.
func Lands(spec *apiv1.PipelineSpec, a *sqlplan.Answer, stream string) bool {
	if spec.GetTransform().GetSql().GetPassthrough() == apiv1.Passthrough_PASSTHROUGH_NONE {
		return false
	}
	return !slices.ContainsFunc(a.Statements, func(st sqlplan.Statement) bool { return st.Replaces && st.Output == stream })
}

// SQLSettingProblems fails the settings a SQL transform's streams set where
// they have no meaning. A stream's holds, collapse and typeChanges are
// those of the stream as the destination gets it: passed through, as
// without a transform, or the output of the statement that replaces it,
// which isn't collapsed, and holds only with a primary key. outs are the
// statements' outputs, as SQLOutputs gives them. A stream passed through is
// checked as one without a transform.
func SQLSettingProblems(spec *apiv1.PipelineSpec, caps destination.Capabilities, a *sqlplan.Answer, inputs []Input, outs []Output) []error {
	out := windowProblems(a, inputs, outs)
	for _, in := range inputs {
		ps := in.Spec
		set := landingSettings(ps)
		if len(set) == 0 || Lands(spec, a, in.Name) {
			continue
		}
		i := slices.IndexFunc(outs, func(o Output) bool { return o.Name == in.Name && o.Spec == ps })
		if i < 0 {
			out = append(out, fmt.Errorf("stream %q sets %s, which apply to it as the destination gets it, and the destination doesn't: "+
				"no statement writes %q, and transform.passthrough is none", in.Name, strings.Join(set, ", "), in.Name))
			continue
		}
		o := outs[i]
		if ps.Collapse != nil {
			out = append(out, fmt.Errorf("stream %q sets collapse, and a statement replaces it, whose output isn't collapsed", in.Name))
		}
		if AsksHolds(ps) && len(o.PrimaryKey) == 0 {
			out = append(out, fmt.Errorf("stream %q sets how it holds cells or columns, and the statement that replaces it gives its output no primary key; only a keyed output holds them", in.Name))
		} else if err := HoldProblem(caps, o.Mode, len(o.PrimaryKey) > 0, in.Name, ps); err != nil {
			out = append(out, err)
		}
	}
	return out
}

// windowProblems fail a group window over a stream whose reads would cut
// its windows short: one read in another order than its event time, other
// than whole, as each read's windows would hold only that read's rows. A
// window open at a sync's end goes on in the next sync, from the state the
// record names. And a stream's watermark.lateness where a group window over
// it writes an output that isn't merged: a row that comes late gives the
// window's row again, which only a merge takes as an update.
func windowProblems(a *sqlplan.Answer, inputs []Input, outs []Output) []error {
	var out []error
	for i, st := range a.Statements {
		if st.Operator == "" {
			continue
		}
		for _, in := range inputs {
			if !slices.Contains(st.Inputs, in.Name) {
				continue
			}
			column := in.Spec.GetEventTime()
			switch {
			case in.Read == apiv1.SyncMode_SYNC_MODE_FULL_REFRESH, column == "":
			case !OrderedRead(in.Read, in.Cursor, column):
				out = append(out, fmt.Errorf("the group window writing %q reads stream %q with %s by cursor %q, not by its eventTime %q, "+
					"so each sync's windows would hold only the rows that sync reads: read it with full_refresh", st.Output, in.Name, syncName(in.Read), in.Cursor, column))
			}
			if i < len(outs) && outs[i].Mode != destination.Merge && in.Spec.GetWatermark().GetLateness().AsDuration() > 0 {
				out = append(out, fmt.Errorf("stream %q sets watermark.lateness, so a row that comes late gives its window's row again, "+
					"and output %q isn't merged, which would keep both rows: set destination.write: merge, or no lateness", in.Name, st.Output))
			}
		}
	}
	return out
}

// landingSettings names the settings ps sets that apply to the stream as
// the destination gets it.
func landingSettings(ps *apiv1.PipelineStream) []string {
	if ps == nil {
		return nil
	}
	var set []string
	if ps.GetQuarantine() == apiv1.QuarantineScope_QUARANTINE_SCOPE_CELL {
		set = append(set, "quarantine")
	}
	held := ps.GetHeld() != nil
	for _, c := range ps.GetColumns() {
		held = held || c.GetHeld() != nil
	}
	if held {
		set = append(set, "held")
	}
	if len(ps.GetGroups()) > 0 {
		set = append(set, "groups")
	}
	if ps.Collapse != nil {
		set = append(set, "collapse")
	}
	if ps.GetTypeChanges() != apiv1.TypeChanges_TYPE_CHANGES_UNSPECIFIED {
		set = append(set, "typeChanges")
	}
	return set
}

// SQLPlan is what a pipeline version keeps of the answer. prev is what the
// version before it keeps, nil for none: the operators it keeps, or dropped,
// that the answer doesn't, the new version drops.
func SQLPlan(a *sqlplan.Answer, prev *apiv1.SqlPlan) *apiv1.SqlPlan {
	out := &apiv1.SqlPlan{}
	for _, st := range a.Statements {
		out.Outputs = append(out.Outputs, &apiv1.SqlOutput{Name: st.Output, Columns: schemaColumns(st.Columns), PrimaryKey: st.Key, ArrowSchema: st.ArrowSchema, Operator: st.Operator})
		if st.Operator != "" {
			out.Operators = append(out.Operators, st.Operator)
		}
	}
	for _, id := range slices.Concat(prev.GetOperators(), prev.GetDropped()) {
		if !slices.Contains(out.Operators, id) && !slices.Contains(out.Dropped, id) {
			out.Dropped = append(out.Dropped, id)
		}
	}
	slices.Sort(out.Dropped)
	return out
}

func schemaColumns(fs []sqlplan.Field) []*apiv1.SchemaColumn {
	out := make([]*apiv1.SchemaColumn, 0, len(fs))
	for _, f := range fs {
		out = append(out, &apiv1.SchemaColumn{Name: f.Name, Type: f.Type, Nullable: f.Nullable})
	}
	return out
}

// SQLOutputs are the outputs of a SQL transform's statements. An output that
// replaces a source's stream takes that stream's require, as its table in
// destination.tables. Without destination.write, it's overwritten when its
// statement's inputs are all read with full_refresh, merged when it has a
// primary key and an input is read with cdc, and appended to otherwise,
// where the destination has the mode.
func SQLOutputs(spec *apiv1.PipelineSpec, caps destination.Capabilities, a *sqlplan.Answer, inputs []Input) ([]Output, [][]Input) {
	var outs []Output
	var reads [][]Input
	for _, st := range a.Statements {
		var ins []Input
		for _, name := range st.Inputs {
			if i := slices.IndexFunc(inputs, func(in Input) bool { return in.Name == name }); i >= 0 {
				ins = append(ins, inputs[i])
			}
		}
		o := Output{Name: st.Output, PrimaryKey: st.Key, Exact: len(ins) > 0, Engine: "sql"}
		refresh, cdc := len(ins) > 0, false
		for _, in := range ins {
			o.Exact = o.Exact && in.Replay == source.Exact
			refresh = refresh && in.Read == apiv1.SyncMode_SYNC_MODE_FULL_REFRESH
			cdc = cdc || in.Read == apiv1.SyncMode_SYNC_MODE_CDC
		}
		if i := slices.IndexFunc(inputs, func(in Input) bool { return in.Name == st.Output }); st.Replaces && i >= 0 {
			o.Require, o.Spec = Level(inputs[i].Spec.GetRequire()), inputs[i].Spec
		}
		switch {
		case WriteMode(spec) != 0:
			o.Mode = WriteMode(spec)
		case refresh && caps.SupportsMode(destination.Overwrite):
			o.Mode = destination.Overwrite
		case cdc && len(o.PrimaryKey) > 0 && caps.SupportsMode(destination.Merge):
			o.Mode = destination.Merge
		default:
			o.Mode = destination.Append
		}
		outs = append(outs, o)
		reads = append(reads, ins)
	}
	return outs, reads
}
