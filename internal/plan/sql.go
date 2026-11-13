package plan

import (
	"slices"

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
		out.Statements = append(out.Statements, &apiv1.StatementPlan{
			Line: st.At.Line, Column: st.At.Column, Output: st.Output, Replaces: st.Replaces, Class: st.Class,
			Inputs: st.Inputs, Views: st.Views, Columns: schemaColumns(st.Columns), PrimaryKey: st.Key, Exchange: st.Exchange, Operator: st.Operator,
		})
	}
	for _, v := range a.Views {
		out.Views = append(out.Views, &apiv1.ViewPlan{
			Name: v.Name, Line: v.At.Line, Column: v.At.Column, Inputs: v.Inputs, Columns: schemaColumns(v.Columns), PrimaryKey: v.Key,
		})
	}
	for _, s := range selected(spec, cat) {
		if lands(spec, a, s.cs.GetName()) {
			out.PassedThrough = append(out.PassedThrough, s.cs.GetName())
		} else {
			out.Withheld = append(out.Withheld, s.cs.GetName())
		}
	}
	return out
}

// lands reports whether the source's stream reaches the destination as it
// is: no statement writes its name, and passthrough isn't none.
func lands(spec *apiv1.PipelineSpec, a *sqlplan.Answer, stream string) bool {
	if spec.GetTransform().GetSql().GetPassthrough() == apiv1.Passthrough_PASSTHROUGH_NONE {
		return false
	}
	return !slices.ContainsFunc(a.Statements, func(st sqlplan.Statement) bool { return st.Replaces && st.Output == stream })
}

// SQLPlan is what a pipeline version keeps of the answer.
func SQLPlan(a *sqlplan.Answer) *apiv1.SqlPlan {
	out := &apiv1.SqlPlan{}
	for _, st := range a.Statements {
		out.Outputs = append(out.Outputs, &apiv1.SqlOutput{Name: st.Output, Columns: schemaColumns(st.Columns), PrimaryKey: st.Key, ArrowSchema: st.ArrowSchema})
		if st.Operator != "" {
			out.Operators = append(out.Operators, st.Operator)
		}
	}
	return out
}

func schemaColumns(fs []sqlplan.Field) []*apiv1.SchemaColumn {
	out := make([]*apiv1.SchemaColumn, 0, len(fs))
	for _, f := range fs {
		out = append(out, &apiv1.SchemaColumn{Name: f.Name, Type: f.Type, Nullable: f.Nullable})
	}
	return out
}

// sqlOutputs are the outputs of a SQL transform's statements. An output that
// replaces a source's stream takes that stream's require, as its table in
// destination.tables. Without destination.write, it's overwritten when its
// statement's inputs are all read with full_refresh, merged when it has a
// primary key and an input is read with cdc, and appended to otherwise,
// where the destination has the mode.
func sqlOutputs(spec *apiv1.PipelineSpec, caps destination.Capabilities, a *sqlplan.Answer, inputs []Input) ([]Output, [][]Input) {
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
			if st.Replaces && in.Name == st.Output {
				o.Require = Level(in.Spec.GetRequire())
			}
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
