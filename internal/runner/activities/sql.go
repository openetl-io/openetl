package activities

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/sqlplan"
	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// planSQL plans a SQL transform's statements against the job's streams.
type planSQL func(ctx context.Context, sql string, streams []runner.Stream) (sqlplan.Answer, error)

// withRestored plans with engine, as engineclient.Plan does, against each
// stream's declared schema and the one the record the attempt would restore
// has, without claiming the pipeline: a column the source no longer declares
// is still in the stream, as nulls. Where HEAD's record can't be read,
// that's the one before it. A log that has neither is the attempt's to fail
// on.
func withRestored(engine func(context.Context, sqlplan.Request) (sqlplan.Answer, error), log *checkpoint.Log) planSQL {
	return func(ctx context.Context, sql string, streams []runner.Stream) (sqlplan.Answer, error) {
		rec, err := log.Restorable(ctx)
		if err != nil && !errors.Is(err, checkpoint.ErrUnreadable) {
			return sqlplan.Answer{}, err
		}
		restored := map[string][]byte{}
		if rec != nil {
			for name, st := range rec.Streams {
				restored[name] = st.Schema
			}
		}
		req, unknown, err := sqlRequest(sql, streams, restored)
		if err != nil {
			return sqlplan.Answer{}, err
		}
		a, err := engine(ctx, req)
		if err != nil {
			return a, err
		}
		return withUnknown(a, unknown), nil
	}
}

// sqlRequest is what planning sql needs: each stream it may name, as the
// job's start has it. unknown are those whose columns neither the source
// declares nor a sync recorded; they go without columns, so a statement may
// still write their names.
func sqlRequest(sql string, streams []runner.Stream, restored map[string][]byte) (req sqlplan.Request, unknown []string, err error) {
	req.SQL = sql
	for _, s := range streams {
		if !sqlplan.Mentions(sql, s.Name) {
			continue
		}
		st := sqlplan.Stream{Name: s.Name, Changelog: s.Read == source.CDC, Nanoseconds: s.NanosecondTimestamps, RestoredSchema: restored[s.Name]}
		if d := s.Declared(); d != nil {
			if st.DeclaredSchema, err = arrowipc.EncodeSchema(d); err != nil {
				return req, nil, fmt.Errorf("runner: stream %s's declared schema: %w", s.Name, err)
			}
		}
		if st.DeclaredSchema == nil && st.RestoredSchema == nil {
			unknown = append(unknown, s.Name)
		} else {
			st.Key = s.PrimaryKey
		}
		req.Streams = append(req.Streams, st)
	}
	return req, unknown, nil
}

// withUnknown is a with errors where a statement reads a stream of unknown,
// or where the SQL didn't plan and such a stream may be why.
func withUnknown(a sqlplan.Answer, unknown []string) sqlplan.Answer {
	if len(unknown) == 0 {
		return a
	}
	why := func(names []string) string {
		subject := "stream " + names[0] + " has"
		if len(names) > 1 {
			subject = "streams " + strings.Join(names, ", ") + " have"
		}
		return subject + " no known columns: a statement reads a stream whose source declares its columns, or one an earlier sync has recorded"
	}
	if len(a.Errors) > 0 {
		a.Errors = append(a.Errors, sqlplan.Error{Kind: "name", Message: why(unknown)})
		return a
	}
	var reads []sqlplan.Error
	for _, st := range a.Statements {
		var names []string
		for _, in := range st.Inputs {
			if slices.Contains(unknown, in) {
				names = append(names, in)
			}
		}
		if len(names) > 0 {
			reads = append(reads, sqlplan.Error{Kind: "name", At: &st.At, Message: why(names)})
		}
	}
	if len(reads) > 0 {
		return sqlplan.Answer{Errors: reads}
	}
	return a
}

// sqlOutputs plans a SQL transform's statements against the streams they
// read, and gives the destination's streams: each statement's output, in
// the order they're written, then each stream that passes through. A
// statement's output is written as plan.SQLOutputs says, and is read as
// its stream is; one passed through is that stream, and is delivered as it
// would be without the transform.
func sqlOutputs(ctx context.Context, p *apiv1.PipelineSpec, caps destination.Capabilities, deletes destination.DeleteMode,
	planner planSQL, streams []runner.Stream, inputs []plan.Input) (*runner.SQLTransform, []runner.Stream, error) {
	t := p.GetTransform().GetSql()
	if planner == nil {
		return nil, nil, errs.New(errs.ConfigInvalid, "transform.sql: this runner can't plan SQL")
	}
	a, err := planner(ctx, t.GetSql(), streams)
	if err != nil {
		return nil, nil, err
	}
	if len(a.Errors) > 0 {
		msgs := make([]string, len(a.Errors))
		for i, e := range a.Errors {
			msgs[i] = e.Error()
		}
		return nil, nil, errs.New(errs.ConfigInvalid, "%s", strings.Join(msgs, "\n"))
	}
	stream := func(name string) (runner.Stream, plan.Input, error) {
		si := slices.IndexFunc(streams, func(s runner.Stream) bool { return s.Name == name })
		ii := slices.IndexFunc(inputs, func(in plan.Input) bool { return in.Name == name })
		if si < 0 || ii < 0 {
			return runner.Stream{}, plan.Input{}, errs.New(errs.Internal, "runner: %q isn't a stream of the job", name)
		}
		return streams[si], inputs[ii], nil
	}
	var outputs []runner.Stream
	var expected []runner.ExpectedOutput
	outs, reads := plan.SQLOutputs(p, caps, &a, inputs)
	for i, st := range a.Statements {
		o := outs[i]
		if len(st.Inputs) != 1 {
			return nil, nil, errs.New(errs.Internal, "runner: the statement at %s reads %d streams", st.At, len(st.Inputs))
		}
		s, _, err := stream(st.Inputs[0])
		if err != nil {
			return nil, nil, err
		}
		if o.Mode == destination.Merge && len(o.PrimaryKey) == 0 {
			return nil, nil, errs.New(errs.ConfigInvalid,
				"destination.write: merge needs a primary key, and output %q has none: its statement doesn't keep stream %q's", o.Name, s.Name)
		}
		if err := plan.OutputProblem(o, reads[i]); err != nil {
			return nil, nil, errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		schema, err := arrowipc.DecodeSchema(st.ArrowSchema)
		if err != nil {
			return nil, nil, errs.Wrap(errs.Internal, err, fmt.Sprintf("runner: output %s's schema", o.Name))
		}
		expected = append(expected, runner.ExpectedOutput{Name: o.Name, Schema: schema, PrimaryKey: o.PrimaryKey, Input: s.Name})
		outputs = append(outputs, runner.Stream{
			Name:                 o.Name,
			Replay:               s.Replay,
			Read:                 s.Read,
			Mode:                 o.Mode,
			PrimaryKey:           o.PrimaryKey,
			NanosecondTimestamps: s.NanosecondTimestamps,
			Require:              o.Require,
			Deletes:              deletes,
			AllowTypeChanges:     plan.TypeChanges(p, nil),
			Transformed:          true,
			SplitsByKey:          s.SplitsByKey,
			Table:                p.GetDestination().GetTables()[o.Name],
			From:                 s.Name,
		})
	}
	for _, s := range streams {
		if !plan.Lands(p, &a, s.Name) {
			continue
		}
		_, in, err := stream(s.Name)
		if err != nil {
			return nil, nil, err
		}
		mode := plan.StreamMode(p, caps, in.Read, len(s.PrimaryKey) > 0)
		if err := plan.SyncProblem(mode, s.Name, in.Read); err != nil {
			return nil, nil, errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		outputs = append(outputs, runner.Stream{
			Name:                 s.Name,
			Replay:               s.Replay,
			Read:                 s.Read,
			Mode:                 mode,
			PrimaryKey:           s.PrimaryKey,
			NanosecondTimestamps: s.NanosecondTimestamps,
			Require:              s.Require,
			Deletes:              deletes,
			AllowTypeChanges:     plan.TypeChanges(p, nil),
			SplitsByKey:          s.SplitsByKey,
			Table:                p.GetDestination().GetTables()[s.Name],
			From:                 s.Name,
		})
	}
	for _, name := range slices.Sorted(maps.Keys(p.GetDestination().GetTables())) {
		if !slices.ContainsFunc(outputs, func(o runner.Stream) bool { return o.Name == name }) {
			return nil, nil, errs.New(errs.ConfigInvalid, "destination.tables names %q, which is neither a statement's output nor a stream passed through", name)
		}
	}
	if len(outputs) == 0 {
		return nil, nil, errs.New(errs.ConfigInvalid, "transform.sql: the statements write nothing")
	}
	return &runner.SQLTransform{SQL: t.GetSql(), Expected: expected}, outputs, nil
}
