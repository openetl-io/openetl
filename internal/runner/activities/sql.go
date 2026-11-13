package activities

import (
	"context"
	"maps"
	"slices"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// sqlOutputs plans a SQL transform's statements against the streams they
// read, and gives the destination's streams: each statement's output, in
// the order they're written, then each stream no statement writes, unless
// passthrough is none. An output is written as a stream would be that is
// read as its stream is and has its key.
func sqlOutputs(ctx context.Context, p *apiv1.PipelineSpec, caps destination.Capabilities, deletes destination.DeleteMode,
	planner runner.SQLPlanner, streams []runner.Stream, inputs []plan.Input) (*runner.SQLTransform, []runner.Stream, error) {
	t := p.GetTransform().GetSql()
	if planner == nil {
		return nil, nil, errs.New(errs.ConfigInvalid, "transform.sql: this runner can't plan SQL")
	}
	planned, err := planner.PlanSQL(ctx, t.GetSql(), streams)
	if err != nil {
		return nil, nil, err
	}
	output := func(name string, key []string, from string) (runner.Stream, error) {
		si := slices.IndexFunc(streams, func(s runner.Stream) bool { return s.Name == from })
		ii := slices.IndexFunc(inputs, func(in plan.Input) bool { return in.Name == from })
		if si < 0 || ii < 0 {
			return runner.Stream{}, errs.New(errs.Internal, "runner: output %q reads %q, which isn't a stream of the job", name, from)
		}
		s, in := streams[si], inputs[ii]
		mode := plan.StreamMode(p, caps, in.Read, len(key) > 0)
		if mode == destination.Merge && len(key) == 0 {
			return runner.Stream{}, errs.New(errs.ConfigInvalid,
				"transform.sql: output %q is written with merge, which needs a primary key, and its statement doesn't keep stream %q's", name, from)
		}
		if err := plan.SyncProblem(mode, name, in.Read); err != nil {
			return runner.Stream{}, errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		return runner.Stream{
			Name:                 name,
			Replay:               s.Replay,
			Read:                 s.Read,
			Mode:                 mode,
			PrimaryKey:           key,
			NanosecondTimestamps: s.NanosecondTimestamps,
			Require:              s.Require,
			Deletes:              deletes,
			AllowTypeChanges:     plan.TypeChanges(p, nil),
			Transformed:          true,
			SplitsByKey:          s.SplitsByKey,
			Table:                p.GetDestination().GetTables()[name],
			From:                 from,
		}, nil
	}
	var outputs []runner.Stream
	for _, e := range planned {
		o, err := output(e.Name, e.PrimaryKey, e.Input)
		if err != nil {
			return nil, nil, err
		}
		outputs = append(outputs, o)
	}
	if t.GetPassthrough() != apiv1.Passthrough_PASSTHROUGH_NONE {
		for _, s := range streams {
			if slices.ContainsFunc(planned, func(e runner.ExpectedOutput) bool { return e.Name == s.Name }) {
				continue
			}
			o, err := output(s.Name, s.PrimaryKey, s.Name)
			if err != nil {
				return nil, nil, err
			}
			outputs = append(outputs, o)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(p.GetDestination().GetTables())) {
		if !slices.ContainsFunc(outputs, func(o runner.Stream) bool { return o.Name == name }) {
			return nil, nil, errs.New(errs.ConfigInvalid, "destination.tables names %q, which is neither a statement's output nor a stream passed through", name)
		}
	}
	if len(outputs) == 0 {
		return nil, nil, errs.New(errs.ConfigInvalid, "transform.sql: the statements write nothing")
	}
	return &runner.SQLTransform{SQL: t.GetSql(), Expected: planned}, outputs, nil
}
