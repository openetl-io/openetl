package activities

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/plan"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// buildJob binds and starts the connectors, and settles the settings. A SQL
// transform's statements are planned with planner. The caller sets the
// attempt's owner, log and dataflow, and closes the connectors, even after
// an error.
func buildJob(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse, env connectorhost.Env, planner planSQL) (runner.Job, error) {
	p := spec.GetPipeline()
	env.RateLimitScale = spec.GetRateLimitScale()
	srcCfg, err := protojson.Marshal(spec.GetSource().GetConfig())
	if err != nil {
		return runner.Job{}, err
	}
	srcName, _ := connectorhost.SplitRef(spec.GetSource().GetConnector())
	src, err := connectorhost.NewSource(srcName, srcCfg, env)
	if err != nil {
		return runner.Job{}, err
	}
	job := runner.Job{Source: src}
	dstCfg, err := protojson.Marshal(spec.GetDestination().GetConfig())
	if err != nil {
		return job, err
	}
	dstName, _ := connectorhost.SplitRef(spec.GetDestination().GetConnector())
	dst, err := connectorhost.NewDestination(dstName, dstCfg, env)
	if err != nil {
		return job, err
	}
	job.Destination = dst
	if err := errors.Join(connectorhost.Start(ctx, src), connectorhost.Start(ctx, dst)); err != nil {
		return job, err
	}
	if err := connectorhost.Setup(ctx, spec.GetPipelineId(), src, dst); err != nil {
		return job, err
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return job, err
	}

	deletes, caps := plan.Deletes(p), dst.Capabilities()
	transformed := p.GetTransform() != nil
	// A transform reads every stream; an Apply that adds one resumes them.
	paused := map[string]bool{}
	if !transformed {
		for _, name := range spec.GetPausedStreams() {
			paused[name] = true
		}
	}
	var streams, pausedStreams []runner.Stream
	add := func(s runner.Stream) {
		if paused[s.Name] {
			pausedStreams = append(pausedStreams, s)
		} else {
			streams = append(streams, s)
		}
	}
	var inputs []plan.Input
	stream := func(cs source.CatalogStream, ps *apiv1.PipelineStream) (runner.Stream, error) {
		declared := make([]apiv1.SyncMode, len(cs.SyncModes))
		for i, m := range cs.SyncModes {
			declared[i] = ints.Enum[apiv1.SyncMode](m)
		}
		read := plan.ReadMode(p, declared, ps)
		replay, cursor := plan.Replay(cs.Replay, read), cmp.Or(ps.GetCursor(), cs.CursorField)
		if err := plan.CursorProblem(cs.Name, read, len(cs.PrimaryKey) > 0, cursor); err != nil {
			return runner.Stream{}, errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		// A transform's inputs aren't written; its outputs are.
		mode := destination.Append
		if transformed {
			in := plan.Input{Name: cs.Name, PrimaryKey: cs.PrimaryKey, Read: read, Replay: replay, Spec: ps}
			if err := plan.InputProblem(p, in); err != nil {
				return runner.Stream{}, errs.Wrap(errs.ConfigInvalid, err, "runner")
			}
			inputs = append(inputs, in)
		} else {
			mode = plan.StreamMode(p, caps, read, len(cs.PrimaryKey) > 0)
		}
		keyed := len(cs.PrimaryKey) > 0
		var holdProblem error
		if !transformed {
			holdProblem = plan.HoldProblem(caps, mode, keyed, cs.Name, ps)
		}
		if err := errors.Join(plan.SyncProblem(mode, cs.Name, read), plan.ReadProblem(p, cs.Name, read), holdProblem); err != nil {
			return runner.Stream{}, errs.Wrap(errs.ConfigInvalid, err, "runner")
		}
		s := runner.Stream{
			Name:                 cs.Name,
			Replay:               replay,
			Read:                 source.SyncMode(read),
			Mode:                 mode,
			PrimaryKey:           cs.PrimaryKey,
			CursorField:          cursor,
			Schema:               cs.Schema,
			Seq:                  cs.Seq,
			NanosecondTimestamps: ps.GetTimestamps() == apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND,
			Require:              plan.Level(ps.GetRequire()),
			Deletes:              deletes,
			AllowTypeChanges:     plan.TypeChanges(p, ps),
			SplitsByKey:          cs.SplitsByKey,
			Table:                p.GetDestination().GetTables()[cs.Name],
		}
		if ps != nil {
			s.Collapse = ps.Collapse
		}
		if !transformed && plan.Holds(caps, mode, keyed) {
			s.Holds = streamHolds(ps)
		}
		for name, c := range ps.GetColumns() {
			if t := columnTypes[c.GetType()]; t != nil {
				if s.Overrides == nil {
					s.Overrides = map[string]arrow.DataType{}
				}
				s.Overrides[name] = t
			}
		}
		return s, nil
	}
	if len(p.GetSource().GetStreams()) == 0 {
		for _, cs := range cat.Streams {
			s, err := stream(cs, nil)
			if err != nil {
				return job, err
			}
			add(s)
		}
	}
	for _, ps := range p.GetSource().GetStreams() {
		found := false
		for _, cs := range cat.Streams {
			if cs.Name == ps.GetName() {
				s, err := stream(cs, ps)
				if err != nil {
					return job, err
				}
				add(s)
				found = true
			}
		}
		if !found {
			return job, errs.New(errs.ConfigInvalid, "the source has no stream %q", ps.GetName())
		}
	}
	if len(streams) == 0 && len(pausedStreams) > 0 {
		return job, errs.New(errs.ConfigInvalid, "every stream of the pipeline is paused; resume one, or pause the pipeline instead")
	}
	var outputs []runner.Stream
	var sqlT *runner.SQLTransform
	switch {
	case p.GetTransform().GetSql() != nil:
		if sqlT, outputs, err = sqlOutputs(ctx, p, caps, deletes, planner, streams, inputs); err != nil {
			return job, err
		}
	case transformed:
		changelog := slices.ContainsFunc(inputs, func(in plan.Input) bool { return in.Read == apiv1.SyncMode_SYNC_MODE_CDC })
		for _, o := range plan.Outputs(p, caps, inputs) {
			if err := plan.OutputProblem(o, inputs); err != nil {
				return job, errs.Wrap(errs.ConfigInvalid, err, "runner")
			}
			d := o.Delivery()
			out := runner.Stream{Name: o.Name, Replay: d.Replay, Mode: o.Mode, PrimaryKey: o.PrimaryKey, Require: o.Require, Deletes: deletes, Transformed: true,
				AllowTypeChanges: plan.TypeChanges(p, nil)}
			if o.Mode == destination.Merge && changelog {
				out.Read = source.CDC
			}
			outputs = append(outputs, out)
		}
	}
	policy := runner.AllowAll
	switch p.GetSchemaPolicy() {
	case apiv1.SchemaPolicy_SCHEMA_POLICY_ALLOW_COLUMNS:
		policy = runner.AllowColumns
	case apiv1.SchemaPolicy_SCHEMA_POLICY_BLOCK:
		policy = runner.Block
	case apiv1.SchemaPolicy_SCHEMA_POLICY_IGNORE_NEW:
		policy = runner.IgnoreNew
	}
	return runner.Job{
		Pipeline:           spec.GetPipelineId(),
		Continuous:         plan.Continuous(p),
		Source:             src,
		Streams:            streams,
		Paused:             pausedStreams,
		Outputs:            outputs,
		SQL:                sqlT,
		Destination:        dst,
		Namespace:          p.GetDestination().GetNamespace(),
		Settings:           plan.Settings(p, dst.Capabilities()),
		KeepSourceNames:    p.GetDestination().GetNaming() == apiv1.Naming_NAMING_SOURCE,
		SchemaPolicy:       policy,
		QuarantineInfinity: p.GetTypes().GetInfinity() == apiv1.Infinity_INFINITY_QUARANTINE,
		Releases:           releases(spec.GetReleases()),
		Resets:             resets(spec.GetResets()),
		Rewind:             rewindPolicy(p.GetHeal().GetMode(), spec.GetRewind()),
		KeyGroups:          int(spec.GetKeyGroups()),
	}, nil
}

var columnTypes = map[apiv1.ColumnType]arrow.DataType{
	apiv1.ColumnType_COLUMN_TYPE_STRING:  arrow.BinaryTypes.String,
	apiv1.ColumnType_COLUMN_TYPE_FLOAT64: arrow.PrimitiveTypes.Float64,
}

// streamHolds is how a stream holds back columns and cells.
func streamHolds(ps *apiv1.PipelineStream) *runner.StreamHolds {
	h := &runner.StreamHolds{
		Cells: ps.GetQuarantine() == apiv1.QuarantineScope_QUARANTINE_SCOPE_CELL,
		Held:  held(ps.GetHeld()),
	}
	for name, c := range ps.GetColumns() {
		if c.GetHeld() == nil {
			continue
		}
		if h.Columns == nil {
			h.Columns = map[string]runner.Held{}
		}
		h.Columns[name] = held(c.GetHeld())
	}
	for _, g := range ps.GetGroups() {
		h.Groups = append(h.Groups, g.GetColumns())
	}
	return h
}

func held(h *apiv1.Held) runner.Held {
	switch h.GetMode() {
	case apiv1.HeldMode_HELD_MODE_VALUE:
		return runner.Held{Mode: runner.HeldValue, Value: h.GetValue()}
	case apiv1.HeldMode_HELD_MODE_LAST:
		return runner.Held{Mode: runner.HeldLast, Value: h.GetValue()}
	}
	return runner.Held{}
}

// releases are the holds the user released or discarded.
func releases(rs []*dataplanev1.HoldRelease) []runner.Release {
	out := make([]runner.Release, len(rs))
	for i, r := range rs {
		since := checkpoint.RecordRef{Generation: r.GetSince().GetGeneration(), Epoch: r.GetSince().GetEpoch()}
		out[i] = runner.Release{ID: r.GetId(), Stream: r.GetStream(), Column: r.GetColumn(), Since: since, Discard: r.GetDiscard(), NewVersion: r.GetNewVersion()}
	}
	return out
}

// resets are the requests to read streams again from the start.
func resets(rs []*dataplanev1.Reset) []runner.Reset {
	out := make([]runner.Reset, len(rs))
	for i, r := range rs {
		out[i] = runner.Reset{ID: r.GetId(), Streams: r.GetStreams()}
	}
	return out
}

// rewindPolicy is what an attempt does when HEAD's record can't be read: what
// a person allowed, or under heal mode auto and auto_safe, a restart that
// keeps every stream's guarantee.
func rewindPolicy(mode apiv1.HealMode, approval string) runner.Rewind {
	switch {
	case approval != "":
		return runner.Rewind{Policy: runner.RewindAny, Approval: approval}
	case mode == apiv1.HealMode_HEAL_MODE_OFF || mode == apiv1.HealMode_HEAL_MODE_SUGGEST:
		return runner.Rewind{}
	}
	return runner.Rewind{Policy: runner.RewindSafe}
}
