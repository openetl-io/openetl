package activities

import (
	"cmp"
	"context"
	"errors"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/plan"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// buildJob binds and starts the connectors, and settles the settings. The
// caller sets the attempt's owner, log and dataflow, and closes the
// connectors, even after an error.
func buildJob(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse, env connectorhost.Env) (runner.Job, error) {
	p := spec.GetPipeline()
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
	cat, err := src.Discover(ctx)
	if err != nil {
		return job, err
	}

	mode := plan.WriteMode(p)
	var streams []runner.Stream
	stream := func(cs source.CatalogStream, ps *apiv1.PipelineStream) runner.Stream {
		s := runner.Stream{
			Name:                 cs.Name,
			Replay:               cs.Replay,
			Mode:                 mode,
			PrimaryKey:           cs.PrimaryKey,
			CursorField:          cmp.Or(ps.GetCursor(), cs.CursorField),
			Schema:               cs.Schema,
			NanosecondTimestamps: ps.GetTimestamps() == apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND,
			Require:              plan.Level(ps.GetRequire()),
		}
		if ps != nil {
			s.Collapse = ps.Collapse
		}
		for name, c := range ps.GetColumns() {
			if t := columnTypes[c.GetType()]; t != nil {
				if s.Overrides == nil {
					s.Overrides = map[string]arrow.DataType{}
				}
				s.Overrides[name] = t
			}
		}
		return s
	}
	if len(p.GetSource().GetStreams()) == 0 {
		for _, cs := range cat.Streams {
			streams = append(streams, stream(cs, nil))
		}
	}
	for _, ps := range p.GetSource().GetStreams() {
		found := false
		for _, cs := range cat.Streams {
			if cs.Name == ps.GetName() {
				streams = append(streams, stream(cs, ps))
				found = true
			}
		}
		if !found {
			return job, errs.New(errs.ConfigInvalid, "the source has no stream %q", ps.GetName())
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
		Source:             src,
		Streams:            streams,
		Destination:        dst,
		Namespace:          p.GetDestination().GetNamespace(),
		Settings:           plan.Settings(p.GetDelivery(), dst.Capabilities()),
		KeepSourceNames:    p.GetDestination().GetNaming() == apiv1.Naming_NAMING_SOURCE,
		SchemaPolicy:       policy,
		QuarantineInfinity: p.GetTypes().GetInfinity() == apiv1.Infinity_INFINITY_QUARANTINE,
	}, nil
}

var columnTypes = map[apiv1.ColumnType]arrow.DataType{
	apiv1.ColumnType_COLUMN_TYPE_STRING:  arrow.BinaryTypes.String,
	apiv1.ColumnType_COLUMN_TYPE_FLOAT64: arrow.PrimitiveTypes.Float64,
}
