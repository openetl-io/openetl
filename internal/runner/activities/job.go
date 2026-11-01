package activities

import (
	"cmp"
	"context"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Defaults for scheduled pipelines.
const (
	defaultCommitInterval     = 5 * time.Minute
	defaultCommitMaxBytes     = 256e6
	defaultResumePointTimeout = time.Minute
)

// buildJob binds the connectors and settles the settings. The caller sets
// the attempt's owner, log and dataflow.
func buildJob(ctx context.Context, spec *dataplanev1.GetSyncSpecResponse) (runner.Job, error) {
	p := spec.GetPipeline()
	srcCfg, err := protojson.Marshal(spec.GetSource().GetConfig())
	if err != nil {
		return runner.Job{}, err
	}
	src, err := connectorhost.NewSource(spec.GetSource().GetConnector(), srcCfg)
	if err != nil {
		return runner.Job{}, err
	}
	dstCfg, err := protojson.Marshal(spec.GetDestination().GetConfig())
	if err != nil {
		return runner.Job{}, err
	}
	dst, err := connectorhost.NewDestination(spec.GetDestination().GetConnector(), dstCfg)
	if err != nil {
		return runner.Job{}, err
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return runner.Job{}, err
	}

	mode := destination.Append
	switch p.GetDestination().GetWrite() {
	case apiv1.WriteMode_WRITE_MODE_OVERWRITE:
		mode = destination.Overwrite
	case apiv1.WriteMode_WRITE_MODE_MERGE:
		mode = destination.Merge
	}
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
			return runner.Job{}, errs.New(errs.ConfigInvalid, "the source has no stream %q", ps.GetName())
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
		Settings:           settingsFor(p.GetDelivery(), dst.Capabilities()),
		KeepSourceNames:    p.GetDestination().GetNaming() == apiv1.Naming_NAMING_SOURCE,
		SchemaPolicy:       policy,
		QuarantineInfinity: p.GetTypes().GetInfinity() == apiv1.Infinity_INFINITY_QUARANTINE,
	}, nil
}

var columnTypes = map[apiv1.ColumnType]arrow.DataType{
	apiv1.ColumnType_COLUMN_TYPE_STRING:  arrow.BinaryTypes.String,
	apiv1.ColumnType_COLUMN_TYPE_FLOAT64: arrow.PrimitiveTypes.Float64,
}

func settingsFor(d *apiv1.Delivery, caps destination.Capabilities) delivery.Settings {
	s := delivery.Settings{
		Require:            delivery.ExactlyOnce,
		CommitInterval:     defaultCommitInterval,
		CommitMaxBytes:     defaultCommitMaxBytes,
		Scope:              destination.ScopeStream,
		ResumePointTimeout: defaultResumePointTimeout,
	}
	if caps.SupportsScope(destination.ScopePipeline) {
		s.Scope = destination.ScopePipeline
	}
	switch d.GetRequire() {
	case apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE:
		s.Require = delivery.AtLeastOnce
	case apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE:
		s.Require = delivery.EffectivelyOnce
	}
	if c := d.GetCommit(); c != nil {
		if c.GetInterval() != nil {
			s.CommitInterval = c.GetInterval().AsDuration()
		}
		if c.GetMaxBytes() > 0 {
			s.CommitMaxBytes = c.GetMaxBytes()
		}
		switch c.GetScope() {
		case apiv1.CommitScope_COMMIT_SCOPE_PIPELINE:
			s.Scope = destination.ScopePipeline
		case apiv1.CommitScope_COMMIT_SCOPE_STREAM:
			s.Scope = destination.ScopeStream
		}
	}
	if t := d.GetResumePointTimeout(); t != nil {
		s.ResumePointTimeout = t.AsDuration()
	}
	return s
}
