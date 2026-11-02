// Package plan works out how a pipeline version will sync before a runner
// runs it: its delivery settings, each stream's level, and the checks a sync
// would fail. The API plans from the source's latest catalog, and the runner
// checks again against the catalog it discovers.
package plan

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Defaults for scheduled pipelines.
const (
	defaultCommitInterval     = 5 * time.Minute
	defaultCommitMaxBytes     = 256e6
	defaultResumePointTimeout = time.Minute
)

// Settings settles spec.delivery against the destination's capabilities.
func Settings(d *apiv1.Delivery, caps destination.Capabilities) delivery.Settings {
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
	if r := Level(d.GetRequire()); r != 0 {
		s.Require = r
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

// WriteMode is the spec's destination.write; 0 if it's unset.
func WriteMode(p *apiv1.PipelineSpec) destination.WriteMode {
	switch p.GetDestination().GetWrite() {
	case apiv1.WriteMode_WRITE_MODE_APPEND:
		return destination.Append
	case apiv1.WriteMode_WRITE_MODE_OVERWRITE:
		return destination.Overwrite
	case apiv1.WriteMode_WRITE_MODE_MERGE:
		return destination.Merge
	}
	return 0
}

// ReadMode is how a stream is read: its sync in the spec, or else the way
// its source declares it reads it first. Unspecified if neither says.
func ReadMode(declared []apiv1.SyncMode, ps *apiv1.PipelineStream) apiv1.SyncMode {
	if m := ps.GetSync(); m != apiv1.SyncMode_SYNC_MODE_UNSPECIFIED {
		return m
	}
	if len(declared) > 0 {
		return declared[0]
	}
	return apiv1.SyncMode_SYNC_MODE_UNSPECIFIED
}

// StreamMode is a stream's write mode: destination.write, or without one,
// overwrite for a full refresh into a destination that has it, so its table
// holds what the source holds, and append otherwise.
func StreamMode(p *apiv1.PipelineSpec, caps destination.Capabilities, read apiv1.SyncMode) destination.WriteMode {
	if m := WriteMode(p); m != 0 {
		return m
	}
	if read == apiv1.SyncMode_SYNC_MODE_FULL_REFRESH && caps.SupportsMode(destination.Overwrite) {
		return destination.Overwrite
	}
	return destination.Append
}

func Deletes(p *apiv1.PipelineSpec) destination.DeleteMode {
	if p.GetDestination().GetDeletes() == apiv1.DeleteMode_DELETE_MODE_HARD {
		return destination.HardDelete
	}
	return destination.SoftDelete
}

// SyncProblem fails a stream whose read mode its write mode can't take:
// overwrite replaces the table with what one read wrote, so it needs a full
// refresh.
func SyncProblem(mode destination.WriteMode, stream string, read apiv1.SyncMode) error {
	if mode != destination.Overwrite {
		return nil
	}
	switch read {
	case apiv1.SyncMode_SYNC_MODE_INCREMENTAL, apiv1.SyncMode_SYNC_MODE_CDC, apiv1.SyncMode_SYNC_MODE_STREAM:
		return fmt.Errorf("destination.write: overwrite replaces each table with what one read wrote, so it needs full_refresh, and stream %q is read with %s",
			stream, strings.ToLower(strings.TrimPrefix(read.String(), "SYNC_MODE_")))
	}
	return nil
}

// Level is a spec's delivery level; 0 if it's unset.
func Level(l apiv1.DeliveryLevel) delivery.Level {
	for k, v := range levels {
		if v == l {
			return k
		}
	}
	return 0
}

// deliveryStream is what sets a stream's level: the catalog's, and the
// spec's settings for it, if it names the stream.
func deliveryStream(cs *apiv1.CatalogStream, ps *apiv1.PipelineStream, mode destination.WriteMode) delivery.Stream {
	d := delivery.Stream{Name: cs.GetName(), Replay: replay(cs.GetReplay()), Mode: mode, Keyed: len(cs.GetPrimaryKey()) > 0, Require: Level(ps.GetRequire())}
	if ps != nil {
		d.Collapse = ps.Collapse
	}
	return d
}

var levels = map[delivery.Level]apiv1.DeliveryLevel{
	delivery.AtLeastOnce:     apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE,
	delivery.EffectivelyOnce: apiv1.DeliveryLevel_DELIVERY_LEVEL_EFFECTIVELY_ONCE,
	delivery.ExactlyOnce:     apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE,
}

// Pipeline plans each stream the spec reads, and returns every check a sync
// would fail and what the plan warns about. Without a catalog, it checks
// only the settings.
func Pipeline(spec *apiv1.PipelineSpec, cat *apiv1.Catalog, caps destination.Capabilities) ([]*apiv1.StreamPlan, []error, []*apiv1.PlanWarning) {
	settings := Settings(spec.GetDelivery(), caps)
	var warnings []*apiv1.PlanWarning
	if spec.GetDestination().GetDeletes() != apiv1.DeleteMode_DELETE_MODE_UNSPECIFIED && WriteMode(spec) != destination.Merge {
		warnings = append(warnings, &apiv1.PlanWarning{Message: "destination.deletes applies only to merge, so it's ignored"})
	}
	if cat == nil {
		problems := delivery.Problems(caps, settings, nil)
		if m := WriteMode(spec); m != 0 {
			if err := delivery.ModeProblem(caps, m); err != nil {
				problems = append(problems, err)
			}
		}
		for _, ps := range spec.GetSource().GetStreams() {
			read := ReadMode(nil, ps)
			if err := SyncProblem(StreamMode(spec, caps, read), ps.GetName(), read); err != nil {
				problems = append(problems, err)
			}
		}
		return nil, problems, warnings
	}
	var problems []error
	type selected struct {
		cs *apiv1.CatalogStream
		ps *apiv1.PipelineStream
	}
	var streams []selected
	if len(spec.GetSource().GetStreams()) == 0 {
		for _, cs := range cat.GetStreams() {
			streams = append(streams, selected{cs: cs})
		}
	}
	for _, ps := range spec.GetSource().GetStreams() {
		i := slices.IndexFunc(cat.GetStreams(), func(cs *apiv1.CatalogStream) bool { return cs.GetName() == ps.GetName() })
		if i < 0 {
			problems = append(problems, fmt.Errorf("the source has no stream %q", ps.GetName()))
			continue
		}
		streams = append(streams, selected{cs: cat.GetStreams()[i], ps: ps})
	}

	var plans []*apiv1.StreamPlan
	var ds []delivery.Stream
	for _, s := range streams {
		name := s.cs.GetName()
		for _, column := range slices.Sorted(maps.Keys(s.ps.GetColumns())) {
			if s.ps.GetColumns()[column].GetType() == apiv1.ColumnType_COLUMN_TYPE_UNSPECIFIED {
				continue
			}
			i := slices.IndexFunc(s.cs.GetColumns(), func(c *apiv1.CatalogColumn) bool { return c.GetName() == column })
			if i < 0 || !strings.HasPrefix(s.cs.GetColumns()[i].GetType(), "decimal(") {
				problems = append(problems, fmt.Errorf("stream %q: column %q has a type override, but the source doesn't declare it a decimal", name, column))
			}
		}
		if s.ps.GetTimestamps() == apiv1.TimestampUnit_TIMESTAMP_UNIT_NANOSECOND && !caps.NanosecondTimestamps {
			problems = append(problems, fmt.Errorf("stream %q keeps nanosecond timestamps, but the destination keeps only microseconds", name))
		}
		read := ReadMode(s.cs.GetSyncModes(), s.ps)
		mode := StreamMode(spec, caps, read)
		if err := SyncProblem(mode, name, read); err != nil {
			problems = append(problems, err)
		}
		d := deliveryStream(s.cs, s.ps, mode)
		ds = append(ds, d)
		p := delivery.PlanStream(caps, d, settings.Require)
		plans = append(plans, &apiv1.StreamPlan{Name: name, Level: levels[p.Level], Dedupe: p.Dedupe != destination.DedupeNone, Collapse: p.Collapse, Reason: p.Why,
			Write: apiv1.WriteMode(mode)}) //nolint:gosec // small
	}
	problems = append(problems, delivery.Problems(caps, settings, ds)...)
	for _, w := range delivery.Warnings(caps, settings, ds) {
		warnings = append(warnings, &apiv1.PlanWarning{Stream: w.Stream, Message: w.Message})
	}
	return plans, problems, warnings
}

func replay(r apiv1.Replay) source.Replay {
	if r == apiv1.Replay_REPLAY_EXACT {
		return source.Exact
	}
	return source.Superset
}
