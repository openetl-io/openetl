package heal

import (
	"context"
	"time"

	"github.com/google/uuid"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/store/db"
)

// WhyNotPause says why a stream can't be paused on its own, or "" if it
// can. read is how the source reads it; transform, the pipeline has one;
// others, how many of its other streams it still reads.
func WhyNotPause(read apiv1.SyncMode, transform bool, others int) string {
	switch {
	case transform:
		return "the pipeline's transform reads it together with its other streams"
	case read == apiv1.SyncMode_SYNC_MODE_CDC:
		return "it's read with cdc, which shares the source's change log with the pipeline's other cdc streams, so skipping it would lose its changes"
	case read == apiv1.SyncMode_SYNC_MODE_UNSPECIFIED:
		return "no sync has said yet how the source reads it"
	case others == 0:
		return "it's the only stream the pipeline still reads"
	}
	return ""
}

// stopping is what quarantine's playbook stops.
type stopping int

const (
	stopNothing stopping = iota
	stopStream
	// The stream is the only one the pipeline still reads.
	stopPipeline
)

// quarantineStops is what quarantine's playbook stops for o: the stream,
// or the pipeline if it's the only stream left, and nothing where the
// stream can't stop on its own. why says why not.
func quarantineStops(o Observation, t target) (s stopping, why string) {
	why = WhyNotPause(o.Read, t.transform, o.OtherStreams)
	switch {
	case why == "":
		return stopStream, ""
	case o.OtherStreams == 0 && WhyNotPause(o.Read, t.transform, 1) == "":
		return stopPipeline, ""
	}
	return stopNothing, why
}

// PauseStream pauses a stream on its own. It returns false if it was
// paused already.
func PauseStream(ctx context.Context, q *db.Queries, org uuid.UUID, pipeline uuid.UUID, stream, reason string, incident *uuid.UUID) (bool, error) {
	n, err := q.PauseStream(ctx, db.PauseStreamParams{OrgID: org, PipelineID: pipeline, Stream: stream, Reason: reason, IncidentID: incident})
	if err != nil || n == 0 {
		return false, err
	}
	t, err := names(ctx, q, org, pipeline)
	if err != nil {
		return false, err
	}
	data := events.Pipeline{Pipeline: t.name(), Stream: stream}
	if incident != nil {
		data.Incident = t.name() + "/incidents/" + incident.String()
	}
	_, err = events.Record(ctx, q, org, events.Event{Workspace: t.workspaceID, Type: events.StreamPaused, Pipeline: t.pipeline, Data: data})
	return err == nil, err
}

// ResumeStream resumes a stream paused on its own. It returns false if it
// wasn't paused.
func ResumeStream(ctx context.Context, q *db.Queries, org uuid.UUID, pipeline uuid.UUID, stream string) (bool, error) {
	n, err := q.ResumeStream(ctx, db.ResumeStreamParams{OrgID: org, PipelineID: pipeline, Stream: stream})
	if err != nil || n == 0 {
		return false, err
	}
	t, err := names(ctx, q, org, pipeline)
	if err != nil {
		return false, err
	}
	_, err = events.Record(ctx, q, org, events.Event{
		Workspace: t.workspaceID, Type: events.StreamResumed, Pipeline: t.pipeline, Data: events.Pipeline{Pipeline: t.name(), Stream: stream},
	})
	return err == nil, err
}

// pauseStream pauses inc's stream if the heal mode lets it, or proposes
// it, and records the action on inc.
func pauseStream(ctx context.Context, q *db.Queries, org uuid.UUID, t target, inc *db.Incident) (bool, error) {
	if t.mode == apiv1.HealMode_HEAL_MODE_OFF {
		return false, nil
	}
	a := Action{Kind: "pause_stream", State: "proposed", Time: time.Now().UTC(),
		Description: "Pause the stream, so it quarantines no more rows while the pipeline's other streams carry on; heal mode suggest doesn't."}
	applied := false
	if t.mode != apiv1.HealMode_HEAL_MODE_SUGGEST {
		var err error
		applied, err = PauseStream(ctx, q, org, inc.PipelineID, inc.Stream, "It quarantined too many of its rows.", &inc.ID)
		if err != nil || !applied {
			return false, err
		}
		a.State, a.Description = "applied", "Paused the stream, so it quarantines no more rows while the pipeline's other streams carry on."
	}
	return applied, addActions(ctx, q, org, inc, a)
}
