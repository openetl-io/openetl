// Package heal opens and resolves incidents and runs their playbooks (L1),
// in the transaction of the report or outcome that shows them.
package heal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/store/db"
)

type Class string

const (
	SyncFailed      Class = "sync_failed"
	Auth            Class = "auth"
	SchemaBlocked   Class = "schema_blocked"
	RateLimited     Class = "rate_limited"
	PositionLost    Class = "position_lost"
	CrashLoop       Class = "crash_loop"
	Lagging         Class = "lagging"
	RetentionAtRisk Class = "retention_at_risk"
	Quarantine      Class = "quarantine"
	Held            Class = "held"
	// The record a read resumes from can't be read; or, with an action taken,
	// an attempt restarted from the one before it.
	CheckpointCorrupt Class = "checkpoint_corrupt"
)

// ResolvedByProgress are the classes a pipeline making progress again
// resolves: a sync that succeeds, or a continuous run's commit.
var ResolvedByProgress = []string{string(SyncFailed), string(Auth), string(SchemaBlocked), string(RateLimited), string(PositionLost), string(CrashLoop), string(CheckpointCorrupt)}

// ClassOf is the class of a sync or run that failed with code.
func ClassOf(code string) Class {
	switch code {
	case errs.AuthInvalid.String(), errs.AuthExpired.String():
		return Auth
	case errs.SchemaDrift.String():
		return SchemaBlocked
	case errs.RateLimited.String():
		return RateLimited
	case errs.CheckpointCorrupt.String():
		return CheckpointCorrupt
	case errs.PositionLost.String():
		return PositionLost
	}
	return SyncFailed
}

type Severity string

const (
	Warning  Severity = "warning"
	Critical Severity = "critical"
)

// Observation is one sighting of an incident.
type Observation struct {
	Pipeline uuid.UUID
	// The pipeline version it was seen with; empty: the current one.
	Version string
	// Empty: the whole pipeline.
	Stream   string
	Class    Class
	Code     string
	Severity Severity
	// The error, or what was measured.
	Message string
	Details map[string]string
	// Held's holds, each with its ways out.
	Holds []*apiv1.Hold
	// What a runner did about it already, recorded on the incident in place
	// of the playbook's own.
	Taken []Action
	Sync  *uuid.UUID
	// What makes this occurrence, such as the sync's ID: seen again with the
	// same key, it isn't another.
	Key string
	// Quarantine's: how the source reads the stream, and how many of the
	// pipeline's other streams it still reads.
	Read         apiv1.SyncMode
	OtherStreams int
	// How long the sync ran, for rate_limited's.
	Ran time.Duration
}

// Seen is what observing did.
type Seen struct {
	Incident db.Incident
	Opened   bool
	// A playbook paused the pipeline; the caller pauses what runs it once
	// the transaction commits.
	Paused bool
	// A playbook paused a stream; the caller restarts a continuous
	// pipeline's run once the transaction commits.
	StreamPaused bool
	// A playbook changed when syncs start, to this; the caller updates the
	// schedule once the transaction commits.
	Schedule   *apiv1.Schedule
	Continuous bool
}

// target is what playbooks need of the pipeline.
type target struct {
	workspace, pipeline             string
	workspaceID                     uuid.UUID
	continuous, transform           bool
	schedule                        *apiv1.Schedule
	mode                            apiv1.HealMode
	sourceSecrets                   []string
	destinationSecrets              []string
	sourceDigest, destinationDigest string
}

func (t target) name() string { return "workspaces/" + t.workspace + "/pipelines/" + t.pipeline }

func loadTarget(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID, version string) (target, error) {
	if version == "" {
		p, err := q.GetPipelineByID(ctx, db.GetPipelineByIDParams{OrgID: org, ID: pipeline})
		if err != nil {
			return target{}, err
		}
		version = p.Version
	}
	v, err := q.GetSyncSpec(ctx, db.GetSyncSpecParams{OrgID: org, PipelineID: pipeline, Version: version})
	if err != nil {
		return target{}, err
	}
	var spec apiv1.PipelineSpec
	if err := protojson.Unmarshal(v.PipelineSpec, &spec); err != nil {
		return target{}, err
	}
	return target{
		workspace: v.Workspace, pipeline: v.Pipeline, workspaceID: v.WorkspaceID,
		continuous:    spec.GetMode() == apiv1.PipelineMode_PIPELINE_MODE_CONTINUOUS,
		transform:     spec.GetTransform() != nil,
		schedule:      spec.GetSchedule(),
		mode:          spec.GetHeal().GetMode(),
		sourceSecrets: v.SourceSecrets, destinationSecrets: v.DestinationSecrets,
		sourceDigest: v.SourceConnector, destinationDigest: v.DestinationConnector,
	}, nil
}

// An incident keeps this much of its error.
const maxMessage = 4096

// Observe opens o's incident, or records it again on the one open, and runs
// its playbook. It records incident.opened for a new one, and
// incident.escalated when it turns critical.
func Observe(ctx context.Context, q *db.Queries, org uuid.UUID, o Observation) (Seen, error) {
	var seen Seen
	t, err := loadTarget(ctx, q, org, o.Pipeline, o.Version)
	if err != nil {
		return seen, err
	}
	seen.Continuous = t.continuous
	if o.Severity == "" {
		o.Severity = Critical
	}
	summary, next := describe(o, t)
	details, err := json.Marshal(o.Details)
	if err != nil {
		return seen, err
	}
	if o.Details == nil {
		details = []byte("{}")
	}
	fp := Fingerprint(o.Class, o.Code, o.Message, t.sourceDigest, t.destinationDigest)
	if len(o.Message) > maxMessage {
		o.Message = strings.ToValidUTF8(o.Message[:maxMessage], "") + "…"
	}
	id, err := uuid.NewV7()
	if err != nil {
		return seen, err
	}
	arg := db.InsertIncidentParams{
		OrgID: org, ID: id, WorkspaceID: t.workspaceID, PipelineID: o.Pipeline, Stream: o.Stream, Class: string(o.Class),
		Fingerprint: fp, Severity: string(o.Severity), Summary: summary, Message: o.Message, NextSteps: next,
		Details: details, Actions: []byte("[]"), SyncID: o.Sync, OccurrenceKey: o.Key,
	}
	if o.Code != "" {
		arg.Code = &o.Code
	}
	inc, err := q.InsertIncident(ctx, arg)
	switch {
	case err == nil:
		seen.Opened = true
	case errors.Is(err, pgx.ErrNoRows):
		var r db.RepeatIncidentRow
		r, err = q.RepeatIncident(ctx, db.RepeatIncidentParams{
			OrgID: org, PipelineID: o.Pipeline, Stream: o.Stream, Fingerprint: fp, OccurrenceKey: o.Key,
			Severity: string(o.Severity), Summary: summary, Message: o.Message, Details: details, SyncID: o.Sync,
		})
		if err != nil {
			return seen, err
		}
		inc = db.Incident{
			OrgID: r.OrgID, ID: r.ID, WorkspaceID: r.WorkspaceID, PipelineID: r.PipelineID, Stream: r.Stream, Class: r.Class,
			Code: r.Code, Fingerprint: r.Fingerprint, Status: r.Status, Severity: r.Severity, Summary: r.Summary,
			Message: r.Message, NextSteps: r.NextSteps, Details: r.Details, Actions: r.Actions, SyncID: r.SyncID,
			OccurrenceKey: r.OccurrenceKey, Occurrences: r.Occurrences, FirstSeenTime: r.FirstSeenTime,
			LastSeenTime: r.LastSeenTime, ResolveTime: r.ResolveTime, Resolution: r.Resolution,
		}
		seen.Incident = inc
		if r.WasSeverity != string(Critical) && o.Severity == Critical {
			if err := record(ctx, q, org, t, events.IncidentEscalated, inc); err != nil {
				return seen, err
			}
		}
		if r.WasKey == o.Key {
			return seen, nil
		}
	default:
		return seen, err
	}
	if seen.Opened {
		if err := record(ctx, q, org, t, events.IncidentOpened, inc); err != nil {
			return seen, err
		}
	}
	switch {
	case len(o.Taken) > 0:
		err = addActions(ctx, q, org, &inc, o.Taken...)
	case pauses(o.Class):
		seen.Paused, err = pause(ctx, q, org, t, &inc, "Paused the pipeline, so it doesn't fail the same way until it's fixed.")
	case o.Class == Quarantine:
		switch s, _ := quarantineStops(o, t); s {
		case stopStream:
			seen.StreamPaused, err = pauseStream(ctx, q, org, t, &inc)
		case stopPipeline:
			seen.Paused, err = pause(ctx, q, org, t, &inc, "Paused the pipeline, so the stream, the only one it reads, quarantines no more rows.")
		}
	case o.Class == RateLimited:
		seen.Schedule, err = slowDown(ctx, q, org, t, o, &inc)
	}
	if err != nil {
		return seen, err
	}
	seen.Incident = inc
	return seen, nil
}

// Action is a playbook's action, as incidents.actions keeps it.
type Action struct {
	Kind        string    `json:"kind"`
	State       string    `json:"state"`
	Description string    `json:"description"`
	Time        time.Time `json:"time"`
}

// pause pauses the pipeline if its heal mode lets it, or proposes it, and
// records the action on inc. done says what pausing did, in the past tense.
func pause(ctx context.Context, q *db.Queries, org uuid.UUID, t target, inc *db.Incident, done string) (bool, error) {
	if t.mode == apiv1.HealMode_HEAL_MODE_OFF {
		return false, nil
	}
	a := Action{Kind: "pause", State: "proposed", Time: time.Now().UTC()}
	applied := false
	switch t.mode {
	case apiv1.HealMode_HEAL_MODE_SUGGEST:
		a.Description = "Pause" + strings.TrimSuffix(strings.TrimPrefix(done, "Paused"), ".") + "; heal mode suggest doesn't."
	default:
		n, err := q.SetPipelinePaused(ctx, db.SetPipelinePausedParams{OrgID: org, ID: inc.PipelineID, Paused: true})
		if err != nil {
			return false, err
		}
		if n == 0 {
			return false, nil
		}
		applied = true
		a.State, a.Description = "applied", done
		name := t.name()
		_, err = events.Record(ctx, q, org, events.Event{
			Workspace: t.workspaceID, Type: events.PipelinePaused, Pipeline: t.pipeline,
			Data: events.Pipeline{Pipeline: name, Incident: name + "/incidents/" + inc.ID.String()},
		})
		if err != nil {
			return false, err
		}
	}
	return applied, addActions(ctx, q, org, inc, a)
}

// Resolve resolves the pipeline's open incidents of the classes, of stream
// only if it's set, last seen before this transaction, and records
// incident.resolved for each.
func Resolve(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID, classes []string, stream *string, resolution string) ([]db.Incident, error) {
	rows, err := q.ResolveIncidents(ctx, db.ResolveIncidentsParams{OrgID: org, PipelineID: pipeline, Classes: classes, Stream: stream, Resolution: &resolution})
	if err != nil || len(rows) == 0 {
		return rows, err
	}
	t, err := names(ctx, q, org, pipeline)
	if err != nil {
		return rows, err
	}
	for _, r := range rows {
		if err := record(ctx, q, org, t, events.IncidentResolved, r); err != nil {
			return rows, err
		}
	}
	return rows, nil
}

// ResolveOne resolves an open incident by hand. A pipeline that's still
// lagging opens a new incident at its next checkpoint, so resolving doesn't
// silence it.
func ResolveOne(ctx context.Context, q *db.Queries, org, id uuid.UUID, resolution string) (db.Incident, error) {
	r, err := q.ResolveIncident(ctx, db.ResolveIncidentParams{OrgID: org, ID: id, Resolution: &resolution})
	if err != nil {
		return r, err
	}
	if r.Class == string(Lagging) {
		if _, err := q.StopLagging(ctx, db.StopLaggingParams{OrgID: org, ID: r.PipelineID}); err != nil {
			return r, err
		}
	}
	t, err := names(ctx, q, org, r.PipelineID)
	if err != nil {
		return r, err
	}
	return r, record(ctx, q, org, t, events.IncidentResolved, r)
}

func names(ctx context.Context, q *db.Queries, org, pipeline uuid.UUID) (target, error) {
	n, err := q.GetPipelineNames(ctx, db.GetPipelineNamesParams{OrgID: org, ID: pipeline})
	if err != nil {
		return target{}, err
	}
	return target{workspace: n.Workspace, pipeline: n.Name, workspaceID: n.WorkspaceID}, nil
}

func record(ctx context.Context, q *db.Queries, org uuid.UUID, t target, typ string, r db.Incident) error {
	data, err := Event(t.name(), r)
	if err != nil {
		return err
	}
	_, err = events.Record(ctx, q, org, events.Event{
		Workspace: t.workspaceID, Type: typ, Pipeline: t.pipeline, Data: data,
		Key: typ + "/" + r.ID.String() + "/" + r.OccurrenceKey,
	})
	return err
}

// Event is an incident's event data; pipeline is its pipeline's name.
func Event(pipeline string, r db.Incident) (events.Incident, error) {
	e := events.Incident{
		Incident: pipeline + "/incidents/" + r.ID.String(), Pipeline: pipeline, Stream: r.Stream, Class: r.Class,
		Severity: r.Severity, Summary: r.Summary, Message: r.Message, NextSteps: r.NextSteps, Fingerprint: r.Fingerprint,
		Occurrences: r.Occurrences, FirstSeen: r.FirstSeenTime, LastSeen: r.LastSeenTime, Resolved: r.ResolveTime,
	}
	if r.Code != nil {
		e.Code = *r.Code
	}
	if r.SyncID != nil {
		e.Sync = pipeline + "/syncs/" + r.SyncID.String()
	}
	if r.Resolution != nil {
		e.Resolution = *r.Resolution
	}
	if err := json.Unmarshal(r.Details, &e.Details); err != nil {
		return e, err
	}
	return e, nil
}

// Fingerprint identifies a fault across pipelines: its class, and its
// connector versions. An error's class adds its code, and a sync that failed
// for no class's reason its error without the specifics, since that's what
// tells one fault from another. A condition's, such as a crash loop's, is
// the same whatever errors come with it.
func Fingerprint(class Class, code, message, source, destination string) string {
	h := sha256.New()
	parts := []string{string(class), source, destination}
	switch class {
	case SyncFailed:
		parts = append(parts, code, Normalize(message))
	case Auth, SchemaBlocked, RateLimited, PositionLost, CheckpointCorrupt:
		parts = append(parts, code)
	}
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

var (
	uuids   = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	quoted  = regexp.MustCompile(`"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*'`)
	hexes   = regexp.MustCompile(`\b(?:0x)?[0-9a-fA-F]{8,}\b`)
	numbers = regexp.MustCompile(`\d+`)
	spaces  = regexp.MustCompile(`\s+`)
)

// Normalize takes an error's specifics out: IDs, quoted values, hex and
// numbers.
func Normalize(message string) string {
	s := uuids.ReplaceAllString(message, "<id>")
	s = quoted.ReplaceAllString(s, "<value>")
	s = hexes.ReplaceAllStringFunc(s, func(h string) string {
		if !strings.ContainsAny(h, "0123456789") {
			return h
		}
		return "<hex>"
	})
	s = numbers.ReplaceAllString(s, "<n>")
	return strings.ToLower(spaces.ReplaceAllString(strings.TrimSpace(s), " "))
}

var streamIn = regexp.MustCompile(`\bstream ([^\s:]+): `)

// StreamOf is the stream a schema_blocked incident's error names, which the
// engine puts first.
func StreamOf(class Class, message string) string {
	if class != SchemaBlocked {
		return ""
	}
	if m := streamIn.FindStringSubmatch(message); m != nil {
		return m[1]
	}
	return ""
}
