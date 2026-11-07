// Package events records platform events in the outbox, in the transaction
// that changes what they report, for the notifier to deliver to webhooks.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/openetl-io/openetl/internal/store/db"
)

const (
	SyncStarted       = "sync.started"
	SyncSucceeded     = "sync.succeeded"
	SyncFailed        = "sync.failed"
	SyncStopped       = "sync.stopped"
	PipelineCreated   = "pipeline.created"
	PipelineUpdated   = "pipeline.updated"
	PipelineDeleted   = "pipeline.deleted"
	PipelinePaused    = "pipeline.paused"
	PipelineResumed   = "pipeline.resumed"
	PipelineLagging   = "pipeline.lagging"
	PipelineRecovered = "pipeline.recovered"
	PipelineRescaled  = "pipeline.rescaled"
	SchemaChanged     = "schema.changed"
	SchemaBlocked     = "schema.blocked"
	IncidentOpened    = "incident.opened"
	IncidentEscalated = "incident.escalated"
	IncidentResolved  = "incident.resolved"
	WebhookTest       = "webhook.test"
)

// Types is every event type there is.
var Types = []string{
	SyncStarted, SyncSucceeded, SyncFailed, SyncStopped,
	PipelineCreated, PipelineUpdated, PipelineDeleted, PipelinePaused, PipelineResumed, PipelineLagging, PipelineRecovered,
	PipelineRescaled,
	SchemaChanged, SchemaBlocked,
	IncidentOpened, IncidentEscalated, IncidentResolved,
	WebhookTest,
}

// Event is one event to record. Data is its payload's data, which becomes
// JSON.
type Event struct {
	Workspace uuid.UUID
	Type      string
	// The pipeline's name, for webhooks' pipeline filters; empty for the
	// workspace's own events.
	Pipeline string
	Data     any
	// Set: the event is recorded once, however often it's recorded with the
	// same key.
	Key string
	// Set: only this webhook gets it.
	Webhook *uuid.UUID
}

// Record writes e to the outbox in q's transaction, and returns its ID.
func Record(ctx context.Context, q *db.Queries, org uuid.UUID, e Event) (uuid.UUID, error) {
	if !slices.Contains(Types, e.Type) {
		return uuid.Nil, fmt.Errorf("events: unknown type %q", e.Type)
	}
	data, err := json.Marshal(e.Data)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.Nil, err
	}
	arg := db.InsertEventParams{OrgID: org, ID: id, WorkspaceID: e.Workspace, Type: e.Type, Data: data, WebhookID: e.Webhook}
	if e.Pipeline != "" {
		arg.Pipeline = &e.Pipeline
	}
	if e.Key != "" {
		arg.Key = &e.Key
	}
	return id, q.InsertEvent(ctx, arg)
}

// CheckPattern checks that p is an event type, or a glob (path.Match's
// syntax) that matches one.
func CheckPattern(p string) error {
	if _, err := path.Match(p, ""); err != nil {
		return fmt.Errorf("%q isn't a valid pattern", p)
	}
	for _, t := range Types {
		if ok, _ := path.Match(p, t); ok {
			return nil
		}
	}
	return fmt.Errorf("%q matches no event type; they are %s", p, strings.Join(Types, ", "))
}

// Matches reports whether typ matches any of the patterns.
func Matches(patterns []string, typ string) bool {
	for _, p := range patterns {
		if ok, _ := path.Match(p, typ); ok {
			return true
		}
	}
	return false
}

// Payload is a delivery's body, as Standard Webhooks lays it out.
type Payload struct {
	Type      string          `json:"type"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data"`
}

// The data of each kind of event. Names are API resource names; times are
// RFC 3339, durations seconds.

// Sync is a sync's or a continuous pipeline's run's.
type Sync struct {
	Pipeline   string     `json:"pipeline"`
	Sync       string     `json:"sync"`
	Version    string     `json:"version"`
	Continuous bool       `json:"continuous"`
	StartTime  time.Time  `json:"startTime"`
	EndTime    *time.Time `json:"endTime,omitempty"`
	Error      string     `json:"error,omitempty"`
	// The error's code, such as SCHEMA_DRIFT.
	Code string `json:"code,omitempty"`
}

type Pipeline struct {
	Pipeline        string `json:"pipeline"`
	Version         string `json:"version,omitempty"`
	PreviousVersion string `json:"previousVersion,omitempty"`
	// Where the version came from, such as cli:shop.yaml#3.
	Provenance string `json:"provenance,omitempty"`
	// The incident a playbook paused it for.
	Incident string `json:"incident,omitempty"`
}

// Lag is a continuous pipeline's read falling behind, or catching up.
type Lag struct {
	Pipeline         string    `json:"pipeline"`
	Sync             string    `json:"sync"`
	LagSeconds       float64   `json:"lagSeconds"`
	ThresholdSeconds float64   `json:"thresholdSeconds"`
	Since            time.Time `json:"since"`
	// The stream furthest behind; empty for the whole read.
	Stream string `json:"stream,omitempty"`
}

// Rescale is a pipeline's jobs rescaled, through the API or by the job loop.
// A running job stops at a checkpoint and starts again at the new size.
type Rescale struct {
	Pipeline string `json:"pipeline"`
	Version  string `json:"version"`
	// api or autoscaler.
	By string `json:"by"`
	// The API's size, such as l or "3 vCPU, 6 GiB"; empty when it gives the
	// spec back its say.
	Size string `json:"size,omitempty"`
	// The job loop's lanes, and those of the job it rescales.
	Lanes     int `json:"lanes,omitempty"`
	FromLanes int `json:"fromLanes,omitempty"`
}

type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Schema is a stream's new schema, and how it differs from the one before.
type Schema struct {
	Pipeline string `json:"pipeline"`
	Stream   string `json:"stream"`
	// The stream is one of a transform's outputs.
	Output  bool     `json:"output,omitempty"`
	Version int32    `json:"version"`
	Table   string   `json:"table"`
	Columns []Column `json:"columns"`
	Added   []string `json:"added,omitempty"`
	Removed []string `json:"removed,omitempty"`
	Changed []string `json:"changed,omitempty"`
}

// Incident is an incident opened, escalated or resolved.
type Incident struct {
	Incident string `json:"incident"`
	Pipeline string `json:"pipeline"`
	Stream   string `json:"stream,omitempty"`
	// Such as auth or crash_loop.
	Class string `json:"class"`
	// The error's taxonomy code, if it came from one.
	Code        string            `json:"code,omitempty"`
	Severity    string            `json:"severity"`
	Summary     string            `json:"summary"`
	Message     string            `json:"message"`
	NextSteps   string            `json:"nextSteps,omitempty"`
	Fingerprint string            `json:"fingerprint"`
	Occurrences int32             `json:"occurrences"`
	Details     map[string]string `json:"details,omitempty"`
	Sync        string            `json:"sync,omitempty"`
	FirstSeen   time.Time         `json:"firstSeenTime"`
	LastSeen    time.Time         `json:"lastSeenTime"`
	Resolved    *time.Time        `json:"resolveTime,omitempty"`
	Resolution  string            `json:"resolution,omitempty"`
}

type Test struct {
	Webhook string `json:"webhook"`
}
