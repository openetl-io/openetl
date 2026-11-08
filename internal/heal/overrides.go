package heal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/declarative"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/store/db"
)

// The spec fields heal overrides.
const (
	OverrideEvery          = "schedule.every"
	OverrideRateLimitScale = "source.rate_limit_scale"
)

// Overrides are the fields heal can override, for RemovePipelineOverride.
var Overrides = []string{OverrideEvery, OverrideRateLimitScale}

// How far heal slows a pipeline down: its schedule's interval doubles up to
// maxEvery, and a declarative source's rate limit halves down to
// minRateLimitScale of what it declares.
const (
	maxEvery          = 24 * time.Hour
	minRateLimitScale = 0.125
	// Every this many syncs in a row that fail rate limited.
	slowDownEvery = 3
)

func everyValue(d time.Duration) ([]byte, error) { return protojson.Marshal(durationpb.New(d)) }

// ParseEvery reads an every override's value.
func ParseEvery(v []byte) (time.Duration, error) {
	var d durationpb.Duration
	if err := protojson.Unmarshal(v, &d); err != nil {
		return 0, fmt.Errorf("heal: an every override of %s: %w", v, err)
	}
	return d.AsDuration(), nil
}

// ParseScale reads a rate limit scale override's value.
func ParseScale(v []byte) (float64, error) {
	s, err := strconv.ParseFloat(string(v), 64)
	if err != nil || s <= 0 || s > 1 {
		return 0, fmt.Errorf("heal: a rate limit scale override of %s", v)
	}
	return s, nil
}

// Schedule is when syncs start: the spec's, with every overridden by every
// if it's set, as ListPipelinesToRun's every_override has it. A cron isn't
// overridden.
func Schedule(spec *apiv1.Schedule, every []byte) (*apiv1.Schedule, error) {
	if len(every) == 0 || spec.GetEvery() == nil {
		return spec, nil
	}
	d, err := ParseEvery(every)
	if err != nil {
		return nil, err
	}
	out := proto.Clone(spec).(*apiv1.Schedule)
	out.Trigger = &apiv1.Schedule_Every{Every: durationpb.New(d)}
	return out, nil
}

// Override is an override as the API has it; pipeline is its pipeline's
// name, and spec its current version's spec.
func Override(pipeline string, spec *apiv1.PipelineSpec, o db.PipelineOverride) *apiv1.PipelineOverride {
	out := &apiv1.PipelineOverride{
		Field: o.Field, Value: string(o.Value), Reason: o.Reason, UpdateTime: timestamppb.New(o.UpdateTime),
	}
	if o.IncidentID != nil {
		out.Incident = pipeline + "/incidents/" + o.IncidentID.String()
	}
	switch o.Field {
	case OverrideEvery:
		if e := spec.GetSchedule().GetEvery(); e != nil {
			if b, err := protojson.Marshal(e); err == nil {
				out.SpecValue = string(b)
			}
		}
	case OverrideRateLimitScale:
		out.SpecValue = "1"
	}
	return out
}

// Kept says whether an override outlasts a new version: one whose field
// the version changes is dropped. prev and next are the versions' specs,
// and prevSource and nextSource their source connectors' digests.
func Kept(field string, prev, next *apiv1.PipelineSpec, prevSource, nextSource string) bool {
	switch field {
	case OverrideEvery:
		return next.GetSchedule().GetEvery() != nil && proto.Equal(prev.GetSchedule(), next.GetSchedule())
	case OverrideRateLimitScale:
		return prevSource == nextSource
	}
	return false
}

// declaresRateLimit says whether the source connector with digest is a
// declarative source that declares a rate limit, which heal can lower.
func declaresRateLimit(ctx context.Context, q *db.Queries, org uuid.UUID, digest string) (bool, error) {
	if digest == "" {
		return false, nil
	}
	a, err := q.GetArtifact(ctx, db.GetArtifactParams{OrgID: org, Digest: digest})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil || a.MediaType != connectorhost.MediaTypeManifest {
		return false, err
	}
	m, err := declarative.Load(a.Content)
	if err != nil {
		return false, nil
	}
	return m.RateLimit() > 0, nil
}

// slowDown is rate_limited's playbook, from the incident's third
// occurrence and every third after: syncs that keep failing rate limited
// need a source that sends fewer requests, or fewer syncs. It halves a
// declarative source's declared rate limit, and doubles the schedule's
// interval if the source has no limit left to lower, or the sync ran at
// least as long as the interval. It returns the schedule syncs now start
// on, if it changed.
func slowDown(ctx context.Context, q *db.Queries, org uuid.UUID, t target, o Observation, inc *db.Incident) (*apiv1.Schedule, error) {
	if t.mode == apiv1.HealMode_HEAL_MODE_OFF || inc.Occurrences%slowDownEvery != 0 {
		return nil, nil
	}
	apply := t.mode != apiv1.HealMode_HEAL_MODE_SUGGEST
	now := time.Now().UTC()
	var actions []Action
	lowered := false
	declares, err := declaresRateLimit(ctx, q, org, t.sourceDigest)
	if err != nil {
		return nil, err
	}
	if declares {
		scale := 1.0
		if v, err := q.GetPipelineOverride(ctx, db.GetPipelineOverrideParams{OrgID: org, PipelineID: inc.PipelineID, Field: OverrideRateLimitScale}); err == nil {
			if scale, err = ParseScale(v.Value); err != nil {
				return nil, err
			}
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return nil, err
		}
		if next := max(scale/2, minRateLimitScale); next < scale {
			lowered = true
			a := Action{Kind: "lower_rate_limit", State: "proposed", Time: now,
				Description: fmt.Sprintf("Lower the source's rate limit to %s of what it declares, so it sends fewer requests; heal mode suggest doesn't.", fraction(next))}
			if apply {
				v := []byte(strconv.FormatFloat(next, 'f', -1, 64))
				reason := fmt.Sprintf("%d syncs in a row failed rate limited.", inc.Occurrences)
				if err := setOverride(ctx, q, org, t, inc, OverrideRateLimitScale, v, reason); err != nil {
					return nil, err
				}
				a.State, a.Description = "applied", fmt.Sprintf("Lowered the source's rate limit to %s of what it declares, so it sends fewer requests.", fraction(next))
			}
			actions = append(actions, a)
		}
	}
	var sched *apiv1.Schedule
	if every := t.schedule.GetEvery(); every != nil {
		cur := every.AsDuration()
		ov, err := q.GetPipelineOverride(ctx, db.GetPipelineOverrideParams{OrgID: org, PipelineID: inc.PipelineID, Field: OverrideEvery})
		switch {
		case err == nil:
			if cur, err = ParseEvery(ov.Value); err != nil {
				return nil, err
			}
		case !errors.Is(err, pgx.ErrNoRows):
			return nil, err
		}
		next := min(cur*2, maxEvery)
		if next > cur && (!lowered || o.Ran >= cur) {
			a := Action{Kind: "widen_schedule", State: "proposed", Time: now,
				Description: fmt.Sprintf("Start syncs every %s instead of every %s, so the source gets fewer requests; heal mode suggest doesn't.", short(next), short(cur))}
			if apply {
				v, err := everyValue(next)
				if err != nil {
					return nil, err
				}
				reason := fmt.Sprintf("%d syncs in a row failed rate limited.", inc.Occurrences)
				if err := setOverride(ctx, q, org, t, inc, OverrideEvery, v, reason); err != nil {
					return nil, err
				}
				if sched, err = Schedule(t.schedule, v); err != nil {
					return nil, err
				}
				a.State, a.Description = "applied", fmt.Sprintf("Syncs start every %s instead of every %s, so the source gets fewer requests.", short(next), short(cur))
			}
			actions = append(actions, a)
		}
	}
	return sched, addActions(ctx, q, org, inc, actions...)
}

// setOverride sets one of inc's playbook's overrides, and records
// pipeline.overridden.
func setOverride(ctx context.Context, q *db.Queries, org uuid.UUID, t target, inc *db.Incident, field string, value []byte, reason string) error {
	if _, err := q.SetPipelineOverride(ctx, db.SetPipelineOverrideParams{
		OrgID: org, PipelineID: inc.PipelineID, Field: field, Value: value, Reason: reason, IncidentID: &inc.ID,
	}); err != nil {
		return err
	}
	return RecordOverride(ctx, q, org, t.workspaceID, t.workspace, t.pipeline, field, string(value), &inc.ID)
}

// RecordOverride records pipeline.overridden for an override set to value,
// or removed if value is empty.
func RecordOverride(ctx context.Context, q *db.Queries, org, workspaceID uuid.UUID, workspace, pipeline, field, value string, incident *uuid.UUID) error {
	name := "workspaces/" + workspace + "/pipelines/" + pipeline
	data := events.Pipeline{Pipeline: name, Field: field, Value: value}
	if incident != nil {
		data.Incident = name + "/incidents/" + incident.String()
	}
	_, err := events.Record(ctx, q, org, events.Event{Workspace: workspaceID, Type: events.PipelineOverridden, Pipeline: pipeline, Data: data})
	return err
}

// addActions records actions on inc.
func addActions(ctx context.Context, q *db.Queries, org uuid.UUID, inc *db.Incident, add ...Action) error {
	if len(add) == 0 {
		return nil
	}
	var actions []Action
	if err := json.Unmarshal(inc.Actions, &actions); err != nil {
		return err
	}
	raw, err := json.Marshal(append(actions, add...))
	if err != nil {
		return err
	}
	if err := q.SetIncidentActions(ctx, db.SetIncidentActionsParams{OrgID: org, ID: inc.ID, Actions: raw}); err != nil {
		return err
	}
	inc.Actions = raw
	return nil
}

func fraction(s float64) string {
	switch s {
	case 0.5:
		return "half"
	case 0.25:
		return "a quarter"
	case 0.125:
		return "an eighth"
	}
	return strconv.FormatFloat(s, 'f', -1, 64) + "×"
}

// short is d as the spec writes it, without zero units: 2h, not 2h0m0s.
func short(d time.Duration) string {
	s := d.String()
	if strings.HasSuffix(s, "m0s") {
		s = s[:len(s)-2]
	}
	if strings.HasSuffix(s, "h0m") {
		s = s[:len(s)-2]
	}
	return s
}
