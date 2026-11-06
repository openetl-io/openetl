package api

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/heal"
	"github.com/openetl-io/openetl/internal/store/db"
)

func toIncident(pipeline string, r db.Incident) (*apiv1.Incident, error) {
	out := &apiv1.Incident{
		Name:          pipeline + "/incidents/" + r.ID.String(),
		Class:         apiv1.IncidentClass(apiv1.IncidentClass_value["INCIDENT_CLASS_"+strings.ToUpper(r.Class)]),
		Stream:        r.Stream,
		State:         apiv1.IncidentState(apiv1.IncidentState_value["INCIDENT_STATE_"+strings.ToUpper(r.Status)]),
		Severity:      apiv1.IncidentSeverity(apiv1.IncidentSeverity_value["INCIDENT_SEVERITY_"+strings.ToUpper(r.Severity)]),
		Summary:       r.Summary,
		Message:       r.Message,
		NextSteps:     r.NextSteps,
		Fingerprint:   r.Fingerprint,
		Occurrences:   r.Occurrences,
		FirstSeenTime: timestamppb.New(r.FirstSeenTime),
		LastSeenTime:  timestamppb.New(r.LastSeenTime),
	}
	if r.Code != nil {
		out.Code = *r.Code
	}
	if r.SyncID != nil {
		out.Sync = pipeline + "/syncs/" + r.SyncID.String()
	}
	if r.ResolveTime != nil {
		out.ResolveTime = timestamppb.New(*r.ResolveTime)
	}
	if r.Resolution != nil {
		out.Resolution = *r.Resolution
	}
	if err := json.Unmarshal(r.Details, &out.Details); err != nil {
		return nil, err
	}
	var actions []heal.Action
	if err := json.Unmarshal(r.Actions, &actions); err != nil {
		return nil, err
	}
	for _, a := range actions {
		out.Actions = append(out.Actions, &apiv1.HealAction{
			Kind:        apiv1.HealActionKind(apiv1.HealActionKind_value["HEAL_ACTION_KIND_"+strings.ToUpper(a.Kind)]),
			State:       apiv1.HealActionState(apiv1.HealActionState_value["HEAL_ACTION_STATE_"+strings.ToUpper(a.State)]),
			Description: a.Description,
			Time:        timestamppb.New(a.Time),
		})
	}
	return out, nil
}

// getIncident returns the incident named
// workspaces/{w}/pipelines/{p}/incidents/{i}, and its pipeline's name.
func (s *Server) getIncident(ctx context.Context, name string) (string, db.Incident, error) {
	pipeline, id, ok := strings.Cut(name, "/incidents/")
	inc, err := uuid.Parse(id)
	if !ok || err != nil || inc.String() != id {
		return "", db.Incident{}, invalid("name %q isn't workspaces/{workspace}/pipelines/{pipeline}/incidents/{incident}", name)
	}
	_, p, err := s.getPipeline(ctx, pipeline)
	if err != nil {
		return "", db.Incident{}, err
	}
	r, err := s.Store.GetIncident(ctx, db.GetIncidentParams{OrgID: s.Tenant.Org, PipelineID: p.ID, ID: inc})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", db.Incident{}, notFound("%s doesn't exist", name)
	}
	return pipeline, r.Incident, err
}

func (s *Server) GetIncident(ctx context.Context, req *connect.Request[apiv1.GetIncidentRequest]) (*connect.Response[apiv1.GetIncidentResponse], error) {
	pipeline, r, err := s.getIncident(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := toIncident(pipeline, r)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetIncidentResponse{Incident: out}), nil
}

func (s *Server) ListIncidents(ctx context.Context, req *connect.Request[apiv1.ListIncidentsRequest]) (*connect.Response[apiv1.ListIncidentsResponse], error) {
	parent := req.Msg.GetParent()
	ws, id, err := parseName(parent, "pipelines")
	if err != nil {
		return nil, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListIncidentsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, MaxRows: n}
	if id != "-" {
		_, p, err := s.getPipeline(ctx, parent)
		if err != nil {
			return nil, err
		}
		params.PipelineID = &p.ID
	}
	if st := req.Msg.GetState(); st != apiv1.IncidentState_INCIDENT_STATE_UNSPECIFIED {
		status := strings.ToLower(strings.TrimPrefix(st.String(), "INCIDENT_STATE_"))
		params.Status = &status
	}
	if after != "" {
		at, last, _ := strings.Cut(after, " ")
		t, err1 := time.Parse(time.RFC3339Nano, at)
		u, err2 := uuid.Parse(last)
		if err1 != nil || err2 != nil {
			return nil, invalid("bad page_token")
		}
		params.BeforeTime, params.BeforeID = &t, u
	}
	rows, err := s.Store.ListIncidents(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListIncidentsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1].Incident
		res.NextPageToken = nextToken(last.FirstSeenTime.Format(time.RFC3339Nano) + " " + last.ID.String())
	}
	for _, r := range rows {
		out, err := toIncident(resourceName(ws, "pipelines", r.Pipeline), r.Incident)
		if err != nil {
			return nil, err
		}
		res.Incidents = append(res.Incidents, out)
	}
	return connect.NewResponse(res), nil
}

func (s *Server) ResolveIncident(ctx context.Context, req *connect.Request[apiv1.ResolveIncidentRequest]) (*connect.Response[apiv1.ResolveIncidentResponse], error) {
	pipeline, r, err := s.getIncident(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	resolution := "resolved by " + callerOf(ctx).actor()
	if c := strings.TrimSpace(req.Msg.GetComment()); c != "" {
		resolution += ": " + truncate(c, 1000)
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		r, err = heal.ResolveOne(ctx, q, s.Tenant.Org, r.ID, resolution)
		return err
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(req.Msg.GetName()+" is resolved already"))
	}
	if err != nil {
		return nil, err
	}
	out, err := toIncident(pipeline, r)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ResolveIncidentResponse{Incident: out}), nil
}

// crashLoopFailures is how many attempts of a continuous run fail in a row,
// without a commit, before it's a crash loop.
const crashLoopFailures = 3

func (s *Server) ReportAttemptFailure(ctx context.Context, req *connect.Request[dataplanev1.ReportAttemptFailureRequest]) (*connect.Response[dataplanev1.ReportAttemptFailureResponse], error) {
	m := req.Msg
	id, err := uuid.Parse(m.GetPipelineId())
	if err != nil {
		return nil, invalid("pipeline_id: %v", err)
	}
	if err := s.pipelineOnDataPlane(ctx, id); err != nil {
		return nil, err
	}
	if m.GetRunId() == "" || m.GetAttempt() < 1 {
		return nil, invalid("run_id and attempt are needed")
	}
	at := time.Now()
	if m.GetTime().IsValid() {
		at = m.GetTime().AsTime()
	}
	msg := truncate(m.GetMessage(), maxError)
	arg := db.RecordAttemptFailureParams{
		OrgID: s.Tenant.Org, PipelineID: id, RunID: m.GetRunId(), Attempt: m.GetAttempt(), Error: &msg, Time: &at,
	}
	if code := truncate(m.GetCode(), 64); code != "" {
		arg.Code = &code
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		row, err := q.RecordAttemptFailure(ctx, arg)
		if errors.Is(err, pgx.ErrNoRows) {
			// Reported already, or its sync has ended.
			return nil
		}
		if err != nil || !row.Continuous || row.FailuresSinceCommit < crashLoopFailures {
			return err
		}
		_, err = heal.Observe(ctx, q, s.Tenant.Org, heal.Observation{
			Pipeline: id, Version: row.Version, Class: heal.CrashLoop, Code: m.GetCode(), Message: msg,
			Details: map[string]string{"failures": strconv.Itoa(int(row.FailuresSinceCommit)), "attempt": strconv.Itoa(int(m.GetAttempt()))},
			Sync:    &row.ID, Key: row.ID.String(),
		})
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportAttemptFailureResponse{}), nil
}
