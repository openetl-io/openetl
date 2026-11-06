package api

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/events"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
)

func (s *Server) applyWebhook(ctx context.Context, q *db.Queries, doc *spec.Document, wsID uuid.UUID) (*apiv1.AppliedResource, error) {
	w := doc.Webhook
	for _, p := range w.GetEvents() {
		if err := events.CheckPattern(p); err != nil {
			return nil, invalid("events: %v", err)
		}
	}
	if err := s.checkSecretsExist(ctx, q, wsID, doc.Workspace, w.GetSecrets()); err != nil {
		return nil, err
	}
	version, err := digest(w)
	if err != nil {
		return nil, err
	}
	stored, err := protojson.Marshal(w)
	if err != nil {
		return nil, err
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: resourceName(doc.Workspace, "webhooks", doc.Name), Version: version}
	cur, err := q.GetWebhook(ctx, db.GetWebhookParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: doc.Name})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
		err = q.InsertWebhook(ctx, db.InsertWebhookParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: uuid.New(), Name: doc.Name, Spec: stored, Version: version, Secrets: w.GetSecrets()})
	case err != nil:
		return nil, err
	case cur.Version == version:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	default:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
		err = q.UpdateWebhook(ctx, db.UpdateWebhookParams{OrgID: s.Tenant.Org, ID: cur.ID, Spec: stored, Version: version, Secrets: w.GetSecrets()})
	}
	return res, err
}

func toWebhook(ws string, r db.Webhook) (*apiv1.Webhook, error) {
	out := &apiv1.Webhook{
		Name: resourceName(ws, "webhooks", r.Name), Spec: &apiv1.WebhookSpec{}, Version: r.Version, Etag: etag(r.Revision),
		State: apiv1.WebhookState_WEBHOOK_STATE_ENABLED, CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime),
	}
	if r.Disabled {
		out.State = apiv1.WebhookState_WEBHOOK_STATE_DISABLED
	}
	if r.DisabledReason != nil {
		out.DisabledReason = *r.DisabledReason
	}
	if r.FailingSince != nil {
		out.FailingSince = timestamppb.New(*r.FailingSince)
	}
	return out, protojson.Unmarshal(r.Spec, out.Spec)
}

func (s *Server) getWebhook(ctx context.Context, name string) (string, db.Webhook, error) {
	ws, id, err := parseName(name, "webhooks")
	if err != nil {
		return "", db.Webhook{}, err
	}
	wsID, err := s.wsID(ctx, ws)
	if err != nil {
		return "", db.Webhook{}, err
	}
	r, err := s.Store.GetWebhook(ctx, db.GetWebhookParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return "", r, notFound("%s doesn't exist", name)
	}
	return ws, r, err
}

func (s *Server) GetWebhook(ctx context.Context, req *connect.Request[apiv1.GetWebhookRequest]) (*connect.Response[apiv1.GetWebhookResponse], error) {
	ws, r, err := s.getWebhook(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := toWebhook(ws, r)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetWebhookResponse{Webhook: out}), nil
}

func (s *Server) ListWebhooks(ctx context.Context, req *connect.Request[apiv1.ListWebhooksRequest]) (*connect.Response[apiv1.ListWebhooksResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListWebhooks(ctx, db.ListWebhooksParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListWebhooksResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	for _, r := range rows {
		out, err := toWebhook(ws, r)
		if err != nil {
			return nil, err
		}
		res.Webhooks = append(res.Webhooks, out)
	}
	return connect.NewResponse(res), nil
}

func (s *Server) DeleteWebhook(ctx context.Context, req *connect.Request[apiv1.DeleteWebhookRequest]) (*connect.Response[apiv1.DeleteWebhookResponse], error) {
	_, r, err := s.getWebhook(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		cur, err := q.LockWebhook(ctx, db.LockWebhookParams{OrgID: s.Tenant.Org, WorkspaceID: r.WorkspaceID, Name: r.Name})
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		if err != nil {
			return err
		}
		if err := checkEtag(req.Msg.GetEtag(), cur.Revision); err != nil {
			return err
		}
		return q.DeleteWebhook(ctx, db.DeleteWebhookParams{OrgID: s.Tenant.Org, ID: cur.ID})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteWebhookResponse{}), nil
}

func (s *Server) DisableWebhook(ctx context.Context, req *connect.Request[apiv1.DisableWebhookRequest]) (*connect.Response[apiv1.DisableWebhookResponse], error) {
	out, err := s.setWebhookDisabled(ctx, req.Msg.GetName(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DisableWebhookResponse{Webhook: out}), nil
}

func (s *Server) EnableWebhook(ctx context.Context, req *connect.Request[apiv1.EnableWebhookRequest]) (*connect.Response[apiv1.EnableWebhookResponse], error) {
	out, err := s.setWebhookDisabled(ctx, req.Msg.GetName(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.EnableWebhookResponse{Webhook: out}), nil
}

// setWebhookDisabled changes nothing if the webhook is in that state already.
func (s *Server) setWebhookDisabled(ctx context.Context, name string, disabled bool) (*apiv1.Webhook, error) {
	ws, r, err := s.getWebhook(ctx, name)
	if err != nil {
		return nil, err
	}
	if disabled {
		reason := "disabled by " + callerOf(ctx).principal
		err = s.Store.DisableWebhook(ctx, db.DisableWebhookParams{OrgID: s.Tenant.Org, ID: r.ID, Reason: &reason})
	} else {
		err = s.Store.EnableWebhook(ctx, db.EnableWebhookParams{OrgID: s.Tenant.Org, ID: r.ID})
	}
	if err != nil {
		return nil, err
	}
	if _, r, err = s.getWebhook(ctx, name); err != nil {
		return nil, err
	}
	return toWebhook(ws, r)
}

// TestWebhook records the test event already fanned out to the webhook, so
// its delivery can be named.
func (s *Server) TestWebhook(ctx context.Context, req *connect.Request[apiv1.TestWebhookRequest]) (*connect.Response[apiv1.TestWebhookResponse], error) {
	_, r, err := s.getWebhook(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	if r.Disabled {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New(req.Msg.GetName()+" is disabled; enable it first"))
	}
	delivery, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		ev, err := events.Record(ctx, q, s.Tenant.Org, events.Event{
			Workspace: r.WorkspaceID, Type: events.WebhookTest, Data: events.Test{Webhook: req.Msg.GetName()}, Webhook: &r.ID,
		})
		if err != nil {
			return err
		}
		if err := q.InsertDelivery(ctx, db.InsertDeliveryParams{OrgID: s.Tenant.Org, ID: delivery, WebhookID: r.ID, EventID: ev}); err != nil {
			return err
		}
		return q.MarkEventsDispatched(ctx, db.MarkEventsDispatchedParams{OrgID: s.Tenant.Org, Ids: []uuid.UUID{ev}})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.TestWebhookResponse{Delivery: req.Msg.GetName() + "/deliveries/" + delivery.String()}), nil
}

func (s *Server) ListWebhookDeliveries(ctx context.Context, req *connect.Request[apiv1.ListWebhookDeliveriesRequest]) (*connect.Response[apiv1.ListWebhookDeliveriesResponse], error) {
	_, r, err := s.getWebhook(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	params := db.ListWebhookDeliveriesParams{OrgID: s.Tenant.Org, WebhookID: r.ID, MaxRows: n}
	if after != "" {
		at, id, _ := strings.Cut(after, " ")
		t, err1 := time.Parse(time.RFC3339Nano, at)
		u, err2 := uuid.Parse(id)
		if err1 != nil || err2 != nil {
			return nil, invalid("bad page_token")
		}
		params.BeforeTime, params.BeforeID = &t, u
	}
	rows, err := s.Store.ListWebhookDeliveries(ctx, params)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListWebhookDeliveriesResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		last := rows[len(rows)-1]
		res.NextPageToken = nextToken(last.CreateTime.Format(time.RFC3339Nano) + " " + last.ID.String())
	}
	for _, d := range rows {
		res.Deliveries = append(res.Deliveries, toDelivery(req.Msg.GetParent(), d))
	}
	return connect.NewResponse(res), nil
}

func toDelivery(webhook string, d db.ListWebhookDeliveriesRow) *apiv1.WebhookDelivery {
	out := &apiv1.WebhookDelivery{
		Name: webhook + "/deliveries/" + d.ID.String(), EventId: d.EventID.String(), EventType: d.EventType,
		State: apiv1.DeliveryState(apiv1.DeliveryState_value["DELIVERY_STATE_"+strings.ToUpper(d.State)]), Attempts: d.Attempts,
		CreateTime: timestamppb.New(d.CreateTime),
	}
	if d.State == "pending" {
		out.NextAttemptTime = timestamppb.New(d.NextAttemptTime)
	}
	if d.LastAttemptTime != nil {
		out.LastAttemptTime = timestamppb.New(*d.LastAttemptTime)
	}
	if d.ResponseStatus != nil {
		out.ResponseStatus = *d.ResponseStatus
	}
	if d.ResponseBody != nil {
		out.ResponseBody = *d.ResponseBody
	}
	if d.Error != nil {
		out.Error = *d.Error
	}
	if d.LatencyMs != nil {
		out.Latency = durationpb.New(time.Duration(*d.LatencyMs) * time.Millisecond)
	}
	return out
}
