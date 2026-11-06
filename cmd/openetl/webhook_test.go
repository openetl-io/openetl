package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeWebhooks struct {
	apiv1connect.UnimplementedWebhookServiceHandler
	calls []string
}

func (f *fakeWebhooks) ListWebhooks(_ context.Context, req *connect.Request[apiv1.ListWebhooksRequest]) (*connect.Response[apiv1.ListWebhooksResponse], error) {
	f.calls = append(f.calls, "list "+req.Msg.GetParent())
	return connect.NewResponse(&apiv1.ListWebhooksResponse{Webhooks: []*apiv1.Webhook{
		{Name: "workspaces/w/webhooks/ops", State: apiv1.WebhookState_WEBHOOK_STATE_ENABLED, Spec: &apiv1.WebhookSpec{Url: "https://example.com/hook", Events: []string{"sync.*"}}},
		{Name: "workspaces/w/webhooks/old", State: apiv1.WebhookState_WEBHOOK_STATE_DISABLED, DisabledReason: "its endpoint answered 410 Gone",
			Spec: &apiv1.WebhookSpec{Url: "https://example.com/old", Events: []string{"*"}, Pipelines: []string{"shop"}}},
	}}), nil
}

func (f *fakeWebhooks) ListWebhookDeliveries(_ context.Context, req *connect.Request[apiv1.ListWebhookDeliveriesRequest]) (*connect.Response[apiv1.ListWebhookDeliveriesResponse], error) {
	f.calls = append(f.calls, "deliveries "+req.Msg.GetParent())
	return connect.NewResponse(&apiv1.ListWebhookDeliveriesResponse{Deliveries: []*apiv1.WebhookDelivery{
		{EventType: "sync.failed", State: apiv1.DeliveryState_DELIVERY_STATE_PENDING, Attempts: 2, ResponseStatus: 503, Error: "503 Service Unavailable",
			NextAttemptTime: timestamppb.Now(), CreateTime: timestamppb.Now()},
	}}), nil
}

func (f *fakeWebhooks) TestWebhook(_ context.Context, req *connect.Request[apiv1.TestWebhookRequest]) (*connect.Response[apiv1.TestWebhookResponse], error) {
	f.calls = append(f.calls, "test "+req.Msg.GetName())
	return connect.NewResponse(&apiv1.TestWebhookResponse{Delivery: req.Msg.GetName() + "/deliveries/1"}), nil
}

func (f *fakeWebhooks) DisableWebhook(_ context.Context, req *connect.Request[apiv1.DisableWebhookRequest]) (*connect.Response[apiv1.DisableWebhookResponse], error) {
	f.calls = append(f.calls, "disable "+req.Msg.GetName())
	return connect.NewResponse(&apiv1.DisableWebhookResponse{}), nil
}

func TestWebhookCommands(t *testing.T) {
	f := &fakeWebhooks{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewWebhookServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")

	var out bytes.Buffer
	for _, args := range [][]string{
		{"list", "--workspace", "w"},
		{"deliveries", "--workspace", "w", "ops"},
		{"test", "--workspace", "w", "ops"},
		{"disable", "--workspace", "w", "ops"},
	} {
		if err := run(t.Context(), append([]string{"webhook", args[0], "--api-url", srv.URL}, args[1:]...), &out); err != nil {
			t.Fatalf("%v: %v", args, err)
		}
	}
	got := out.String()
	for _, want := range []string{
		"ops   enabled", "sync.*", "all",
		"old   disabled: its endpoint answered 410 Gone", "shop",
		"sync.failed  pending, next", "503", "sent workspaces/w/webhooks/ops/deliveries/1", "disabled workspaces/w/webhooks/ops",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("output lacks %q:\n%s", want, got)
		}
	}
	if want := "list workspaces/w,deliveries workspaces/w/webhooks/ops,test workspaces/w/webhooks/ops,disable workspaces/w/webhooks/ops"; strings.Join(f.calls, ",") != want {
		t.Errorf("calls %v", f.calls)
	}
	if err := run(t.Context(), []string{"webhook", "test", "--api-url", srv.URL}, &out); err == nil {
		t.Error("webhook test without a name")
	}
}
