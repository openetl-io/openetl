package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeApply struct {
	apiv1connect.UnimplementedApplyServiceHandler
}

func (fakeApply) Apply(_ context.Context, req *connect.Request[apiv1.ApplyRequest]) (*connect.Response[apiv1.ApplyResponse], error) {
	if !req.Msg.GetValidateOnly() {
		return nil, connect.NewError(connect.CodeInternal, errors.New("want validate only"))
	}
	return connect.NewResponse(&apiv1.ApplyResponse{
		Resources: []*apiv1.AppliedResource{
			{Name: "workspaces/default/sources/shop", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "0123456789abcdef"},
		},
		Dependents: []*apiv1.AppliedResource{
			{Name: "workspaces/default/pipelines/shop-to-dw", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "fedcba9876543210"},
		},
		Plans: []*apiv1.PipelinePlan{{
			Pipeline: "workspaces/default/pipelines/shop-to-dw", Version: "fedcba9876543210",
			Changes: []*apiv1.PlanChange{{Field: "source.version", Before: `"01"`, After: `"02"`}, {Field: "delivery.require", After: `"DELIVERY_LEVEL_AT_LEAST_ONCE"`}},
			Streams: []*apiv1.StreamPlan{
				{Name: "orders", Level: apiv1.DeliveryLevel_DELIVERY_LEVEL_EXACTLY_ONCE, Reason: "exact replay", Write: apiv1.WriteMode_WRITE_MODE_OVERWRITE},
				{Name: "events", Level: apiv1.DeliveryLevel_DELIVERY_LEVEL_AT_LEAST_ONCE, Reason: "superset replay, no primary key, and not collapsed", Write: apiv1.WriteMode_WRITE_MODE_APPEND},
			},
			Problems: []string{`delivery.require: stream "events" gets at_least_once`},
			Notes:    []string{"a note"},
			Warnings: []*apiv1.PlanWarning{{Stream: "logs", Message: "it has no primary key"}},
		}},
	}), nil
}

// openetl apply lists the pipelines that a change gave new versions, and
// openetl plan shows their plans.
func TestApplyListsDependents(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewApplyServiceHandler(fakeApply{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	file := filepath.Join(t.TempDir(), "shop.yaml")
	if err := os.WriteFile(file, []byte("apiVersion: openetl.io/v1alpha1\nkind: Source\nmetadata: {name: shop}\nspec: {connector: faker}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(t.Context(), []string{"apply", "--api-url", srv.URL, "--dry-run", "-f", file}, &out); err != nil {
		t.Fatal(err)
	}
	want := "would be updated workspaces/default/sources/shop (version 0123456789ab)\n" +
		"would be updated workspaces/default/pipelines/shop-to-dw (version fedcba987654), which uses what changed\n"
	if w := want + "\nwarnings for workspaces/default/pipelines/shop-to-dw:\n  logs: it has no primary key\n"; out.String() != w {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), w)
	}

	out.Reset()
	err := run(t.Context(), []string{"plan", "--api-url", srv.URL, "-f", file}, &out)
	if err == nil || !strings.Contains(err.Error(), "apply would refuse it") {
		t.Errorf("a plan with problems: %v", err)
	}
	want += `
workspaces/default/pipelines/shop-to-dw (version fedcba987654)
  changes:
    source.version: "01" -> "02"
    delivery.require: (unset) -> "DELIVERY_LEVEL_AT_LEAST_ONCE"
  streams:
    orders  overwrite  exactly_once   exact replay
    events  append     at_least_once  superset replay, no primary key, and not collapsed
  problems:
    delivery.require: stream "events" gets at_least_once
  warnings:
    logs: it has no primary key
  notes:
    a note
`
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}
