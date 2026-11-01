package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

type fakeApply struct {
	apiv1connect.UnimplementedApplyServiceHandler
}

func (fakeApply) Apply(_ context.Context, req *connect.Request[apiv1.ApplyRequest]) (*connect.Response[apiv1.ApplyResponse], error) {
	return connect.NewResponse(&apiv1.ApplyResponse{
		Resources: []*apiv1.AppliedResource{
			{Name: "workspaces/default/sources/shop", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "0123456789abcdef"},
		},
		Dependents: []*apiv1.AppliedResource{
			{Name: "workspaces/default/pipelines/shop-to-dw", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "fedcba9876543210"},
		},
	}), nil
}

// openetl apply lists the pipelines that a change gave new versions.
func TestApplyListsDependents(t *testing.T) {
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewApplyServiceHandler(fakeApply{}))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	file := filepath.Join(t.TempDir(), "shop.yaml")
	if err := os.WriteFile(file, []byte("apiVersion: openetl.dev/v1alpha1\nkind: Source\nmetadata: {name: shop}\nspec: {connector: faker}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err := run(t.Context(), []string{"apply", "--api-url", srv.URL, "--dry-run", "-f", file}, &out); err != nil {
		t.Fatal(err)
	}
	want := "would be updated workspaces/default/sources/shop (version 0123456789ab)\n" +
		"would be updated workspaces/default/pipelines/shop-to-dw (version fedcba987654), which uses what changed\n"
	if out.String() != want {
		t.Errorf("printed\n%s\nwant\n%s", out.String(), want)
	}
}
