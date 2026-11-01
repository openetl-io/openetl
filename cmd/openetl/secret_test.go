package main

import (
	"bytes"
	"context"
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

type fakeSecrets struct {
	apiv1connect.UnimplementedSecretServiceHandler
	set []*apiv1.UpdateSecretRequest
}

func (f *fakeSecrets) UpdateSecret(_ context.Context, req *connect.Request[apiv1.UpdateSecretRequest]) (*connect.Response[apiv1.UpdateSecretResponse], error) {
	f.set = append(f.set, req.Msg)
	return connect.NewResponse(&apiv1.UpdateSecretResponse{Secret: &apiv1.Secret{Name: req.Msg.GetSecret().GetName()}, Created: len(f.set) == 1}), nil
}

func (f *fakeSecrets) ListSecrets(_ context.Context, req *connect.Request[apiv1.ListSecretsRequest]) (*connect.Response[apiv1.ListSecretsResponse], error) {
	if req.Msg.GetPageToken() == "" {
		return connect.NewResponse(&apiv1.ListSecretsResponse{NextPageToken: "2", Secrets: []*apiv1.Secret{
			{Name: "workspaces/w/secrets/pg-dsn", UsedBy: []string{"workspaces/w/sources/shop", "workspaces/w/destinations/dw"}},
		}}), nil
	}
	return connect.NewResponse(&apiv1.ListSecretsResponse{Secrets: []*apiv1.Secret{{Name: "workspaces/w/secrets/unused"}}}), nil
}

func TestSecretCommands(t *testing.T) {
	f := &fakeSecrets{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewSecretServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	file := filepath.Join(t.TempDir(), "dsn")
	if err := os.WriteFile(file, []byte("postgres://u:p@h/db\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	for range 2 {
		if err := run(t.Context(), []string{"secret", "set", "--api-url", srv.URL, "--workspace", "w", "--from-file", file, "pg-dsn"}, &out); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.set) != 2 {
		t.Fatalf("%d updates", len(f.set))
	}
	if s := f.set[0]; s.GetSecret().GetName() != "workspaces/w/secrets/pg-dsn" || s.GetSecret().GetValue() != "postgres://u:p@h/db" || !s.GetAllowMissing() {
		t.Errorf("update %v", s)
	}
	if out.String() != "created workspaces/w/secrets/pg-dsn\nupdated workspaces/w/secrets/pg-dsn\n" {
		t.Errorf("set printed %q", out.String())
	}

	out.Reset()
	if err := run(t.Context(), []string{"secret", "list", "--api-url", srv.URL, "--workspace", "w"}, &out); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[1], "pg-dsn ") || !strings.HasSuffix(lines[1], "sources/shop, destinations/dw") || !strings.HasPrefix(lines[2], "unused ") {
		t.Errorf("list printed\n%s", out.String())
	}
}
