package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
)

type fakeAccess struct {
	apiv1connect.UnimplementedAccessServiceHandler
	grants []*apiv1.GrantRoleRequest
	keys   []*apiv1.CreateKeyRequest
}

func (f *fakeAccess) GrantRole(_ context.Context, req *connect.Request[apiv1.GrantRoleRequest]) (*connect.Response[apiv1.GrantRoleResponse], error) {
	f.grants = append(f.grants, req.Msg)
	return connect.NewResponse(&apiv1.GrantRoleResponse{}), nil
}

func (f *fakeAccess) CreateKey(_ context.Context, req *connect.Request[apiv1.CreateKeyRequest]) (*connect.Response[apiv1.CreateKeyResponse], error) {
	f.keys = append(f.keys, req.Msg)
	return connect.NewResponse(&apiv1.CreateKeyResponse{Key: &apiv1.Key{Name: req.Msg.GetParent() + "/keys/k1"}, Secret: "oetl_new"}), nil
}

func (f *fakeAccess) CreateRunnerToken(_ context.Context, req *connect.Request[apiv1.CreateRunnerTokenRequest]) (*connect.Response[apiv1.CreateRunnerTokenResponse], error) {
	return connect.NewResponse(&apiv1.CreateRunnerTokenResponse{Token: &apiv1.RunnerToken{Name: req.Msg.GetParent() + "/tokens/t1"}, Secret: "oetl_dp_new"}), nil
}

func TestAccessCommands(t *testing.T) {
	f := &fakeAccess{}
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewAccessServiceHandler(f))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	// words is the command, without its flags.
	cmd := func(words int, args ...string) string {
		t.Helper()
		var out bytes.Buffer
		args = append(args[:words:words], append([]string{"--api-url", srv.URL}, args[words:]...)...)
		if err := run(t.Context(), args, &out); err != nil {
			t.Fatalf("%s: %v", strings.Join(args, " "), err)
		}
		return out.String()
	}

	if out := cmd(2, "principal", "grant", "--workspace", "sales", "ci", "operator"); out != "granted principals/ci operator on workspace sales\n" {
		t.Errorf("grant printed %q", out)
	}
	if g := f.grants[0]; g.GetName() != "principals/ci" || g.GetBinding().GetRole() != apiv1.Role_ROLE_OPERATOR || g.GetBinding().GetWorkspace() != "workspaces/sales" {
		t.Errorf("grant %v", g)
	}
	if err := run(t.Context(), []string{"principal", "grant", "--api-url", srv.URL, "ci", "superuser"}, &bytes.Buffer{}); err == nil {
		t.Error("granted a role that doesn't exist")
	}

	if out := cmd(2, "key", "create", "--expires", "90d", "ci"); !strings.HasSuffix(out, "\noetl_new\n") {
		t.Errorf("key create printed %q", out)
	}
	if e := f.keys[0].GetExpireTime().AsTime(); time.Until(e) < 89*24*time.Hour || time.Until(e) > 91*24*time.Hour {
		t.Errorf("expires %v", e)
	}
	file := filepath.Join(t.TempDir(), "runner.token")
	if out := cmd(3, "dataplane", "token", "create", "--out", file, "eu"); strings.Contains(out, "oetl_dp_new") {
		t.Errorf("printed the token it wrote to a file: %q", out)
	}
	if got, err := auth.ReadKeyFile(file); err != nil || got != "oetl_dp_new" {
		t.Errorf("token file %q, %v", got, err)
	}
}
