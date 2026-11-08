//go:build integration

package api_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"gocloud.dev/secrets/localsecrets"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

// oneConnection is a server whose pool has one connection, so a request that
// asks for a second while holding a transaction waits for ever. Its keyring
// has no data key yet.
func oneConnection(t *testing.T) (context.Context, string) {
	t.Helper()
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	setup, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(setup.Close)
	if err := setup.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := setup.Bootstrap(ctx, "dp1", nil)
	if err != nil {
		t.Fatal(err)
	}
	one, err := store.Open(ctx, url+"?pool_max_conns=1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(one.Close)
	kek, err := localsecrets.NewRandomKey()
	if err != nil {
		t.Fatal(err)
	}
	keys := secrets.New(localsecrets.NewKeeper(kek), one)
	t.Cleanup(func() { _ = keys.Close() })
	srv := &api.Server{Store: one, Tenant: tenant, Schedules: &refreshes{}, DefaultDataPlane: "dp1", Authless: true, Secrets: keys, Logger: slog.New(slog.DiscardHandler)}
	h, err := srv.Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	waiting, cancel := context.WithTimeout(ctx, 15*time.Second)
	t.Cleanup(cancel)
	return waiting, ts.URL
}

func TestWorkspaceGrantsNeedOneConnection(t *testing.T) {
	ctx, url := oneConnection(t)
	access := apiv1connect.NewAccessServiceClient(http.DefaultClient, url)
	p, err := access.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: "reader", Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT, DisplayName: "reader"},
	}))
	if err != nil {
		t.Fatal(err)
	}
	name := p.Msg.GetPrincipal().GetName()
	binding := &apiv1.RoleBinding{Role: apiv1.Role_ROLE_VIEWER, Workspace: "workspaces/default"}
	if _, err := access.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: name, Binding: binding})); err != nil {
		t.Fatalf("granting: %v", err)
	}
	if _, err := access.RevokeRole(ctx, connect.NewRequest(&apiv1.RevokeRoleRequest{Name: name, Binding: binding})); err != nil {
		t.Fatalf("revoking: %v", err)
	}
	missing := &apiv1.RoleBinding{Role: apiv1.Role_ROLE_VIEWER, Workspace: "workspaces/nope"}
	_, err = access.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: name, Binding: missing}))
	if connect.CodeOf(err) != connect.CodeNotFound || !strings.Contains(err.Error(), `workspace "nope" doesn't exist`) {
		t.Errorf("granting in a workspace that doesn't exist: %v", err)
	}
}

func TestSettingTheFirstSecretNeedsOneConnection(t *testing.T) {
	ctx, url := oneConnection(t)
	c := apiv1connect.NewSecretServiceClient(http.DefaultClient, url)
	res, err := c.UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/token", Value: "hunter2"}, AllowMissing: true,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if !res.Msg.GetCreated() {
		t.Errorf("response %v, want it created", res.Msg)
	}
}

func TestApplyNeedsOneConnection(t *testing.T) {
	ctx, url := oneConnection(t)
	applier := apiv1connect.NewApplyServiceClient(http.DefaultClient, url)
	if _, err := applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Source
metadata: { name: gen }
spec: { connector: faker, config: { seed: 1, count: 10 } }
`)}, {Content: []byte(`
apiVersion: openetl.io/v1alpha1
kind: Destination
metadata: { name: files }
spec: { connector: parquet, config: { url: "file:///tmp/openetl-pool-test" } }
`)}}})); err != nil {
		t.Fatal(err)
	}
}
