package api

import (
	"context"
	"fmt"
	"testing"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	_ "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/auth"
)

// TestEveryRPCHasARule fails on an RPC the interceptor would refuse, and on a
// rule whose scope can't read its request.
func TestEveryRPCHasARule(t *testing.T) {
	served := map[string]bool{}
	for _, pkg := range []protoreflect.FullName{"openetl.api.v1", "openetl.dataplane.v1"} {
		protoregistry.GlobalFiles.RangeFilesByPackage(pkg, func(f protoreflect.FileDescriptor) bool {
			for i := range f.Services().Len() {
				svc := f.Services().Get(i)
				for j := range svc.Methods().Len() {
					m := svc.Methods().Get(j)
					proc := fmt.Sprintf("/%s/%s", svc.FullName(), m.Name())
					served[proc] = true
					r, ok := rules[proc]
					if !ok {
						t.Errorf("%s has no rule", proc)
						continue
					}
					if r.runner != (pkg == "openetl.dataplane.v1") {
						t.Errorf("%s: only DataPlaneService takes runner tokens", proc)
					}
					if !r.runner {
						if _, ok := permissions[r.permission]; !ok {
							t.Errorf("%s: no permission %q", proc, r.permission)
						}
					}
					if r.scope == nil {
						continue
					}
					mt, err := protoregistry.GlobalTypes.FindMessageByName(m.Input().FullName())
					if err != nil {
						t.Fatal(err)
					}
					if ws, err := r.scope(mt.New().Interface()); err == nil && ws != "" {
						t.Errorf("%s: an empty request names workspace %q", proc, ws)
					}
				}
			}
			return true
		})
	}
	for proc := range rules {
		if !served[proc] {
			t.Errorf("rule for %s, which isn't an RPC", proc)
		}
	}
}

func TestScopes(t *testing.T) {
	cases := []struct {
		r    rule
		req  any
		want string
	}{
		{rules["/openetl.api.v1.PipelineService/SyncPipeline"], &apiv1.SyncPipelineRequest{Name: "workspaces/sales/pipelines/p"}, "sales"},
		{rules["/openetl.api.v1.SourceService/ListSources"], &apiv1.ListSourcesRequest{Parent: "workspaces/ops"}, "ops"},
		{rules["/openetl.api.v1.SecretService/UpdateSecret"], &apiv1.UpdateSecretRequest{Secret: &apiv1.Secret{Name: "workspaces/w/secrets/s"}}, "w"},
		{rules["/openetl.api.v1.AccessService/GrantRole"], &apiv1.GrantRoleRequest{Name: "principals/p", Binding: &apiv1.RoleBinding{Workspace: "workspaces/w"}}, ""},
	}
	for _, c := range cases {
		if got, err := c.r.scope(c.req); err != nil || got != c.want {
			t.Errorf("%T: %q, %v; want %q", c.req, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "workspaces/", "workspaces//pipelines/p", "principals/p", "pipelines/p"} {
		if _, err := workspaceOf(bad); err == nil {
			t.Errorf("%q names a workspace", bad)
		}
	}
}

func TestAuthorize(t *testing.T) {
	ctx := context.WithValue(context.Background(), callerKey{}, &caller{
		principal: "principals/p",
		roles:     auth.ParseBindings([]string{"viewer", "operator/ops", "editor/sales"}),
	})
	cases := []struct {
		permission, workspace string
		ok                    bool
	}{
		{"pipelines.get", "any", true},
		{"pipelines.get", "", true},
		{"pipelines.sync", "any", false},
		{"pipelines.sync", "ops", true},
		{"pipelines.delete", "ops", false},
		{"documents.apply", "sales", true},
		{"connectors.apply", "", false},
		{"principals.get", "", true},
		{"principals.create", "", false},
		{"owners.manage", "", false},
	}
	for _, c := range cases {
		err := authorize(ctx, c.permission, c.workspace)
		if (err == nil) != c.ok {
			t.Errorf("%s in %q: %v", c.permission, c.workspace, err)
		}
		if err != nil && connect.CodeOf(err) != connect.CodePermissionDenied {
			t.Errorf("%s: code %v", c.permission, connect.CodeOf(err))
		}
	}
	runner := context.WithValue(context.Background(), callerKey{}, &caller{dataPlane: "default", roles: auth.ParseBindings([]string{"owner"})})
	if err := authorize(runner, "pipelines.get", ""); err == nil {
		t.Error("a runner token holds no role")
	}
	if err := authorize(context.Background(), "pipelines.get", ""); err == nil {
		t.Error("no caller, and authorized")
	}
	if err := authorize(ctx, "no.such", ""); err == nil || connect.CodeOf(err) != connect.CodeUnknown {
		t.Errorf("an unknown permission: %v", err)
	}
}

func TestOnDataPlane(t *testing.T) {
	ctx := context.WithValue(context.Background(), callerKey{}, &caller{dataPlane: "eu"})
	if err := onDataPlane(ctx, "eu", "p"); err != nil {
		t.Error(err)
	}
	if err := onDataPlane(ctx, "us", "p"); connect.CodeOf(err) != connect.CodePermissionDenied {
		t.Errorf("another data plane's work: %v", err)
	}
	key := context.WithValue(context.Background(), callerKey{}, &caller{principal: "principals/p"})
	if err := onDataPlane(key, "", "p"); err == nil {
		t.Error("an API key is no data plane's")
	}
}
