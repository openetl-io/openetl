//go:build e2e

package e2e

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
)

// Keys act with their principal's roles, runner tokens serve only their own
// data plane's work, and both stop working as soon as they're deleted.
func TestAccess(t *testing.T) {
	ctx := t.Context()
	owner := apiv1connect.NewAccessServiceClient(env.http, env.apiURL)
	list := func(c *http.Client) error {
		_, err := apiv1connect.NewPipelineServiceClient(c, env.apiURL).ListPipelines(ctx, connect.NewRequest(&apiv1.ListPipelinesRequest{Parent: "workspaces/default"}))
		return err
	}
	wantCode(t, list(http.DefaultClient), connect.CodeUnauthenticated, "a call without a key")
	wantCode(t, list(withKey("oetl_unknown")), connect.CodeUnauthenticated, "an unknown key")
	wantCode(t, list(env.runner), connect.CodeUnauthenticated, "a runner token on the API")
	_, err := dataplanev1connect.NewDataPlaneServiceClient(env.http, env.apiURL).GetArtifact(ctx, connect.NewRequest(&dataplanev1.GetArtifactRequest{Digest: "sha256:00"}))
	wantCode(t, err, connect.CodeUnauthenticated, "an API key on DataPlaneService")

	name := unique("ci")
	principal := "principals/" + name
	if _, err := owner.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: name, Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT},
	})); err != nil {
		t.Fatal(err)
	}
	grant := func(c apiv1connect.AccessServiceClient, who string, role apiv1.Role, workspace string) error {
		_, err := c.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: who, Binding: &apiv1.RoleBinding{Role: role, Workspace: workspace}}))
		return err
	}
	k, err := owner.CreateKey(ctx, connect.NewRequest(&apiv1.CreateKeyRequest{Parent: principal}))
	if err != nil {
		t.Fatal(err)
	}
	as := withKey(k.Msg.GetSecret())
	access := apiv1connect.NewAccessServiceClient(as, env.apiURL)
	wantCode(t, list(as), connect.CodePermissionDenied, "a principal without roles")

	if err := grant(owner, principal, apiv1.Role_ROLE_VIEWER, "workspaces/default"); err != nil {
		t.Fatal(err)
	}
	if err := list(as); err != nil {
		t.Errorf("a viewer lists pipelines: %v", err)
	}
	_, err = access.ListPrincipals(ctx, connect.NewRequest(&apiv1.ListPrincipalsRequest{}))
	wantCode(t, err, connect.CodePermissionDenied, "a workspace's viewer reads the org's principals")
	applyAs := func(docs ...string) error {
		req := &apiv1.ApplyRequest{}
		for _, d := range docs {
			req.Documents = append(req.Documents, &apiv1.Document{Content: []byte(d)})
		}
		_, err := apiv1connect.NewApplyServiceClient(as, env.apiURL).Apply(ctx, connect.NewRequest(req))
		return err
	}
	docs := pipelineDocs(name, `{"seed": 7, "count": 20}`, `{}`)
	wantCode(t, applyAs(docs...), connect.CodePermissionDenied, "a viewer applies")

	if err := grant(owner, principal, apiv1.Role_ROLE_EDITOR, "workspaces/default"); err != nil {
		t.Fatal(err)
	}
	if err := applyAs(docs...); err != nil {
		t.Fatalf("an editor applies: %v", err)
	}
	if _, err := apiv1connect.NewPipelineServiceClient(as, env.apiURL).PausePipeline(ctx, connect.NewRequest(&apiv1.PausePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
		t.Errorf("an editor pauses: %v", err)
	}
	wantCode(t, applyAs(append(docs, manifest(name, "1.0.0", "/v1/contacts"))...), connect.CodePermissionDenied, "an editor applies a connector version")
	_, err = access.CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/default"}))
	wantCode(t, err, connect.CodePermissionDenied, "an editor makes a runner token")

	if err := grant(owner, principal, apiv1.Role_ROLE_ADMIN, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := access.ListPrincipals(ctx, connect.NewRequest(&apiv1.ListPrincipalsRequest{})); err != nil {
		t.Errorf("an admin lists principals: %v", err)
	}
	wantCode(t, grant(access, principal, apiv1.Role_ROLE_OWNER, ""), connect.CodePermissionDenied, "an admin makes itself an owner")
	_, err = access.CreateKey(ctx, connect.NewRequest(&apiv1.CreateKeyRequest{Parent: "principals/admin"}))
	wantCode(t, err, connect.CodePermissionDenied, "an admin makes an owner's key")
	_, err = access.DisablePrincipal(ctx, connect.NewRequest(&apiv1.DisablePrincipalRequest{Name: "principals/admin"}))
	wantCode(t, err, connect.CodePermissionDenied, "an admin disables an owner")
	wantCode(t, grant(owner, principal, apiv1.Role_ROLE_ADMIN, "workspaces/default"), connect.CodeInvalidArgument, "admin in a workspace")

	_, err = owner.RevokeRole(ctx, connect.NewRequest(&apiv1.RevokeRoleRequest{Name: "principals/admin", Binding: &apiv1.RoleBinding{Role: apiv1.Role_ROLE_OWNER}}))
	wantCode(t, err, connect.CodeFailedPrecondition, "revoking the last owner")
	_, err = owner.DisablePrincipal(ctx, connect.NewRequest(&apiv1.DisablePrincipalRequest{Name: "principals/admin"}))
	wantCode(t, err, connect.CodeFailedPrecondition, "disabling the last owner")

	if _, err := owner.DisablePrincipal(ctx, connect.NewRequest(&apiv1.DisablePrincipalRequest{Name: principal})); err != nil {
		t.Fatal(err)
	}
	wantCode(t, list(as), connect.CodeUnauthenticated, "a disabled principal's key")
	if _, err := owner.EnablePrincipal(ctx, connect.NewRequest(&apiv1.EnablePrincipalRequest{Name: principal})); err != nil {
		t.Fatal(err)
	}
	if err := list(as); err != nil {
		t.Errorf("after enabling: %v", err)
	}
	keys, err := owner.ListKeys(ctx, connect.NewRequest(&apiv1.ListKeysRequest{Parent: principal}))
	if err != nil || len(keys.Msg.GetKeys()) != 1 || keys.Msg.GetKeys()[0].GetLastUsedTime() == nil || keys.Msg.GetKeys()[0].GetPrefix() != k.Msg.GetSecret()[:12] {
		t.Fatalf("keys %v, %v", keys, err)
	}
	if _, err := owner.DeleteKey(ctx, connect.NewRequest(&apiv1.DeleteKeyRequest{Name: k.Msg.GetKey().GetName()})); err != nil {
		t.Fatal(err)
	}
	wantCode(t, list(as), connect.CodeUnauthenticated, "a deleted key")

	// Runner tokens.
	other := unique("other")
	pgExec(t, "INSERT INTO data_planes (id, org_id) SELECT '"+other+"', id FROM orgs")
	tok, err := owner.CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/" + other}))
	if err != nil {
		t.Fatal(err)
	}
	elsewhere := dataplanev1connect.NewDataPlaneServiceClient(withKey(tok.Msg.GetSecret()), env.apiURL)
	here := dataplanev1connect.NewDataPlaneServiceClient(env.runner, env.apiURL)
	id, err := query[string](t, `SELECT id::text FROM pipelines WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	version, err := query[string](t, `SELECT version FROM pipelines WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	spec := connect.NewRequest(&dataplanev1.GetSyncSpecRequest{PipelineId: id, Version: version})
	if _, err := here.GetSyncSpec(ctx, spec); err != nil {
		t.Errorf("the pipeline's data plane gets its spec: %v", err)
	}
	_, err = elsewhere.GetSyncSpec(ctx, spec)
	wantCode(t, err, connect.CodePermissionDenied, "another data plane's sync spec")
	_, err = elsewhere.ReportSchemas(ctx, connect.NewRequest(&dataplanev1.ReportSchemasRequest{PipelineId: id}))
	wantCode(t, err, connect.CodePermissionDenied, "reporting another data plane's schemas")
	_, err = elsewhere.ReportCheckpoint(ctx, connect.NewRequest(&dataplanev1.ReportCheckpointRequest{PipelineId: id}))
	wantCode(t, err, connect.CodePermissionDenied, "reporting another data plane's checkpoint")
	src, err := query[string](t, `SELECT id::text FROM sources WHERE name = $1`, name)
	if err != nil {
		t.Fatal(err)
	}
	cfg := connect.NewRequest(&dataplanev1.GetConnectorConfigRequest{
		Connector: &dataplanev1.GetConnectorConfigRequest_SourceId{SourceId: src}, OperationId: uuid.NewString(),
	})
	_, err = here.GetConnectorConfig(ctx, cfg)
	wantCode(t, err, connect.CodePermissionDenied, "a source's config without a check or discover")
	if _, err := owner.DeleteRunnerToken(ctx, connect.NewRequest(&apiv1.DeleteRunnerTokenRequest{Name: tok.Msg.GetToken().GetName()})); err != nil {
		t.Fatal(err)
	}
	_, err = elsewhere.GetArtifact(ctx, connect.NewRequest(&dataplanev1.GetArtifactRequest{Digest: "sha256:00"}))
	wantCode(t, err, connect.CodeUnauthenticated, "a deleted runner token")
}
