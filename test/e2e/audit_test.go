//go:build e2e

package e2e

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
)

// The audit log has every call that changes something or reads secrets,
// refused ones too, with who made it; never a read, or a secret's value.
func TestAudit(t *testing.T) {
	startRunner(t)
	ctx := t.Context()
	log := apiv1connect.NewAuditServiceClient(env.http, env.apiURL)
	list := func(req *apiv1.ListAuditEntriesRequest) []*apiv1.AuditEntry {
		t.Helper()
		res, err := log.ListAuditEntries(ctx, connect.NewRequest(req))
		if err != nil {
			t.Fatal(err)
		}
		return res.Msg.GetEntries()
	}
	start := time.Now()

	// A viewer's refused apply, and its reads.
	name := unique("audit")
	viewer := "principals/" + name
	access := apiv1connect.NewAccessServiceClient(env.http, env.apiURL)
	if _, err := access.CreatePrincipal(ctx, connect.NewRequest(&apiv1.CreatePrincipalRequest{
		PrincipalId: name, Principal: &apiv1.Principal{Type: apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT},
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := access.GrantRole(ctx, connect.NewRequest(&apiv1.GrantRoleRequest{Name: viewer, Binding: &apiv1.RoleBinding{Role: apiv1.Role_ROLE_VIEWER, Workspace: "workspaces/default"}})); err != nil {
		t.Fatal(err)
	}
	k, err := access.CreateKey(ctx, connect.NewRequest(&apiv1.CreateKeyRequest{Parent: viewer}))
	if err != nil {
		t.Fatal(err)
	}
	as := withKey(k.Msg.GetSecret())
	dsn := setSecret(t, env.dbURL)
	secret := "workspaces/default/secrets/" + dsn
	docs := pipelineDocs(name, `{"seed": 3, "count": 10}`, `{}`)
	docs[1] = strings.Replace(docs[1], `{"secret": "e2e-dsn"}`, `{"secret": `+strconv.Quote(dsn)+`}`, 1)
	req := &apiv1.ApplyRequest{}
	for _, d := range docs {
		req.Documents = append(req.Documents, &apiv1.Document{Content: []byte(d)})
	}
	_, err = apiv1connect.NewApplyServiceClient(as, env.apiURL).Apply(ctx, connect.NewRequest(req))
	wantCode(t, err, connect.CodePermissionDenied, "a viewer applies")
	if _, err := apiv1connect.NewPipelineServiceClient(as, env.apiURL).ListPipelines(ctx, connect.NewRequest(&apiv1.ListPipelinesRequest{Parent: "workspaces/default"})); err != nil {
		t.Fatal(err)
	}
	_, err = apiv1connect.NewAuditServiceClient(as, env.apiURL).ListAuditEntries(ctx, connect.NewRequest(&apiv1.ListAuditEntriesRequest{}))
	wantCode(t, err, connect.CodePermissionDenied, "a viewer reads the audit log")
	got := list(&apiv1.ListAuditEntriesRequest{Actor: viewer})
	if len(got) != 1 {
		t.Fatalf("the viewer's entries: %v", got)
	}
	pipeline := "workspaces/default/pipelines/" + name
	if e := got[0]; e.GetMethod() != apiv1connect.ApplyServiceApplyProcedure || e.GetCode() != "permission_denied" ||
		e.GetCredential() != k.Msg.GetKey().GetName() || !slices.Contains(e.GetResources(), pipeline) || e.GetEndTime() == nil {
		t.Errorf("the refused apply: %v", e)
	}

	// The owner applies and checks; the runner reads the secret for the
	// sync, and for the check.
	apply(t, docs...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	destination := "workspaces/default/destinations/" + name
	if _, err := apiv1connect.NewDestinationServiceClient(env.http, env.apiURL).CheckDestination(ctx, connect.NewRequest(&apiv1.CheckDestinationRequest{Name: destination})); err != nil {
		t.Fatal(err)
	}
	// Newest first: the check's read, each attempt's spec, and setting it.
	got = list(&apiv1.ListAuditEntriesRequest{Resource: secret})
	methods := make([]string, len(got))
	for i, e := range got {
		methods[i] = e.GetMethod()
	}
	n := len(methods)
	if n < 3 || methods[0] != dataplanev1connect.DataPlaneServiceGetConnectorConfigProcedure || methods[n-1] != apiv1connect.SecretServiceUpdateSecretProcedure ||
		slices.ContainsFunc(methods[1:n-1], func(m string) bool { return m != dataplanev1connect.DataPlaneServiceGetSyncSpecProcedure }) {
		t.Fatalf("entries naming the secret: %v", methods)
	}
	read, synced := got[0], got[1]
	if synced.GetActor() != "dataPlanes/default" || !strings.HasPrefix(synced.GetCredential(), "dataPlanes/default/tokens/") || !slices.Contains(synced.GetResources(), pipeline) {
		t.Errorf("the sync's spec: %v", synced)
	}
	check := list(&apiv1.ListAuditEntriesRequest{Resource: destination, Method: "CheckDestination"})
	if len(check) != 1 || check[0].GetActor() != "principals/admin" || check[0].GetCode() != "ok" {
		t.Fatalf("the check: %v", check)
	}
	op := check[0].GetDetails().GetFields()["operation"].GetStringValue()
	if op == "" || read.GetDetails().GetFields()["operation"].GetStringValue() != op {
		t.Errorf("the check's operation %q; the runner's read %v", op, read)
	}
	applied := list(&apiv1.ListAuditEntriesRequest{Actor: "principals/admin", Method: apiv1connect.ApplyServiceApplyProcedure, StartTime: check[0].GetTime()})
	if len(applied) != 0 {
		t.Errorf("start_time didn't filter: %v", applied)
	}

	// No reads, and no secret's value, anywhere since the test began.
	r := &apiv1.ListAuditEntriesRequest{StartTime: timestamppb.New(start), PageSize: 100}
	for {
		res, err := log.ListAuditEntries(ctx, connect.NewRequest(r))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range res.Msg.GetEntries() {
			method := e.GetMethod()[strings.LastIndex(e.GetMethod(), "/")+1:]
			if strings.HasPrefix(method, "List") || strings.HasPrefix(method, "Report") || method == "GetArtifact" {
				t.Errorf("a read is audited: %v", e)
			}
			if b, _ := protojson.Marshal(e); strings.Contains(string(b), env.dbURL) || strings.Contains(string(b), k.Msg.GetSecret()) {
				t.Errorf("an entry holds a secret: %s", b)
			}
		}
		if r.PageToken = res.Msg.GetNextPageToken(); r.PageToken == "" {
			break
		}
	}
}
