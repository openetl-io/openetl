package activities

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"net/url"
	"slices"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/flink"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type teardownAPI struct {
	dataplanev1connect.DataPlaneServiceClient
	resp *dataplanev1.GetFlinkTeardownResponse
}

func (a teardownAPI) GetFlinkTeardown(_ context.Context, req *connect.Request[dataplanev1.GetFlinkTeardownRequest]) (*connect.Response[dataplanev1.GetFlinkTeardownResponse], error) {
	if req.Msg.GetOperationId() != "op" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("no such teardown"))
	}
	return connect.NewResponse(a.resp), nil
}

// deployments are FlinkDeployments, each gone once deleted.
type deployments struct {
	mu      sync.Mutex
	objs    map[string]string
	opened  []byte
	deleted []string
}

func (k *deployments) Create(context.Context, map[string]any) error { return errors.ErrUnsupported }

func (k *deployments) Get(_ context.Context, name string) (map[string]any, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.objs[name]; !ok {
		return nil, nil
	}
	return map[string]any{"metadata": map[string]any{"name": name}}, nil
}

func (k *deployments) List(_ context.Context, selector string) ([]map[string]any, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	var out []map[string]any
	for name, pipeline := range k.objs {
		if selector == "openetl.io/pipeline="+pipeline {
			out = append(out, map[string]any{"metadata": map[string]any{"name": name}})
		}
	}
	return out, nil
}

func (k *deployments) Delete(_ context.Context, name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	delete(k.objs, name)
	k.deleted = append(k.deleted, name)
	return nil
}

func (k *deployments) Proxy(string, int) (*url.URL, *http.Client, error) {
	return nil, nil, errors.ErrUnsupported
}

// A deleted pipeline's teardown deletes its FlinkDeployments, and no other
// pipeline's, with the kubeconfig its cluster's version pinned.
func TestTeardownFlink(t *testing.T) {
	kube := &deployments{objs: map[string]string{"openetl-a": "p1", "openetl-b": "p1", "openetl-c": "p2"}}
	cluster := &dataplanev1.PinnedFlinkCluster{Version: "v1", Spec: &apiv1.FlinkClusterSpec{Target: &apiv1.FlinkClusterSpec_Kubernetes{
		Kubernetes: &apiv1.FlinkKubernetes{Namespace: "flink", Kubeconfig: structpb.NewStringValue("kubeconfig")},
	}}}
	a := &Ops{
		API:     teardownAPI{resp: &dataplanev1.GetFlinkTeardownResponse{OrgId: "o", PipelineId: "p1", Cluster: cluster}},
		Storage: t.TempDir(),
		Flink: flink.Options{Poll: time.Millisecond, Kube: func(_ context.Context, kubeconfig []byte, _ string) (flink.Kube, error) {
			kube.opened = kubeconfig
			return kube, nil
		}},
	}
	if _, err := a.TeardownFlink(t.Context(), contract.ConnectorOpInput{OpID: "op", OrgID: "o", PipelineID: "p1"}); err != nil {
		t.Fatal(err)
	}
	if string(kube.opened) != "kubeconfig" {
		t.Errorf("opened Kubernetes with %q", kube.opened)
	}
	slices.Sort(kube.deleted)
	if !slices.Equal(kube.deleted, []string{"openetl-a", "openetl-b"}) || !slices.Equal(slices.Collect(maps.Keys(kube.objs)), []string{"openetl-c"}) {
		t.Errorf("deleted %v, left %v", kube.deleted, kube.objs)
	}

	for _, in := range []contract.ConnectorOpInput{
		{OpID: "op", OrgID: "o", PipelineID: "p2"},
		{OpID: "op", OrgID: "other", PipelineID: "p1"},
	} {
		_, err := a.TeardownFlink(t.Context(), in)
		if errs.CodeOf(err) != errs.ConfigInvalid {
			t.Errorf("%+v: %v, want CONFIG_INVALID", in, err)
		}
	}
	if len(kube.objs) != 1 {
		t.Errorf("left %v", kube.objs)
	}
	a.Storage = ""
	if _, err := a.TeardownFlink(t.Context(), contract.ConnectorOpInput{OpID: "op", OrgID: "o", PipelineID: "p1"}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("without a store: %v, want CONFIG_INVALID", err)
	}
}

// A connector's items come out filled in, and a failed item fails the
// result even when the answer had no error.
func TestCheckResult(t *testing.T) {
	r := check.FromProto(&protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{
		{Name: "connect", Title: "Reach {host}", Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED, Message: "PostgreSQL {version}", Params: map[string]string{"host": "db", "version": "17"}},
		{Name: "schema", Title: "Use {schema}", Status: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Message: "{user} has no USAGE on {schema}.", Field: "schema",
			Params: map[string]string{"schema": "public", "user": "etl"}, Fix: "GRANT USAGE ON SCHEMA {schema} TO {user}", Reason: "privilege_missing", Detail: "pq: denied",
			Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Message: "etl has no USAGE on public."}},
		{Name: "lag", Title: "Replicate", Status: 9},
	}})
	items, code, message := checkResult(r)
	want := []contract.CheckItem{
		{Name: "connect", Title: "Reach db", Status: "passed", Message: "PostgreSQL 17"},
		{Name: "schema", Title: "Use public", Status: "failed", Message: "etl has no USAGE on public: pq: denied", Code: "PERMISSION_DENIED", Fix: "GRANT USAGE ON SCHEMA public TO etl", Field: "schema"},
		{Name: "lag", Title: "Replicate", Status: "failed", Message: "The check failed: the connector gave the item the status 9", Code: "INTERNAL"},
	}
	if !slices.Equal(items, want) {
		t.Errorf("items\n %+v\nwant\n %+v", items, want)
	}
	if code != "PERMISSION_DENIED" || message != "Use public: etl has no USAGE on public: pq: denied" {
		t.Errorf("the result's %s: %s", code, message)
	}
	if items, code, message := checkResult(check.FromError(errs.New(errs.AuthInvalid, "bad password"))); len(items) != 0 || code != "AUTH_INVALID" || message != "bad password" {
		t.Errorf("an error alone: %v, %s: %s", items, code, message)
	}
	if items, code, message := checkResult(check.Report{}); len(items) != 0 || code != "" || message != "" {
		t.Errorf("an empty report: %v, %s: %s", items, code, message)
	}
}

type remote struct{ checked, listed bool }

func (e *remote) CheckReport(context.Context) check.Report {
	e.checked = true
	return check.FromError(errors.New("external"))
}

func (e *remote) Checks() []check.Check {
	e.listed = true
	return nil
}

type builtIn struct{}

func (builtIn) Checks() []check.Check {
	return []check.Check{{Name: "connect", Title: "Connect", Run: func(context.Context, *check.Probe) error { return errs.New(errs.Transient, "refused") }}}
}

type legacy struct{}

func (legacy) Check(context.Context) error { return errs.New(errs.AuthInvalid, "bad password") }

// An external connector's report is its own; a built-in one's checks run
// here, or its Check.
func TestRunCheck(t *testing.T) {
	e := &remote{}
	if r := runCheck(t.Context(), e); !e.checked || e.listed || r.Err() == nil || r.Err().Error() != "external" {
		t.Errorf("an external connector: %+v, %v", e, r.Err())
	}
	if r := runCheck(t.Context(), builtIn{}); len(r.Items) != 1 || r.Items[0].Status != check.Failed || r.OK() {
		t.Errorf("a built-in connector's checks: %+v", r.Items)
	}
	if r := runCheck(t.Context(), legacy{}); len(r.Items) != 0 || errs.CodeOf(r.Err()) != errs.AuthInvalid {
		t.Errorf("a built-in connector's Check: %v", r.Err())
	}
	if r := runCheck(t.Context(), struct{}{}); !r.OK() || len(r.Items) != 0 {
		t.Errorf("a connector without a check: %v", r.Err())
	}
}
