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
