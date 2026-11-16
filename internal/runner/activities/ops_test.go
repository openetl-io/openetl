package activities

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/flink"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/host"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
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

// A connector's items come out as it sent them, templates unfilled, with
// every field; a failed item fails the result even when the answer had no
// error.
func TestCheckResult(t *testing.T) {
	r := check.FromProto(&protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{
		{Name: "connect", Title: "Reach {host}", Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED, Message: "PostgreSQL {version}", Kind: protocolv1.CheckKind_CHECK_KIND_CONNECTION,
			Description: "Open a session to {host}", Params: map[string]string{"host": "db", "version": "17"}, Duration: durationpb.New(40 * time.Millisecond)},
		{Name: "schema", Title: "Use {schema}", Status: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Message: "{user} has no USAGE on {schema}.", Field: "schema",
			Kind: protocolv1.CheckKind_CHECK_KIND_PERMISSION, Params: map[string]string{"schema": "public", "user": "etl"}, Fix: "GRANT USAGE ON SCHEMA {schema} TO {user}",
			Reason: "privilege_missing", Detail: "pq: denied", Duration: durationpb.New(3 * time.Second),
			Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Message: "etl has no USAGE on public."}},
		{Name: "lag", Title: "Replicate", Status: protocolv1.CheckStatus_CHECK_STATUS_WARNING, Message: "{lag} behind", Params: map[string]string{"lag": "5m"}, Reason: "setting_mismatch",
			Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_TRANSIENT}},
		{Name: "odd", Title: "Odd", Status: 9},
	}})
	items, code, message := checkResult(r)
	want := []contract.CheckItem{
		{Name: "connect", Title: "Reach {host}", Status: "passed", Message: "PostgreSQL {version}", Kind: "connection", Description: "Open a session to {host}",
			Params: map[string]string{"host": "db", "version": "17"}, Duration: 40 * time.Millisecond},
		{Name: "schema", Title: "Use {schema}", Status: "failed", Message: "{user} has no USAGE on {schema}.", Code: "PERMISSION_DENIED", Fix: "GRANT USAGE ON SCHEMA {schema} TO {user}",
			Field: "schema", Kind: "permission", Reason: "privilege_missing", Params: map[string]string{"schema": "public", "user": "etl"}, Detail: "pq: denied", Duration: 3 * time.Second},
		{Name: "lag", Title: "Replicate", Status: "warning", Message: "{lag} behind", Code: "TRANSIENT", Kind: "custom", Reason: "setting_mismatch", Params: map[string]string{"lag": "5m"}},
		{Name: "odd", Title: "Odd", Status: "failed", Message: "The check failed.", Code: "INTERNAL", Kind: "custom", Reason: "unknown", Detail: "the connector gave the item the status 9"},
	}
	if !reflect.DeepEqual(items, want) {
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

func (e *remote) CheckReport(context.Context) (check.Report, error) {
	e.checked = true
	return check.FromError(errors.New("external")), nil
}

func (e *remote) Checks() []check.Check {
	e.listed = true
	return nil
}

type builtIn struct{}

func (builtIn) Checks() []check.Check {
	return []check.Check{{Name: "connect", Title: "Connect", Run: func(context.Context, *check.Probe) error {
		return fmt.Errorf("parquet: %w", errs.New(errs.AuthInvalid, "the key was rejected"))
	}}}
}

type legacy struct{}

func (legacy) Check(context.Context) error { return errs.New(errs.AuthInvalid, "bad password") }

// An external connector's report is its own; a built-in one's checks run
// here, with the codes of their errors, or its Check.
func TestRunCheck(t *testing.T) {
	e := &remote{}
	if r, err := runCheck(t.Context(), e); err != nil || !e.checked || e.listed || r.Err() == nil || r.Err().Error() != "external" {
		t.Errorf("an external connector: %+v, %v, %v", e, r.Err(), err)
	}
	r, _ := runCheck(t.Context(), builtIn{})
	if items, code, _ := checkResult(r); len(items) != 1 || items[0].Status != "failed" || items[0].Code != "AUTH_INVALID" || code != "AUTH_INVALID" {
		t.Errorf("a built-in connector's checks: %+v, %s", items, code)
	}
	if r, _ := runCheck(t.Context(), legacy{}); len(r.Items) != 0 || errs.CodeOf(r.Err()) != errs.AuthInvalid {
		t.Errorf("a built-in connector's Check: %v", r.Err())
	}
	if r, _ := runCheck(t.Context(), struct{}{}); !r.OK() || len(r.Items) != 0 {
		t.Errorf("a connector without a check: %v", r.Err())
	}
}

// opsAPI is the control plane as a connector op sees it.
type opsAPI struct {
	dataplanev1connect.DataPlaneServiceClient
	spec *dataplanev1.GetSyncSpecResponse
	cfg  *dataplanev1.GetConnectorConfigResponse
}

func (a opsAPI) GetSyncSpec(context.Context, *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	return connect.NewResponse(a.spec), nil
}

func (a opsAPI) GetConnectorConfig(context.Context, *connect.Request[dataplanev1.GetConnectorConfigRequest]) (*connect.Response[dataplanev1.GetConnectorConfigResponse], error) {
	return connect.NewResponse(a.cfg), nil
}

// testConnector makes this test binary the connector name in dir, serving
// as TestMain does for process.
func testConnector(t *testing.T, dir, name, process string) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	script := fmt.Sprintf("#!/bin/sh\n%s=%s exec %q -test.run='^$'\n", processEnv, process, exe)
	if err := os.WriteFile(filepath.Join(dir, name), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
}

// lockedSource's account is locked: its check fails, and Discover, which
// writes the file at marker, isn't reached by a preflight.
var lockedSource = sdk.Connector{
	Name:         "locked",
	ConfigSchema: []byte(`{"type": "object", "properties": {"marker": {"type": "string"}}}`),
	Source: func(config []byte, _ host.Host) (source.Source, error) {
		var c struct{ Marker string }
		if err := json.Unmarshal(config, &c); err != nil {
			return nil, err
		}
		return locked{c.Marker}, nil
	},
}

var accountLocked = check.Reason{Name: "account_locked", Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, Message: "The account is locked.", Fix: "Unlock the account."}

type locked struct{ marker string }

func (locked) Checks() []check.Check {
	return []check.Check{{Name: "login", Kind: check.Authentication, Title: "Sign in", Run: func(context.Context, *check.Probe) error { return accountLocked.Fail() }}}
}

func (l locked) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{Streams: []source.CatalogStream{{Name: "s", SyncModes: []source.SyncMode{source.FullRefresh}}}}, os.WriteFile(l.marker, nil, 0o600)
}

func (locked) Read(context.Context, source.ReadRequest, source.Emitter) error { return nil }

// dyingSource's process exits when it's checked.
var dyingSource = sdk.Connector{Name: "dies", Source: func([]byte, host.Host) (source.Source, error) { return dies{}, nil }}

type dies struct{}

func (dies) Check(context.Context) error {
	os.Exit(3)
	return nil
}

func (dies) Discover(context.Context) (source.Catalog, error) { return source.Catalog{}, nil }

func (dies) Read(context.Context, source.ReadRequest, source.Emitter) error { return nil }

// chattySource's Discover fails quoting its password, among control
// characters.
var chattySource = sdk.Connector{
	Name:         "chatty",
	ConfigSchema: []byte(`{"type": "object", "properties": {"password": {"type": "string", "x-secret": true}}}`),
	Egress:       sdk.Egress{Tier: sdk.EgressRaw},
	Source: func(config []byte, _ host.Host) (source.Source, error) {
		var c struct{ Password string }
		if err := json.Unmarshal(config, &c); err != nil {
			return nil, err
		}
		return chatty(c.Password), nil
	},
}

type chatty string

func (c chatty) Discover(context.Context) (source.Catalog, error) {
	return source.Catalog{}, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, fmt.Errorf("dial postgres://u:%s@db\x1b[2J\u202e: refused", string(c)))
}

func (chatty) Read(context.Context, source.ReadRequest, source.Emitter) error { return nil }

// A check the connector answers is a result, failed or not; a connector
// that doesn't answer is a failure of the op, which isn't retried.
func TestCheckConnector(t *testing.T) {
	dir := t.TempDir()
	testConnector(t, dir, "source-locked", "locked")
	testConnector(t, dir, "source-dies", "dies")
	cfg, err := structpb.NewStruct(map[string]any{"marker": filepath.Join(t.TempDir(), "discovered")})
	if err != nil {
		t.Fatal(err)
	}
	api := opsAPI{cfg: &dataplanev1.GetConnectorConfigResponse{OrgId: "org", Version: "v1", Name: "db", Config: &apiv1.ConnectorConfig{Connector: "locked", Config: cfg}}}
	a := &Ops{API: api, Connectors: connectorhost.Env{Connectors: dir}}
	in := contract.ConnectorOpInput{OpID: "op", OrgID: "org", SourceID: "src"}
	res, err := a.CheckConnector(t.Context(), in)
	if err != nil || res.Version != "v1" || res.Code != "AUTH_INVALID" || len(res.Checks) != 1 || res.Checks[0].Fix != "Unlock the account." {
		t.Errorf("a check that fails: %+v, %v", res, err)
	}

	api.cfg.Config.Connector = "dies"
	_, err = a.CheckConnector(t.Context(), in)
	if f, ok := errors.AsType[*contract.Failure](err); !ok || f.Code != "INTERNAL" || !strings.Contains(f.Message, "exited unexpectedly") {
		t.Errorf("a connector that dies: %#v", err)
	}

	api.cfg.Config.Connector = "missing"
	if res, err := a.CheckConnector(t.Context(), in); err != nil || res.Code == "" || len(res.Checks) != 0 {
		t.Errorf("a connector that can't be bound: %+v, %v", res, err)
	}
}

// A result too large for the orchestrator, as it travels, fails the op and
// isn't retried; one under the limit is kept as it is.
func TestFitResult(t *testing.T) {
	under := contract.ConnectorOpResult{Preflight: []contract.CheckGroup{{Role: roleSource, Stage: stageCheck,
		Items: []contract.CheckItem{{Name: "a", Title: "A", Status: "passed", Detail: strings.Repeat("x", 1_500_000)}}}}}
	if res, err := fitResult("preflight", under); err != nil || !reflect.DeepEqual(res, under) {
		t.Errorf("under the limit: %v", err)
	}
	// 300,000 bytes that encoding/json writes as 1,800,000.
	over := contract.ConnectorOpResult{Checks: []contract.CheckItem{{Name: "a", Title: "A", Status: "failed", Detail: strings.Repeat("<", 300_000)}}}
	b, err := json.Marshal(over)
	if err != nil {
		t.Fatal(err)
	}
	want := fmt.Sprintf("the result of the connector check is %d bytes, and a result has at most 1572864: the connectors answered with too much", len(b))
	if _, err := fitResult("connector check", over); err == nil {
		t.Error("over the limit: no error")
	} else if f, ok := errors.AsType[*contract.Failure](err); !ok || f.Code != "INTERNAL" || f.Message != want {
		t.Errorf("over the limit: %#v", err)
	}
}
