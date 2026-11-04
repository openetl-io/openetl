package flink

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/structpb"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
)

// fakeKube keeps FlinkDeployments in memory. A deleted one is gone after
// the next read, as the operator's finalizer takes a moment.
type fakeKube struct {
	mu       sync.Mutex
	objs     map[string]map[string]any
	deleting map[string]bool
	deleted  []string
	proxy    *httptest.Server
	opened   []byte
}

func (k *fakeKube) Create(_ context.Context, obj map[string]any) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	name, _, _ := unstructured.NestedString(obj, "metadata", "name")
	if _, ok := k.objs[name]; ok {
		return errs.New(errs.ConfigInvalid, "already exists")
	}
	k.objs[name] = obj
	return nil
}

func (k *fakeKube) Get(_ context.Context, name string) (map[string]any, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.deleting[name] {
		delete(k.objs, name)
		delete(k.deleting, name)
		return nil, nil
	}
	return k.objs[name], nil
}

func (k *fakeKube) List(_ context.Context, selector string) ([]map[string]any, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	label, value, _ := strings.Cut(selector, "=")
	var out []map[string]any
	for _, o := range k.objs {
		if v, _, _ := unstructured.NestedString(o, "metadata", "labels", label); v == value {
			out = append(out, o)
		}
	}
	return out, nil
}

func (k *fakeKube) Delete(_ context.Context, name string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if _, ok := k.objs[name]; ok {
		k.deleting[name] = true
		k.deleted = append(k.deleted, name)
	}
	return nil
}

func (k *fakeKube) Proxy(service string, _ int) (*url.URL, *http.Client, error) {
	u, _ := url.Parse(k.proxy.URL)
	return u.JoinPath("proxy", service), k.proxy.Client(), nil
}

func (k *fakeKube) setStatus(name string, status map[string]any) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.objs[name]["status"] = status
}

func newKubeCluster(t *testing.T) (*Cluster, *fakeKube) {
	t.Helper()
	k := &fakeKube{objs: map[string]map[string]any{}, deleting: map[string]bool{}}
	var stops []string
	k.proxy = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/stop"):
			stops = append(stops, r.URL.Path)
			_, _ = w.Write([]byte(`{"request-id": "t"}`))
		case strings.Contains(r.URL.Path, "/savepoints/"):
			_, _ = w.Write([]byte(`{"status": {"id": "COMPLETED"}, "operation": {"location": "x"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(k.proxy.Close)
	c, _ := newTestCluster(t, "http://unused", nil)
	spec := &apiv1.FlinkKubernetes{Namespace: "flink", Kubeconfig: structpb.NewStringValue("apiVersion: v1"), TaskManager: &apiv1.FlinkResources{Cpu: 2, Memory: "4g"}}
	target, err := newKubernetes(t.Context(), spec, c, func(_ context.Context, kubeconfig []byte, _ string) (Kube, error) {
		k.opened = kubeconfig
		return k, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	c.target = target
	return c, k
}

func TestKubernetesDeploy(t *testing.T) {
	c, k := newKubeCluster(t)
	if string(k.opened) != "apiVersion: v1" {
		t.Errorf("kubeconfig = %q", k.opened)
	}
	j, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 7, RestoreFrom: "s3://x/chk-3", Bridge: "runner:7443", Fingerprint: "ab", Token: "tok", CheckpointInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	name := deploymentName(JobID("pay", 7))
	obj := k.objs[name]
	str := func(fields ...string) string { v, _, _ := unstructured.NestedString(obj, fields...); return v }
	if str("metadata", "labels", labelPipeline) != "pay" || str("metadata", "labels", labelGeneration) != "7" || str("spec", "flinkVersion") != "v1_20" ||
		str("spec", "image") != "flink:1.20" || str("spec", "serviceAccount") != "flink" || str("spec", "job", "initialSavepointPath") != "s3://x/chk-3" ||
		str("spec", "job", "entryClass") != "com.acme.Fraud" || !strings.HasSuffix(str("spec", "job", "jarURI"), "/org/pay/_flink/code/"+c.s.Transform.GetCode().GetSha256()+"/fraud.jar") ||
		str("spec", "job", "upgradeMode") != "stateless" {
		t.Errorf("deployment = %v", obj)
	}
	conf, _, _ := unstructured.NestedStringMap(obj, "spec", "flinkConfiguration")
	if conf["$internal.pipeline.job-id"] != j.ID() || conf["high-availability.type"] != "kubernetes" || conf["execution.state-recovery.claim-mode"] != "NO_CLAIM" ||
		conf["execution.shutdown-on-application-finish"] != "false" || !strings.HasSuffix(conf["high-availability.storageDir"], "/org/pay/_flink/ha") {
		t.Errorf("conf = %v", conf)
	}
	if tm, _, _ := unstructured.NestedMap(obj, "spec", "taskManager", "resource"); tm["cpu"] != 2.0 || tm["memory"] != "4g" {
		t.Errorf("task manager = %v", tm)
	}
	args, _, _ := unstructured.NestedSlice(obj, "spec", "job", "args")
	if !slices.Equal(args, []any{"--openetl.bridge", "runner:7443", "--openetl.fingerprint", "ab", "--openetl.token", "tok"}) {
		t.Errorf("args = %v", args)
	}

	k.setStatus(name, map[string]any{"jobStatus": map[string]any{"state": "FAILED", "jobId": j.ID()}, "error": `{"type": "org.apache.flink.runtime.JobException", "message": "Recovery is suppressed"}`})
	var failure *runner.FlinkFailed
	if err := j.Wait(t.Context()); !errors.As(err, &failure) || failure.Message != "Recovery is suppressed" {
		t.Errorf("err = %v", err)
	}
	k.setStatus(name, map[string]any{"jobStatus": map[string]any{"state": "RUNNING", "jobId": "0000"}})
	if err := j.Wait(t.Context()); errs.CodeOf(err) != errs.Internal {
		t.Errorf("another job ID: %v", err)
	}
	k.setStatus(name, map[string]any{"jobManagerDeploymentStatus": "ERROR", "error": "ImagePullBackOff"})
	if err := j.Wait(t.Context()); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a JobManager that can't start: %v", err)
	}
}

func TestKubernetesStopAndCancel(t *testing.T) {
	c, k := newKubeCluster(t)
	j, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	name := deploymentName(j.ID())
	k.setStatus(name, map[string]any{"jobStatus": map[string]any{"state": "FINISHED", "jobId": j.ID()}})
	if err := j.Stop(t.Context(), c.SavepointDir("s")); err != nil {
		t.Fatal(err)
	}
	if err := j.Wait(t.Context()); err != nil {
		t.Errorf("a stopped job's wait: %v", err)
	}

	// Another pipeline's deployment is left.
	_ = k.Create(t.Context(), map[string]any{"metadata": map[string]any{"name": "other", "labels": map[string]any{labelPipeline: "payroll"}}})
	if _, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 2}); err != nil {
		t.Fatal(err)
	}
	if err := c.CancelAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	slices.Sort(k.deleted)
	if want := []string{deploymentName(JobID("pay", 1)), deploymentName(JobID("pay", 2))}; !slices.Equal(k.deleted, slices.Sorted(slices.Values(want))) || len(k.objs) != 1 {
		t.Errorf("deleted = %v, left %d", k.deleted, len(k.objs))
	}
	if err := j.Wait(t.Context()); errs.CodeOf(err) != errs.Transient {
		t.Errorf("a deleted deployment's wait: %v", err)
	}
}
