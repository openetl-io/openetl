//go:build e2e

package e2e

import (
	"cmp"
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/tools/clientcmd"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// kube is the kind cluster make kind-up starts: its kubeconfig, its
// namespace for jobs, and how its pods reach this host.
type kube struct {
	config    []byte
	namespace string
	host      string
	dyn       dynamic.Interface
}

var (
	flinkDeployments = k8sschema.GroupVersionResource{Group: "flink.apache.org", Version: "v1beta1", Resource: "flinkdeployments"}
	pods             = k8sschema.GroupVersionResource{Version: "v1", Resource: "pods"}
)

func kubeEnv(t *testing.T) *kube {
	t.Helper()
	path := os.Getenv("OPENETL_E2E_KUBECONFIG")
	if path == "" {
		t.Skip("set OPENETL_E2E_KUBECONFIG to run the Kubernetes targets (make kind-up starts a cluster)")
	}
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(b)
	if err != nil {
		t.Fatal(err)
	}
	d, err := dynamic.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return &kube{
		config:    b,
		namespace: cmp.Or(os.Getenv("OPENETL_E2E_KUBE_NAMESPACE"), "openetl"),
		host:      cmp.Or(os.Getenv("OPENETL_E2E_KUBE_HOST"), "host.docker.internal"),
		dyn:       d,
	}
}

func (k *kube) list(t *testing.T, r k8sschema.GroupVersionResource, selector string) []unstructured.Unstructured {
	t.Helper()
	l, err := k.dyn.Resource(r).Namespace(k.namespace).List(t.Context(), metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// deployments is the pipeline's FlinkDeployments: each one's generation,
// job state and parallelism.
func (k *kube) deployments(t *testing.T, pipelineID string) []string {
	t.Helper()
	var out []string
	for _, d := range k.list(t, flinkDeployments, "openetl.io/pipeline="+pipelineID) {
		gen := d.GetLabels()["openetl.io/generation"]
		state, _, _ := unstructured.NestedString(d.Object, "status", "jobStatus", "state")
		p, _, _ := unstructured.NestedInt64(d.Object, "spec", "job", "parallelism")
		out = append(out, fmt.Sprintf("%s %s %d", gen, cmp.Or(state, "-"), p))
	}
	return out
}

// running is the pipeline's one FlinkDeployment whose job runs, and its
// parallelism; "" while there's none, or more than one deployment.
func (k *kube) running(t *testing.T, pipelineID string) (name string, parallelism int64) {
	t.Helper()
	all := k.list(t, flinkDeployments, "openetl.io/pipeline="+pipelineID)
	if len(all) != 1 {
		return "", 0
	}
	state, _, _ := unstructured.NestedString(all[0].Object, "status", "jobStatus", "state")
	if state != "RUNNING" {
		return "", 0
	}
	p, _, _ := unstructured.NestedInt64(all[0].Object, "spec", "job", "parallelism")
	return all[0].GetName(), p
}

// killJobManager deletes the deployment's JobManager pods.
func (k *kube) killJobManager(t *testing.T, deployment string) {
	t.Helper()
	jms := k.list(t, pods, "component=jobmanager,app="+deployment)
	if len(jms) == 0 {
		t.Fatalf("no JobManager pod for %s", deployment)
	}
	for _, p := range jms {
		zero := int64(0)
		if err := k.dyn.Resource(pods).Namespace(k.namespace).Delete(t.Context(), p.GetName(), metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
			t.Fatal(err)
		}
	}
}

// A continuous pipeline's rows go through Flink on the Kubernetes operator,
// a FlinkDeployment for each generation, with Flink's Kubernetes HA. Every
// row lands once through a JobManager killed, an upgrade that rescales the
// job from a savepoint, a runner killed and another drained. A pause stops
// the job with a savepoint and deletes its deployment.
func TestFlinkOnKubernetes(t *testing.T) {
	k := kubeEnv(t)
	lib := flinkLib(t)
	name := unique("flinkkube")
	bucket := strings.ReplaceAll(name, "_", "-")
	store := "s3://" + bucket + "?endpoint=" + url.QueryEscape(s3Endpoint())
	c, err := objectstore.S3Client(mustParse(t, store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })
	code, digest := codeInS3(t, lib)
	first := flinkRunnerFor(t, store, k.host)

	s3conf := fmt.Sprintf(`"s3.endpoint": "http://%s:8333", "s3.path.style.access": "true", "s3.access-key": "any", "s3.secret-key": "any",
		"containerized.master.env.ENABLE_BUILT_IN_PLUGINS": "flink-s3-fs-hadoop-1.20.5.jar",
		"containerized.taskmanager.env.ENABLE_BUILT_IN_PLUGINS": "flink-s3-fs-hadoop-1.20.5.jar",
		"taskmanager.numberOfTaskSlots": "2"`, k.host)
	cluster := fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "FlinkCluster", "metadata": {"name": %q},
		"spec": {"kubernetes": {"namespace": %q, "image": "flink:1.20.5", "kubeconfig": {"secret": %q},
			"jobManager": {"cpu": 0.5, "memory": "1024m"}, "taskManager": {"cpu": 0.5, "memory": "1536m"}},
			"conf": {%s}}}`, name, k.namespace, setSecret(t, string(k.config)), s3conf)
	pipeline := func(parallelism int) []string {
		docs := continuousDocs(name, endlessFaker)
		docs[2] = strings.Replace(docs[2], `"delivery": {"commit": {"interval": "1s"}}`, fmt.Sprintf(`"delivery": {"commit": {"interval": "2s"}},
			"transform": {"flink": {"cluster": %q, "code": {"jar": %q, "sha256": %q, "class": "io.openetl.flink.examples.Passthrough"},
				"parallelism": %d}}`, name, code, digest, parallelism), 1)
		return docs
	}
	apply(t, append([]string{cluster}, pipeline(1)...)...)
	pipelineName := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName}))
	})
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	// deployed waits for a running job of this parallelism, other than not.
	deployed := func(what string, parallelism int64, not string) string {
		t.Helper()
		var d string
		eventually(t, 5*time.Minute, what, func() bool {
			var p int64
			d, p = k.running(t, id)
			return d != "" && d != not && p == parallelism
		})
		t.Logf("%s: %v", what, k.deployments(t, id))
		return d
	}
	more := func(what string) {
		t.Helper()
		from := landed(t, name)
		eventually(t, 5*time.Minute, what, func() bool { return landed(t, name) >= from+50 })
	}

	d := deployed("the job running on the operator", 1, "")
	more("rows through the Flink job")

	// HA brings the job back from its latest checkpoint, in place.
	k.killJobManager(t, d)
	more("rows after the JobManager's restart")
	if got := deployed("the job after the JobManager's restart", 1, ""); got != d {
		t.Errorf("running %s after the JobManager's restart, want %s", got, d)
	}

	// A new version stops the job with a savepoint, and the next generation
	// starts from it, rescaled.
	apply(t, pipeline(2)...)
	d = deployed("the upgraded job", 2, d)
	more("rows after the upgrade")

	// Another runner takes over when the first dies, and a third when the
	// second drains.
	second := flinkRunnerFor(t, store, k.host)
	first.kill(t)
	d = deployed("the job after the takeover", 2, d)
	more("rows after the takeover")
	third := flinkRunnerFor(t, store, k.host)
	if err := second.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case <-second.done:
	case <-time.After(5 * time.Minute):
		t.Fatal("the drained runner didn't exit")
	}
	if !strings.Contains(second.out.String(), "stream job drained") {
		t.Errorf("the second runner didn't drain its job:\n%s", second.out.String())
	}
	deployed("the job after the drain", 2, d)
	more("rows after the drain")
	if !third.hosts() {
		t.Error("the third runner doesn't run the job")
	}

	if _, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Minute, "the paused job's deployment deleted", func() bool { return len(k.deployments(t, id)) == 0 })
	if got := statuses(runs(t, name)); !strings.HasSuffix(got, "stopped") || strings.Contains(got, "failed") {
		t.Errorf("runs %q", got)
	}
	landed(t, name)
}
