//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

// platform is openetl installed in kind from deploy/kubernetes with its KEDA
// component, as its README says, on a database and a bucket of its own, in a
// namespace of its own. The test's helpers reach its API at a NodePort on
// kind's node.
type platform struct {
	cp         *controlPlane
	client     kubernetes.Interface
	namespace  string
	kubeconfig string
}

// platformOnKube installs it, with runners that each take capacity, which
// KEDA's thresholds match.
func platformOnKube(t *testing.T, capacity resources.Size) *platform {
	t.Helper()
	k := kubeEnv(t)
	image := os.Getenv("OPENETL_E2E_KUBE_IMAGE")
	if image == "" {
		t.Skip("set OPENETL_E2E_KUBE_IMAGE to openetld's image in the cluster (make image kind-load, then KIND_IMAGE)")
	}
	cfg, err := clientcmd.RESTConfigFromKubeConfig(k.config)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx := t.Context()
	p := &platform{cp: separateControlPlane(t), client: client, namespace: unique("openetl"), kubeconfig: os.Getenv("OPENETL_E2E_KUBECONFIG")}
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: p.namespace}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = client.CoreV1().Namespaces().Delete(ctx, p.namespace, metav1.DeleteOptions{})
		zero := int64(0)
		_ = client.CoreV1().Pods(p.namespace).DeleteCollection(ctx, metav1.DeleteOptions{GracePeriodSeconds: &zero}, metav1.ListOptions{})
	})
	t.Cleanup(func() {
		if t.Failed() {
			p.dump(t)
		}
	})

	bucket := unique("platform")
	c, err := objectstore.S3Client(mustParse(t, "s3://"+bucket+"?endpoint="+url.QueryEscape(s3Endpoint())))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })
	store := "s3://" + bucket + "?endpoint=" + url.QueryEscape("http://"+net.JoinHostPort(k.host, "8333"))
	dsn, err := url.Parse(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Host = net.JoinHostPort(k.host, dsn.Port())

	key := make([]byte, 32)
	_, _ = rand.Read(key)
	p.secret(t, "openetl", map[string]string{"OPENETL_DATABASE_URL": dsn.String(), "OPENETL_SECRETS_KEY": "base64key://" + base64.URLEncoding.EncodeToString(key)})
	p.apply(t, image, capacity, store)
	p.kubectl(t, nil, "rollout", "status", "deploy/openetl-server", "--timeout", "3m")

	svc, err := client.CoreV1().Services(p.namespace).Create(ctx, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "openetl-server-e2e"},
		Spec: corev1.ServiceSpec{
			Type:     corev1.ServiceTypeNodePort,
			Selector: map[string]string{"app.kubernetes.io/name": "openetl", "app.kubernetes.io/component": "server"},
			Ports:    []corev1.ServicePort{{Port: 8080, TargetPort: intstr.FromString("api")}},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := client.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil || len(nodes.Items) == 0 {
		t.Fatalf("kind's nodes: %v", err)
	}
	var node string
	for _, a := range nodes.Items[0].Status.Addresses {
		if a.Type == corev1.NodeInternalIP && !strings.Contains(a.Address, ":") {
			node = a.Address
		}
	}
	env.apiURL = "http://" + net.JoinHostPort(node, strconv.Itoa(int(svc.Spec.Ports[0].NodePort)))

	// The owner key, as the README says to get it.
	owner := strings.TrimSpace(string(p.kubectl(t, nil, "exec", "deploy/openetl-server", "--", "sh", "-c", "openetld bootstrap-key 2>/dev/null && cat .openetl/admin.key")))
	if err := auth.WriteKeyFile(env.keyFile, owner); err != nil {
		t.Fatal(err)
	}
	env.http = withKey(owner)
	eventually(t, time.Minute, "the API at "+env.apiURL, func() bool {
		tok, err := apiv1connect.NewAccessServiceClient(env.http, env.apiURL).CreateRunnerToken(ctx, connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/default"}))
		if err != nil {
			return false
		}
		env.runnerToken = tok.Msg.GetSecret()
		return true
	})
	env.runner = withKey(env.runnerToken)
	p.secret(t, "openetl-runner", map[string]string{"OPENETL_RUNNER_TOKEN": env.runnerToken})
	if err := applyOfficialConnectors(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := apiv1connect.NewSecretServiceClient(env.http, env.apiURL).UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/e2e-dsn", Value: dsn.String()}, AllowMissing: true,
	})); err != nil {
		t.Fatal(err)
	}
	for _, d := range []string{"openetl-orchestrator", "openetl-runner-default", "openetl-launcher-default"} {
		p.kubectl(t, nil, "rollout", "status", "deploy/"+d, "--timeout", "3m")
	}
	p.kubectl(t, nil, "wait", "scaledobject/openetl-runner-default", "--for", "condition=Ready", "--timeout", "2m")
	eventually(t, time.Minute, "a runner and the launcher to report", func() bool {
		n, err := query[int](t, "SELECT count(*) FROM runners WHERE pool = 'default' AND attempt = '' AND (launcher OR capacity_cpu > 0)")
		return err == nil && n == 2
	})
	return p
}

// apply builds an overlay of deploy/kubernetes for kind and applies it.
func (p *platform) apply(t *testing.T, image string, capacity resources.Size, store string) {
	t.Helper()
	dir := t.TempDir()
	rel := func(path string) string {
		abs, err := filepath.Abs(path)
		if err != nil {
			t.Fatal(err)
		}
		r, err := filepath.Rel(dir, abs)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	name, tag := image, "latest"
	if i := strings.LastIndex(image, ":"); i > strings.LastIndex(image, "/") {
		name, tag = image[:i], image[i+1:]
	}
	overlay := fmt.Sprintf(`apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: %s
resources:
  - %s
components:
  - %s
images:
  - name: openetl
    newName: %s
    newTag: %s
patches:
  - patch: |
      apiVersion: v1
      kind: ConfigMap
      metadata:
        name: openetl
      data:
        OPENETL_CHECKPOINTS: %q
        OPENETL_EGRESS_ALLOW: %q
        OPENETL_REPORT_INTERVAL: %q
        OPENETL_LOG_FORMAT: text
  - patch: |
      apiVersion: apps/v1
      kind: Deployment
      metadata:
        name: openetl-runner-default
      spec:
        template:
          spec:
            containers:
              - name: runner
                env:
                  - name: OPENETL_CAPACITY
                    value: %q
                resources:
                  requests:
                    cpu: 100m
                    memory: 256Mi
                  limits:
                    cpu: "3"
                    memory: 6Gi
  # kind's node has room for little more than the other tests' pods.
  - target:
      kind: Deployment
      labelSelector: app.kubernetes.io/name=openetl
    patch: |
      - op: replace
        path: /spec/template/spec/containers/0/resources/requests/cpu
        value: 10m
  - target:
      kind: ScaledObject
      name: openetl-runner-default
    patch: |
      - op: replace
        path: /spec/pollingInterval
        value: 5
      - op: replace
        path: /spec/advanced/horizontalPodAutoscalerConfig/behavior/scaleDown/stabilizationWindowSeconds
        value: 15
      - op: replace
        path: /spec/advanced/horizontalPodAutoscalerConfig/behavior/scaleDown/policies/0/periodSeconds
        value: 15
      - op: replace
        path: /spec/triggers/0/metadata/threshold
        value: %q
      - op: replace
        path: /spec/triggers/1/metadata/threshold
        value: %q
`, p.namespace, rel("../../deploy/kubernetes"), rel("../../deploy/kubernetes/keda"), name, tag,
		store, egressAllow, reportInterval(), capacity.Flag(),
		strconv.FormatFloat(float64(capacity.CPU)/1000, 'f', -1, 64), strconv.FormatInt(capacity.Memory, 10))
	if err := os.WriteFile(filepath.Join(dir, "kustomization.yaml"), []byte(overlay), 0o600); err != nil {
		t.Fatal(err)
	}
	p.kubectl(t, p.kubectl(t, nil, "kustomize", "--load-restrictor", "LoadRestrictionsNone", dir), "apply", "-f", "-")
}

func (p *platform) secret(t *testing.T, name string, data map[string]string) {
	t.Helper()
	if _, err := p.client.CoreV1().Secrets(p.namespace).Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name}, StringData: data,
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
}

// run runs kubectl in the platform's namespace.
func (p *platform) run(stdin []byte, args ...string) ([]byte, error) {
	cmd := exec.Command("kubectl", append([]string{"--kubeconfig", p.kubeconfig, "-n", p.namespace}, args...)...)
	if stdin != nil {
		cmd.Stdin = bytes.NewReader(stdin)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return out, fmt.Errorf("%w: %s", err, stderr.String())
	}
	return out, nil
}

func (p *platform) kubectl(t *testing.T, stdin []byte, args ...string) []byte {
	t.Helper()
	out, err := p.run(stdin, args...)
	if err != nil {
		t.Fatalf("kubectl %s: %v", strings.Join(args, " "), err)
	}
	return out
}

// dump logs what a failed test needs: the objects, events and logs.
func (p *platform) dump(t *testing.T) {
	for _, args := range [][]string{
		{"get", "pods,deployments,scaledobjects,hpa", "-o", "wide"},
		{"get", "events", "--sort-by=.lastTimestamp"},
		{"logs", "-l", "app.kubernetes.io/name=openetl", "--all-containers", "--prefix", "--tail", "200", "--max-log-requests", "20"},
	} {
		out, err := p.run(nil, args...)
		t.Logf("kubectl %s (%v):\n%s", strings.Join(args, " "), err, out)
	}
}

// runners is how many of the default pool's runner pods are ready and not
// stopping, and how many its Deployment asks for.
func (p *platform) runners(t *testing.T) (ready, want int) {
	t.Helper()
	d, err := p.client.AppsV1().Deployments(p.namespace).Get(t.Context(), "openetl-runner-default", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	pods, err := p.client.CoreV1().Pods(p.namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "app.kubernetes.io/component=runner,openetl.io/pool=default"})
	if err != nil {
		t.Fatal(err)
	}
	for _, pod := range pods.Items {
		for _, c := range pod.Status.Conditions {
			if c.Type == corev1.PodReady && c.Status == corev1.ConditionTrue && pod.DeletionTimestamp == nil {
				ready++
			}
		}
	}
	return ready, int(*d.Spec.Replicas)
}

// KEDA scales a pool's runners on the placer's metrics: up to the room the
// jobs waiting for one ask for, and down once they've gone, a runner draining
// its job to a checkpoint before it stops. Each row lands once.
func TestRunnerPoolScalesOnKubernetes(t *testing.T) {
	p := platformOnKube(t, resources.S)
	if ready, want := p.runners(t); ready != 1 || want != 1 {
		t.Fatalf("%d runners ready of %d, want the one KEDA keeps", ready, want)
	}
	var names []string
	for i := range 3 {
		name := unique(fmt.Sprintf("scaled-%d", i))
		names = append(names, name)
		sizedStreamOn(t, name, "default", `{"size": "s"}`)
	}
	running := func() int {
		n, err := query[int](t, "SELECT count(*) FROM dispatches WHERE kind = 'RunStreamJob' AND state = 'running'")
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	eventually(t, 4*time.Minute, "three runners, each running a job", func() bool {
		ready, want := p.runners(t)
		return ready == 3 && want == 3 && running() == 3
	})
	for _, name := range names {
		waitRows(t, name, 100)
	}

	for _, name := range names[1:] {
		if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name})); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, 5*time.Minute, "one runner left, running the last job", func() bool {
		ready, want := p.runners(t)
		return ready == 1 && want == 1 && running() == 1
	})
	waitRows(t, names[0], 100)
	landed(t, names[0])
}

// longerFaker runs for about two minutes, past an orchestrator's recovery or a
// rolling upgrade.
const longerFaker = `{"seed": 4, "count": 60000, "batchSize": 50, "delay": "100ms"}`

// An orchestrator's pod killed mid-sync: another recovers the sync within a
// minute, and it carries on with the same attempt. Then an upgrade rolls the
// server and orchestrators onto a new version with workflows of the old one in
// flight: the sync is started again and carries on from the checkpoint log,
// and the stream keeps its job.
func TestOrchestratorsOnKubernetes(t *testing.T) {
	requireDBOS(t)
	p := platformOnKube(t, resources.L)

	killed := unique("orchestrator-killed")
	apply(t, pipelineDocs(killed, longerFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 2*time.Minute, "a committed epoch", func() bool { return committedEpochs(t, schema(killed)) > 0 })
	row := mustQuery[string](t, `SELECT s.run_id FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, killed)
	var victim string
	eventually(t, 30*time.Second, "the sync's orchestrator", func() bool {
		victim = p.cp.executorOf(row)
		return victim != ""
	})
	zero := int64(0)
	if err := p.client.CoreV1().Pods(p.namespace).Delete(t.Context(), victim, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	deleted := time.Now()
	eventually(t, time.Minute, "another orchestrator to recover the sync", func() bool {
		e := p.cp.executorOf(row)
		return e != "" && e != victim
	})
	t.Logf("recovered after %v", time.Since(deleted).Round(time.Second))
	if status, stats := waitSync(t, killed, 4*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(killed), 60000, "users", "purchases")
	if n := mustQuery[int](t, `SELECT count(*) FROM attempts a JOIN pipelines p ON p.id = a.pipeline_id WHERE p.name = $1`, killed); n != 1 {
		t.Errorf("%d attempts, want the one carrying on", n)
	}

	stream := unique("upgraded-stream")
	startContinuous(t, stream)
	eventually(t, 2*time.Minute, "the stream's committed epoch", func() bool { return committedEpochs(t, schema(stream)) > 0 })
	scheduled := unique("upgraded-sync")
	apply(t, pipelineDocs(scheduled, longerFaker, `{"commit": {"interval": "1s"}}`)...)
	eventually(t, 2*time.Minute, "the sync's committed epoch", func() bool { return committedEpochs(t, schema(scheduled)) > 0 })

	p.kubectl(t, nil, "set", "env", "deploy/openetl-server", "deploy/openetl-orchestrator", "DBOS__APPVERSION=new")
	for _, d := range []string{"openetl-server", "openetl-orchestrator"} {
		p.kubectl(t, nil, "rollout", "status", "deploy/"+d, "--timeout", "3m")
	}
	before := committedEpochs(t, schema(stream))
	eventually(t, 2*time.Minute, "the sync to be started again", func() bool {
		return mustQuery[int](t, `SELECT count(*) FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1`, scheduled) >= 2
	})
	if status, stats := waitSync(t, scheduled, 4*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(scheduled), 60000, "users", "purchases")
	eventually(t, time.Minute, "the stream to carry on", func() bool { return committedEpochs(t, schema(stream)) > before+2 })
	if got := statuses(runs(t, stream)); got != "running" {
		t.Errorf("runs %q, want the one still running", got)
	}
	if n := mustQuery[int](t, `SELECT count(*) FROM attempts WHERE pipeline_id = $1`, pipelineID(t, stream)); n != 1 {
		t.Errorf("%d jobs, want the same one still running", n)
	}
	landed(t, stream)
}
