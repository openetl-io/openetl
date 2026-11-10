//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// podsOnKube is what a test's attempts' pods need in kind: the API, the
// checkpoint store and Postgres, at addresses pods reach.
type podsOnKube struct {
	k            *kube
	client       kubernetes.Interface
	plane, token string
	// The template's file, and the store's URL from this host.
	template, store string
	// The DSN secret for pods' destinations.
	dsn string
}

func kubeForPods(t *testing.T) *podsOnKube {
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
	plane, token := placementPlane(t)

	bucket := strings.ReplaceAll(unique("pods"), "_", "-")
	store := "s3://" + bucket + "?anonymous=true&endpoint=" + url.QueryEscape(s3Endpoint())
	c, err := objectstore.S3Client(t.Context(), mustParse(t, store))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })

	// The API on every address, for pods.
	target, err := url.Parse(env.apiURL)
	if err != nil {
		t.Fatal(err)
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &http.Server{Handler: httputil.NewSingleHostReverseProxy(target), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = proxy.Serve(ln) }()
	t.Cleanup(func() { _ = proxy.Close() })
	api := "http://" + net.JoinHostPort(k.host, fmt.Sprint(ln.Addr().(*net.TCPAddr).Port))

	secret := unique("runner-token")
	if _, err := client.CoreV1().Secrets(k.namespace).Create(t.Context(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secret}, StringData: map[string]string{"token": token},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = client.CoreV1().Secrets(k.namespace).Delete(context.Background(), secret, metav1.DeleteOptions{})
	})

	podStore := "s3://" + bucket + "?anonymous=true&endpoint=" + url.QueryEscape("http://"+net.JoinHostPort(k.host, "8333"))
	template := filepath.Join(t.TempDir(), "pod.yaml")
	if err := os.WriteFile(template, fmt.Appendf(nil, `apiVersion: v1
kind: Pod
spec:
  containers:
  - name: runner
    image: %s
    imagePullPolicy: Never
    args: [runner]
    env:
    - {name: OPENETL_API_URL, value: %q}
    - {name: OPENETL_RUNNER_TOKEN, valueFrom: {secretKeyRef: {name: %s, key: token}}}
    - {name: OPENETL_CHECKPOINTS, value: %q}
    - {name: OPENETL_EGRESS_ALLOW, value: %q}
    - {name: OPENETL_REPORT_INTERVAL, value: %q}
    - {name: OPENETL_LOG_FORMAT, value: text}
`, image, api, secret, podStore, egressAllow, reportInterval()), 0o600); err != nil {
		t.Fatal(err)
	}
	dsn, err := url.Parse(env.dbURL)
	if err != nil {
		t.Fatal(err)
	}
	dsn.Host = net.JoinHostPort(k.host, dsn.Port())
	return &podsOnKube{k: k, client: client, plane: plane, token: token, template: template, store: store, dsn: setSecret(t, dsn.String())}
}

// launcher starts a pod launcher for the data plane on this host, which
// makes every job's pod in the cluster.
func (p *podsOnKube) launcher(t *testing.T) *runner {
	t.Helper()
	return startRunnerWith(t, p.token, env.apiURL, "--data-plane", p.plane, "--checkpoints", p.store, "--launcher", "kubernetes",
		"--kubeconfig", os.Getenv("OPENETL_E2E_KUBECONFIG"), "--pod-namespace", p.k.namespace, "--pod-template", p.template,
		"--pod-min-size", "0,0", "--pod-scratch", "1Gi", "--reserve", "0.1,256Mi")
}

// all are the pipeline's pods, whatever their phase.
func (p *podsOnKube) all(t *testing.T, pipeline string) []corev1.Pod {
	t.Helper()
	l, err := p.client.CoreV1().Pods(p.k.namespace).List(t.Context(), metav1.ListOptions{LabelSelector: "openetl.io/pipeline=" + pipeline})
	if err != nil {
		t.Fatal(err)
	}
	return l.Items
}

// pods are the running pods of the pipeline named name.
func (p *podsOnKube) pods(t *testing.T, name string) []corev1.Pod {
	t.Helper()
	var out []corev1.Pod
	for _, pod := range p.all(t, mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)) {
		if pod.Status.Phase == corev1.PodRunning && pod.DeletionTimestamp == nil {
			out = append(out, pod)
		}
	}
	return out
}

// running waits for the pipeline's pod, other than not, and returns it.
func (p *podsOnKube) running(t *testing.T, name, not string) string {
	t.Helper()
	var pod string
	eventually(t, 2*time.Minute, "a pod running "+name, func() bool {
		ps := p.pods(t, name)
		if len(ps) != 1 || ps[0].Name == not {
			return false
		}
		pod = ps[0].Name
		return true
	})
	return pod
}

// A sync and a continuous job run in pods. A pod deleted at once is
// replaced, and an evicted one drains to a checkpoint; either way every row
// lands once. Pods go once their attempts end.
func TestAttemptsInPodsOnKubernetes(t *testing.T) {
	p := kubeForPods(t)
	p.launcher(t)
	onPlane := func(ds []string, res string) []string {
		ds[1] = strings.Replace(ds[1], "e2e-dsn", p.dsn, 1)
		ds[2] = strings.Replace(ds[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": %s, "source":`, p.plane, res), 1)
		return ds
	}

	batch := unique("kube-sync")
	apply(t, onPlane(pipelineDocs(batch, `{"seed": 4, "count": 2000, "batchSize": 100}`, `{"commit": {"interval": "1s"}}`), `{"size": "xs"}`)...)
	if status, stats := waitSync(t, batch, 3*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(batch), 2000, "users", "purchases")
	if n, err := query[int](t, `SELECT count(*) FROM dispatches d JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id WHERE p.name = $1 AND d.launcher IS NOT NULL`, batch); err != nil || n == 0 {
		t.Fatalf("none of the sync's attempts ran in a pod (%v)", err)
	}
	batchID := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", batch)
	eventually(t, time.Minute, "the sync's pod to go", func() bool { return len(p.all(t, batchID)) == 0 })

	stream := unique("kube-stream")
	apply(t, onPlane(continuousDocs(stream, endlessFaker), `{"size": "xs"}`)...)
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + stream}))
	})
	first := p.running(t, stream, "")
	waitRows(t, stream, 100)
	pod, err := p.client.CoreV1().Pods(p.k.namespace).Get(t.Context(), first, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if pod.Annotations["karpenter.sh/do-not-disrupt"] != "true" || pod.Spec.Containers[0].Resources.Limits.Cpu().MilliValue() != 600 {
		t.Errorf("the stream's pod has annotations %v and limits %v", pod.Annotations, pod.Spec.Containers[0].Resources.Limits)
	}

	// Deleted at once, as when its node goes.
	zero := int64(0)
	if err := p.client.CoreV1().Pods(p.k.namespace).Delete(t.Context(), first, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	second := p.running(t, stream, first)
	waitRows(t, stream, 100)

	// Evicted, as a spot interruption or a node drain does: it drains to a
	// checkpoint, and the job starts again at once in a new pod.
	if err := p.client.CoreV1().Pods(p.k.namespace).EvictV1(t.Context(), &policyv1.Eviction{ObjectMeta: metav1.ObjectMeta{Name: second, Namespace: p.k.namespace}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Minute, "the evicted attempt to drain", func() bool {
		n, err := query[int](t, `SELECT count(*) FROM attempts a JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id
			WHERE p.name = $1 AND a.code = 'DRAINING'`, stream)
		return err == nil && n > 0
	})
	p.running(t, stream, second)
	waitRows(t, stream, 100)

	landed(t, stream)
	streamID := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", stream)
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + stream})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 2*time.Minute, "the stream's pods to go", func() bool { return len(p.all(t, streamID)) == 0 })
}
