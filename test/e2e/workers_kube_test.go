//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"gocloud.dev/blob"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// podLog is what a pod has logged so far.
func (p *podsOnKube) podLog(t *testing.T, name string) string {
	t.Helper()
	b, err := p.client.CoreV1().Pods(p.k.namespace).GetLogs(name, &corev1.PodLogOptions{Container: "runner"}).DoRaw(t.Context())
	if err != nil {
		return ""
	}
	return string(b)
}

// gang is the running pods of the pipeline's attempt on workers: its
// coordinator's, and its workers' by index from 1, once they all run.
func (p *podsOnKube) gang(t *testing.T, name string, workers int) (coordinator corev1.Pod, ws map[int]corev1.Pod) {
	t.Helper()
	eventually(t, 3*time.Minute, fmt.Sprintf("%s's coordinator and %d workers to run", name, workers), func() bool {
		ws = map[int]corev1.Pod{}
		found := false
		for _, pod := range p.pods(t, name) {
			base, w, ok := strings.Cut(pod.Name, "-w")
			if !ok {
				coordinator, found = pod, pod.Status.PodIP != ""
				continue
			}
			n, err := strconv.Atoi(w)
			if err != nil || base == "" {
				continue
			}
			ws[n] = pod
		}
		if !found || len(ws) != workers {
			return false
		}
		for _, w := range ws {
			if !strings.HasPrefix(w.Name, coordinator.Name+"-w") {
				return false
			}
		}
		return true
	})
	return coordinator, ws
}

func envOf(pod corev1.Pod, name string) (corev1.EnvVar, bool) {
	for _, c := range pod.Spec.Containers {
		if c.Name != "runner" {
			continue
		}
		for _, e := range c.Env {
			if e.Name == name {
				return e, true
			}
		}
	}
	return corev1.EnvVar{}, false
}

// s3Has reports whether any object is under the prefix.
func s3Has(t *testing.T, b *blob.Bucket, prefix string) bool {
	t.Helper()
	_, err := b.List(&blob.ListOptions{Prefix: prefix}).Next(t.Context())
	return err == nil
}

// docsNetworkPolicy is the NetworkPolicy the Kubernetes page gives for the
// route between job pods.
func docsNetworkPolicy(t *testing.T) *networkingv1.NetworkPolicy {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "docs", "self-host", "kubernetes.md"))
	if err != nil {
		t.Fatal(err)
	}
	_, after, ok := strings.Cut(string(b), "#### Allow the route in a NetworkPolicy")
	if ok {
		_, after, ok = strings.Cut(after, "```yaml\n")
	}
	block, _, closed := strings.Cut(after, "```")
	if !ok || !closed {
		t.Fatal("the Kubernetes page has no NetworkPolicy for the route between job pods")
	}
	var np networkingv1.NetworkPolicy
	if err := yaml.UnmarshalStrict([]byte(block), &np); err != nil {
		t.Fatalf("the Kubernetes page's NetworkPolicy: %v", err)
	}
	return &np
}

// isolate makes the data plane's pods as a namespace that denies by default
// has them: no traffic but what a policy allows. Others allow what any job
// pod needs, DNS and everything outside the cluster's pods (the API, the
// bucket and Postgres, on this host), and the Kubernetes page's policy the
// route between job pods.
func (p *podsOnKube) isolate(t *testing.T) {
	t.Helper()
	nodes, err := p.client.CoreV1().Nodes().List(t.Context(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var podCIDRs []string
	for _, n := range nodes.Items {
		podCIDRs = append(podCIDRs, n.Spec.PodCIDRs...)
	}
	if len(podCIDRs) == 0 {
		t.Fatal("the cluster's nodes have no pod CIDRs")
	}
	plane := metav1.LabelSelector{MatchLabels: map[string]string{"openetl.io/data-plane": p.plane}}
	udp, tcp, dns := corev1.ProtocolUDP, corev1.ProtocolTCP, intstr.FromInt32(53)
	both := []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}
	policies := []*networkingv1.NetworkPolicy{
		{ObjectMeta: metav1.ObjectMeta{Name: p.plane + "-deny"}, Spec: networkingv1.NetworkPolicySpec{PodSelector: plane, PolicyTypes: both}},
		{ObjectMeta: metav1.ObjectMeta{Name: p.plane + "-outside"}, Spec: networkingv1.NetworkPolicySpec{
			PodSelector: plane, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeEgress},
			Egress: []networkingv1.NetworkPolicyEgressRule{
				{To: []networkingv1.NetworkPolicyPeer{{IPBlock: &networkingv1.IPBlock{CIDR: "0.0.0.0/0", Except: podCIDRs}}}},
				{
					To:    []networkingv1.NetworkPolicyPeer{{NamespaceSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"kubernetes.io/metadata.name": "kube-system"}}}},
					Ports: []networkingv1.NetworkPolicyPort{{Protocol: &udp, Port: &dns}, {Protocol: &tcp, Port: &dns}},
				},
			},
		}},
		docsNetworkPolicy(t),
	}
	api := p.client.NetworkingV1().NetworkPolicies(p.k.namespace)
	for _, np := range policies {
		_ = api.Delete(t.Context(), np.Name, metav1.DeleteOptions{})
		if _, err := api.Create(t.Context(), np, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = api.Delete(context.Background(), np.Name, metav1.DeleteOptions{}) })
	}
}

// reaches says whether a pod of the data plane, with labels besides the
// data plane's, opens a TCP connection to addr within tries attempts of 5
// seconds each, 5 seconds apart: a new pod's allow rules may take a moment
// to apply.
func (p *podsOnKube) reaches(t *testing.T, labels map[string]string, addr string, tries int) bool {
	t.Helper()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	labels["openetl.io/data-plane"] = p.plane
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: unique("probe"), Labels: labels},
		Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, Containers: []corev1.Container{{
			Name: "probe", Image: os.Getenv("OPENETL_E2E_KUBE_IMAGE"), ImagePullPolicy: corev1.PullNever,
			Command: []string{"bash", "-c", fmt.Sprintf("for i in $(seq %d); do [ $i = 1 ] || sleep 5; timeout 5 bash -c '</dev/tcp/%s/%s' && exit 0; done; exit 1", tries, host, port)},
		}}},
	}
	pods := p.client.CoreV1().Pods(p.k.namespace)
	if _, err := pods.Create(t.Context(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pods.Delete(context.Background(), pod.Name, metav1.DeleteOptions{}) })
	var phase corev1.PodPhase
	eventually(t, time.Minute+time.Duration(tries)*10*time.Second, pod.Name+" to end", func() bool {
		got, err := pods.Get(t.Context(), pod.Name, metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		phase = got.Status.Phase
		return phase == corev1.PodSucceeded || phase == corev1.PodFailed
	})
	return phase == corev1.PodSucceeded
}

// An attempt on workers runs as pods of the cluster, which one launcher
// makes all or none of: a pod for each worker and one for their
// coordinator, which serves them at its pod's IP on port 7720, where each
// worker's pod joins it, all in a namespace that denies by default, under
// the Kubernetes page's NetworkPolicy. A worker's pod deleted mid-sync fails the attempt
// as transient, and the retry lands every row once. An attempt whose
// workers and coordinator are more pods than the launcher has fails,
// saying what it needs, and no pod of it is made.
func TestWorkersOnKubernetes(t *testing.T) {
	p := kubeForPods(t)
	endpoint := "http://" + net.JoinHostPort(p.k.host, "8333")
	workerDocs := func(name, faker, dst, res string) []string {
		d := docs(name, fmt.Sprintf(`{"connector": "faker", "config": %s}`, faker), dst, `{"commit": {"interval": "1s"}}`)
		d[2] = strings.Replace(d[2], `"source":`, fmt.Sprintf(`"dataPlane": %q, "resources": %s, "source":`, p.plane, res), 1)
		d[2] = strings.Replace(d[2], `}, "delivery":`, s3Append+`}, "delivery":`, 1)
		return d
	}
	remove := func(name string) {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
	}

	// Applied before the launcher reports, so the plan can't refuse it: 3
	// workers and their coordinator are 4 pods, and the launcher has 3.
	tooMany := unique("kube-gang-too-many")
	dst, _ := s3DestAt(t, endpoint)
	apply(t, workerDocs(tooMany, `{"seed": 7, "count": 100}`, dst, `{"size": "xs", "workers": 3}`)...)
	t.Cleanup(func() { remove(tooMany) })
	p.launcher(t, "--max-pods", "3")
	eventually(t, 2*time.Minute, "the attempt the launcher can't take to fail", func() bool {
		e, err := query[string](t, `SELECT coalesce(s.error, '') FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id WHERE p.name = $1 ORDER BY s.start_time DESC LIMIT 1`, tooMany)
		if err != nil || !strings.Contains(e, "need 4 pods") {
			return false
		}
		if !strings.Contains(e, "at most 3 pods at once") {
			t.Errorf("error %q, want the launcher's cap", e)
		}
		return true
	})
	if n := mustQuery[int](t, `SELECT count(*) FROM dispatches d JOIN attempts a ON a.org_id = d.org_id AND a.id = d.id
		JOIN pipelines p ON p.org_id = a.org_id AND p.id = a.pipeline_id WHERE p.name = $1 AND d.launcher IS NOT NULL`, tooMany); n != 0 {
		t.Errorf("%d of the attempts the launcher can't take were placed on it", n)
	}
	if pods := p.all(t, mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", tooMany)); len(pods) != 0 {
		t.Errorf("%d pods for the attempt the launcher can't take", len(pods))
	}
	remove(tooMany)

	p.isolate(t)
	name := unique("kube-workers")
	dst, b := s3DestAt(t, endpoint)
	const rows = 60_000
	apply(t, workerDocs(name, fmt.Sprintf(`{"seed": 6, "count": %d, "batchSize": 100, "delay": "100ms"}`, rows), dst, `{"size": "xs", "workers": 2}`)...)
	t.Cleanup(func() { remove(name) })
	coordinator, workers := p.gang(t, name, 2)

	// The coordinator listens on 7720 and advertises its pod's IP, which the
	// workers join it at.
	if e, ok := envOf(coordinator, "OPENETL_POD_IP"); !ok || e.ValueFrom == nil || e.ValueFrom.FieldRef == nil || e.ValueFrom.FieldRef.FieldPath != "status.podIP" {
		t.Errorf("the coordinator's OPENETL_POD_IP is %+v", e)
	}
	if e, _ := envOf(coordinator, "OPENETL_ATTEMPT_LISTEN"); e.Value != ":7720" {
		t.Errorf("the coordinator listens at %q, want :7720", e.Value)
	}
	if e, _ := envOf(coordinator, "OPENETL_ATTEMPT_ADVERTISE"); e.Value != "$(OPENETL_POD_IP)" {
		t.Errorf("the coordinator advertises %q, want its pod's IP", e.Value)
	}
	if !slices.ContainsFunc(coordinator.Spec.Containers[0].Ports, func(port corev1.ContainerPort) bool { return port.ContainerPort == 7720 }) {
		t.Errorf("the coordinator's pod declares ports %v, want 7720", coordinator.Spec.Containers[0].Ports)
	}
	if _, ok := envOf(coordinator, "OPENETL_WORKER"); ok {
		t.Error("the coordinator's pod has OPENETL_WORKER")
	}
	at := net.JoinHostPort(coordinator.Status.PodIP, "7720")
	for n, w := range workers {
		if e, _ := envOf(w, "OPENETL_WORKER"); e.Value != strconv.Itoa(n) {
			t.Errorf("worker pod %s has OPENETL_WORKER %q, want %d", w.Name, e.Value, n)
		}
		// Its engine listens for the other worker's on 7720, at its pod's
		// IP: faker's keyed streams aren't split by key, so the engines
		// exchange their rows.
		if e, _ := envOf(w, "OPENETL_EXCHANGE_LISTEN"); e.Value != ":7720" {
			t.Errorf("worker pod %s's engine listens at %q, want :7720", w.Name, e.Value)
		}
		if e, _ := envOf(w, "OPENETL_EXCHANGE_ADVERTISE"); e.Value != "$(OPENETL_POD_IP)" {
			t.Errorf("worker pod %s's engine advertises %q, want its pod's IP", w.Name, e.Value)
		}
		eventually(t, 2*time.Minute, w.Name+" to join its coordinator at "+at, func() bool {
			log := p.podLog(t, w.Name)
			return strings.Contains(log, `msg="runner running one worker of an attempt"`) && strings.Contains(log, fmt.Sprintf("worker=%d", n)) &&
				strings.Contains(log, `msg="worker joining its coordinator"`) && strings.Contains(log, "coordinator="+at)
		})
	}
	// The cluster enforces the policies: a job pod reaches the coordinator,
	// and then, with the policies applied for as long, another pod of the
	// data plane doesn't, in one try, while a job pod still does after it,
	// so the coordinator was listening throughout.
	job := func() map[string]string {
		return map[string]string{"app.kubernetes.io/managed-by": "openetl", "app.kubernetes.io/component": "attempt"}
	}
	if !p.reaches(t, job(), at, 12) {
		t.Fatalf("a job pod doesn't reach the coordinator at %s under the Kubernetes page's NetworkPolicy", at)
	}
	denied := p.reaches(t, map[string]string{"app.kubernetes.io/name": "probe"}, at, 1)
	if !p.reaches(t, job(), at, 1) {
		t.Fatalf("the coordinator at %s stopped listening during the denied probe, so that probe shows nothing", at)
	}
	if denied {
		t.Errorf("a pod no policy allows reaches the coordinator at %s, so the cluster doesn't enforce NetworkPolicy", at)
	}

	// A worker's pod goes, as when its node does, once an epoch committed.
	eventually(t, 2*time.Minute, "a committed epoch", func() bool { return s3Has(t, b, "users/") || s3Has(t, b, "purchases/") })
	zero := int64(0)
	if err := p.client.CoreV1().Pods(p.k.namespace).Delete(t.Context(), workers[1].Name, metav1.DeleteOptions{GracePeriodSeconds: &zero}); err != nil {
		t.Fatal(err)
	}
	if status, stats := waitSync(t, name, 6*time.Minute); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkS3(t, b, rows, "users", "purchases")
	attempts := latestSync(t, name).GetAttempts()
	if !slices.ContainsFunc(attempts, func(a *apiv1.SyncAttempt) bool { return a.GetCode() == "TRANSIENT" }) {
		t.Errorf("attempts %v, want one failed as transient when its worker's pod went", attempts)
	}
	if last := lastAttempt(t, name); last.GetAttempt() < 2 || last.GetWorkers() != 2 {
		t.Errorf("the sync finished in attempt %d on %d workers, want a retry on 2", last.GetAttempt(), last.GetWorkers())
	}
	id := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	eventually(t, 2*time.Minute, "the sync's pods to go", func() bool { return len(p.all(t, id)) == 0 })
}
