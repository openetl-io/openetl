package pods

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/yaml"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

const template = `
apiVersion: v1
kind: Pod
metadata:
  labels: {team: data}
spec:
  serviceAccountName: openetl-attempt
  affinity:
    nodeAffinity:
      requiredDuringSchedulingIgnoredDuringExecution:
        nodeSelectorTerms:
        - matchExpressions: [{key: zone, operator: In, values: [a]}]
  containers:
  - name: sidecar
    image: proxy
  - name: runner
    image: ghcr.io/openetl-io/openetl:dev
    args: [runner]
    env: [{name: OPENETL_API_URL, value: "http://openetl:8080"}]
`

func launcher(t *testing.T) (*Kubernetes, *fake.Clientset) {
	t.Helper()
	tmpl, err := ParseTemplate([]byte(template))
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientset()
	return &Kubernetes{
		Client: c, Namespace: "etl", Template: tmpl, DataPlane: "default", Pool: "big",
		Reserve: resources.Size{CPU: 500, Memory: 512 * resources.MiB}, Scratch: 10 * resources.GiB, Arch: "arm64",
	}, c
}

func batch() Launch {
	return Launch{Attempt: "run/attempt/1", Placement: 2, Kind: contract.RunSyncAttempt, Size: resources.M, Pipeline: "8f0b6d5e-7c1a-4a43-9f5e-1d2c3b4a5f60", StopTimeout: 120}
}

func env(c *corev1.Container) map[string]string {
	out := map[string]string{}
	for _, e := range c.Env {
		out[e.Name] = e.Value
	}
	return out
}

// A pod is the template with the attempt's size, labels and environment,
// and what keeps batch pods off nothing and streams off spot.
func TestPodFromTemplate(t *testing.T) {
	k, _ := launcher(t)
	p := k.Pod(batch())
	if p.Name != Name("run/attempt/1", 2) || len(p.Name) > 63 || p.Namespace != "etl" {
		t.Errorf("name %q in %q", p.Name, p.Namespace)
	}
	if p.Labels["team"] != "data" || p.Labels[labelJob] != "batch" || p.Labels[labelPool] != "big" || p.Labels[labelPipeline] != "8f0b6d5e-7c1a-4a43-9f5e-1d2c3b4a5f60" {
		t.Errorf("labels %v", p.Labels)
	}
	if p.Annotations[annoAttempt] != "run/attempt/1" || p.Annotations[annoPlacement] != "2" || p.Annotations[annoDoNotDisrupt] != "" {
		t.Errorf("annotations %v", p.Annotations)
	}
	if p.Spec.RestartPolicy != corev1.RestartPolicyNever || *p.Spec.TerminationGracePeriodSeconds != 150 || p.Spec.ServiceAccountName != "openetl-attempt" {
		t.Errorf("spec %+v", p.Spec)
	}
	if p.Spec.NodeSelector[corev1.LabelArchStable] != "arm64" || p.Spec.NodeSelector[corev1.LabelOSStable] != "linux" {
		t.Errorf("node selector %v", p.Spec.NodeSelector)
	}
	if len(p.Spec.Tolerations) != len(spotTaints) {
		t.Errorf("tolerations %v, want spot's", p.Spec.Tolerations)
	}
	if exprs := p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions; len(exprs) != 1 {
		t.Errorf("a batch pod's node affinity %v, want the template's", exprs)
	}
	c := &p.Spec.Containers[1]
	if e := env(c); e["OPENETL_ATTEMPT"] != "run/attempt/1" || e["OPENETL_PLACEMENT"] != "2" || e["OPENETL_POOL"] != "big" || e["OPENETL_CAPACITY"] != "2,4294967296" || e["OPENETL_API_URL"] == "" {
		t.Errorf("env %v", e)
	}
	if len(p.Spec.Containers[0].Env) != 0 {
		t.Errorf("the sidecar has %v, want it left be", p.Spec.Containers[0].Env)
	}
	want := corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse("2500m"), corev1.ResourceMemory: resource.MustParse("4608Mi"), corev1.ResourceEphemeralStorage: resource.MustParse("10Gi"),
	}
	for name, q := range want {
		if r, l := c.Resources.Requests[name], c.Resources.Limits[name]; r.Cmp(q) != 0 || l.Cmp(q) != 0 {
			t.Errorf("%s: requests %s and limits %s, want %s", name, r.String(), l.String(), q.String())
		}
	}
	if c.TerminationMessagePolicy != corev1.TerminationMessageFallbackToLogsOnError {
		t.Errorf("termination message policy %q", c.TerminationMessagePolicy)
	}
	if k.Template.Labels[labelJob] != "" || len(k.Template.Spec.Containers[1].Env) != 1 {
		t.Error("making a pod changed the template")
	}

	notSpot := func(exprs []corev1.NodeSelectorRequirement) bool {
		return slices.ContainsFunc(exprs, func(e corev1.NodeSelectorRequirement) bool {
			return e.Key == "karpenter.sh/capacity-type" && e.Operator == corev1.NodeSelectorOpNotIn && slices.Equal(e.Values, []string{"spot"})
		})
	}
	// By default a stream pod prefers nodes that aren't spot, and can still
	// run on spot ones where there's nothing else.
	l := batch()
	l.Kind, l.StopTimeout = contract.RunStreamJob, 600
	p = k.Pod(l)
	if p.Labels[labelJob] != "stream" || p.Annotations[annoDoNotDisrupt] != "true" || *p.Spec.TerminationGracePeriodSeconds != 630 || len(p.Spec.Tolerations) != len(spotTaints) {
		t.Errorf("stream pod %v %v %d %v", p.Labels, p.Annotations, *p.Spec.TerminationGracePeriodSeconds, p.Spec.Tolerations)
	}
	na := p.Spec.Affinity.NodeAffinity
	if exprs := na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions; len(exprs) != 1 {
		t.Errorf("a stream pod's required node affinity %v, want the template's alone", exprs)
	}
	if pref := na.PreferredDuringSchedulingIgnoredDuringExecution; len(pref) != 1 || pref[0].Weight != 100 || len(pref[0].Preference.MatchExpressions) != len(spotLabels) || !notSpot(pref[0].Preference.MatchExpressions) {
		t.Errorf("a stream pod's preferred node affinity %v, want not spot", pref)
	}

	k.StreamsAvoidSpot = true
	p = k.Pod(l)
	if len(p.Spec.Tolerations) != 0 || p.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution != nil {
		t.Errorf("an avoiding stream pod's tolerations %v and preferences %v", p.Spec.Tolerations, p.Spec.Affinity.NodeAffinity.PreferredDuringSchedulingIgnoredDuringExecution)
	}
	exprs := p.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions
	if len(exprs) != 1+len(spotLabels) || !notSpot(exprs) {
		t.Errorf("an avoiding stream pod's node affinity %v, want the template's and not spot", exprs)
	}
}

// A PodTemplate in the pods' namespace serves as the template, and a missing
// one is named.
func TestPodTemplateFromTheAPI(t *testing.T) {
	file, err := ParseTemplate([]byte(template))
	if err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientset(&corev1.PodTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: "openetl-attempt", Namespace: "etl"},
		Template:   corev1.PodTemplateSpec{ObjectMeta: file.ObjectMeta, Spec: file.Spec},
	})
	p, err := PodTemplate(t.Context(), c, "etl", "openetl-attempt")
	if err != nil {
		t.Fatal(err)
	}
	if p.Labels["team"] != "data" || len(p.Spec.Containers) != 2 || p.Spec.Containers[1].Image != "ghcr.io/openetl-io/openetl:dev" {
		t.Errorf("template %+v", p)
	}
	if _, err := PodTemplate(t.Context(), c, "other", "openetl-attempt"); err == nil || !strings.Contains(err.Error(), "PodTemplate openetl-attempt in namespace other") {
		t.Errorf("err %v", err)
	}
}

func TestLabelValues(t *testing.T) {
	for in, want := range map[string]string{"default": "default", "a/b c": "a_b_c", "-x-": "x", strings.Repeat("a", 70): strings.Repeat("a", 63), "": ""} {
		if got := labelValue(in); got != want {
			t.Errorf("labelValue(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestLaunchErrors(t *testing.T) {
	k, c := launcher(t)
	if err := k.Launch(t.Context(), batch()); err != nil {
		t.Fatal(err)
	}
	if err := k.Launch(t.Context(), batch()); err != nil {
		t.Fatalf("launching again: %v, want nil", err)
	}
	gr := schema.GroupResource{Resource: "pods"}
	for _, tc := range []struct {
		err  error
		code errs.Code
	}{
		{apierrors.NewForbidden(gr, "x", errors.New(`exceeded quota: compute, requested: cpu=4`)), errs.Transient},
		{apierrors.NewForbidden(gr, "x", errors.New(`User "system:serviceaccount:etl:openetl" cannot create resource "pods"`)), errs.ConfigInvalid},
		{apierrors.NewInvalid(schema.GroupKind{Kind: "Pod"}, "x", nil), errs.ConfigInvalid},
		{apierrors.NewServiceUnavailable("down"), errs.Transient},
	} {
		c.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) { return true, nil, tc.err })
		l := batch()
		l.Placement++
		if err := k.Launch(t.Context(), l); errs.CodeOf(err) != tc.code {
			t.Errorf("%v: got %v (%s), want %s", tc.err, err, errs.CodeOf(err), tc.code)
		}
	}
}

func status(phase corev1.PodPhase, cs ...corev1.ContainerStatus) corev1.PodStatus {
	return corev1.PodStatus{Phase: phase, ContainerStatuses: cs}
}

// Pods says which ended and how, and which are stuck.
func TestPodsStates(t *testing.T) {
	k, c := launcher(t)
	add := func(attempt string, st corev1.PodStatus) {
		l := batch()
		l.Attempt = attempt
		p := k.Pod(l)
		p.Status = st
		if _, err := c.CoreV1().Pods("etl").Create(t.Context(), p, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	add("running", status(corev1.PodRunning))
	add("pulling", status(corev1.PodPending, corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "no such image"}}}))
	add("starting", status(corev1.PodPending, corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "ContainerCreating"}}}))
	add("oom", status(corev1.PodFailed, corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}}))
	add("crashed", status(corev1.PodFailed, corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "Error", ExitCode: 1, Message: "daemon: the runner's token isn't a runner token\n"}}}))
	add("evicted", corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted", Message: "The node was low on resource: ephemeral-storage."})
	add("done", status(corev1.PodSucceeded))
	// Someone else's pod with the labels, but none of ours.
	other := k.Pod(batch())
	other.Name, other.Annotations = "stray", nil
	if _, err := c.CoreV1().Pods("etl").Create(t.Context(), other, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	got, err := k.Pods(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	byAttempt := map[string]Pod{}
	for _, p := range got {
		byAttempt[p.Attempt] = p
	}
	if len(got) != 7 {
		t.Fatalf("got %d pods, want 7: %+v", len(got), got)
	}
	check := func(attempt string, ended bool, code errs.Code, has string) {
		t.Helper()
		p := byAttempt[attempt]
		if p.Ended != ended || p.Placement != 2 || p.Name != Name(attempt, 2) {
			t.Errorf("%s: %+v", attempt, p)
		}
		if ended && (errs.CodeOf(p.Failure) != code || !strings.Contains(p.Failure.Error(), has)) {
			t.Errorf("%s: failure %v, want %s with %q", attempt, p.Failure, code, has)
		}
	}
	check("running", false, 0, "")
	check("starting", false, 0, "")
	check("oom", true, errs.ResourceExhausted, "went over its memory limit, 4608Mi")
	check("crashed", true, errs.Transient, "exit code 1 (Error): daemon: the runner's token isn't a runner token")
	check("evicted", true, errs.Transient, "Evicted The node was low on resource")
	check("done", true, errs.Transient, "ended without the attempt's result")
	if byAttempt["pulling"].Stuck != "ImagePullBackOff: no such image" || byAttempt["starting"].Stuck != "" {
		t.Errorf("stuck: %q and %q", byAttempt["pulling"].Stuck, byAttempt["starting"].Stuck)
	}

	if err := k.Delete(t.Context(), byAttempt["done"]); err != nil {
		t.Fatal(err)
	}
	if err := k.Delete(t.Context(), byAttempt["done"]); err != nil {
		t.Fatalf("deleting again: %v, want nil", err)
	}
	if got, _ := k.Pods(t.Context()); len(got) != 6 {
		t.Errorf("%d pods after a delete, want 6", len(got))
	}
}

// A process gets the attempt in its environment, ends as it ends, and stops
// when it's deleted.
func TestProcesses(t *testing.T) {
	p := &Processes{Command: []string{"/bin/sh", "-c", `test "$OPENETL_ATTEMPT" = a/1 && test "$OPENETL_PLACEMENT" = 1 || exit 7; [ "$OPENETL_POOL" = big ] || exit 8; exec sleep 30`}, Pool: "big"}
	t.Cleanup(p.Close)
	l := Launch{Attempt: "a/1", Placement: 1, Kind: contract.RunSyncAttempt, Size: resources.S}
	if err := p.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	if err := p.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	pods, _ := p.Pods(t.Context())
	if len(pods) != 1 || pods[0].Ended || pods[0].Name != Name("a/1", 1) {
		t.Fatalf("pods %+v, want one running", pods)
	}
	if err := p.Delete(t.Context(), pods[0]); err != nil {
		t.Fatal(err)
	}
	if pods, _ := p.Pods(t.Context()); len(pods) != 0 {
		t.Fatalf("pods %+v after a delete", pods)
	}

	l.Attempt = "b/1"
	if err := p.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		pods, _ := p.Pods(t.Context())
		if len(pods) == 1 && pods[0].Ended {
			if errs.CodeOf(pods[0].Failure) != errs.Transient || !strings.Contains(pods[0].Failure.Error(), "exit status 7") {
				t.Errorf("failure %v", pods[0].Failure)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("pods %+v, want b's ended", pods)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func onWorkers() Launch {
	l := batch()
	l.Workers, l.Coordinator = 3, resources.XS
	return l
}

// An attempt on workers is a pod for each worker, of the attempt's size, and
// the coordinator's, of its own, which serves the attempt protocol at its
// pod's IP.
func TestWorkerPods(t *testing.T) {
	k, _ := launcher(t)
	l := onWorkers()
	co := k.Pod(l)
	c := &co.Spec.Containers[1]
	e := env(c)
	if co.Name != Name(l.Attempt, 2) || co.Annotations[annoWorker] != "" || e["OPENETL_WORKER"] != "" {
		t.Errorf("the coordinator's pod %q, annotations %v, env %v", co.Name, co.Annotations, e)
	}
	if e["OPENETL_ATTEMPT_LISTEN"] != ":7720" || e["OPENETL_ATTEMPT_ADVERTISE"] != "$(OPENETL_POD_IP)" || e["OPENETL_CAPACITY"] != resources.XS.Flag() {
		t.Errorf("the coordinator's env %v", e)
	}
	var podIP *corev1.EnvVar
	for i := range c.Env {
		if c.Env[i].Name == "OPENETL_POD_IP" {
			podIP = &c.Env[i]
		}
	}
	// $(OPENETL_POD_IP) expands only to a variable defined before it.
	if podIP == nil || podIP.ValueFrom.FieldRef.FieldPath != "status.podIP" || slices.IndexFunc(c.Env, func(v corev1.EnvVar) bool { return v.Name == "OPENETL_POD_IP" }) >
		slices.IndexFunc(c.Env, func(v corev1.EnvVar) bool { return v.Name == "OPENETL_ATTEMPT_ADVERTISE" }) {
		t.Errorf("the coordinator's pod IP %+v in %v", podIP, c.Env)
	}
	if !slices.ContainsFunc(c.Ports, func(p corev1.ContainerPort) bool { return p.Name == "attempt" && p.ContainerPort == AttemptPort }) {
		t.Errorf("the coordinator's ports %v", c.Ports)
	}
	if cpu := c.Resources.Limits.Cpu().MilliValue(); cpu != 1000 {
		t.Errorf("the coordinator's CPU %dm, want xs and the reserve", cpu)
	}
	for w := 1; w <= 3; w++ {
		p := k.WorkerPod(l, w)
		c := &p.Spec.Containers[1]
		e := env(c)
		if p.Name != WorkerName(l.Attempt, 2, w) || len(p.Name) > 63 || p.Annotations[annoWorker] != strconv.Itoa(w) || p.Annotations[annoAttempt] != l.Attempt {
			t.Errorf("worker %d's pod %q, annotations %v", w, p.Name, p.Annotations)
		}
		if e["OPENETL_WORKER"] != strconv.Itoa(w) || e["OPENETL_CAPACITY"] != resources.M.Flag() || e["OPENETL_ATTEMPT_LISTEN"] != "" || len(c.Ports) != 0 {
			t.Errorf("worker %d's env %v and ports %v", w, e, c.Ports)
		}
		if cpu := c.Resources.Limits.Cpu().MilliValue(); cpu != 2500 {
			t.Errorf("worker %d's CPU %dm, want m and the reserve", w, cpu)
		}
	}
}

// A launcher makes an attempt's pods all or none: one it can't make deletes
// those it made. Its workers' pods end and go with the attempt's.
func TestLaunchWorkersAllOrNone(t *testing.T) {
	k, c := launcher(t)
	l := onWorkers()
	if err := k.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	got, err := k.Pods(t.Context())
	if err != nil || len(got) != 4 {
		t.Fatalf("got %d pods (%v), want a coordinator and 3 workers", len(got), err)
	}
	for _, p := range got {
		if p.Attempt != l.Attempt || p.Placement != 2 || p.Name != WorkerName(l.Attempt, 2, p.Worker) {
			t.Errorf("pod %+v", p)
		}
	}

	l.Placement = 3
	made := 0
	c.PrependReactor("create", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		if made++; made == 3 {
			return true, nil, apierrors.NewServiceUnavailable("down")
		}
		return false, nil, nil
	})
	if err := k.Launch(t.Context(), l); errs.CodeOf(err) != errs.Transient {
		t.Fatalf("got %v, want a transient failure", err)
	}
	got, _ = k.Pods(t.Context())
	for _, p := range got {
		if p.Placement == 3 {
			t.Errorf("pod %s of a launch that failed is left", p.Name)
		}
	}

	// A pod of a launch that failed, still being deleted when the launch is
	// tried again, fails that launch too, which leaves no pod of it; once
	// it's gone, the next makes them all.
	l.Placement = 4
	terminating := k.WorkerPod(l, 2)
	terminating.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	terminating.Finalizers = []string{"test"}
	if _, err := c.CoreV1().Pods("etl").Create(t.Context(), terminating, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := k.Launch(t.Context(), l); errs.CodeOf(err) != errs.Transient || !strings.Contains(err.Error(), "still being deleted") {
		t.Fatalf("got %v, want a transient failure for the pod being deleted", err)
	}
	placement4 := func() []Pod {
		got, _ := k.Pods(t.Context())
		var out []Pod
		for _, p := range got {
			if p.Placement == 4 {
				out = append(out, p)
			}
		}
		return out
	}
	if left := placement4(); len(left) != 1 || left[0].Worker != 2 {
		t.Fatalf("pods %+v, want only the one being deleted", left)
	}
	if err := c.CoreV1().Pods("etl").Delete(t.Context(), terminating.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := k.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	if made := placement4(); len(made) != 4 {
		t.Fatalf("pods %+v, want the coordinator's and its 3 workers'", made)
	}
	// All of it with what the shipped launcher's Role allows on pods.
	allowed := launcherPodVerbs(t)
	for _, a := range c.Actions() {
		if a.GetResource().Resource == "pods" && !allowed[a.GetVerb()] {
			t.Errorf("the launcher would %s pods, which its Role doesn't allow (%v)", a.GetVerb(), allowed)
		}
	}

	// A worker's pod that ended well has no failure; one that failed has.
	for _, tc := range []struct {
		worker int
		st     corev1.PodStatus
	}{
		{1, status(corev1.PodSucceeded)},
		{2, status(corev1.PodFailed, corev1.ContainerStatus{Name: "runner", State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{Reason: "OOMKilled", ExitCode: 137}}})},
	} {
		p, err := c.CoreV1().Pods("etl").Get(t.Context(), WorkerName(l.Attempt, 2, tc.worker), metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		p.Status = tc.st
		if _, err := c.CoreV1().Pods("etl").UpdateStatus(t.Context(), p, metav1.UpdateOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	got, _ = k.Pods(t.Context())
	for _, p := range got {
		switch {
		case p.Placement != 2:
		case p.Worker == 1:
			if !p.Ended || p.Failure != nil {
				t.Errorf("worker 1 ended well: %+v", p)
			}
		case p.Worker == 2:
			if !p.Ended || errs.CodeOf(p.Failure) != errs.ResourceExhausted {
				t.Errorf("worker 2 went over its memory: %+v", p)
			}
		}
	}
}

// launcherPodVerbs are the verbs on pods of the Role that
// deploy/kubernetes gives the launcher.
func launcherPodVerbs(t *testing.T) map[string]bool {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "deploy", "kubernetes", "launcher-default.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	verbs := map[string]bool{}
	for _, doc := range strings.Split(string(b), "\n---\n") {
		var role struct {
			Kind  string
			Rules []struct{ Resources, Verbs []string }
		}
		if err := yaml.Unmarshal([]byte(doc), &role); err != nil {
			t.Fatal(err)
		}
		for _, r := range role.Rules {
			if role.Kind == "Role" && slices.Contains(r.Resources, "pods") {
				for _, v := range r.Verbs {
					verbs[v] = true
				}
			}
		}
	}
	if len(verbs) == 0 {
		t.Fatal("no Role in launcher-default.yaml gives verbs on pods")
	}
	return verbs
}

// The process launcher starts a process for each worker, with which one it
// is in its environment, and the attempt's own; if one can't start, it stops
// the others.
func TestProcessesForWorkers(t *testing.T) {
	dir := t.TempDir()
	p := &Processes{Command: []string{"/bin/sh", "-c", `echo "$OPENETL_CAPACITY" > "` + dir + `/w$OPENETL_WORKER"; exec sleep 30`}, Pool: "big"}
	t.Cleanup(p.Close)
	l := Launch{Attempt: "a/1", Placement: 1, Kind: contract.RunSyncAttempt, Size: resources.S, Workers: 2, Coordinator: resources.XS}
	if err := p.Launch(t.Context(), l); err != nil {
		t.Fatal(err)
	}
	pods, _ := p.Pods(t.Context())
	if len(pods) != 3 {
		t.Fatalf("pods %+v, want the attempt's and its 2 workers'", pods)
	}
	want := map[string]string{"w": resources.XS.Flag(), "w1": resources.S.Flag(), "w2": resources.S.Flag()}
	deadline := time.Now().Add(10 * time.Second)
	for name, size := range want {
		for {
			b, err := os.ReadFile(filepath.Join(dir, name))
			if err == nil && strings.TrimSpace(string(b)) == size {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %q (%v), want %s", name, b, err, size)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	for _, pod := range pods {
		if err := p.Delete(t.Context(), pod); err != nil {
			t.Fatal(err)
		}
	}

	broken := &Processes{Command: []string{filepath.Join(dir, "missing")}}
	t.Cleanup(broken.Close)
	if err := broken.Launch(t.Context(), l); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Fatalf("got %v, want the processes not to start", err)
	}
	if pods, _ := broken.Pods(t.Context()); len(pods) != 0 {
		t.Fatalf("pods %+v of a launch that failed", pods)
	}
}
