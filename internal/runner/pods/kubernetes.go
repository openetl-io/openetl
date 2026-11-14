package pods

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
)

// Labels and annotations on the pods a launcher makes.
const (
	labelManagedBy = "app.kubernetes.io/managed-by"
	labelComponent = "app.kubernetes.io/component"
	labelDataPlane = "openetl.io/data-plane"
	labelPool      = "openetl.io/pool"
	labelJob       = "openetl.io/job"
	labelPipeline  = "openetl.io/pipeline"
	annoAttempt    = "openetl.io/attempt"
	annoPlacement  = "openetl.io/placement"
	// Which of the attempt's workers the pod runs, from 1; none on the
	// attempt's own pod.
	annoWorker = "openetl.io/worker"
	// Karpenter doesn't move a pod with it to consolidate nodes.
	annoDoNotDisrupt = "karpenter.sh/do-not-disrupt"
)

// Node labels and taints of spot capacity on Karpenter, EKS, GKE and AKS.
// NotIn matches a node without the label, so stream pods still schedule where
// there's none.
var (
	spotLabels = []struct{ key, value string }{
		{"karpenter.sh/capacity-type", "spot"},
		{"eks.amazonaws.com/capacityType", "SPOT"},
		{"cloud.google.com/gke-spot", "true"},
		{"cloud.google.com/gke-preemptible", "true"},
		{"kubernetes.azure.com/scalesetpriority", "spot"},
	}
	spotTaints = []corev1.Toleration{
		{Key: "cloud.google.com/gke-spot", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
		{Key: "cloud.google.com/gke-preemptible", Operator: corev1.TolerationOpEqual, Value: "true", Effect: corev1.TaintEffectNoSchedule},
		{Key: "kubernetes.azure.com/scalesetpriority", Operator: corev1.TolerationOpEqual, Value: "spot", Effect: corev1.TaintEffectNoSchedule},
	}
)

// Kubernetes makes the pods in one namespace, from a template.
type Kubernetes struct {
	Client    kubernetes.Interface
	Namespace string
	// A pod whose container named runner, or else its first, runs openetld's
	// image with what a runner needs: the API's URL, the runner token, and the
	// checkpoint store. The launcher sets the rest.
	Template  *corev1.Pod
	DataPlane string
	Pool      string
	// What a pod has besides its attempt's size, for its runner.
	Reserve resources.Size
	// Its scratch, as ephemeral storage; 0 sets none.
	Scratch int64
	// GOARCH of the pods' nodes; empty: the launcher's.
	Arch string
	// Stream pods never go on spot nodes. Otherwise they only prefer not to,
	// so they still run where there's nothing else.
	StreamsAvoidSpot bool
}

// KubernetesOptions say where a Kubernetes launcher makes its pods.
type KubernetesOptions struct {
	// A kubeconfig file; empty: the launcher's service account. A launcher
	// never reads a kubeconfig it isn't given.
	Kubeconfig string
	// Empty: the kubeconfig context's, or the service account's.
	Namespace string
	// A file holding the pod template, in YAML, or podtemplate/NAME for a
	// PodTemplate in the pods' namespace.
	Template string
}

// OpenKubernetes is a Kubernetes launcher, but for the fields o doesn't set.
func OpenKubernetes(ctx context.Context, o KubernetesOptions) (*Kubernetes, error) {
	if o.Template == "" {
		return nil, errors.New("pods: the kubernetes launcher needs --pod-template, a pod whose container runs openetld's image")
	}
	var (
		tmpl *corev1.Pod
		err  error
	)
	name, fromAPI := strings.CutPrefix(o.Template, "podtemplate/")
	if !fromAPI {
		b, err := os.ReadFile(o.Template)
		if err != nil {
			return nil, fmt.Errorf("pods: --pod-template: %w", err)
		}
		if tmpl, err = ParseTemplate(b); err != nil {
			return nil, err
		}
	}
	var cfg *restclient.Config
	ns := o.Namespace
	if o.Kubeconfig != "" {
		loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(&clientcmd.ClientConfigLoadingRules{ExplicitPath: o.Kubeconfig}, &clientcmd.ConfigOverrides{})
		if cfg, err = loader.ClientConfig(); err != nil {
			return nil, fmt.Errorf("pods: --kubeconfig: %w", err)
		}
		if ns == "" {
			if ns, _, err = loader.Namespace(); err != nil {
				return nil, fmt.Errorf("pods: --kubeconfig's namespace: %w", err)
			}
		}
	} else {
		if cfg, err = restclient.InClusterConfig(); err != nil {
			return nil, fmt.Errorf("pods: the launcher's service account, outside a pod; --kubeconfig names a cluster: %w", err)
		}
		if ns == "" {
			b, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
			if err != nil {
				return nil, fmt.Errorf("pods: the launcher's namespace: %w", err)
			}
			ns = strings.TrimSpace(string(b))
		}
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("pods: the Kubernetes API: %w", err)
	}
	ns = cmp.Or(ns, "default")
	if fromAPI {
		if tmpl, err = PodTemplate(ctx, client, ns, name); err != nil {
			return nil, err
		}
	}
	return &Kubernetes{Client: client, Namespace: ns, Template: tmpl}, nil
}

// PodTemplate reads the template from a PodTemplate, which kustomize's images
// and patches reach as they do a Deployment's.
func PodTemplate(ctx context.Context, client kubernetes.Interface, namespace, name string) (*corev1.Pod, error) {
	pt, err := client.CoreV1().PodTemplates(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("pods: --pod-template: the PodTemplate %s in namespace %s: %w", name, namespace, err)
	}
	p := &corev1.Pod{ObjectMeta: pt.Template.ObjectMeta, Spec: pt.Template.Spec}
	return p, checkTemplate(p)
}

// ParseTemplate reads a pod template.
func ParseTemplate(b []byte) (*corev1.Pod, error) {
	var p corev1.Pod
	if err := yaml.UnmarshalStrict(b, &p); err != nil {
		return nil, fmt.Errorf("pods: the pod template: %w", err)
	}
	return &p, checkTemplate(&p)
}

func checkTemplate(p *corev1.Pod) error {
	if len(p.Spec.Containers) == 0 {
		return errors.New("pods: the pod template has no container to run openetld in")
	}
	return nil
}

// Launch makes l's pods: its workers', if it has any, and then its own. If
// one can't be made, it deletes those it made.
func (k *Kubernetes) Launch(ctx context.Context, l Launch) error {
	var made []string
	for _, worker := range l.pods() {
		p := k.WorkerPod(l, worker)
		_, err := k.Client.CoreV1().Pods(k.Namespace).Create(ctx, p, metav1.CreateOptions{})
		if err == nil || apierrors.IsAlreadyExists(err) {
			made = append(made, p.Name)
			continue
		}
		for _, name := range made {
			if derr := k.Client.CoreV1().Pods(k.Namespace).Delete(ctx, name, metav1.DeleteOptions{}); derr != nil && !apierrors.IsNotFound(derr) {
				err = errors.Join(err, derr)
			}
		}
		switch {
		case apierrors.IsInvalid(err), apierrors.IsBadRequest(err), apierrors.IsNotFound(err),
			apierrors.IsForbidden(err) && !strings.Contains(err.Error(), "exceeded quota"):
			return errs.Wrap(errs.ConfigInvalid, err, "pods: make the attempt's pod in namespace "+k.Namespace)
		}
		return errs.Wrap(errs.Transient, err, "pods: make the attempt's pod in namespace "+k.Namespace)
	}
	return nil
}

// Pod is l's own pod, whose runner runs the attempt, and coordinates its
// workers if it has any.
func (k *Kubernetes) Pod(l Launch) *corev1.Pod { return k.WorkerPod(l, 0) }

// WorkerPod is the pod of l's worker, counting from 1; 0 is l's own. A
// coordinator's serves its workers the attempt protocol on AttemptPort, at
// its pod's IP.
func (k *Kubernetes) WorkerPod(l Launch, worker int) *corev1.Pod {
	p := k.Template.DeepCopy()
	p.Name, p.GenerateName, p.Namespace = WorkerName(l.Attempt, l.Placement, worker), "", k.Namespace
	job := "batch"
	if l.Kind == contract.RunStreamJob {
		job = "stream"
	}
	p.Labels = with(p.Labels, map[string]string{
		labelManagedBy: "openetl", labelComponent: "attempt", labelDataPlane: labelValue(k.DataPlane),
		labelPool: labelValue(k.Pool), labelJob: job, labelPipeline: labelValue(l.Pipeline),
	})
	p.Annotations = with(p.Annotations, map[string]string{annoAttempt: l.Attempt, annoPlacement: strconv.Itoa(int(l.Placement))})
	if worker > 0 {
		p.Annotations[annoWorker] = strconv.Itoa(worker)
	}
	s := &p.Spec
	s.RestartPolicy = corev1.RestartPolicyNever
	grace := l.StopTimeout + 30
	s.TerminationGracePeriodSeconds = &grace
	s.NodeSelector = with(s.NodeSelector, map[string]string{corev1.LabelOSStable: "linux", corev1.LabelArchStable: cmp.Or(k.Arch, runtime.GOARCH)})
	switch {
	case job == "stream" && k.StreamsAvoidSpot:
		p.Annotations[annoDoNotDisrupt] = "true"
		avoidSpot(s)
	case job == "stream":
		p.Annotations[annoDoNotDisrupt] = "true"
		preferNoSpot(s)
		s.Tolerations = append(s.Tolerations, spotTaints...)
	default:
		s.Tolerations = append(s.Tolerations, spotTaints...)
	}

	c := &s.Containers[0]
	for i := range s.Containers {
		if s.Containers[i].Name == "runner" {
			c = &s.Containers[i]
		}
	}
	for _, v := range Env(l, k.Pool, worker) {
		name, value, _ := strings.Cut(v, "=")
		c.Env = append(c.Env, corev1.EnvVar{Name: name, Value: value})
	}
	size := l.Size
	if l.Workers > 0 && worker == 0 {
		size = l.Coordinator
		c.Env = append(c.Env,
			corev1.EnvVar{Name: "OPENETL_POD_IP", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"}}},
			corev1.EnvVar{Name: "OPENETL_ATTEMPT_LISTEN", Value: fmt.Sprintf(":%d", AttemptPort)},
			corev1.EnvVar{Name: "OPENETL_ATTEMPT_ADVERTISE", Value: "$(OPENETL_POD_IP)"},
		)
		c.Ports = append(c.Ports, corev1.ContainerPort{Name: "attempt", ContainerPort: AttemptPort, Protocol: corev1.ProtocolTCP})
	}
	size = size.Add(k.Reserve)
	amounts := corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(size.CPU, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(size.Memory, resource.BinarySI),
	}
	if k.Scratch > 0 {
		amounts[corev1.ResourceEphemeralStorage] = *resource.NewQuantity(k.Scratch, resource.BinarySI)
	}
	c.Resources.Requests, c.Resources.Limits = amounts, amounts.DeepCopy()
	// A pod that fails says why in its status, from its log's end.
	c.TerminationMessagePolicy = corev1.TerminationMessageFallbackToLogsOnError
	return p
}

func notSpot() []corev1.NodeSelectorRequirement {
	var exprs []corev1.NodeSelectorRequirement
	for _, l := range spotLabels {
		exprs = append(exprs, corev1.NodeSelectorRequirement{Key: l.key, Operator: corev1.NodeSelectorOpNotIn, Values: []string{l.value}})
	}
	return exprs
}

// preferNoSpot has a pod prefer nodes that aren't spot. Karpenter asks for
// such a node first, and relaxes the preference only when its node pools
// can't give one.
func preferNoSpot(s *corev1.PodSpec) {
	if s.Affinity == nil {
		s.Affinity = &corev1.Affinity{}
	}
	if s.Affinity.NodeAffinity == nil {
		s.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := s.Affinity.NodeAffinity
	na.PreferredDuringSchedulingIgnoredDuringExecution = append(na.PreferredDuringSchedulingIgnoredDuringExecution,
		corev1.PreferredSchedulingTerm{Weight: 100, Preference: corev1.NodeSelectorTerm{MatchExpressions: notSpot()}})
}

// avoidSpot keeps a pod off spot nodes, adding to each of the template's node
// selector terms, which are ORed.
func avoidSpot(s *corev1.PodSpec) {
	exprs := notSpot()
	if s.Affinity == nil {
		s.Affinity = &corev1.Affinity{}
	}
	if s.Affinity.NodeAffinity == nil {
		s.Affinity.NodeAffinity = &corev1.NodeAffinity{}
	}
	na := s.Affinity.NodeAffinity
	if na.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		na.RequiredDuringSchedulingIgnoredDuringExecution = &corev1.NodeSelector{}
	}
	terms := &na.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if len(*terms) == 0 {
		*terms = append(*terms, corev1.NodeSelectorTerm{})
	}
	for i := range *terms {
		(*terms)[i].MatchExpressions = append((*terms)[i].MatchExpressions, exprs...)
	}
}

func with(m, add map[string]string) map[string]string {
	if m == nil {
		m = map[string]string{}
	}
	for k, v := range add {
		m[k] = v
	}
	return m
}

// labelValue is v as a label value can be: 63 characters at most, of letters,
// digits, '-', '_' and '.', starting and ending with a letter or digit.
func labelValue(v string) string {
	b := []byte(v)
	for i, c := range b {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '-' && c != '_' && c != '.' {
			b[i] = '_'
		}
	}
	return strings.Trim(string(b[:min(len(b), 63)]), "-_.")
}

func (k *Kubernetes) selector() string {
	return fmt.Sprintf("%s=openetl,%s=attempt,%s=%s", labelManagedBy, labelComponent, labelDataPlane, labelValue(k.DataPlane))
}

func (k *Kubernetes) Pods(ctx context.Context) ([]Pod, error) {
	l, err := k.Client.CoreV1().Pods(k.Namespace).List(ctx, metav1.ListOptions{LabelSelector: k.selector()})
	if err != nil {
		return nil, errs.Wrap(errs.Transient, err, "pods: list the attempts' pods in namespace "+k.Namespace)
	}
	out := make([]Pod, 0, len(l.Items))
	for i := range l.Items {
		p := &l.Items[i]
		n, err := strconv.ParseInt(p.Annotations[annoPlacement], 10, 32)
		if err != nil || p.Annotations[annoAttempt] == "" {
			continue
		}
		pod := Pod{Name: p.Name, Attempt: p.Annotations[annoAttempt], Placement: int32(n)}
		if w, err := strconv.Atoi(p.Annotations[annoWorker]); err == nil && w > 0 {
			pod.Worker = w
		}
		switch phase := p.Status.Phase; {
		case phase == corev1.PodSucceeded && pod.Worker > 0:
			pod.Ended = true
		case phase == corev1.PodSucceeded:
			pod.Ended, pod.Failure = true, errs.New(errs.Transient, "its pod %s ended without the attempt's result", p.Name)
		case phase == corev1.PodFailed:
			pod.Ended, pod.Failure = true, failure(p)
		default:
			pod.Stuck = stuck(p)
		}
		out = append(out, pod)
	}
	return out, nil
}

// failure is why a pod failed.
func failure(p *corev1.Pod) error {
	for _, c := range p.Status.ContainerStatuses {
		t := c.State.Terminated
		if t == nil {
			continue
		}
		if t.Reason == "OOMKilled" {
			return errs.New(errs.ResourceExhausted, "its pod %s went over its memory limit, %s, and was killed", p.Name, memoryLimit(p, c.Name))
		}
		if t.ExitCode != 0 {
			msg := strings.TrimSpace(t.Message)
			if msg != "" {
				msg = ": " + msg
			}
			return errs.New(errs.Transient, "its pod %s failed with exit code %d (%s)%s", p.Name, t.ExitCode, t.Reason, msg)
		}
	}
	return errs.New(errs.Transient, "its pod %s failed: %s", p.Name, strings.TrimSpace(p.Status.Reason+" "+p.Status.Message))
}

func memoryLimit(p *corev1.Pod, container string) string {
	for _, c := range p.Spec.Containers {
		if c.Name == container {
			return c.Resources.Limits.Memory().String()
		}
	}
	return "?"
}

// stuck is why a pod hasn't started, if something stops it rather than
// slows it: a node that's coming is no reason.
func stuck(p *corev1.Pod) string {
	for _, c := range p.Status.ContainerStatuses {
		if w := c.State.Waiting; w != nil && w.Reason != "ContainerCreating" && w.Reason != "PodInitializing" {
			return strings.TrimSpace(w.Reason + ": " + w.Message)
		}
	}
	return ""
}

func (k *Kubernetes) Delete(ctx context.Context, p Pod) error {
	err := k.Client.CoreV1().Pods(k.Namespace).Delete(ctx, p.Name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return errs.Wrap(errs.Transient, err, "pods: delete pod "+p.Name)
	}
	return nil
}
