package flink

import (
	"cmp"
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
)

// Kube is the part of the Kubernetes API the runner uses, in one namespace:
// FlinkDeployments, and a JobManager's REST API through the API server.
type Kube interface {
	Create(ctx context.Context, obj map[string]any) error
	// Get returns nil if there's no such FlinkDeployment.
	Get(ctx context.Context, name string) (map[string]any, error)
	List(ctx context.Context, labelSelector string) ([]map[string]any, error)
	// Delete asks for it to be deleted; a deployment that's gone is left.
	Delete(ctx context.Context, name string) error
	// Proxy reaches port of service through the API server.
	Proxy(service string, port int) (*url.URL, *http.Client, error)
}

const (
	labelPipeline   = "openetl.io/pipeline"
	labelGeneration = "openetl.io/generation"
)

var flinkDeployments = schema.GroupVersionResource{Group: "flink.apache.org", Version: "v1beta1", Resource: "flinkdeployments"}

type kubeAPI struct {
	ns  string
	cfg *restclient.Config
	dyn dynamic.ResourceInterface
}

// defaultKube opens the Kubernetes API with kubeconfig, or with the
// runner's own service account when it's nil. It never reads the runner's
// own kubeconfig files.
func defaultKube(_ context.Context, kubeconfig []byte, namespace string) (Kube, error) {
	var cfg *restclient.Config
	var err error
	if kubeconfig != nil {
		cfg, err = clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	} else {
		cfg, err = restclient.InClusterConfig()
	}
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "flink: the Kubernetes API, with the cluster's kubeconfig or this runner's service account")
	}
	d, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "flink: the Kubernetes API")
	}
	return &kubeAPI{ns: namespace, cfg: cfg, dyn: d.Resource(flinkDeployments).Namespace(namespace)}, nil
}

func kubeErr(err error, what string) error {
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) || apierrors.IsInvalid(err) || apierrors.IsNotFound(err) {
		return errs.Wrap(errs.ConfigInvalid, err, "flink: "+what)
	}
	return errs.Wrap(errs.Transient, err, "flink: "+what)
}

func (k *kubeAPI) Create(ctx context.Context, obj map[string]any) error {
	_, err := k.dyn.Create(ctx, &unstructured.Unstructured{Object: obj}, metav1.CreateOptions{})
	if err != nil {
		return kubeErr(err, "create a FlinkDeployment in namespace "+k.ns)
	}
	return nil
}

func (k *kubeAPI) Get(ctx context.Context, name string) (map[string]any, error) {
	u, err := k.dyn.Get(ctx, name, metav1.GetOptions{})
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, kubeErr(err, "read FlinkDeployment "+name)
	}
	return u.Object, nil
}

func (k *kubeAPI) List(ctx context.Context, selector string) ([]map[string]any, error) {
	l, err := k.dyn.List(ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return nil, kubeErr(err, "list FlinkDeployments in namespace "+k.ns)
	}
	out := make([]map[string]any, len(l.Items))
	for i, u := range l.Items {
		out[i] = u.Object
	}
	return out, nil
}

func (k *kubeAPI) Delete(ctx context.Context, name string) error {
	err := k.dyn.Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return kubeErr(err, "delete FlinkDeployment "+name)
	}
	return nil
}

func (k *kubeAPI) Proxy(service string, port int) (*url.URL, *http.Client, error) {
	c, err := restclient.HTTPClientFor(k.cfg)
	if err != nil {
		return nil, nil, errs.Wrap(errs.ConfigInvalid, err, "flink: the Kubernetes API")
	}
	u, err := url.Parse(k.cfg.Host)
	if err != nil {
		return nil, nil, errs.Wrap(errs.ConfigInvalid, err, "flink: the Kubernetes API's host")
	}
	return u.JoinPath("api/v1/namespaces", k.ns, "services", service+":"+strconv.Itoa(port), "proxy"), c, nil
}

// kubernetes runs each generation as a FlinkDeployment of the operator.
type kubernetes struct {
	c    *Cluster
	spec *apiv1.FlinkKubernetes
	kube Kube
}

func newKubernetes(ctx context.Context, spec *apiv1.FlinkKubernetes, c *Cluster, open func(context.Context, []byte, string) (Kube, error)) (*kubernetes, error) {
	if open == nil {
		open = defaultKube
	}
	var kubeconfig []byte
	if v := spec.GetKubeconfig(); v != nil {
		kubeconfig = []byte(v.GetStringValue())
	}
	k, err := open(ctx, kubeconfig, spec.GetNamespace())
	if err != nil {
		return nil, err
	}
	return &kubernetes{c: c, spec: spec, kube: k}, nil
}

// deploymentName is a generation's FlinkDeployment: short enough for
// Flink's cluster ID, which names its services.
func deploymentName(jobID string) string { return "openetl-" + jobID[:20] }

func resources(r *apiv1.FlinkResources) map[string]any {
	cpu := r.GetCpu()
	if cpu == 0 {
		cpu = 1
	}
	return map[string]any{"resource": map[string]any{"cpu": cpu, "memory": cmp.Or(r.GetMemory(), "2048m")}}
}

func (k *kubernetes) deploy(ctx context.Context, j *job) (runner.FlinkJob, error) {
	conf := map[string]any{}
	for key, v := range j.conf {
		conf[key] = v
	}
	conf["high-availability.type"] = "kubernetes"
	conf["high-availability.storageDir"] = k.c.objectURI("ha")
	conf["execution.shutdown-on-application-finish"] = "false"
	conf["kubernetes.operator.periodic.savepoint.interval"] = "0"
	conf["kubernetes.operator.job.restart.failed"] = "false"
	spec := map[string]any{
		"image":              cmp.Or(k.spec.GetImage(), "flink:1.20"),
		"flinkVersion":       "v1_20",
		"serviceAccount":     cmp.Or(k.spec.GetServiceAccount(), "flink"),
		"flinkConfiguration": conf,
		"jobManager":         resources(k.spec.GetJobManager()),
		"taskManager":        resources(k.spec.GetTaskManager()),
	}
	jobSpec := map[string]any{"jarURI": k.c.objectURI(j.jar), "args": toAny(j.args), "upgradeMode": "stateless", "state": "running"}
	if j.class != "" {
		jobSpec["entryClass"] = j.class
	}
	if j.parallelism > 0 {
		jobSpec["parallelism"] = int64(j.parallelism)
	}
	if j.restore != "" {
		jobSpec["initialSavepointPath"] = j.restore
	}
	spec["job"] = jobSpec
	name := deploymentName(j.id)
	obj := map[string]any{
		"apiVersion": "flink.apache.org/v1beta1",
		"kind":       "FlinkDeployment",
		"metadata": map[string]any{
			"name": name, "namespace": k.spec.GetNamespace(),
			"labels": map[string]any{labelPipeline: k.c.s.Pipeline, labelGeneration: strconv.FormatUint(j.gen, 10)},
		},
		"spec": spec,
	}
	if err := k.kube.Create(ctx, obj); err != nil {
		return nil, err
	}
	return &kubeJob{k: k, id: j.id, name: name}, nil
}

func toAny(s []string) []any {
	out := make([]any, len(s))
	for i, v := range s {
		out[i] = v
	}
	return out
}

func (k *kubernetes) cancelAll(ctx context.Context, _ string, _ []string) error {
	all, err := k.kube.List(ctx, labelPipeline+"="+k.c.s.Pipeline)
	if err != nil {
		return err
	}
	var names []string
	for _, d := range all {
		name, _, _ := unstructured.NestedString(d, "metadata", "name")
		if err := k.kube.Delete(ctx, name); err != nil {
			return err
		}
		names = append(names, name)
	}
	for _, name := range names {
		if err := k.gone(ctx, name); err != nil {
			return err
		}
	}
	return nil
}

// gone waits until the FlinkDeployment is deleted: the operator removes its
// finalizer once the job is cancelled and its pods and HA data are gone.
func (k *kubernetes) gone(ctx context.Context, name string) error {
	for {
		d, err := k.kube.Get(ctx, name)
		if err != nil || d == nil {
			return err
		}
		if err := wait(ctx, k.c.poll); err != nil {
			return err
		}
	}
}

type kubeJob struct {
	k        *kubernetes
	id, name string
}

func (j *kubeJob) ID() string { return j.id }

func (j *kubeJob) UI() string { return "" }

// status is the FlinkDeployment's job state, and its error; gone if it was
// deleted.
func (j *kubeJob) status(ctx context.Context) (state, deployment, message string, gone bool, err error) {
	d, err := j.k.kube.Get(ctx, j.name)
	if err != nil || d == nil {
		return "", "", "", d == nil, err
	}
	state, _, _ = unstructured.NestedString(d, "status", "jobStatus", "state")
	deployment, _, _ = unstructured.NestedString(d, "status", "jobManagerDeploymentStatus")
	message, _, _ = unstructured.NestedString(d, "status", "error")
	if id, _, _ := unstructured.NestedString(d, "status", "jobStatus", "jobId"); id != "" && id != j.id {
		return "", "", "", false, errs.New(errs.Internal, "flink: FlinkDeployment %s runs job %s, not %s; the operator didn't keep the job ID", j.name, id, j.id)
	}
	return state, deployment, operatorError(message), false, nil
}

// operatorError is the message in the operator's JSON status.error.
func operatorError(s string) string {
	var e struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	if json.Unmarshal([]byte(s), &e) == nil && e.Message != "" {
		return e.Message
	}
	return s
}

func (j *kubeJob) Wait(ctx context.Context) error {
	for {
		state, deployment, message, gone, err := j.status(ctx)
		switch {
		case err != nil:
			return err
		case gone:
			return errs.New(errs.Transient, "flink: FlinkDeployment %s was deleted", j.name)
		case state == "FINISHED":
			return nil
		case state == "FAILED":
			return failed(cmp.Or(message, "the job failed"))
		case state == "CANCELED":
			return errs.New(errs.Transient, "flink: job %s was cancelled", j.id)
		case deployment == "ERROR":
			return errs.New(errs.ConfigInvalid, "flink: FlinkDeployment %s: %s", j.name, cmp.Or(message, "the JobManager's deployment failed"))
		}
		if err := wait(ctx, j.k.c.poll); err != nil {
			return err
		}
	}
}

// rest reaches the JobManager's REST API through the API server.
func (j *kubeJob) rest() (*client, error) {
	u, c, err := j.k.kube.Proxy(j.name+"-rest", 8081)
	if err != nil {
		return nil, err
	}
	return &client{base: u, http: c}, nil
}

func (j *kubeJob) Stop(ctx context.Context, dir string) error {
	cl, err := j.rest()
	if err != nil {
		return err
	}
	if err := cl.stop(ctx, j.k.c, j.id, dir); err != nil {
		return err
	}
	for {
		state, _, _, gone, err := j.status(ctx)
		switch {
		case err != nil:
			return err
		case gone || terminal(state):
			return nil
		}
		if err := wait(ctx, j.k.c.poll); err != nil {
			return err
		}
	}
}

func (j *kubeJob) Cancel(ctx context.Context) error {
	if err := j.k.kube.Delete(ctx, j.name); err != nil {
		return err
	}
	return j.k.gone(ctx, j.name)
}
