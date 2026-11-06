//go:build e2e

package e2e

import (
	"archive/zip"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
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

// jar is the deployment's jarURI.
func (k *kube) jar(t *testing.T, deployment string) string {
	t.Helper()
	d, err := k.dyn.Resource(flinkDeployments).Namespace(k.namespace).Get(t.Context(), deployment, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	jar, _, _ := unstructured.NestedString(d.Object, "spec", "job", "jarURI")
	return jar
}

// withEntry copies the jar with one more entry, so it has another digest
// and the same classes.
func withEntry(t *testing.T, jar string) string {
	t.Helper()
	r, err := zip.OpenReader(jar)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	out := filepath.Join(t.TempDir(), filepath.Base(jar))
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	w := zip.NewWriter(f)
	for _, e := range r.File {
		// A jar needs no directory entries, and Copy refuses ones with a body.
		if strings.HasSuffix(e.Name, "/") {
			continue
		}
		if err := w.Copy(e); err != nil {
			t.Fatal(err)
		}
	}
	e, err := w.Create("openetl-e2e-upgrade.txt")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Write([]byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return out
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
// job from a savepoint into new code, a runner killed and another drained. A
// pause stops the job with a savepoint and deletes its deployment, and so
// does deleting the pipeline once its runner is dead.
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
	pipeline := func(code, digest string, parallelism int) []string {
		docs := continuousDocs(name, endlessFaker)
		docs[2] = strings.Replace(docs[2], `"delivery": {"commit": {"interval": "1s"}}`, fmt.Sprintf(`"delivery": {"commit": {"interval": "2s"}},
			"transform": {"flink": {"cluster": %q, "code": {"jar": %q, "sha256": %q, "class": "io.openetl.flink.examples.Passthrough"},
				"parallelism": %d}}`, name, code, digest, parallelism), 1)
		return docs
	}
	apply(t, append([]string{cluster}, pipeline(code, digest, 1)...)...)
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

	// New code stops the job with a savepoint, and the next generation
	// starts from it, rescaled.
	code2, digest2 := codeInS3(t, withEntry(t, lib))
	apply(t, pipeline(code2, digest2, 2)...)
	d = deployed("the upgraded job", 2, d)
	if jar := k.jar(t, d); !strings.Contains(jar, digest2) {
		t.Errorf("the upgraded job runs %s, not the code of digest %s", jar, digest2)
	}
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

	// A pipeline deleted while its runner is dead has no attempt to stop its
	// job; the deletion's teardown deletes the deployment, on the next
	// runner to come up.
	if _, err := pipelines().ResumePipeline(t.Context(), connect.NewRequest(&apiv1.ResumePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	deployed("the resumed job", 2, "")
	third.kill(t)
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	flinkRunnerFor(t, store, k.host)
	deleted := time.Now()
	eventually(t, 5*time.Minute, "the deleted pipeline's deployment deleted", func() bool { return len(k.deployments(t, id)) == 0 })
	t.Logf("the deleted pipeline's deployment went in %v", time.Since(deleted).Round(time.Second))
}

// pySparkOnKube makes dir below the share make kind-up mounts into Spark's
// pods, puts openetl_spark there, and returns a SparkCluster on kind's API
// server named name, with the conf its PySpark code needs.
func pySparkOnKube(t *testing.T, k *kube, name string) (dir, submit, cluster, pyFiles string) {
	t.Helper()
	share, submit := os.Getenv("OPENETL_SPARK_SHARE"), os.Getenv("OPENETL_E2E_KUBE_SPARK_SUBMIT")
	if share == "" || submit == "" {
		t.Skip("set OPENETL_SPARK_SHARE and OPENETL_E2E_KUBE_SPARK_SUBMIT to run Spark on Kubernetes (make e2e-kube does)")
	}
	dir = filepath.Join(share, name)
	// Spark's pods, whose user isn't ours, write here.
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	zipped := filepath.Join(dir, "openetl_spark.zip")
	zipDir(t, "../../libs/openetl-spark/python", "openetl_spark", zipped)
	// The pods run as this user, who owns what the runner keeps in the share:
	// its store's directories aren't open to others. The image's entrypoint
	// would switch root to its own user, and only that user writes its
	// working directory.
	template := filepath.Join(dir, "pod.yaml")
	pod := fmt.Sprintf("apiVersion: v1\nkind: Pod\nspec:\n  serviceAccountName: spark\n  securityContext:\n    runAsUser: %d\n    runAsGroup: %d\n"+
		"  containers:\n  - name: spark\n    workingDir: /tmp\n", os.Getuid(), os.Getgid())
	if err := os.WriteFile(template, []byte(pod), 0o644); err != nil {
		t.Fatal(err)
	}
	vol := func(pod string) string {
		p := "spark.kubernetes." + pod + "."
		return fmt.Sprintf(`%q: %q, %q: %q, %q: %q`, p+"volumes.hostPath.share.mount.path", share, p+"volumes.hostPath.share.options.path", share,
			p+"podTemplateFile", template)
	}
	cluster = fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "SparkCluster", "metadata": {"name": %q},
		"spec": {"submit": {"master": "k8s://https://openetl-control-plane:6443",
			"conf": {"spark.kubernetes.namespace": %q, "spark.kubernetes.container.image": "apache/spark:3.5.8-python3",
				"spark.kubernetes.authenticate.driver.serviceAccountName": "spark", "spark.kubernetes.file.upload.path": %q,
				"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.executor.instances": "1",
				"spark.kubernetes.driver.request.cores": "0.25", "spark.kubernetes.executor.request.cores": "0.25",
				"spark.kubernetes.driverEnv.HADOOP_USER_NAME": "openetl", "spark.executorEnv.HADOOP_USER_NAME": "openetl", "spark.jars.ivy": "/tmp/ivy",
				"spark.hadoop.fs.permissions.umask-mode": "000", %s, %s}}}}`,
		name, k.namespace, "file://"+filepath.Join(dir, "uploads"), vol("driver"), vol("executor"))
	return dir, submit, cluster, "local://" + zipped
}

// zipDir writes the package below root to out, as Python imports it.
func zipDir(t *testing.T, root, pkg, out string) {
	t.Helper()
	f, err := os.Create(out)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	w := zip.NewWriter(f)
	err = filepath.WalkDir(filepath.Join(root, pkg), func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".py" {
			return err
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		e, err := w.Create(filepath.ToSlash(rel))
		if err != nil {
			return err
		}
		_, err = e.Write(b)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

// PySpark on Spark on Kubernetes: a post-sync step that reads its sync's
// context, and a transform inside a sync that lands every row once.
func TestPySparkOnKubernetes(t *testing.T) {
	k := kubeEnv(t)
	name := unique("pyspark")
	dir, submit, cluster, pyFiles := pySparkOnKube(t, k, name)
	startRunnerVia(t, env.apiURL, "--checkpoints", filepath.Join(dir, "store"), "--spark-submit", submit)
	ctxCode, ctxDigest := codeInS3(t, "testdata/pyspark/context_check.py")
	enrichCode, enrichDigest := codeInS3(t, "testdata/pyspark/enrich_check.py")

	out := filepath.Join(dir, "out.json")
	post := pipelineDocs(name, `{"seed": 9, "count": 25}`, `{}`)
	post[2] = strings.Replace(post[2], `"delivery": {}`, fmt.Sprintf(`"delivery": {}, "postSync": [{"spark": {"cluster": %q,
		"code": {"python": %q, "sha256": %q}, "args": [%q], "conf": {"spark.submit.pyFiles": %q}}}]`,
		name, ctxCode, ctxDigest, "file://"+out, pyFiles), 1)
	apply(t, append([]string{cluster}, post...)...)
	status, stats := waitSync(t, name, 6*time.Minute)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s %s", status, stats, lastSyncError(t, name))
	}
	sync := latestSync(t, name)
	var got struct {
		Sync    string
		Rows    int
		Counted int
	}
	b, err := os.ReadFile(out)
	if err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("the application wrote %q, %v", b, err)
	}
	want := 0
	for _, st := range sync.GetStreams() {
		want += int(st.GetRows())
	}
	if syncID := sync.GetName()[strings.LastIndex(sync.GetName(), "/")+1:]; got.Sync != syncID || want < 25 || got.Rows != want || got.Counted != want {
		t.Errorf("the application read %+v; want sync %s with the %d rows its streams read", got, syncID, want)
	}

	transformed := name + "-t"
	tr := pipelineDocs(transformed, `{"seed": 4, "count": 300, "batchSize": 50, "delay": "100ms"}`, `{"commit": {"maxBytes": "8KB"}}`)
	tr[2] = strings.Replace(tr[2], `"delivery":`, fmt.Sprintf(`"transform": {"spark": {"cluster": %q,
		"code": {"python": %q, "sha256": %q}, "conf": {"spark.submit.pyFiles": %q}}}, "delivery":`, name, enrichCode, enrichDigest, pyFiles), 1)
	apply(t, tr...)
	status, stats = waitSync(t, transformed, 8*time.Minute)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s %s", status, stats, lastSyncError(t, transformed))
	}
	checkRows(t, schema(transformed), 300, "users", "purchases")
	users, purchases := pgx.Identifier{schema(transformed), "users"}.Sanitize(), pgx.Identifier{schema(transformed), "purchases"}.Sanitize()
	v, err := query[string](t, "SELECT count(*) FILTER (WHERE spark_epoch IS NULL OR spark_epoch < 1) || ' ' || count(DISTINCT spark_epoch) FROM (SELECT spark_epoch FROM "+
		users+" UNION ALL SELECT spark_epoch FROM "+purchases+") r")
	if err != nil {
		t.Fatal(err)
	}
	var missing, epochs int
	if _, err := fmt.Sscan(v, &missing, &epochs); err != nil || missing != 0 || epochs < 2 {
		t.Errorf("%d rows without spark_epoch, from %d epochs; want none, from more than one", missing, epochs)
	}
}
