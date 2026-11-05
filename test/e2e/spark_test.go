//go:build e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// sparkEnv is the compose stack's Spark cluster, and openetl-spark's jars.
func sparkEnv(t *testing.T) (share, submit, lib, tests string) {
	t.Helper()
	share, submit, dir := os.Getenv("OPENETL_SPARK_SHARE"), os.Getenv("OPENETL_SPARK_SUBMIT"), os.Getenv("OPENETL_SPARK_LIB")
	if share == "" || submit == "" || dir == "" {
		t.Skip("set OPENETL_SPARK_SHARE, OPENETL_SPARK_SUBMIT and OPENETL_SPARK_LIB to run Spark steps (make e2e does, with the compose stack's Spark)")
	}
	jars, _ := filepath.Glob(filepath.Join(dir, "openetl-spark-*.jar"))
	for _, j := range jars {
		if strings.HasSuffix(j, "-tests.jar") {
			tests = j
		} else {
			lib = j
		}
	}
	if lib == "" || tests == "" {
		t.Skipf("no openetl-spark jars in %s: make spark-lib builds them", dir)
	}
	return share, submit, lib, tests
}

// codeInS3 puts a file in a new SeaweedFS bucket, as app with the file's
// extension, which tells spark-submit a Python file, and returns its s3://
// URI, which runners read with the bucket's endpoint, and its SHA-256.
func codeInS3(t *testing.T, path string) (string, string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bucket := unique("spark-code")
	endpoint := "?endpoint=" + url.QueryEscape(s3Endpoint())
	c, err := objectstore.S3Client(mustParse(t, "s3://"+bucket+endpoint))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })
	st, err := objectstore.Open(t.Context(), "s3://"+bucket+endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	key := "jobs/app" + filepath.Ext(path)
	if err := st.WriteAll(t.Context(), key, b, nil); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return "s3://" + bucket + "/" + key + endpoint, hex.EncodeToString(h[:])
}

// A sync runs its Spark step on the compose stack's standalone master after
// its data lands: the application reads the sync's context through
// openetl-spark. One that fails fails the sync with SPARK_FAILED, and a Spark
// cluster, or a secret the pipeline's sparkAccess names, can't be deleted
// while a pipeline uses it.
func TestSparkPostSync(t *testing.T) {
	share, submit, lib, tests := sparkEnv(t)
	name := unique("spark")
	dir := filepath.Join(share, name)
	// The Spark containers, whose user isn't ours, write the application's
	// output here.
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	startRunnerVia(t, env.temporal, env.apiURL, "--checkpoints", filepath.Join(dir, "store"), "--spark-submit", submit)
	libCopy := filepath.Join(dir, "openetl-spark.jar")
	b, err := os.ReadFile(lib)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(libCopy, b, 0o644); err != nil {
		t.Fatal(err)
	}
	code, digest := codeInS3(t, tests)
	out := filepath.Join(dir, "out.json")

	cluster := fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "SparkCluster", "metadata": {"name": %q},
		"spec": {"submit": {"master": "spark://spark-master:7077", "ui": "http://127.0.0.1:8090",
			"conf": {"spark.driver.memory": "512m", "spark.executor.memory": "512m"}}}}`, name)
	step := func(args ...string) []string {
		a, _ := json.Marshal(append([]string{"file://" + out}, args...))
		docs := pipelineDocs(name, `{"seed": 9, "count": 25}`, `{}`)
		docs[2] = strings.Replace(docs[2], `"delivery": {}`, fmt.Sprintf(`"delivery": {}, "postSync": [{"spark": {"cluster": %q,
			"code": {"jar": %q, "sha256": %q, "class": "io.openetl.spark.e2e.ContextCheck"}, "args": %s,
			"conf": {"spark.jars": %q}}}]`, name, code, digest, a, "file://"+libCopy), 1)
		return docs
	}
	res := apply(t, append([]string{cluster}, step()...)...)
	if res[0].GetKind() != "SparkCluster" || res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Fatalf("applied %v", res[0])
	}
	status, stats := waitSync(t, name, 4*time.Minute)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s %s", status, stats, lastSyncError(t, name))
	}
	sync := latestSync(t, name)
	syncID := sync.GetName()[strings.LastIndex(sync.GetName(), "/")+1:]
	var got struct {
		Sync    string
		Rows    int
		Counted int
	}
	b, err = os.ReadFile(out)
	if err != nil || json.Unmarshal(b, &got) != nil {
		t.Fatalf("the application wrote %q, %v", b, err)
	}
	want := 0
	for _, st := range sync.GetStreams() {
		want += int(st.GetRows())
	}
	if got.Sync != syncID || want < 25 || got.Rows != want || got.Counted != want {
		t.Errorf("the application read %+v; want sync %s with the %d rows its streams read", got, syncID, want)
	}
	if !strings.Contains(stats, `"Spark"`) || !strings.Contains(stats, `"Submission": "driver-`) || !strings.Contains(stats, `"FINISHED"`) {
		t.Errorf("stats %s: want the Spark step's result", stats)
	}
	log := wholeLog(t, sync.GetName())
	if logged(log, 1, 1, "the Spark application finished") != 1 || !fromComponent(log, "spark-submit") {
		t.Errorf("the log has no Spark step: %v", log)
	}

	clusters := apiv1connect.NewSparkClusterServiceClient(env.http, env.apiURL)
	clusterName, pipelineName := "workspaces/default/sparkClusters/"+name, "workspaces/default/pipelines/"+name
	c, err := clusters.GetSparkCluster(t.Context(), connect.NewRequest(&apiv1.GetSparkClusterRequest{Name: clusterName}))
	if err != nil {
		t.Fatal(err)
	}
	if u := c.Msg.GetSparkCluster().GetUsedBy(); len(u) != 1 || u[0] != pipelineName || c.Msg.GetSparkCluster().GetSpec().GetSubmit().GetMaster() != "spark://spark-master:7077" {
		t.Errorf("got %v", c.Msg.GetSparkCluster())
	}
	if _, err := clusters.DeleteSparkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteSparkClusterRequest{Name: clusterName})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("deleting a cluster in use: %v", err)
	}

	// A failing application fails the sync, after its data landed.
	apply(t, step("--fail")...)
	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 4*time.Minute, "the failing sync", func() bool {
		s := latestSync(t, name)
		return s.GetName() != sync.GetName() && s.GetState() == apiv1.SyncState_SYNC_STATE_FAILED
	})
	if msg := lastSyncError(t, name); !strings.Contains(msg, "post-sync step 1, Spark cluster "+name) || !strings.Contains(msg, contract.SparkFailed) || !strings.Contains(msg, "ended FAILED") {
		t.Errorf("error %q", msg)
	}

	// sparkAccess: an EMR role is refused when no cluster is EMR's; a secret
	// its keys name can't be deleted while the pipeline names it; and S3A
	// keys fail a sync whose runner's store isn't on S3.
	withAccess := func(access string) []string {
		d := step()
		d[2] = strings.Replace(d[2], `"postSync":`, `"sparkAccess": `+access+`, "postSync":`, 1)
		return d
	}
	if err := applyErr(t, withAccess(`{"emrRole": "arn:aws:iam::123456789012:role/openetl"}`)...); err == nil || !strings.Contains(err.Error(), "sparkAccess.emrRole is for EMR clusters") {
		t.Errorf("an EMR role without an EMR cluster: %v", err)
	}
	key := setSecret(t, "s3cret")
	apply(t, withAccess(fmt.Sprintf(`{"keys": {"accessKeyId": "AKIAEXAMPLE", "secretAccessKey": {"secret": %q}}}`, key))...)
	secrets := apiv1connect.NewSecretServiceClient(env.http, env.apiURL)
	deleteKey := func() error {
		_, err := secrets.DeleteSecret(t.Context(), connect.NewRequest(&apiv1.DeleteSecretRequest{Name: "workspaces/default/secrets/" + key}))
		return err
	}
	if err := deleteKey(); connect.CodeOf(err) != connect.CodeFailedPrecondition || !strings.Contains(err.Error(), pipelineName) {
		t.Errorf("deleting a secret the pipeline names: %v", err)
	}
	failed := latestSync(t, name)
	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 4*time.Minute, "the sync with S3A keys", func() bool {
		s := latestSync(t, name)
		return s.GetName() != failed.GetName() && s.GetState() == apiv1.SyncState_SYNC_STATE_FAILED
	})
	if msg := lastSyncError(t, name); !strings.Contains(msg, "CONFIG_INVALID") || !strings.Contains(msg, "sparkAccess.keys are S3A's") {
		t.Errorf("error %q", msg)
	}

	apply(t, pipelineDocs(name, `{"seed": 9, "count": 25}`, `{}`)[2])
	if _, err := clusters.DeleteSparkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteSparkClusterRequest{Name: clusterName})); err != nil {
		t.Errorf("deleting the cluster once unused: %v", err)
	}
	if err := deleteKey(); err != nil {
		t.Errorf("deleting the secret once unused: %v", err)
	}
}

// A sync's epochs go through the user's Spark transform on the compose
// stack's standalone master: every row lands once, through more than one
// epoch, with the column the transform adds; records keep the outputs apart
// from the inputs; and once the sync ends, nothing of its epochs is left in
// the pipeline's _spark/. A transform that throws fails the sync with
// SPARK_FAILED, which isn't retried.
func TestSparkTransform(t *testing.T) {
	share, submit, lib, tests := sparkEnv(t)
	name := unique("transform")
	dir := filepath.Join(share, name)
	if err := os.MkdirAll(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	store := filepath.Join(dir, "store")
	startRunnerVia(t, env.temporal, env.apiURL, "--checkpoints", store, "--spark-submit", submit)
	libCopy := filepath.Join(dir, "openetl-spark.jar")
	b, err := os.ReadFile(lib)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(libCopy, b, 0o644); err != nil {
		t.Fatal(err)
	}
	code, digest := codeInS3(t, tests)

	// The Spark containers' user isn't ours; with umask 000, the runner can
	// delete what they write.
	cluster := fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "SparkCluster", "metadata": {"name": %q},
		"spec": {"submit": {"master": "spark://spark-master:7077", "ui": "http://127.0.0.1:8090",
			"conf": {"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.hadoop.fs.permissions.umask-mode": "000"}}}}`, name)
	docs := func(conf string) []string {
		// The pause between batches lets barriers in between them, so the
		// sync takes several epochs however fast the runner is.
		d := pipelineDocs(name, `{"seed": 4, "count": 300, "batchSize": 50, "delay": "100ms"}`, `{"commit": {"maxBytes": "8KB"}}`)
		d[2] = strings.Replace(d[2], `"delivery":`, fmt.Sprintf(`"transform": {"spark": {"cluster": %q,
			"code": {"jar": %q, "sha256": %q, "class": "io.openetl.spark.e2e.EnrichCheck"},
			"conf": {"spark.jars": %q%s}}}, "delivery":`, name, code, digest, "file://"+libCopy, conf), 1)
		return d
	}
	apply(t, append([]string{cluster}, docs("")...)...)
	status, stats := waitSync(t, name, 6*time.Minute)
	if status != "succeeded" {
		t.Fatalf("sync %s: %s %s", status, stats, lastSyncError(t, name))
	}
	checkRows(t, schema(name), 300, "users", "purchases")
	users, purchases := pgx.Identifier{schema(name), "users"}.Sanitize(), pgx.Identifier{schema(name), "purchases"}.Sanitize()
	v, err := query[string](t, "SELECT count(*) FILTER (WHERE spark_epoch IS NULL OR spark_epoch < 1) || ' ' || count(DISTINCT spark_epoch) FROM (SELECT spark_epoch FROM "+
		users+" UNION ALL SELECT spark_epoch FROM "+purchases+") r")
	if err != nil {
		t.Fatal(err)
	}
	var missing, epochs int
	if _, err := fmt.Sscan(v, &missing, &epochs); err != nil || missing != 0 || epochs < 2 {
		t.Errorf("%d rows without spark_epoch, from %d epochs; want none, from more than one", missing, epochs)
	}
	log := wholeLog(t, latestSync(t, name).GetName())
	if logged(log, 0, 1, "submitted the Spark transform") != 1 || !fromComponent(log, "spark-submit") {
		t.Errorf("the log has no Spark transform: %v", log)
	}

	// Schema history keeps the outputs apart from the inputs of their names.
	pipelineName := "workspaces/default/pipelines/" + name
	eventually(t, time.Minute, "the outputs' schema history", func() bool {
		r, err := pipelines().ListStreamSchemas(t.Context(), connect.NewRequest(&apiv1.ListStreamSchemasRequest{Parent: pipelineName}))
		if err != nil {
			t.Fatal(err)
		}
		epochCol := map[bool]bool{}
		kinds := map[bool]int{}
		for _, s := range r.Msg.GetStreamSchemas() {
			kinds[s.GetOutput()]++
			for _, c := range s.GetColumns() {
				epochCol[s.GetOutput()] = epochCol[s.GetOutput()] || c.GetName() == "spark_epoch"
			}
		}
		return kinds[true] == 2 && kinds[false] == 2 && epochCol[true] && !epochCol[false]
	})

	heads, _ := filepath.Glob(filepath.Join(store, "*", "*", "_checkpoints", "HEAD"))
	if len(heads) != 1 {
		t.Fatalf("HEADs: %v", heads)
	}
	pipeDir := filepath.Dir(filepath.Dir(heads[0]))
	var head struct {
		Record struct{ Epoch, Generation uint64 }
	}
	if b, err := os.ReadFile(heads[0]); err != nil || json.Unmarshal(b, &head) != nil {
		t.Fatalf("HEAD: %s, %v", b, err)
	}
	var rec struct {
		Streams map[string]json.RawMessage
		Outputs map[string]struct{ Schema []byte }
	}
	recFile := filepath.Join(pipeDir, "_checkpoints", fmt.Sprintf("%020d-g%020d.json", head.Record.Epoch, head.Record.Generation))
	if b, err := os.ReadFile(recFile); err != nil || json.Unmarshal(b, &rec) != nil {
		t.Fatalf("HEAD's record: %v", err)
	}
	if len(rec.Streams) != 2 || len(rec.Outputs) != 2 || len(rec.Outputs["users"].Schema) == 0 {
		t.Errorf("HEAD's record keeps streams %v and outputs %v", rec.Streams, rec.Outputs)
	}
	left, _ := filepath.Glob(filepath.Join(pipeDir, "_spark", "[0-9]*", "[0-9]*"))
	end, _ := filepath.Glob(filepath.Join(pipeDir, "_spark", "[0-9]*", "_end"))
	if len(left) != 0 || len(end) != 1 {
		t.Errorf("_spark/ holds epochs %v, and %d _end", left, len(end))
	}

	apply(t, docs(`, "spark.e2e.fail": "true"`)...)
	first := latestSync(t, name)
	if _, err := pipelines().SyncPipeline(t.Context(), connect.NewRequest(&apiv1.SyncPipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 4*time.Minute, "the failing sync", func() bool {
		s := latestSync(t, name)
		return s.GetName() != first.GetName() && s.GetState() == apiv1.SyncState_SYNC_STATE_FAILED
	})
	if msg := lastSyncError(t, name); !strings.Contains(msg, contract.SparkFailed) || !strings.Contains(msg, "asked to fail") {
		t.Errorf("error %q", msg)
	}
	for _, e := range wholeLog(t, latestSync(t, name).GetName()) {
		if e.GetAttempt() > 1 {
			t.Errorf("the failed transform was retried: %v", e)
			break
		}
	}
}
