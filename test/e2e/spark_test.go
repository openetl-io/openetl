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

// codeInS3 puts a file in a new SeaweedFS bucket, and returns its s3:// URI,
// which runners read with the bucket's endpoint, and its SHA-256.
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
	st, err := objectstore.Open(t.Context(), "s3://"+bucket+endpoint)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	if err := st.WriteAll(t.Context(), "jobs/app.jar", b, nil); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(b)
	return "s3://" + bucket + "/jobs/app.jar" + endpoint, hex.EncodeToString(h[:])
}

// A sync runs its Spark step on the compose stack's standalone master after
// its data lands: the application reads the sync's context through
// openetl-spark. One that fails fails the sync with SPARK_FAILED, and a Spark
// cluster can't be deleted while a pipeline uses it.
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

	apply(t, pipelineDocs(name, `{"seed": 9, "count": 25}`, `{}`)[2])
	if _, err := clusters.DeleteSparkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteSparkClusterRequest{Name: clusterName})); err != nil {
		t.Errorf("deleting the cluster once unused: %v", err)
	}
}
