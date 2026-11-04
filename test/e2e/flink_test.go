//go:build e2e

package e2e

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/platform/objectstore"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

// A Flink cluster is pinned by the continuous pipelines whose transforms
// name it, repins them when it changes, and can't be deleted while one
// uses it.
func TestFlinkClusterPinning(t *testing.T) {
	name := unique("flink")
	cluster := func(url string) string {
		return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "FlinkCluster", "metadata": {"name": %q},
			"spec": {"rest": {"url": %q}, "conf": {"taskmanager.memory.process.size": "1g"}}}`, name, url)
	}
	docs := continuousDocs(name, endlessFaker)
	docs[2] = strings.Replace(docs[2], `"delivery": {"commit": {"interval": "1s"}}`, fmt.Sprintf(`"delivery": {"commit": {"interval": "1s"}},
		"transform": {"flink": {"cluster": %q, "code": {"jar": "https://jobs.example/fraud.jar", "sha256": %q, "class": "com.acme.Fraud"},
			"parallelism": 2, "outputs": [{"name": "scored"}]}}`, name, strings.Repeat("ab", 32)), 1)
	res := apply(t, append([]string{cluster("http://127.0.0.1:8091")}, docs...)...)
	pipelineName, clusterName := "workspaces/default/pipelines/"+name, "workspaces/default/flinkClusters/"+name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName}))
	})
	if res[0].GetKind() != "FlinkCluster" || res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || res[3].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Fatalf("applied %v", res)
	}
	clusters := apiv1connect.NewFlinkClusterServiceClient(env.http, env.apiURL)
	c, err := clusters.GetFlinkCluster(t.Context(), connect.NewRequest(&apiv1.GetFlinkClusterRequest{Name: clusterName}))
	if err != nil {
		t.Fatal(err)
	}
	if u := c.Msg.GetFlinkCluster().GetUsedBy(); len(u) != 1 || u[0] != pipelineName || c.Msg.GetFlinkCluster().GetSpec().GetRest().GetUrl() != "http://127.0.0.1:8091" {
		t.Errorf("got %v", c.Msg.GetFlinkCluster())
	}
	version := func() *apiv1.PipelineVersion {
		t.Helper()
		v, err := pipelines().ListPipelineVersions(t.Context(), connect.NewRequest(&apiv1.ListPipelineVersionsRequest{Parent: pipelineName}))
		if err != nil {
			t.Fatal(err)
		}
		return v.Msg.GetPipelineVersions()[0]
	}
	first := version()
	if first.GetFlinkClusters()[name] != c.Msg.GetFlinkCluster().GetVersion() || first.GetSpec().GetTransform().GetFlink().GetParallelism() != 2 {
		t.Errorf("version %v pins %v", first.GetName(), first.GetFlinkClusters())
	}

	// Changing the cluster gives the pipeline a new version that pins it.
	changed := applyAll(t, false, cluster("http://127.0.0.1:8092"))
	if d := changed.GetDependents(); len(d) != 1 || d[0].GetName() != pipelineName || d[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Errorf("dependents = %v", d)
	}
	if v := version(); v.GetName() == first.GetName() || v.GetFlinkClusters()[name] == first.GetFlinkClusters()[name] {
		t.Errorf("the pipeline wasn't repinned: %v", v.GetFlinkClusters())
	}

	if _, err := clusters.DeleteFlinkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteFlinkClusterRequest{Name: clusterName})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("deleting a cluster in use: %v", err)
	}
	if _, err := pipelines().DeletePipeline(t.Context(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	if _, err := clusters.DeleteFlinkCluster(t.Context(), connect.NewRequest(&apiv1.DeleteFlinkClusterRequest{Name: clusterName})); err != nil {
		t.Errorf("deleting a cluster no pipeline uses: %v", err)
	}
}

const flinkREST = "http://127.0.0.1:8091"

// flinkEnv is openetl-flink's jar, once compose's Flink session cluster
// answers.
func flinkEnv(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("OPENETL_FLINK_LIB")
	if dir == "" {
		t.Skip("set OPENETL_FLINK_LIB to run Flink transforms (make e2e does, with the compose stack's Flink)")
	}
	jars, _ := filepath.Glob(filepath.Join(dir, "openetl-flink-*.jar"))
	if len(jars) != 1 {
		t.Skipf("no openetl-flink jar in %s: make flink-lib builds it", dir)
	}
	res, err := http.Get(flinkREST + "/overview")
	if err != nil {
		t.Skipf("compose's Flink isn't up: %v", err)
	}
	_ = res.Body.Close()
	return jars[0]
}

// flinkRunner starts a runner whose store is bucket, which Flink reaches
// too, and whose bridge the compose stack's containers reach.
func flinkRunner(t *testing.T, store string) *runner {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	host := cmp.Or(os.Getenv("OPENETL_TEST_BRIDGE_HOST"), "0.0.0.0")
	return startRunnerVia(t, env.temporal, env.apiURL, "--checkpoints", store,
		"--bridge-listen", fmt.Sprintf("%s:%d", host, port), "--bridge-advertise", fmt.Sprintf("host.docker.internal:%d", port))
}

// flinkJobs counts the session cluster's jobs by state.
func flinkJobs(t *testing.T) map[string]int {
	t.Helper()
	res, err := http.Get(flinkREST + "/jobs/overview")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = res.Body.Close() }()
	var o struct{ Jobs []struct{ State string } }
	if err := json.NewDecoder(res.Body).Decode(&o); err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for _, j := range o.Jobs {
		out[j.State]++
	}
	return out
}

// A continuous pipeline's rows go through the user's Flink job on the
// compose stack's session cluster, openetl-flink's pass-through, which
// writes each stream to the output of its name. Every row lands once; a
// second runner takes the job over when the first dies; and pausing the
// pipeline stops the job with a savepoint.
func TestFlinkTransform(t *testing.T) {
	lib := flinkEnv(t)
	name := unique("flinkjob")
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
	first := flinkRunner(t, store)

	cluster := fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "FlinkCluster", "metadata": {"name": %q},
		"spec": {"rest": {"url": %q}}}`, name, flinkREST)
	docs := continuousDocs(name, endlessFaker)
	docs[2] = strings.Replace(docs[2], `"delivery": {"commit": {"interval": "1s"}}`, fmt.Sprintf(`"delivery": {"commit": {"interval": "2s"}},
		"transform": {"flink": {"cluster": %q, "code": {"jar": %q, "sha256": %q, "class": "io.openetl.flink.examples.Passthrough"},
			"parallelism": 2}}`, name, code, digest), 1)
	apply(t, append([]string{cluster}, docs...)...)
	pipelineName := "workspaces/default/pipelines/" + name
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: pipelineName}))
	})

	eventually(t, 4*time.Minute, "rows through the Flink job", func() bool { return landed(t, name) >= 50 && committedEpochs(t, schema(name)) >= 3 })
	if n := flinkJobs(t)["RUNNING"]; n != 1 {
		t.Errorf("%d Flink jobs running, want the pipeline's", n)
	}

	// Another runner takes over: it cancels the job, and deploys it again
	// from the checkpoint the pipeline's records stand at.
	before := landed(t, name)
	first.kill(t)
	flinkRunner(t, store)
	eventually(t, 4*time.Minute, "rows after the takeover", func() bool { return landed(t, name) >= before+50 })

	if _, err := pipelines().PausePipeline(t.Context(), connect.NewRequest(&apiv1.PausePipelineRequest{Name: pipelineName})); err != nil {
		t.Fatal(err)
	}
	eventually(t, 3*time.Minute, "the job stopped with a savepoint", func() bool {
		jobs := flinkJobs(t)
		return jobs["RUNNING"] == 0 && jobs["FINISHED"] > 0
	})
	if jobs := flinkJobs(t); jobs["CANCELED"] == 0 {
		t.Errorf("Flink's jobs: %v; want the first runner's cancelled by the second", jobs)
	}
	landed(t, name)
}
