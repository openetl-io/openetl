//go:build e2e && chaos

package e2e

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
)

// A transform's cluster and code, applied once for the suite's runs.
type transformSetup struct {
	cluster, code, digest string
	// Spark's: openetl-spark's jar where the cluster reads it, and the data
	// plane its runs go to.
	lib, dataPlane string
}

// setupFlink applies a FlinkCluster for compose's session cluster, with
// openetl-flink's jar as the runs' code. It returns nil without Flink.
func setupFlink(t *testing.T) *transformSetup {
	t.Helper()
	jars, _ := filepath.Glob(filepath.Join(os.Getenv("OPENETL_FLINK_LIB"), "openetl-flink-*.jar"))
	if len(jars) != 1 || !answers(flinkREST+"/overview") {
		t.Log("chaos: no Flink runs: set OPENETL_FLINK_LIB, make flink-lib, and start compose's Flink")
		return nil
	}
	f := &transformSetup{cluster: unique("chaos-flink")}
	f.code, f.digest = codeInS3(t, jars[0])
	apply(t, fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "FlinkCluster", "metadata": {"name": %q},
		"spec": {"rest": {"url": %q}}}`, f.cluster, flinkREST))
	return f
}

// setupSpark applies a SparkCluster for compose's standalone master, with
// openetl-spark's test transform as the runs' code, and creates the data
// plane Spark runs go to. Compose's Spark has no S3A, so its runners keep
// their checkpoints in a directory the cluster mounts, which no other run's
// runner may share. It returns nil without Spark.
func setupSpark(t *testing.T) *transformSetup {
	t.Helper()
	share, submit, dir := os.Getenv("OPENETL_SPARK_SHARE"), os.Getenv("OPENETL_SPARK_SUBMIT"), os.Getenv("OPENETL_SPARK_LIB")
	var lib, tests string
	jars, _ := filepath.Glob(filepath.Join(dir, "openetl-spark-*.jar"))
	for _, j := range jars {
		if strings.HasSuffix(j, "-tests.jar") {
			tests = j
		} else {
			lib = j
		}
	}
	if share == "" || submit == "" || lib == "" || tests == "" || !answers("http://127.0.0.1:8090") {
		t.Log("chaos: no Spark runs: set OPENETL_SPARK_SHARE, OPENETL_SPARK_SUBMIT and OPENETL_SPARK_LIB, make spark-lib, and start compose's Spark")
		return nil
	}
	s := &transformSetup{cluster: unique("chaos-spark"), dataPlane: unique("chaos-spark")}
	root := filepath.Join(share, s.cluster)
	if err := os.MkdirAll(root, 0o777); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(root, 0o777); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	b, err := os.ReadFile(lib)
	if err != nil {
		t.Fatal(err)
	}
	s.lib = filepath.Join(root, "openetl-spark.jar")
	if err := os.WriteFile(s.lib, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s.code, s.digest = codeInS3(t, tests)
	apply(t, fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "SparkCluster", "metadata": {"name": %q},
		"spec": {"submit": {"master": "spark://spark-master:7077", "ui": "http://127.0.0.1:8090",
			"conf": {"spark.driver.memory": "512m", "spark.executor.memory": "512m", "spark.hadoop.fs.permissions.umask-mode": "000"}}}}`, s.cluster))
	pgExec(t, "INSERT INTO data_planes (id, org_id) SELECT '"+s.dataPlane+"', id FROM orgs")
	return s
}

// startSparkPool starts the Spark data plane's runners.
func (s *chaosSuite) startSparkPool(ctx context.Context, t *testing.T, first int) *chaosPool {
	t.Helper()
	tok, err := apiv1connect.NewAccessServiceClient(env.http, env.apiURL).CreateRunnerToken(t.Context(),
		connect.NewRequest(&apiv1.CreateRunnerTokenRequest{Parent: "dataPlanes/" + s.spark.dataPlane}))
	if err != nil {
		t.Fatal(err)
	}
	return startPool(ctx, t, poolConfig{runners: 2, first: first, dir: filepath.Join(filepath.Dir(s.spark.lib), "store"),
		dataPlane: s.spark.dataPlane, token: tok.Msg.GetSecret(), sparkSubmit: os.Getenv("OPENETL_SPARK_SUBMIT")}, *chaosLogs, s.book)
}

func answers(u string) bool {
	res, err := http.Get(u)
	if err != nil {
		return false
	}
	_ = res.Body.Close()
	return true
}

// transformSpec is the run's pipeline's transform, and the data plane it
// goes to.
func (s *chaosSuite) transformSpec(c chaosConfig) (map[string]any, string) {
	switch c.transform {
	case "flink":
		return map[string]any{"flink": map[string]any{"cluster": s.flink.cluster, "parallelism": c.parallelism,
			"code": map[string]string{"jar": s.flink.code, "sha256": s.flink.digest, "class": "io.openetl.flink.examples.Passthrough"}}}, ""
	case "spark":
		return map[string]any{"spark": map[string]any{"cluster": s.spark.cluster, "conf": map[string]string{"spark.jars": "file://" + s.spark.lib},
			"code": map[string]string{"jar": s.spark.code, "sha256": s.spark.digest, "class": "io.openetl.spark.e2e.EnrichCheck"}}}, s.spark.dataPlane
	}
	return nil, ""
}

// composeContainer is the ID of the compose service's container, running
// or not.
func composeContainer(service string) string {
	out, err := exec.Command("docker", "ps", "-aq", "--filter", "label=com.docker.compose.service="+service).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

// killDriver kills the Spark driver of the pipeline's transform, whose
// command line names its application.
func killDriver(pipeline string) bool {
	id := composeContainer("spark-worker")
	return id != "" && exec.Command("docker", "exec", id, "pkill", "-9", "-f", "openetl-"+strings.ToLower(pipeline)+"-g").Run() == nil
}

// downFor kills a compose service's container, and starts it again after a
// pause.
func downFor(ctx context.Context, service string, rng *rand.Rand) {
	id := composeContainer(service)
	if out, err := exec.Command("docker", "kill", id).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: kill %s: %v\n%s", service, err, out)
		return
	}
	pause(ctx, 2*time.Second+upTo(rng, 8*time.Second))
	if out, err := exec.Command("docker", "start", id).CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "chaos: start %s again: %v\n%s", service, err, out)
	}
}
