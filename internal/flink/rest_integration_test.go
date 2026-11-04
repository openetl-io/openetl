//go:build integration

package flink

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"gocloud.dev/blob"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
	"github.com/unsuitable001/openetl/internal/runner"
)

// Runs against the compose Flink session cluster and SeaweedFS (make
// deps-up DEPS="s3 flink-jobmanager flink-taskmanager"), with Flink's
// StateMachineExample, which runs until it's stopped.
func TestRESTCluster(t *testing.T) {
	flinkURL := cmp.Or(os.Getenv("OPENETL_TEST_FLINK_URL"), "http://127.0.0.1:8091")
	jar := exampleJar(t)
	sum := sha256.Sum256(jar)
	code := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jar) }))
	t.Cleanup(code.Close)
	store, bucket := testStore(t)

	settings := Settings{
		Org: "org", Pipeline: "pay" + strings.ToLower(rand.Text())[:6], Store: store,
		Cluster: &apiv1.FlinkClusterSpec{Target: &apiv1.FlinkClusterSpec_Rest{Rest: &apiv1.FlinkRest{Url: flinkURL}}},
		Transform: &apiv1.FlinkTransform{Code: &apiv1.FlinkCode{
			Jar: code.URL + "/StateMachineExample.jar", Sha256: hex.EncodeToString(sum[:]),
			MainClass: "org.apache.flink.streaming.examples.statemachine.StateMachineExample",
		}},
	}
	c, err := New(t.Context(), settings, Options{HTTP: code.Client(), Poll: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.CancelAll(context.Background())
		_ = c.Close()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Minute)
	defer cancel()
	deploy := func(gen uint64, restore string) runner.FlinkJob {
		t.Helper()
		j, err := c.Deploy(ctx, runner.FlinkDeploy{Generation: gen, RestoreFrom: restore, Bridge: "host.docker.internal:1", Fingerprint: "00", Token: "t", CheckpointInterval: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return j
	}
	completed := func(job string, n int) []runner.FlinkCheckpoint {
		t.Helper()
		for {
			cps, err := c.Completed(ctx, job)
			if err != nil {
				t.Fatal(err)
			}
			if len(cps) >= n {
				return cps
			}
			if err := wait(ctx, 500*time.Millisecond); err != nil {
				t.Fatalf("job %s completed %d checkpoints: %v", job, len(cps), err)
			}
		}
	}

	j1 := deploy(1, "")
	cps := completed(j1.ID(), 2)
	if len(cps) > 2 || !strings.HasPrefix(cps[0].Path, "s3://"+bucket+"/org/"+settings.Pipeline+"/_flink/checkpoints/"+j1.ID()+"/chk-") {
		t.Errorf("checkpoints = %v: two are retained, below the pipeline's _flink/", cps)
	}
	dir := c.SavepointDir("g1")
	if err := j1.Stop(ctx, dir); err != nil {
		t.Fatal(err)
	}
	if err := j1.Wait(ctx); err != nil {
		t.Errorf("a stopped job's wait: %v", err)
	}
	sp, err := c.Savepoint(ctx, dir)
	if err != nil || !strings.HasPrefix(sp, dir+"/savepoint-") {
		t.Fatalf("savepoint = %q, %v", sp, err)
	}

	j2 := deploy(2, sp)
	cps = completed(j2.ID(), 1)
	// A job restored from savepoint X numbers on from X+1.
	if cps[0].ID < 2 {
		t.Errorf("job 2's first checkpoint is %d, so it didn't restore", cps[0].ID)
	}
	if err := c.CancelAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := j2.Wait(ctx); errs.CodeOf(err) != errs.Transient {
		t.Errorf("a cancelled job's wait: %v", err)
	}
	// Retained on cancellation, so the next generation could restore.
	if cps, err := c.Completed(ctx, j2.ID()); err != nil || len(cps) == 0 {
		t.Errorf("job 2's checkpoints after its cancel: %v, %v", cps, err)
	}

	if err := c.Prune(ctx, []string{j2.ID()}); err != nil {
		t.Fatal(err)
	}
	if cps, _ := c.Completed(ctx, j1.ID()); len(cps) != 0 {
		t.Errorf("job 1's checkpoints weren't pruned: %v", cps)
	}
	if sp, _ := c.Savepoint(ctx, dir); sp != "" {
		t.Errorf("the savepoint wasn't pruned: %s", sp)
	}
}

// A job whose tasks keep failing fails for good once the restart strategy
// gives up, with the job's exception.
func TestRESTClusterJobFails(t *testing.T) {
	flinkURL := cmp.Or(os.Getenv("OPENETL_TEST_FLINK_URL"), "http://127.0.0.1:8091")
	jar := exampleJar(t)
	sum := sha256.Sum256(jar)
	code := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jar) }))
	t.Cleanup(code.Close)
	store, _ := testStore(t)
	c, err := New(t.Context(), Settings{
		Org: "org", Pipeline: "fails" + strings.ToLower(rand.Text())[:6], Store: store,
		Cluster: &apiv1.FlinkClusterSpec{Target: &apiv1.FlinkClusterSpec_Rest{Rest: &apiv1.FlinkRest{Url: flinkURL}}},
		Transform: &apiv1.FlinkTransform{
			Code: &apiv1.FlinkCode{Jar: code.URL + "/StateMachineExample.jar", Sha256: hex.EncodeToString(sum[:]), MainClass: "org.apache.flink.streaming.examples.statemachine.StateMachineExample"},
			// Keyed state in RocksDB, which can't make its directory.
			Conf: map[string]string{"state.backend.type": "rocksdb", "state.backend.rocksdb.localdir": "/proc/openetl"},
		},
	}, Options{HTTP: code.Client(), Poll: 200 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = c.CancelAll(context.Background())
		_ = c.Close()
	})
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	j, err := c.Deploy(ctx, runner.FlinkDeploy{Generation: 1, CheckpointInterval: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	var failure *runner.FlinkFailed
	if err := j.Wait(ctx); !errors.As(err, &failure) || !strings.Contains(failure.Message, "Exception") {
		t.Errorf("err = %v", err)
	}
}

// exampleJar is Flink's StateMachineExample, from the compose cluster's
// image.
func exampleJar(t *testing.T) []byte {
	t.Helper()
	if p := os.Getenv("OPENETL_TEST_FLINK_JAR"); p != "" {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	out := filepath.Join(t.TempDir(), "job.jar")
	cmd := exec.CommandContext(t.Context(), "docker", "compose", "-f", "../../deploy/compose/dev.yml", "cp", "flink-jobmanager:/opt/flink/examples/streaming/StateMachineExample.jar", out)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("copy the example job from the compose cluster (make deps-up): %v: %s", err, b)
	}
	b, err := os.ReadFile(out)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// testStore is a new bucket of the compose S3 store, deleted with what's
// in it when the test ends.
func testStore(t *testing.T) (store, bucket string) {
	t.Helper()
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	bucket = "openetl-test-flink-" + strings.ToLower(rand.Text())[:12]
	store = "s3://" + bucket + "?endpoint=" + url.QueryEscape(endpoint)
	u, _ := url.Parse(store)
	s3c, err := objectstore.S3Client(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s3c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket (is the S3 store running? make deps-up): %v", err)
	}
	t.Cleanup(func() { dropBucket(t, store, s3c, bucket) })
	return store, bucket
}

func dropBucket(t *testing.T, store string, s3c *s3.Client, bucket string) {
	ctx := context.Background()
	b, err := objectstore.Open(ctx, store)
	if err == nil {
		it := b.List(&blob.ListOptions{})
		for {
			obj, err := it.Next(ctx)
			if errors.Is(err, io.EOF) || err != nil {
				break
			}
			_ = b.Delete(ctx, obj.Key)
		}
		_ = b.Close()
	}
	if _, err := s3c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
		t.Logf("delete bucket %s: %v", bucket, err)
	}
}
