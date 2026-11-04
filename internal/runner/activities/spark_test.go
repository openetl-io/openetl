package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
)

type sparkAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler
	step *apiv1.SparkStep
}

func (f *sparkAPI) GetSyncSpec(context.Context, *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	conf, _ := structpb.NewStruct(map[string]any{"spark.executor.memory": "2g"})
	return connect.NewResponse(&dataplanev1.GetSyncSpecResponse{
		OrgId:       "org",
		Pipeline:    &apiv1.PipelineSpec{Destination: &apiv1.PipelineDestination{Namespace: "shop"}, PostSync: []*apiv1.PostSyncStep{{Step: &apiv1.PostSyncStep_Spark{Spark: f.step}}}},
		Destination: &apiv1.ConnectorConfig{Connector: "postgres"},
		SparkClusters: map[string]*dataplanev1.PinnedSparkCluster{"lab": {
			Version: "cv1",
			Spec:    &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "spark://m:7077", Conf: conf}}},
		}},
	}), nil
}

// fakeStandalone is a spark-submit for a standalone master. Each --status
// prints the next state in dir/states, one a line, and then the last one;
// --kill records the driver.
func fakeStandalone(t *testing.T, states ...string) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "spark-submit")
	script := `#!/bin/sh
d=$(dirname "$0")
echo "$*" >> "$d/calls"
case "$*" in
*--kill*) echo "$*" > "$d/killed"; exit 0;;
*--status*)
  s=$(head -n 1 "$d/states")
  [ "$(wc -l < "$d/states")" -gt 1 ] && tail -n +2 "$d/states" > "$d/states.next" && mv "$d/states.next" "$d/states"
  echo "INFO Client: State of driver-1 is $s"; exit 0;;
esac
echo "INFO Client: Driver successfully submitted as driver-1"
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "states"), []byte(strings.Join(states, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func submits(t *testing.T, dir string) int {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(dir, "calls"))
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.Contains(l, "--deploy-mode cluster") {
			n++
		}
	}
	return n
}

func TestRunSpark(t *testing.T) {
	jar := []byte("PK a jar")
	code := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jar) }))
	defer code.Close()
	step := &apiv1.SparkStep{Cluster: "lab", Code: &apiv1.SparkCode{File: &apiv1.SparkCode_Jar{Jar: code.URL + "/models.jar"}, Sha256: sum(jar), MainClass: "com.acme.Models"}}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(&sparkAPI{step: step}))
	api := httptest.NewServer(mux)
	defer api.Close()

	setup := func(t *testing.T, states ...string) (*Spark, string, string) {
		t.Helper()
		storage := t.TempDir()
		store, err := checkpoint.NewFSStore(storage)
		if err != nil {
			t.Fatal(err)
		}
		sess, err := checkpoint.NewLog(store, "org", "p").Claim(t.Context(), "a1")
		if err != nil {
			t.Fatal(err)
		}
		if err := sess.Commit(t.Context(), &checkpoint.Record{Epoch: 1, Sync: "s1", SyncStart: &checkpoint.RecordRef{}, SyncRows: map[string]uint64{"orders": 3}}); err != nil {
			t.Fatal(err)
		}
		bin, dir := fakeStandalone(t, states...)
		return &Spark{
			API: dataplanev1connect.NewDataPlaneServiceClient(api.Client(), api.URL), Checkpoints: store, Storage: storage,
			HTTP: code.Client(), SparkSubmit: bin, Logs: store, Poll: 10 * time.Millisecond,
		}, storage, dir
	}
	in := contract.SparkInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s1", StepNumber: 2,
		Step: contract.SparkStep{Cluster: "lab", ClusterVersion: "cv1", Jar: code.URL + "/models.jar", SHA256: sum(jar), MainClass: "com.acme.Models"}}
	run := func(a *Spark, env *testsuite.TestActivityEnvironment) (contract.SparkResult, error) {
		env.RegisterActivity(a.RunSpark)
		v, err := env.ExecuteActivity(a.RunSpark, in)
		var res contract.SparkResult
		if err == nil {
			err = v.Get(&res)
		}
		return res, err
	}
	var suite testsuite.WorkflowTestSuite

	t.Run("finished", func(t *testing.T) {
		a, storage, dir := setup(t, "SUBMITTED", "RUNNING", "FINISHED")
		res, err := run(a, suite.NewTestActivityEnvironment())
		if err != nil || res.Submission != "driver-1" || res.State != "FINISHED" {
			t.Fatalf("got %+v, %v", res, err)
		}
		calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
		codeURI := "file://" + filepath.ToSlash(storage) + "/org/p/_spark/code/" + sum(jar) + "/models.jar"
		ctxURI := "file://" + filepath.ToSlash(storage) + "/org/p/_spark/context/s1/2.json"
		if !strings.Contains(string(calls), "--conf spark.executor.memory=2g --conf spark.openetl.context="+ctxURI+" --class com.acme.Models "+codeURI) {
			t.Errorf("spark-submit got %s", calls)
		}
		var c struct {
			Sync    string
			Through checkpoint.RecordRef
			Streams []struct {
				Name string
				Rows uint64
			}
		}
		b, err := os.ReadFile(strings.TrimPrefix(ctxURI, "file://"))
		if err != nil || json.Unmarshal(b, &c) != nil || c.Sync != "s1" || c.Through.Epoch != 1 || len(c.Streams) != 1 || c.Streams[0].Rows != 3 {
			t.Errorf("context %s, %v", b, err)
		}
		if got, err := os.ReadFile(filepath.Join(storage, "org/p/_logs/s1/spark-2.json")); err != nil || string(got) != `{"kind":"standalone","id":"driver-1"}` {
			t.Errorf("submission %s, %v", got, err)
		}

		// A retried activity watches the application it finds, and
		// submits nothing.
		res, err = run(a, suite.NewTestActivityEnvironment())
		if err != nil || res.Submission != "driver-1" || submits(t, dir) != 1 {
			t.Errorf("retried: %+v, %v, %d submissions", res, err, submits(t, dir))
		}
	})

	t.Run("failed", func(t *testing.T) {
		a, _, _ := setup(t, "RUNNING", "FAILED")
		_, err := run(a, suite.NewTestActivityEnvironment())
		var app *temporal.ApplicationError
		if !asApplicationError(err, &app) || app.Type() != contract.SparkFailed || !app.NonRetryable() || !strings.Contains(app.Message(), "driver driver-1 ended FAILED") {
			t.Errorf("got %v", err)
		}
	})

	t.Run("out of time", func(t *testing.T) {
		a, _, dir := setup(t, "RUNNING")
		env := suite.NewTestActivityEnvironment()
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
		if _, err := run(a, env); err == nil {
			t.Fatal("it finished")
		}
		if b, err := os.ReadFile(filepath.Join(dir, "killed")); err != nil || !strings.Contains(string(b), "--kill driver-1") {
			t.Errorf("killed %q, %v", b, err)
		}
	})

	t.Run("the runner stops", func(t *testing.T) {
		a, _, dir := setup(t, "RUNNING")
		env := suite.NewTestActivityEnvironment()
		stop := make(chan struct{})
		close(stop)
		env.SetWorkerStopChannel(stop)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		env.SetWorkerOptions(worker.Options{BackgroundActivityContext: ctx})
		// The test environment reports its own timeout in place of the
		// activity's TRANSIENT.
		if _, err := run(a, env); err == nil {
			t.Fatal("it finished")
		}
		if _, err := os.Stat(filepath.Join(dir, "killed")); !os.IsNotExist(err) {
			t.Errorf("a stopping runner killed the application: %v", err)
		}
	})
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func asApplicationError(err error, target **temporal.ApplicationError) bool {
	return errors.As(err, target)
}
