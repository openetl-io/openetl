package activities

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner/worker"
	"github.com/unsuitable001/openetl/internal/spark"
)

type sparkAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler
	step *apiv1.SparkStep
	ui   func() string
}

func (f *sparkAPI) GetSyncSpec(context.Context, *connect.Request[dataplanev1.GetSyncSpecRequest]) (*connect.Response[dataplanev1.GetSyncSpecResponse], error) {
	conf, _ := structpb.NewStruct(map[string]any{"spark.executor.memory": "2g"})
	return connect.NewResponse(&dataplanev1.GetSyncSpecResponse{
		OrgId:       "org",
		Pipeline:    &apiv1.PipelineSpec{Destination: &apiv1.PipelineDestination{Namespace: "shop"}, PostSync: []*apiv1.PostSyncStep{{Step: &apiv1.PostSyncStep_Spark{Spark: f.step}}}},
		Destination: &apiv1.ConnectorConfig{Connector: "postgres"},
		SparkClusters: map[string]*dataplanev1.PinnedSparkCluster{"lab": {
			Version: "cv1",
			Spec:    &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "spark://m:7077", Conf: conf, Ui: f.ui()}}},
		}},
	}), nil
}

// fakeStandalone is a standalone master: a spark-submit that submits
// driver-1, and a UI that reports the next of states at each look, then the
// last one, and records a kill in dir/killed.
func fakeStandalone(t *testing.T, states ...string) (bin, dir, ui string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "spark-submit")
	script := `#!/bin/sh
echo "$*" >> "$(dirname "$0")/calls"
echo "INFO Client: Driver successfully submitted as driver-1"
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/json/":
			_, _ = fmt.Fprintf(w, `{"activedrivers": [{"id": "driver-1", "state": %q}]}`, states[0])
			if len(states) > 1 {
				states = states[1:]
			}
		case "/driver/kill/":
			_ = os.WriteFile(filepath.Join(dir, "killed"), []byte(r.FormValue("id")), 0o600)
		}
	}))
	t.Cleanup(srv.Close)
	return bin, dir, srv.URL
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
	var sparkUI string
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(&sparkAPI{step: step, ui: func() string { return sparkUI }}))
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
		// Records name the sync by its workflow's run.
		if err := sess.Commit(t.Context(), &checkpoint.Record{Epoch: 1, Sync: "run1", SyncStart: &checkpoint.RecordRef{}, SyncRows: map[string]uint64{"orders": 3}}); err != nil {
			t.Fatal(err)
		}
		bin, dir, ui := fakeStandalone(t, states...)
		sparkUI = ui
		return &Spark{
			API: dataplanev1connect.NewDataPlaneServiceClient(api.Client(), api.URL), Checkpoints: store, Storage: storage,
			HTTP: code.Client(), SparkSubmit: bin, Logs: store, Poll: 10 * time.Millisecond,
		}, storage, dir
	}
	in := contract.SparkInput{OrgID: "org", PipelineID: "p", Version: "v1", SyncID: "s1", RunKey: "run1", StepNumber: 2, Attempt: 1,
		Step: contract.SparkStep{Cluster: "lab", ClusterVersion: "cv1", Jar: code.URL + "/models.jar", SHA256: sum(jar), MainClass: "com.acme.Models"}}
	run := func(ctx context.Context, a *Spark) (contract.SparkResult, error) { return a.RunSpark(ctx, in) }

	t.Run("finished", func(t *testing.T) {
		a, storage, dir := setup(t, "SUBMITTED", "RUNNING", "FINISHED")
		res, err := run(t.Context(), a)
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

		// A retried step watches the application it finds, and submits
		// nothing.
		res, err = run(t.Context(), a)
		if err != nil || res.Submission != "driver-1" || submits(t, dir) != 1 {
			t.Errorf("retried: %+v, %v, %d submissions", res, err, submits(t, dir))
		}
	})

	t.Run("failed", func(t *testing.T) {
		a, _, _ := setup(t, "RUNNING", "FAILED")
		_, err := run(t.Context(), a)
		var f *contract.Failure
		if !errors.As(err, &f) || f.Code != contract.SparkFailed || !strings.Contains(f.Message, "driver driver-1 ended FAILED") {
			t.Errorf("got %v", err)
		}
	})

	t.Run("out of time", func(t *testing.T) {
		a, _, dir := setup(t, "RUNNING")
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if _, err := run(ctx, a); err == nil {
			t.Fatal("it finished")
		}
		if b, err := os.ReadFile(filepath.Join(dir, "killed")); err != nil || string(b) != "driver-1" {
			t.Errorf("killed %q, %v", b, err)
		}
	})

	t.Run("the runner stops", func(t *testing.T) {
		a, _, dir := setup(t, "RUNNING")
		stop := make(chan struct{})
		close(stop)
		ctx, cancel := context.WithTimeout(worker.NewContext(t.Context(), worker.NewWork(worker.Spec{ID: "w"}, stop)), time.Second)
		defer cancel()
		if _, err := run(ctx, a); errs.CodeOf(err) != errs.Transient {
			t.Fatalf("got %v, want TRANSIENT", err)
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

func TestGrant(t *testing.T) {
	keys := func() *structpb.Struct {
		s, _ := structpb.NewStruct(map[string]any{"accessKeyId": "AKIA", "secretAccessKey": "s3cret"})
		return s
	}
	spec := func(access *apiv1.SparkAccess, external ...*dataplanev1.ExternalSecret) *dataplanev1.GetSyncSpecResponse {
		return &dataplanev1.GetSyncSpecResponse{Pipeline: &apiv1.PipelineSpec{SparkAccess: access}, SparkAccessSecrets: external}
	}
	area, err := spark.Open(t.Context(), "s3://lake/base?endpoint=http://127.0.0.1:1", "org", "p")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = area.Close() }()
	a := &Spark{}
	role := "arn:aws:iam::123456789012:role/openetl-p"
	both := &apiv1.SparkAccess{EmrRole: role, Keys: keys()}

	app := spark.Application{Conf: map[string]string{"spark.hadoop.fs.s3a.bucket.lake.access.key": "the user's"}}
	if err := a.grant(t.Context(), spec(both), false, area, &app); err != nil || app.Role != "" ||
		app.Conf["spark.hadoop.fs.s3a.bucket.lake.access.key"] != "AKIA" || app.Conf["spark.hadoop.fs.s3a.bucket.lake.secret.key"] != "s3cret" {
		t.Errorf("S3A: %v, %+v", err, app)
	}
	app = spark.Application{Conf: map[string]string{}}
	if err := a.grant(t.Context(), spec(both), true, area, &app); err != nil || app.Role != role || len(app.Conf) != 0 {
		t.Errorf("EMR: %v, %+v", err, app)
	}
	app = spark.Application{Conf: map[string]string{}}
	if err := a.grant(t.Context(), spec(nil), false, area, &app); err != nil || len(app.Conf) != 0 {
		t.Errorf("no access: %v, %+v", err, app)
	}

	dir, err := spark.Open(t.Context(), t.TempDir(), "org", "p")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	if err := a.grant(t.Context(), spec(&apiv1.SparkAccess{Keys: keys()}), false, dir, &spark.Application{Conf: map[string]string{}}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("keys for a directory: %v", err)
	}
	// A secret in a cloud store, and a runner that can't read those.
	cloud := keys()
	cloud.Fields["secretAccessKey"] = structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"secret": structpb.NewStringValue("k")}})
	ext := &dataplanev1.ExternalSecret{Field: "secretAccessKey", Secret: "workspaces/default/secrets/k", Ref: "aws-sm://k"}
	if err := a.grant(t.Context(), spec(&apiv1.SparkAccess{Keys: cloud}, ext), false, area, &spark.Application{Conf: map[string]string{}}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("a cloud secret without a reader: %v", err)
	}
}
