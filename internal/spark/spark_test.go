package spark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func TestStageCode(t *testing.T) {
	jar := []byte("PK fake jar")
	fetches := 0
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fetches++
		_, _ = w.Write(jar)
	}))
	defer srv.Close()
	store := t.TempDir()
	a, err := Open(t.Context(), store, "org", "p")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	uri := srv.URL + "/jobs/models-1.0.jar?token=x"
	if _, err := a.StageCode(t.Context(), uri, strings.Repeat("0", 64), false, srv.Client()); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), sum(jar)) || strings.Contains(err.Error(), "token") {
		t.Fatalf("a wrong digest: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store, "org/p/_spark", CodeKey(strings.Repeat("0", 64), uri, false))); !os.IsNotExist(err) {
		t.Errorf("a copy that didn't match was kept: %v", err)
	}
	key, err := a.StageCode(t.Context(), uri, sum(jar), false, srv.Client())
	if err != nil {
		t.Fatal(err)
	}
	if key != "code/"+sum(jar)+"/models-1.0.jar" {
		t.Errorf("key %q", key)
	}
	got, err := os.ReadFile(filepath.Join(store, "org/p/_spark", key))
	if err != nil || string(got) != string(jar) {
		t.Fatalf("the copy holds %q, %v", got, err)
	}
	if _, err := a.StageCode(t.Context(), uri, sum(jar), false, srv.Client()); err != nil || fetches != 2 {
		t.Errorf("staging it again: %v, %d fetches", err, fetches)
	}
	if want := "file://" + filepath.ToSlash(store) + "/org/p/_spark/" + key; a.URI(key, false) != want {
		t.Errorf("URI %q, want %q", a.URI(key, false), want)
	}
	other, err := a.WriteContext(t.Context(), "s1", 2, []byte(`{}`))
	if err != nil || other != "context/s1/2.json" {
		t.Fatalf("context %q, %v", other, err)
	}
	stale := "code/" + strings.Repeat("1", 64) + "/old.jar"
	if err := os.MkdirAll(filepath.Dir(filepath.Join(store, "org/p/_spark", stale)), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(store, "org/p/_spark", stale), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := a.DropCode(t.Context(), map[string]bool{key: true}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(store, "org/p/_spark", stale)); !os.IsNotExist(err) {
		t.Errorf("the old copy is still there: %v", err)
	}
	if _, err := os.Stat(filepath.Join(store, "org/p/_spark", key)); err != nil {
		t.Errorf("the pinned copy is gone: %v", err)
	}
	if _, err := a.StageCode(t.Context(), "hdfs:///jobs/a.jar", sum(jar), false, srv.Client()); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("hdfs: %v", err)
	}
	// spark-submit runs a .py file as Python, and anything else as a jar.
	if py, err := a.StageCode(t.Context(), srv.URL+"/jobs/enrich?token=x", sum(jar), true, srv.Client()); err != nil || py != "code/"+sum(jar)+"/enrich.py" {
		t.Errorf("a Python file's copy is %q, %v", py, err)
	}
}

func TestURIs(t *testing.T) {
	a, err := Open(t.Context(), "s3://lake/base?region=eu-west-1&endpoint=http://127.0.0.1:1", "org", "p")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if got := a.URI("code/x/a.jar", false); got != "s3a://lake/base/org/p/_spark/code/x/a.jar" {
		t.Errorf("S3A: %s", got)
	}
	if got := a.URI("code/x/a.jar", true); got != "s3://lake/base/org/p/_spark/code/x/a.jar" {
		t.Errorf("EMRFS: %s", got)
	}
	if _, err := Open(t.Context(), "gs://lake", "org", "p"); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("GCS: %v", err)
	}
	// Keys for the bucket alone, so the cluster's identity serves the rest.
	keys, err := a.S3AKeys("AKIA", "s3cret")
	if err != nil || len(keys) != 3 || keys["spark.hadoop.fs.s3a.bucket.lake.access.key"] != "AKIA" || keys["spark.hadoop.fs.s3a.bucket.lake.secret.key"] != "s3cret" ||
		keys["spark.hadoop.fs.s3a.bucket.lake.aws.credentials.provider"] != "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" {
		t.Errorf("S3A keys: %v, %v", keys, err)
	}
	dir, err := Open(t.Context(), t.TempDir(), "org", "p")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dir.Close() }()
	if _, err := dir.S3AKeys("AKIA", "s3cret"); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("S3A keys for a directory: %v", err)
	}
}

func TestBuildContext(t *testing.T) {
	ctx := t.Context()
	store := checkpoint.NewMemStore()
	log := checkpoint.NewLog(store, "org", "p")
	dst := Destination{Connector: "postgres", Namespace: "shop"}
	c, err := BuildContext(ctx, log, "p", "s1", "r1", dst)
	if err != nil || *c.After != *c.Through || c.Through.Epoch != 0 || len(c.Streams) != 0 {
		t.Fatalf("before any record: %+v, %v", c, err)
	}
	sess, err := log.Claim(ctx, "a1")
	if err != nil {
		t.Fatal(err)
	}
	prev := checkpoint.RecordRef{Epoch: 3, Generation: 1}
	rec := &checkpoint.Record{Epoch: 5, Sync: "r1", SyncStart: &prev, SyncRows: map[string]uint64{"orders": 7, "customers": 2},
		Streams: map[string]checkpoint.StreamState{"orders": {Schema: orderSchema(t)}, "customers": {}}}
	if err := sess.Commit(ctx, rec); err != nil {
		t.Fatal(err)
	}
	c, err = BuildContext(ctx, log, "p", "s1", "r1", dst)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := c.JSON()
	var got map[string]any
	_ = json.Unmarshal(b, &got)
	want := `{"after":{"epoch":3,"generation":1},"destination":{"connector":"postgres","namespace":"shop"},"pipeline":"p","streams":[{"name":"customers","rows":2},{"name":"orders","rows":7,"table":"shop_orders"}],"sync":"s1","through":{"epoch":5,"generation":1},"version":1}`
	if g, _ := json.Marshal(got); string(g) != want {
		t.Errorf("context\n %s\nwant\n %s", g, want)
	}
	c, err = BuildContext(ctx, log, "p", "s2", "r2", dst)
	if err != nil || *c.After != *c.Through || c.Through.Epoch != 5 || len(c.Streams) != 0 {
		t.Errorf("a sync that committed nothing: %+v, %v", c, err)
	}
}

// fakeSubmit writes a spark-submit that appends its arguments to calls and
// prints out-{verb} from dir, then exits with exit-{verb}'s code, if there's
// one. The verbs are submit, status and kill.
func fakeSubmit(t *testing.T) (bin, dir string) {
	t.Helper()
	dir = t.TempDir()
	bin = filepath.Join(dir, "spark-submit")
	script := `#!/bin/sh
d=$(dirname "$0")
echo "$*" >> "$d/calls"
verb=submit
for a in "$@"; do
  case "$a" in --status) verb=status;; --kill) verb=kill;; esac
done
[ -f "$d/out-$verb" ] && cat "$d/out-$verb"
[ -f "$d/exit-$verb" ] && exit "$(cat "$d/exit-$verb")"
exit 0
`
	if err := os.WriteFile(bin, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	return bin, dir
}

func say(t *testing.T, dir, verb, out string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "out-"+verb), []byte(out), 0o600); err != nil {
		t.Fatal(err)
	}
}

func calls(t *testing.T, dir string) []string {
	t.Helper()
	b, _ := os.ReadFile(filepath.Join(dir, "calls"))
	return strings.Split(strings.TrimSpace(string(b)), "\n")
}

var app = Application{Name: "openetl-s1-2", Code: "file:///data/org/p/_spark/code/x/a.jar", MainClass: "com.acme.Models", Args: []string{"--full"}, Conf: map[string]string{"spark.executor.memory": "2g"}}

// fakeMaster is a standalone master's UI with one driver, in state.
func fakeMaster(t *testing.T, state *string, killed *string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/json/" && *state == "":
			_, _ = w.Write([]byte(`{"activedrivers": [], "completeddrivers": []}`))
		case r.URL.Path == "/json/":
			_, _ = fmt.Fprintf(w, `{"activedrivers": [], "completeddrivers": [{"id": "driver-0", "state": "FINISHED"}, {"id": "driver-1", "state": %q}]}`, *state)
		case r.URL.Path == "/driver/kill/" && r.Method == http.MethodPost && r.FormValue("terminate") == "true":
			*killed = r.FormValue("id")
			http.Redirect(w, r, "/", http.StatusFound)
		case r.URL.Path == "/":
			_, _ = w.Write([]byte("<html>Spark Master</html>"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestStandalone(t *testing.T) {
	ctx := context.Background()
	bin, dir := fakeSubmit(t)
	var state, killed string
	ui := fakeMaster(t, &state, &killed)
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "spark://m:7077", Ui: ui.URL}}}
	sub, err := New(ctx, spec, nil, Options{SparkSubmit: bin, HTTP: ui.Client()})
	if err != nil {
		t.Fatal(err)
	}
	say(t, dir, "submit", "26/11/04 10:00:00 INFO Client: Driver successfully submitted as driver-20261104100000-0003\n")
	id, err := sub.Submit(ctx, app)
	if err != nil || id != "driver-20261104100000-0003" {
		t.Fatalf("submitted %q, %v", id, err)
	}
	if got, want := calls(t, dir)[0], "--master spark://m:7077 --deploy-mode cluster --name openetl-s1-2 --conf spark.executor.memory=2g --class com.acme.Models file:///data/org/p/_spark/code/x/a.jar --full"; got != want {
		t.Errorf("spark-submit got\n %s\nwant\n %s", got, want)
	}
	if id, err := sub.Find(ctx, app.Name); id != "" || err != nil {
		t.Errorf("found %q, %v on a standalone master", id, err)
	}
	for st, want := range map[string]State{
		"RUNNING":  {State: "RUNNING"},
		"FINISHED": {Done: true, State: "FINISHED"},
		"FAILED":   {Done: true, Failed: true, State: "FAILED", Message: "driver driver-1 ended FAILED; its log is on the worker that ran it, in the master's UI"},
		"":         {Done: true, Failed: true, State: "UNKNOWN", Message: "the master no longer knows driver driver-1, so whether it ran is unknown"},
	} {
		state = st
		want.URL = ui.URL
		if got, err := sub.Status(ctx, "driver-1"); err != nil || got != want {
			t.Errorf("status with the driver %q: %+v, %v; want %+v", st, got, err, want)
		}
	}
	if err := sub.Kill(ctx, "driver-1"); err != nil || killed != "driver-1" {
		t.Errorf("kill: %v, killed %q", err, killed)
	}
	ui.Close()
	if _, err := sub.Status(ctx, "driver-1"); errs.CodeOf(err) != errs.Transient {
		t.Errorf("an unreachable master: %v", err)
	}
	if got := standaloneUI("spark://m1:7077,m2:7077", ""); got != "http://m1:8080" {
		t.Errorf("the default UI is %s", got)
	}
	missing, err := New(ctx, spec, nil, Options{SparkSubmit: filepath.Join(dir, "nope")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := missing.Submit(ctx, app); errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), "--spark-submit") {
		t.Errorf("no spark-submit: %v", err)
	}
}

func TestKubernetes(t *testing.T) {
	ctx := context.Background()
	bin, dir := fakeSubmit(t)
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "k8s://https://k:6443"}}}
	sub, err := New(ctx, spec, map[string]string{"spark.kubernetes.namespace": "jobs"}, Options{SparkSubmit: bin})
	if err != nil {
		t.Fatal(err)
	}
	py := app
	py.Python, py.MainClass, py.Code = true, "", "s3a://lake/org/p/_spark/code/x/a.py"
	id, err := sub.Submit(ctx, py)
	if err != nil || id != "jobs:openetl-s1-2" {
		t.Fatalf("submitted %q, %v", id, err)
	}
	if c := calls(t, dir)[0]; !strings.Contains(c, "--conf spark.kubernetes.driver.pod.name=openetl-s1-2") || !strings.Contains(c, "--conf spark.kubernetes.submission.waitAppCompletion=false") || !strings.HasSuffix(c, "a.py --full") {
		t.Errorf("spark-submit got %s", c)
	}
	// A second submission's pod already exists: it's the first one's.
	say(t, dir, "submit", "ERROR Client: Failure executing: POST at: https://k:6443/api/v1/namespaces/jobs/pods. Message: pods \"openetl-s1-2\" already exists.\n")
	if err := os.WriteFile(filepath.Join(dir, "exit-submit"), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if id, err := sub.Submit(ctx, py); err != nil || id != "jobs:openetl-s1-2" {
		t.Errorf("resubmitted %q, %v", id, err)
	}
	say(t, dir, "status", "Application status (driver):\n\t pod name: openetl-s1-2\n\t namespace: jobs\n\t phase: Failed\n\t container status:\n\t\t container name: spark-kubernetes-driver\n\t\t container state: terminated\n\t\t exit code: 1\n\t\t termination reason: Error\n")
	st, err := sub.Status(ctx, id)
	if err != nil || !st.Done || !st.Failed || st.Message != "driver pod jobs:openetl-s1-2 failed with exit code 1 (Error)" {
		t.Errorf("status %+v, %v", st, err)
	}
	if found, err := sub.Find(ctx, "openetl-s1-2"); err != nil || found != id {
		t.Errorf("found %q, %v", found, err)
	}
	say(t, dir, "status", "Application not found.\n")
	if found, err := sub.Find(ctx, "openetl-s1-2"); err != nil || found != "" {
		t.Errorf("found a missing pod: %q, %v", found, err)
	}
}

func TestYARN(t *testing.T) {
	ctx := context.Background()
	bin, dir := fakeSubmit(t)
	var killed string
	rm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/ws/v1/cluster/apps" && r.URL.Query().Get("applicationTags") == "openetl-s1-2":
			_, _ = w.Write([]byte(`{"apps": {"app": [{"id": "application_1_0001", "startedTime": 1}, {"id": "application_1_0002", "startedTime": 2}]}}`))
		case r.URL.Path == "/ws/v1/cluster/apps":
			_, _ = w.Write([]byte(`{"apps": null}`))
		case r.URL.Path == "/ws/v1/cluster/apps/application_1_0002":
			_, _ = w.Write([]byte(`{"app": {"id": "application_1_0002", "state": "FINISHED", "finalStatus": "FAILED", "diagnostics": "User class threw exception: boom", "trackingUrl": "http://rm/proxy/application_1_0002/"}}`))
		case r.URL.Path == "/ws/v1/cluster/apps/application_1_0002/state" && r.Method == http.MethodPut:
			killed = "application_1_0002"
		default:
			http.NotFound(w, r)
		}
	}))
	defer rm.Close()
	conf := t.TempDir()
	site := `<configuration><property><name>yarn.resourcemanager.webapp.address</name><value>` + strings.TrimPrefix(rm.URL, "http://") + `</value></property></configuration>`
	if err := os.WriteFile(filepath.Join(conf, "yarn-site.xml"), []byte(site), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("YARN_CONF_DIR", "")
	t.Setenv("HADOOP_CONF_DIR", conf)
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "yarn"}}}
	sub, err := New(ctx, spec, nil, Options{SparkSubmit: bin, HTTP: rm.Client()})
	if err != nil {
		t.Fatal(err)
	}
	say(t, dir, "submit", "INFO Client: Submitted application application_1_0002\n")
	if id, err := sub.Submit(ctx, app); err != nil || id != "application_1_0002" {
		t.Fatalf("submitted %q, %v", id, err)
	}
	if c := calls(t, dir)[0]; !strings.Contains(c, "--conf spark.yarn.tags=openetl-s1-2") || !strings.Contains(c, "--master yarn") {
		t.Errorf("spark-submit got %s", c)
	}
	if id, err := sub.Find(ctx, "openetl-s1-2"); err != nil || id != "application_1_0002" {
		t.Errorf("found %q, %v", id, err)
	}
	if id, err := sub.Find(ctx, "openetl-s9-1"); err != nil || id != "" {
		t.Errorf("found %q, %v", id, err)
	}
	st, err := sub.Status(ctx, "application_1_0002")
	if err != nil || !st.Failed || st.State != "FAILED" || st.Message != "application application_1_0002 ended FAILED: User class threw exception: boom" || st.URL == "" {
		t.Errorf("status %+v, %v", st, err)
	}
	if err := sub.Kill(ctx, "application_1_0002"); err != nil || killed != "application_1_0002" {
		t.Errorf("kill: %v, killed %q", err, killed)
	}
	t.Setenv("HADOOP_CONF_DIR", "")
	if _, err := New(ctx, spec, nil, Options{SparkSubmit: bin}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("no HADOOP_CONF_DIR: %v", err)
	}
}
