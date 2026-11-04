package flink

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
)

// fakeREST is a session cluster's REST API, in memory.
type fakeREST struct {
	mu        sync.Mutex
	config    map[string]string
	jars      map[string]string // id -> name
	uploads   int
	runs      []map[string]any
	jobs      map[string]*jobState
	cancelled []string
	stops     []string
	exception string
	// Requests without this bearer token are refused.
	token string
}

func newFakeREST(t *testing.T) (*fakeREST, *httptest.Server) {
	f := &fakeREST{
		config: map[string]string{"jobmanager.execution.failover-strategy": "full", "execution.checkpointing.num-retained": "2"},
		jars:   map[string]string{}, jobs: map[string]*jobState{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeREST) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.token != "" && r.Header.Get("Authorization") != "Bearer "+f.token {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	switch {
	case r.URL.Path == "/jobmanager/config":
		var out []map[string]string
		for k, v := range f.config {
			out = append(out, map[string]string{"key": k, "value": v})
		}
		reply(out)
	case r.URL.Path == "/jars" && r.Method == http.MethodGet:
		var files []map[string]string
		for id, name := range f.jars {
			files = append(files, map[string]string{"id": id, "name": name})
		}
		reply(map[string]any{"files": files})
	case r.URL.Path == "/jars/upload":
		file, hdr, err := r.FormFile("jarfile")
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_, _ = io.Copy(io.Discard, file)
		f.uploads++
		id := "u" + strings.Repeat("x", f.uploads) + "_" + hdr.Filename
		f.jars[id] = hdr.Filename
		reply(map[string]string{"filename": "/tmp/flink-web/upload/" + id, "status": "success"})
	case len(parts) == 3 && parts[0] == "jars" && parts[2] == "run":
		if _, ok := f.jars[parts[1]]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			reply(map[string]any{"errors": []string{"Jar file does not exist"}})
			return
		}
		var in map[string]any
		_ = json.NewDecoder(r.Body).Decode(&in)
		f.runs = append(f.runs, in)
		id := in["jobId"].(string)
		f.jobs[id] = &jobState{ID: id, Name: in["flinkConfiguration"].(map[string]any)["pipeline.name"].(string), State: "RUNNING"}
		reply(map[string]string{"jobid": id})
	case r.URL.Path == "/jobs/overview":
		var jobs []jobState
		for _, j := range f.jobs {
			jobs = append(jobs, *j)
		}
		reply(map[string]any{"jobs": jobs})
	case len(parts) == 2 && parts[0] == "jobs" && r.Method == http.MethodPatch:
		j, ok := f.jobs[parts[1]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		f.cancelled = append(f.cancelled, j.ID)
		j.State = "CANCELED"
		w.WriteHeader(http.StatusAccepted)
	case len(parts) == 2 && parts[0] == "jobs":
		j, ok := f.jobs[parts[1]]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			reply(map[string]any{"errors": []string{"Job could not be found."}})
			return
		}
		reply(j)
	case len(parts) == 3 && parts[2] == "exceptions":
		reply(map[string]any{"exceptionHistory": map[string]any{"entries": []map[string]string{{"exceptionName": "java.lang.RuntimeException", "stacktrace": f.exception}}}})
	case len(parts) == 3 && parts[2] == "stop":
		var in struct {
			Target string `json:"targetDirectory"`
		}
		_ = json.NewDecoder(r.Body).Decode(&in)
		f.stops = append(f.stops, in.Target)
		f.jobs[parts[1]].State = "FINISHED"
		reply(map[string]string{"request-id": "t1"})
	case len(parts) == 4 && parts[2] == "savepoints":
		reply(map[string]any{"status": map[string]string{"id": "COMPLETED"}, "operation": map[string]string{"location": "s3://x/savepoint-1"}})
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

// newTestCluster is a REST cluster at url over a directory store, with a
// jar to deploy.
func newTestCluster(t *testing.T, url string, conf map[string]string) (*Cluster, string) {
	t.Helper()
	jar := []byte("PK fake jar")
	sum := sha256.Sum256(jar)
	code := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jar) }))
	t.Cleanup(code.Close)
	store := t.TempDir()
	c, err := New(t.Context(), Settings{
		Org: "org", Pipeline: "pay", Store: store,
		Cluster: &apiv1.FlinkClusterSpec{Target: &apiv1.FlinkClusterSpec_Rest{Rest: &apiv1.FlinkRest{Url: url}}},
		Transform: &apiv1.FlinkTransform{
			Code: &apiv1.FlinkCode{Jar: code.URL + "/fraud.jar", Sha256: hex.EncodeToString(sum[:]), MainClass: "com.acme.Fraud"},
			Conf: conf, Parallelism: 2,
		},
	}, Options{HTTP: code.Client(), Poll: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, store
}

func TestRESTDeploy(t *testing.T) {
	f, srv := newFakeREST(t)
	c, store := newTestCluster(t, srv.URL, map[string]string{"state.backend.type": "rocksdb", "execution.checkpointing.mode": "AT_LEAST_ONCE"})
	d := runner.FlinkDeploy{Generation: 3, RestoreFrom: "file:///x/chk-4", Bridge: "runner:7443", Fingerprint: "ab", Token: "tok", CheckpointInterval: 5 * time.Second}
	j, err := c.Deploy(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	if j.ID() != JobID("pay", 3) || len(j.ID()) != 32 || !strings.HasSuffix(j.UI(), "/#/job/running/"+j.ID()+"/overview") {
		t.Errorf("job = %s, %s", j.ID(), j.UI())
	}
	run := f.runs[0]
	conf := run["flinkConfiguration"].(map[string]any)
	args := run["programArgsList"].([]any)
	if run["entryClass"] != "com.acme.Fraud" || run["savepointPath"] != "file:///x/chk-4" || run["claimMode"] != "NO_CLAIM" || run["parallelism"] != float64(2) ||
		!slices.Equal(args, []any{"--openetl.bridge", "runner:7443", "--openetl.fingerprint", "ab", "--openetl.token", "tok"}) {
		t.Errorf("run = %v", run)
	}
	if conf["state.backend.type"] != "rocksdb" || conf["execution.checkpointing.mode"] != "EXACTLY_ONCE" || conf["execution.checkpointing.interval"] != "5000 ms" ||
		conf["pipeline.name"] != "openetl-pay-g3" || conf["execution.checkpointing.dir"] != "file://"+filepath.ToSlash(store)+"/org/pay/_flink/checkpoints" ||
		conf["jobmanager.execution.failover-strategy"] != "full" || conf["restart-strategy.type"] != "failure-rate" || conf["$internal.pipeline.job-id"] != nil {
		t.Errorf("conf = %v", conf)
	}
	// The code was checked and copied, and is uploaded once.
	if _, err := os.Stat(filepath.Join(store, "org/pay/_flink/code")); err != nil {
		t.Error(err)
	}
	if _, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 4}); err != nil || f.uploads != 1 || len(f.runs) != 2 {
		t.Errorf("err = %v, uploads = %d, runs = %d", err, f.uploads, len(f.runs))
	}
}

func TestRESTRefusesClusterSettings(t *testing.T) {
	f, srv := newFakeREST(t)
	f.config["jobmanager.execution.failover-strategy"] = "region"
	c, _ := newTestCluster(t, srv.URL, nil)
	_, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1})
	if errs.CodeOf(err) != errs.ConfigInvalid || !strings.Contains(err.Error(), `jobmanager.execution.failover-strategy ("region")`) {
		t.Errorf("err = %v", err)
	}
}

func TestRESTJobLifecycle(t *testing.T) {
	f, srv := newFakeREST(t)
	c, _ := newTestCluster(t, srv.URL, nil)
	j, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.jobs[j.ID()].State = "FAILED"
	f.exception = "java.lang.IllegalStateException: bad record\n\tat com.acme.Fraud.map(Fraud.java:12)"
	f.mu.Unlock()
	var failure *runner.FlinkFailed
	if err := j.Wait(t.Context()); !errors.As(err, &failure) || !strings.Contains(failure.Message, "bad record") {
		t.Errorf("err = %v", err)
	}

	j, err = c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	dir := c.SavepointDir("stop-2")
	if err := j.Stop(t.Context(), dir); err != nil || f.stops[0] != dir {
		t.Errorf("err = %v, stops = %v", err, f.stops)
	}
	if err := j.Wait(t.Context()); err != nil {
		t.Errorf("a stopped job's wait: %v", err)
	}

	// Another pipeline's job is left.
	f.mu.Lock()
	f.jobs["other"] = &jobState{ID: "other", Name: "openetl-payroll-g1", State: "RUNNING"}
	f.mu.Unlock()
	j3, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 3})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.CancelAll(t.Context()); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(f.cancelled, []string{j3.ID()}) || f.jobs["other"].State != "RUNNING" {
		t.Errorf("cancelled = %v", f.cancelled)
	}
	if err := j3.Wait(t.Context()); errs.CodeOf(err) != errs.Transient {
		t.Errorf("a cancelled job's wait: %v", err)
	}
}

func TestRESTToken(t *testing.T) {
	f, srv := newFakeREST(t)
	f.token = "s3cret"
	c, _ := newTestCluster(t, srv.URL, nil)
	if _, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1}); errs.CodeOf(err) != errs.ConfigInvalid {
		t.Errorf("without the token: %v", err)
	}
	r := c.target.(*rest)
	r.cl.token, r.checked, r.check = "s3cret", sync.Once{}, nil
	if _, err := c.Deploy(t.Context(), runner.FlinkDeploy{Generation: 1}); err != nil {
		t.Errorf("with the token: %v", err)
	}
}
