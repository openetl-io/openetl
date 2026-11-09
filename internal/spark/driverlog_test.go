package spark

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	k8sclient "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
)

// kubeSubmitter is a submitter on Kubernetes whose API is srv.
func kubeSubmitter(t *testing.T, srv *httptest.Server) (*kubernetes, *bytes.Buffer) {
	t.Helper()
	var log bytes.Buffer
	spec := &apiv1.SparkClusterSpec{Target: &apiv1.SparkClusterSpec_Submit{Submit: &apiv1.SparkSubmit{Master: "k8s://https://api:6443"}}}
	sub, err := New(t.Context(), spec, map[string]string{"spark.kubernetes.namespace": "jobs"}, Options{Logger: slog.New(slog.NewTextHandler(&log, nil))})
	if err != nil {
		t.Fatal(err)
	}
	k := sub.(*kubernetes)
	k.client = func() (k8sclient.Interface, error) { return k8sclient.NewForConfig(&restclient.Config{Host: srv.URL}) }
	return k, &log
}

func TestFailedDriverLastLines(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/api/v1/namespaces/jobs/pods/openetl-s1-2/log" || q.Get("tailLines") != "100" || q.Get("limitBytes") != fmt.Sprint(driverLogRead) || q.Get("container") != "main" {
			t.Errorf("asked for %s", r.URL)
		}
		_, _ = fmt.Fprint(w, "starting up\nkey is s3cr3t-value-1\nException: asked to fail\n")
	}))
	defer srv.Close()
	k, log := kubeSubmitter(t, srv)
	LogDriver(t.Context(), k, "jobs:openetl-s1-2", Application{Conf: map[string]string{
		"spark.kubernetes.driver.podTemplateContainerName": "main",
		"spark.hadoop.fs.s3a.secret.key":                   "s3cr3t-value-1",
		"spark.executor.memory":                            "512m-not-secret",
	}})
	got := log.String()
	first, last := strings.Index(got, "starting up"), strings.Index(got, "Exception: asked to fail")
	if first < 0 || last < first || !strings.Contains(got, "last 3 lines") || !strings.Contains(got, "component=spark-driver") {
		t.Errorf("the log says %s", got)
	}
	if strings.Contains(got, "s3cr3t-value-1") || !strings.Contains(got, "key is "+redacted) {
		t.Errorf("a secret property's value wasn't masked: %s", got)
	}
}

func TestDriverLogIsBounded(t *testing.T) {
	var sent bytes.Buffer
	for i := range 2000 {
		fmt.Fprintf(&sent, "%0*d\n", 1023, i)
	}
	sent.WriteString(strings.Repeat("x", 100<<10) + "\n")
	sent.WriteString("the last line\n")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sent.Bytes()) }))
	defer srv.Close()
	k, _ := kubeSubmitter(t, srv)
	lines, err := k.driverLines(t.Context(), "jobs", "p", "")
	if err != nil {
		t.Fatal(err)
	}
	size := 0
	for _, l := range lines {
		size += len(l)
		if len(l) > driverLogLine+len("... (cut)") {
			t.Errorf("a line of %d bytes", len(l))
		}
	}
	if len(lines) > driverLogLines || size > driverLogBytes || lines[len(lines)-1] != "the last line" {
		t.Errorf("kept %d lines, %d bytes, ending %.40q", len(lines), size, lines[len(lines)-1])
	}
}

func TestLogDriverOnlySaysWhyItCouldnt(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"forbidden": func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "no", http.StatusForbidden) },
		"gone":      func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(handler)
			defer srv.Close()
			k, log := kubeSubmitter(t, srv)
			LogDriver(t.Context(), k, "jobs:p", Application{})
			if n := strings.Count(strings.TrimSpace(log.String()), "\n"); n != 0 || !strings.Contains(log.String(), "couldn't be read") {
				t.Errorf("the log says %s", log)
			}
		})
	}
	t.Run("no client", func(t *testing.T) {
		srv := httptest.NewServer(http.NotFoundHandler())
		defer srv.Close()
		k, log := kubeSubmitter(t, srv)
		k.client = func() (k8sclient.Interface, error) { return nil, errors.New("no credentials") }
		LogDriver(t.Context(), k, "jobs:p", Application{})
		if !strings.Contains(log.String(), "no credentials") {
			t.Errorf("the log says %s", log)
		}
	})
	t.Run("another cluster's submitter", func(t *testing.T) {
		LogDriver(t.Context(), &yarn{}, "application_1", Application{})
	})
}

func TestDriverClientUsesOnlyTheMastersAPIServer(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	kubeconfig := filepath.Join(t.TempDir(), "config")
	config := `apiVersion: v1
kind: Config
current-context: c
contexts:
- name: c
  context: {cluster: k, user: u}
clusters:
- name: k
  cluster: {server: "https://api:6443"}
users:
- name: u
  user: {token: t}
`
	if err := os.WriteFile(kubeconfig, []byte(config), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KUBECONFIG", kubeconfig)
	if _, err := driverClient("k8s://https://api:6443", nil)(); err != nil {
		t.Errorf("its own API server: %v", err)
	}
	if _, err := driverClient("k8s://https://elsewhere:6443", nil)(); err == nil || !strings.Contains(err.Error(), "credentials are for") {
		t.Errorf("another API server: %v", err)
	}
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "none"))
	if _, err := driverClient("k8s://https://api:6443", nil)(); err == nil || !strings.Contains(err.Error(), "no Kubernetes credentials") {
		t.Errorf("no kubeconfig: %v", err)
	}
}

func TestSameAPIServer(t *testing.T) {
	t.Setenv("KUBERNETES_SERVICE_HOST", "10.96.0.1")
	for _, c := range []struct {
		credentials, master string
		want                bool
	}{
		{"https://api:6443", "https://api:6443", true},
		{"https://API:6443/", "api:6443", true},
		{"https://api", "https://api:443", true},
		{"https://rancher/k8s/clusters/c-1", "https://rancher/k8s/clusters/c-2", false},
		{"https://api:6443", "https://api:443", false},
		{"https://api:6443", "https://evil:6443", false},
		{"https://10.96.0.1:443", "https://kubernetes.default.svc", true},
		{"https://10.96.0.1:443", "https://kubernetes.default.svc.cluster.local:443", true},
		{"https://10.96.0.1:443", "https://kubernetes.default.svc:6443", false},
		{"https://192.0.2.1:443", "https://kubernetes.default.svc", false},
		{"", "https://api", false},
	} {
		if got := sameAPIServer(c.credentials, c.master); got != c.want {
			t.Errorf("sameAPIServer(%q, %q) = %v", c.credentials, c.master, got)
		}
	}
}
