package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/apache/arrow-go/v18/parquet/file"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/sdk/go/conformance"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// testConnectors builds the external package's test connector under each
// name, which picks what it serves.
func testConnectors(t *testing.T, names ...string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "testconnector")
	if out, err := exec.CommandContext(t.Context(), "go", "build", "-o", bin, "../../internal/connectorhost/external/testconnector").CombinedOutput(); err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	for _, n := range names {
		if err := os.Link(bin, filepath.Join(dir, n)); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// runOut runs the CLI and returns what it printed, and reported on stderr.
func runOut(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errs bytes.Buffer
	old := stderr
	stderr = &errs
	defer func() { stderr = old }()
	err := run(t.Context(), append([]string{"connector"}, args...), &out)
	return out.String(), errs.String(), err
}

func TestConnectorRun(t *testing.T) {
	dir := testConnectors(t, "source-faker", "source-typed", "source-both", "destination-files")
	faker := writeFile(t, dir, "faker.yaml", "seed: 1\ncount: 3\n")

	out, _, err := runOut(t, "spec", filepath.Join(dir, "source-faker"))
	if err != nil || !strings.Contains(out, "role: source") || !strings.Contains(out, "protocol: v1") {
		t.Errorf("spec: %v\n%s", err, out)
	}
	if out, _, err = runOut(t, "spec", "--role", "destination", "--format", "json", filepath.Join(dir, "source-both")); err != nil || !strings.Contains(out, `"class": "stage_and_apply"`) {
		t.Errorf("a destination's spec: %v\n%s", err, out)
	}
	if _, _, err := runOut(t, "spec", filepath.Join(dir, "source-both")); err == nil || !strings.Contains(err.Error(), "both roles") {
		t.Errorf("both roles, and none named: %v", err)
	}

	files := writeFile(t, dir, "files.json", `{"dir": "`+t.TempDir()+`", "token": {"secret": "tok"}}`)
	if out, _, err := runOut(t, "check", "--config", files, "--secret", "tok=s3cr3t", filepath.Join(dir, "destination-files")); err != nil || !strings.Contains(out, "passed") {
		t.Errorf("check: %v %s", err, out)
	}
	t.Setenv("OPENETL_SECRET_TOK", "wrong")
	if _, _, err := runOut(t, "check", "--config", files, filepath.Join(dir, "destination-files")); err == nil || !strings.Contains(err.Error(), "AUTH_INVALID") {
		t.Errorf("a wrong secret from the environment: %v", err)
	}

	out, _, err = runOut(t, "discover", "--format", "json", "--config", faker, filepath.Join(dir, "source-faker"))
	var cat struct{ Streams []streamView }
	if err != nil || json.Unmarshal([]byte(out), &cat) != nil || len(cat.Streams) != 2 || cat.Streams[0].Name != "users" {
		t.Errorf("discover: %v\n%s", err, out)
	}
	if out, _, err = runOut(t, "discover", filepath.Join(dir, "source-typed")); err != nil || !strings.Contains(out, "extension<arrow.uuid>") {
		t.Errorf("a typed stream's columns: %v\n%s", err, out)
	}

	state := filepath.Join(dir, "state.json")
	out, report, err := runOut(t, "read", "--format", "jsonl", "--stream", "users", "--config", faker, "--state-out", state, filepath.Join(dir, "source-faker"))
	if err != nil || strings.Count(out, `{"stream":"users","record":{"id":`) != 3 || !strings.Contains(report, "users: 3 records, complete") {
		t.Errorf("read as JSON lines: %v\n%s\n%s", err, out, report)
	}
	if b, err := os.ReadFile(state); err != nil || !json.Valid(b) {
		t.Errorf("--state-out: %v %s", err, b)
	}
	out, report, err = runOut(t, "read", "--limit", "2", "--config", faker, filepath.Join(dir, "source-faker"))
	if err != nil || !strings.Contains(out, "users (2 records)") || !strings.Contains(report, "users: 2 records, stopped at --limit") {
		t.Errorf("read as a table: %v\n%s\n%s", err, out, report)
	}
	pq := filepath.Join(dir, "pq")
	if _, _, err := runOut(t, "read", "--format", "parquet", "--out", pq, filepath.Join(dir, "source-typed")); err != nil {
		t.Fatal(err)
	}
	r, err := file.OpenParquetFile(filepath.Join(pq, "things.parquet"), false)
	if err != nil {
		t.Fatal(err)
	}
	if r.NumRows() != 3 {
		t.Errorf("the Parquet file has %d rows", r.NumRows())
	}
	_ = r.Close()
	if _, _, err := runOut(t, "read", "--format", "parquet", "--out", pq, "--config", faker, filepath.Join(dir, "source-faker")); err == nil || !strings.Contains(err.Error(), "JSON lines") {
		t.Errorf("JSON records as Parquet: %v", err)
	}
	if _, _, err := runOut(t, "read", "--stream", "nope", "--config", faker, filepath.Join(dir, "source-faker")); err == nil {
		t.Error("a stream the source hasn't")
	}
	if _, _, err := runOut(t, "discover", "--config", files, "--secret", "tok=s3cr3t", filepath.Join(dir, "destination-files")); err == nil || !strings.Contains(err.Error(), "take a source") {
		t.Errorf("discovering a destination: %v", err)
	}
}

// A descriptor's binary is downloaded by its digest and must return the
// descriptor's spec; a manifest runs in this process, with the platform's
// host services.
func TestConnectorRunDocuments(t *testing.T) {
	dir := testConnectors(t, "destination-files")
	srv := httptest.NewServer(http.FileServer(http.Dir(dir)))
	defer srv.Close()
	doc, err := connectorhost.Package(t.Context(), "acme/files", "1.0.0", spec.Info{Repository: "https://github.com/acme/files"}, map[string]connectorhost.PackageRole{
		"destination": {Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: filepath.Join(dir, "destination-files"), URL: srv.URL + "/destination-files"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc := writeFile(t, dir, "files.connector.yaml", string(doc))
	files := writeFile(t, dir, "files.json", `{"dir": "`+t.TempDir()+`", "token": "s3cr3t"}`)
	cache := t.TempDir()
	if out, _, err := runOut(t, "check", "--cache", cache, "--config", files, desc); err != nil || !strings.Contains(out, "acme/files 1.0.0's destination passed") {
		t.Errorf("a descriptor: %v %s", err, out)
	}
	if _, _, err := runOut(t, "check", "--cache", cache, "--role", "source", desc); err == nil || !strings.Contains(err.Error(), "no source role") {
		t.Errorf("a role the package hasn't: %v", err)
	}
	changed := writeFile(t, dir, "changed.connector.yaml", strings.Replace(string(doc), "tier: raw", "tier: proxy", 1))
	if _, _, err := runOut(t, "spec", "--cache", cache, changed); err == nil || !strings.Contains(err.Error(), "isn't its descriptor's") {
		t.Errorf("a binary whose spec isn't its descriptor's: %v", err)
	}

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k3y" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]`))
	}))
	defer api.Close()
	manifest := "../../internal/connectorpkg/testdata/acme/acme.yaml"
	if out, _, err := runOut(t, "spec", manifest); err != nil || !strings.Contains(out, "api_key") || !strings.Contains(out, "tier: host") {
		t.Errorf("a manifest's spec: %v\n%s", err, out)
	}
	acme := writeFile(t, dir, "acme.json", `{"base_url": "`+api.URL+`", "api_key": {"secret": "key"}}`)
	if _, _, err := runOut(t, "read", "--format", "jsonl", "--config", acme, "--secret", "key=k3y", manifest); err == nil || !strings.Contains(err.Error(), "not a public address") {
		t.Errorf("a private address without --egress-allow: %v", err)
	}
	out, report, err := runOut(t, "read", "--format", "jsonl", "--egress-allow", "127.0.0.1/32", "--config", acme, "--secret", "key=k3y", manifest)
	if err != nil || strings.Count(out, `"stream":"contacts"`) != 2 {
		t.Errorf("a manifest's read: %v\n%s\n%s", err, out, report)
	}
}

// --attach prints the environment to start the connector with, and waits
// for it.
func TestConnectorRunAttach(t *testing.T) {
	dir := testConnectors(t, "source-faker")
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := "tcp://" + ln.Addr().String()
	_ = ln.Close()

	pr, pw := io.Pipe()
	old := stderr
	stderr = pw
	defer func() { stderr = old }()
	envs := make(chan []string, 1)
	go func() {
		sc := bufio.NewScanner(pr)
		for sc.Scan() {
			if line := strings.TrimSpace(sc.Text()); strings.HasPrefix(line, "OPENETL_CONNECTOR_ADDR=") {
				envs <- strings.Fields(line)
			}
		}
	}()
	done := make(chan error, 1)
	var out bytes.Buffer
	go func() {
		done <- run(t.Context(), []string{"connector", "read", "--format", "jsonl", "--attach", addr, "--config", writeFile(t, dir, "faker.json", `{"seed": 1, "count": 2}`)}, &out)
	}()
	cmd := exec.CommandContext(t.Context(), filepath.Join(dir, "source-faker"))
	cmd.Env = append(os.Environ(), <-envs...)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	if err := <-done; err != nil || strings.Count(out.String(), `"stream":"users"`) != 2 {
		t.Errorf("read: %v\n%s", err, out.String())
	}
	_ = pw.Close()
}

// --image runs an image, and package reads an image-only role's spec the
// same way. It needs Docker or Podman, and OPENETL_TEST_CONTAINER=1.
func TestConnectorRunImage(t *testing.T) {
	if os.Getenv("OPENETL_TEST_CONTAINER") != "1" {
		t.Skip("set OPENETL_TEST_CONTAINER=1 to build and run an image")
	}
	engine, err := conformance.ContainerEngine()
	if err != nil {
		t.Fatal(err)
	}
	image := conformance.BuildImage(t, engine, "../../sdk/go/conformance/testdata/both")
	if out, _, err := runOut(t, "read", "--format", "jsonl", "--image", image, "--stream", "launch"); err != nil || !strings.Contains(out, `"stream":"launch"`) {
		t.Errorf("read an image: %v\n%s", err, out)
	}
	s, err := imageSpec(t.Context(), image, "destination", nil)
	if err != nil || s.GetKind() != protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		t.Errorf("an image's destination spec: %v, %v", s.GetKind(), err)
	}
}
