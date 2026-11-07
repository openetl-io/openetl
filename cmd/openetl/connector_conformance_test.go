package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	ggcr "github.com/google/go-containerregistry/pkg/registry"

	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorpkg"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

// The checks these tests run, so checks the kit adds don't change them.
const (
	sourceChecks      = "^(spec|check|discover|read/full|read/resume)$"
	destinationChecks = "^(spec|check|errors|write/commit|write/crash-after-record|write/fence)$"
)

func TestConnectorTestSource(t *testing.T) {
	dir := testConnectors(t, "source-faker")
	faker := writeFile(t, dir, "faker.yaml", "seed: 1\ncount: 30\n")
	out, report, err := runOut(t, "test", "-q", "--run", sourceChecks, "--config", faker, filepath.Join(dir, "source-faker"))
	if err != nil {
		t.Fatalf("%v\n%s\n%s", err, out, report)
	}
	for _, c := range []string{"spec", "check", "discover", "read/full", "read/resume"} {
		if !strings.Contains(out, "PASS "+c+"\n") {
			t.Errorf("%s didn't pass:\n%s", c, out)
		}
	}
	if !strings.Contains(out, "flag: the spec has no config schema") || strings.Contains(out, "read/incremental") {
		t.Errorf("a passing check's flag, and only the checks --run names:\n%s", out)
	}
	if !strings.Contains(out, "source-faker: 5 checks, 5 passed, 0 failed, 0 skipped") {
		t.Errorf("summary:\n%s", out)
	}

	out, _, err = runOut(t, "test", "-q", "--json", "--run", "^(discover|read/incremental)$", "--stream", "users", "--config", faker, filepath.Join(dir, "source-faker"))
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
	var results []struct {
		Check, Result string
		Log           []string
	}
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		var r struct {
			Check, Result string
			Log           []string
		}
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("%q: %v", line, err)
		}
		results = append(results, r)
	}
	if len(results) != 2 || results[0].Check != "discover" || results[0].Result != "pass" || results[1].Result != "skip" ||
		!strings.Contains(strings.Join(results[1].Log, ""), "reads no stream incrementally") {
		t.Errorf("--json: %+v", results)
	}

	if _, _, err := runOut(t, "test", "-q", "--run", "^nothing$", "--config", faker, filepath.Join(dir, "source-faker")); err == nil || !strings.Contains(err.Error(), "no check matches") {
		t.Errorf("a --run matching nothing: %v", err)
	}
	if _, _, err := runOut(t, "test", "-q", "--read-command", "true", "--config", faker, filepath.Join(dir, "source-faker")); err == nil || !strings.Contains(err.Error(), "for destinations") {
		t.Errorf("a destination's flag for a source: %v", err)
	}
	if _, _, err := runOut(t, "test", "-q", "--stream", "users:sometimes", "--config", faker, filepath.Join(dir, "source-faker")); err == nil || !strings.Contains(err.Error(), "isn't full_refresh") {
		t.Errorf("an unknown mode: %v", err)
	}
}

// A destination's write checks see what it made visible through
// --read-command, and skip without one.
func TestConnectorTestDestination(t *testing.T) {
	dir := testConnectors(t, "destination-jsonl")
	data := t.TempDir()
	cfg := writeFile(t, dir, "jsonl.json", fmt.Sprintf(`{"dir": %q, "token": {"secret": "tok"}}`, data))
	bad := writeFile(t, dir, "bad.json", fmt.Sprintf(`{"dir": %q, "token": "nope"}`, data))
	t.Setenv("DATA", data)
	read := `cat "$DATA/$OPENETL_NAMESPACE/$OPENETL_STREAM.jsonl" 2>/dev/null || true`
	drop := `echo "$OPENETL_NAMESPACE" >> "$DATA/dropped"; rm -rf "$DATA/$OPENETL_NAMESPACE"`
	out, report, err := runOut(t, "test", "-q", "--untyped", "--run", destinationChecks, "--config", cfg, "--secret", "tok=s3cr3t",
		"--bad-credentials", bad, "--read-command", read, "--drop-command", drop, filepath.Join(dir, "destination-jsonl"))
	if err != nil {
		t.Fatalf("%v\n%s\n%s", err, out, report)
	}
	if !strings.Contains(out, "destination-jsonl: 6 checks, 6 passed, 0 failed, 0 skipped") {
		t.Errorf("summary:\n%s", out)
	}
	if b, err := os.ReadFile(filepath.Join(data, "dropped")); err != nil || len(strings.Fields(string(b))) == 0 {
		t.Errorf("--drop-command wasn't run: %v", err)
	}

	// A reader that sees nothing fails the checks that write.
	out, _, err = runOut(t, "test", "-q", "--run", "^write/commit$", "--config", cfg, "--secret", "tok=s3cr3t", "--read-command", "true", filepath.Join(dir, "destination-jsonl"))
	if err == nil || !strings.Contains(out, "FAIL write/commit") {
		t.Errorf("a reader that sees nothing: %v\n%s", err, out)
	}

	out, _, err = runOut(t, "test", "-q", "--run", "^(check|write/commit)$", "--config", cfg, "--secret", "tok=s3cr3t", filepath.Join(dir, "destination-jsonl"))
	if err != nil || !strings.Contains(out, "No --read-command") || !strings.Contains(out, "PASS check") ||
		!strings.Contains(out, "SKIP write/commit\n    the suite has no Read") {
		t.Errorf("without --read-command: %v\n%s", err, out)
	}
}

func TestJSONRecords(t *testing.T) {
	recs, err := jsonRecords([]byte("{\"id\": 8589934592, \"v\": \"a\", \"x\": null}\n\n{\"id\": 1.5}\n"))
	if err != nil || len(recs) != 2 || fmt.Sprint(recs[0]["id"]) != "8589934592" || recs[0]["x"] != nil || fmt.Sprint(recs[1]["id"]) != "1.5" {
		t.Errorf("%v, %v", recs, err)
	}
	if recs, err := jsonRecords(nil); err != nil || recs != nil {
		t.Errorf("nothing: %v, %v", recs, err)
	}
	for _, bad := range []string{"[1]", "null", "{"} {
		if _, err := jsonRecords([]byte(bad)); err == nil {
			t.Errorf("%s took", bad)
		}
	}
}

// A descriptor openetl connector package wrote runs its binary from beside
// it before anything is released, and a binary whose spec isn't the
// descriptor's fails before any check.
func TestConnectorTestDescriptor(t *testing.T) {
	dir := testConnectors(t, "destination-jsonl")
	dist := t.TempDir()
	asset := connectorpkg.Asset("jsonl", "destination", connectorhost.Platform())
	if err := os.Link(filepath.Join(dir, "destination-jsonl"), filepath.Join(dist, asset)); err != nil {
		t.Fatal(err)
	}
	doc, err := connectorhost.Package(t.Context(), "acme/jsonl", "1.0.0", spec.Info{Repository: "https://github.com/acme/jsonl"}, map[string]connectorhost.PackageRole{
		"destination": {Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: filepath.Join(dist, asset),
			URL: "https://github.com/acme/jsonl/releases/download/v1.0.0/" + asset}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc := writeFile(t, dist, "jsonl.connector.yaml", string(doc))
	cfg := writeFile(t, dir, "jsonl.json", fmt.Sprintf(`{"dir": %q, "token": "s3cr3t"}`, t.TempDir()))
	cache := t.TempDir()
	out, report, err := runOut(t, "test", "-q", "--cache", cache, "--run", "^(spec|check)$", "--config", cfg, desc)
	if err != nil || !strings.Contains(out, "acme/jsonl 1.0.0's destination: 2 checks, 2 passed") {
		t.Errorf("a packaged descriptor: %v\n%s\n%s", err, out, report)
	}
	if ents, _ := os.ReadDir(cache); len(ents) != 0 {
		t.Errorf("the binary beside the descriptor was downloaded: %v", ents)
	}

	changed := writeFile(t, dist, "changed.connector.yaml", strings.Replace(string(doc), "tier: raw", "tier: proxy", 1))
	out, _, err = runOut(t, "test", "-q", "--cache", cache, "--config", cfg, changed)
	if err == nil || !strings.Contains(err.Error(), "isn't its descriptor's") || out != "" {
		t.Errorf("a binary whose spec isn't its descriptor's: %v\n%s", err, out)
	}

	if err := os.WriteFile(filepath.Join(dist, asset+".new"), []byte("not it"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dist, asset+".new"), filepath.Join(dist, asset)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := runOut(t, "test", "-q", "--cache", cache, "--config", cfg, desc); err == nil || !strings.Contains(err.Error(), "beside the descriptor, has digest") {
		t.Errorf("a stale binary beside the descriptor: %v", err)
	}
}

// References resolve to a descriptor in a registry or a GitHub release, and
// its binary is fetched by its digest.
func TestConnectorTestReferences(t *testing.T) {
	dir := testConnectors(t, "destination-jsonl")
	dist := t.TempDir()
	asset := connectorpkg.Asset("jsonl", "destination", connectorhost.Platform())
	if err := os.Link(filepath.Join(dir, "destination-jsonl"), filepath.Join(dist, asset)); err != nil {
		t.Fatal(err)
	}
	doc, err := connectorhost.Package(t.Context(), "acme/jsonl", "1.0.0", spec.Info{Repository: "https://github.com/acme/jsonl"}, map[string]connectorhost.PackageRole{
		"destination": {Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: filepath.Join(dist, asset),
			URL: "https://github.com/acme/jsonl/releases/download/v1.0.0/" + asset}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	desc := writeFile(t, dist, "jsonl.connector.yaml", string(doc))
	cfg := writeFile(t, dir, "jsonl.json", fmt.Sprintf(`{"dir": %q, "token": "s3cr3t"}`, t.TempDir()))
	t.Setenv("DOCKER_CONFIG", t.TempDir())

	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	defer srv.Close()
	ns, err := oci.Parse(oci.Scheme + strings.TrimPrefix(srv.URL, "http://") + "/acme/connectors")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connectorpkg.Publish(t.Context(), &oci.Client{HTTP: srv.Client()}, ns, desc); err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	ref := ns.Child("jsonl").At("1.0.0").String()
	out, report, err := runOut(t, "test", "-q", "--cache", cache, "--run", "^(spec|check)$", "--config", cfg, ref)
	if err != nil || !strings.Contains(out, "2 checks, 2 passed") {
		t.Errorf("%s: %v\n%s\n%s", ref, err, out, report)
	}
	if out, _, err := runOut(t, "validate", "--artifact", ref); err != nil || strings.Contains(out, "FAIL") || !strings.Contains(out, "ok   index") {
		t.Errorf("validate --artifact %s: %v\n%s", ref, err, out)
	}
	if out, _, err := runOut(t, "validate", "--artifact", ns.Child("jsonl").At("9.9.9").String()); err == nil {
		t.Errorf("validate --artifact of a version that isn't there: %s", out)
	}

	// The release's binary is the one the registry served, in the cache by
	// its digest.
	gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/repos/acme/jsonl/releases/tags/v1.0.0":
			_, _ = fmt.Fprintf(w, `{"tag_name": "v1.0.0", "html_url": "https://github.com/acme/jsonl/releases/tag/v1.0.0",
				"assets": [{"id": 7, "name": "jsonl.connector.yaml", "url": "http://%s/assets/7"}]}`, r.Host)
		case "/assets/7":
			_, _ = w.Write(doc)
		default:
			http.NotFound(w, r)
		}
	}))
	defer gh.Close()
	t.Setenv("GITHUB_API_URL", gh.URL)
	if out, report, err := runOut(t, "test", "-q", "--cache", cache, "--run", "^spec$", "--config", cfg, "github.com/acme/jsonl@1.0.0"); err != nil || !strings.Contains(out, "PASS spec") {
		t.Errorf("a GitHub release: %v\n%s\n%s", err, out, report)
	}
	if _, _, err := runOut(t, "spec", "github.com/acme/jsonl@2.0.0"); err == nil || !strings.Contains(err.Error(), "release v2.0.0") {
		t.Errorf("a release that isn't there: %v", err)
	}
}
