//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
)

func manifest(name, version, path string) string {
	return fmt.Sprintf(`apiVersion: openetl.io/v1alpha1
kind: DeclarativeSource
metadata: {name: %s, version: %s}
spec:
  config:
    type: object
    required: [base_url, api_key]
    properties:
      base_url: {type: string}
      api_key: {type: string, x-secret: true}
  http:
    base_url: "{config.base_url}"
    auth: {type: bearer, token: {secret: api_key}}
  streams:
    - name: contacts
      primary_key: [id]
      request: {path: %s}
`, name, version, path)
}

func applyErr(t *testing.T, docs ...string) error {
	t.Helper()
	req := &apiv1.ApplyRequest{}
	for i, d := range docs {
		req.Documents = append(req.Documents, &apiv1.Document{Origin: fmt.Sprintf("doc %d", i+1), Content: []byte(d)})
	}
	_, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(t.Context(), connect.NewRequest(req))
	return err
}

// A manifest is applied as a connector version, and the runner fetches it
// by digest: it has no other way to get it.
func TestDeclarativeSource(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer k1" || r.URL.Path != "/v1/contacts" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write([]byte(`[{"id": 1, "name": "a"}, {"id": 2, "name": "b"}, {"id": 3, "name": "c"}]`))
	}))
	defer api.Close()
	startRunner(t)
	name := unique("manifest")

	res := apply(t, manifest(name, "0.1.0", "/v1/contacts"))
	if res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || res[0].GetName() != "connectors/source-"+name+"/versions/0.1.0" ||
		!strings.HasPrefix(res[0].GetVersion(), "sha256:") {
		t.Fatalf("applied %v", res[0])
	}
	digest := res[0].GetVersion()
	if again := apply(t, manifest(name, "0.1.0", "/v1/contacts")); again[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
		t.Errorf("reapplied: %v", again[0].GetAction())
	}
	if err := applyErr(t, manifest(name, "0.1.0", "/v2/contacts")); err == nil || !strings.Contains(err.Error(), "new version") {
		t.Errorf("version 0.1.0 with other content: %v", err)
	}
	// A prerelease doesn't replace the release as the default.
	apply(t, manifest(name, "0.2.0-rc.1", "/v2/contacts"))
	if err := applyErr(t, sourceDoc(name, fmt.Sprintf(`{"connector": "%s@9.9.9", "config": {}}`, name))); err == nil || !strings.Contains(err.Error(), "0.1.0") {
		t.Errorf("a version that wasn't applied: %v", err)
	}
	if err := applyErr(t, sourceDoc(name, fmt.Sprintf(`{"connector": %q, "config": {"base_url": 1}}`, name))); err == nil {
		t.Error("applied a config the manifest's schema refuses")
	}

	docs := []string{
		sourceDoc(name, fmt.Sprintf(`{"connector": %q, "config": {"base_url": %q, "api_key": {"secret": %q}}}`, name, api.URL, setSecret(t, "k1"))),
		destinationDoc(name, `{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}}}`),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "1h"}, "source": {"ref": %q}, "destination": {"ref": %q, "namespace": %q},
			"delivery": {"require": "at_least_once"}}}`, name, name, name, schema(name)),
	}
	apply(t, docs...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkRows(t, schema(name), 3, "contacts")
	got, err := query[string](t, "SELECT connector_digest FROM sources WHERE name = $1", name)
	if err != nil || got != digest {
		t.Errorf("the source resolved to %q, %v; want 0.1.0's %s", got, err, digest)
	}
	cached := filepath.Join(env.cache, "artifacts", "sha256-"+strings.TrimPrefix(digest, "sha256:"))
	if _, err := os.Stat(cached); err != nil {
		t.Errorf("the runner didn't cache the manifest: %v", err)
	}
}

// Sources and destinations on the official connectors resolve to their
// applied descriptors, and the runner runs the executables it pulled.
func TestExternalConnectorsArePinned(t *testing.T) {
	startRunner(t)
	name := unique("pinned")
	apply(t, pipelineDocs(name, `{"seed": 3, "count": 50}`, `{}`)...)
	if status, stats := waitSync(t, name, 60*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	digest, err := query[string](t, "SELECT connector_digest FROM destinations WHERE name = $1", name)
	if err != nil || !strings.HasPrefix(digest, "sha256:") {
		t.Fatalf("the destination's connector digest is %q, %v", digest, err)
	}
	ents, err := os.ReadDir(filepath.Join(env.cache, "executables"))
	if err != nil || len(ents) == 0 {
		t.Fatalf("no executables in the runner's cache: %v", err)
	}
	if err := applyErr(t, destinationDoc(name, `{"connector": "postgres@sha256:`+strings.Repeat("0", 64)+`", "config": {}}`)); err == nil {
		t.Error("applied a destination on a digest that isn't a version")
	}
	if err := applyErr(t, sourceDoc(name, `{"connector": "faker@1.0.0", "config": {}}`)); err == nil || !strings.Contains(err.Error(), "built in") {
		t.Errorf("a version of a built-in: %v", err)
	}
}
