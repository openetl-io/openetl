//go:build e2e

package e2e

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
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
	if res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || res[0].GetName() != "connectors/openetl/"+name+"/versions/0.1.0" ||
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
	got, err := apiv1connect.NewDestinationServiceClient(env.http, env.apiURL).GetDestination(t.Context(),
		connect.NewRequest(&apiv1.GetDestinationRequest{Name: "workspaces/default/destinations/" + name}))
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Msg.GetDestination().GetConnector(); c.GetDigest() != digest || c.GetVersion() != "0.1.0" ||
		c.GetAuthor() != "OpenETL" || c.GetReleaseTag() != "postgres/v0.1.0" || c.GetRepository() != officialRepository {
		t.Errorf("the destination's connector version is %v", c)
	}

	// A descriptor without digests is pinned to what its URLs serve when
	// it's applied.
	exe := filepath.Join(env.connectors, "destination-postgres")
	desc, err := connectorhost.Package(t.Context(), "postgres", "0.1.1", spec.Info{Repository: officialRepository}, map[string]connectorhost.PackageRole{
		"destination": {Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: exe, URL: env.downloads + "/destination-postgres"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	unpinned := regexp.MustCompile(`(?m)^\s*digest: \S+\n`).ReplaceAllString(string(desc), "")
	if err := applyErr(t, unpinned); err != nil {
		t.Fatal(err)
	}
	content, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := query[string](t, `SELECT convert_from(a.content, 'UTF8') FROM connector_roles r JOIN artifacts a USING (org_id, digest)
		WHERE r.role = 'destination' AND r.name = 'openetl/postgres' AND r.version = '0.1.1'`)
	if err != nil || !strings.Contains(stored, connectorhost.Digest(content)) {
		t.Errorf("the applied descriptor %s, %v: want the binary pinned to %s", stored, err, connectorhost.Digest(content))
	}
	missing := strings.Replace(strings.Replace(unpinned, "0.1.1", "0.1.2", 1), env.downloads+"/destination-postgres", env.downloads+"/missing", 1)
	if err := applyErr(t, missing); err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("a descriptor whose binary isn't there: %v", err)
	}

	if err := applyErr(t, destinationDoc(name, `{"connector": "postgres@sha256:`+strings.Repeat("0", 64)+`", "config": {}}`)); err == nil {
		t.Error("applied a destination on a digest that isn't a version")
	}
	if err := applyErr(t, sourceDoc(name, `{"connector": "faker@1.0.0", "config": {}}`)); err == nil || !strings.Contains(err.Error(), "built in") {
		t.Errorf("a version of a built-in: %v", err)
	}
}

// A connector package is owner/name with roles. Its name stays with its
// repository unless a version is transferred; sources and destinations take
// version constraints, and a role it hasn't fails at apply. v1alpha1
// descriptors, one role each, make one version together.
func TestConnectorPackages(t *testing.T) {
	name := "acme/" + unique("pkg")
	pkg := func(version, repo string, roles ...string) string {
		pr := map[string]connectorhost.PackageRole{}
		for _, role := range roles {
			exe := role + "-postgres"
			pr[role] = connectorhost.PackageRole{Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: filepath.Join(env.connectors, exe), URL: env.downloads + "/" + exe}}}
		}
		doc, err := connectorhost.Package(t.Context(), name, version, spec.Info{Repository: repo}, pr)
		if err != nil {
			t.Fatal(err)
		}
		return string(doc)
	}
	first, other := "https://github.com/acme/first", "https://github.com/acme/other"
	res := apply(t, pkg("1.0.0", first, "source"), pkg("1.2.0", first, "source"))
	if res[0].GetName() != "connectors/"+name+"/versions/1.0.0" || res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Errorf("applied %v", res[0])
	}
	if err := applyErr(t, pkg("2.0.0", other, "source")); err == nil || !strings.Contains(err.Error(), "--transfer") {
		t.Errorf("a version from another repository: %v", err)
	}
	req := &apiv1.ApplyRequest{Documents: []*apiv1.Document{{Origin: "moved", Content: []byte(pkg("2.0.0", other, "source"))}}, Transfer: []string{name}}
	if _, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(t.Context(), connect.NewRequest(req)); err != nil {
		t.Errorf("a transferred version: %v", err)
	}
	if err := applyErr(t, pkg("1.0.0", first, "source", "destination")); err == nil || !strings.Contains(err.Error(), "new version") {
		t.Errorf("a role added to a v1alpha2 version: %v", err)
	}

	src := unique("pkg")
	apply(t, sourceDoc(src, fmt.Sprintf(`{"connector": "%s@1", "config": {"dsn": {"secret": "e2e-dsn"}}}`, name)))
	digest, err := query[string](t, "SELECT connector_digest FROM sources WHERE name = $1", src)
	if want, _ := query[string](t, "SELECT digest FROM connector_roles WHERE name = $1 AND version = '1.2.0'", name); err != nil || digest != want {
		t.Errorf("@1 resolved to %s, %v; want 1.2.0's %s", digest, err, want)
	}
	if err := applyErr(t, destinationDoc(src, fmt.Sprintf(`{"connector": %q, "config": {}}`, name))); err == nil || !strings.Contains(err.Error(), "no destination role") {
		t.Errorf("a destination on a package without one: %v", err)
	}
	if err := applyErr(t, sourceDoc(src, fmt.Sprintf(`{"connector": "%s@1.x", "config": {}}`, name))); err == nil {
		t.Error("a constraint that isn't one")
	}

	legacy := unique("legacy")
	v1alpha1 := func(role string) string {
		exe := filepath.Join(env.connectors, role+"-postgres")
		content, err := os.ReadFile(exe)
		if err != nil {
			t.Fatal(err)
		}
		caps := ""
		if role == "destination" {
			caps = `, "capabilities": {"class": "stage_and_apply"}`
		}
		return fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Connector", "metadata": {"name": %q, "version": "0.1.0"},
			"spec": {"binaries": {%q: {"url": %q, "digest": %q}}, "protocol": "v1", "kind": %q, "egress": {"tier": "raw"}, "config": {}%s}}`,
			legacy, connectorhost.Platform(), env.downloads+"/"+role+"-postgres", connectorhost.Digest(content), role, caps)
	}
	if res := apply(t, v1alpha1("source")); res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Errorf("a v1alpha1 source: %v", res[0])
	}
	if res := apply(t, v1alpha1("destination")); res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UPDATED {
		t.Errorf("a v1alpha1 destination of the same version: %v", res[0])
	}
	if res := apply(t, v1alpha1("destination")); res[0].GetAction() != apiv1.ApplyAction_APPLY_ACTION_UNCHANGED {
		t.Errorf("the destination again: %v", res[0])
	}
	if n, err := query[int64](t, "SELECT count(*) FROM connector_roles WHERE name = $1", "openetl/"+legacy); err != nil || n != 2 {
		t.Errorf("%d roles, %v", n, err)
	}
}
