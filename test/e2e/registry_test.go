//go:build e2e

package e2e

import (
	"cmp"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorpkg"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/spec"
)

const registryLogo = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"><rect width="1" height="1"/></svg>`

// A package published to an OCI registry is listed from its connected
// namespace, applied by reference, and run: the runner fetches its binary
// from the registry by digest. The compose registry (make deps-up), or
// OPENETL_E2E_REGISTRY.
func TestConnectorFromRegistry(t *testing.T) {
	ctx := t.Context()
	host := cmp.Or(os.Getenv("OPENETL_E2E_REGISTRY"), "127.0.0.1:5001")
	ns, err := oci.Parse(oci.Scheme + host + "/" + unique("e2e") + "/connectors")
	if err != nil {
		t.Fatal(err)
	}
	short := unique("pg")
	name := "acme/" + short
	dist := t.TempDir()
	exe, err := os.ReadFile(filepath.Join(env.connectors, "destination-postgres"))
	if err != nil {
		t.Fatal(err)
	}
	asset := filepath.Join(dist, connectorpkg.Asset(short, "destination", connectorhost.Platform()))
	if err := os.WriteFile(asset, exe, 0o700); err != nil {
		t.Fatal(err)
	}
	logo := connectorpkg.LogoAsset(short, "image/svg+xml")
	if err := os.WriteFile(filepath.Join(dist, logo), []byte(registryLogo), 0o600); err != nil {
		t.Fatal(err)
	}
	release := "https://github.com/acme/" + short + "/releases/download/v1.0.0/"
	doc, err := connectorhost.Package(ctx, name, "1.0.0", spec.Info{Title: "Postgres, from a registry", Repository: "https://github.com/acme/" + short, Logo: release + logo},
		map[string]connectorhost.PackageRole{"destination": {Executables: []connectorhost.Executable{{Platform: connectorhost.Platform(), Path: asset, URL: release + filepath.Base(asset)}}}})
	if err != nil {
		t.Fatal(err)
	}
	descriptor := filepath.Join(dist, connectorpkg.DescriptorAsset(short))
	if err := os.WriteFile(descriptor, doc, 0o600); err != nil {
		t.Fatal(err)
	}
	pub, err := connectorpkg.Publish(ctx, &oci.Client{HTTP: &http.Client{Timeout: time.Minute}}, ns, descriptor)
	if err != nil {
		t.Fatalf("publish to %s (is the compose registry up?): %v", ns, err)
	}
	if !pub.Indexed {
		t.Errorf("published %+v", pub)
	}

	source := unique("registry")
	apply(t, `{"apiVersion": "openetl.io/v1alpha1", "kind": "ConnectorSource", "metadata": {"name": "`+source+`"}, "spec": {"url": "`+ns.String()+`"}}`)
	conns := apiv1connect.NewConnectorServiceClient(env.http, env.apiURL)
	if _, err := conns.RefreshCatalog(ctx, connect.NewRequest(&apiv1.RefreshCatalogRequest{})); err != nil {
		t.Fatal(err)
	}
	list, err := conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	var pkg *apiv1.CatalogPackage
	for _, p := range list.Msg.GetPackages() {
		if p.GetSource() == "connectorSources/"+source {
			pkg = p
		}
	}
	if pkg == nil || pkg.GetPackage() != name || pkg.GetVersion() != "1.0.0" || pkg.GetTitle() != "Postgres, from a registry" ||
		pkg.GetReference() != ns.Child(short).String()+":1.0.0" || pkg.GetLogo() == "" || pkg.GetAppliedVersion() != "" {
		t.Fatalf("the catalog's package %v", pkg)
	}
	resp, err := env.http.Get(env.apiURL + pkg.GetLogo())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != registryLogo {
		t.Errorf("logo %s: %q", resp.Status, body)
	}

	res, err := apiv1connect.NewApplyServiceClient(env.http, env.apiURL).Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{
		Client: "cli", Documents: []*apiv1.Document{{Reference: pkg.GetReference()}},
	}))
	if err != nil {
		t.Fatal(err)
	}
	if r := res.Msg.GetResources()[0]; r.GetName() != "connectors/"+name+"/versions/1.0.0" || r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Errorf("applied %v", r)
	}
	provenance, err := query[string](t, "SELECT provenance FROM connector_versions WHERE name = $1", name)
	if err != nil || !strings.HasPrefix(provenance, "cli:"+ns.Child(short).String()+"@sha256:") {
		t.Errorf("provenance %q, %v", provenance, err)
	}

	startRunner(t)
	pipe := unique("from-registry")
	docs := pipelineDocs(pipe, `{"seed": 5, "count": 20}`, `{}`)
	docs[1] = strings.Replace(docs[1], `"connector": "postgres"`, `"connector": "`+name+`@1"`, 1)
	apply(t, docs...)
	if status, stats := waitSync(t, pipe, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	if n, err := query[int64](t, "SELECT count(*) FROM "+schema(pipe)+".users"); err != nil || n == 0 {
		t.Errorf("%d rows landed, %v", n, err)
	}
	digest := connectorhost.Digest(exe)
	if _, err := os.Stat(filepath.Join(env.cache, "executables", strings.Replace(digest, ":", "-", 1))); err != nil {
		t.Errorf("the runner didn't cache the registry's binary %s: %v", digest, err)
	}
	list, _ = conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	for _, p := range list.Msg.GetPackages() {
		if p.GetPackage() == name && p.GetAppliedVersion() != "1.0.0" {
			t.Errorf("applied version %q", p.GetAppliedVersion())
		}
	}
}
