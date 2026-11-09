//go:build integration

package api_test

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/uuid"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/api"
	"github.com/openetl-io/openetl/internal/auth"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/pkgcatalog"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const catalogLogo = `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1 1"><script>alert(1)</script><rect width="1" height="1"/></svg>`

// testDescriptor is a package version's descriptor as canonical JSON, with a
// binary for a platform no test runs.
func testDescriptor(t *testing.T, name, version, binURL, logo string) []byte {
	t.Helper()
	s := &protocolv1.SpecResponse{
		ProtocolVersion: "v1", Kind: protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE, Egress: &protocolv1.Egress{Tier: protocolv1.EgressTier_EGRESS_TIER_RAW},
		ConfigSchema: []byte(`{"type":"object","required":["dsn"],"properties":{"dsn":{"type":"string","title":"DSN","x-secret":true},"schema":{"type":["string","null"]}}}`),
		Source:       &protocolv1.SourceCapabilities{},
	}
	digest := "sha256:" + strings.Repeat("a", 64)
	if d, ok := spec.OCIBlobURL(binURL); ok {
		digest = d
	}
	d := &spec.Descriptor{Roles: map[string]*spec.Role{"source": {
		Requires: spec.Requires{Protocol: "v1"}, Spec: s,
		Binaries: map[string]spec.Binary{"plan9/386": {URL: binURL, Digest: digest}},
	}}}
	b, err := spec.EncodeDescriptor(name, version, spec.Info{Title: "Title of " + name, Repository: "https://github.com/acme/conn", Logo: logo, Description: "d"}, d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// fakeGitHub serves acme/conn's releases, as GitHub's API does, and records
// the tokens it's sent.
type fakeGitHub struct {
	*httptest.Server
	mu     sync.Mutex
	tokens []string
	assets map[string][]byte
}

func newFakeGitHub(t *testing.T, files, impostor []byte) *fakeGitHub {
	g := &fakeGitHub{assets: map[string][]byte{"1": files, "2": impostor, "3": []byte(catalogLogo)}}
	g.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g.mu.Lock()
		g.tokens = append(g.tokens, r.Header.Get("Authorization"))
		g.mu.Unlock()
		asset := func(id, name string) map[string]any {
			return map[string]any{"id": json.Number(id), "name": name, "url": g.URL + "/repos/acme/conn/releases/assets/" + id,
				"browser_download_url": "https://github.com/acme/conn/releases/download/x/" + name, "updated_at": "2026-11-08T00:00:00Z"}
		}
		files := map[string]any{"tag_name": "files/v1.0.0", "html_url": "https://github.com/acme/conn/releases/tag/files/v1.0.0",
			"assets": []any{asset("1", "files.connector.yaml"), asset("3", "files.logo.svg")}}
		old := map[string]any{"tag_name": "files/v0.9.0", "html_url": "x", "assets": []any{asset("9", "files.connector.yaml")}}
		imp := map[string]any{"tag_name": "postgres/v9.0.0", "html_url": "https://github.com/acme/conn/releases/tag/postgres/v9.0.0",
			"assets": []any{asset("2", "postgres.connector.yaml")}}
		switch {
		case r.URL.Path == "/repos/acme/conn/releases":
			_ = json.NewEncoder(w).Encode([]any{imp, files, old})
		case r.URL.Path == "/repos/acme/conn/releases/tags/files/v1.0.0":
			_ = json.NewEncoder(w).Encode(files)
		case r.URL.Path == "/repos/acme/conn/releases/tags/postgres/v9.0.0":
			_ = json.NewEncoder(w).Encode(imp)
		case strings.HasPrefix(r.URL.Path, "/repos/acme/conn/releases/assets/") && r.Header.Get("Accept") == "application/octet-stream":
			b, ok := g.assets[strings.TrimPrefix(r.URL.Path, "/repos/acme/conn/releases/assets/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(g.Close)
	return g
}

func TestCatalog(t *testing.T) {
	ctx := t.Context()
	reg := httptest.NewServer(registry.New(registry.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(reg.Close)
	host := strings.TrimPrefix(reg.URL, "http://")
	official, err := oci.Parse(oci.Scheme + host + "/openetl-io/openetl-connectors")
	if err != nil {
		t.Fatal(err)
	}
	c := &oci.Client{HTTP: reg.Client()}
	// The official postgres, published as connectorpkg.Publish would.
	repo := official.Child("postgres")
	bin := []byte("binary")
	logoURL := oci.BlobURL(repo, []byte(catalogLogo))
	desc := testDescriptor(t, "openetl/postgres", "1.0.0", oci.BlobURL(repo, bin), logoURL)
	if _, err := c.Push(ctx, repo, oci.Version{Name: "openetl/postgres", Version: "1.0.0", Descriptor: desc,
		Binaries: []oci.Blob{{MediaType: oci.MediaTypeBinary, Data: bin}}, Logo: &oci.Blob{MediaType: "image/svg+xml", Data: []byte(catalogLogo)}}); err != nil {
		t.Fatal(err)
	}
	card := oci.Card{Name: "openetl/postgres", Title: "PostgreSQL", Version: "1.0.0", Roles: []string{"source"}, Repository: repo.String()}
	if _, err := c.UpdateIndex(ctx, official, card, &oci.Blob{MediaType: "image/svg+xml", Data: []byte(catalogLogo)}); err != nil {
		t.Fatal(err)
	}
	gh := newFakeGitHub(t,
		[]byte(strings.Replace(string(testDescriptor(t, "acme/files", "1.0.0", "https://github.com/acme/conn/releases/download/files/v1.0.0/files-source-plan9-386", "https://github.com/acme/conn/releases/download/files/v1.0.0/files.logo.svg")), `"title":"Title of acme/files"`, `"title":"Files"`, 1)),
		testDescriptor(t, "openetl/postgres", "9.0.0", "https://github.com/acme/conn/releases/download/postgres/v9.0.0/x", ""))

	url := storetest.NewDatabase(t)
	st, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	bootstrap := func() store.Tenant {
		tenant, err := st.Bootstrap(ctx, "dp1", func(q *db.Queries, org uuid.UUID) error {
			return pkgcatalog.ConnectOfficial(ctx, q, org, official.String())
		})
		if err != nil {
			t.Fatal(err)
		}
		return tenant
	}
	tenant := bootstrap()
	keys, err := secrets.Open(ctx, "base64key://"+base64.URLEncoding.EncodeToString([]byte(strings.Repeat("k", 32))), st, []uuid.UUID{tenant.Org})
	if err != nil {
		t.Fatal(err)
	}
	var token auth.Credential
	if err := st.Tx(ctx, func(q *db.Queries) error {
		token, _, err = store.CreateRunnerToken(ctx, q, tenant.Org, "dp1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	srv := &api.Server{
		Store: st, Tenant: tenant, Schedules: &refreshes{}, DefaultDataPlane: "dp1", Authless: true, Secrets: keys,
		Downloads: http.DefaultClient, Packages: api.PackageOptions{GitHubAPI: gh.URL, Official: []string{official.String()}},
		Logger: slog.New(slog.DiscardHandler),
	}
	h, err := srv.Handler()
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(h)
	t.Cleanup(ts.Close)
	conns := apiv1connect.NewConnectorServiceClient(http.DefaultClient, ts.URL)
	applier := apiv1connect.NewApplyServiceClient(http.DefaultClient, ts.URL)
	runner := dataplanev1connect.NewDataPlaneServiceClient(http.DefaultClient, ts.URL, auth.WithBearer(token.Secret))

	// The official source is connected with the org, and lists the index.
	res, err := conns.RefreshCatalog(ctx, connect.NewRequest(&apiv1.RefreshCatalogRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if s := res.Msg.GetSources(); len(s) != 1 || s[0].GetName() != "connectorSources/official" || !s[0].GetOfficial() || s[0].GetPackages() != 1 || s[0].GetError() != "" {
		t.Fatalf("sources %v", s)
	}
	list, err := conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	pg := list.Msg.GetPackages()[0]
	if pg.GetPackage() != "openetl/postgres" || pg.GetTitle() != "PostgreSQL" || !pg.GetOfficial() || pg.GetReference() != repo.String()+":1.0.0" ||
		!strings.HasPrefix(pg.GetLogo(), "/logos/sha256:") || pg.GetAppliedVersion() != "" {
		t.Errorf("official package %v", pg)
	}
	resp, err := http.Get(ts.URL + pg.GetLogo())
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != catalogLogo || resp.Header.Get("Content-Type") != "image/svg+xml" || !strings.Contains(resp.Header.Get("Content-Security-Policy"), "sandbox") ||
		resp.Header.Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("logo %s %v %q", resp.Status, resp.Header, body)
	}
	missing, err := http.Get(ts.URL + "/logos/sha256:" + strings.Repeat("0", 64))
	if err != nil {
		t.Fatal(err)
	}
	_ = missing.Body.Close()
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("a missing logo: %s", missing.Status)
	}

	got, err := conns.GetCatalogPackage(ctx, connect.NewRequest(&apiv1.GetCatalogPackageRequest{Name: pg.GetName()}))
	if err != nil {
		t.Fatal(err)
	}
	if r := got.Msg.GetRoles(); len(r) != 1 || r[0].GetRole() != "source" || len(r[0].GetConfig()) != 2 {
		t.Fatalf("roles %v", r)
	} else if f := r[0].GetConfig()[0]; f.GetName() != "dsn" || !f.GetRequired() || !f.GetSecret() || f.GetTitle() != "DSN" || r[0].GetConfig()[1].GetType() != "string" {
		t.Errorf("config fields %v", r[0].GetConfig())
	}

	// Applied by reference, a version records the manifest as its
	// provenance, and its logo is served.
	ap, err := applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Reference: pg.GetReference()}}}))
	if err != nil {
		t.Fatal(err)
	}
	if r := ap.Msg.GetResources()[0]; r.GetName() != "connectors/openetl/postgres/versions/1.0.0" || r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED || r.GetVersion() != connectorhost.Digest(desc) {
		t.Errorf("applied %v; want the descriptor's digest %s", r, connectorhost.Digest(desc))
	}
	v, err := st.GetConnectorVersion(ctx, db.GetConnectorVersionParams{OrgID: tenant.Org, Name: "openetl/postgres", Version: "1.0.0"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(v.Provenance, "api:"+repo.String()+"@sha256:") || v.LogoDigest == nil || v.Title != "Title of openetl/postgres" {
		t.Errorf("version %+v", v)
	}
	list, _ = conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	if a := list.Msg.GetPackages()[0].GetAppliedVersion(); a != "1.0.0" {
		t.Errorf("applied version %q", a)
	}

	// A GitHub repository, with a token.
	if _, err := apiv1connect.NewSecretServiceClient(http.DefaultClient, ts.URL).UpdateSecret(ctx, connect.NewRequest(&apiv1.UpdateSecretRequest{
		Secret: &apiv1.Secret{Name: "workspaces/default/secrets/gh", Value: "tok"}, AllowMissing: true,
	})); err != nil {
		t.Fatal(err)
	}
	src := "apiVersion: openetl.io/v1alpha1\nkind: ConnectorSource\nmetadata: {name: acme}\nspec: {url: \"https://github.com/acme/conn\", secret: gh}\n"
	ap, err = applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Content: []byte(src)}}}))
	if err != nil {
		t.Fatal(err)
	}
	if r := ap.Msg.GetResources()[0]; r.GetName() != "connectorSources/acme" || r.GetAction() != apiv1.ApplyAction_APPLY_ACTION_CREATED {
		t.Errorf("applied %v", r)
	}
	if _, err := conns.RefreshCatalog(ctx, connect.NewRequest(&apiv1.RefreshCatalogRequest{})); err != nil {
		t.Fatal(err)
	}
	list, _ = conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	byName := map[string]*apiv1.CatalogPackage{}
	for _, p := range list.Msg.GetPackages() {
		byName[p.GetName()] = p
	}
	files := byName["catalog/acme/acme/files"]
	if files == nil || files.GetVersion() != "1.0.0" || files.GetTitle() != "Files" || files.GetReference() != "github.com/acme/conn/files@1.0.0" || files.GetLogo() != pg.GetLogo() {
		t.Errorf("GitHub package %v of %v", files, list.Msg.GetPackages())
	}
	if imp := byName["catalog/acme/openetl/postgres"]; imp == nil || !imp.GetImpostor() || imp.GetOfficial() {
		t.Errorf("an openetl package from another source %v", imp)
	}
	gh.mu.Lock()
	for _, tok := range gh.tokens {
		if tok != "Bearer tok" {
			t.Errorf("GitHub was sent %q", tok)
		}
	}
	gh.mu.Unlock()
	if _, err := applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Reference: "github.com/acme/conn/postgres@9.0.0"}}})); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Errorf("an openetl package from elsewhere: %v", err)
	}
	ap, err = applier.Apply(ctx, connect.NewRequest(&apiv1.ApplyRequest{Documents: []*apiv1.Document{{Reference: files.GetReference()}}}))
	if err != nil {
		t.Fatal(err)
	}
	if r := ap.Msg.GetResources()[0]; r.GetName() != "connectors/acme/files/versions/1.0.0" {
		t.Errorf("applied %v", r)
	}

	// Runners get the credential of the longest source that covers a URL.
	for u, want := range map[string]string{
		"https://github.com/acme/conn/releases/download/files/v1.0.0/x": "acme",
		"https://github.com/acme/connx/releases/download/v1/x":          "",
		repo.String() + "@sha256:" + strings.Repeat("a", 64):            "",
	} {
		cred, err := runner.GetFetchCredential(ctx, connect.NewRequest(&dataplanev1.GetFetchCredentialRequest{Url: u}))
		if err != nil {
			t.Fatal(err)
		}
		if m := cred.Msg; m.GetSource() != want || (want != "") != (m.GetPassword() == "tok") {
			t.Errorf("%s: %v", u, m)
		}
	}

	// A deleted official source stays deleted.
	if _, err := conns.DeleteConnectorSource(ctx, connect.NewRequest(&apiv1.DeleteConnectorSourceRequest{Name: "connectorSources/official"})); err != nil {
		t.Fatal(err)
	}
	bootstrap()
	sources, err := conns.ListConnectorSources(ctx, connect.NewRequest(&apiv1.ListConnectorSourcesRequest{}))
	if err != nil {
		t.Fatal(err)
	}
	if s := sources.Msg.GetSources(); len(s) != 1 || s[0].GetName() != "connectorSources/acme" {
		t.Errorf("sources after deleting the official one %v", s)
	}
	list, _ = conns.ListCatalog(ctx, connect.NewRequest(&apiv1.ListCatalogRequest{}))
	for _, p := range list.Msg.GetPackages() {
		if p.GetSource() == "connectorSources/official" {
			t.Errorf("a deleted source's package %v", p)
		}
	}
}
