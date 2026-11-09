package api

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/connectorhost/external"
	"github.com/openetl-io/openetl/internal/oci"
	"github.com/openetl-io/openetl/internal/pkgcatalog"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store/db"
)

// PackageOptions are how the control plane reaches registries and GitHub,
// through Server.Downloads.
type PackageOptions struct {
	// Registry hosts reached over plain HTTP, besides loopback ones.
	PlainHTTP []string
	// Empty: GitHub's.
	GitHubAPI string
	// Where official packages come from; empty: the official registry
	// namespace and repository.
	Official []string
}

// catalog is the org's catalog, reached through q.
func (s *Server) catalog(q *db.Queries) (*pkgcatalog.Catalog, error) {
	if s.Downloads == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this server doesn't reach registries or GitHub"))
	}
	return &pkgcatalog.Catalog{
		Queries: q, Org: s.Tenant.Org, Secrets: s.Secrets, HTTP: s.Downloads,
		GitHubAPI: s.Packages.GitHubAPI, PlainHTTP: s.Packages.PlainHTTP, Official: s.Packages.Official,
		Logger: s.log().With("component", "catalog"),
	}, nil
}

func sourceName(name string) string { return "connectorSources/" + name }

func (s *Server) applyConnectorSource(ctx context.Context, q *db.Queries, doc *spec.Document, ch *changes) (*apiv1.AppliedResource, error) {
	sp := doc.ConnectorSource
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: sourceName(doc.Name)}
	if sp.GetSecret() != "" {
		ws, secret, _ := strings.Cut(strings.TrimPrefix(sp.GetSecret(), "workspaces/"), "/secrets/")
		wsID, err := s.workspace(ctx, q, ws)
		if err != nil {
			return nil, err
		}
		r, err := q.GetSecret(ctx, db.GetSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: secret})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil, invalid("secret %s doesn't exist; set it with openetl secret set", sp.GetSecret())
		case err != nil:
			return nil, err
		case r.Ref != nil:
			return nil, invalid("secret %s is a reference that runners read, and the control plane lists sources with it, so it must hold its value", sp.GetSecret())
		}
	}
	cur, err := q.GetConnectorSource(ctx, db.GetConnectorSourceParams{OrgID: s.Tenant.Org, Name: doc.Name})
	switch {
	case err == nil && cur.Url == sp.GetUrl() && cur.Username == sp.GetUsername() && cur.Secret == sp.GetSecret():
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	case err == nil && cur.Official && cur.Url != sp.GetUrl():
		return nil, invalid("%s is the official connectors' source, whose URL doesn't change; delete it to stop listing them", res.Name)
	case err != nil && !errors.Is(err, pgx.ErrNoRows):
		return nil, err
	}
	created, err := q.UpsertConnectorSource(ctx, db.UpsertConnectorSourceParams{
		OrgID: s.Tenant.Org, Name: doc.Name, Url: sp.GetUrl(), Username: sp.GetUsername(), Secret: sp.GetSecret(),
	})
	if err != nil {
		return nil, err
	}
	res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
	if created {
		res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
	}
	ch.listSources = append(ch.listSources, doc.Name)
	return res, nil
}

// listSourcesSoon lists sources an apply connected or changed, in the
// background, so the catalog shows them before the next refresh.
func (s *Server) listSourcesSoon(names []string) {
	if len(names) == 0 || s.Downloads == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		c, err := s.catalog(s.Store.Queries)
		if err != nil {
			return
		}
		if _, err := c.Refresh(ctx); err != nil {
			s.log().Warn("listing connector sources failed", "sources", names, "err", err)
		}
	}()
}

func (s *Server) ListConnectorSources(ctx context.Context, _ *connect.Request[apiv1.ListConnectorSourcesRequest]) (*connect.Response[apiv1.ListConnectorSourcesResponse], error) {
	sources, err := s.connectorSources(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.ListConnectorSourcesResponse{Sources: sources}), nil
}

func (s *Server) connectorSources(ctx context.Context) ([]*apiv1.ConnectorSource, error) {
	rows, err := s.Store.ListConnectorSources(ctx, s.Tenant.Org)
	if err != nil {
		return nil, err
	}
	out := make([]*apiv1.ConnectorSource, len(rows))
	for i, r := range rows {
		cs := &apiv1.ConnectorSource{
			Name: sourceName(r.Name), Official: r.Official, Error: r.Error, Packages: r.Packages,
			Spec: &apiv1.ConnectorSourceSpec{Url: r.Url, Username: r.Username, Secret: r.Secret},
		}
		if r.RefreshTime != nil {
			cs.RefreshTime = timestamppb.New(*r.RefreshTime)
		}
		if r.ErrorTime != nil {
			cs.ErrorTime = timestamppb.New(*r.ErrorTime)
		}
		out[i] = cs
	}
	return out, nil
}

func (s *Server) DeleteConnectorSource(ctx context.Context, req *connect.Request[apiv1.DeleteConnectorSourceRequest]) (*connect.Response[apiv1.DeleteConnectorSourceResponse], error) {
	name, ok := strings.CutPrefix(req.Msg.GetName(), "connectorSources/")
	if !ok || !spec.ValidName(name) {
		return nil, invalid("name %q isn't connectorSources/{source}", req.Msg.GetName())
	}
	audit(ctx).touched(req.Msg.GetName())
	n, err := s.Store.DeleteConnectorSource(ctx, db.DeleteConnectorSourceParams{OrgID: s.Tenant.Org, Name: name})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	return connect.NewResponse(&apiv1.DeleteConnectorSourceResponse{}), nil
}

func (s *Server) RefreshCatalog(ctx context.Context, _ *connect.Request[apiv1.RefreshCatalogRequest]) (*connect.Response[apiv1.RefreshCatalogResponse], error) {
	c, err := s.catalog(s.Store.Queries)
	if err != nil {
		return nil, err
	}
	if _, err := c.Refresh(ctx); err != nil {
		return nil, err
	}
	sources, err := s.connectorSources(ctx)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RefreshCatalogResponse{Sources: sources}), nil
}

func catalogPackage(r db.GetCatalogPackageRow) *apiv1.CatalogPackage {
	owner, _ := spec.SplitName(r.Package)
	p := &apiv1.CatalogPackage{
		Name: "catalog/" + r.Source + "/" + r.Package, Source: sourceName(r.Source), Package: r.Package, Title: r.Title,
		Version: r.Version, Author: r.Author, Description: r.Description, License: r.License, Repository: r.Repository,
		Docs: r.Docs, Roles: r.Roles, Tags: r.Tags, Reference: r.Reference,
		Official: r.OfficialSource && owner == spec.OfficialOwner, Impostor: !r.OfficialSource && owner == spec.OfficialOwner,
	}
	if r.LogoDigest != nil {
		p.Logo = pkgcatalog.LogoPath(*r.LogoDigest)
	}
	for _, v := range r.Applied {
		if p.AppliedVersion == "" || oci.Newer(v, p.AppliedVersion) {
			p.AppliedVersion = v
		}
	}
	p.UpdateAvailable = p.AppliedVersion != "" && spec.CompareVersions(p.Version, p.AppliedVersion) > 0
	return p
}

func (s *Server) ListCatalog(ctx context.Context, _ *connect.Request[apiv1.ListCatalogRequest]) (*connect.Response[apiv1.ListCatalogResponse], error) {
	rows, err := s.Store.ListCatalog(ctx, s.Tenant.Org)
	if err != nil {
		return nil, err
	}
	out := &apiv1.ListCatalogResponse{}
	for _, r := range rows {
		out.Packages = append(out.Packages, catalogPackage(db.GetCatalogPackageRow(r)))
	}
	manual, err := s.manualPackages(ctx)
	if err != nil {
		return nil, err
	}
	out.Packages = append(out.Packages, manual...)
	return connect.NewResponse(out), nil
}

// manualSource stands for the source in the name of a package no connected
// source lists. No source can have this name.
const manualSource = "-"

// manualPackages are the packages the org has applied that no connected
// source lists, each at its highest applied version, sorted by name.
func (s *Server) manualPackages(ctx context.Context) ([]*apiv1.CatalogPackage, error) {
	rows, err := s.Store.ListManualConnectorVersions(ctx, s.Tenant.Org)
	if err != nil {
		return nil, err
	}
	var out []*apiv1.CatalogPackage
	highest := map[string]int{}
	for _, r := range rows {
		i, seen := highest[r.Name]
		if seen && !oci.Newer(r.Version, out[i].GetVersion()) {
			continue
		}
		_, short := spec.SplitName(r.Name)
		p := &apiv1.CatalogPackage{
			Name: "catalog/" + manualSource + "/" + r.Name, Package: r.Name, Title: cmp.Or(r.Title, short), Version: r.Version, Author: r.Author,
			Description: r.Description, License: r.License, Repository: r.Repository, Docs: r.Docs, Roles: r.Roles, Tags: r.Tags,
			AppliedVersion: r.Version, Manual: true,
		}
		if r.LogoDigest != nil {
			p.Logo = pkgcatalog.LogoPath(*r.LogoDigest)
		}
		if seen {
			out[i] = p
			continue
		}
		highest[r.Name] = len(out)
		out = append(out, p)
	}
	return out, nil
}

// manualCatalogPackage is a manual package with its roles' config fields,
// from what the org applied.
func (s *Server) manualCatalogPackage(ctx context.Context, name string) (*connect.Response[apiv1.GetCatalogPackageResponse], error) {
	manual, err := s.manualPackages(ctx)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(manual, func(p *apiv1.CatalogPackage) bool { return p.GetPackage() == name })
	if i < 0 {
		return nil, notFound("catalog/%s/%s isn't in the catalog", manualSource, name)
	}
	out := &apiv1.GetCatalogPackageResponse{Package: manual[i]}
	roles, err := s.Store.ListVersionRoles(ctx, db.ListVersionRolesParams{OrgID: s.Tenant.Org, Name: name, Version: manual[i].GetVersion()})
	if err != nil {
		return nil, err
	}
	for _, r := range roles {
		a, err := s.Store.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: r.Digest})
		if err != nil {
			return nil, err
		}
		v, err := connectorhost.ParseVersion(a.Content)
		if err != nil {
			return nil, fmt.Errorf("%s %s's %s: %w", name, manual[i].GetVersion(), r.Role, err)
		}
		out.Roles = append(out.Roles, &apiv1.CatalogRole{Role: r.Role, Config: configFields(v.Roles[r.Role].Role)})
	}
	return connect.NewResponse(out), nil
}

func (s *Server) GetCatalogPackage(ctx context.Context, req *connect.Request[apiv1.GetCatalogPackageRequest]) (*connect.Response[apiv1.GetCatalogPackageResponse], error) {
	parts := strings.Split(req.Msg.GetName(), "/")
	if len(parts) != 4 || parts[0] != "catalog" || (parts[1] != manualSource && !spec.ValidName(parts[1])) || !spec.ValidPackageName(parts[2]+"/"+parts[3]) {
		return nil, invalid("name %q isn't catalog/{source}/{owner}/{name}", req.Msg.GetName())
	}
	if parts[1] == manualSource {
		return s.manualCatalogPackage(ctx, parts[2]+"/"+parts[3])
	}
	r, err := s.Store.GetCatalogPackage(ctx, db.GetCatalogPackageParams{OrgID: s.Tenant.Org, Source: parts[1], Package: parts[2] + "/" + parts[3]})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s isn't in the catalog", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	out := &apiv1.GetCatalogPackageResponse{Package: catalogPackage(r)}
	c, err := s.catalog(s.Store.Queries)
	if err != nil {
		return nil, err
	}
	doc, err := c.Descriptor(ctx, r)
	if err != nil {
		return nil, connect.NewError(connect.CodeUnavailable, fmt.Errorf("%s's descriptor: %w", r.Reference, err))
	}
	for _, role := range doc.Descriptor.Kinds() {
		out.Roles = append(out.Roles, &apiv1.CatalogRole{Role: role, Config: configFields(doc.Descriptor.Roles[role])})
	}
	return connect.NewResponse(out), nil
}

// configFields are a role's config's top-level fields, from its JSON Schema.
func configFields(r *spec.Role) []*apiv1.ConfigField {
	schema := r.Spec.GetConfigSchema()
	if r.Manifest != nil {
		var m struct {
			Config json.RawMessage `json:"config"`
		}
		if json.Unmarshal(r.Manifest, &m) != nil {
			return nil
		}
		schema = m.Config
	}
	var s struct {
		Properties map[string]struct {
			Type        any    `json:"type"`
			Title       string `json:"title"`
			Description string `json:"description"`
			Secret      bool   `json:"x-secret"`
		} `json:"properties"`
		Required []string `json:"required"`
	}
	if json.Unmarshal(schema, &s) != nil {
		return nil
	}
	secret := map[string]bool{}
	if names, err := external.SecretProperties(schema); err == nil {
		for _, n := range names {
			secret[n] = true
		}
	}
	var out []*apiv1.ConfigField
	for name, p := range s.Properties {
		f := &apiv1.ConfigField{Name: name, Title: p.Title, Description: p.Description, Required: slices.Contains(s.Required, name), Secret: p.Secret || secret[name]}
		switch t := p.Type.(type) {
		case string:
			f.Type = t
		case []any:
			for _, v := range t {
				if v, ok := v.(string); ok && v != "null" {
					f.Type = v
					break
				}
			}
		}
		out = append(out, f)
	}
	slices.SortFunc(out, func(a, b *apiv1.ConfigField) int {
		if a.Required != b.Required {
			if a.Required {
				return -1
			}
			return 1
		}
		return strings.Compare(a.Name, b.Name)
	})
	return out
}

// serveLogo serves a logo artifact by digest, to anyone: a third party's
// image, sandboxed so an SVG opened directly runs nothing.
func (s *Server) serveLogo(w http.ResponseWriter, r *http.Request) {
	digest := r.PathValue("digest")
	if !strings.HasPrefix(digest, "sha256:") || len(digest) != len("sha256:")+64 {
		http.NotFound(w, r)
		return
	}
	l, err := s.Store.GetLogo(r.Context(), db.GetLogoParams{OrgID: s.Tenant.Org, Digest: digest})
	if err != nil {
		http.NotFound(w, r)
		return
	}
	h := w.Header()
	h.Set("Content-Type", l.MediaType)
	h.Set("Content-Security-Policy", "sandbox; default-src 'none'; style-src 'unsafe-inline'")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cross-Origin-Resource-Policy", "same-origin")
	h.Set("Cache-Control", "public, max-age=31536000, immutable")
	_, _ = w.Write(l.Content)
}

func (s *Server) GetFetchCredential(ctx context.Context, req *connect.Request[dataplanev1.GetFetchCredentialRequest]) (*connect.Response[dataplanev1.GetFetchCredentialResponse], error) {
	u := req.Msg.GetUrl()
	if !strings.HasPrefix(u, oci.Scheme) && !strings.HasPrefix(u, "https://github.com/") {
		return nil, invalid("url %q isn't an oci:// or https://github.com/ URL", u)
	}
	c := &pkgcatalog.Catalog{Queries: s.Store.Queries, Org: s.Tenant.Org, Secrets: s.Secrets}
	cred, err := c.Credential(ctx, u)
	if err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	if cred.Source != "" {
		audit(ctx).touched(sourceName(cred.Source))
	}
	return connect.NewResponse(&dataplanev1.GetFetchCredentialResponse{Source: cred.Source, Username: cred.Username, Password: cred.Password}), nil
}
