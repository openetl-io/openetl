package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
)

func versioned(doc *spec.Document) bool {
	return doc.Kind == spec.KindConnector || doc.Kind == spec.KindDeclarativeSource
}

// roles are a connector version document's roles, sorted.
func roles(doc *spec.Document) []string {
	if doc.Kind == spec.KindConnector {
		return doc.Descriptor.Kinds()
	}
	return []string{spec.RoleSource}
}

// pinBinaries downloads the binaries whose digests descriptors leave out,
// to pin them. It runs before the transaction, since it downloads.
func (s *Server) pinBinaries(ctx context.Context, docs []*spec.Document, origins []string) error {
	for i, doc := range docs {
		if doc.Kind != spec.KindConnector || len(doc.Descriptor.Unpinned()) == 0 {
			continue
		}
		if s.Downloads == nil {
			return invalid("%s: this server doesn't download binaries, so each needs its digest", origins[i])
		}
		if err := connectorhost.PinBinaries(ctx, s.Downloads, doc.Descriptor); err != nil {
			return invalid("%s: %v", origins[i], err)
		}
	}
	return nil
}

// applyConnectorVersion stores a connector version. Versions are immutable:
// applying one again with other content fails, except that a one-role
// artifact may add its role to a version without it.
func (s *Server) applyConnectorVersion(ctx context.Context, q *db.Queries, doc *spec.Document, provenance string, transfer map[string]bool) (*apiv1.AppliedResource, error) {
	content := doc.Manifest
	media := connectorhost.MediaTypeManifest
	if doc.Kind == spec.KindConnector {
		var err error
		if content, err = spec.EncodeVersion(doc); err != nil {
			return nil, invalid("%v", err)
		}
		media = connectorhost.MediaTypeDescriptor
	}
	v, err := connectorhost.ParseVersion(content)
	if err != nil {
		return nil, invalid("%v", err)
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: connectorName(v.Name, v.Version), Version: v.Digest}
	cur, err := q.GetConnectorVersion(ctx, db.GetConnectorVersionParams{OrgID: s.Tenant.Org, Name: v.Name, Version: v.Version})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		cur = db.ConnectorVersion{}
	case err != nil:
		return nil, err
	}
	have, err := q.ListVersionRoles(ctx, db.ListVersionRolesParams{OrgID: s.Tenant.Org, Name: v.Name, Version: v.Version})
	if err != nil {
		return nil, err
	}
	digests := map[string]string{}
	for _, r := range have {
		digests[r.Role] = r.Digest
	}
	var missing []string
	for role := range v.Roles {
		switch d, ok := digests[role]; {
		case !ok:
			missing = append(missing, role)
		case d != v.Digest:
			return nil, invalid("%s %s's %s was applied with other content; versions don't change, so give this one a new version", v.Name, v.Version, role)
		}
	}
	slices.Sort(missing)
	switch {
	case len(missing) == 0:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	case len(have) > 0 && !v.OneRole:
		return nil, invalid("%s %s was applied without the %s role; versions don't change, so give this one a new version", v.Name, v.Version, strings.Join(missing, " and "))
	case len(have) > 0 && !sameInfo(cur, v.Info):
		return nil, invalid("%s %s was applied with other metadata; versions don't change, so give this one a new version", v.Name, v.Version)
	}
	if len(have) == 0 {
		if err := s.checkRepository(ctx, q, v, transfer); err != nil {
			return nil, err
		}
		i := v.Info
		err := q.InsertConnectorVersion(ctx, db.InsertConnectorVersionParams{
			OrgID: s.Tenant.Org, Name: v.Name, Version: v.Version, Provenance: provenance,
			Description: i.Description, License: i.License, Repository: i.Repository, Docs: i.Docs, Tags: nonNil(i.Tags),
			Author: i.Author, Logo: i.Logo, ReleaseTag: i.ReleaseTag, ReleaseUrl: i.ReleaseURL,
		})
		if err != nil {
			return nil, err
		}
	}
	if err := q.InsertArtifact(ctx, db.InsertArtifactParams{OrgID: s.Tenant.Org, Digest: v.Digest, MediaType: media, Content: content}); err != nil {
		return nil, err
	}
	for _, role := range missing {
		if err := q.InsertConnectorRole(ctx, db.InsertConnectorRoleParams{OrgID: s.Tenant.Org, Name: v.Name, Version: v.Version, Role: role, Digest: v.Digest}); err != nil {
			return nil, err
		}
	}
	res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
	if len(have) > 0 {
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UPDATED
	}
	return res, nil
}

// checkRepository keeps a package's name with its repository: a version
// from another repository than the latest that names one needs a transfer.
func (s *Server) checkRepository(ctx context.Context, q *db.Queries, v *connectorhost.Version, transfer map[string]bool) error {
	repo, err := q.LatestRepository(ctx, db.LatestRepositoryParams{OrgID: s.Tenant.Org, Name: v.Name})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return err
	case repo == v.Info.Repository || transfer[v.Name]:
		return nil
	case v.Info.Repository == "":
		return invalid("%s's versions come from %s, and %s names no repository; apply it with --transfer %s to take the name over", v.Name, repo, v.Version, v.Name)
	}
	return invalid("%s's versions come from %s, not %s; apply it with --transfer %s to move the name", v.Name, repo, v.Info.Repository, v.Name)
}

func sameInfo(cur db.ConnectorVersion, i spec.Info) bool {
	return cur.Description == i.Description && cur.License == i.License && cur.Repository == i.Repository && cur.Docs == i.Docs &&
		slices.Equal(cur.Tags, i.Tags) && cur.Author == i.Author && cur.Logo == i.Logo && cur.ReleaseTag == i.ReleaseTag && cur.ReleaseUrl == i.ReleaseURL
}

// connectorName is a connector version's resource name; name is full.
func connectorName(name, version string) string {
	return fmt.Sprintf("connectors/%s/versions/%s", name, version)
}

// resolveConnector finds the connector version a source or destination
// names, and checks it has the role: postgres for the highest version (a
// prerelease only if there's nothing else), postgres@1 or postgres@1.4 for
// the highest of those, postgres@1.2.0, or postgres@sha256:…. It returns
// nil for built-ins, and for names without versions, which a runner looks
// for in its connectors directory.
func (s *Server) resolveConnector(ctx context.Context, q *db.Queries, role, name, ref string) (*connectorhost.Version, error) {
	if connectorhost.Builtin(role, name) {
		if ref != "" {
			return nil, invalid("%s connector %s is built in, so it has no versions", role, name)
		}
		return nil, nil
	}
	if !spec.ValidPackageName(name) {
		return nil, invalid("connector %q isn't owner/name or name", name)
	}
	full := spec.FullName(name)
	rows, err := q.ListConnectorRoles(ctx, db.ListConnectorRolesParams{OrgID: s.Tenant.Org, Name: full, Role: role})
	if err != nil {
		return nil, err
	}
	pick, err := pickVersion(rows, ref)
	if err != nil {
		return nil, err
	}
	if pick == nil {
		versions, err := q.ListConnectorVersions(ctx, db.ListConnectorVersionsParams{OrgID: s.Tenant.Org, Name: full})
		if err != nil {
			return nil, err
		}
		if ref == "" && len(versions) == 0 {
			return nil, nil
		}
		if len(rows) == 0 && len(versions) > 0 {
			return nil, invalid("connector %s has no %s role", name, role)
		}
		applied := make([]string, len(rows))
		for i, r := range rows {
			applied[i] = r.Version
		}
		slices.SortFunc(applied, spec.CompareVersions)
		return nil, invalid("%s connector %s has no version %s; applied: %s", role, name, ref, strings.Join(applied, ", "))
	}
	a, err := q.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: pick.Digest})
	if err != nil {
		return nil, err
	}
	return connectorhost.ParseVersion(a.Content)
}

// pickVersion is the version ref names among rows, or nil: a digest, an
// exact version, MAJOR or MAJOR.MINOR for the highest release with that
// prefix, or the highest version for "".
func pickVersion(rows []db.ListConnectorRolesRow, ref string) (*db.ListConnectorRolesRow, error) {
	var prefix string
	switch parts := strings.Split(ref, "."); {
	case ref == "", strings.HasPrefix(ref, "sha256:"), spec.ValidVersion(ref):
	case len(parts) <= 2 && spec.ValidVersion(ref+strings.Repeat(".0", 3-len(parts))):
		prefix = ref + "."
	default:
		return nil, invalid("connector version %q isn't a version (1.2.0), a major or minor version (1 or 1.2), or a digest (sha256:…)", ref)
	}
	var pick *db.ListConnectorRolesRow
	for i, r := range rows {
		switch {
		case strings.HasPrefix(ref, "sha256:"):
			if r.Digest == ref {
				pick = &rows[i]
			}
		case prefix != "":
			if strings.HasPrefix(r.Version, prefix) && !spec.Prerelease(r.Version) && (pick == nil || spec.CompareVersions(r.Version, pick.Version) > 0) {
				pick = &rows[i]
			}
		case ref != "":
			if r.Version == ref {
				pick = &rows[i]
			}
		case pick == nil, spec.Prerelease(pick.Version) && !spec.Prerelease(r.Version),
			spec.Prerelease(pick.Version) == spec.Prerelease(r.Version) && spec.CompareVersions(r.Version, pick.Version) > 0:
			pick = &rows[i]
		}
	}
	return pick, nil
}

func (s *Server) GetArtifact(ctx context.Context, req *connect.Request[dataplanev1.GetArtifactRequest]) (*connect.Response[dataplanev1.GetArtifactResponse], error) {
	a, err := s.Store.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: req.Msg.GetDigest()})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("no artifact %s", req.Msg.GetDigest())
	}
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.GetArtifactResponse{MediaType: a.MediaType, Content: a.Content}), nil
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
