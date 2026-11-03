package api

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store/db"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

func versioned(doc *spec.Document) bool {
	return doc.Kind == spec.KindConnector || doc.Kind == spec.KindDeclarativeSource
}

// connectorKind is a connector version's kind: source or destination.
func connectorKind(doc *spec.Document) string {
	if doc.Kind == spec.KindConnector && doc.Descriptor.Spec.GetKind() == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		return "destination"
	}
	return "source"
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
// applying one again with other content fails.
func (s *Server) applyConnectorVersion(ctx context.Context, q *db.Queries, doc *spec.Document, provenance string) (*apiv1.AppliedResource, error) {
	content := doc.Manifest
	media := connectorhost.MediaTypeManifest
	if doc.Kind == spec.KindConnector {
		var err error
		if content, err = spec.EncodeDescriptor(doc.Name, doc.Version, doc.Info, doc.Descriptor); err != nil {
			return nil, invalid("%v", err)
		}
		media = connectorhost.MediaTypeDescriptor
	}
	v, err := connectorhost.ParseVersion(content)
	if err != nil {
		return nil, invalid("%v", err)
	}
	res := &apiv1.AppliedResource{Kind: doc.Kind, Name: connectorName(v.Kind, v.Name, v.Version), Version: v.Digest}
	cur, err := q.GetConnectorVersion(ctx, db.GetConnectorVersionParams{OrgID: s.Tenant.Org, Kind: v.Kind, Name: v.Name, Version: v.Version})
	switch {
	case err == nil && cur.Digest == v.Digest:
		res.Action = apiv1.ApplyAction_APPLY_ACTION_UNCHANGED
		return res, nil
	case err == nil:
		return nil, invalid("%s connector %s %s was applied with other content; versions don't change, so give this one a new version", v.Kind, v.Name, v.Version)
	case !errors.Is(err, pgx.ErrNoRows):
		return nil, err
	}
	if err := q.InsertArtifact(ctx, db.InsertArtifactParams{OrgID: s.Tenant.Org, Digest: v.Digest, MediaType: media, Content: content}); err != nil {
		return nil, err
	}
	err = q.InsertConnectorVersion(ctx, db.InsertConnectorVersionParams{
		OrgID: s.Tenant.Org, Kind: v.Kind, Name: v.Name, Version: v.Version, Digest: v.Digest, Provenance: provenance,
		Author: v.Info.Author, Logo: v.Info.Logo, ReleaseTag: v.Info.ReleaseTag, ReleaseUrl: v.Info.ReleaseURL,
	})
	res.Action = apiv1.ApplyAction_APPLY_ACTION_CREATED
	return res, err
}

func connectorName(kind, name, version string) string {
	return fmt.Sprintf("connectors/%s-%s/versions/%s", kind, name, version)
}

// resolveConnector finds the connector version a source or destination
// names: postgres for the highest version (a prerelease only if there's
// nothing else), postgres@1.2.0, or postgres@sha256:…. It returns nil for
// built-ins, and for names without versions, which a runner looks for in
// its connectors directory.
func (s *Server) resolveConnector(ctx context.Context, q *db.Queries, kind, name, ref string) (*connectorhost.Version, error) {
	if connectorhost.Builtin(kind, name) {
		if ref != "" {
			return nil, invalid("%s connector %s is built in, so it has no versions", kind, name)
		}
		return nil, nil
	}
	rows, err := q.ListConnectorVersions(ctx, db.ListConnectorVersionsParams{OrgID: s.Tenant.Org, Kind: kind, Name: name})
	if err != nil {
		return nil, err
	}
	pick := pickVersion(rows, ref)
	if pick == nil {
		if ref == "" {
			return nil, nil
		}
		applied := make([]string, len(rows))
		for i, r := range rows {
			applied[i] = r.Version
		}
		return nil, invalid("%s connector %s has no version %s; applied: %s", kind, name, ref, strings.Join(applied, ", "))
	}
	a, err := q.GetArtifact(ctx, db.GetArtifactParams{OrgID: s.Tenant.Org, Digest: pick.Digest})
	if err != nil {
		return nil, err
	}
	return connectorhost.ParseVersion(a.Content)
}

// pickVersion is the version ref names among rows, or nil.
func pickVersion(rows []db.ConnectorVersion, ref string) *db.ConnectorVersion {
	var pick *db.ConnectorVersion
	for i, r := range rows {
		switch {
		case strings.HasPrefix(ref, "sha256:"):
			if r.Digest == ref {
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
	return pick
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
