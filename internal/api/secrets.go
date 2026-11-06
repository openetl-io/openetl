package api

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/secretstore"
	"github.com/openetl-io/openetl/internal/spec"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

const maxSecretSize = 64 << 10

func (s *Server) parseSecretName(ctx context.Context, name string) (ws string, wsID uuid.UUID, secret string, err error) {
	ws, secret, err = parseName(name, "secrets")
	if err != nil {
		return "", uuid.Nil, "", err
	}
	if !spec.ValidName(secret) {
		return "", uuid.Nil, "", invalid("secret name %q must be lowercase letters, digits and dashes", secret)
	}
	wsID, err = s.wsID(ctx, ws)
	return ws, wsID, secret, err
}

func toSecret(ws string, r db.Secret, usedBy []string) *apiv1.Secret {
	out := &apiv1.Secret{Name: resourceName(ws, "secrets", r.Name), Etag: etag(r.Revision), UsedBy: usedBy,
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime)}
	if r.Ref != nil {
		out.Ref = *r.Ref
	}
	return out
}

// secretUsers lists the sources, destinations, webhooks, Spark and Flink clusters and
// pipelines (by their current versions) that name each secret.
func (s *Server) secretUsers(ctx context.Context, q *db.Queries, ws string, wsID uuid.UUID, names []string) (map[string][]string, error) {
	users := map[string][]string{}
	srcs, err := q.ListSourcesUsingSecrets(ctx, db.ListSourcesUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range srcs {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "sources", r.Name))
		}
	}
	dsts, err := q.ListDestinationsUsingSecrets(ctx, db.ListDestinationsUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range dsts {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "destinations", r.Name))
		}
	}
	hooks, err := q.ListWebhooksUsingSecrets(ctx, db.ListWebhooksUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range hooks {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "webhooks", r.Name))
		}
	}
	clusters, err := q.ListSparkClustersUsingSecrets(ctx, db.ListSparkClustersUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range clusters {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "sparkClusters", r.Name))
		}
	}
	flinks, err := q.ListFlinkClustersUsingSecrets(ctx, db.ListFlinkClustersUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range flinks {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "flinkClusters", r.Name))
		}
	}
	pipes, err := q.ListPipelinesUsingSecrets(ctx, db.ListPipelinesUsingSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	for _, r := range pipes {
		for _, n := range r.Secrets {
			users[n] = append(users[n], resourceName(ws, "pipelines", r.Name))
		}
	}
	return users, nil
}

func (s *Server) GetSecret(ctx context.Context, req *connect.Request[apiv1.GetSecretRequest]) (*connect.Response[apiv1.GetSecretResponse], error) {
	ws, wsID, name, err := s.parseSecretName(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	r, err := s.Store.GetSecret(ctx, db.GetSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	if err != nil {
		return nil, err
	}
	users, err := s.secretUsers(ctx, s.Store.Queries, ws, wsID, []string{name})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetSecretResponse{Secret: toSecret(ws, r, users[name])}), nil
}

func (s *Server) ListSecrets(ctx context.Context, req *connect.Request[apiv1.ListSecretsRequest]) (*connect.Response[apiv1.ListSecretsResponse], error) {
	ws, wsID, after, n, err := s.listArgs(ctx, req.Msg.GetParent(), req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListSecrets(ctx, db.ListSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListSecretsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	names := make([]string, len(rows))
	for i, r := range rows {
		names[i] = r.Name
	}
	users, err := s.secretUsers(ctx, s.Store.Queries, ws, wsID, names)
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		res.Secrets = append(res.Secrets, toSecret(ws, r, users[r.Name]))
	}
	return connect.NewResponse(res), nil
}

func (s *Server) UpdateSecret(ctx context.Context, req *connect.Request[apiv1.UpdateSecretRequest]) (*connect.Response[apiv1.UpdateSecretResponse], error) {
	in := req.Msg.GetSecret()
	ws, wsID, name, err := s.parseSecretName(ctx, in.GetName())
	if err != nil {
		return nil, err
	}
	var ref *string
	switch v := in.GetValue(); {
	case v != "" && in.GetRef() != "":
		return nil, invalid("%s: set a value or a reference, not both", in.GetName())
	case in.GetRef() != "":
		if _, err := secretstore.Parse(in.GetRef()); err != nil {
			return nil, invalid("%s: %v", in.GetName(), err)
		}
		ref = &in.Ref
		audit(ctx).set("ref", in.GetRef())
	case v == "":
		return nil, invalid("%s: a secret's value can't be empty", in.GetName())
	case len(v) > maxSecretSize:
		return nil, invalid("%s: a secret's value is at most %d KiB", in.GetName(), maxSecretSize>>10)
	case s.Secrets == nil:
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this server has no secrets key (--secrets-key), so it can't store secrets' values"))
	}
	seal := func(id uuid.UUID) ([]byte, error) {
		if ref != nil {
			return nil, nil
		}
		return s.Secrets.Seal(ctx, s.Tenant.Org, secrets.AAD(s.Tenant.Org, wsID, id), []byte(in.GetValue()))
	}
	res := &apiv1.UpdateSecretResponse{}
	var row db.Secret
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		cur, err := q.LockSecret(ctx, db.LockSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			if !req.Msg.GetAllowMissing() {
				return notFound("%s doesn't exist; set allow_missing to create it", in.GetName())
			}
			if err := checkEtag(in.GetEtag(), 0); err != nil {
				return err
			}
			id := uuid.New()
			sealed, err := seal(id)
			if err != nil {
				return err
			}
			res.Created = true
			err = q.InsertSecret(ctx, db.InsertSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: id, Name: name, Ciphertext: sealed, Ref: ref})
			if store.IsUniqueViolation(err) {
				return connect.NewError(connect.CodeAborted, fmt.Errorf("%s was created at the same time; try again", in.GetName()))
			}
			if err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			if err := checkEtag(in.GetEtag(), cur.Revision); err != nil {
				return err
			}
			sealed, err := seal(cur.ID)
			if err != nil {
				return err
			}
			if err := q.UpdateSecret(ctx, db.UpdateSecretParams{OrgID: s.Tenant.Org, ID: cur.ID, Ciphertext: sealed, Ref: ref}); err != nil {
				return err
			}
		}
		row, err = q.GetSecret(ctx, db.GetSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
		return err
	})
	if err != nil {
		return nil, err
	}
	users, err := s.secretUsers(ctx, s.Store.Queries, ws, wsID, []string{name})
	if err != nil {
		return nil, err
	}
	res.Secret = toSecret(ws, row, users[name])
	return connect.NewResponse(res), nil
}

func (s *Server) DeleteSecret(ctx context.Context, req *connect.Request[apiv1.DeleteSecretRequest]) (*connect.Response[apiv1.DeleteSecretResponse], error) {
	ws, wsID, name, err := s.parseSecretName(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		// The lock waits for applies that share it, so their configs are
		// seen below.
		cur, err := q.LockSecret(ctx, db.LockSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Name: name})
		if errors.Is(err, pgx.ErrNoRows) {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		if err != nil {
			return err
		}
		if err := checkEtag(req.Msg.GetEtag(), cur.Revision); err != nil {
			return err
		}
		users, err := s.secretUsers(ctx, q, ws, wsID, []string{name})
		if err != nil {
			return err
		}
		if u := users[name]; len(u) > 0 {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is used by %s", req.Msg.GetName(), strings.Join(u, ", ")))
		}
		return q.DeleteSecret(ctx, db.DeleteSecretParams{OrgID: s.Tenant.Org, ID: cur.ID})
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteSecretResponse{}), nil
}

// secretRef is the secret v names as {"secret": NAME}, if it names one.
func secretRef(v *structpb.Value) (string, bool) {
	f := v.GetStructValue().GetFields()
	ref, ok := f["secret"]
	if !ok || len(f) != 1 {
		return "", false
	}
	name, ok := ref.GetKind().(*structpb.Value_StringValue)
	if !ok {
		return "", false
	}
	return name.StringValue, true
}

// secretRefs are the {"secret": NAME} values in a config's objects, by dotted
// path.
func secretRefs(cfg *structpb.Struct, prefix string) map[string]*structpb.Value {
	refs := map[string]*structpb.Value{}
	for k, v := range cfg.GetFields() {
		if _, ok := secretRef(v); ok {
			refs[prefix+k] = v
		} else if sv := v.GetStructValue(); sv != nil {
			maps.Copy(refs, secretRefs(sv, prefix+k+"."))
		}
	}
	return refs
}

// bindSecrets checks a config's secret references. With the connector's
// secret properties known, as dotted paths, each one the config sets must
// name a secret as {"secret": NAME}; with them unknown, any {"secret": NAME}
// in an object names one. It returns the secrets named, and the config with
// a placeholder for each, to validate.
func bindSecrets(config *structpb.Struct, props []string, known bool) ([]string, *structpb.Struct, error) {
	names := []string{}
	var walk func(in *structpb.Struct, prefix string) (*structpb.Struct, error)
	walk = func(in *structpb.Struct, prefix string) (*structpb.Struct, error) {
		out := &structpb.Struct{Fields: map[string]*structpb.Value{}}
		for k, v := range in.GetFields() {
			path := prefix + k
			out.Fields[k] = v
			name, ok := secretRef(v)
			secret := slices.Contains(props, path)
			switch {
			case !ok && secret:
				return nil, invalid("config.%s is a secret, so the config names it as {secret: NAME}; store its value with openetl secret set NAME", path)
			case !ok || known && !secret:
				if sv := v.GetStructValue(); sv != nil {
					inner, err := walk(sv, path+".")
					if err != nil {
						return nil, err
					}
					out.Fields[k] = structpb.NewStructValue(inner)
				}
				continue
			case !spec.ValidName(name):
				return nil, invalid("config.%s: secret name %q must be lowercase letters, digits and dashes", path, name)
			}
			names = append(names, name)
			out.Fields[k] = structpb.NewStringValue("secret")
		}
		return out, nil
	}
	out, err := walk(config, "")
	if err != nil {
		return nil, nil, err
	}
	slices.Sort(names)
	return slices.Compact(names), out, nil
}

// checkSecretsExist checks that the secrets exist in the workspace, and
// keeps them from being deleted until q's transaction ends.
func (s *Server) checkSecretsExist(ctx context.Context, q *db.Queries, wsID uuid.UUID, workspace string, names []string) error {
	if len(names) == 0 {
		return nil
	}
	found, err := q.ShareSecrets(ctx, db.ShareSecretsParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return err
	}
	for _, n := range names {
		if !slices.Contains(found, n) {
			return invalid("secret %q doesn't exist in workspace %q; set it with openetl secret set %s", n, workspace, n)
		}
	}
	return nil
}

// resolveSecrets replaces the config's references to the named secrets with
// their values, for a runner. It returns the fields that name secrets in
// cloud secret stores, which keep their references for the runner to read.
func (s *Server) resolveSecrets(ctx context.Context, what string, wsID uuid.UUID, cfg *apiv1.ConnectorConfig, names []string) ([]*dataplanev1.ExternalSecret, error) {
	if len(names) == 0 {
		return nil, nil
	}
	rows, err := s.Store.GetSecretValues(ctx, db.GetSecretValuesParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return nil, err
	}
	byName := map[string]db.GetSecretValuesRow{}
	for _, r := range rows {
		byName[r.Name] = r
	}
	var external []*dataplanev1.ExternalSecret
	for k, v := range secretRefs(cfg.GetConfig(), "") {
		name, _ := secretRef(v)
		if !slices.Contains(names, name) {
			continue
		}
		r, ok := byName[name]
		if !ok {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s names secret %q, which doesn't exist", what, name))
		}
		secret := resourceName(r.Workspace, "secrets", r.Name)
		audit(ctx).touched(secret)
		if r.Ref != nil {
			external = append(external, &dataplanev1.ExternalSecret{Field: k, Secret: secret, Ref: *r.Ref})
			continue
		}
		if s.Secrets == nil {
			return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s names secrets, but this server has no secrets key (--secrets-key)", what))
		}
		value, err := s.Secrets.Open(ctx, s.Tenant.Org, secrets.AAD(s.Tenant.Org, wsID, r.ID), r.Ciphertext)
		if err != nil {
			return nil, err
		}
		connectorhost.SetConfigValue(cfg.Config, k, structpb.NewStringValue(string(value)))
	}
	slices.SortFunc(external, func(a, b *dataplanev1.ExternalSecret) int { return strings.Compare(a.GetField(), b.GetField()) })
	return external, nil
}
