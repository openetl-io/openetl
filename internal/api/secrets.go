package api

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/spec"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

const maxSecretSize = 64 << 10

// secretAAD binds a sealed value to its secret.
func secretAAD(org, ws, id uuid.UUID) []byte {
	return []byte("openetl.secret.v1/" + org.String() + "/" + ws.String() + "/" + id.String())
}

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
	return &apiv1.Secret{Name: resourceName(ws, "secrets", r.Name), Etag: etag(r.Revision), UsedBy: usedBy,
		CreateTime: timestamppb.New(r.CreateTime), UpdateTime: timestamppb.New(r.UpdateTime)}
}

// secretUsers lists the sources and destinations that name each secret.
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
	if s.Secrets == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("this server has no secrets key (--secrets-key), so it can't store secrets"))
	}
	in := req.Msg.GetSecret()
	ws, wsID, name, err := s.parseSecretName(ctx, in.GetName())
	if err != nil {
		return nil, err
	}
	switch v := in.GetValue(); {
	case v == "":
		return nil, invalid("%s: a secret's value can't be empty", in.GetName())
	case len(v) > maxSecretSize:
		return nil, invalid("%s: a secret's value is at most %d KiB", in.GetName(), maxSecretSize>>10)
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
			sealed, err := s.Secrets.Seal(ctx, s.Tenant.Org, secretAAD(s.Tenant.Org, wsID, id), []byte(in.GetValue()))
			if err != nil {
				return err
			}
			res.Created = true
			err = q.InsertSecret(ctx, db.InsertSecretParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, ID: id, Name: name, Ciphertext: sealed})
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
			sealed, err := s.Secrets.Seal(ctx, s.Tenant.Org, secretAAD(s.Tenant.Org, wsID, cur.ID), []byte(in.GetValue()))
			if err != nil {
				return err
			}
			if err := q.UpdateSecret(ctx, db.UpdateSecretParams{OrgID: s.Tenant.Org, ID: cur.ID, Ciphertext: sealed}); err != nil {
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

// bindSecrets checks a config's secret references. With the connector's
// secret properties known, each one the config sets must name a secret as
// {"secret": NAME}; with them unknown, any top-level {"secret": NAME} names
// one. It returns the secrets named, and the config with a placeholder for
// each, to validate.
func bindSecrets(config *structpb.Struct, props []string, known bool) ([]string, *structpb.Struct, error) {
	names := []string{}
	out := &structpb.Struct{Fields: map[string]*structpb.Value{}}
	for k, v := range config.GetFields() {
		out.Fields[k] = v
		name, ok := secretRef(v)
		secret := slices.Contains(props, k)
		switch {
		case known && !secret:
			continue
		case !ok && secret:
			return nil, nil, invalid("config.%s is a secret, so the config names it as {secret: NAME}; store its value with openetl secret set NAME", k)
		case !ok:
			continue
		case !spec.ValidName(name):
			return nil, nil, invalid("config.%s: secret name %q must be lowercase letters, digits and dashes", k, name)
		}
		names = append(names, name)
		out.Fields[k] = structpb.NewStringValue("secret")
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
// their values, for a runner.
func (s *Server) resolveSecrets(ctx context.Context, what string, wsID uuid.UUID, cfg *apiv1.ConnectorConfig, names []string) error {
	if len(names) == 0 {
		return nil
	}
	if s.Secrets == nil {
		return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s names secrets, but this server has no secrets key (--secrets-key)", what))
	}
	rows, err := s.Store.GetSecretValues(ctx, db.GetSecretValuesParams{OrgID: s.Tenant.Org, WorkspaceID: wsID, Names: names})
	if err != nil {
		return err
	}
	values := map[string]string{}
	for _, r := range rows {
		v, err := s.Secrets.Open(ctx, s.Tenant.Org, secretAAD(s.Tenant.Org, wsID, r.ID), r.Ciphertext)
		if err != nil {
			return err
		}
		values[r.Name] = string(v)
	}
	for k, v := range cfg.GetConfig().GetFields() {
		name, ok := secretRef(v)
		if !ok || !slices.Contains(names, name) {
			continue
		}
		value, ok := values[name]
		if !ok {
			return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s names secret %q, which doesn't exist", what, name))
		}
		cfg.Config.Fields[k] = structpb.NewStringValue(value)
	}
	return nil
}
