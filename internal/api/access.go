package api

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
)

var principalID = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

func principalName(id string) string { return "principals/" + id }

// parseOrgName splits {collection}/{id}, and {collection}/{id}/{sub}/{subID}
// when sub is set.
func parseOrgName(name, collection, sub string) (id, subID string, err error) {
	parts := strings.Split(name, "/")
	want := 2
	if sub != "" {
		want = 4
	}
	if len(parts) != want || parts[0] != collection || parts[1] == "" || sub != "" && (parts[2] != sub || parts[3] == "") {
		pattern := collection + "/{id}"
		if sub != "" {
			pattern += "/" + sub + "/{id}"
		}
		return "", "", invalid("name %q isn't %s", name, pattern)
	}
	if sub != "" {
		subID = parts[3]
	}
	return parts[1], subID, nil
}

var principalTypes = map[apiv1.PrincipalType]string{
	apiv1.PrincipalType_PRINCIPAL_TYPE_USER:            "user",
	apiv1.PrincipalType_PRINCIPAL_TYPE_SERVICE_ACCOUNT: "service_account",
}

func roleOf(r apiv1.Role) auth.Role {
	if r <= apiv1.Role_ROLE_UNSPECIFIED || r > apiv1.Role_ROLE_OWNER {
		return auth.None
	}
	return auth.Role(r)
}

// principals adds each principal's role bindings.
func (s *Server) principals(ctx context.Context, q *db.Queries, ps []db.Principal) ([]*apiv1.Principal, error) {
	ids := make([]uuid.UUID, len(ps))
	for i, p := range ps {
		ids[i] = p.ID
	}
	bindings, err := q.ListRoleBindings(ctx, db.ListRoleBindingsParams{OrgID: s.Tenant.Org, PrincipalIds: ids})
	if err != nil {
		return nil, err
	}
	out := make([]*apiv1.Principal, len(ps))
	for i, p := range ps {
		out[i] = &apiv1.Principal{
			Name: principalName(p.Name), DisplayName: p.DisplayName, Email: p.Email, Disabled: p.Disabled,
			CreateTime: timestamppb.New(p.CreateTime),
		}
		for t, name := range principalTypes {
			if name == p.Type {
				out[i].Type = t
			}
		}
		for _, b := range bindings {
			if b.PrincipalID != p.ID {
				continue
			}
			r, _ := auth.ParseRole(b.Role)
			rb := &apiv1.RoleBinding{Role: apiv1.Role(r)}
			if b.Workspace != "" {
				rb.Workspace = "workspaces/" + b.Workspace
			}
			out[i].RoleBindings = append(out[i].RoleBindings, rb)
		}
	}
	return out, nil
}

func (s *Server) onePrincipal(ctx context.Context, q *db.Queries, p db.Principal) (*apiv1.Principal, error) {
	out, err := s.principals(ctx, q, []db.Principal{p})
	if err != nil {
		return nil, err
	}
	return out[0], nil
}

func (s *Server) getPrincipal(ctx context.Context, q *db.Queries, name string) (db.Principal, error) {
	id, _, err := parseOrgName(name, "principals", "")
	if err != nil {
		return db.Principal{}, err
	}
	p, err := q.GetPrincipal(ctx, db.GetPrincipalParams{OrgID: s.Tenant.Org, Name: id})
	if errors.Is(err, pgx.ErrNoRows) {
		return p, notFound("%s doesn't exist", name)
	}
	return p, err
}

// changeOwner fails unless the caller may change p: owners only by owners.
// It also says whether p is the org's last enabled owner, after locking the
// owners in q's transaction.
func (s *Server) changeOwner(ctx context.Context, q *db.Queries, p db.Principal) (last bool, err error) {
	owners, err := q.LockOrgOwners(ctx, s.Tenant.Org)
	if err != nil {
		return false, err
	}
	bindings, err := q.ListRoleBindings(ctx, db.ListRoleBindingsParams{OrgID: s.Tenant.Org, PrincipalIds: []uuid.UUID{p.ID}})
	if err != nil {
		return false, err
	}
	owner := slices.ContainsFunc(bindings, func(b db.ListRoleBindingsRow) bool { return b.Role == auth.Owner.String() && b.Workspace == "" })
	if !owner {
		return false, nil
	}
	if err := authorize(ctx, "owners.manage", ""); err != nil {
		return false, err
	}
	return len(owners) == 1 && owners[0] == p.ID, nil
}

func lastOwner(name string) error {
	return connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("%s is the org's last owner; make another owner first", name))
}

func (s *Server) CreatePrincipal(ctx context.Context, req *connect.Request[apiv1.CreatePrincipalRequest]) (*connect.Response[apiv1.CreatePrincipalResponse], error) {
	in := req.Msg.GetPrincipal()
	id := req.Msg.GetPrincipalId()
	audit(ctx).touched(principalName(id))
	if !principalID.MatchString(id) {
		return nil, invalid("principal_id %q must be a lowercase letter, then lowercase letters, digits and hyphens, at most 63 in all", id)
	}
	typ, ok := principalTypes[in.GetType()]
	if !ok {
		return nil, invalid("set the principal's type: user or service account")
	}
	if in.GetEmail() != "" && (typ != "user" || !strings.Contains(in.GetEmail(), "@")) {
		return nil, invalid("only users have an email, which has an @")
	}
	p := db.Principal{ID: uuid.New(), Name: id, Type: typ, DisplayName: in.GetDisplayName(), Email: in.GetEmail(), CreateTime: time.Now()}
	err := s.Store.CreatePrincipal(ctx, db.CreatePrincipalParams{
		OrgID: s.Tenant.Org, ID: p.ID, Name: id, Type: typ, DisplayName: p.DisplayName, Email: p.Email,
	})
	if store.IsUniqueViolation(err) {
		return nil, connect.NewError(connect.CodeAlreadyExists, fmt.Errorf("%s exists", principalName(id)))
	}
	if err != nil {
		return nil, err
	}
	out, err := s.onePrincipal(ctx, s.Store.Queries, p)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.CreatePrincipalResponse{Principal: out}), nil
}

func (s *Server) GetPrincipal(ctx context.Context, req *connect.Request[apiv1.GetPrincipalRequest]) (*connect.Response[apiv1.GetPrincipalResponse], error) {
	p, err := s.getPrincipal(ctx, s.Store.Queries, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	out, err := s.onePrincipal(ctx, s.Store.Queries, p)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GetPrincipalResponse{Principal: out}), nil
}

func (s *Server) ListPrincipals(ctx context.Context, req *connect.Request[apiv1.ListPrincipalsRequest]) (*connect.Response[apiv1.ListPrincipalsResponse], error) {
	after, n, err := page(req.Msg.GetPageSize(), req.Msg.GetPageToken())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListPrincipals(ctx, db.ListPrincipalsParams{OrgID: s.Tenant.Org, After: after, MaxRows: n})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListPrincipalsResponse{}
	if len(rows) == int(n) {
		rows = rows[:n-1]
		res.NextPageToken = nextToken(rows[len(rows)-1].Name)
	}
	if res.Principals, err = s.principals(ctx, s.Store.Queries, rows); err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

func (s *Server) DeletePrincipal(ctx context.Context, req *connect.Request[apiv1.DeletePrincipalRequest]) (*connect.Response[apiv1.DeletePrincipalResponse], error) {
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, req.Msg.GetName())
		if err != nil {
			return err
		}
		if last, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		} else if last {
			return lastOwner(req.Msg.GetName())
		}
		_, err = q.DeletePrincipal(ctx, db.DeletePrincipalParams{OrgID: s.Tenant.Org, ID: p.ID})
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeletePrincipalResponse{}), nil
}

func (s *Server) setDisabled(ctx context.Context, name string, disabled bool) (*apiv1.Principal, error) {
	var out *apiv1.Principal
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, name)
		if err != nil {
			return err
		}
		if last, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		} else if last && disabled {
			return lastOwner(name)
		}
		if err := q.SetPrincipalDisabled(ctx, db.SetPrincipalDisabledParams{OrgID: s.Tenant.Org, ID: p.ID, Disabled: disabled}); err != nil {
			return err
		}
		if disabled {
			if _, err := q.DeleteSessions(ctx, db.DeleteSessionsParams{OrgID: s.Tenant.Org, PrincipalID: p.ID}); err != nil {
				return err
			}
		}
		p.Disabled = disabled
		out, err = s.onePrincipal(ctx, q, p)
		return err
	})
	return out, err
}

func (s *Server) DisablePrincipal(ctx context.Context, req *connect.Request[apiv1.DisablePrincipalRequest]) (*connect.Response[apiv1.DisablePrincipalResponse], error) {
	p, err := s.setDisabled(ctx, req.Msg.GetName(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DisablePrincipalResponse{Principal: p}), nil
}

func (s *Server) EnablePrincipal(ctx context.Context, req *connect.Request[apiv1.EnablePrincipalRequest]) (*connect.Response[apiv1.EnablePrincipalResponse], error) {
	p, err := s.setDisabled(ctx, req.Msg.GetName(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.EnablePrincipalResponse{Principal: p}), nil
}

// changeRole grants or revokes a binding. Revoking one the principal doesn't
// hold changes nothing.
func (s *Server) changeRole(ctx context.Context, name string, b *apiv1.RoleBinding, grant bool) (*apiv1.Principal, error) {
	role := roleOf(b.GetRole())
	if role == auth.None {
		return nil, invalid("set the binding's role")
	}
	audit(ctx).set("role", role.String())
	if b.GetWorkspace() != "" {
		audit(ctx).set("workspace", b.GetWorkspace())
	}
	var ws string
	if b.GetWorkspace() != "" {
		var err error
		if ws, err = parseParent(b.GetWorkspace()); err != nil {
			return nil, err
		}
		if role.OrgOnly() {
			return nil, invalid("%s is bound only on the org", role)
		}
	}
	if role == auth.Owner {
		if err := authorize(ctx, "owners.manage", ""); err != nil {
			return nil, err
		}
	}
	var out *apiv1.Principal
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, name)
		if err != nil {
			return err
		}
		var wsID *uuid.UUID
		if ws != "" {
			id, err := s.wsID(ctx, ws)
			if err != nil {
				return err
			}
			wsID = &id
		}
		params := db.InsertRoleBindingParams{OrgID: s.Tenant.Org, PrincipalID: p.ID, Role: role.String(), WorkspaceID: wsID}
		switch {
		case grant:
			_, err = q.InsertRoleBinding(ctx, params)
		case role == auth.Owner:
			last, lerr := s.changeOwner(ctx, q, p)
			if lerr != nil {
				return lerr
			}
			if last {
				return lastOwner(name)
			}
			fallthrough
		default:
			_, err = q.DeleteRoleBinding(ctx, db.DeleteRoleBindingParams(params))
		}
		if err != nil {
			return err
		}
		out, err = s.onePrincipal(ctx, q, p)
		return err
	})
	return out, err
}

func (s *Server) GrantRole(ctx context.Context, req *connect.Request[apiv1.GrantRoleRequest]) (*connect.Response[apiv1.GrantRoleResponse], error) {
	p, err := s.changeRole(ctx, req.Msg.GetName(), req.Msg.GetBinding(), true)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.GrantRoleResponse{Principal: p}), nil
}

func (s *Server) RevokeRole(ctx context.Context, req *connect.Request[apiv1.RevokeRoleRequest]) (*connect.Response[apiv1.RevokeRoleResponse], error) {
	p, err := s.changeRole(ctx, req.Msg.GetName(), req.Msg.GetBinding(), false)
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.RevokeRoleResponse{Principal: p}), nil
}

func keyResource(principal string, id uuid.UUID, prefix string, expire, used *time.Time, created time.Time) *apiv1.Key {
	k := &apiv1.Key{Name: principalName(principal) + "/keys/" + id.String(), Prefix: prefix, CreateTime: timestamppb.New(created)}
	if expire != nil {
		k.ExpireTime = timestamppb.New(*expire)
	}
	if used != nil {
		k.LastUsedTime = timestamppb.New(*used)
	}
	return k
}

func (s *Server) CreateKey(ctx context.Context, req *connect.Request[apiv1.CreateKeyRequest]) (*connect.Response[apiv1.CreateKeyResponse], error) {
	var expire *time.Time
	if e := req.Msg.GetExpireTime(); e != nil {
		t := e.AsTime()
		if !t.After(time.Now()) {
			return nil, invalid("expire_time is in the past")
		}
		expire = &t
	}
	res := &apiv1.CreateKeyResponse{}
	err := s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, req.Msg.GetParent())
		if err != nil {
			return err
		}
		if _, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		}
		key, id, err := store.CreateAPIKey(ctx, q, s.Tenant.Org, p.ID, expire)
		if err != nil {
			return err
		}
		res.Secret = key.Secret
		res.Key = keyResource(p.Name, id, key.Shown, expire, nil, time.Now())
		audit(ctx).touched(res.Key.GetName())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(res), nil
}

func (s *Server) ListKeys(ctx context.Context, req *connect.Request[apiv1.ListKeysRequest]) (*connect.Response[apiv1.ListKeysResponse], error) {
	p, err := s.getPrincipal(ctx, s.Store.Queries, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListAPIKeys(ctx, db.ListAPIKeysParams{OrgID: s.Tenant.Org, PrincipalID: p.ID})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListKeysResponse{}
	for _, r := range rows {
		res.Keys = append(res.Keys, keyResource(p.Name, r.ID, r.Prefix, r.ExpireTime, r.LastUsedTime, r.CreateTime))
	}
	return connect.NewResponse(res), nil
}

func (s *Server) DeleteKey(ctx context.Context, req *connect.Request[apiv1.DeleteKeyRequest]) (*connect.Response[apiv1.DeleteKeyResponse], error) {
	principal, keyID, err := parseOrgName(req.Msg.GetName(), "principals", "keys")
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(keyID)
	if err != nil {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	err = s.Store.Tx(ctx, func(q *db.Queries) error {
		p, err := s.getPrincipal(ctx, q, principalName(principal))
		if err != nil {
			return err
		}
		if _, err := s.changeOwner(ctx, q, p); err != nil {
			return err
		}
		n, err := q.DeleteAPIKey(ctx, db.DeleteAPIKeyParams{OrgID: s.Tenant.Org, PrincipalID: p.ID, ID: id})
		if err == nil && n == 0 {
			return notFound("%s doesn't exist", req.Msg.GetName())
		}
		return err
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.DeleteKeyResponse{}), nil
}

func (s *Server) ListDataPlanes(ctx context.Context, _ *connect.Request[apiv1.ListDataPlanesRequest]) (*connect.Response[apiv1.ListDataPlanesResponse], error) {
	rows, err := s.Store.ListDataPlanes(ctx, s.Tenant.Org)
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListDataPlanesResponse{}
	for _, r := range rows {
		res.DataPlanes = append(res.DataPlanes, &apiv1.DataPlane{Name: "dataPlanes/" + r.ID, CreateTime: timestamppb.New(r.CreateTime)})
	}
	return connect.NewResponse(res), nil
}

func (s *Server) dataPlane(ctx context.Context, name string) (string, error) {
	dp, _, err := parseOrgName(name, "dataPlanes", "")
	if err != nil {
		return "", err
	}
	if _, err := s.Store.GetDataPlane(ctx, db.GetDataPlaneParams{OrgID: s.Tenant.Org, ID: dp}); errors.Is(err, pgx.ErrNoRows) {
		return "", notFound("%s doesn't exist", name)
	} else if err != nil {
		return "", err
	}
	return dp, nil
}

func tokenResource(dp string, id uuid.UUID, prefix string, used *time.Time, created time.Time) *apiv1.RunnerToken {
	t := &apiv1.RunnerToken{Name: "dataPlanes/" + dp + "/tokens/" + id.String(), Prefix: prefix, CreateTime: timestamppb.New(created)}
	if used != nil {
		t.LastUsedTime = timestamppb.New(*used)
	}
	return t
}

func (s *Server) CreateRunnerToken(ctx context.Context, req *connect.Request[apiv1.CreateRunnerTokenRequest]) (*connect.Response[apiv1.CreateRunnerTokenResponse], error) {
	dp, err := s.dataPlane(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	token, id, err := store.CreateRunnerToken(ctx, s.Store.Queries, s.Tenant.Org, dp)
	if err != nil {
		return nil, err
	}
	t := tokenResource(dp, id, token.Shown, nil, time.Now())
	audit(ctx).touched(t.GetName())
	return connect.NewResponse(&apiv1.CreateRunnerTokenResponse{Token: t, Secret: token.Secret}), nil
}

func (s *Server) ListRunnerTokens(ctx context.Context, req *connect.Request[apiv1.ListRunnerTokensRequest]) (*connect.Response[apiv1.ListRunnerTokensResponse], error) {
	dp, err := s.dataPlane(ctx, req.Msg.GetParent())
	if err != nil {
		return nil, err
	}
	rows, err := s.Store.ListRunnerTokens(ctx, db.ListRunnerTokensParams{OrgID: s.Tenant.Org, DataPlaneID: dp})
	if err != nil {
		return nil, err
	}
	res := &apiv1.ListRunnerTokensResponse{}
	for _, r := range rows {
		res.Tokens = append(res.Tokens, tokenResource(dp, r.ID, r.Prefix, r.LastUsedTime, r.CreateTime))
	}
	return connect.NewResponse(res), nil
}

func (s *Server) DeleteRunnerToken(ctx context.Context, req *connect.Request[apiv1.DeleteRunnerTokenRequest]) (*connect.Response[apiv1.DeleteRunnerTokenResponse], error) {
	dp, tokenID, err := parseOrgName(req.Msg.GetName(), "dataPlanes", "tokens")
	if err != nil {
		return nil, err
	}
	id, err := uuid.Parse(tokenID)
	if err != nil {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	n, err := s.Store.DeleteRunnerToken(ctx, db.DeleteRunnerTokenParams{OrgID: s.Tenant.Org, DataPlaneID: dp, ID: id})
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, notFound("%s doesn't exist", req.Msg.GetName())
	}
	return connect.NewResponse(&apiv1.DeleteRunnerTokenResponse{}), nil
}
