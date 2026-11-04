package api

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/auth"
	"github.com/unsuitable001/openetl/internal/store/db"
)

// permissions are what each role may do, by the least role that may.
var permissions = map[string]auth.Role{
	"pipelines.get":        auth.Viewer,
	"pipelines.pause":      auth.Operator,
	"pipelines.sync":       auth.Operator,
	"pipelines.delete":     auth.Editor,
	"syncs.get":            auth.Viewer,
	"logs.get":             auth.Viewer,
	"sources.get":          auth.Viewer,
	"sources.check":        auth.Operator,
	"sources.discover":     auth.Operator,
	"sources.delete":       auth.Editor,
	"destinations.get":     auth.Viewer,
	"destinations.check":   auth.Operator,
	"destinations.delete":  auth.Editor,
	"dbtProjects.get":      auth.Viewer,
	"dbtProjects.delete":   auth.Editor,
	"sparkClusters.get":    auth.Viewer,
	"sparkClusters.delete": auth.Editor,
	"secrets.get":          auth.Viewer,
	"secrets.update":       auth.Editor,
	"secrets.delete":       auth.Editor,
	"webhooks.get":         auth.Viewer,
	"webhooks.update":      auth.Operator,
	"webhooks.test":        auth.Operator,
	"webhooks.delete":      auth.Editor,
	"incidents.get":        auth.Viewer,
	"incidents.resolve":    auth.Operator,
	// Sources, destinations, pipelines, dbt projects and webhooks.
	"documents.apply":     auth.Editor,
	"connectors.apply":    auth.Admin,
	"principals.get":      auth.Viewer,
	"principals.create":   auth.Admin,
	"principals.update":   auth.Admin,
	"principals.delete":   auth.Admin,
	"roles.grant":         auth.Admin,
	"keys.get":            auth.Viewer,
	"keys.create":         auth.Admin,
	"keys.delete":         auth.Admin,
	"dataPlanes.get":      auth.Viewer,
	"runnerTokens.get":    auth.Viewer,
	"runnerTokens.create": auth.Admin,
	"runnerTokens.delete": auth.Admin,
	"audit.get":           auth.Admin,
	"sessions.revoke":     auth.Admin,
	"passwords.reset":     auth.Admin,
	// Any principal, on itself.
	"caller.get":      auth.None,
	"password.change": auth.None,
	// Granting or revoking owner, and changing an owner or its keys.
	"owners.manage": auth.Owner,
}

// A rule is an RPC's permission, and the workspace it's checked in.
type rule struct {
	permission string
	// The workspace's name, or "" for the org. Nil: the handler checks.
	scope func(any) (string, error)
	// DataPlaneService takes runner tokens; its handlers check that the work
	// is the token's data plane's.
	runner bool
}

// noCredential is the permission of sign-in's calls, which take no
// credential. Their handlers check what they need, and write their own
// audit entries.
const noCredential = "public"

func (r rule) public() bool { return r.permission == noCredential }

var (
	onOrg    = func(any) (string, error) { return "", nil }
	inName   = func(m any) (string, error) { return workspaceOf(m.(interface{ GetName() string }).GetName()) }
	inParent = func(m any) (string, error) { return workspaceOf(m.(interface{ GetParent() string }).GetParent()) }
	inSecret = func(m any) (string, error) { return workspaceOf(m.(*apiv1.UpdateSecretRequest).GetSecret().GetName()) }
	runner   = rule{runner: true}
	public   = rule{permission: noCredential}
)

// rules has every RPC the server serves; one that isn't here is refused.
var rules = map[string]rule{
	apiv1connect.ApplyServiceApplyProcedure: {permission: "documents.apply"},

	apiv1connect.PipelineServiceGetPipelineProcedure:          {"pipelines.get", inName, false},
	apiv1connect.PipelineServiceListPipelinesProcedure:        {"pipelines.get", inParent, false},
	apiv1connect.PipelineServiceDeletePipelineProcedure:       {"pipelines.delete", inName, false},
	apiv1connect.PipelineServicePausePipelineProcedure:        {"pipelines.pause", inName, false},
	apiv1connect.PipelineServiceResumePipelineProcedure:       {"pipelines.pause", inName, false},
	apiv1connect.PipelineServiceSyncPipelineProcedure:         {"pipelines.sync", inName, false},
	apiv1connect.PipelineServiceListPipelineVersionsProcedure: {"pipelines.get", inParent, false},
	apiv1connect.PipelineServiceGetPipelineVersionProcedure:   {"pipelines.get", inName, false},
	apiv1connect.PipelineServiceGetPipelinePlanProcedure:      {"pipelines.get", inName, false},
	apiv1connect.PipelineServiceListStreamSchemasProcedure:    {"pipelines.get", inParent, false},
	apiv1connect.PipelineServiceListCheckpointsProcedure:      {"pipelines.get", inParent, false},

	apiv1connect.SyncServiceListSyncsProcedure:   {"syncs.get", inParent, false},
	apiv1connect.SyncServiceGetSyncProcedure:     {"syncs.get", inName, false},
	apiv1connect.SyncServiceReadSyncLogProcedure: {"logs.get", inName, false},

	apiv1connect.SourceServiceGetSourceProcedure:      {"sources.get", inName, false},
	apiv1connect.SourceServiceListSourcesProcedure:    {"sources.get", inParent, false},
	apiv1connect.SourceServiceDeleteSourceProcedure:   {"sources.delete", inName, false},
	apiv1connect.SourceServiceCheckSourceProcedure:    {"sources.check", inName, false},
	apiv1connect.SourceServiceDiscoverSourceProcedure: {"sources.discover", inName, false},

	apiv1connect.DestinationServiceGetDestinationProcedure:    {"destinations.get", inName, false},
	apiv1connect.DestinationServiceListDestinationsProcedure:  {"destinations.get", inParent, false},
	apiv1connect.DestinationServiceDeleteDestinationProcedure: {"destinations.delete", inName, false},
	apiv1connect.DestinationServiceCheckDestinationProcedure:  {"destinations.check", inName, false},

	apiv1connect.DbtProjectServiceGetDbtProjectProcedure:    {"dbtProjects.get", inName, false},
	apiv1connect.DbtProjectServiceListDbtProjectsProcedure:  {"dbtProjects.get", inParent, false},
	apiv1connect.DbtProjectServiceDeleteDbtProjectProcedure: {"dbtProjects.delete", inName, false},

	apiv1connect.SparkClusterServiceGetSparkClusterProcedure:    {"sparkClusters.get", inName, false},
	apiv1connect.SparkClusterServiceListSparkClustersProcedure:  {"sparkClusters.get", inParent, false},
	apiv1connect.SparkClusterServiceDeleteSparkClusterProcedure: {"sparkClusters.delete", inName, false},

	apiv1connect.SecretServiceGetSecretProcedure:    {"secrets.get", inName, false},
	apiv1connect.SecretServiceListSecretsProcedure:  {"secrets.get", inParent, false},
	apiv1connect.SecretServiceUpdateSecretProcedure: {"secrets.update", inSecret, false},
	apiv1connect.SecretServiceDeleteSecretProcedure: {"secrets.delete", inName, false},

	// The handlers also check owners.manage where an owner is involved.
	apiv1connect.AccessServiceCreatePrincipalProcedure:    {"principals.create", onOrg, false},
	apiv1connect.AccessServiceGetPrincipalProcedure:       {"principals.get", onOrg, false},
	apiv1connect.AccessServiceListPrincipalsProcedure:     {"principals.get", onOrg, false},
	apiv1connect.AccessServiceDeletePrincipalProcedure:    {"principals.delete", onOrg, false},
	apiv1connect.AccessServiceDisablePrincipalProcedure:   {"principals.update", onOrg, false},
	apiv1connect.AccessServiceEnablePrincipalProcedure:    {"principals.update", onOrg, false},
	apiv1connect.AccessServiceGrantRoleProcedure:          {"roles.grant", onOrg, false},
	apiv1connect.AccessServiceRevokeRoleProcedure:         {"roles.grant", onOrg, false},
	apiv1connect.AccessServiceCreateKeyProcedure:          {"keys.create", onOrg, false},
	apiv1connect.AccessServiceListKeysProcedure:           {"keys.get", onOrg, false},
	apiv1connect.AccessServiceDeleteKeyProcedure:          {"keys.delete", onOrg, false},
	apiv1connect.AccessServiceListDataPlanesProcedure:     {"dataPlanes.get", onOrg, false},
	apiv1connect.AccessServiceCreateRunnerTokenProcedure:  {"runnerTokens.create", onOrg, false},
	apiv1connect.AccessServiceListRunnerTokensProcedure:   {"runnerTokens.get", onOrg, false},
	apiv1connect.AccessServiceDeleteRunnerTokenProcedure:  {"runnerTokens.delete", onOrg, false},
	apiv1connect.AccessServiceGetCallerProcedure:          {"caller.get", onOrg, false},
	apiv1connect.AccessServiceChangePasswordProcedure:     {"password.change", onOrg, false},
	apiv1connect.AccessServiceRevokeSessionsProcedure:     {"sessions.revoke", onOrg, false},
	apiv1connect.AccessServiceCreatePasswordLinkProcedure: {"passwords.reset", onOrg, false},
	apiv1connect.AccessServiceCreateSessionProcedure:      public,
	apiv1connect.AccessServiceDeleteSessionProcedure:      public,
	apiv1connect.AccessServiceSetPasswordProcedure:        public,

	apiv1connect.AuditServiceListAuditEntriesProcedure: {"audit.get", onOrg, false},

	apiv1connect.WebhookServiceGetWebhookProcedure:            {"webhooks.get", inName, false},
	apiv1connect.WebhookServiceListWebhooksProcedure:          {"webhooks.get", inParent, false},
	apiv1connect.WebhookServiceDeleteWebhookProcedure:         {"webhooks.delete", inName, false},
	apiv1connect.WebhookServiceDisableWebhookProcedure:        {"webhooks.update", inName, false},
	apiv1connect.WebhookServiceEnableWebhookProcedure:         {"webhooks.update", inName, false},
	apiv1connect.WebhookServiceTestWebhookProcedure:           {"webhooks.test", inName, false},
	apiv1connect.WebhookServiceListWebhookDeliveriesProcedure: {"webhooks.get", inParent, false},

	apiv1connect.IncidentServiceGetIncidentProcedure:     {"incidents.get", inName, false},
	apiv1connect.IncidentServiceListIncidentsProcedure:   {"incidents.get", inParent, false},
	apiv1connect.IncidentServiceResolveIncidentProcedure: {"incidents.resolve", inName, false},

	dataplanev1connect.DataPlaneServiceGetSyncSpecProcedure:          runner,
	dataplanev1connect.DataPlaneServiceReportSchemasProcedure:        runner,
	dataplanev1connect.DataPlaneServiceReportCheckpointProcedure:     runner,
	dataplanev1connect.DataPlaneServiceGetConnectorConfigProcedure:   runner,
	dataplanev1connect.DataPlaneServiceReportCatalogProcedure:        runner,
	dataplanev1connect.DataPlaneServiceGetArtifactProcedure:          runner,
	dataplanev1connect.DataPlaneServiceReportAttemptFailureProcedure: runner,
}

// workspaceOf is the workspace in a name or parent, workspaces/{w}/….
func workspaceOf(name string) (string, error) {
	rest, ok := strings.CutPrefix(name, "workspaces/")
	ws, _, _ := strings.Cut(rest, "/")
	if !ok || ws == "" {
		return "", invalid("%q isn't in a workspace", name)
	}
	return ws, nil
}

// A caller is an API key's or a session's principal, or a runner token's
// data plane.
type caller struct {
	org         uuid.UUID
	principal   string
	principalID uuid.UUID
	key         uuid.UUID
	session     uuid.UUID
	// When the key or session stops working; nil if never.
	expire *time.Time
	roles  auth.Bindings

	dataPlane string
	token     uuid.UUID
}

type callerKey struct{}

func callerOf(ctx context.Context) *caller {
	c, _ := ctx.Value(callerKey{}).(*caller)
	if c == nil {
		return &caller{}
	}
	return c
}

// authorize fails unless the caller holds permission in workspace, or on
// the org for "".
func authorize(ctx context.Context, permission, workspace string) error {
	need, ok := permissions[permission]
	if !ok {
		return fmt.Errorf("api: no permission %q", permission)
	}
	c := callerOf(ctx)
	if c.principal != "" && c.roles.In(workspace) >= need {
		return nil
	}
	where := "on the org"
	if workspace != "" {
		where = "in workspace " + workspace
	}
	return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s needs %s (%s or above) %s", cmp.Or(c.principal, "the caller"), permission, need, where))
}

// authInterceptor authenticates every call, and checks the RPC's permission
// unless its handler does.
type authInterceptor struct{ s *Server }

func (a authInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		procedure := req.Spec().Procedure
		r, ok := rules[procedure]
		if !ok {
			return nil, connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s has no permission, so nobody may call it", procedure))
		}
		if r.public() {
			return next(ctx, req)
		}
		c, err := a.s.authenticate(ctx, req.Header(), r.runner)
		if err != nil {
			// Anyone can make these, so they're logged, not audited.
			if connect.CodeOf(err) == connect.CodeUnauthenticated {
				a.s.log().Info("call refused", "procedure", procedure, "peer", req.Peer().Addr, "err", err)
			}
			return nil, err
		}
		ctx = context.WithValue(ctx, callerKey{}, c)
		call := func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if r.scope != nil {
				ws, err := r.scope(req.Any())
				if err != nil {
					return nil, err
				}
				if err := authorize(ctx, r.permission, ws); err != nil {
					return nil, err
				}
			}
			return next(ctx, req)
		}
		if !audited(procedure, r) {
			return call(ctx, req)
		}
		return a.s.audit(ctx, req, call)
	}
}

func (authInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return next
}

// WrapStreamingHandler refuses streams: no rule can see their messages
// before the handler runs.
func (authInterceptor) WrapStreamingHandler(connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(_ context.Context, conn connect.StreamingHandlerConn) error {
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s streams, and streams aren't authorized", conn.Spec().Procedure))
	}
}

// lastUsedEvery is how often a key's or token's last use is written.
const lastUsedEvery = time.Minute

func (s *Server) authenticate(ctx context.Context, h http.Header, runner bool) (*caller, error) {
	secret, ok := strings.CutPrefix(h.Get("Authorization"), "Bearer ")
	if !ok && !runner {
		if s.Authless {
			return s.local(), nil
		}
		if c, found, err := s.sessionCaller(ctx, h); found || err != nil {
			return c, err
		}
	}
	switch {
	case !ok || secret == "":
		if runner {
			return nil, unauthenticated("send a runner token as Authorization: Bearer")
		}
		return nil, unauthenticated("sign in, or send an API key as Authorization: Bearer")
	case runner && !auth.IsRunnerToken(secret):
		return nil, unauthenticated("DataPlaneService takes runner tokens, not API keys")
	case !runner && auth.IsRunnerToken(secret):
		return nil, unauthenticated("runner tokens work only on DataPlaneService; send an API key")
	}
	hash := auth.Hash(secret)
	if runner {
		t, err := s.Store.AuthenticateRunnerToken(ctx, hash)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && t.OrgID != s.Tenant.Org {
			return nil, unauthenticated("the runner token isn't known; it may have been deleted")
		}
		if err != nil {
			return nil, err
		}
		if stale(t.LastUsedTime) {
			if err := s.Store.TouchRunnerToken(ctx, db.TouchRunnerTokenParams{OrgID: t.OrgID, ID: t.ID}); err != nil {
				return nil, err
			}
		}
		return &caller{org: t.OrgID, dataPlane: t.DataPlaneID, token: t.ID}, nil
	}
	k, err := s.Store.AuthenticateAPIKey(ctx, hash)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && k.OrgID != s.Tenant.Org {
		return nil, unauthenticated("the API key isn't known, has expired, or its principal is disabled")
	}
	if err != nil {
		return nil, err
	}
	if stale(k.LastUsedTime) {
		if err := s.Store.TouchAPIKey(ctx, db.TouchAPIKeyParams{OrgID: k.OrgID, ID: k.ID}); err != nil {
			return nil, err
		}
	}
	return &caller{
		org: k.OrgID, principal: principalName(k.Principal), principalID: k.PrincipalID, key: k.ID, expire: k.ExpireTime, roles: auth.ParseBindings(k.Bindings),
	}, nil
}

func stale(last *time.Time) bool { return last == nil || time.Since(*last) >= lastUsedEvery }

func unauthenticated(format string, args ...any) error {
	return connect.NewError(connect.CodeUnauthenticated, fmt.Errorf(format, args...))
}

// onDataPlane fails unless the caller is a runner token of dataPlane.
func onDataPlane(ctx context.Context, dataPlane, what string) error {
	c := callerOf(ctx)
	if c.dataPlane != "" && c.dataPlane == dataPlane {
		return nil
	}
	return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("%s runs on data plane %q, not this token's %q", what, dataPlane, c.dataPlane))
}

// pipelineOnDataPlane fails unless a version of the pipeline runs on the
// caller's data plane. Earlier versions count, so attempts still running
// when a pipeline moves can report.
func (s *Server) pipelineOnDataPlane(ctx context.Context, id uuid.UUID) error {
	dp := callerOf(ctx).dataPlane
	r, err := s.Store.PipelineOnDataPlane(ctx, db.PipelineOnDataPlaneParams{OrgID: s.Tenant.Org, PipelineID: id, DataPlaneID: dp})
	switch {
	case err != nil:
		return err
	case !r.Found:
		return notFound("pipeline %s doesn't exist", id)
	case !r.OnDataPlane:
		return connect.NewError(connect.CodePermissionDenied, fmt.Errorf("pipeline %s doesn't run on data plane %q", id, dp))
	}
	return nil
}
