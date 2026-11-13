// Package api serves the public API and the data-plane API over Connect.
package api

import (
	"context"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"connectrpc.com/vanguard"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration/dispatch"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/secrets"
	"github.com/openetl-io/openetl/internal/store"
)

// Scheduler is implemented by schedules.Schedules.
type Scheduler interface {
	Upsert(ctx context.Context, orgID, pipelineID string, s *apiv1.Schedule, paused bool) error
	Delete(ctx context.Context, pipelineID string) error
	SetPaused(ctx context.Context, pipelineID string, paused bool) error
	// Returns schedules.ErrSyncRunning if a sync is running.
	Trigger(ctx context.Context, pipelineID string) error
	// Starts a continuous pipeline's supervisor, or has it look at the
	// pipeline again.
	Refresh(ctx context.Context, orgID, pipelineID string) error
	// Stops the pipeline's supervisor, if it has one.
	StopStream(ctx context.Context, pipelineID string) error
}

type Server struct {
	Store     *store.Store
	Tenant    store.Tenant
	Schedules Scheduler
	Ops       OpRunner
	// Reads syncs' logs on their data planes. Nil: they can't be read.
	Logs LogReader
	// Plans pipelines' transform.sql on their data planes. Nil: the plan
	// says it couldn't.
	Planner SQLPlanner
	// Hands workflows' work to runners, and takes their reports and results.
	Dispatch *dispatch.Dispatcher
	// For pipelines and connector operations that name none.
	DefaultDataPlane string
	// Which connectors exist, to validate configs; only names, manifests
	// and the connectors directory matter. Applied connector versions are
	// added to it.
	Connectors connectorhost.Env
	// Downloads external connectors' binaries, to pin the digests their
	// descriptors leave out. Nil: only pinned descriptors can be applied.
	Downloads *http.Client
	// Seals secrets' values. Nil: secrets can't be stored.
	Secrets *secrets.Keyring
	// How the catalog and Apply reach registries and GitHub, through
	// Downloads.
	Packages PackageOptions
	// How far behind its source a continuous pipeline's read gets before
	// it's lagging. Zero: 5 min.
	LaggingAfter time.Duration
	// Sign-in is off: calls without a credential act as LocalPrincipal, an
	// owner.
	Authless bool
	// With Authless, the server is meant to be reached from elsewhere, so a
	// call may name it by any Host.
	AuthlessRemote bool
	// The URL browsers use. Password links start with it, and an https one
	// makes the session's cookie Secure. Empty: http://localhost.
	PublicURL string
	// How long a session lasts idle, and in all. Zero: 12 hours and 7 days.
	SessionIdle, SessionMaxAge time.Duration
	// Sign-ins a minute each client address may make. Zero: 20.
	SignInRate int
	Logger     *slog.Logger

	signIns signIns
}

var (
	_ apiv1connect.ApplyServiceHandler           = (*Server)(nil)
	_ apiv1connect.PipelineServiceHandler        = (*Server)(nil)
	_ apiv1connect.SyncServiceHandler            = (*Server)(nil)
	_ apiv1connect.SourceServiceHandler          = (*Server)(nil)
	_ apiv1connect.DestinationServiceHandler     = (*Server)(nil)
	_ apiv1connect.DbtProjectServiceHandler      = (*Server)(nil)
	_ apiv1connect.SparkClusterServiceHandler    = (*Server)(nil)
	_ apiv1connect.FlinkClusterServiceHandler    = (*Server)(nil)
	_ apiv1connect.SecretServiceHandler          = (*Server)(nil)
	_ apiv1connect.AccessServiceHandler          = (*Server)(nil)
	_ apiv1connect.AuditServiceHandler           = (*Server)(nil)
	_ apiv1connect.WebhookServiceHandler         = (*Server)(nil)
	_ apiv1connect.IncidentServiceHandler        = (*Server)(nil)
	_ apiv1connect.ConnectorServiceHandler       = (*Server)(nil)
	_ dataplanev1connect.DataPlaneServiceHandler = (*Server)(nil)
)

// The most a request's message may hold, decompressed. Apply carries dbt
// bundles of up to 16 MiB, base64-encoded in their documents, and runners
// report the catalogs and schemas of large databases.
const (
	maxRequestBytes = 4 << 20
	maxApplyBytes   = 32 << 20
	maxRunnerBytes  = 32 << 20
)

//go:embed openapi.yaml
var openAPI []byte

// Handler serves the public API over Connect, gRPC, gRPC-Web and REST (its
// google.api.http rules, transcoded by Vanguard), and DataPlaneService over
// the first three.
func (s *Server) Handler() (http.Handler, error) {
	otel, err := otelconnect.NewInterceptor()
	if err != nil {
		return nil, err
	}
	opts := connect.WithHandlerOptions(connect.WithInterceptors(otel, s.errorInterceptor(), authInterceptor{s}), connect.WithReadMaxBytes(maxRequestBytes))
	services := []*vanguard.Service{
		vanguard.NewService(apiv1connect.NewApplyServiceHandler(s, opts, connect.WithReadMaxBytes(maxApplyBytes))),
		vanguard.NewService(apiv1connect.NewPipelineServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewSyncServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewSourceServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewDestinationServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewDbtProjectServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewSparkClusterServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewFlinkClusterServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewSecretServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewAccessServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewAuditServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewWebhookServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewIncidentServiceHandler(s, opts)),
		vanguard.NewService(apiv1connect.NewConnectorServiceHandler(s, opts)),
	}
	// REST bodies are buffered whole before the handler's limit applies.
	transcoder, err := vanguard.NewTranscoder(services, vanguard.WithDefaultServiceOptions(vanguard.WithMaxMessageBufferBytes(2*maxApplyBytes)))
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	guarded, err := s.guardCookies(transcoder)
	if err != nil {
		return nil, err
	}
	if guarded, err = s.guardAuthless(guarded); err != nil {
		return nil, err
	}
	mux.Handle("/", guarded)
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(s, opts, connect.WithReadMaxBytes(maxRunnerBytes)))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("GET /logos/{digest}", s.serveLogo)
	mux.HandleFunc("GET /openapi.yaml", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/yaml")
		_, _ = w.Write(openAPI)
	})
	return mux, nil
}

func (s *Server) log() *slog.Logger {
	if s.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Logger
}

// errorInterceptor turns errors that aren't Connect errors into Connect
// codes.
func (s *Server) errorInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			if err != nil {
				return nil, s.connectError(req.Spec().Procedure, err)
			}
			return res, nil
		}
	}
}

// connectError turns an error that isn't a Connect error into a Connect
// code, and logs internal ones.
func (s *Server) connectError(procedure string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[*connect.Error](err); ok {
		return err
	}
	// Postgres rolled the transaction back for a deadlock or a serialization
	// failure, so the request changed nothing.
	if pe, ok := errors.AsType[*pgconn.PgError](err); ok && (pe.Code == "40P01" || pe.Code == "40001") {
		return connect.NewError(connect.CodeAborted, errors.New("the request conflicted with another one and changed nothing; try again"))
	}
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return connect.NewError(connect.CodeNotFound, err)
	case errs.CodeOf(err) == errs.ConfigInvalid:
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, context.Canceled):
		return connect.NewError(connect.CodeCanceled, err)
	}
	s.log().Error("request failed", "procedure", procedure, "err", err)
	return connect.NewError(connect.CodeInternal, errors.New("internal error"))
}

func invalid(format string, args ...any) error {
	return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf(format, args...))
}

func notFound(format string, args ...any) error {
	return connect.NewError(connect.CodeNotFound, fmt.Errorf(format, args...))
}

// parseName splits workspaces/{workspace}/{collection}/{id}.
func parseName(name, collection string) (workspace, id string, err error) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[0] != "workspaces" || parts[2] != collection || parts[1] == "" || parts[3] == "" {
		return "", "", invalid("name %q isn't workspaces/{workspace}/%s/{id}", name, collection)
	}
	return parts[1], parts[3], nil
}

// parseParent parses workspaces/{workspace}.
func parseParent(parent string) (string, error) {
	ws, ok := strings.CutPrefix(parent, "workspaces/")
	if !ok || ws == "" || strings.Contains(ws, "/") {
		return "", invalid("%q isn't workspaces/{workspace}", parent)
	}
	return ws, nil
}

func resourceName(workspace, collection, id string) string {
	return "workspaces/" + workspace + "/" + collection + "/" + id
}

const (
	defaultPageSize = 50
	maxPageSize     = 500
)

// page returns the name to list after, and how many rows to fetch: one more
// than the page, to learn whether another page follows.
func page(size int32, token string) (after string, rows int32, err error) {
	if token != "" {
		b, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return "", 0, invalid("bad page_token")
		}
		after = string(b)
	}
	switch {
	case size < 0:
		return "", 0, invalid("page_size must not be negative")
	case size == 0:
		size = defaultPageSize
	case size > maxPageSize:
		size = maxPageSize
	}
	return after, size + 1, nil
}

func nextToken(last string) string { return base64.RawURLEncoding.EncodeToString([]byte(last)) }
