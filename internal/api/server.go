// Package api serves the public API and the data-plane API over Connect.
package api

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/gen/openetl/api/v1/apiv1connect"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/store"
)

// Scheduler is implemented by schedules.Schedules.
type Scheduler interface {
	Upsert(ctx context.Context, orgID, pipelineID string, s *apiv1.Schedule, paused bool) error
	Delete(ctx context.Context, pipelineID string) error
	SetPaused(ctx context.Context, pipelineID string, paused bool) error
	// Returns schedules.ErrSyncRunning if a sync is running.
	Trigger(ctx context.Context, pipelineID string) error
}

type Server struct {
	Store     *store.Store
	Tenant    store.Tenant
	Schedules Scheduler
	Ops       OpRunner
	// For pipelines and connector operations that name none.
	DefaultDataPlane string
	// Which connectors exist, to validate configs; only names, manifests
	// and the connectors directory matter.
	Connectors connectorhost.Env
	Logger     *slog.Logger
}

var (
	_ apiv1connect.ApplyServiceHandler           = (*Server)(nil)
	_ apiv1connect.PipelineServiceHandler        = (*Server)(nil)
	_ apiv1connect.SourceServiceHandler          = (*Server)(nil)
	_ apiv1connect.DestinationServiceHandler     = (*Server)(nil)
	_ dataplanev1connect.DataPlaneServiceHandler = (*Server)(nil)
)

func (s *Server) Handler() http.Handler {
	opts := connect.WithInterceptors(s.errorInterceptor())
	mux := http.NewServeMux()
	mux.Handle(apiv1connect.NewApplyServiceHandler(s, opts))
	mux.Handle(apiv1connect.NewPipelineServiceHandler(s, opts))
	mux.Handle(apiv1connect.NewSourceServiceHandler(s, opts))
	mux.Handle(apiv1connect.NewDestinationServiceHandler(s, opts))
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(s, opts))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	return mux
}

func (s *Server) log() *slog.Logger {
	if s.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return s.Logger
}

// errorInterceptor turns errors that aren't Connect errors into Connect
// codes, and logs internal ones.
func (s *Server) errorInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			res, err := next(ctx, req)
			if err == nil {
				return res, nil
			}
			if _, ok := errors.AsType[*connect.Error](err); ok {
				return nil, err
			}
			switch {
			case errors.Is(err, pgx.ErrNoRows):
				return nil, connect.NewError(connect.CodeNotFound, err)
			case errs.CodeOf(err) == errs.ConfigInvalid:
				return nil, connect.NewError(connect.CodeInvalidArgument, err)
			case errors.Is(err, context.Canceled):
				return nil, connect.NewError(connect.CodeCanceled, err)
			}
			s.log().Error("request failed", "procedure", req.Spec().Procedure, "err", err)
			return nil, connect.NewError(connect.CodeInternal, errors.New("internal error"))
		}
	}
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
