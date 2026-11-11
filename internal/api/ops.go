package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/store/db"
)

// OpRunner is implemented by ops.Ops.
type OpRunner interface {
	Run(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error)
}

func (s *Server) CheckSource(ctx context.Context, req *connect.Request[apiv1.CheckSourceRequest]) (*connect.Response[apiv1.CheckSourceResponse], error) {
	_, c, err := s.getSource(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), c, contract.ConnectorOpInput{Op: contract.OpCheck, SourceID: c.id.String()})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.CheckSourceResponse{Result: checkResult(res)}), nil
}

func (s *Server) CheckDestination(ctx context.Context, req *connect.Request[apiv1.CheckDestinationRequest]) (*connect.Response[apiv1.CheckDestinationResponse], error) {
	_, c, err := s.getDestination(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), c, contract.ConnectorOpInput{Op: contract.OpCheck, DestinationID: c.id.String()})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&apiv1.CheckDestinationResponse{Result: checkResult(res)}), nil
}

func (s *Server) DiscoverSource(ctx context.Context, req *connect.Request[apiv1.DiscoverSourceRequest]) (*connect.Response[apiv1.DiscoverSourceResponse], error) {
	_, c, err := s.getSource(ctx, req.Msg.GetName())
	if err != nil {
		return nil, err
	}
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), c, contract.ConnectorOpInput{Op: contract.OpDiscover, SourceID: c.id.String()})
	if err != nil {
		return nil, err
	}
	row, err := s.Store.GetSourceCatalog(ctx, db.GetSourceCatalogParams{OrgID: s.Tenant.Org, SourceID: c.id, Digest: res.Catalog})
	if err != nil {
		return nil, err
	}
	cat := &apiv1.Catalog{}
	if err := protojson.Unmarshal(row.Catalog, cat); err != nil {
		return nil, err
	}
	cat.Version, cat.DiscoverTime = row.SourceVersion, timestamppb.New(row.DiscoverTime)
	return connect.NewResponse(&apiv1.DiscoverSourceResponse{Catalog: cat}), nil
}

// teardowns are what the deleted pipeline p's connectors keep for it and its
// Flink jobs, to remove once it's gone, recorded with the deletion so a
// failure can't lose them. They run on a runner: orchestrators retry them,
// and DeleteSource and the like don't wait for them, as each keeps what its
// runner reads.
func (s *Server) teardowns(ctx context.Context, p db.Pipeline, pins []db.GetPinnedFlinkClustersRow) ([]db.InsertTeardownParams, error) {
	src, err := s.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: s.Tenant.Org, ID: p.SourceID})
	if err != nil {
		return nil, err
	}
	dst, err := s.Store.GetDestinationByID(ctx, db.GetDestinationByIDParams{OrgID: s.Tenant.Org, ID: p.DestinationID})
	if err != nil {
		return nil, err
	}
	var out []db.InsertTeardownParams
	for _, c := range []struct {
		role string
		conn connection
	}{{"source", fromSource(src)}, {"destination", fromDestination(dst)}} {
		cfg, err := c.conn.config()
		if err != nil {
			s.log().Error("no teardown for a connector whose config can't be read", "pipeline", p.ID, "of", c.conn.name, "err", err)
			continue
		}
		if !s.mayNeedSetup(ctx, c.role, cfg.GetConnector(), c.conn.connector) {
			continue
		}
		t := db.InsertTeardownParams{
			OrgID: s.Tenant.Org, ID: uuid.New(), WorkspaceID: c.conn.workspace, DataPlaneID: p.DataPlaneID, PipelineID: p.ID, Pipeline: p.Name,
			Op: contract.OpTeardown, Connector: cfg.GetConnector(), Name: c.conn.name, Spec: c.conn.spec, Version: c.conn.version,
			ConnectorDigest: c.conn.connector, Secrets: nonNil(c.conn.secrets),
		}
		if c.role == "source" {
			t.SourceID = &c.conn.id
		} else {
			t.DestinationID = &c.conn.id
		}
		out = append(out, t)
	}
	for _, pin := range pins {
		out = append(out, db.InsertTeardownParams{
			OrgID: s.Tenant.Org, ID: uuid.New(), WorkspaceID: p.WorkspaceID, DataPlaneID: p.DataPlaneID, PipelineID: p.ID, Pipeline: p.Name,
			Op: contract.OpTeardownFlink, Name: pin.Name, Spec: pin.Spec, Version: pin.Version, Secrets: nonNil(pin.Secrets),
		})
	}
	return out, nil
}

// teardownOf is the connection a teardown sent to the caller's data plane
// kept, if op is one on the source or destination, which may be gone by now.
func (s *Server) teardownOf(ctx context.Context, op string, source, destination uuid.UUID) (connection, bool, error) {
	id, err := uuid.Parse(op)
	if err != nil {
		return connection{}, false, nil
	}
	r, err := s.Store.GetTeardown(ctx, db.GetTeardownParams{OrgID: s.Tenant.Org, ID: id, DataPlaneID: callerOf(ctx).dataPlane})
	if errors.Is(err, pgx.ErrNoRows) {
		return connection{}, false, nil
	}
	if err != nil {
		return connection{}, false, err
	}
	onSource := source != uuid.Nil && r.SourceID != nil && *r.SourceID == source
	onDestination := destination != uuid.Nil && r.DestinationID != nil && *r.DestinationID == destination
	if !onSource && !onDestination {
		return connection{}, false, nil
	}
	return connection{name: r.Name, workspace: r.WorkspaceID, spec: r.Spec, version: r.Version, connector: r.ConnectorDigest, secrets: r.Secrets}, true, nil
}

// mayNeedSetup is false when the connector is known not to take Setup and
// Teardown: a manifest, or a package whose spec doesn't set setup.
func (s *Server) mayNeedSetup(ctx context.Context, role, connector, digest string) bool {
	if digest == "" {
		return true
	}
	name, _ := connectorhost.SplitRef(connector)
	v, err := s.resolveConnector(ctx, s.Store.Queries, role, name, digest)
	if err != nil || v == nil {
		return true
	}
	r, ok := v.Roles[role]
	return !ok || r.Manifest == nil && (r.Spec == nil || r.Spec.GetSetup())
}

func checkResult(r contract.ConnectorOpResult) *apiv1.CheckResult {
	return &apiv1.CheckResult{Ok: r.Code == "", Code: r.Code, Message: r.Message, Version: r.Version}
}

// runOp runs a connector operation on c, on the data plane dp or the
// default, and waits for it.
func (s *Server) runOp(ctx context.Context, dp string, c connection, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	dp = cmp.Or(dp, s.DefaultDataPlane)
	if _, err := s.Store.GetDataPlane(ctx, db.GetDataPlaneParams{OrgID: s.Tenant.Org, ID: dp}); errors.Is(err, pgx.ErrNoRows) {
		return contract.ConnectorOpResult{}, invalid("data plane %q doesn't exist", dp)
	} else if err != nil {
		return contract.ConnectorOpResult{}, err
	}
	cfg, err := c.config()
	if err != nil {
		return contract.ConnectorOpResult{}, err
	}
	in.OrgID, in.WorkspaceID, in.DataPlane, in.Connector = s.Tenant.Org.String(), c.workspace.String(), dp, cfg.GetConnector()
	if err := s.sendOp(ctx, &in); err != nil {
		return contract.ConnectorOpResult{}, err
	}
	audit(ctx).set("operation", in.OpID)
	audit(ctx).set("dataPlane", dp)
	defer func() {
		id := uuid.MustParse(in.OpID)
		if err := s.Store.DeleteConnectorOp(context.WithoutCancel(ctx), db.DeleteConnectorOpParams{OrgID: s.Tenant.Org, ID: id}); err != nil {
			s.log().Warn("connector operation not deleted; it expires", "op", in.OpID, "err", err)
		}
	}()
	res, err := s.Ops.Run(ctx, in)
	if err == nil {
		return res, nil
	}
	if f, ok := errors.AsType[*orchestration.Failure](err); ok {
		if f.Code == contract.NoRunner {
			return res, connect.NewError(connect.CodeUnavailable, fmt.Errorf("no runner of data plane %q took the %s within %s", dp, in.Op, contract.OpPickup))
		}
		code := connect.CodeFailedPrecondition
		switch f.Code {
		case errs.ConfigInvalid.String():
			code = connect.CodeInvalidArgument
		case errs.Transient.String(), errs.RateLimited.String(), errs.DestinationUnavailable.String(), errs.Internal.String():
			code = connect.CodeUnavailable
		}
		return res, connect.NewError(code, fmt.Errorf("%s: %s", f.Code, f.Message))
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return res, err
	}
	s.log().Error("connector operation failed", "op", in.Op, "err", err)
	return res, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the %s failed: %w", in.Op, err))
}

// sendOp records the operation, so the data plane's runners may read the
// connector's config for it.
func (s *Server) sendOp(ctx context.Context, in *contract.ConnectorOpInput) error {
	var op db.InsertConnectorOpParams
	var err error
	if in.SourceID != "" {
		op.SourceID, err = parseUUID(in.SourceID)
	} else {
		op.DestinationID, err = parseUUID(in.DestinationID)
	}
	if err != nil {
		return err
	}
	return s.insertOp(ctx, in, op)
}

// insertOp records op, which names what it's on, as sent to in's data
// plane, and sets in's OpID.
func (s *Server) insertOp(ctx context.Context, in *contract.ConnectorOpInput, op db.InsertConnectorOpParams) error {
	if err := s.Store.DeleteExpiredConnectorOps(ctx, s.Tenant.Org); err != nil {
		return err
	}
	op.OrgID, op.ID, op.DataPlaneID = s.Tenant.Org, uuid.New(), in.DataPlane
	op.ExpireTime = time.Now().Add(contract.OpTimeout + time.Minute)
	in.OpID = op.ID.String()
	return s.Store.InsertConnectorOp(ctx, op)
}

func parseUUID(s string) (*uuid.UUID, error) {
	u, err := uuid.Parse(s)
	return &u, err
}

// sentOp fails unless op is a check or discover of the connector that the
// API sent to the caller's data plane.
func (s *Server) sentOp(ctx context.Context, op string, source, destination uuid.UUID) error {
	dp := callerOf(ctx).dataPlane
	denied := connect.NewError(connect.CodePermissionDenied, fmt.Errorf("the API sent no operation %q on it to data plane %q", op, dp))
	id, err := uuid.Parse(op)
	if err != nil {
		return denied
	}
	row, err := s.Store.GetConnectorOp(ctx, db.GetConnectorOpParams{OrgID: s.Tenant.Org, ID: id, DataPlaneID: dp})
	if errors.Is(err, pgx.ErrNoRows) {
		return denied
	}
	if err != nil {
		return err
	}
	if source != uuid.Nil && row.SourceID != nil && *row.SourceID == source || destination != uuid.Nil && row.DestinationID != nil && *row.DestinationID == destination {
		return nil
	}
	return denied
}

func (s *Server) GetConnectorConfig(ctx context.Context, req *connect.Request[dataplanev1.GetConnectorConfigRequest]) (*connect.Response[dataplanev1.GetConnectorConfigResponse], error) {
	var c connection
	what := "source "
	audit(ctx).set("operation", req.Msg.GetOperationId())
	switch id := req.Msg.GetConnector().(type) {
	case *dataplanev1.GetConnectorConfigRequest_SourceId:
		audit(ctx).set("sourceId", id.SourceId)
		u, err := uuid.Parse(id.SourceId)
		if err != nil {
			return nil, invalid("source_id: %v", err)
		}
		kept, ok, err := s.teardownOf(ctx, req.Msg.GetOperationId(), u, uuid.Nil)
		if err != nil {
			return nil, err
		}
		if ok {
			c = kept
			break
		}
		if err := s.sentOp(ctx, req.Msg.GetOperationId(), u, uuid.Nil); err != nil {
			return nil, err
		}
		r, err := s.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: s.Tenant.Org, ID: u})
		if err != nil {
			return nil, err
		}
		c = fromSource(r)
	case *dataplanev1.GetConnectorConfigRequest_DestinationId:
		audit(ctx).set("destinationId", id.DestinationId)
		u, err := uuid.Parse(id.DestinationId)
		if err != nil {
			return nil, invalid("destination_id: %v", err)
		}
		what = "destination "
		kept, ok, err := s.teardownOf(ctx, req.Msg.GetOperationId(), uuid.Nil, u)
		if err != nil {
			return nil, err
		}
		if ok {
			c = kept
			break
		}
		if err := s.sentOp(ctx, req.Msg.GetOperationId(), uuid.Nil, u); err != nil {
			return nil, err
		}
		r, err := s.Store.GetDestinationByID(ctx, db.GetDestinationByIDParams{OrgID: s.Tenant.Org, ID: u})
		if err != nil {
			return nil, err
		}
		c = fromDestination(r)
	default:
		return nil, invalid("name a source_id or a destination_id")
	}
	cfg, err := c.config()
	if err != nil {
		return nil, err
	}
	external, err := s.resolveSecrets(ctx, what+c.name, c.workspace, cfg, c.secrets)
	if err != nil {
		return nil, err
	}
	ws, err := s.Store.GetWorkspaceName(ctx, db.GetWorkspaceNameParams{OrgID: s.Tenant.Org, ID: c.workspace})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.GetConnectorConfigResponse{OrgId: s.Tenant.Org.String(), Version: c.version, Config: cfg, Connector: c.connector, Secrets: external,
		Workspace: ws, Name: c.name}), nil
}

func (s *Server) ReportCatalog(ctx context.Context, req *connect.Request[dataplanev1.ReportCatalogRequest]) (*connect.Response[dataplanev1.ReportCatalogResponse], error) {
	id, err := uuid.Parse(req.Msg.GetSourceId())
	if err != nil {
		return nil, invalid("source_id: %v", err)
	}
	if err := s.sentOp(ctx, req.Msg.GetOperationId(), id, uuid.Nil); err != nil {
		return nil, err
	}
	cat := &apiv1.Catalog{Streams: req.Msg.GetCatalog().GetStreams()}
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(cat)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(b)
	digest := hex.EncodeToString(sum[:])
	stored, err := protojson.Marshal(cat)
	if err != nil {
		return nil, err
	}
	if _, err := s.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: s.Tenant.Org, ID: id}); err != nil {
		return nil, err
	}
	err = s.Store.UpsertSourceCatalog(ctx, db.UpsertSourceCatalogParams{
		OrgID: s.Tenant.Org, SourceID: id, Digest: digest, SourceVersion: req.Msg.GetVersion(), Catalog: stored,
	})
	if err != nil {
		return nil, err
	}
	return connect.NewResponse(&dataplanev1.ReportCatalogResponse{Digest: digest}), nil
}
