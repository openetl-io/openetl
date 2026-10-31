package api

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/store/db"
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
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), contract.ConnectorOpInput{Op: contract.OpCheck, SourceID: c.id.String()})
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
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), contract.ConnectorOpInput{Op: contract.OpCheck, DestinationID: c.id.String()})
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
	res, err := s.runOp(ctx, req.Msg.GetDataPlane(), contract.ConnectorOpInput{Op: contract.OpDiscover, SourceID: c.id.String()})
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

func checkResult(r contract.ConnectorOpResult) *apiv1.CheckResult {
	return &apiv1.CheckResult{Ok: r.Code == "", Code: r.Code, Message: r.Message, Version: r.Version}
}

// runOp runs a connector operation on the data plane, the default if dp is
// empty, and waits for it.
func (s *Server) runOp(ctx context.Context, dp string, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	dp = cmp.Or(dp, s.DefaultDataPlane)
	if _, err := s.Store.GetDataPlane(ctx, db.GetDataPlaneParams{OrgID: s.Tenant.Org, ID: dp}); errors.Is(err, pgx.ErrNoRows) {
		return contract.ConnectorOpResult{}, invalid("data plane %q doesn't exist", dp)
	} else if err != nil {
		return contract.ConnectorOpResult{}, err
	}
	in.OrgID, in.DataPlane = s.Tenant.Org.String(), dp
	res, err := s.Ops.Run(ctx, in)
	if err == nil {
		return res, nil
	}
	if app, ok := errors.AsType[*temporal.ApplicationError](err); ok {
		code := connect.CodeFailedPrecondition
		switch app.Type() {
		case errs.ConfigInvalid.String():
			code = connect.CodeInvalidArgument
		case errs.Transient.String(), errs.RateLimited.String(), errs.DestinationUnavailable.String(), errs.Internal.String():
			code = connect.CodeUnavailable
		}
		return res, connect.NewError(code, fmt.Errorf("%s: %s", app.Type(), app.Message()))
	}
	if t, ok := errors.AsType[*temporal.TimeoutError](err); ok && t.TimeoutType() == enumspb.TIMEOUT_TYPE_SCHEDULE_TO_START {
		return res, connect.NewError(connect.CodeUnavailable, fmt.Errorf("no runner of data plane %q took the %s within %s", dp, in.Op, contract.OpPickup))
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return res, err
	}
	s.log().Error("connector operation failed", "op", in.Op, "err", err)
	return res, connect.NewError(connect.CodeUnavailable, fmt.Errorf("the %s failed: %w", in.Op, err))
}

func (s *Server) GetConnectorConfig(ctx context.Context, req *connect.Request[dataplanev1.GetConnectorConfigRequest]) (*connect.Response[dataplanev1.GetConnectorConfigResponse], error) {
	var c connection
	switch id := req.Msg.GetConnector().(type) {
	case *dataplanev1.GetConnectorConfigRequest_SourceId:
		u, err := uuid.Parse(id.SourceId)
		if err != nil {
			return nil, invalid("source_id: %v", err)
		}
		r, err := s.Store.GetSourceByID(ctx, db.GetSourceByIDParams{OrgID: s.Tenant.Org, ID: u})
		if err != nil {
			return nil, err
		}
		c = fromSource(r)
	case *dataplanev1.GetConnectorConfigRequest_DestinationId:
		u, err := uuid.Parse(id.DestinationId)
		if err != nil {
			return nil, invalid("destination_id: %v", err)
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
	return connect.NewResponse(&dataplanev1.GetConnectorConfigResponse{OrgId: s.Tenant.Org.String(), Version: c.version, Config: cfg}), nil
}

func (s *Server) ReportCatalog(ctx context.Context, req *connect.Request[dataplanev1.ReportCatalogRequest]) (*connect.Response[dataplanev1.ReportCatalogResponse], error) {
	id, err := uuid.Parse(req.Msg.GetSourceId())
	if err != nil {
		return nil, invalid("source_id: %v", err)
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
