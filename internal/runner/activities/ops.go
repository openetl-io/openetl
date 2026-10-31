package activities

import (
	"context"
	"time"

	"connectrpc.com/connect"
	"go.temporal.io/sdk/activity"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/unsuitable001/openetl/gen/openetl/dataplane/v1"
	"github.com/unsuitable001/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/temporalx"
	"github.com/unsuitable001/openetl/internal/streamschema"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Ops runs connector operations for ConnectorOpWorkflow.
type Ops struct {
	API dataplanev1connect.DataPlaneServiceClient
}

// CheckConnector checks a source or destination. A failed check is a result,
// not an error.
func (a *Ops) CheckConnector(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	cfg, err := a.config(ctx, in)
	if err != nil {
		return contract.ConnectorOpResult{}, temporalx.ApplicationError(err)
	}
	res := contract.ConnectorOpResult{Version: cfg.GetVersion()}
	if err := check(ctx, in, cfg.GetConfig()); err != nil {
		res.Code, res.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
	}
	return res, nil
}

func check(ctx context.Context, in contract.ConnectorOpInput, cfg *apiv1.ConnectorConfig) error {
	raw, err := protojson.Marshal(cfg.GetConfig())
	if err != nil {
		return err
	}
	if in.SourceID != "" {
		src, err := connectorhost.NewSource(cfg.GetConnector(), raw)
		if err != nil {
			return err
		}
		if c, ok := src.(source.Checker); ok {
			return c.Check(ctx)
		}
		return nil
	}
	dst, err := connectorhost.NewDestination(cfg.GetConnector(), raw)
	if err != nil {
		return err
	}
	if c, ok := dst.(destination.Checker); ok {
		return c.Check(ctx)
	}
	return nil
}

// DiscoverSource discovers a source's catalog and reports it to the API,
// returning its digest.
func (a *Ops) DiscoverSource(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	stop := heartbeats(ctx, func() any { return nil })
	defer stop()
	res, err := a.discover(ctx, in)
	return res, temporalx.ApplicationError(err)
}

func (a *Ops) discover(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	var res contract.ConnectorOpResult
	cfg, err := a.config(ctx, in)
	if err != nil {
		return res, err
	}
	res.Version = cfg.GetVersion()
	raw, err := protojson.Marshal(cfg.GetConfig().GetConfig())
	if err != nil {
		return res, err
	}
	src, err := connectorhost.NewSource(cfg.GetConfig().GetConnector(), raw)
	if err != nil {
		return res, err
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return res, err
	}
	rep, err := a.API.ReportCatalog(ctx, connect.NewRequest(&dataplanev1.ReportCatalogRequest{
		SourceId: in.SourceID, Version: res.Version, Catalog: catalogProto(cat),
	}))
	if err != nil {
		return res, errs.Wrap(errs.Transient, err, "report the catalog")
	}
	res.Catalog = rep.Msg.GetDigest()
	return res, nil
}

func (a *Ops) config(ctx context.Context, in contract.ConnectorOpInput) (*dataplanev1.GetConnectorConfigResponse, error) {
	req := &dataplanev1.GetConnectorConfigRequest{}
	if in.SourceID != "" {
		req.Connector = &dataplanev1.GetConnectorConfigRequest_SourceId{SourceId: in.SourceID}
	} else {
		req.Connector = &dataplanev1.GetConnectorConfigRequest_DestinationId{DestinationId: in.DestinationID}
	}
	resp, err := a.API.GetConnectorConfig(ctx, connect.NewRequest(req))
	if err != nil {
		code := errs.Transient
		if c := connect.CodeOf(err); c == connect.CodeNotFound || c == connect.CodeInvalidArgument {
			code = errs.ConfigInvalid
		}
		return nil, errs.Wrap(code, err, "fetch the connector config")
	}
	if resp.Msg.GetOrgId() != in.OrgID {
		return nil, errs.New(errs.ConfigInvalid, "the connector belongs to another org")
	}
	return resp.Msg, nil
}

var replays = map[source.Replay]apiv1.Replay{source.Exact: apiv1.Replay_REPLAY_EXACT, source.Superset: apiv1.Replay_REPLAY_SUPERSET}

func catalogProto(cat source.Catalog) *apiv1.Catalog {
	out := &apiv1.Catalog{}
	for _, cs := range cat.Streams {
		s := &apiv1.CatalogStream{Name: cs.Name, PrimaryKey: cs.PrimaryKey, CursorField: cs.CursorField, Replay: replays[cs.Replay]}
		if cs.Schema != nil {
			for _, c := range streamschema.Columns(cs.Schema) {
				s.Columns = append(s.Columns, &apiv1.CatalogColumn{Name: c.Name, Type: c.Type, Nullable: c.Nullable})
			}
		}
		out.Streams = append(out.Streams, s)
	}
	return out
}

// heartbeats records a heartbeat with progress() every 30 s, or a quarter
// of the heartbeat timeout if that's shorter, until stop is called.
func heartbeats(ctx context.Context, progress func() any) (stop func()) {
	every := 30 * time.Second
	if t := activity.GetInfo(ctx).HeartbeatTimeout / 4; t > 0 && t < every {
		every = t
	}
	done := make(chan struct{})
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		t := time.NewTicker(every)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				activity.RecordHeartbeat(ctx, progress())
			case <-done:
				return
			}
		}
	}()
	return func() {
		close(done)
		<-finished
	}
}
