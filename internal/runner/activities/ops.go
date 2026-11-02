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
	"github.com/unsuitable001/openetl/internal/secretstore"
	"github.com/unsuitable001/openetl/internal/streamschema"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// Ops runs connector operations for ConnectorOpWorkflow.
type Ops struct {
	API        dataplanev1connect.DataPlaneServiceClient
	Connectors connectorhost.Env
	// Nil: connector versions can't be fetched.
	Versions *connectorhost.Fetcher
	// Nil: secrets in cloud secret stores can't be read.
	Secrets *secretstore.Reader
}

// CheckConnector checks a source or destination. A failed check is a result,
// not an error.
func (a *Ops) CheckConnector(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	cfg, err := a.config(ctx, in)
	if err != nil {
		return contract.ConnectorOpResult{}, temporalx.ApplicationError(err)
	}
	res := contract.ConnectorOpResult{Version: cfg.GetVersion()}
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, cfg.GetConnector())
	if err == nil {
		err = readSecrets(ctx, a.Secrets, cfg.GetConfig(), cfg.GetSecrets())
	}
	if err == nil {
		err = check(ctx, in, cfg.GetConfig(), env)
	}
	if err != nil {
		res.Code, res.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
	}
	return res, nil
}

func check(ctx context.Context, in contract.ConnectorOpInput, cfg *apiv1.ConnectorConfig, env connectorhost.Env) error {
	raw, err := protojson.Marshal(cfg.GetConfig())
	if err != nil {
		return err
	}
	name, _ := connectorhost.SplitRef(cfg.GetConnector())
	if in.SourceID != "" {
		src, err := connectorhost.NewSource(name, raw, env)
		if err != nil {
			return err
		}
		defer connectorhost.Close(src)
		if c, ok := src.(source.Checker); ok {
			return c.Check(ctx)
		}
		return nil
	}
	dst, err := connectorhost.NewDestination(name, raw, env)
	if err != nil {
		return err
	}
	defer connectorhost.Close(dst)
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
	if err := readSecrets(ctx, a.Secrets, cfg.GetConfig(), cfg.GetSecrets()); err != nil {
		return res, err
	}
	raw, err := protojson.Marshal(cfg.GetConfig().GetConfig())
	if err != nil {
		return res, err
	}
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, cfg.GetConnector())
	if err != nil {
		return res, err
	}
	name, _ := connectorhost.SplitRef(cfg.GetConfig().GetConnector())
	src, err := connectorhost.NewSource(name, raw, env)
	if err != nil {
		return res, err
	}
	defer connectorhost.Close(src)
	cat, err := src.Discover(ctx)
	if err != nil {
		return res, err
	}
	rep, err := a.API.ReportCatalog(ctx, connect.NewRequest(&dataplanev1.ReportCatalogRequest{
		SourceId: in.SourceID, Version: res.Version, Catalog: catalogProto(cat), OperationId: in.OpID,
	}))
	if err != nil {
		return res, errs.Wrap(errs.Transient, err, "report the catalog")
	}
	res.Catalog = rep.Msg.GetDigest()
	return res, nil
}

func (a *Ops) config(ctx context.Context, in contract.ConnectorOpInput) (*dataplanev1.GetConnectorConfigResponse, error) {
	req := &dataplanev1.GetConnectorConfigRequest{OperationId: in.OpID}
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
		for _, m := range cs.SyncModes {
			s.SyncModes = append(s.SyncModes, apiv1.SyncMode(m)) //nolint:gosec // small
		}
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

// connectorEnv is env with the connector versions at digests added; empty
// digests add nothing.
func connectorEnv(ctx context.Context, env connectorhost.Env, versions *connectorhost.Fetcher, digests ...string) (connectorhost.Env, error) {
	for _, d := range digests {
		if d == "" {
			continue
		}
		if versions == nil {
			return env, errs.New(errs.ConfigInvalid, "this runner can't fetch connector versions")
		}
		v, err := versions.Fetch(ctx, d)
		if err != nil {
			return env, err
		}
		env = env.With(v)
	}
	return env, nil
}

// Artifacts fetches artifacts by digest from the API, for
// connectorhost.Fetcher.
func Artifacts(api dataplanev1connect.DataPlaneServiceClient) func(context.Context, string) ([]byte, error) {
	return func(ctx context.Context, digest string) ([]byte, error) {
		res, err := api.GetArtifact(ctx, connect.NewRequest(&dataplanev1.GetArtifactRequest{Digest: digest}))
		if err != nil {
			code := errs.Transient
			if connect.CodeOf(err) == connect.CodeNotFound {
				code = errs.ConfigInvalid
			}
			return nil, errs.Wrap(code, err, "fetch artifact "+digest)
		}
		return res.Msg.GetContent(), nil
	}
}
