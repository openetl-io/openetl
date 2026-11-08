package activities

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/flink"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/secretstore"
	"github.com/openetl-io/openetl/internal/streamschema"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// Ops runs connector operations for ConnectorOpWorkflow.
type Ops struct {
	API        dataplanev1connect.DataPlaneServiceClient
	Connectors connectorhost.Env
	// Nil: connector versions can't be fetched.
	Versions *connectorhost.Fetcher
	// Nil: secrets in cloud secret stores can't be read.
	Secrets *secretstore.Reader
	// The data plane's object store, as Sync's; empty for none, and then a
	// deleted pipeline's Flink jobs aren't cancelled.
	Storage string
	// How TeardownFlink reaches Flink clusters.
	Flink flink.Options
}

// CheckConnector checks a source or destination. A failed check is a result,
// not an error.
func (a *Ops) CheckConnector(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	cfg, err := a.config(ctx, in)
	if err != nil {
		return contract.ConnectorOpResult{}, err
	}
	res := contract.ConnectorOpResult{Version: cfg.GetVersion()}
	sd, dd := opDigests(in, cfg.GetConnector())
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, sd, dd)
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

// opDigests are an op's connector version digest as the source's or the
// destination's, for connectorEnv.
func opDigests(in contract.ConnectorOpInput, digest string) (source, destination string) {
	if in.SourceID != "" {
		return digest, ""
	}
	return "", digest
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
	sd, dd := opDigests(in, cfg.GetConnector())
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, sd, dd)
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

// TeardownSource removes what a source keeps upstream for a deleted
// pipeline.
func (a *Ops) TeardownSource(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	return a.teardown(ctx, in)
}

// TeardownDestination removes what a destination keeps for a deleted
// pipeline besides its data.
func (a *Ops) TeardownDestination(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	return a.teardown(ctx, in)
}

func (a *Ops) teardown(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
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
	sd, dd := opDigests(in, cfg.GetConnector())
	env, err := connectorEnv(ctx, a.Connectors, a.Versions, sd, dd)
	if err != nil {
		return res, err
	}
	name, _ := connectorhost.SplitRef(cfg.GetConfig().GetConnector())
	var c any
	if in.SourceID != "" {
		c, err = connectorhost.NewSource(name, raw, env)
	} else {
		c, err = connectorhost.NewDestination(name, raw, env)
	}
	if err != nil {
		return res, err
	}
	defer connectorhost.Close(c)
	if t, ok := c.(interface {
		Teardown(context.Context, string) error
	}); ok {
		return res, t.Teardown(ctx, in.PipelineID)
	}
	return res, nil
}

// TeardownFlink cancels a deleted pipeline's Flink jobs on the cluster its
// last version pinned, and waits until they're gone.
func (a *Ops) TeardownFlink(ctx context.Context, in contract.ConnectorOpInput) (contract.ConnectorOpResult, error) {
	return contract.ConnectorOpResult{}, a.teardownFlink(ctx, in)
}

func (a *Ops) teardownFlink(ctx context.Context, in contract.ConnectorOpInput) error {
	if a.Storage == "" {
		return errs.New(errs.ConfigInvalid, "cancelling a deleted pipeline's Flink jobs needs the data plane's object store, and this runner has none")
	}
	resp, err := a.API.GetFlinkTeardown(ctx, connect.NewRequest(&dataplanev1.GetFlinkTeardownRequest{OperationId: in.OpID}))
	if err != nil {
		code := errs.Transient
		if c := connect.CodeOf(err); c == connect.CodeNotFound || c == connect.CodeInvalidArgument {
			code = errs.ConfigInvalid
		}
		return errs.Wrap(code, err, "fetch the Flink cluster")
	}
	if resp.Msg.GetOrgId() != in.OrgID || resp.Msg.GetPipelineId() != in.PipelineID {
		return errs.New(errs.ConfigInvalid, "the Flink teardown is another pipeline's")
	}
	pc := resp.Msg.GetCluster()
	read := func(ctx context.Context, cfg *apiv1.ConnectorConfig, ext []*dataplanev1.ExternalSecret) error {
		return readSecrets(ctx, a.Secrets, cfg, ext)
	}
	if err := flink.ResolveSecrets(ctx, pc, read); err != nil {
		return err
	}
	s := flink.Settings{Org: in.OrgID, Pipeline: in.PipelineID, Store: a.Storage, Cluster: pc.GetSpec(), Transform: &apiv1.FlinkTransform{}}
	c, err := flink.New(ctx, s, a.Flink)
	if err != nil {
		return err
	}
	defer func() { _ = c.Close() }()
	return c.CancelAll(ctx)
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
			s.SyncModes = append(s.SyncModes, ints.Enum[apiv1.SyncMode](m))
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

// connectorEnv is env with the source's and the destination's connector
// versions added, by digest; an empty digest adds nothing.
func connectorEnv(ctx context.Context, env connectorhost.Env, versions *connectorhost.Fetcher, source, destination string) (connectorhost.Env, error) {
	for _, r := range []struct{ role, digest string }{{"source", source}, {"destination", destination}} {
		if r.digest == "" {
			continue
		}
		if versions == nil {
			return env, errs.New(errs.ConfigInvalid, "this runner can't fetch connector versions")
		}
		v, err := versions.Fetch(ctx, r.digest, r.role)
		if err != nil {
			return env, err
		}
		env = env.With(v, r.role)
	}
	return env, nil
}

// FetchCredentials asks the API for the credential of the connected source
// of connectors that covers a binary's URL.
func FetchCredentials(api dataplanev1connect.DataPlaneServiceClient) func(context.Context, string) (string, string, error) {
	return func(ctx context.Context, url string) (string, string, error) {
		res, err := api.GetFetchCredential(ctx, connect.NewRequest(&dataplanev1.GetFetchCredentialRequest{Url: url}))
		if err != nil {
			code := errs.Transient
			if connect.CodeOf(err) == connect.CodeFailedPrecondition {
				code = errs.ConfigInvalid
			}
			return "", "", errs.Wrap(code, err, "the credential for "+url)
		}
		return res.Msg.GetUsername(), res.Msg.GetPassword(), nil
	}
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
