package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/openetl-io/openetl/sdk/go/configschema"
	"github.com/openetl-io/openetl/sdk/go/destination"
	"github.com/openetl-io/openetl/sdk/go/host"
	"github.com/openetl-io/openetl/sdk/go/internal/ints"
	"github.com/openetl-io/openetl/sdk/go/internal/launch"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// The launch contract: what the runner sets when it starts an external
// connector.
const (
	// Where the connector serves: unix:///path or tcp://host:port.
	EnvConnectorAddr = "OPENETL_CONNECTOR_ADDR"
	// Where host services are.
	EnvHostAddr = "OPENETL_HOST_ADDR"
	// Set with TCP addresses: the connector refuses calls without it, and
	// sends it to host services.
	EnvToken = "OPENETL_TOKEN"
	// source or destination: the role to serve, for an executable with both.
	EnvRole = "OPENETL_CONNECTOR_ROLE"
	// Set to 1: exit when stdin closes.
	EnvWatchStdin = "OPENETL_WATCH_STDIN"
)

// MaxMessageSize bounds one protocol message, such as a batch.
const MaxMessageSize = 256 << 20

const socketBuffer = 1 << 20

// gRPC's own defaults, 32 KiB buffers and 64 KiB windows, cost a syscall or
// a window update every few KiB of a batch.
const flowWindow = 16 << 20

// Connector is what an external connector's main serves: a source, a
// destination, or both, one per process, as EnvRole says.
type Connector struct {
	Name string
	// A JSON Schema for the config. Properties marked "x-secret": true, at
	// any depth, hold secret names. On the proxy and raw egress tiers, the
	// connector gets each replaced by the secret's value.
	ConfigSchema []byte
	Egress       Egress

	// Source or Destination binds a config. It must not do I/O.
	Source func(config []byte, h host.Host) (source.Source, error)
	// Committed is called on the source, which implements
	// source.Acknowledger.
	Acknowledges bool
	// The source implements source.Splitter, and divides streams into
	// splits.
	Splits bool
	// The source implements source.Resplitter.
	Resplit bool

	Destination  func(config []byte, h host.Host) (destination.Destination, error)
	Capabilities destination.Capabilities

	// Additions to the protocol after its baseline that the connector can't
	// run without. A platform without one refuses it. host.Supports tells
	// the connector about the rest.
	RequiredFeatures []string
}

type Egress struct {
	Tier EgressTier
	// Hosts for EgressProxy, host:port pairs for EgressRaw.
	Allow []string
}

type EgressTier int

const (
	// EgressHost: only through host services.
	EgressHost EgressTier = iota + 1
	// EgressProxy: HTTPS through a forward proxy, for vendor SDKs.
	EgressProxy
	// EgressRaw: sockets, for database drivers.
	EgressRaw
)

// Launch is what the runner tells a connector it starts: the launch
// contract's variables.
type Launch struct {
	// unix:///path or tcp://host:port.
	Addr, HostAddr string
	// Required with a TCP address.
	Token string
	// source or destination; empty for a connector with one role.
	Role string
}

// LaunchFromEnv reads the launch contract's variables.
func LaunchFromEnv() Launch {
	return Launch{Addr: os.Getenv(EnvConnectorAddr), HostAddr: os.Getenv(EnvHostAddr), Token: os.Getenv(EnvToken), Role: os.Getenv(EnvRole)}
}

// Serve serves c as an external connector, where the launch contract's
// variables say. It prints "ready" on stdout once it listens, and returns on
// SIGTERM or SIGINT, or when stdin closes if EnvWatchStdin is 1. If serving
// fails, it exits with status 1.
func Serve(c Connector) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if os.Getenv(EnvWatchStdin) == "1" {
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin)
			stop()
		}()
	}
	err := c.Run(ctx, LaunchFromEnv(), func() { fmt.Println("ready") })
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", c.Name, err)
		stop()
		os.Exit(1)
	}
}

// Run serves c at l.Addr until ctx is done, with host services at
// l.HostAddr. ready, if set, is called once it listens.
func (c Connector) Run(ctx context.Context, l Launch, ready func()) error {
	c, err := c.role(l.Role)
	if err != nil {
		return err
	}
	if l.Addr == "" || l.HostAddr == "" {
		return fmt.Errorf("run me from openetl: %s and %s must name addresses", EnvConnectorAddr, EnvHostAddr)
	}
	addr, err := launch.Parse(l.Addr)
	if err != nil {
		return fmt.Errorf("%s: %w", EnvConnectorAddr, err)
	}
	hostAddr, err := launch.Parse(l.HostAddr)
	if err != nil {
		return fmt.Errorf("%s: %w", EnvHostAddr, err)
	}
	if (addr.Network == "tcp" || hostAddr.Network == "tcp") && l.Token == "" {
		return fmt.Errorf("a TCP address needs %s", EnvToken)
	}
	b, err := newBase(c)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(hostAddr.Target(), append(launch.DialOptions(l.Token),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))...)
	if err != nil {
		return fmt.Errorf("host services: %w", err)
	}
	defer func() { _ = conn.Close() }()
	b.host = host.NewClient(conn)

	ln, err := launch.Listen(ctx, addr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(append(launch.ServerOptions(l.Token),
		grpc.MaxRecvMsgSize(MaxMessageSize), grpc.MaxSendMsgSize(MaxMessageSize),
		grpc.ReadBufferSize(socketBuffer), grpc.WriteBufferSize(socketBuffer),
		grpc.InitialWindowSize(flowWindow), grpc.InitialConnWindowSize(flowWindow))...)
	protocolv1.RegisterConnectorServiceServer(srv, b)
	if c.Source != nil {
		protocolv1.RegisterSourceServiceServer(srv, &sourceServer{base: b})
	} else {
		protocolv1.RegisterDestinationServiceServer(srv, &destinationServer{base: b})
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	if ready != nil {
		ready()
	}
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	done := make(chan struct{})
	go func() {
		srv.GracefulStop()
		close(done)
	}()
	t := time.NewTimer(5 * time.Second)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		srv.Stop()
	}
	return nil
}

// role is c as the role it serves: its only one, whatever role names, so
// the runner learns of a mismatch from Spec, or the one named.
func (c Connector) role(role string) (Connector, error) {
	switch {
	case c.Source == nil && c.Destination == nil:
		return c, errors.New("a connector is a source, a destination or both")
	case c.Source == nil || c.Destination == nil:
	case role == "source":
		c.Destination = nil
	case role == "destination":
		c.Source = nil
	default:
		return c, fmt.Errorf("%s has both roles, so %s must be source or destination, not %q", c.Name, EnvRole, role)
	}
	return c, nil
}

// base serves ConnectorService, and binds configs for the other services.
type base struct {
	protocolv1.UnimplementedConnectorServiceServer
	c    Connector
	host *host.Client
	// Config properties that hold secret names, as dotted paths.
	secrets []string
}

func newBase(c Connector) (*base, error) {
	b := &base{c: c}
	var err error
	b.secrets, err = configschema.SecretPaths(c.ConfigSchema)
	return b, err
}

func (b *base) Spec(_ context.Context, req *protocolv1.SpecRequest) (*protocolv1.SpecResponse, error) {
	if b.host != nil {
		b.host.SetPlatform(req.GetHost())
	}
	resp := &protocolv1.SpecResponse{
		ProtocolVersion:  "v1",
		ConfigSchema:     b.c.ConfigSchema,
		Egress:           &protocolv1.Egress{Tier: ints.Enum[protocolv1.EgressTier](max(b.c.Egress.Tier, EgressHost)), Allow: b.c.Egress.Allow},
		RequiredFeatures: b.c.RequiredFeatures,
	}
	if b.c.Source != nil {
		resp.Kind = protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE
		resp.Source = &protocolv1.SourceCapabilities{Acknowledges: b.c.Acknowledges, Splits: b.c.Splits, Resplit: b.c.Resplit}
	} else {
		resp.Kind = protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION
		resp.Destination = capabilitiesToProto(b.c.Capabilities)
	}
	return resp, nil
}

func (b *base) Check(ctx context.Context, req *protocolv1.CheckRequest) (*protocolv1.CheckResponse, error) {
	var err error
	if b.c.Source != nil {
		var s source.Source
		if s, err = b.source(ctx, req.GetConfig()); err == nil {
			if c, ok := s.(source.Checker); ok {
				err = c.Check(ctx)
			}
		}
	} else {
		var d destination.Destination
		if d, err = b.destination(ctx, req.GetConfig()); err == nil {
			if c, ok := d.(destination.Checker); ok {
				err = c.Check(ctx)
			}
		}
	}
	return &protocolv1.CheckResponse{Error: protoError(err)}, nil
}

func (b *base) Setup(context.Context, *protocolv1.SetupRequest) (*protocolv1.SetupResponse, error) {
	return &protocolv1.SetupResponse{}, nil
}

func (b *base) Teardown(ctx context.Context, req *protocolv1.TeardownRequest) (*protocolv1.TeardownResponse, error) {
	if b.c.Source == nil {
		return &protocolv1.TeardownResponse{}, nil
	}
	s, err := b.source(ctx, req.GetConfig())
	if err != nil {
		return nil, status(err)
	}
	if t, ok := s.(source.Teardowner); ok {
		if err := t.Teardown(ctx, req.GetPipeline()); err != nil {
			return nil, status(err)
		}
	}
	return &protocolv1.TeardownResponse{}, nil
}

func (b *base) source(ctx context.Context, config []byte) (source.Source, error) {
	config, err := b.config(ctx, config)
	if err != nil {
		return nil, err
	}
	s, err := b.c.Source(config, b.host)
	return s, configError(err)
}

func (b *base) destination(ctx context.Context, config []byte) (destination.Destination, error) {
	config, err := b.config(ctx, config)
	if err != nil {
		return nil, err
	}
	d, err := b.c.Destination(config, b.host)
	return d, configError(err)
}

// config puts the secrets' values in place of their names, for the tiers
// that build their own auth.
func (b *base) config(ctx context.Context, config []byte) ([]byte, error) {
	if len(b.secrets) == 0 || b.c.Egress.Tier < EgressProxy || len(bytes.TrimSpace(config)) == 0 {
		return config, nil
	}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, configError(fmt.Errorf("config: %w", err))
	}
	for _, k := range b.secrets {
		v, ok := configschema.Lookup(m, k)
		if !ok {
			continue
		}
		name, ok := v.(string)
		if !ok {
			return nil, configError(fmt.Errorf("config.%s must name a secret", k))
		}
		val, err := b.host.GetSecret(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("config.%s: %w", k, err)
		}
		configschema.Replace(m, k, val)
	}
	return json.Marshal(m)
}

func configError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := errors.AsType[interface {
		error
		ErrorCode() protocolv1.ErrorCode
	}](err); ok {
		return err
	}
	return WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, err)
}

// protoError classifies err, ErrFenced as FENCED.
func protoError(err error) *protocolv1.Error {
	if err == nil {
		return nil
	}
	p := rpcerr.Proto(err)
	if errors.Is(err, destination.ErrFenced) {
		p.Code = protocolv1.ErrorCode_ERROR_CODE_FENCED
	}
	return p
}

func status(err error) error {
	if errors.Is(err, destination.ErrFenced) {
		err = WithCode(protocolv1.ErrorCode_ERROR_CODE_FENCED, err)
	}
	return rpcerr.Status(err)
}
