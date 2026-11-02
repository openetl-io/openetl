package sdk

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	"github.com/unsuitable001/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

// What the runner sets when it starts an external connector.
const (
	EnvSocket     = "OPENETL_CONNECTOR_SOCKET"
	EnvHostSocket = "OPENETL_HOST_SOCKET"
	// Set to 1: exit when stdin closes.
	EnvWatchStdin = "OPENETL_WATCH_STDIN"
)

// MaxMessageSize bounds one protocol message, such as a batch.
const MaxMessageSize = 256 << 20

const socketBuffer = 1 << 20

// gRPC's own defaults, 32 KiB buffers and 64 KiB windows, cost a syscall or
// a window update every few KiB of a batch.
const flowWindow = 16 << 20

// Connector is what an external connector's main serves: a source or a
// destination.
type Connector struct {
	Name string
	// A JSON Schema for the config. Top-level properties marked
	// "x-secret": true hold secret names. On the proxy and raw egress tiers,
	// the connector gets each replaced by the secret's value.
	ConfigSchema []byte
	Egress       Egress

	// Source or Destination binds a config. It must not do I/O.
	Source func(config []byte, h host.Host) (source.Source, error)
	// Committed is called on the source, which implements
	// source.Acknowledger.
	Acknowledges bool

	Destination  func(config []byte, h host.Host) (destination.Destination, error)
	Capabilities destination.Capabilities
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

// Serve serves c as an external connector: on the socket EnvSocket names,
// with host services on EnvHostSocket's. It prints "ready" on stdout once it
// listens, and returns on SIGTERM or SIGINT, or when stdin closes if
// EnvWatchStdin is 1. If serving fails, it exits with status 1.
func Serve(c Connector) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	if os.Getenv(EnvWatchStdin) == "1" {
		go func() {
			_, _ = io.Copy(io.Discard, os.Stdin)
			stop()
		}()
	}
	err := c.Run(ctx, os.Getenv(EnvSocket), os.Getenv(EnvHostSocket), func() { fmt.Println("ready") })
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", c.Name, err)
		stop()
		os.Exit(1) //nolint:gocritic // stop has run
	}
}

// Run serves c on socket until ctx is done, with host services on
// hostSocket. ready, if set, is called once it listens.
func (c Connector) Run(ctx context.Context, socket, hostSocket string, ready func()) error {
	if (c.Source == nil) == (c.Destination == nil) {
		return errors.New("a connector is a source or a destination")
	}
	if socket == "" || hostSocket == "" {
		return fmt.Errorf("run me from openetl: %s and %s must name sockets", EnvSocket, EnvHostSocket)
	}
	b, err := newBase(c)
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient("unix://"+hostSocket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize), grpc.MaxCallSendMsgSize(MaxMessageSize)))
	if err != nil {
		return fmt.Errorf("host services: %w", err)
	}
	defer func() { _ = conn.Close() }()
	b.host = host.NewClient(conn)

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", socket)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(MaxMessageSize), grpc.MaxSendMsgSize(MaxMessageSize),
		grpc.ReadBufferSize(socketBuffer), grpc.WriteBufferSize(socketBuffer),
		grpc.InitialWindowSize(flowWindow), grpc.InitialConnWindowSize(flowWindow))
	protocolv1.RegisterConnectorServiceServer(srv, b)
	if c.Source != nil {
		protocolv1.RegisterSourceServiceServer(srv, &sourceServer{base: b})
	} else {
		protocolv1.RegisterDestinationServiceServer(srv, &destinationServer{base: b})
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(sizedListener{ln.(*net.UnixListener)}) }()
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

// sizedListener gives each connection 1 MiB socket buffers: the defaults
// (8 KiB on darwin) cost a context switch every few KiB of a batch.
type sizedListener struct{ *net.UnixListener }

func (l sizedListener) Accept() (net.Conn, error) {
	c, err := l.AcceptUnix()
	if err != nil {
		return nil, err
	}
	_ = c.SetReadBuffer(socketBuffer)
	_ = c.SetWriteBuffer(socketBuffer)
	return c, nil
}

// base serves ConnectorService, and binds configs for the other services.
type base struct {
	protocolv1.UnimplementedConnectorServiceServer
	c    Connector
	host *host.Client
	// Top-level config properties that hold secret names.
	secrets []string
}

func newBase(c Connector) (*base, error) {
	b := &base{c: c}
	if len(c.ConfigSchema) == 0 {
		return b, nil
	}
	var schema struct {
		Properties map[string]struct {
			Secret bool `json:"x-secret"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(c.ConfigSchema, &schema); err != nil {
		return nil, fmt.Errorf("config schema: %w", err)
	}
	for name, p := range schema.Properties {
		if p.Secret {
			b.secrets = append(b.secrets, name)
		}
	}
	return b, nil
}

func (b *base) Spec(context.Context, *protocolv1.SpecRequest) (*protocolv1.SpecResponse, error) {
	resp := &protocolv1.SpecResponse{
		ProtocolVersion: "v1",
		ConfigSchema:    b.c.ConfigSchema,
		Egress:          &protocolv1.Egress{Tier: protocolv1.EgressTier(max(b.c.Egress.Tier, EgressHost)), Allow: b.c.Egress.Allow}, //nolint:gosec // small
	}
	if b.c.Source != nil {
		resp.Kind = protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE
		resp.Source = &protocolv1.SourceCapabilities{Acknowledges: b.c.Acknowledges}
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
	var m map[string]json.RawMessage
	if err := json.Unmarshal(config, &m); err != nil {
		return nil, configError(fmt.Errorf("config: %w", err))
	}
	for _, k := range b.secrets {
		v, ok := m[k]
		if !ok {
			continue
		}
		var name string
		if err := json.Unmarshal(v, &name); err != nil {
			return nil, configError(fmt.Errorf("config.%s must name a secret", k))
		}
		val, err := b.host.GetSecret(ctx, name)
		if err != nil {
			return nil, fmt.Errorf("config.%s: %w", k, err)
		}
		if m[k], err = json.Marshal(val); err != nil {
			return nil, err
		}
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
