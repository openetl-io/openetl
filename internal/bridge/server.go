// Package bridge serves openetl.bridge.v1 to the Flink jobs a runner deploys
// (ADR-0022 §3): over TLS with a certificate the runner makes when it starts,
// which jobs pin by fingerprint, and to each attempt's job by its own token.
package bridge

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/openetl-io/openetl/gen/openetl/bridge/v1"
)

// Protocol is the bridge protocol this runner speaks. A library sends its
// own in the openetl-bridge-protocol header; another major version is
// refused.
const Protocol = "v1"

const (
	protocolHeader = "openetl-bridge-protocol"
	maxMessage     = 256 << 20
)

// Server is a runner's bridge listener.
type Server struct {
	lis         net.Listener
	grpc        *grpc.Server
	advertise   string
	fingerprint string

	mu       sync.RWMutex
	handlers map[string]bridgev1.BridgeServiceServer
}

// Listen listens on addr. Jobs reach it at advertise (host:port); empty: the
// listener's address.
func Listen(ctx context.Context, addr, advertise string) (*Server, error) {
	cert, fingerprint, err := selfSigned()
	if err != nil {
		return nil, fmt.Errorf("bridge: certificate: %w", err)
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("bridge: listen: %w", err)
	}
	if advertise == "" {
		advertise = lis.Addr().String()
	}
	s := &Server{lis: lis, advertise: advertise, fingerprint: fingerprint, handlers: map[string]bridgev1.BridgeServiceServer{}}
	s.grpc = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})),
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
		// A TaskManager that dies without closing its connections is noticed
		// within about 15 s.
		grpc.KeepaliveParams(keepalive.ServerParameters{Time: 10 * time.Second, Timeout: 5 * time.Second}),
		grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 5 * time.Second, PermitWithoutStream: true}),
		grpc.UnaryInterceptor(s.unary),
		grpc.StreamInterceptor(s.stream),
	)
	bridgev1.RegisterBridgeServiceServer(s.grpc, service{})
	go func() { _ = s.grpc.Serve(lis) }()
	return s, nil
}

// Address is where jobs reach the bridge, as host:port.
func (s *Server) Address() string { return s.advertise }

// Fingerprint is the SHA-256 of the bridge's certificate, in lowercase hex.
func (s *Server) Fingerprint() string { return s.fingerprint }

// Register serves h to calls carrying token, until unregister is called.
// Calls already running keep running.
func (s *Server) Register(token string, h bridgev1.BridgeServiceServer) (unregister func()) {
	s.mu.Lock()
	s.handlers[token] = h
	s.mu.Unlock()
	return func() {
		s.mu.Lock()
		delete(s.handlers, token)
		s.mu.Unlock()
	}
}

// Close stops the server, ending every call.
func (s *Server) Close() { s.grpc.Stop() }

type handlerKey struct{}

func (s *Server) authorize(ctx context.Context) (context.Context, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	if p := first(md, protocolHeader); strings.SplitN(p, ".", 2)[0] != Protocol {
		return nil, status.Errorf(codes.FailedPrecondition, "bridge: this runner speaks bridge protocol %s, and the library speaks %q", Protocol, p)
	}
	token, ok := strings.CutPrefix(first(md, "authorization"), "Bearer ")
	if !ok || token == "" {
		return nil, status.Error(codes.Unauthenticated, "bridge: no token")
	}
	s.mu.RLock()
	h := s.handlers[token]
	s.mu.RUnlock()
	if h == nil {
		return nil, status.Error(codes.Unauthenticated, "bridge: the token isn't an attempt's that this runner is running")
	}
	return context.WithValue(ctx, handlerKey{}, h), nil
}

func first(md metadata.MD, key string) string {
	if v := md.Get(key); len(v) > 0 {
		return v[0]
	}
	return ""
}

func (s *Server) unary(ctx context.Context, req any, _ *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	ctx, err := s.authorize(ctx)
	if err != nil {
		return nil, err
	}
	return next(ctx, req)
}

func (s *Server) stream(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	ctx, err := s.authorize(ss.Context())
	if err != nil {
		return err
	}
	return next(srv, authorized{ServerStream: ss, ctx: ctx})
}

type authorized struct {
	grpc.ServerStream
	ctx context.Context
}

func (a authorized) Context() context.Context { return a.ctx }

func handler(ctx context.Context) bridgev1.BridgeServiceServer {
	return ctx.Value(handlerKey{}).(bridgev1.BridgeServiceServer)
}

// service hands each call to its token's handler.
type service struct {
	bridgev1.UnimplementedBridgeServiceServer
}

func (service) Describe(ctx context.Context, req *bridgev1.DescribeRequest) (*bridgev1.DescribeResponse, error) {
	return handler(ctx).Describe(ctx, req)
}

func (service) Coordinate(st grpc.BidiStreamingServer[bridgev1.CoordinateRequest, bridgev1.CoordinateResponse]) error {
	return handler(st.Context()).Coordinate(st)
}

func (service) Read(req *bridgev1.ReadRequest, st grpc.ServerStreamingServer[bridgev1.ReadResponse]) error {
	return handler(st.Context()).Read(req, st)
}

func (service) Write(st grpc.BidiStreamingServer[bridgev1.WriteRequest, bridgev1.WriteResponse]) error {
	return handler(st.Context()).Write(st)
}

func selfSigned() (tls.Certificate, string, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, "", err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "openetl-runner-bridge"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, "", err
	}
	sum := sha256.Sum256(der)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, hex.EncodeToString(sum[:]), nil
}

// ClientTLS is the TLS config a client pinning fingerprint uses, for tests
// and the simulation.
func ClientTLS(fingerprint string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true, //nolint:gosec // the certificate is pinned below
		VerifyConnection: func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("bridge: no certificate")
			}
			sum := sha256.Sum256(cs.PeerCertificates[0].Raw)
			if hex.EncodeToString(sum[:]) != fingerprint {
				return errors.New("bridge: the certificate's fingerprint isn't the pinned one")
			}
			return nil
		},
	}
}

// Dial connects to a bridge as a job's library does, for tests and the
// simulation.
func Dial(addr, fingerprint, token string) (bridgev1.BridgeServiceClient, func() error, error) {
	auth := func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token, protocolHeader, Protocol)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(ClientTLS(fingerprint))),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessage), grpc.MaxCallSendMsgSize(maxMessage)),
		grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			return invoker(auth(ctx), method, req, reply, cc, opts...)
		}),
		grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return streamer(auth(ctx), desc, cc, method, opts...)
		}),
	)
	if err != nil {
		return nil, nil, err
	}
	return bridgev1.NewBridgeServiceClient(conn), conn.Close, nil
}
