package attempt

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

const (
	// The name in a certificate the server makes, which workers verify.
	serverName = "openetl-attempt"
	// An assignment holds the restored record, and a share its engine's
	// schemas.
	maxMessage = 64 << 20
)

// ListenConfig is where a runner serves the attempt protocol.
type ListenConfig struct {
	// host:port.
	Address string
	// Where workers reach it, as host:port; empty: the listener's address.
	Advertise string
	// The server's TLS; nil: a certificate made now, which Certificate
	// returns for workers to trust.
	TLS *tls.Config
}

// Server is a runner's attempt server. It hands each call to the attempt
// whose token it carries.
type Server struct {
	lis       net.Listener
	grpc      *grpc.Server
	advertise string
	cert      []byte

	mu       sync.RWMutex
	attempts map[string]*coordinator
	closed   bool
}

func Listen(ctx context.Context, cfg ListenConfig) (*Server, error) {
	tlsConfig, certPEM := cfg.TLS, []byte(nil)
	if tlsConfig == nil {
		cert, p, err := selfSigned()
		if err != nil {
			return nil, fmt.Errorf("attempt: certificate: %w", err)
		}
		tlsConfig, certPEM = &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS13}, p
	}
	lis, err := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("attempt: listen: %w", err)
	}
	advertise := cfg.Advertise
	if advertise == "" {
		advertise = lis.Addr().String()
	}
	s := &Server{lis: lis, advertise: advertise, cert: certPEM, attempts: map[string]*coordinator{}}
	s.grpc = grpc.NewServer(
		grpc.Creds(credentials.NewTLS(tlsConfig)),
		grpc.MaxRecvMsgSize(maxMessage),
		grpc.MaxSendMsgSize(maxMessage),
		grpc.StreamInterceptor(s.authorize),
	)
	attemptv1.RegisterAttemptServiceServer(s.grpc, service{})
	go func() { _ = s.grpc.Serve(lis) }()
	return s, nil
}

// Address is where workers reach the server, as host:port.
func (s *Server) Address() string { return s.advertise }

// Certificate is the server's certificate, PEM-encoded, for workers to
// trust; nil with a TLS config of the listener's own.
func (s *Server) Certificate() []byte { return s.cert }

// Open serves an attempt to its workers until it's over. The time they have
// to join starts now.
func (s *Server) Open(a Attempt, opts Options) (Coordinator, error) {
	switch {
	case a.ID == "", a.Generation == 0, a.Workers <= 0:
		return nil, fmt.Errorf("attempt: an attempt needs an ID, a generation and workers: %q, %d, %d", a.ID, a.Generation, a.Workers)
	case a.Token == "":
		return nil, errors.New("attempt: an attempt needs a token")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("attempt: the attempt server is closed")
	}
	if s.attempts[a.Token] != nil {
		return nil, errors.New("attempt: another attempt has the token")
	}
	var c *coordinator
	c = newCoordinator(a, opts, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.attempts[a.Token] == c {
			delete(s.attempts, a.Token)
		}
	})
	s.attempts[a.Token] = c
	return c, nil
}

// Close stops the server: it fails every attempt it serves, ends every
// call, and opens no more.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	open := make([]*coordinator, 0, len(s.attempts))
	for _, c := range s.attempts {
		open = append(open, c)
	}
	s.mu.Unlock()
	// After s.mu: finishing unregisters, which takes it.
	for _, c := range open {
		c.finish(errs.New(errs.Transient, "attempt: the attempt server closed"))
	}
	s.grpc.Stop()
}

type coordinatorKey struct{}

func (s *Server) authorize(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	md, _ := metadata.FromIncomingContext(ss.Context())
	var header string
	if v := md.Get("authorization"); len(v) > 0 {
		header = v[0]
	}
	token, ok := strings.CutPrefix(header, "Bearer ")
	if !ok || token == "" {
		return status.Error(codes.Unauthenticated, "attempt: no token")
	}
	s.mu.RLock()
	c := s.attempts[token]
	s.mu.RUnlock()
	if c == nil {
		return status.Error(codes.Unauthenticated, "attempt: the token isn't an attempt's this runner coordinates")
	}
	return next(srv, authorized{ServerStream: ss, ctx: context.WithValue(ss.Context(), coordinatorKey{}, c)})
}

type authorized struct {
	grpc.ServerStream
	ctx context.Context
}

func (a authorized) Context() context.Context { return a.ctx }

// service hands each call to its attempt's coordinator.
type service struct {
	attemptv1.UnimplementedAttemptServiceServer
}

func (service) Attend(st grpc.BidiStreamingServer[attemptv1.AttendRequest, attemptv1.AttendResponse]) error {
	return st.Context().Value(coordinatorKey{}).(*coordinator).serve(st)
}

// selfSigned makes a certificate for serverName, and returns it with its
// PEM encoding.
func selfSigned() (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: serverName},
		DNSNames:     []string{serverName},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.AddDate(10, 0, 0),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), nil
}
