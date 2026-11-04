// Package launch holds what both ends of the launch contract share: its
// addresses, and the token calls over TCP carry.
package launch

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const socketBuffer = 1 << 20

// Addr is an address of the launch contract: unix:///path or
// tcp://host:port.
type Addr struct {
	// unix or tcp.
	Network string
	// A path, or host:port.
	Address string
}

// Parse reads an address of the launch contract.
func Parse(s string) (Addr, error) {
	if p, ok := strings.CutPrefix(s, "unix://"); ok {
		if !filepath.IsAbs(p) {
			return Addr{}, fmt.Errorf("%q: unix:// takes an absolute path, as in unix:///tmp/c.sock", s)
		}
		return Addr{Network: "unix", Address: p}, nil
	}
	if hp, ok := strings.CutPrefix(s, "tcp://"); ok {
		if _, port, err := net.SplitHostPort(hp); err != nil || port == "" {
			return Addr{}, fmt.Errorf("%q: tcp:// takes host:port", s)
		}
		return Addr{Network: "tcp", Address: hp}, nil
	}
	return Addr{}, fmt.Errorf("%q isn't unix:///path or tcp://host:port", s)
}

func (a Addr) String() string { return a.Network + "://" + a.Address }

// Target is the address as a gRPC client's target.
func (a Addr) Target() string {
	if a.Network == "unix" {
		return "unix://" + a.Address
	}
	return "passthrough:///" + a.Address
}

// Listen listens at a, and gives each connection 1 MiB socket buffers: the
// defaults (8 KiB for a Unix socket on darwin) cost a context switch every
// few KiB of a batch.
func Listen(ctx context.Context, a Addr) (net.Listener, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, a.Network, a.Address)
	if err != nil {
		return nil, err
	}
	return sized{ln}, nil
}

type sized struct{ net.Listener }

func (l sized) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if b, ok := c.(interface {
		SetReadBuffer(int) error
		SetWriteBuffer(int) error
	}); ok {
		_ = b.SetReadBuffer(socketBuffer)
		_ = b.SetWriteBuffer(socketBuffer)
	}
	return c, nil
}

// NewToken is a random token for one launch.
func NewToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// Credentials sends a token with each call, in the authorization metadata.
type Credentials string

func (c Credentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(c)}, nil
}

func (Credentials) RequireTransportSecurity() bool { return false }

// DialOptions send token with each call; none without one.
func DialOptions(token string) []grpc.DialOption {
	if token == "" {
		return nil
	}
	return []grpc.DialOption{grpc.WithPerRPCCredentials(Credentials(token))}
}

// ServerOptions refuse calls that don't carry token; none without one.
func ServerOptions(token string) []grpc.ServerOption {
	if token == "" {
		return nil
	}
	want := []byte("Bearer " + token)
	check := func(ctx context.Context) error {
		md, _ := metadata.FromIncomingContext(ctx)
		for _, v := range md.Get("authorization") {
			if subtle.ConstantTimeCompare([]byte(v), want) == 1 {
				return nil
			}
		}
		return status.Error(codes.Unauthenticated, "the call doesn't carry the launch's token")
	}
	return []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if err := check(ctx); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}),
		grpc.ChainStreamInterceptor(func(srv any, ss grpc.ServerStream, _ *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if err := check(ss.Context()); err != nil {
				return err
			}
			return h(srv, ss)
		}),
	}
}
