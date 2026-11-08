package bridge

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	bridgev1 "github.com/openetl-io/openetl/gen/openetl/bridge/v1"
)

type describer struct {
	bridgev1.UnimplementedBridgeServiceServer
	pipeline string
}

func (d describer) Describe(context.Context, *bridgev1.DescribeRequest) (*bridgev1.DescribeResponse, error) {
	return &bridgev1.DescribeResponse{Pipeline: d.pipeline}, nil
}

func TestCallsReachTheirTokensHandler(t *testing.T) {
	s, err := Listen(context.Background(), "127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	unregister := s.Register("a", describer{pipeline: "p-a"})
	s.Register("b", describer{pipeline: "p-b"})
	ctx := context.Background()
	for token, want := range map[string]string{"a": "p-a", "b": "p-b"} {
		c, closeConn, err := dial(s.Address(), s.Fingerprint(), token)
		if err != nil {
			t.Fatal(err)
		}
		res, err := c.Describe(ctx, &bridgev1.DescribeRequest{})
		if err != nil || res.GetPipeline() != want {
			t.Fatalf("token %s: %v, %v", token, res, err)
		}
		_ = closeConn()
	}
	unregister()
	c, closeConn, _ := dial(s.Address(), s.Fingerprint(), "a")
	defer func() { _ = closeConn() }()
	if _, err := c.Describe(ctx, &bridgev1.DescribeRequest{}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("after unregister: %v", err)
	}
}

func TestAnotherCertificateIsRefused(t *testing.T) {
	s, err := Listen(context.Background(), "127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Register("a", describer{})
	other, err := Listen(context.Background(), "127.0.0.1:0", "")
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	c, closeConn, _ := dial(s.Address(), other.Fingerprint(), "a")
	defer func() { _ = closeConn() }()
	if _, err := c.Describe(context.Background(), &bridgev1.DescribeRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("pinned to another certificate: %v", err)
	}
}

// clientTLS is the TLS config of a client pinning fingerprint, as a job's
// library does.
func clientTLS(fingerprint string) *tls.Config {
	return &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
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

// dial connects to a bridge as a job's library does.
func dial(addr, fingerprint, token string) (bridgev1.BridgeServiceClient, func() error, error) {
	auth := func(ctx context.Context) context.Context {
		return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token, protocolHeader, Protocol)
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(credentials.NewTLS(clientTLS(fingerprint))),
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
