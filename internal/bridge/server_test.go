package bridge

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
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
		c, closeConn, err := Dial(s.Address(), s.Fingerprint(), token)
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
	c, closeConn, _ := Dial(s.Address(), s.Fingerprint(), "a")
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
	c, closeConn, _ := Dial(s.Address(), other.Fingerprint(), "a")
	defer func() { _ = closeConn() }()
	if _, err := c.Describe(context.Background(), &bridgev1.DescribeRequest{}); status.Code(err) != codes.Unavailable {
		t.Fatalf("pinned to another certificate: %v", err)
	}
}
