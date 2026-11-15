package engineclient

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/attempt"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A job that failed only because it lost another engine says so, and keeps
// its code; any other failure doesn't.
func TestALostPeerIsMarked(t *testing.T) {
	detail := &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, Message: "engine 1 at w1:7720 was lost"}
	lost, err := status.New(codes.Internal, "lost").WithDetails(detail, &enginev1.PeerLost{Engine: 1})
	if err != nil {
		t.Fatal(err)
	}
	got := fromStatus(lost.Err())
	var peer *attempt.PeerLost
	if !errors.As(got, &peer) || errs.CodeOf(got) != errs.Transient {
		t.Errorf("%v: not a lost peer's transient failure", got)
	}
	own, err := status.New(codes.Internal, "own").WithDetails(detail)
	if err != nil {
		t.Fatal(err)
	}
	if got := fromStatus(own.Err()); errors.As(got, &peer) || errs.CodeOf(got) != errs.Transient {
		t.Errorf("%v: a failure of its own marked as a lost peer's", got)
	}
}

// A worker's mesh reaches the engine as it is.
func TestTheMeshReachesTheEngine(t *testing.T) {
	m := &runner.Mesh{Engine: 1, Listen: ":7720", Certificate: []byte("c"), Key: []byte("k"), Secret: []byte("s"),
		Engines: []runner.MeshEngine{{Address: "a:7720", Lanes: 1, WriterLanes: 1}, {Address: "b:7720", Lanes: 2, WriterLanes: 1}}}
	req, _, _, err := startRequest(runner.Plan{Streams: []runner.Stream{{Name: "s"}}, Lanes: 2, FirstLane: 1, FirstWriterLane: 1, AttemptWriterLanes: 2, Mesh: m})
	if err != nil {
		t.Fatal(err)
	}
	want := &enginev1.Mesh{Engine: 1, Listen: ":7720", Certificate: []byte("c"), Key: []byte("k"), Secret: []byte("s"),
		Engines: []*enginev1.MeshEngine{{Address: "a:7720", Lanes: 1, WriterLanes: 1}, {Address: "b:7720", Lanes: 2, WriterLanes: 1}}}
	if !proto.Equal(req.GetMesh(), want) {
		t.Errorf("mesh %v, want %v", req.GetMesh(), want)
	}
}
