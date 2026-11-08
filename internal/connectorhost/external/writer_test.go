package external

import (
	"errors"
	"io"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// scriptedStream is a Write stream whose destination sends what reply gives
// for each request.
type scriptedStream struct {
	grpc.ClientStream
	replies chan *protocolv1.WriteResponse
	reply   func(*protocolv1.WriteRequest) *protocolv1.WriteResponse
}

func (s *scriptedStream) Send(m *protocolv1.WriteRequest) error {
	s.replies <- s.reply(m)
	return nil
}

func (s *scriptedStream) Recv() (*protocolv1.WriteResponse, error) {
	m, ok := <-s.replies
	if !ok {
		return nil, io.EOF
	}
	return m, nil
}

func newScripted(t *testing.T, reply func(*protocolv1.WriteRequest) *protocolv1.WriteResponse) (*writer, *scriptedStream) {
	t.Helper()
	s := &scriptedStream{replies: make(chan *protocolv1.WriteResponse, 16), reply: reply}
	w := newWriter(nil, s, func() {})
	go w.receive()
	t.Cleanup(func() { close(s.replies) })
	return w, s
}

func stagedReply(e *protocolv1.Epoch) *protocolv1.WriteResponse {
	return &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Staged{Staged: &protocolv1.WriteStaged{Epoch: e, Staged: []byte("c")}}}
}

func committedReply(e *protocolv1.Epoch, fail *protocolv1.Error) *protocolv1.WriteResponse {
	return &protocolv1.WriteResponse{Msg: &protocolv1.WriteResponse_Committed{Committed: &protocolv1.WriteCommitted{Epoch: e, Error: fail}}}
}

// A reply says which epoch it answers, so one left over from another epoch
// can't be taken for this one's.
func TestRepliesAnswerTheirOwnEpoch(t *testing.T) {
	e := destination.Epoch{Generation: 3, N: 7}
	for name, other := range map[string]*protocolv1.Epoch{
		"another epoch":      {Generation: 3, N: 6},
		"another generation": {Generation: 2, N: 7},
		"no epoch":           nil,
	} {
		t.Run(name, func(t *testing.T) {
			w, _ := newScripted(t, func(m *protocolv1.WriteRequest) *protocolv1.WriteResponse {
				if m.GetPreCommit() != nil {
					return stagedReply(other)
				}
				return committedReply(other, nil)
			})
			if _, err := w.PreCommit(t.Context(), e); errs.CodeOf(err) != errs.Internal {
				t.Errorf("PreCommit: %v, want INTERNAL", err)
			}
			if _, err := w.Commit(t.Context(), e, nil); errs.CodeOf(err) != errs.Internal {
				t.Errorf("Commit: %v, want INTERNAL", err)
			}
		})
	}

	own := epochProto(e)
	w, _ := newScripted(t, func(m *protocolv1.WriteRequest) *protocolv1.WriteResponse {
		if m.GetPreCommit() != nil {
			return stagedReply(own)
		}
		return committedReply(own, nil)
	})
	if st, err := w.PreCommit(t.Context(), e); err != nil || string(st) != "c" {
		t.Errorf("PreCommit: %q, %v", st, err)
	}
	if _, err := w.Commit(t.Context(), e, nil); err != nil {
		t.Errorf("Commit: %v", err)
	}
}

// A failed commit's error is what matters, whatever epoch it names.
func TestAFencedCommitStaysFenced(t *testing.T) {
	w, _ := newScripted(t, func(*protocolv1.WriteRequest) *protocolv1.WriteResponse {
		return committedReply(nil, errs.ToProto(errs.New(errs.Fenced, "a newer generation")))
	})
	if _, err := w.Commit(t.Context(), destination.Epoch{Generation: 3, N: 7}, nil); !errors.Is(err, destination.ErrFenced) {
		t.Errorf("Commit: %v, want it fenced", err)
	}
}

// Each kind of reply has one call waiting at most, so a second reply nothing
// asked for fails the stream instead of hanging it.
func TestUnsolicitedRepliesFailTheStream(t *testing.T) {
	w, s := newScripted(t, nil)
	s.replies <- stagedReply(nil)
	s.replies <- stagedReply(nil)
	select {
	case <-w.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the stream hung")
	}
	if errs.CodeOf(w.err) != errs.Internal {
		t.Errorf("err: %v, want INTERNAL", w.err)
	}
}
