package attempt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// pipe is a worker's call in memory, ordered as a gRPC call is, for tests
// in a synctest bubble.
type pipe struct {
	up   chan *attemptv1.AttendRequest
	down chan *attemptv1.AttendResponse
	// The worker closed its side.
	half     chan struct{}
	halfOnce sync.Once
	// The coordinator ended the call, with endErr.
	ended    chan struct{}
	endErr   error
	endOnce  sync.Once
	gone     chan struct{}
	goneOnce sync.Once
}

func newPipe() *pipe {
	return &pipe{
		up: make(chan *attemptv1.AttendRequest), down: make(chan *attemptv1.AttendResponse),
		half: make(chan struct{}), ended: make(chan struct{}), gone: make(chan struct{}),
	}
}

func (p *pipe) end(err error) {
	p.endOnce.Do(func() {
		p.endErr = err
		close(p.ended)
	})
}

func (p *pipe) hangUp() { p.goneOnce.Do(func() { close(p.gone) }) }

var errHungUp = status.Error(codes.Canceled, "the worker hung up")

type workerEnd struct{ *pipe }

func (p workerEnd) Send(m *attemptv1.AttendRequest) error {
	select {
	case p.up <- m:
		return nil
	case <-p.ended:
		return io.EOF
	case <-p.gone:
		return errHungUp
	}
}

func (p workerEnd) Recv() (*attemptv1.AttendResponse, error) {
	select {
	case m := <-p.down:
		return m, nil
	case <-p.ended:
		if p.endErr != nil {
			return nil, p.endErr
		}
		return nil, io.EOF
	case <-p.gone:
		return nil, errHungUp
	}
}

func (p workerEnd) CloseSend() error {
	p.halfOnce.Do(func() { close(p.half) })
	return nil
}

type coordinatorEnd struct{ *pipe }

func (p coordinatorEnd) Send(m *attemptv1.AttendResponse) error {
	select {
	case p.down <- m:
		return nil
	case <-p.gone:
		return errHungUp
	case <-p.ended:
		return errors.New("the call ended")
	}
}

func (p coordinatorEnd) Recv() (*attemptv1.AttendRequest, error) {
	select {
	case m := <-p.up:
		return m, nil
	case <-p.half:
		return nil, io.EOF
	case <-p.gone:
		return nil, errHungUp
	case <-p.ended:
		return nil, errors.New("the call ended")
	}
}

// connect opens a call to c, which serves it on a goroutine of its own.
func connect(c *coordinator) *pipe {
	p := newPipe()
	go func() { p.end(c.serve(coordinatorEnd{p})) }()
	return p
}

// coordinatorFor opens a, and ends it once the test is over, whatever the
// test's outcome, so no goroutine outlives a synctest bubble.
func coordinatorFor(t *testing.T, a Attempt, opts Options) *coordinator {
	c := newCoordinator(a, opts, func() {})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()
		_ = c.End(ctx, nil)
	})
	return c
}

// joinPipe starts a worker's part over a call to c, and closes it once the
// test is over.
func joinPipe(t *testing.T, c *coordinator, tg Target, opts Options) *worker {
	p := connect(c)
	w := start(workerEnd{p}, p.hangUp, tg, opts)
	t.Cleanup(func() { _ = w.Close() })
	return w
}

// rawWorker is a worker that sends what a test says, and keeps what the
// coordinator sends it but heartbeats.
type rawWorker struct {
	p    *pipe
	msgs chan *attemptv1.AttendResponse
	// How the call ended.
	err chan error
}

func raw(t *testing.T, c *coordinator) *rawWorker {
	p := connect(c)
	t.Cleanup(p.hangUp)
	r := &rawWorker{p: p, msgs: make(chan *attemptv1.AttendResponse, 64), err: make(chan error, 1)}
	go func() {
		for {
			m, err := workerEnd{p}.Recv()
			if err != nil {
				r.err <- err
				close(r.msgs)
				return
			}
			if m.GetHeartbeat() == nil {
				r.msgs <- m
			}
		}
	}()
	return r
}

func (r *rawWorker) send(t *testing.T, m *attemptv1.AttendRequest) {
	t.Helper()
	if err := (workerEnd{r.p}).Send(m); err != nil {
		t.Fatalf("send %v: %v", m, err)
	}
}

func (r *rawWorker) join(t *testing.T, tg Target) {
	t.Helper()
	r.send(t, &attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Join{Join: &attemptv1.Join{
		Attempt: tg.Attempt, Generation: tg.Generation, Worker: uint32(tg.Worker), Address: tg.Exchange,
	}}})
}

func (r *rawWorker) share(t *testing.T, s *attemptv1.Share) {
	t.Helper()
	r.send(t, &attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Share{Share: s}})
}

func (r *rawWorker) next(t *testing.T) *attemptv1.AttendResponse {
	t.Helper()
	select {
	case m, ok := <-r.msgs:
		if !ok {
			t.Fatalf("the call ended: %v", <-r.err)
		}
		return m
	case <-time.After(time.Hour):
		t.Fatal("nothing from the coordinator")
		return nil
	}
}

// rawCoordinator is a coordinator that sends what a test says, and keeps
// what the worker sends it but heartbeats.
type rawCoordinator struct {
	p    *pipe
	msgs chan *attemptv1.AttendRequest
	err  chan error
}

// rawStart starts a worker's part over a call to a rawCoordinator.
func rawStart(t *testing.T, tg Target, opts Options) (*rawCoordinator, *worker) {
	p := newPipe()
	r := &rawCoordinator{p: p, msgs: make(chan *attemptv1.AttendRequest, 64), err: make(chan error, 1)}
	go func() {
		for {
			m, err := coordinatorEnd{p}.Recv()
			if err != nil {
				r.err <- err
				close(r.msgs)
				return
			}
			if m.GetHeartbeat() == nil {
				r.msgs <- m
			}
		}
	}()
	w := start(workerEnd{p}, p.hangUp, tg, opts)
	t.Cleanup(func() { _ = w.Close() })
	t.Cleanup(func() { p.end(nil) })
	return r, w
}

func (r *rawCoordinator) send(t *testing.T, m *attemptv1.AttendResponse) {
	t.Helper()
	if err := (coordinatorEnd{r.p}).Send(m); err != nil {
		t.Fatalf("send %v: %v", m, err)
	}
}

func (r *rawCoordinator) next(t *testing.T) *attemptv1.AttendRequest {
	t.Helper()
	select {
	case m, ok := <-r.msgs:
		if !ok {
			t.Fatalf("the call ended: %v", <-r.err)
		}
		return m
	case <-time.After(time.Hour):
		t.Fatal("nothing from the worker")
		return nil
	}
}

const (
	testAttempt = "attempt-1"
	testGen     = 3
	testFirst   = 5
)

func testAttemptOf(workers int) Attempt {
	return Attempt{ID: testAttempt, Generation: testGen, Workers: workers, Token: "secret"}
}

func target(i int) Target {
	return Target{Attempt: testAttempt, Generation: testGen, Worker: i, Exchange: fmt.Sprintf("w%d:7720", i)}
}

// layout gives each of n workers a lane and a writer lane, owning a range
// of the key groups, and a split.
func layout(n int) *attemptv1.Assignment {
	a := &attemptv1.Assignment{FirstEpoch: testFirst, KeyGroups: 256, Restored: []byte(`{"epoch":4,"generation":2}`)}
	for i := range n {
		kg := &enginev1.KeyGroupRange{First: uint32(i * 256 / n), Last: uint32((i+1)*256/n - 1)}
		a.Lanes = append(a.Lanes, &attemptv1.Lane{Worker: uint32(i), KeyGroups: kg})
		a.WriterLanes = append(a.WriterLanes, &attemptv1.Lane{Worker: uint32(i), KeyGroups: kg})
		a.Splits = append(a.Splits, &attemptv1.SplitAssignment{Lane: uint32(i), Split: &attemptv1.SplitPosition{
			Stream: "events", Split: &protocolv1.Split{Id: fmt.Sprintf("p%d", i), State: []byte("0")}, Watermark: proto.Int64(1000),
		}})
	}
	return a
}

func epoch(n uint64) *protocolv1.Epoch { return epochOf(testGen, n) }

// shareOf is worker's share of epoch n under layout.
func shareOf(n uint64, worker int, local bool) *attemptv1.Share {
	e := epoch(n)
	return &attemptv1.Share{
		Epoch: e,
		Positions: []*attemptv1.SplitPosition{{
			Stream: "events", Split: &protocolv1.Split{Id: fmt.Sprintf("p%d", worker), State: fmt.Appendf(nil, "%d", n)},
			Complete: n > 6, Watermark: proto.Int64(int64(n) * 1000),
		}},
		Rows:         map[string]uint64{"events": n * 10},
		Committables: []*attemptv1.Committable{{WriterLane: uint32(worker), Staged: fmt.Appendf(nil, "w%d.e%d", worker, n), Local: local}},
		Ready: &enginev1.EpochReady{
			Epoch:      e,
			Schemas:    map[string][]byte{"events": []byte("schema")},
			Snapshots:  []*enginev1.StateSnapshot{{Operator: "agg", Url: fmt.Sprintf("file:///state/%d/%d", n, worker)}},
			Quarantine: []*enginev1.QuarantineFile{{Stream: "events", Url: "file:///q", Rows: 1}},
		},
	}
}

// commitOf asks for the committables of lanes, of a record of epoch e with
// writerLanes writer lanes.
func commitOf(e *protocolv1.Epoch, writerLanes int, lanes ...uint32) *attemptv1.Commit {
	c := &attemptv1.Commit{Commit: &protocolv1.WriteCommit{Epoch: e}, WriterLanes: lanes}
	for w := range writerLanes {
		c.Commit.Staged = append(c.Commit.Staged, fmt.Appendf(nil, "lane %d of epoch %d", w, e.GetN()))
	}
	return c
}

// committedOf answers req, failed with err if it's set.
func committedOf(req *attemptv1.Commit, err *protocolv1.Error) *attemptv1.Committed {
	return &attemptv1.Committed{
		Committed:   &protocolv1.WriteCommitted{Epoch: req.GetCommit().GetEpoch(), Error: err},
		WriterLanes: req.GetWriterLanes(),
	}
}

// within waits for ch, up to d of the bubble's time.
func within[T any](t *testing.T, ch <-chan T, d time.Duration, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(d):
		t.Fatalf("%s: not within %s", what, d)
		var zero T
		return zero
	}
}

func isDone(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
