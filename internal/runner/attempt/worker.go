package attempt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// workerStream is a worker's call to its coordinator.
type workerStream interface {
	Send(*attemptv1.AttendRequest) error
	Recv() (*attemptv1.AttendResponse, error)
	CloseSend() error
}

var workerHeartbeat = &attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Heartbeat{Heartbeat: &attemptv1.Heartbeat{}}}

// Join joins the attempt at t, and returns once the coordinator has taken
// the join. The worker's part outlives ctx, which bounds only the wait. Its
// assignment comes in Requests.
func Join(ctx context.Context, t Target, opts Options) (Worker, error) {
	cfg := t.TLS
	if cfg == nil {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(t.Certificate) {
			return nil, errors.New("attempt: the coordinator's certificate isn't PEM")
		}
		cfg = &tls.Config{RootCAs: pool, ServerName: serverName, MinVersion: tls.VersionTLS13}
	}
	conn, err := grpc.NewClient(t.Address,
		grpc.WithTransportCredentials(credentials.NewTLS(cfg)),
		grpc.WithPerRPCCredentials(bearer(t.Token)),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxMessage), grpc.MaxCallSendMsgSize(maxMessage)),
	)
	if err != nil {
		return nil, fmt.Errorf("attempt: joining at %s: %w", t.Address, err)
	}
	callCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	opened := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			cancel()
		case <-opened:
		}
	}()
	st, err := attemptv1.NewAttemptServiceClient(conn).Attend(callCtx)
	close(opened)
	if err != nil {
		cancel()
		_ = conn.Close()
		return nil, fmt.Errorf("attempt: joining at %s: %w", t.Address, err)
	}
	w := start(st, func() {
		cancel()
		_ = conn.Close()
	}, t, opts)
	if err := w.awaitTaken(ctx); err != nil {
		_ = w.Close()
		return nil, err
	}
	return w, nil
}

// bearer presents the attempt's token on every call.
type bearer string

func (b bearer) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(b)}, nil
}

func (bearer) RequireTransportSecurity() bool { return true }

type worker struct {
	t    Target
	opts Options
	st   workerStream
	// Ends the call and closes its connection.
	hangUp   func()
	out      *outbox[*attemptv1.AttendRequest]
	in       chan received[*attemptv1.AttendResponse]
	sent     chan error
	stopSend chan struct{}
	// Closed once the coordinator has said anything, which says it took
	// the join.
	taken    chan struct{}
	requests chan *attemptv1.AttendResponse
	queued   chan struct{}
	finished chan struct{}

	mu          sync.Mutex
	assignment  *attemptv1.Assignment
	writerLanes map[uint32]bool
	// The last barrier requested, and the last epoch shared.
	requested, shared uint64
	acked             *protocolv1.Epoch
	// The commits asked for, by commitKey: waiting for Committed, or
	// answered as committed.
	commits map[string]*workerCommit
	// Requests not yet taken.
	queue []*attemptv1.AttendResponse
	over  bool
	err   error
	done  chan struct{}
}

type workerCommit struct {
	epoch *protocolv1.Epoch
	// Nil while it waits for Committed.
	answer *attemptv1.Committed
}

var _ Worker = (*worker)(nil)

// start runs a worker's part over st, which it opens with its join.
func start(st workerStream, hangUp func(), t Target, opts Options) *worker {
	w := &worker{
		t: t, opts: opts, st: st, hangUp: hangUp,
		out: newOutbox[*attemptv1.AttendRequest](), in: make(chan received[*attemptv1.AttendResponse]),
		sent: make(chan error, 1), stopSend: make(chan struct{}), taken: make(chan struct{}),
		requests: make(chan *attemptv1.AttendResponse), queued: make(chan struct{}, 1), finished: make(chan struct{}),
		writerLanes: map[uint32]bool{}, commits: map[string]*workerCommit{}, done: make(chan struct{}),
	}
	w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Join{Join: &attemptv1.Join{
		Attempt: t.Attempt, Generation: t.Generation, Worker: ints.Uint32(t.Worker), Address: t.Exchange,
	}}})
	go w.send()
	go receive(st.Recv, w.in, w.finished)
	go w.run()
	go w.deliver()
	return w
}

func (w *worker) send() {
	err := w.out.run(w.st.Send, workerHeartbeat, w.opts.heartbeat(), w.stopSend)
	if err == nil {
		err = w.st.CloseSend()
	}
	w.sent <- err
}

// run follows the call until it ends, or the coordinator is lost.
func (w *worker) run() {
	defer close(w.finished)
	defer w.hangUp()
	timer := time.NewTimer(w.opts.timeout())
	defer timer.Stop()
	assign := time.NewTimer(w.opts.joinTimeout() + w.opts.timeout())
	defer assign.Stop()
	sent, taken := w.sent, false
	for {
		select {
		case r := <-w.in:
			if r.err != nil {
				w.end(w.callEnded(r.err, taken), false, true)
				return
			}
			if !taken {
				taken = true
				close(w.taken)
			}
			timer.Reset(w.opts.timeout())
			w.handle(r.msg)
		case <-timer.C:
			w.end(lost("the coordinator", fmt.Errorf("nothing from it for %s", w.opts.timeout())), false, true)
			return
		case <-assign.C:
			if !w.isAssigned() {
				w.end(lost("the coordinator", fmt.Errorf("no assignment within %s", w.opts.joinTimeout()+w.opts.timeout())), false, true)
				return
			}
		case err := <-sent:
			sent = nil
			// io.EOF: the coordinator ended the call, and Recv returns how,
			// a refusal among them.
			if err != nil && !errors.Is(err, io.EOF) {
				w.end(lost("the coordinator", err), false, true)
				return
			}
			// Otherwise the worker closed its side; the coordinator ends the
			// call.
		}
	}
}

func (w *worker) isAssigned() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.assignment != nil
}

// callEnded is why the call ended without End: before the coordinator took
// the join, a refusal.
func (w *worker) callEnded(err error, taken bool) error {
	if !taken {
		switch status.Code(err) {
		case codes.FailedPrecondition, codes.Unauthenticated, codes.InvalidArgument:
			return refused(strings.TrimPrefix(status.Convert(err).Message(), "attempt: "))
		}
	}
	if errors.Is(err, io.EOF) {
		err = errors.New("it ended the call")
	}
	return lost("the coordinator", err)
}

// end ends the worker's part, once, with err; failure tells the coordinator
// first, and abort stops sending at once.
func (w *worker) end(err error, failure, abort bool) {
	w.mu.Lock()
	if w.over {
		w.mu.Unlock()
		return
	}
	w.over, w.err = true, err
	if failure {
		w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Failure{Failure: errs.ToProto(err)}})
	}
	close(w.done)
	w.mu.Unlock()
	if abort {
		close(w.stopSend)
		return
	}
	w.out.close()
}

func (w *worker) handle(msg *attemptv1.AttendResponse) {
	if end := msg.GetEnd(); end != nil {
		var err error
		if end.GetError() != nil {
			cause := reported(end.GetError())
			err = classedAs(cause, fmt.Errorf("%w: %w", ErrAborted, cause))
		}
		w.end(err, false, false)
		return
	}
	if err := w.take(msg); err != nil {
		w.end(err, true, false)
	}
}

// take takes a message from the coordinator, and returns how it's out of
// the protocol, if it is.
func (w *worker) take(msg *attemptv1.AttendResponse) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.over {
		return nil
	}
	switch x := msg.GetMessage().(type) {
	case *attemptv1.AttendResponse_Heartbeat:
		return nil
	case *attemptv1.AttendResponse_Assignment:
		if err := w.assign(x.Assignment); err != nil {
			return err
		}
	case *attemptv1.AttendResponse_Commit:
		return w.askedToCommit(msg, x.Commit)
	case *attemptv1.AttendResponse_Barrier:
		e := x.Barrier.GetEpoch()
		switch {
		case w.assignment == nil:
			return outOfProtocol("the coordinator requested barrier %d before the assignment", e.GetN())
		case e.GetGeneration() != w.t.Generation || e.GetN() != w.requested+1:
			return outOfProtocol("the coordinator requested barrier %d of generation %d, after %d of %d", e.GetN(), e.GetGeneration(), w.requested, w.t.Generation)
		}
		w.requested = e.GetN()
	case *attemptv1.AttendResponse_Positions:
		e := x.Positions.GetEpoch()
		first := w.assignment.GetFirstEpoch()
		switch {
		case w.assignment == nil:
			return outOfProtocol("the coordinator sent positions before the assignment")
		case e.GetGeneration() > w.t.Generation,
			e.GetGeneration() == w.t.Generation && (e.GetN() < first || e.GetN() > w.shared),
			e.GetGeneration() < w.t.Generation && e.GetN() >= first,
			w.acked != nil && e.GetN() <= w.acked.GetN():
			return outOfProtocol("the coordinator sent positions of epoch %d of generation %d", e.GetN(), e.GetGeneration())
		}
		for _, c := range w.commits {
			if c.answer == nil && (c.epoch.GetGeneration() < e.GetGeneration() || c.epoch.GetGeneration() == e.GetGeneration() && c.epoch.GetN() <= e.GetN()) {
				return outOfProtocol("the coordinator sent positions of epoch %d before the commit of epoch %d was answered", e.GetN(), c.epoch.GetN())
			}
		}
		w.acked = e
		// The coordinator asks for none of these again.
		for key, c := range w.commits {
			if c.answer != nil && (c.epoch.GetGeneration() < e.GetGeneration() || c.epoch.GetGeneration() == e.GetGeneration() && c.epoch.GetN() <= e.GetN()) {
				delete(w.commits, key)
			}
		}
	default:
		return outOfProtocol("the coordinator sent %T", msg.GetMessage())
	}
	w.enqueue(msg)
	return nil
}

// askedToCommit takes a commit request: a new one is the caller's, one
// waiting for its answer is already, and one answered as committed is
// answered again. w.mu is held.
func (w *worker) askedToCommit(msg *attemptv1.AttendResponse, req *attemptv1.Commit) error {
	e := req.GetCommit().GetEpoch()
	first := w.assignment.GetFirstEpoch()
	if err := checkCommit(req); err != nil {
		return outOfProtocol("the coordinator sent %v", err)
	}
	switch {
	case e.GetGeneration() > w.t.Generation:
		return outOfProtocol("the coordinator asked for a commit of an epoch of generation %d, after %d", e.GetGeneration(), w.t.Generation)
	case e.GetGeneration() == w.t.Generation && (w.assignment == nil || e.GetN() < first || e.GetN() > w.shared):
		return outOfProtocol("the coordinator asked for a commit of epoch %d, which wasn't shared", e.GetN())
	case e.GetGeneration() < w.t.Generation && w.assignment != nil && e.GetN() >= first:
		return outOfProtocol("the coordinator asked for a commit of epoch %d of generation %d, past the restored record", e.GetN(), e.GetGeneration())
	}
	key := commitKey(e, req.GetWriterLanes())
	switch c := w.commits[key]; {
	case c != nil && c.answer != nil:
		w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Committed{Committed: c.answer}})
	case c != nil:
	case atOrBefore(e, w.acked):
		// Positions came after every commit up to them succeeded, so it's
		// committed; its answer, and what it counted, are gone.
		w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Committed{Committed: &attemptv1.Committed{
			Committed: &protocolv1.WriteCommitted{Epoch: e}, WriterLanes: req.GetWriterLanes(),
		}}})
	default:
		w.commits[key] = &workerCommit{epoch: e}
		w.enqueue(msg)
	}
	return nil
}

// atOrBefore says whether epoch e is at or before epoch of, which may be
// nil.
func atOrBefore(e, of *protocolv1.Epoch) bool {
	return of != nil && (e.GetGeneration() < of.GetGeneration() || e.GetGeneration() == of.GetGeneration() && e.GetN() <= of.GetN())
}

// enqueue queues a request for Requests; w.mu is held.
func (w *worker) enqueue(msg *attemptv1.AttendResponse) {
	w.queue = append(w.queue, msg)
	select {
	case w.queued <- struct{}{}:
	default:
	}
}

// assign takes the assignment; w.mu is held.
func (w *worker) assign(a *attemptv1.Assignment) error {
	if w.assignment != nil {
		return outOfProtocol("the coordinator sent a second assignment")
	}
	if err := checkAssignment(a, len(a.GetWorkers())); err != nil {
		return outOfProtocol("the assignment: %v", err)
	}
	if w.t.Worker >= len(a.GetWorkers()) {
		return outOfProtocol("the assignment has %d workers, without worker %d", len(a.GetWorkers()), w.t.Worker)
	}
	for i, l := range a.GetWriterLanes() {
		if int(l.GetWorker()) == w.t.Worker {
			w.writerLanes[ints.Uint32(i)] = true
		}
	}
	w.assignment = a
	w.requested, w.shared = a.GetFirstEpoch()-1, a.GetFirstEpoch()-1
	return nil
}

// deliver hands requests on, in order, until the worker's part is over.
func (w *worker) deliver() {
	defer close(w.requests)
	for {
		w.mu.Lock()
		var next *attemptv1.AttendResponse
		if len(w.queue) > 0 {
			next, w.queue = w.queue[0], w.queue[1:]
		}
		w.mu.Unlock()
		if next == nil {
			select {
			case <-w.queued:
				continue
			case <-w.done:
				return
			}
		}
		select {
		case w.requests <- next:
		case <-w.done:
			return
		}
	}
}

func (w *worker) awaitTaken(ctx context.Context) error {
	select {
	case <-w.taken:
		return nil
	case <-w.done:
		return w.overErr()
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *worker) overErr() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overErrLocked()
}

func (w *worker) overErrLocked() error {
	if w.err != nil {
		return w.err
	}
	return ErrOver
}

func (w *worker) Requests() <-chan *attemptv1.AttendResponse { return w.requests }

func (w *worker) Share(s *attemptv1.Share) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	switch e := s.GetEpoch(); {
	case w.over:
		return w.overErrLocked()
	case w.assignment == nil:
		return errors.New("attempt: a share before the assignment")
	case e.GetGeneration() != w.t.Generation || e.GetN() != w.shared+1 || e.GetN() > w.requested:
		return fmt.Errorf("attempt: a share of epoch %d of generation %d, when the next is %d and the last barrier requested %d", e.GetN(), e.GetGeneration(), w.shared+1, w.requested)
	}
	if err := checkShare(s, w.writerLanes); err != nil {
		return fmt.Errorf("attempt: %w", err)
	}
	w.shared = s.GetEpoch().GetN()
	w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Share{Share: s}})
	return nil
}

func (w *worker) Committed(c *attemptv1.Committed) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.over {
		return w.overErrLocked()
	}
	e := c.GetCommitted().GetEpoch()
	key := commitKey(e, c.GetWriterLanes())
	pending := w.commits[key]
	if pending == nil || pending.answer != nil {
		return fmt.Errorf("attempt: no commit of epoch %d of generation %d, writer lanes %v, waits for an answer", e.GetN(), e.GetGeneration(), c.GetWriterLanes())
	}
	if c.GetCommitted().GetError() != nil {
		// Asked again, the caller tries again.
		delete(w.commits, key)
	} else {
		pending.answer = c
	}
	w.out.push(&attemptv1.AttendRequest{Message: &attemptv1.AttendRequest_Committed{Committed: c}})
	return nil
}

func (w *worker) Fail(err error) {
	if err == nil {
		err = errs.New(errs.Internal, "attempt: the worker failed without a reason")
	}
	w.end(err, true, false)
}

func (w *worker) Done() <-chan struct{} { return w.done }

func (w *worker) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *worker) Close() error {
	w.end(errs.New(errs.Internal, "attempt: the worker closed before the attempt ended"), true, false)
	<-w.finished
	return nil
}
