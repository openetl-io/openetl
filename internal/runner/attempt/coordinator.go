package attempt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// coordinatorStream is a worker's call, as its coordinator sees it.
type coordinatorStream interface {
	Send(*attemptv1.AttendResponse) error
	Recv() (*attemptv1.AttendRequest, error)
}

var coordinatorHeartbeat = &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Heartbeat{Heartbeat: &attemptv1.Heartbeat{}}}

type coordinator struct {
	a          Attempt
	opts       Options
	unregister func()
	joinTimer  *time.Timer

	mu sync.Mutex
	// By index; nil until the worker joins.
	members    []*member
	joined     int
	allJoined  chan struct{}
	assignment *attemptv1.Assignment
	// The last barrier requested, and the last epoch Shares returned.
	requested, gathered uint64
	acked               *protocolv1.Epoch
	// By epoch, then by worker.
	shares map[uint64][]*attemptv1.Share
	// Closed, and replaced, when a worker joins or a share arrives.
	changed chan struct{}
	over    bool
	err     error
	done    chan struct{}
}

type member struct {
	index   int
	address string
	out     *outbox[*attemptv1.AttendResponse]
	// The writer lanes the assignment gives it.
	writerLanes map[uint32]bool
	// The last epoch it shared.
	shared uint64
	// The commits it was asked for that haven't succeeded, by commitKey.
	commits map[string]*commitState
	// Closed when its call ends.
	ended chan struct{}
}

type commitState struct {
	epoch *protocolv1.Epoch
	// Waiting for the answer; nil once the last answer said it failed.
	pending *pendingCommit
}

type pendingCommit struct {
	done   chan struct{}
	answer *protocolv1.WriteCommitted
}

var _ Coordinator = (*coordinator)(nil)

func newCoordinator(a Attempt, opts Options, unregister func()) *coordinator {
	c := &coordinator{
		a: a, opts: opts, unregister: unregister,
		members: make([]*member, a.Workers), allJoined: make(chan struct{}),
		shares: map[uint64][]*attemptv1.Share{}, changed: make(chan struct{}), done: make(chan struct{}),
	}
	c.joinTimer = time.AfterFunc(opts.joinTimeout(), c.joinDeadline)
	return c
}

func (c *coordinator) joinDeadline() {
	c.mu.Lock()
	missing := -1
	for i, m := range c.members {
		if m == nil {
			missing = i
			break
		}
	}
	c.mu.Unlock()
	if missing >= 0 {
		c.finish(lost(fmt.Sprintf("worker %d", missing), fmt.Errorf("it didn't join within %s", c.opts.joinTimeout())))
	}
}

// finish ends the attempt, once: err is why it failed, nil if it finished.
// Every worker that joined is told.
func (c *coordinator) finish(err error) {
	c.mu.Lock()
	if c.over {
		c.mu.Unlock()
		return
	}
	c.over, c.err = true, err
	close(c.done)
	end := &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_End{End: &attemptv1.End{Error: errs.ToProto(err)}}}
	for _, m := range c.members {
		if m != nil {
			m.out.push(end)
			m.out.close()
		}
	}
	c.mu.Unlock()
	c.joinTimer.Stop()
	c.unregister()
}

func (c *coordinator) overErr() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.overErrLocked()
}

func (c *coordinator) overErrLocked() error {
	if c.err != nil {
		return c.err
	}
	return ErrOver
}

// admit takes a worker's join, or refuses it with the status its call ends
// with.
func (c *coordinator) admit(j *attemptv1.Join) (*member, error) {
	if j == nil {
		return nil, status.Error(codes.InvalidArgument, "attempt: a worker's first message is its join")
	}
	if j.GetAttempt() != c.a.ID || j.GetGeneration() != c.a.Generation {
		return nil, status.Errorf(codes.FailedPrecondition, "attempt: this is attempt %s of generation %d, not %s of generation %d",
			c.a.ID, c.a.Generation, j.GetAttempt(), j.GetGeneration())
	}
	i := int(j.GetWorker())
	c.mu.Lock()
	var broken error
	switch {
	case c.over:
		c.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "attempt: the attempt is over")
	case i >= len(c.members):
		broken = outOfProtocol("worker %d joined, of %d", i, len(c.members))
	case c.members[i] != nil:
		broken = outOfProtocol("worker %d joined twice", i)
	}
	if broken != nil {
		c.mu.Unlock()
		c.finish(broken)
		return nil, status.Error(codes.FailedPrecondition, broken.Error())
	}
	m := &member{
		index: i, address: j.GetAddress(), out: newOutbox[*attemptv1.AttendResponse](),
		writerLanes: map[uint32]bool{}, commits: map[string]*commitState{}, ended: make(chan struct{}),
	}
	// It says the join is taken.
	m.out.push(coordinatorHeartbeat)
	c.members[i] = m
	if c.joined++; c.joined == len(c.members) {
		close(c.allJoined)
		c.joinTimer.Stop()
	}
	close(c.changed)
	c.changed = make(chan struct{})
	c.mu.Unlock()
	return m, nil
}

// serve is one worker's call, from its join until the call ends.
func (c *coordinator) serve(st coordinatorStream) error {
	in := make(chan received[*attemptv1.AttendRequest])
	stopRecv := make(chan struct{})
	defer close(stopRecv)
	go receive(st.Recv, in, stopRecv)
	var first received[*attemptv1.AttendRequest]
	select {
	case first = <-in:
	case <-c.done:
		return status.Error(codes.FailedPrecondition, "attempt: the attempt is over")
	}
	if first.err != nil {
		return first.err
	}
	m, err := c.admit(first.msg.GetJoin())
	if err != nil {
		return err
	}
	defer close(m.ended)
	who := fmt.Sprintf("worker %d", m.index)

	stopSend, senderDone := make(chan struct{}), make(chan struct{})
	sent := make(chan error, 1)
	go func() {
		defer close(senderDone)
		sent <- m.out.run(st.Send, coordinatorHeartbeat, c.opts.heartbeat(), stopSend)
	}()
	timer := time.NewTimer(c.opts.timeout())
	defer timer.Stop()
	// quit ends the call once the sender has stopped, or a heartbeat later
	// if it's stuck on a worker that doesn't read.
	quit := func(err error) error {
		close(stopSend)
		select {
		case <-senderDone:
		case <-time.After(c.opts.heartbeat()):
		}
		return err
	}
	for {
		select {
		case r := <-in:
			switch {
			case r.err == nil:
			case errors.Is(r.err, io.EOF) && c.isOver():
				return quit(nil)
			case errors.Is(r.err, io.EOF):
				err := outOfProtocol("%s ended its call before the attempt ended", who)
				c.finish(err)
				return quit(status.Error(codes.Aborted, err.Error()))
			default:
				c.finish(lost(who, r.err))
				return quit(r.err)
			}
			timer.Reset(c.opts.timeout())
			if err := c.handle(m, r.msg); err != nil {
				c.finish(err)
			}
		case <-timer.C:
			err := lost(who, fmt.Errorf("nothing from it for %s", c.opts.timeout()))
			c.finish(err)
			return quit(status.Error(codes.Aborted, err.Error()))
		case err := <-sent:
			sent = nil
			if err != nil {
				c.finish(lost(who, err))
				return quit(status.Error(codes.Aborted, err.Error()))
			}
			// End went out: the worker closes its side next.
		}
	}
}

func (c *coordinator) isOver() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.over
}

// handle takes a message from a worker that joined, and returns why it
// fails the attempt, if it does.
func (c *coordinator) handle(m *member, msg *attemptv1.AttendRequest) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.over {
		return nil
	}
	who := fmt.Sprintf("worker %d", m.index)
	switch x := msg.GetMessage().(type) {
	case *attemptv1.AttendRequest_Heartbeat:
		return nil
	case *attemptv1.AttendRequest_Share:
		return c.takeShare(m, x.Share)
	case *attemptv1.AttendRequest_Committed:
		answer := x.Committed.GetCommitted()
		key := commitKey(answer.GetEpoch(), x.Committed.GetWriterLanes())
		// An answer nothing waits for answers a request sent again.
		if st := m.commits[key]; st != nil && st.pending != nil {
			st.pending.answer = answer
			close(st.pending.done)
			st.pending = nil
			if answer.GetError() == nil {
				delete(m.commits, key)
			}
		}
		return nil
	case *attemptv1.AttendRequest_Failure:
		cause := reported(x.Failure)
		return classedAs(cause, fmt.Errorf("attempt: %s failed: %w", who, cause))
	case *attemptv1.AttendRequest_Join:
		return outOfProtocol("%s joined again", who)
	default:
		return outOfProtocol("%s sent an empty message", who)
	}
}

// takeShare keeps a worker's share; c.mu is held.
func (c *coordinator) takeShare(m *member, s *attemptv1.Share) error {
	who := fmt.Sprintf("worker %d", m.index)
	e := s.GetEpoch()
	switch {
	case c.assignment == nil:
		return outOfProtocol("%s shared epoch %d before its assignment", who, e.GetN())
	case e.GetGeneration() != c.a.Generation:
		return outOfProtocol("%s shared an epoch of generation %d, not %d", who, e.GetGeneration(), c.a.Generation)
	case e.GetN() <= m.shared:
		return outOfProtocol("%s shared epoch %d again", who, e.GetN())
	case e.GetN() != m.shared+1 || e.GetN() > c.requested:
		return outOfProtocol("%s shared epoch %d, when its next is %d and the last barrier requested %d", who, e.GetN(), m.shared+1, c.requested)
	}
	if err := checkShare(s, m.writerLanes); err != nil {
		return outOfProtocol("%s: %v", who, err)
	}
	m.shared = e.GetN()
	got := c.shares[e.GetN()]
	if got == nil {
		got = make([]*attemptv1.Share, len(c.members))
		c.shares[e.GetN()] = got
	}
	got[m.index] = s
	close(c.changed)
	c.changed = make(chan struct{})
	return nil
}

// checkShare checks what a share says of itself: its engine's epoch, and a
// committable for each of lanes, its worker's writer lanes.
func checkShare(s *attemptv1.Share, lanes map[uint32]bool) error {
	e := s.GetEpoch()
	if r := s.GetReady().GetEpoch(); r != nil && (r.GetGeneration() != e.GetGeneration() || r.GetN() != e.GetN()) {
		return fmt.Errorf("its share of epoch %d holds its engine's of epoch %d of generation %d", e.GetN(), r.GetN(), r.GetGeneration())
	}
	seen := make(map[uint32]bool, len(lanes))
	for _, cm := range s.GetCommittables() {
		w := cm.GetWriterLane()
		switch {
		case !lanes[w]:
			return fmt.Errorf("its share of epoch %d holds a committable of writer lane %d, which isn't its", e.GetN(), w)
		case seen[w]:
			return fmt.Errorf("its share of epoch %d holds two committables of writer lane %d", e.GetN(), w)
		}
		seen[w] = true
	}
	if len(seen) != len(lanes) {
		return fmt.Errorf("its share of epoch %d holds committables of %d of its %d writer lanes", e.GetN(), len(seen), len(lanes))
	}
	return nil
}

// checkAssignment checks an assignment's layout against its workers.
func checkAssignment(a *attemptv1.Assignment, workers int) error {
	if a.GetFirstEpoch() == 0 {
		return errors.New("its first epoch is 0")
	}
	check := func(what string, ls []*attemptv1.Lane) error {
		for i, l := range ls {
			if int(l.GetWorker()) >= workers {
				return fmt.Errorf("%s %d is worker %d's, of %d", what, i, l.GetWorker(), workers)
			}
			if kg := l.GetKeyGroups(); kg != nil && (kg.GetFirst() > kg.GetLast() || a.GetKeyGroups() > 0 && kg.GetLast() >= a.GetKeyGroups()) {
				return fmt.Errorf("%s %d owns key groups %d to %d, of %d", what, i, kg.GetFirst(), kg.GetLast(), a.GetKeyGroups())
			}
		}
		return nil
	}
	if err := check("lane", a.GetLanes()); err != nil {
		return err
	}
	if err := check("writer lane", a.GetWriterLanes()); err != nil {
		return err
	}
	for _, s := range a.GetSplits() {
		p := s.GetSplit()
		switch {
		case p.GetSplit() == nil:
			return fmt.Errorf("a split of stream %q has no ID", p.GetStream())
		case int(s.GetLane()) >= len(a.GetLanes()):
			return fmt.Errorf("split %q of stream %q is lane %d's, of %d", p.GetSplit().GetId(), p.GetStream(), s.GetLane(), len(a.GetLanes()))
		}
	}
	return nil
}

func (c *coordinator) Assign(ctx context.Context, a *attemptv1.Assignment) error {
	if err := checkAssignment(a, len(c.members)); err != nil {
		return fmt.Errorf("attempt: the assignment: %w", err)
	}
	select {
	case <-c.allJoined:
	case <-c.done:
		return c.overErr()
	case <-ctx.Done():
		return ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.over:
		return c.overErrLocked()
	case c.assignment != nil:
		return errors.New("attempt: the workers have their assignment already")
	}
	a = proto.CloneOf(a)
	a.Workers = make([]string, len(c.members))
	for i, m := range c.members {
		a.Workers[i] = m.address
		m.shared = a.GetFirstEpoch() - 1
	}
	for w, l := range a.GetWriterLanes() {
		c.members[l.GetWorker()].writerLanes[ints.Uint32(w)] = true
	}
	c.assignment = a
	c.requested, c.gathered = a.GetFirstEpoch()-1, a.GetFirstEpoch()-1
	msg := &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Assignment{Assignment: a}}
	for _, m := range c.members {
		m.out.push(msg)
	}
	return nil
}

func (c *coordinator) Barrier(n uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case c.over:
		return c.overErrLocked()
	case c.assignment == nil:
		return errors.New("attempt: a barrier before the assignment")
	case n != c.requested+1:
		return fmt.Errorf("attempt: barrier %d requested after %d", n, c.requested)
	}
	c.requested = n
	msg := &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Barrier{Barrier: &attemptv1.Barrier{Epoch: epochOf(c.a.Generation, n)}}}
	for _, m := range c.members {
		m.out.push(msg)
	}
	return nil
}

func (c *coordinator) Shares(ctx context.Context, n uint64) ([]*attemptv1.Share, error) {
	for {
		c.mu.Lock()
		if c.over {
			err := c.overErrLocked()
			c.mu.Unlock()
			return nil, err
		}
		if n != c.gathered+1 || n > c.requested {
			err := fmt.Errorf("attempt: shares of epoch %d asked for after those of %d, with barrier %d requested", n, c.gathered, c.requested)
			c.mu.Unlock()
			return nil, err
		}
		if got := c.shares[n]; complete(got) {
			delete(c.shares, n)
			c.gathered = n
			c.mu.Unlock()
			return got, nil
		}
		changed := c.changed
		c.mu.Unlock()
		select {
		case <-changed:
		case <-c.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func complete(shares []*attemptv1.Share) bool {
	for _, s := range shares {
		if s == nil {
			return false
		}
	}
	return len(shares) > 0
}

func (c *coordinator) Acknowledge(p *attemptv1.Positions) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.over {
		return c.overErrLocked()
	}
	if c.assignment == nil {
		return errors.New("attempt: positions before the assignment")
	}
	e, first := p.GetEpoch(), c.assignment.GetFirstEpoch()
	switch {
	case e.GetGeneration() > c.a.Generation:
		return fmt.Errorf("attempt: positions of generation %d, after %d", e.GetGeneration(), c.a.Generation)
	case e.GetGeneration() == c.a.Generation && (e.GetN() < first || e.GetN() > c.gathered):
		return fmt.Errorf("attempt: positions of epoch %d, whose shares weren't gathered", e.GetN())
	case e.GetGeneration() < c.a.Generation && e.GetN() >= first:
		return fmt.Errorf("attempt: positions of epoch %d of generation %d, past the restored record", e.GetN(), e.GetGeneration())
	case c.acked != nil && e.GetN() <= c.acked.GetN():
		return fmt.Errorf("attempt: positions of epoch %d, after those of %d", e.GetN(), c.acked.GetN())
	}
	for _, m := range c.members {
		for _, st := range m.commits {
			if f := st.epoch; f.GetGeneration() < e.GetGeneration() || f.GetGeneration() == e.GetGeneration() && f.GetN() <= e.GetN() {
				return fmt.Errorf("attempt: positions of epoch %d, before worker %d's commit of epoch %d succeeded", e.GetN(), m.index, f.GetN())
			}
		}
	}
	c.acked = e
	msg := &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Positions{Positions: p}}
	for _, m := range c.members {
		m.out.push(msg)
	}
	return nil
}

func (c *coordinator) Commit(ctx context.Context, worker int, req *attemptv1.Commit) (*protocolv1.WriteCommitted, error) {
	if worker < 0 || worker >= len(c.members) {
		return nil, fmt.Errorf("attempt: a commit for worker %d, of %d", worker, len(c.members))
	}
	if err := checkCommit(req); err != nil {
		return nil, fmt.Errorf("attempt: %w", err)
	}
	m, err := c.member(ctx, worker)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	e := req.GetCommit().GetEpoch()
	first := c.assignment.GetFirstEpoch()
	switch staged := len(req.GetCommit().GetStaged()); {
	case c.over:
		err = c.overErrLocked()
	case e.GetGeneration() > c.a.Generation:
		err = fmt.Errorf("attempt: a commit of an epoch of generation %d, after %d", e.GetGeneration(), c.a.Generation)
	case e.GetGeneration() == c.a.Generation && (c.assignment == nil || e.GetN() < first || e.GetN() > c.gathered):
		err = fmt.Errorf("attempt: a commit of epoch %d, whose shares weren't gathered", e.GetN())
	case e.GetGeneration() == c.a.Generation && staged != len(c.assignment.GetWriterLanes()):
		err = fmt.Errorf("attempt: a commit of %d committables, for %d writer lanes", staged, len(c.assignment.GetWriterLanes()))
	case e.GetGeneration() < c.a.Generation && c.assignment != nil && e.GetN() >= first:
		err = fmt.Errorf("attempt: a commit of epoch %d of generation %d, past the restored record", e.GetN(), e.GetGeneration())
	}
	if err != nil {
		c.mu.Unlock()
		return nil, err
	}
	key := commitKey(e, req.GetWriterLanes())
	st := m.commits[key]
	if st == nil {
		st = &commitState{epoch: e}
		m.commits[key] = st
	}
	if st.pending == nil {
		st.pending = &pendingCommit{done: make(chan struct{})}
	}
	p := st.pending
	// Sent again if it's waiting already: the worker answers it once.
	m.out.push(&attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Commit{Commit: req}})
	c.mu.Unlock()
	select {
	case <-p.done:
		return p.answer, nil
	case <-c.done:
		return nil, c.overErr()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// member waits for a worker to join.
func (c *coordinator) member(ctx context.Context, i int) (*member, error) {
	for {
		c.mu.Lock()
		m, changed := c.members[i], c.changed
		c.mu.Unlock()
		if m != nil {
			return m, nil
		}
		select {
		case <-changed:
		case <-c.done:
			return nil, c.overErr()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// checkCommit checks what a commit request says of itself.
func checkCommit(req *attemptv1.Commit) error {
	e, staged := req.GetCommit().GetEpoch(), len(req.GetCommit().GetStaged())
	if e.GetN() == 0 {
		return errors.New("a commit of no epoch")
	}
	seen := map[uint32]bool{}
	for _, l := range req.GetWriterLanes() {
		if int(l) >= staged || seen[l] {
			return fmt.Errorf("a commit of epoch %d names writer lane %d again, or of %d", e.GetN(), l, staged)
		}
		seen[l] = true
	}
	return nil
}

// commitKey names a commit request by its epoch and writer lanes, which a
// request sent again keeps.
func commitKey(e *protocolv1.Epoch, lanes []uint32) string {
	lanes = slices.Sorted(slices.Values(lanes))
	return fmt.Sprintf("%d/%d/%v", e.GetGeneration(), e.GetN(), lanes)
}

func (c *coordinator) Done() <-chan struct{} { return c.done }

func (c *coordinator) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

func (c *coordinator) End(ctx context.Context, cause error) error {
	c.finish(cause)
	c.mu.Lock()
	var ended []chan struct{}
	for _, m := range c.members {
		if m != nil {
			ended = append(ended, m.ended)
		}
	}
	c.mu.Unlock()
	for _, e := range ended {
		select {
		case <-e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
