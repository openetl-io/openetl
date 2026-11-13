// Package attempt is the attempt protocol (openetl.attempt.v1) between the
// runner that coordinates an attempt and the attempt's workers, each a
// runner in a pod of its own. It carries the protocol's messages and keeps
// its rules: who may join, the order of barriers, shares, positions and
// commits, one share per worker for an epoch, heartbeats, and that anything
// lost, late or out of the protocol fails the whole attempt. What the
// messages say, an assignment's layout or a share's content, is its
// callers'.
package attempt

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// ErrRefused: the coordinator refused the worker, which is of another
// attempt or generation, or the runner doesn't coordinate its attempt.
var ErrRefused = errors.New("attempt: refused")

// ErrLost: the other end went quiet for longer than the heartbeat timeout,
// or its call broke.
var ErrLost = errors.New("attempt: lost")

// ErrProtocol: the other end sent something the protocol doesn't allow
// there.
var ErrProtocol = errors.New("attempt: out of the protocol")

// ErrAborted: the coordinator ended the attempt because it failed.
var ErrAborted = errors.New("attempt: the coordinator ended the attempt")

// ErrOver: the attempt ended, so there's nothing more to send or wait for.
var ErrOver = errors.New("attempt: over")

const (
	DefaultHeartbeat   = 2 * time.Second
	DefaultTimeout     = 10 * time.Second
	DefaultJoinTimeout = 5 * time.Minute
)

// Options are the protocol's timings. Zero fields take the defaults. Both
// ends of an attempt run one image, so they share them.
type Options struct {
	// How often each end sends a heartbeat.
	Heartbeat time.Duration
	// How long each end waits to hear from the other before taking it as
	// lost.
	Timeout time.Duration
	// How long a coordinator's workers have to join once it's opened. A
	// worker waits this long and Timeout more for its assignment.
	JoinTimeout time.Duration
}

func (o Options) heartbeat() time.Duration {
	if o.Heartbeat > 0 {
		return o.Heartbeat
	}
	return DefaultHeartbeat
}

func (o Options) timeout() time.Duration {
	if o.Timeout > 0 {
		return o.Timeout
	}
	return DefaultTimeout
}

func (o Options) joinTimeout() time.Duration {
	if o.JoinTimeout > 0 {
		return o.JoinTimeout
	}
	return DefaultJoinTimeout
}

// Attempt is an attempt as its coordinator serves it.
type Attempt struct {
	ID         string
	Generation uint64
	// How many workers it launched, which join as 0 up to Workers-1.
	Workers int
	// The secret its workers present, such as crypto/rand.Text's.
	Token string
}

// Coordinator is the coordinating runner's side of one attempt. Its methods
// may be called from any goroutine. Once the attempt fails, every method
// returns why, and every worker is told to stop.
type Coordinator interface {
	// Assign waits for every worker to join, then sends each the
	// assignment, its workers set to the addresses they joined with.
	Assign(ctx context.Context, a *attemptv1.Assignment) error
	// Barrier asks every worker to place barrier n: the assignment's first
	// epoch, then each next one.
	Barrier(n uint64) error
	// Shares waits for every worker's share of epoch n, the one after the
	// last it returned, and returns them by worker.
	Shares(ctx context.Context, n uint64) ([]*attemptv1.Share, error)
	// Acknowledge sends every worker a committed record's positions, which
	// are all a worker acknowledges to its sources. They go in the order of
	// their records, the restored one's or those of epochs Shares returned,
	// once every commit asked for up to them has succeeded.
	Acknowledge(p *attemptv1.Positions) error
	// Commit asks a worker to commit what a written record names, waiting
	// for the worker to join, and then for its answer, whose error says if
	// the commit failed. The record is of an epoch Shares returned, or, to
	// settle the restored one, of an earlier generation, which may be asked
	// before the assignment. A request is idempotent: asking again, as
	// after a failed answer or a ctx that ended, sends it again.
	Commit(ctx context.Context, worker int, c *attemptv1.Commit) (*protocolv1.WriteCommitted, error)
	// Done is closed once the attempt is over: it failed, or End ended it.
	Done() <-chan struct{}
	// Err is why the attempt failed; nil while it runs, or once End ended
	// it without a cause.
	Err() error
	// End ends the attempt, unless it's over, with cause nil when it
	// finished. It tells every worker, waits until they've closed their
	// calls or ctx is done, and stops serving the attempt.
	End(ctx context.Context, cause error) error
}

// Worker is a worker's side of its attempt, from Join. Its methods may be
// called from any goroutine.
type Worker interface {
	// Requests are the coordinator's assignment, barriers, positions and
	// commits, in the order it sent them. It's closed once Done is. A
	// commit asked for again is answered again by the worker's side
	// itself once Committed answered it, and isn't repeated here while it
	// waits for that answer.
	Requests() <-chan *attemptv1.AttendResponse
	// Share sends the worker's share of the next epoch, once its barrier
	// was requested, the worker's lanes have passed it, and its snapshot
	// files and staging are durable. It holds a committable for each of the
	// worker's writer lanes.
	Share(s *attemptv1.Share) error
	// Committed answers a commit from Requests once it has happened, or
	// failed, naming its epoch and writer lanes.
	Committed(c *attemptv1.Committed) error
	// Fail tells the coordinator the worker failed, which fails the
	// attempt, and ends the worker's part in it.
	Fail(err error)
	// Done is closed once the worker's part in the attempt is over.
	Done() <-chan struct{}
	// Err is why: nil once the coordinator ended the attempt as finished.
	Err() error
	// Close ends the worker's part, as Fail does if it isn't over, and
	// waits for its call to end.
	Close() error
}

// Target is where a worker joins, and what as, from its pod's spec.
type Target struct {
	// The coordinating runner's attempt server, as host:port, and its
	// certificate, PEM-encoded, which is all the worker trusts there.
	Address     string
	Certificate []byte
	// Replaces Certificate: how the worker verifies the coordinator, and
	// any certificate it shows itself.
	TLS   *tls.Config
	Token string

	Attempt    string
	Generation uint64
	Worker     int
	// Where other workers' engines reach this worker's, as host:port;
	// empty: nowhere.
	Exchange string
}

func lost(who string, cause error) error {
	return errs.Wrap(errs.Transient, fmt.Errorf("%w: %s: %w", ErrLost, who, cause), "")
}

func outOfProtocol(format string, args ...any) error {
	return errs.Wrap(errs.Internal, fmt.Errorf("%w: %s", ErrProtocol, fmt.Sprintf(format, args...)), "")
}

func refused(reason string) error {
	return errs.Wrap(errs.Fenced, fmt.Errorf("%w: %s", ErrRefused, reason), "")
}

// reported is an error the other end sent.
func reported(p *protocolv1.Error) error {
	if cause := errs.FromProto(p); cause != nil {
		return cause
	}
	return errs.New(errs.Internal, "no reason given")
}

// classedAs is err, which wraps cause, with cause's code.
func classedAs(cause, err error) error {
	return &errs.Error{Code: errs.CodeOf(cause), Err: err, RetryAfter: errs.RetryAfterOf(cause)}
}

func epochOf(gen, n uint64) *protocolv1.Epoch { return &protocolv1.Epoch{Generation: gen, N: n} }
