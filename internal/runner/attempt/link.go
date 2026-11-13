package attempt

import (
	"errors"
	"sync"
	"time"
)

// errStopped: an outbox stopped without sending what it held.
var errStopped = errors.New("attempt: stopped")

// outbox sends one end's messages in order, from a goroutine of its own, so
// no caller waits on the network, with a heartbeat every interval.
type outbox[M any] struct {
	mu     sync.Mutex
	queue  []M
	closed bool
	wake   chan struct{}
}

func newOutbox[M any]() *outbox[M] { return &outbox[M]{wake: make(chan struct{}, 1)} }

// push queues m, and is false once the outbox is closed.
func (o *outbox[M]) push(m M) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return false
	}
	o.queue = append(o.queue, m)
	o.poke()
	return true
}

// close takes no more messages: run returns once it has sent those it
// holds.
func (o *outbox[M]) close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	o.poke()
}

func (o *outbox[M]) poke() {
	select {
	case o.wake <- struct{}{}:
	default:
	}
}

// run sends until the outbox is closed and empty, a send fails, or stop is
// closed.
func (o *outbox[M]) run(send func(M) error, heartbeat M, every time.Duration, stop <-chan struct{}) error {
	tick := time.NewTicker(every)
	defer tick.Stop()
	for {
		o.mu.Lock()
		queue, closed := o.queue, o.closed
		o.queue = nil
		o.mu.Unlock()
		for _, m := range queue {
			select {
			case <-stop:
				return errStopped
			default:
			}
			if err := send(m); err != nil {
				return err
			}
		}
		if closed {
			return nil
		}
		select {
		case <-o.wake:
		case <-tick.C:
			if err := send(heartbeat); err != nil {
				return err
			}
		case <-stop:
			return errStopped
		}
	}
}

type received[M any] struct {
	msg M
	err error
}

// receive hands each message recv gets to in, up to recv's first error,
// or until stop is closed.
func receive[M any](recv func() (M, error), in chan<- received[M], stop <-chan struct{}) {
	for {
		m, err := recv()
		select {
		case in <- received[M]{m, err}:
		case <-stop:
			return
		}
		if err != nil {
			return
		}
	}
}
