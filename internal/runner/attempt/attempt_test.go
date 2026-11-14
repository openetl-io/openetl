package attempt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// start2 opens a two-worker attempt and assigns it layout(2).
func start2(t *testing.T, opts Options) (*coordinator, []*worker) {
	t.Helper()
	c := coordinatorFor(t, testAttemptOf(2), opts)
	ws := []*worker{joinPipe(t, c, target(0), opts), joinPipe(t, c, target(1), opts)}
	if err := c.Assign(t.Context(), layout(2)); err != nil {
		t.Fatal(err)
	}
	want := layout(2)
	want.Workers = []string{"w0:7720", "w1:7720"}
	for i, w := range ws {
		if a := request(t, w).GetAssignment(); !proto.Equal(a, want) {
			t.Fatalf("worker %d's assignment: %v, want %v", i, a, want)
		}
	}
	return c, ws
}

func request(t *testing.T, w *worker) *attemptv1.AttendResponse {
	t.Helper()
	select {
	case m, ok := <-w.Requests():
		if !ok {
			t.Fatalf("the worker's part ended: %v", w.Err())
		}
		return m
	case <-time.After(time.Hour):
		t.Fatal("no request")
		return nil
	}
}

// commitAsync asks worker for req, and hands on the answer, or nil if
// Commit failed.
func commitAsync(t *testing.T, c *coordinator, worker int, req *attemptv1.Commit) <-chan *protocolv1.WriteCommitted {
	ch := make(chan *protocolv1.WriteCommitted, 1)
	go func() {
		a, err := c.Commit(t.Context(), worker, req)
		if err != nil {
			a = nil
		}
		ch <- a
	}()
	return ch
}

// answer waits for commitAsync's answer.
func answer(t *testing.T, ch <-chan *protocolv1.WriteCommitted) *protocolv1.WriteCommitted {
	t.Helper()
	a := within(t, ch, time.Hour, "the commit's answer")
	if a == nil {
		t.Fatal("Commit failed")
	}
	return a
}

func TestAnEpochFromBarrierToPositions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c, ws := start2(t, Options{})

		// Heartbeats keep a quiet attempt going.
		time.Sleep(10 * DefaultTimeout)
		synctest.Wait()
		if isDone(c.Done()) || isDone(ws[0].Done()) || isDone(ws[1].Done()) {
			t.Fatalf("a quiet attempt ended: %v, %v, %v", c.Err(), ws[0].Err(), ws[1].Err())
		}

		if err := c.Barrier(testFirst); err != nil {
			t.Fatal(err)
		}
		for i, w := range ws {
			if e := request(t, w).GetBarrier().GetEpoch(); !proto.Equal(e, epoch(testFirst)) {
				t.Fatalf("worker %d's barrier: %v", i, e)
			}
		}
		sent := []*attemptv1.Share{shareOf(testFirst, 0, true), shareOf(testFirst, 1, false)}
		for i, w := range ws {
			if err := w.Share(sent[i]); err != nil {
				t.Fatal(err)
			}
		}
		got, err := c.Shares(ctx, testFirst)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.EqualFunc(got, sent, func(a, b *attemptv1.Share) bool { return proto.Equal(a, b) }) {
			t.Fatalf("shares: %v, want %v", got, sent)
		}

		// Worker 0 holds a committable only it can commit.
		commit := commitOf(epoch(testFirst), 2, 0)
		answered := commitAsync(t, c, 0, commit)
		if got := request(t, ws[0]).GetCommit(); !proto.Equal(got, commit) {
			t.Fatalf("commit: %v, want %v", got, commit)
		}
		// Asked again while the worker commits, it's answered once.
		again := commitAsync(t, c, 0, commit)
		synctest.Wait()
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err == nil {
			t.Fatal("positions before the commit was answered")
		}
		// A failed commit is answered, and asked again.
		failure := committedOf(commit, &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE, Message: "broker down"})
		if err := ws[0].Committed(failure); err != nil {
			t.Fatal(err)
		}
		for _, ch := range []<-chan *protocolv1.WriteCommitted{answered, again} {
			if a := answer(t, ch); !proto.Equal(a, failure.GetCommitted()) {
				t.Fatalf("answer: %v", a)
			}
		}
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err == nil {
			t.Fatal("positions after the commit failed")
		}
		answered = commitAsync(t, c, 0, commit)
		request(t, ws[0])
		done := committedOf(commit, nil)
		done.Committed.CollapsedRows = map[string]uint64{"events": 2}
		if err := ws[0].Committed(done); err != nil {
			t.Fatal(err)
		}
		if a := answer(t, answered); !proto.Equal(a, done.GetCommitted()) {
			t.Fatalf("answer: %v", a)
		}
		// Asked again once it's committed, the worker's side answers again.
		if a := answer(t, commitAsync(t, c, 0, commit)); !proto.Equal(a, done.GetCommitted()) {
			t.Fatalf("answer again: %v", a)
		}

		// Positions: the restored record's, of an earlier generation, then
		// epoch 5's.
		restored := &attemptv1.Positions{Epoch: epochOf(testGen-1, testFirst-1)}
		for _, s := range layout(2).GetSplits() {
			restored.Splits = append(restored.Splits, s.GetSplit())
		}
		committed := &attemptv1.Positions{Epoch: epoch(testFirst), Splits: append(sent[0].GetPositions(), sent[1].GetPositions()...)}
		for _, p := range []*attemptv1.Positions{restored, committed} {
			if err := c.Acknowledge(p); err != nil {
				t.Fatal(err)
			}
		}
		for i, w := range ws {
			for _, want := range []*attemptv1.Positions{restored, committed} {
				if got := request(t, w).GetPositions(); !proto.Equal(got, want) {
					t.Fatalf("worker %d's positions: %v, want %v", i, got, want)
				}
			}
		}

		if err := c.End(ctx, nil); err != nil {
			t.Fatal(err)
		}
		if c.Err() != nil {
			t.Fatalf("after an orderly end: %v", c.Err())
		}
		for i, w := range ws {
			within(t, w.Done(), time.Hour, "the worker's end")
			if w.Err() != nil {
				t.Fatalf("worker %d after an orderly end: %v", i, w.Err())
			}
			if _, ok := <-w.Requests(); ok {
				t.Fatalf("worker %d's requests are open after the end", i)
			}
			_ = w.Close()
		}
		if err := c.Barrier(testFirst + 1); !errors.Is(err, ErrOver) {
			t.Fatalf("a barrier after the end: %v", err)
		}
	})
}

func TestOneWorkerAppliesEveryWorkersCommittables(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c, ws := start2(t, Options{})
		if err := c.Barrier(testFirst); err != nil {
			t.Fatal(err)
		}
		var sent []*attemptv1.Share
		for i, w := range ws {
			request(t, w)
			sent = append(sent, shareOf(testFirst, i, false))
			if err := w.Share(sent[i]); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Shares(ctx, testFirst); err != nil {
			t.Fatal(err)
		}
		// Worker 1 applies both writer lanes' staging, worker 0's too.
		apply := commitOf(epoch(testFirst), 2)
		apply.Commit.Staged = [][]byte{sent[0].GetCommittables()[0].GetStaged(), sent[1].GetCommittables()[0].GetStaged()}
		answered := commitAsync(t, c, 1, apply)
		if got := request(t, ws[1]).GetCommit(); !proto.Equal(got, apply) {
			t.Fatalf("commit: %v, want %v", got, apply)
		}
		synctest.Wait()
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err == nil {
			t.Fatal("positions before the apply was answered")
		}
		if err := ws[1].Committed(committedOf(apply, nil)); err != nil {
			t.Fatal(err)
		}
		if a := answer(t, answered); a.GetError() != nil {
			t.Fatalf("answer: %v", a)
		}
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err != nil {
			t.Fatal(err)
		}
		if err := c.End(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			_ = w.Close()
		}
	})
}

func TestARestoredEpochIsSettledBeforeTheAssignment(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c := coordinatorFor(t, testAttemptOf(2), Options{})
		// The record restored is an earlier attempt's, of three writer
		// lanes, none of which this attempt's workers staged: worker 0
		// applies them all, and worker 1 settles the third's on its own,
		// before either is joined or assigned.
		restored := epochOf(testGen-1, testFirst-1)
		whole, third := commitOf(restored, 3), commitOf(restored, 3, 2)
		applied, settled := commitAsync(t, c, 0, whole), commitAsync(t, c, 1, third)
		synctest.Wait()
		ws := []*worker{joinPipe(t, c, target(0), Options{}), joinPipe(t, c, target(1), Options{})}
		for i, req := range []*attemptv1.Commit{whole, third} {
			if got := request(t, ws[i]).GetCommit(); !proto.Equal(got, req) {
				t.Fatalf("worker %d's commit: %v, want %v", i, got, req)
			}
		}
		if err := ws[1].Committed(committedOf(third, nil)); err != nil {
			t.Fatal(err)
		}
		answer(t, settled)
		// Worker 0's apply fails, and is asked again.
		if err := ws[0].Committed(committedOf(whole, &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE})); err != nil {
			t.Fatal(err)
		}
		answer(t, applied)
		if err := c.Assign(ctx, layout(2)); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			if request(t, w).GetAssignment() == nil {
				t.Fatal("no assignment")
			}
		}
		positions := &attemptv1.Positions{Epoch: restored}
		if err := c.Acknowledge(positions); err == nil {
			t.Fatal("positions of the restored record before it was settled")
		}
		applied = commitAsync(t, c, 0, whole)
		request(t, ws[0])
		if err := ws[0].Committed(committedOf(whole, nil)); err != nil {
			t.Fatal(err)
		}
		answer(t, applied)
		if err := c.Acknowledge(positions); err != nil {
			t.Fatal(err)
		}
		for name, req := range map[string]*attemptv1.Commit{
			"past the restored record":  commitOf(epochOf(testGen-1, testFirst), 3, 2),
			"of a later generation":     commitOf(epochOf(testGen+1, 1), 3, 2),
			"of no epoch":               commitOf(epochOf(testGen-1, 0), 3, 2),
			"of a writer lane it lacks": commitOf(restored, 3, 3),
			"of a writer lane twice":    commitOf(restored, 3, 1, 1),
		} {
			if _, err := c.Commit(ctx, 0, req); err == nil {
				t.Fatalf("a commit %s", name)
			}
		}
		if err := c.End(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			_ = w.Close()
		}
	})
}

func TestAnotherAttemptOrGenerationIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinatorFor(t, testAttemptOf(1), Options{})
		for _, tg := range []Target{
			{Attempt: "attempt-0", Generation: testGen},
			{Attempt: testAttempt, Generation: testGen - 1},
		} {
			w := joinPipe(t, c, tg, Options{})
			within(t, w.Done(), time.Hour, "the refusal")
			if err := w.Err(); !errors.Is(err, ErrRefused) || errs.CodeOf(err) != errs.Fenced {
				t.Fatalf("%v: %v", tg, err)
			}
			_ = w.Close()
		}
		synctest.Wait()
		if isDone(c.Done()) {
			t.Fatalf("a stale worker failed the attempt: %v", c.Err())
		}
		w := joinPipe(t, c, target(0), Options{})
		if err := c.Assign(t.Context(), layout(1)); err != nil {
			t.Fatal(err)
		}
		if request(t, w).GetAssignment() == nil {
			t.Fatal("no assignment")
		}
		if err := c.End(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
	})
}

// A call refused before the join goes out fails the join's Send with io.EOF
// as Recv returns the refusal; the worker reads it as a refusal, not a lost
// coordinator, whichever it sees first.
func TestARefusalThatEndsTheCallBeforeTheJoin(t *testing.T) {
	for range 32 {
		synctest.Test(t, func(t *testing.T) {
			p := newPipe()
			p.end(status.Error(codes.Unauthenticated, "attempt: no token"))
			w := start(workerEnd{p}, p.hangUp, target(0), Options{})
			within(t, w.Done(), time.Hour, "the refusal")
			if err := w.Err(); !errors.Is(err, ErrRefused) || errs.CodeOf(err) != errs.Fenced {
				t.Fatalf("worker: %v", err)
			}
			_ = w.Close()
		})
	}
}

func TestAWorkerThatJoinsTwiceFailsTheAttempt(t *testing.T) {
	for _, again := range []int{0, 2} {
		t.Run(fmt.Sprintf("as worker %d", again), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := coordinatorFor(t, testAttemptOf(2), Options{})
				w0 := joinPipe(t, c, target(0), Options{})
				synctest.Wait()
				dup := joinPipe(t, c, target(again), Options{})
				within(t, dup.Done(), time.Hour, "the refusal")
				if !errors.Is(dup.Err(), ErrRefused) {
					t.Fatalf("worker %d again: %v", again, dup.Err())
				}
				within(t, c.Done(), time.Hour, "the attempt's failure")
				if !errors.Is(c.Err(), ErrProtocol) {
					t.Fatalf("attempt: %v", c.Err())
				}
				within(t, w0.Done(), time.Hour, "worker 0's end")
				if !errors.Is(w0.Err(), ErrAborted) {
					t.Fatalf("worker 0: %v", w0.Err())
				}
				_ = w0.Close()
				_ = dup.Close()
			})
		})
	}
}

func TestAWorkerThatNeverJoinsFailsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		opts := Options{JoinTimeout: time.Minute}
		c := coordinatorFor(t, testAttemptOf(2), opts)
		w0 := joinPipe(t, c, target(0), opts)
		assigned := make(chan error, 1)
		go func() { assigned <- c.Assign(t.Context(), layout(2)) }()
		time.Sleep(time.Minute - time.Second)
		synctest.Wait()
		if isDone(c.Done()) {
			t.Fatalf("failed before the join timeout: %v", c.Err())
		}
		time.Sleep(2 * time.Second)
		synctest.Wait()
		if !isDone(c.Done()) || !errors.Is(c.Err(), ErrLost) || !strings.Contains(c.Err().Error(), "worker 1: it didn't join within 1m0s") {
			t.Fatalf("after the join timeout: %v", c.Err())
		}
		if err := within(t, assigned, time.Hour, "Assign"); !errors.Is(err, c.Err()) {
			t.Fatalf("Assign: %v", err)
		}
		within(t, w0.Done(), time.Hour, "worker 0's end")
		if err := w0.Err(); !errors.Is(err, ErrAborted) || errs.CodeOf(err) != errs.Transient {
			t.Fatalf("worker 0: %v", err)
		}
		_ = w0.Close()
	})
}

func TestAWorkerLostMidEpochFailsTheAttempt(t *testing.T) {
	t.Run("silent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c := coordinatorFor(t, testAttemptOf(2), Options{})
			w0 := joinPipe(t, c, target(0), Options{})
			r1 := raw(t, c)
			r1.join(t, target(1))
			synctest.Wait()
			from := time.Now()
			if err := c.Assign(t.Context(), layout(2)); err != nil {
				t.Fatal(err)
			}
			r1.next(t)
			request(t, w0)
			if err := c.Barrier(testFirst); err != nil {
				t.Fatal(err)
			}
			request(t, w0)
			r1.next(t)
			if err := w0.Share(shareOf(testFirst, 0, false)); err != nil {
				t.Fatal(err)
			}
			gathered := make(chan error, 1)
			go func() {
				_, err := c.Shares(t.Context(), testFirst)
				gathered <- err
			}()
			time.Sleep(DefaultTimeout - time.Since(from) - time.Millisecond)
			synctest.Wait()
			if isDone(c.Done()) {
				t.Fatalf("failed before the timeout: %v", c.Err())
			}
			time.Sleep(2 * time.Millisecond)
			synctest.Wait()
			if err := c.Err(); !errors.Is(err, ErrLost) || !strings.Contains(err.Error(), "worker 1") {
				t.Fatalf("after the timeout: %v", err)
			}
			if err := within(t, gathered, time.Hour, "Shares"); !errors.Is(err, c.Err()) {
				t.Fatalf("Shares: %v", err)
			}
			// The lost worker's call ends, saying why.
			for range r1.msgs {
			}
			if err := <-r1.err; status.Code(err) != codes.Aborted || !strings.Contains(err.Error(), "nothing from it for 10s") {
				t.Fatalf("worker 1's call: %v", err)
			}
			within(t, w0.Done(), time.Hour, "worker 0's end")
			if !errors.Is(w0.Err(), ErrAborted) {
				t.Fatalf("worker 0: %v", w0.Err())
			}
			_ = w0.Close()
			r1.p.hangUp()
		})
	})
	t.Run("call broken", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, ws := start2(t, Options{})
			if err := c.Barrier(testFirst); err != nil {
				t.Fatal(err)
			}
			ws[1].hangUp()
			within(t, c.Done(), time.Second, "the attempt's failure")
			if err := c.Err(); !errors.Is(err, ErrLost) || !strings.Contains(err.Error(), "worker 1") {
				t.Fatalf("attempt: %v", err)
			}
			within(t, ws[0].Done(), time.Second, "worker 0's end")
			for _, w := range ws {
				_ = w.Close()
			}
		})
	})
	t.Run("before its commit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			c, ws := start2(t, Options{})
			if err := c.Barrier(testFirst); err != nil {
				t.Fatal(err)
			}
			for i, w := range ws {
				request(t, w)
				if err := w.Share(shareOf(testFirst, i, i == 1)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := c.Shares(t.Context(), testFirst); err != nil {
				t.Fatal(err)
			}
			answered := make(chan error, 1)
			go func() {
				_, err := c.Commit(t.Context(), 1, commitOf(epoch(testFirst), 2, 1))
				answered <- err
			}()
			request(t, ws[1])
			ws[1].hangUp()
			if err := within(t, answered, time.Second, "Commit"); !errors.Is(err, ErrLost) {
				t.Fatalf("Commit: %v", err)
			}
			for _, w := range ws {
				_ = w.Close()
			}
		})
	})
}

func TestAShareOutOfTheProtocolFailsTheAttempt(t *testing.T) {
	for name, tc := range map[string]struct {
		share func() *attemptv1.Share
		says  string
	}{
		"of an epoch not requested": {func() *attemptv1.Share { return shareOf(testFirst+1, 0, false) }, "shared epoch 6"},
		"of another generation": {func() *attemptv1.Share {
			s := shareOf(testFirst, 0, false)
			s.Epoch = epochOf(testGen-1, testFirst)
			return s
		}, "generation 2"},
		"of the restored epoch": {func() *attemptv1.Share { return shareOf(testFirst-1, 0, false) }, "again"},
		"whose engine's epoch is another": {func() *attemptv1.Share {
			s := shareOf(testFirst, 0, false)
			s.Ready.Epoch = epoch(testFirst - 1)
			return s
		}, "its engine's"},
		"without its committable": {func() *attemptv1.Share {
			s := shareOf(testFirst, 0, false)
			s.Committables = nil
			return s
		}, "0 of its 1"},
		"with another writer lane's committable": {func() *attemptv1.Share {
			s := shareOf(testFirst, 0, false)
			s.Committables[0].WriterLane = 1
			return s
		}, "isn't its"},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := coordinatorFor(t, testAttemptOf(1), Options{})
				r := raw(t, c)
				r.join(t, target(0))
				if err := c.Assign(t.Context(), layout(1)); err != nil {
					t.Fatal(err)
				}
				r.next(t)
				if err := c.Barrier(testFirst); err != nil {
					t.Fatal(err)
				}
				r.next(t)
				r.share(t, tc.share())
				within(t, c.Done(), time.Second, "the attempt's failure")
				if err := c.Err(); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), tc.says) {
					t.Fatalf("attempt: %v", err)
				}
				if end := r.next(t).GetEnd(); end.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
					t.Fatalf("the worker's end: %v", end)
				}
				r.p.hangUp()
			})
		})
	}
}

func TestADuplicateShareFailsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinatorFor(t, testAttemptOf(2), Options{})
		r := raw(t, c)
		r.join(t, target(0))
		w1 := joinPipe(t, c, target(1), Options{})
		if err := c.Assign(t.Context(), layout(2)); err != nil {
			t.Fatal(err)
		}
		r.next(t)
		if err := c.Barrier(testFirst); err != nil {
			t.Fatal(err)
		}
		r.next(t)
		r.share(t, shareOf(testFirst, 0, false))
		r.share(t, shareOf(testFirst, 0, false))
		within(t, c.Done(), time.Second, "the attempt's failure")
		if err := c.Err(); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "worker 0 shared epoch 5 again") {
			t.Fatalf("attempt: %v", err)
		}
		within(t, w1.Done(), time.Second, "worker 1's end")
		_ = w1.Close()
		r.p.hangUp()
	})
}

func TestAWorkersFailureFailsTheAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, ws := start2(t, Options{})
		ws[1].Fail(errs.New(errs.SchemaDrift, "column v changed type"))
		within(t, c.Done(), time.Second, "the attempt's failure")
		if err := c.Err(); errs.CodeOf(err) != errs.SchemaDrift || !strings.Contains(err.Error(), "worker 1 failed: SCHEMA_DRIFT: column v changed type") {
			t.Fatalf("attempt: %v", err)
		}
		within(t, ws[0].Done(), time.Second, "worker 0's end")
		if err := ws[0].Err(); !errors.Is(err, ErrAborted) || errs.CodeOf(err) != errs.SchemaDrift {
			t.Fatalf("worker 0: %v", err)
		}
		if err := c.End(t.Context(), nil); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			_ = w.Close()
		}
	})
}

func TestEndWithACause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, ws := start2(t, Options{})
		cause := errs.New(errs.Fenced, "HEAD is generation 4")
		if err := c.End(t.Context(), cause); err != nil {
			t.Fatal(err)
		}
		if !errors.Is(c.Err(), cause) {
			t.Fatalf("attempt: %v", c.Err())
		}
		for i, w := range ws {
			within(t, w.Done(), time.Second, "the worker's end")
			if err := w.Err(); !errors.Is(err, ErrAborted) || errs.CodeOf(err) != errs.Fenced {
				t.Fatalf("worker %d: %v", i, err)
			}
			_ = w.Close()
		}
	})
}

func TestACoordinatorLost(t *testing.T) {
	assign := func(t *testing.T, r *rawCoordinator, w *worker) {
		t.Helper()
		if j := r.next(t).GetJoin(); j.GetWorker() != 0 || j.GetAttempt() != testAttempt || j.GetGeneration() != testGen || j.GetAddress() != "w0:7720" {
			t.Fatalf("join: %v", j)
		}
		a := layout(1)
		a.Workers = []string{"w0:7720"}
		r.send(t, &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Assignment{Assignment: a}})
		if request(t, w).GetAssignment() == nil {
			t.Fatal("no assignment")
		}
	}
	t.Run("silent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r, w := rawStart(t, target(0), Options{})
			assign(t, r, w)
			from := time.Now()
			time.Sleep(DefaultTimeout - time.Millisecond)
			synctest.Wait()
			if isDone(w.Done()) {
				t.Fatalf("ended before the timeout: %v", w.Err())
			}
			time.Sleep(2 * time.Millisecond)
			synctest.Wait()
			if err := w.Err(); !errors.Is(err, ErrLost) || errs.CodeOf(err) != errs.Transient {
				t.Fatalf("after %s: %v", time.Since(from), err)
			}
			if _, ok := <-w.Requests(); ok {
				t.Fatal("requests are open")
			}
			_ = w.Close()
		})
	})
	t.Run("call broken", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			r, w := rawStart(t, target(0), Options{})
			assign(t, r, w)
			r.p.end(errors.New("connection reset"))
			within(t, w.Done(), time.Second, "the worker's end")
			if err := w.Err(); !errors.Is(err, ErrLost) {
				t.Fatalf("worker: %v", err)
			}
			_ = w.Close()
		})
	})
	t.Run("never assigns", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			opts := Options{JoinTimeout: time.Minute}
			r, w := rawStart(t, target(0), opts)
			r.next(t)
			beat := &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Heartbeat{Heartbeat: &attemptv1.Heartbeat{}}}
			beating := make(chan struct{})
			go func() {
				defer close(beating)
				for !isDone(w.Done()) {
					_ = coordinatorEnd{r.p}.Send(beat)
					time.Sleep(DefaultHeartbeat)
				}
			}()
			if err := w.awaitTaken(t.Context()); err != nil {
				t.Fatal(err)
			}
			within(t, w.Done(), time.Hour, "the worker's end")
			if err := w.Err(); !errors.Is(err, ErrLost) || !strings.Contains(err.Error(), "no assignment within 1m10s") {
				t.Fatalf("worker: %v", err)
			}
			_ = w.Close()
			<-beating
		})
	})
}

func TestACoordinatorOutOfTheProtocol(t *testing.T) {
	msg := func(m any) *attemptv1.AttendResponse {
		switch x := m.(type) {
		case *attemptv1.Barrier:
			return &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Barrier{Barrier: x}}
		case *attemptv1.Positions:
			return &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Positions{Positions: x}}
		case *attemptv1.Commit:
			return &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Commit{Commit: x}}
		case *attemptv1.Assignment:
			return &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Assignment{Assignment: x}}
		}
		return &attemptv1.AttendResponse{}
	}
	assignment := layout(1)
	assignment.Workers = []string{"w0:7720"}
	for name, tc := range map[string]struct {
		assigned bool
		send     *attemptv1.AttendResponse
	}{
		"a barrier before the assignment":         {false, msg(&attemptv1.Barrier{Epoch: epoch(testFirst)})},
		"positions before the assignment":         {false, msg(&attemptv1.Positions{Epoch: epochOf(testGen-1, testFirst-1)})},
		"a barrier out of order":                  {true, msg(&attemptv1.Barrier{Epoch: epoch(testFirst + 1)})},
		"a barrier of another generation":         {true, msg(&attemptv1.Barrier{Epoch: epochOf(testGen+1, testFirst)})},
		"positions of an epoch not shared":        {true, msg(&attemptv1.Positions{Epoch: epoch(testFirst)})},
		"positions of a later generation":         {true, msg(&attemptv1.Positions{Epoch: epochOf(testGen+1, 1)})},
		"a commit of an epoch not shared":         {true, msg(commitOf(epoch(testFirst), 1))},
		"a commit of its epoch before assignment": {false, msg(commitOf(epoch(testFirst), 1))},
		"a commit of a later generation":          {false, msg(commitOf(epochOf(testGen+1, 1), 1))},
		"a commit past the restored record":       {true, msg(commitOf(epochOf(testGen-1, testFirst), 1))},
		"a commit of a writer lane it lacks":      {false, msg(commitOf(epochOf(testGen-1, testFirst-1), 1, 1))},
		"a second assignment":                     {true, msg(assignment)},
		"an assignment without the worker":        {false, msg(&attemptv1.Assignment{FirstEpoch: testFirst})},
		"an empty message":                        {true, msg(nil)},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, w := rawStart(t, target(0), Options{})
				r.next(t)
				if tc.assigned {
					r.send(t, msg(assignment))
				}
				r.send(t, tc.send)
				if f := r.next(t).GetFailure(); f.GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
					t.Fatalf("failure: %v", f)
				}
				within(t, w.Done(), time.Second, "the worker's end")
				if !errors.Is(w.Err(), ErrProtocol) {
					t.Fatalf("worker: %v", w.Err())
				}
				r.p.end(nil)
				_ = w.Close()
			})
		})
	}
}

func TestTheWorkerKeepsTheProtocol(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		r, w := rawStart(t, target(0), Options{})
		r.next(t)
		if err := w.Share(shareOf(testFirst, 0, false)); err == nil {
			t.Fatal("a share before the assignment")
		}
		a := layout(2)
		a.Workers = []string{"w0:7720", "w1:7720"}
		r.send(t, &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Assignment{Assignment: a}})
		request(t, w)
		if err := w.Share(shareOf(testFirst, 0, false)); err == nil {
			t.Fatal("a share before its barrier")
		}
		r.send(t, &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Barrier{Barrier: &attemptv1.Barrier{Epoch: epoch(testFirst)}}})
		request(t, w)
		for name, s := range map[string]*attemptv1.Share{
			"of the next epoch":            shareOf(testFirst+1, 0, false),
			"of worker 1's writer lane":    shareOf(testFirst, 1, false),
			"without its committable":      {Epoch: epoch(testFirst)},
			"of another generation":        {Epoch: epochOf(testGen-1, testFirst)},
			"whose engine's is another's":  {Epoch: epoch(testFirst), Ready: shareOf(testFirst+1, 0, false).GetReady(), Committables: shareOf(testFirst, 0, false).GetCommittables()},
			"with two of its committables": {Epoch: epoch(testFirst), Committables: append(shareOf(testFirst, 0, false).GetCommittables(), shareOf(testFirst, 0, false).GetCommittables()...)},
		} {
			if err := w.Share(s); err == nil {
				t.Fatalf("a share %s", name)
			}
		}
		if err := w.Share(shareOf(testFirst, 0, true)); err != nil {
			t.Fatal(err)
		}
		if s := r.next(t).GetShare(); !proto.Equal(s, shareOf(testFirst, 0, true)) {
			t.Fatalf("share: %v", s)
		}
		if err := w.Share(shareOf(testFirst, 0, true)); err == nil {
			t.Fatal("a share again")
		}
		commit := commitOf(epoch(testFirst), 2, 0)
		answer := committedOf(commit, nil)
		if err := w.Committed(answer); err == nil {
			t.Fatal("an answer before a commit")
		}
		sendCommit := func() {
			r.send(t, &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_Commit{Commit: commit}})
		}
		sendCommit()
		request(t, w)
		// Asked again before it answered, the request isn't repeated, and
		// nothing is answered on receipt.
		sendCommit()
		synctest.Wait()
		select {
		case m := <-w.Requests():
			t.Fatalf("a request again: %v", m)
		case m := <-r.msgs:
			t.Fatalf("an answer before the commit: %v", m)
		default:
		}
		if err := w.Committed(committedOf(commitOf(epoch(testFirst), 2, 1), nil)); err == nil {
			t.Fatal("an answer for another writer lane")
		}
		if err := w.Committed(answer); err != nil {
			t.Fatal(err)
		}
		if c := r.next(t).GetCommitted(); !proto.Equal(c, answer) {
			t.Fatalf("answer: %v", c)
		}
		if err := w.Committed(answer); err == nil {
			t.Fatal("an answer twice")
		}
		// Asked again once it's committed, the worker's side answers again.
		sendCommit()
		if c := r.next(t).GetCommitted(); !proto.Equal(c, answer) {
			t.Fatalf("answer again: %v", c)
		}
		// An orderly end: the worker closes its side, then the coordinator
		// ends the call.
		r.send(t, &attemptv1.AttendResponse{Message: &attemptv1.AttendResponse_End{End: &attemptv1.End{}}})
		within(t, w.Done(), time.Second, "the worker's end")
		if err := within(t, r.err, time.Second, "the worker's side"); !errors.Is(err, io.EOF) {
			t.Fatalf("the worker's side ended with %v", err)
		}
		if err := w.Share(shareOf(testFirst+1, 0, false)); !errors.Is(err, ErrOver) {
			t.Fatalf("a share after the end: %v", err)
		}
		r.p.end(nil)
		_ = w.Close()
	})
}

func TestTheCoordinatorKeepsTheProtocol(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c := coordinatorFor(t, testAttemptOf(2), Options{})
		if err := c.Barrier(testFirst); err == nil {
			t.Fatal("a barrier before the assignment")
		}
		bad := layout(2)
		bad.Lanes[1].Worker = 2
		if err := c.Assign(ctx, bad); err == nil {
			t.Fatal("an assignment to a third worker")
		}
		ws := []*worker{joinPipe(t, c, target(0), Options{}), joinPipe(t, c, target(1), Options{})}
		if err := c.Assign(ctx, layout(2)); err != nil {
			t.Fatal(err)
		}
		if err := c.Assign(ctx, layout(2)); err == nil {
			t.Fatal("a second assignment")
		}
		for _, w := range ws {
			request(t, w)
		}
		for _, n := range []uint64{testFirst - 1, testFirst + 1} {
			if err := c.Barrier(n); err == nil {
				t.Fatalf("barrier %d first", n)
			}
		}
		if _, err := c.Shares(ctx, testFirst); err == nil {
			t.Fatal("shares before their barrier")
		}
		if err := c.Barrier(testFirst); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Shares(ctx, testFirst+1); err == nil {
			t.Fatal("shares of a later epoch first")
		}
		for i, w := range ws {
			request(t, w)
			if err := w.Share(shareOf(testFirst, i, false)); err != nil {
				t.Fatal(err)
			}
		}
		commit := commitOf(epoch(testFirst), 2, 0)
		if _, err := c.Commit(ctx, 0, commit); err == nil {
			t.Fatal("a commit before the shares were gathered")
		}
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err == nil {
			t.Fatal("positions before the shares were gathered")
		}
		if _, err := c.Shares(ctx, testFirst); err != nil {
			t.Fatal(err)
		}
		for name, tc := range map[string]struct {
			worker int
			commit *attemptv1.Commit
		}{
			"for a third worker":        {2, commit},
			"of a later epoch":          {0, commitOf(epoch(testFirst+1), 2)},
			"of a later generation":     {0, commitOf(epochOf(testGen+1, 1), 2)},
			"of an earlier generation":  {0, commitOf(epochOf(testGen-1, testFirst), 2)},
			"without every committable": {0, commitOf(epoch(testFirst), 1)},
		} {
			if _, err := c.Commit(ctx, tc.worker, tc.commit); err == nil {
				t.Fatalf("a commit %s", name)
			}
		}
		for name, e := range map[string]*protocolv1.Epoch{
			"of a later generation":                   epochOf(testGen+1, 1),
			"of an epoch not gathered":                epoch(testFirst + 1),
			"of an earlier generation past the first": epochOf(testGen-1, testFirst),
		} {
			if err := c.Acknowledge(&attemptv1.Positions{Epoch: e}); err == nil {
				t.Fatalf("positions %s", name)
			}
		}
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err != nil {
			t.Fatal(err)
		}
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epochOf(testGen-1, testFirst-1)}); err == nil {
			t.Fatal("positions going back")
		}
		// A commit given up on is still asked for.
		gone, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := c.Commit(gone, 0, commit); !errors.Is(err, context.Canceled) {
			t.Fatalf("a commit given up on: %v", err)
		}
		synctest.Wait()
		if isDone(c.Done()) {
			t.Fatalf("misuse failed the attempt: %v", c.Err())
		}
		if err := c.End(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			_ = w.Close()
		}
	})
}

func TestAJoinDeadlineThatFiresAtOnce(t *testing.T) {
	for range 2000 {
		c := newCoordinator(testAttemptOf(1), Options{JoinTimeout: time.Nanosecond}, func() {})
		<-c.Done()
		if !errors.Is(c.Err(), ErrLost) {
			t.Fatalf("attempt: %v", c.Err())
		}
	}
}

func TestAWorkerTalkingPastTheEndIsCutOff(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinatorFor(t, testAttemptOf(1), Options{})
		r := raw(t, c)
		r.join(t, target(0))
		synctest.Wait()
		stop := make(chan struct{})
		talked := make(chan struct{})
		go func() {
			defer close(talked)
			for {
				select {
				case <-stop:
					return
				case <-time.After(DefaultHeartbeat):
					_ = workerEnd{r.p}.Send(workerHeartbeat)
				}
			}
		}()
		ended := make(chan error, 1)
		go func() { ended <- c.End(t.Context(), nil) }()
		err := within(t, ended, 2*DefaultTimeout, "End")
		close(stop)
		<-talked
		r.p.hangUp()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestACommitAskedAgainAfterItsPositions(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		c, ws := start2(t, Options{})
		if err := c.Barrier(testFirst); err != nil {
			t.Fatal(err)
		}
		for i, w := range ws {
			request(t, w)
			if err := w.Share(shareOf(testFirst, i, i == 0)); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Shares(ctx, testFirst); err != nil {
			t.Fatal(err)
		}
		commit := commitOf(epoch(testFirst), 2, 0)
		answered := commitAsync(t, c, 0, commit)
		request(t, ws[0])
		if err := ws[0].Committed(committedOf(commit, nil)); err != nil {
			t.Fatal(err)
		}
		answer(t, answered)
		if err := c.Acknowledge(&attemptv1.Positions{Epoch: epoch(testFirst)}); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			request(t, w)
		}
		// The worker's side answers it as committed, and its caller doesn't
		// see it again.
		if a := answer(t, commitAsync(t, c, 0, commit)); a.GetError() != nil || a.GetEpoch().GetN() != testFirst {
			t.Fatalf("answer again: %v", a)
		}
		select {
		case m := <-ws[0].Requests():
			t.Fatalf("the commit reached the worker's caller again: %v", m)
		default:
		}
		if err := c.End(ctx, nil); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			_ = w.Close()
		}
	})
}
