package attempt

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

func listen(t *testing.T) *Server {
	t.Helper()
	s, err := Listen(t.Context(), ListenConfig{Address: "127.0.0.1:0"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

type joined struct {
	w   Worker
	err error
}

func joinAsync(ctx context.Context, tg Target) <-chan joined {
	ch := make(chan joined, 1)
	go func() {
		w, err := Join(ctx, tg, Options{})
		ch <- joined{w, err}
	}()
	return ch
}

func next(ctx context.Context, t *testing.T, w Worker) *attemptv1.AttendResponse {
	t.Helper()
	select {
	case m, ok := <-w.Requests():
		if !ok {
			t.Fatalf("the worker's part ended: %v", w.Err())
		}
		return m
	case <-ctx.Done():
		t.Fatal(ctx.Err())
		return nil
	}
}

func TestAnAttemptOverTLS(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	s := listen(t)
	c, err := s.Open(testAttemptOf(2), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(testAttemptOf(1), Options{}); err == nil {
		t.Fatal("two attempts with one token")
	}
	var joins []<-chan joined
	for i := range 2 {
		tg := target(i)
		tg.Address, tg.Certificate, tg.Token = s.Address(), s.Certificate(), "secret"
		joins = append(joins, joinAsync(ctx, tg))
	}
	if err := c.Assign(ctx, layout(2)); err != nil {
		t.Fatal(err)
	}
	var ws []Worker
	for i, ch := range joins {
		j := <-ch
		if j.err != nil {
			t.Fatalf("worker %d: %v", i, j.err)
		}
		a := next(ctx, t, j.w).GetAssignment()
		if len(a.GetWorkers()) != 2 || a.GetWorkers()[1] != "w1:7720" || !proto.Equal(a.GetLanes()[i], layout(2).GetLanes()[i]) {
			t.Fatalf("worker %d's assignment: %v", i, a)
		}
		ws = append(ws, j.w)
	}

	if err := c.Barrier(testFirst); err != nil {
		t.Fatal(err)
	}
	for i, w := range ws {
		next(ctx, t, w)
		if err := w.Share(shareOf(testFirst, i, i == 1)); err != nil {
			t.Fatal(err)
		}
	}
	shares, err := c.Shares(ctx, testFirst)
	if err != nil {
		t.Fatal(err)
	}
	for i, s := range shares {
		if !proto.Equal(s, shareOf(testFirst, i, i == 1)) {
			t.Fatalf("worker %d's share: %v", i, s)
		}
	}
	commit := commitOf(epoch(testFirst), 2, 1)
	answered := make(chan error, 1)
	go func() {
		a, err := c.Commit(ctx, 1, commit)
		if err == nil && a.GetError() != nil {
			err = errs.FromProto(a.GetError())
		}
		answered <- err
	}()
	if got := next(ctx, t, ws[1]).GetCommit(); !proto.Equal(got, commit) {
		t.Fatalf("commit: %v", got)
	}
	if err := ws[1].Committed(committedOf(commit, nil)); err != nil {
		t.Fatal(err)
	}
	if err := <-answered; err != nil {
		t.Fatal(err)
	}
	positions := &attemptv1.Positions{Epoch: epoch(testFirst), Splits: shares[0].GetPositions()}
	if err := c.Acknowledge(positions); err != nil {
		t.Fatal(err)
	}
	for _, w := range ws {
		if got := next(ctx, t, w).GetPositions(); !proto.Equal(got, positions) {
			t.Fatalf("positions: %v", got)
		}
	}
	if err := c.End(ctx, nil); err != nil {
		t.Fatal(err)
	}
	for i, w := range ws {
		<-w.Done()
		if w.Err() != nil {
			t.Fatalf("worker %d: %v", i, w.Err())
		}
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestJoinsTheServerRefuses(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	s := listen(t)
	c, err := s.Open(testAttemptOf(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	other := listen(t)
	for name, tc := range map[string]struct {
		change  func(*Target)
		refused bool
	}{
		"with no attempt's token":           {func(tg *Target) { tg.Token = "guess" }, true},
		"of another attempt":                {func(tg *Target) { tg.Attempt = "attempt-0" }, true},
		"of an older generation":            {func(tg *Target) { tg.Generation = testGen - 1 }, true},
		"trusting another certificate":      {func(tg *Target) { tg.Certificate = other.Certificate() }, false},
		"with a certificate that isn't one": {func(tg *Target) { tg.Certificate = []byte("x") }, false},
	} {
		tg := target(0)
		tg.Address, tg.Certificate, tg.Token = s.Address(), s.Certificate(), "secret"
		tc.change(&tg)
		_, err := Join(ctx, tg, Options{})
		t.Logf("a join %s: %v", name, err)
		if err == nil || errors.Is(err, ErrRefused) != tc.refused {
			t.Fatalf("a join %s: %v", name, err)
		}
	}
	if err := c.Err(); err != nil {
		t.Fatalf("refused joins failed the attempt: %v", err)
	}
	if err := c.End(ctx, nil); err != nil {
		t.Fatal(err)
	}
	tg := target(0)
	tg.Address, tg.Certificate, tg.Token = s.Address(), s.Certificate(), "secret"
	if _, err := Join(ctx, tg, Options{}); !errors.Is(err, ErrRefused) {
		t.Fatalf("a join after the attempt ended: %v", err)
	}
}

func TestClosingTheServerEndsItsAttempts(t *testing.T) {
	s := listen(t)
	c, err := s.Open(testAttemptOf(1), Options{})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if !isDone(c.Done()) || errs.CodeOf(c.Err()) != errs.Transient {
		t.Fatalf("the attempt after Close: %v", c.Err())
	}
	other := testAttemptOf(1)
	other.Token = "another"
	if _, err := s.Open(other, Options{}); err == nil {
		t.Fatal("an attempt opened after Close")
	}
}
