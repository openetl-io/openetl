package sdk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

type fakeReadStream struct {
	grpc.ServerStream
	ctx context.Context

	// Set: each Send says it has started, then waits for a value here.
	started, gate chan struct{}
	// Set: the nth Send, from 1, fails with this.
	failAt  int
	failure error

	mu   sync.Mutex
	sent []*protocolv1.ReadResponse
}

func (f *fakeReadStream) Context() context.Context { return f.ctx }

func (f *fakeReadStream) Send(m *protocolv1.ReadResponse) error {
	if f.gate != nil {
		f.started <- struct{}{}
		<-f.gate
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failure != nil && len(f.sent)+1 == f.failAt {
		return f.failure
	}
	f.sent = append(f.sent, m)
	return nil
}

func (f *fakeReadStream) messages() []*protocolv1.ReadResponse {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
}

func gated() *fakeReadStream {
	return &fakeReadStream{ctx: context.Background(), started: make(chan struct{}, 64), gate: make(chan struct{}, 64)}
}

// batchOf has rows values, all v.
func batchOf(mem memory.Allocator, v int64, rows int) arrow.RecordBatch {
	b := array.NewInt64Builder(mem)
	defer b.Release()
	for range rows {
		b.Append(v)
	}
	a := b.NewArray()
	defer a.Release()
	return array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "v", Type: arrow.PrimitiveTypes.Int64}}, nil), []arrow.Array{a}, int64(rows))
}

func valueOf(t *testing.T, m *protocolv1.ReadResponse) int64 {
	t.Helper()
	rec, err := arrowipc.DecodeBatch(m.GetBatch().GetArrow(), memory.DefaultAllocator)
	if err != nil {
		t.Fatal(err)
	}
	defer rec.Release()
	return rec.Column(0).(*array.Int64).Value(0)
}

func TestEmitterKeepsOrderAndReleases(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	s := &fakeReadStream{ctx: context.Background()}
	e := newEmitter(s, 4)
	out := e.Split("a", "s1")
	const n = 40
	for i := range n {
		// The earlier batches are the larger, so they're encoded last if
		// nothing keeps the order.
		if err := out.Records(batchOf(mem, int64(i), (n-i)*2000)); err != nil {
			t.Fatal(err)
		}
		if err := out.ResumePoint(fmt.Appendf(nil, "%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := out.Complete([]byte("next")); err != nil {
		t.Fatal(err)
	}
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
	got := s.messages()
	if len(got) != 2*n+1 {
		t.Fatalf("%d messages, want %d", len(got), 2*n+1)
	}
	for i := range n {
		if v := valueOf(t, got[2*i]); v != int64(i) {
			t.Fatalf("message %d carries batch %d", 2*i, v)
		}
		if st := string(got[2*i+1].GetResumePoint().GetState()); st != fmt.Sprint(i) {
			t.Fatalf("resume point %d says %q", i, st)
		}
	}
	if string(got[2*n].GetComplete().GetNext()) != "next" {
		t.Fatalf("last message: %v", got[2*n])
	}
}

func TestEmitterKeepsEachSplitsOrder(t *testing.T) {
	s := &fakeReadStream{ctx: context.Background()}
	e := newEmitter(s, 4)
	var wg sync.WaitGroup
	for split := range 4 {
		wg.Go(func() {
			out := e.Split("a", fmt.Sprint(split))
			for i := range 50 {
				if err := out.Records(batchOf(memory.DefaultAllocator, int64(i), 100)); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
	next := map[string]int64{}
	for _, m := range s.messages() {
		id := m.GetBatch().GetSplit()
		if v := valueOf(t, m); v != next[id] {
			t.Fatalf("split %s: batch %d after %d", id, v, next[id])
		}
		next[id]++
	}
	if len(next) != 4 {
		t.Fatalf("splits: %v", next)
	}
}

// What the caller does with its memory after a call returns doesn't reach
// what's still in the queue.
func TestEmitterKeepsNoneOfTheCallersMemory(t *testing.T) {
	s := gated()
	e := newEmitter(s, 1)
	out := e.Split("a", "s1")

	// The first message holds the sender, so the rest wait in the queue.
	if err := out.ResumePoint([]byte("first")); err != nil {
		t.Fatal(err)
	}
	<-s.started

	row := []byte(`{"a":1}`)
	if err := out.Rows([]json.RawMessage{row}); err != nil {
		t.Fatal(err)
	}
	copy(row, `{"a":2}`)
	state := []byte("state")
	if err := out.ResumePoint(state); err != nil {
		t.Fatal(err)
	}
	copy(state, "XXXXX")

	for range 3 {
		s.gate <- struct{}{}
	}
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
	got := s.messages()
	if r := string(got[1].GetBatch().GetRows().GetRows()[0]); r != `{"a":1}` {
		t.Fatalf("row: %s", r)
	}
	if st := string(got[2].GetResumePoint().GetState()); st != "state" {
		t.Fatalf("state: %s", st)
	}
}

func TestEmitterSendFailure(t *testing.T) {
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)
	broken := errors.New("broken")
	s := &fakeReadStream{ctx: context.Background(), failAt: 2, failure: broken}
	e := newEmitter(s, 1)
	out := e.Split("a", "s1")

	// A later call says so. A batch it refuses stays the caller's.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if time.Now().After(deadline) {
			t.Fatal("no call reported the failure")
		}
		rec := batchOf(mem, 1, 10)
		err := out.Records(rec)
		if err == nil {
			continue
		}
		rec.Release()
		if !errors.Is(err, broken) {
			t.Fatalf("Records: %v", err)
		}
		break
	}
	if err := e.finish(); !errors.Is(err, broken) {
		t.Fatalf("finish: %v", err)
	}
	if n := len(s.messages()); n != 1 {
		t.Fatalf("%d messages sent, want the one before the failure", n)
	}
}

// A read that stops after its last emit still reports a failure to send it.
func TestEmitterFinishReportsAFailure(t *testing.T) {
	broken := errors.New("broken")
	s := &fakeReadStream{ctx: context.Background(), failAt: 1, failure: broken}
	e := newEmitter(s, 1)
	if err := e.Split("a", "s1").Complete(nil); err != nil {
		t.Fatal(err)
	}
	if err := e.finish(); !errors.Is(err, broken) {
		t.Fatalf("finish: %v", err)
	}
}

func TestEmitterFinishWaitsForTheQueue(t *testing.T) {
	s := gated()
	e := newEmitter(s, 1)
	out := e.Split("a", "s1")
	for i := range 3 {
		if err := out.ResumePoint(fmt.Appendf(nil, "%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	finished := make(chan error, 1)
	go func() { finished <- e.finish() }()
	<-s.started
	select {
	case err := <-finished:
		t.Fatalf("finish returned with messages queued: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	for range 3 {
		s.gate <- struct{}{}
	}
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if n := len(s.messages()); n != 3 {
		t.Fatalf("%d messages sent", n)
	}
}

// The queue is bounded: past it a call blocks until the read is cancelled.
func TestEmitterBlocksThenFailsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := gated()
	s.ctx = ctx
	e := newEmitter(s, 1)
	out := e.Split("a", "s1")

	// One being sent and two in the queue.
	for range 3 {
		if err := out.ResumePoint([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	<-s.started

	blocked := make(chan error, 1)
	go func() { blocked <- out.ResumePoint([]byte("y")) }()
	select {
	case err := <-blocked:
		t.Fatalf("a call past the queue returned: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cancel()
	if err := <-blocked; !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked call: %v", err)
	}
	if err := out.ResumePoint([]byte("z")); !errors.Is(err, context.Canceled) {
		t.Fatalf("call after cancelling: %v", err)
	}
	for range 3 {
		s.gate <- struct{}{}
	}
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
}

func TestEmitterAfterTheReadEnds(t *testing.T) {
	s := &fakeReadStream{ctx: context.Background()}
	e := newEmitter(s, 1)
	if err := e.finish(); err != nil {
		t.Fatal(err)
	}
	if err := e.Split("a", "s1").Complete(nil); !errors.Is(err, errReadOver) {
		t.Fatalf("Complete: %v", err)
	}
}
