package attempt

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/protobuf/proto"

	attemptv1 "github.com/openetl-io/openetl/gen/openetl/attempt/v1"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func splitRead() *attemptv1.Call {
	return &attemptv1.Call{Call: &attemptv1.Call_SplitRead{SplitRead: &attemptv1.SplitRead{Streams: []*attemptv1.EndedRead{{Stream: "events"}}}}}
}

// callAsync makes a call, and hands on its answer, or nil if Call failed.
func callAsync(t *testing.T, c *coordinator, worker int, call *attemptv1.Call) <-chan *attemptv1.Answer {
	ch := make(chan *attemptv1.Answer, 1)
	go func() {
		a, err := c.Call(t.Context(), worker, call)
		if err != nil {
			a = nil
		}
		ch <- a
	}()
	return ch
}

// A worker answers each call once, before its assignment or after.
func TestCallsAreAnsweredOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinatorFor(t, testAttemptOf(2), Options{})
		answered := callAsync(t, c, 1, splitRead())
		ws := []*worker{joinPipe(t, c, target(0), Options{}), joinPipe(t, c, target(1), Options{})}
		call := request(t, ws[1]).GetCall()
		if call.GetId() == 0 || call.GetSplitRead() == nil {
			t.Fatalf("worker 1's call: %v", call)
		}
		if err := ws[0].Answer(&attemptv1.Answer{Id: call.GetId()}); err == nil {
			t.Fatal("worker 0 answered worker 1's call")
		}
		synctest.Wait()
		if isDone(c.Done()) {
			t.Fatal("an answer not sent failed the attempt")
		}
		ans := &attemptv1.Answer{Id: call.GetId(), Answer: &attemptv1.Answer_ReadSplits{ReadSplits: &attemptv1.ReadSplits{Resplits: true}}}
		if err := ws[1].Answer(ans); err != nil {
			t.Fatal(err)
		}
		if got := within(t, answered, time.Second, "the answer"); !proto.Equal(got, ans) {
			t.Fatalf("answer %v, want %v", got, ans)
		}
		if err := ws[1].Answer(ans); err == nil {
			t.Fatal("a call answered twice")
		}
		if err := c.Assign(t.Context(), layout(2)); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			request(t, w)
		}
		// Its answer's error is the call's, not Call's.
		failed := callAsync(t, c, 0, splitRead())
		call = request(t, ws[0]).GetCall()
		e := &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE}
		if err := ws[0].Answer(&attemptv1.Answer{Id: call.GetId(), Error: e}); err != nil {
			t.Fatal(err)
		}
		if got := within(t, failed, time.Second, "the answer"); got.GetError().GetCode() != e.GetCode() {
			t.Fatalf("answer %v", got)
		}
		if _, err := c.Call(t.Context(), 2, splitRead()); err == nil {
			t.Fatal("a call to worker 2 of 2")
		}
		if _, err := c.Call(t.Context(), 0, &attemptv1.Call{}); err == nil {
			t.Fatal("an empty call")
		}
	})
}

// Signals, schema proposals and usage reach the coordinator's inbox in each
// worker's order, and the coordinator's schemas reach one worker or all.
func TestSignalsProposalsAndSchemas(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := coordinatorFor(t, testAttemptOf(2), Options{})
		ws := []*worker{joinPipe(t, c, target(0), Options{}), joinPipe(t, c, target(1), Options{})}
		placed := &attemptv1.Signal{After: testFirst, Signal: &attemptv1.Signal_Placed{Placed: &attemptv1.Placed{}}}
		proposal := &attemptv1.SchemaProposal{Stream: "events", Base: 0, ArrowSchema: []byte("schema")}
		schema := &attemptv1.StreamSchema{Stream: "events", Version: 1, ArrowSchema: []byte("schema")}
		usage := &attemptv1.Usage{CpuSeconds: 2, PeakMemoryBytes: 1 << 20, Rows: 10}
		if err := ws[0].Report(usage); err == nil {
			t.Fatal("a usage report before the assignment")
		}
		if err := ws[0].Signal(placed); err == nil {
			t.Fatal("a signal before the assignment")
		}
		if err := ws[0].Propose(proposal); err == nil {
			t.Fatal("a proposal before the assignment")
		}
		if err := c.Schema(-1, schema); err == nil {
			t.Fatal("a schema before the assignment")
		}
		if err := c.Assign(t.Context(), layout(2)); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			request(t, w)
		}
		if err := ws[0].Signal(placed); err == nil {
			t.Fatal("a signal after a barrier not requested")
		}
		if err := c.Barrier(testFirst, 1<<20); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			if b := request(t, w).GetBarrier(); b.GetMaxBytes() != 1<<20 {
				t.Fatalf("barrier %v", b)
			}
		}
		emitted := &attemptv1.Signal{After: testFirst - 1, Signal: &attemptv1.Signal_Emitted{Emitted: &attemptv1.Emitted{}}}
		for _, err := range []error{ws[1].Signal(emitted), ws[1].Propose(proposal), ws[1].Signal(placed), ws[0].Report(usage), ws[0].Signal(placed)} {
			if err != nil {
				t.Fatal(err)
			}
		}
		want := []From{{Worker: 1, Signal: emitted}, {Worker: 1, Proposal: proposal}, {Worker: 1, Signal: placed}, {Worker: 0, Usage: usage}, {Worker: 0, Signal: placed}}
		var got []From
		for range want {
			got = append(got, within(t, c.Inbox(), time.Second, "the inbox"))
		}
		// Each worker's in its order; the workers' between them in any.
		slicesByWorker := func(fs []From, w int) []From {
			var out []From
			for _, f := range fs {
				if f.Worker == w {
					out = append(out, f)
				}
			}
			return out
		}
		for w := range 2 {
			g, x := slicesByWorker(got, w), slicesByWorker(want, w)
			if len(g) != len(x) {
				t.Fatalf("worker %d's inbox: %v, want %v", w, g, x)
			}
			for i := range g {
				if !proto.Equal(g[i].Signal, x[i].Signal) || !proto.Equal(g[i].Proposal, x[i].Proposal) || !proto.Equal(g[i].Usage, x[i].Usage) {
					t.Fatalf("worker %d's inbox: %v, want %v", w, g, x)
				}
			}
		}
		if err := c.Schema(1, schema); err != nil {
			t.Fatal(err)
		}
		if got := request(t, ws[1]).GetSchema(); !proto.Equal(got, schema) {
			t.Fatalf("worker 1's schema: %v", got)
		}
		if err := c.Schema(-1, schema); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			if got := request(t, w).GetSchema(); !proto.Equal(got, schema) {
				t.Fatalf("schema %v", got)
			}
		}
	})
}

// An answer to a call never made, or a signal before the assignment, fails
// the attempt; a call sent twice, or a schema before the assignment, fails
// the worker.
func TestCallsAndSignalsOutOfTheProtocol(t *testing.T) {
	for name, send := range map[string]*attemptv1.AttendRequest{
		"an answer to no call": {Message: &attemptv1.AttendRequest_Answer{Answer: &attemptv1.Answer{Id: 7}}},
		"a signal before the assignment": {Message: &attemptv1.AttendRequest_Signal{Signal: &attemptv1.Signal{
			After: testFirst - 1, Signal: &attemptv1.Signal_Full{Full: &attemptv1.Full{}},
		}}},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := coordinatorFor(t, testAttemptOf(1), Options{})
				r := raw(t, c)
				r.join(t, target(0))
				r.send(t, send)
				within(t, c.Done(), time.Second, "the attempt's failure")
				if err := c.Err(); !errors.Is(err, ErrProtocol) {
					t.Fatalf("attempt: %v", err)
				}
				r.p.hangUp()
			})
		})
	}
	for name, msgs := range map[string][]*attemptv1.AttendResponse{
		"a call sent twice": {
			{Message: &attemptv1.AttendResponse_Call{Call: &attemptv1.Call{Id: 1, Call: splitRead().GetCall()}}},
			{Message: &attemptv1.AttendResponse_Call{Call: &attemptv1.Call{Id: 1, Call: splitRead().GetCall()}}},
		},
		"a schema before the assignment": {
			{Message: &attemptv1.AttendResponse_Schema{Schema: &attemptv1.StreamSchema{Stream: "events", Version: 1}}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				rc, w := rawStart(t, target(0), Options{})
				if rc.next(t).GetJoin() == nil {
					t.Fatal("no join")
				}
				rc.send(t, coordinatorHeartbeat)
				for _, m := range msgs {
					rc.send(t, m)
				}
				within(t, w.Done(), time.Second, "the worker's end")
				if err := w.Err(); !errors.Is(err, ErrProtocol) {
					t.Fatalf("worker: %v", err)
				}
			})
		})
	}
}
