package conformance

import (
	"context"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// How long a cancelled read, or a connector restarting one, may take.
const (
	stopTimeout    = 10 * time.Second
	restartTimeout = 30 * time.Second
)

// liveRead is a read the check watches as it goes.
type liveRead struct {
	cancel  context.CancelFunc
	mu      sync.Mutex
	events  []event
	err     error
	ended   bool
	changed chan struct{}
	done    chan struct{}
}

func startRead(ctx context.Context, conn grpc.ClientConnInterface, req *protocolv1.ReadRequest) *liveRead {
	ctx, cancel := context.WithCancel(ctx)
	l := &liveRead{cancel: cancel, changed: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(l.done)
		res, err := readWith(ctx, conn, req, func(e event) {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.events = append(l.events, e)
			l.notify()
		})
		if err == nil {
			err = res.err
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		l.err, l.ended = err, true
		l.notify()
	}()
	return l
}

func (l *liveRead) notify() {
	close(l.changed)
	l.changed = make(chan struct{})
}

// wait waits until cond holds for the events so far, the read ends, or
// timeout passes, and reports whether cond holds.
func (l *liveRead) wait(timeout time.Duration, cond func([]event) bool) bool {
	deadline := time.After(timeout)
	for {
		l.mu.Lock()
		ok, ch, ended := cond(l.events), l.changed, l.ended
		l.mu.Unlock()
		if ok || ended {
			return ok
		}
		select {
		case <-ch:
		case <-deadline:
			return false
		}
	}
}

// state is the events so far, and how the read ended, if it has.
func (l *liveRead) state() ([]event, bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.events), l.ended, l.err
}

// stop cancels the read and waits for it to end.
func (l *liveRead) stop() {
	l.cancel()
	<-l.done
}

func eventsOf(events []event, stream string) []event {
	return (&readResult{events: events}).of(stream)
}

// checkpointAt is what a checkpoint taken at events[i] holds: each stream's
// last resume point's state, or where its next sync starts once it has
// completed.
func checkpointAt(events []event, i int) map[string][]byte {
	states := map[string][]byte{}
	for _, e := range events[:i+1] {
		switch {
		case e.resume:
			states[e.stream] = e.state
		case e.complete && e.hasNext:
			states[e.stream] = e.next
		case e.complete:
			delete(states, e.stream)
		}
	}
	return states
}

// lastResume is the index of stream's last resume point in events[:i+1],
// or -1.
func lastResume(events []event, stream string, i int) int {
	for j := i; j >= 0; j-- {
		if events[j].stream == stream && events[j].resume {
			return j
		}
		if events[j].stream == stream && events[j].complete {
			return -1
		}
	}
	return -1
}

func streamRowsAfter(events []event, stream string, i int) []row {
	var out []row
	for _, e := range events[i+1:] {
		if e.stream == stream {
			out = append(out, e.rows...)
		}
	}
	return out
}

func (r *sourceRun) acknowledge(t T, states map[string][]byte) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	_, err := protocolv1.NewSourceServiceClient(r.proc.Conn()).Committed(ctx, &protocolv1.CommittedRequest{States: states, Config: r.proc.Config()})
	return rpcerr.FromStatus(err)
}

// Read: committed: acknowledging a checkpoint, while a read runs and with
// none running, loses nothing after it, and an older one acknowledged again
// does no harm.
func (r *sourceRun) checkCommitted(t T) {
	full, streams := r.fullRead(t)
	if !r.spec.GetSource().GetAcknowledges() {
		t.Skipf("the source doesn't acknowledge, so the platform never calls Committed")
	}
	// A pipeline of its own, since what a source keeps upstream for one,
	// such as a slot, is what acknowledging moves.
	pipeline := r.pipeline()
	ctx, cancel := context.WithTimeout(t.Context(), r.readTimeout())
	defer cancel()
	l := startRead(ctx, r.proc.Conn(), &protocolv1.ReadRequest{Config: r.proc.Config(), Streams: r.declare(t, streams), Pipeline: pipeline})
	defer l.stop()
	first := -1
	l.wait(r.readTimeout(), func(events []event) bool {
		first = slices.IndexFunc(events, func(e event) bool { return e.resume })
		return first >= 0
	})
	// As a runner does once the checkpoint commits, while the read goes on.
	var during error
	if first >= 0 {
		events, _, _ := l.state()
		during = r.acknowledge(t, checkpointAt(events, first))
	}
	l.wait(r.readTimeout(), func([]event) bool { return false })
	events, ended, err := l.state()
	switch {
	case !ended:
		t.Fatalf("a bounded read didn't end within %s", r.readTimeout())
	case err != nil:
		t.Fatalf("read: %s: %v", codeOf(err), err)
	case first < 0:
		t.Skipf("no stream sent a resume point, so there's nothing to acknowledge")
	}
	if during != nil {
		t.Errorf("Committed, while a read ran: %s: %v", codeOf(during), during)
	}
	res := &readResult{events: events}
	for _, cs := range streams {
		if !completed(res.of(cs.GetName())) {
			t.Errorf("%s: the read didn't complete after Committed", cs.GetName())
		}
		if missing := countRows(allRows(full.of(cs.GetName()))).minus(countRows(allRows(res.of(cs.GetName())))); len(missing) > 0 {
			t.Errorf("%s: a read that acknowledged its first checkpoint lost %d of the full read's records: %s", cs.GetName(), len(missing), sample(missing))
		}
	}

	// Checkpoints taken at each stream's resume points, spread over its
	// read, acknowledged in order; after each, the stream whose point it
	// was restarts from it.
	var chosen []int
	modes := map[string]*protocolv1.ConfiguredStream{}
	for _, cs := range streams {
		modes[cs.GetName()] = cs
		var points []int
		for i, e := range events {
			if e.resume && e.stream == cs.GetName() {
				points = append(points, i)
			}
		}
		chosen = append(chosen, spread(points, 3)...)
	}
	slices.Sort(chosen)
	cat := r.discover(t)
	for k, i := range chosen {
		states := checkpointAt(events, i)
		for try := range 2 {
			if err := r.acknowledge(t, states); err != nil {
				t.Fatalf("Committed with checkpoint %d of %d, time %d, with no read running: %s: %v", k+1, len(chosen), try+1, codeOf(err), err)
			}
		}
		name := events[i].stream
		cs := modes[name]
		remaining := countRows(streamRowsAfter(events, name, i))
		st := &protocolv1.ConfiguredStream{Name: name, SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor(), State: states[name]}
		got := countRows(allRows(r.readUnder(t, pipeline, st).of(name)))
		if missing := remaining.minus(got); len(missing) > 0 {
			t.Errorf("%s: restarting from acknowledged checkpoint %d skipped %d records that followed it: %s", name, k+1, len(missing), sample(missing))
		}
		if cat[name].GetReplay() == protocolv1.Replay_REPLAY_EXACT {
			if extra := got.minus(remaining); len(extra) > 0 {
				t.Errorf("%s: the replay is exact, but restarting from acknowledged checkpoint %d read %d records again: %s", name, k+1, len(extra), sample(extra))
			}
		}
	}
	if err := r.acknowledge(t, checkpointAt(events, chosen[0])); err != nil {
		t.Errorf("Committed with an older checkpoint than the last, as a runner may send after a restart: %s: %v", codeOf(err), err)
	}
}

// declare gives streams the types the catalog declares for them, as the
// platform's reads do: without control columns. A stream whose declared
// schema doesn't decode goes without; checkDiscover reports that.
func (r *sourceRun) declare(t T, streams []*protocolv1.ConfiguredStream) []*protocolv1.ConfiguredStream {
	t.Helper()
	cat := r.discover(t)
	out := make([]*protocolv1.ConfiguredStream, len(streams))
	for i, cs := range streams {
		out[i] = cs
		if len(cs.GetArrowSchema()) > 0 {
			continue
		}
		decl, err := arrowipc.DecodeSchema(cat[cs.GetName()].GetArrowSchema())
		if err != nil {
			continue
		}
		fields, _ := source.SplitDeclared(decl)
		b, err := arrowipc.EncodeSchema(fields)
		if err != nil || len(b) == 0 {
			continue
		}
		out[i] = proto.CloneOf(cs)
		out[i].ArrowSchema = b
	}
	return out
}

// readUnder is a bounded read of streams under pipeline.
func (r *sourceRun) readUnder(t T, pipeline string, streams ...*protocolv1.ConfiguredStream) *readResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), r.readTimeout())
	defer cancel()
	res, err := read(ctx, r.proc.Conn(), &protocolv1.ReadRequest{Config: r.proc.Config(), Streams: r.declare(t, streams), Pipeline: pipeline})
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if res.err != nil {
		if ctx.Err() != nil {
			t.Fatalf("a bounded read didn't end within %s", r.readTimeout())
		}
		t.Fatalf("read: %s: %v", codeOf(res.err), res.err)
	}
	return res
}

func changelog(m protocolv1.SyncMode) bool {
	return m == protocolv1.SyncMode_SYNC_MODE_CDC || m == protocolv1.SyncMode_SYNC_MODE_STREAM
}

// Read: continuous: streams read with cdc or stream follow their source
// without completing, read what's added upstream and a resume point after
// it, and a read restarted from the last resume point, under the same
// pipeline once the first is cancelled, misses nothing after it.
func (r *sourceRun) checkContinuous(t T) {
	full, streams := r.fullRead(t)
	var follow []*protocolv1.ConfiguredStream
	for _, cs := range streams {
		if changelog(cs.GetSyncMode()) {
			follow = append(follow, cs)
		}
	}
	if len(follow) == 0 {
		t.Skipf("the suite reads no stream with cdc or stream")
	}
	pipeline := r.pipeline()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	first := startRead(ctx, r.proc.Conn(), &protocolv1.ReadRequest{Config: r.proc.Config(), Streams: r.declare(t, follow), Pipeline: pipeline, Continuous: true})
	defer first.stop()
	if !first.wait(r.readTimeout(), func(events []event) bool { return caughtUp(full, follow, events) }) {
		r.liveFailed(t, first, "a bounded read's records")
	}
	r.addAndFollow(t, first, follow)
	events, _, _ := first.state()
	for _, cs := range follow {
		if completed(eventsOf(events, cs.GetName())) {
			t.Errorf("%s: read with %s, it completed in a continuous read", cs.GetName(), cs.GetSyncMode())
		}
	}
	first.stop()

	// The read restarted from each stream's last resume point.
	var again []*protocolv1.ConfiguredStream
	want := map[string]multiset{}
	for _, cs := range follow {
		j := lastResume(events, cs.GetName(), len(events)-1)
		if j < 0 {
			t.Errorf("%s: a continuous read sent no resume point, so the platform can't commit it", cs.GetName())
			continue
		}
		again = append(again, &protocolv1.ConfiguredStream{Name: cs.GetName(), SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor(), State: events[j].state})
		want[cs.GetName()] = countRows(streamRowsAfter(events, cs.GetName(), j))
	}
	if t.Failed() {
		return
	}
	var second *liveRead
	deadline := time.Now().Add(restartTimeout)
	for {
		second = startRead(ctx, r.proc.Conn(), &protocolv1.ReadRequest{Config: r.proc.Config(), Streams: r.declare(t, again), Pipeline: pipeline, Continuous: true})
		got := second.wait(r.readTimeout(), func(events []event) bool {
			for name, rows := range want {
				if len(rows.minus(countRows(allRows(eventsOf(events, name))))) > 0 {
					return false
				}
			}
			return true
		})
		if got {
			// Time to fail, for a read that starts with nothing to send.
			second.wait(2*time.Second, func([]event) bool { return false })
		}
		_, ended, err := second.state()
		got = got && !ended
		retryable := codeOf(err) == protocolv1.ErrorCode_ERROR_CODE_TRANSIENT || codeOf(err) == protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED
		if got || !ended || !retryable || time.Now().After(deadline) {
			if !got {
				second.stop()
				r.liveFailed(t, second, "the records that followed the last resume point, restarted from it")
			}
			break
		}
		second.stop()
		time.Sleep(time.Second)
	}
	defer second.stop()
	r.addAndFollow(t, second, again)
}

// Read: continuous splits: a continuous read with one lane reads every split
// of each stream read with cdc or stream that the source splits, everything
// a bounded read of the streams just before had, and what's added upstream
// after: such splits don't complete to free a lane, so none may wait for one.
func (r *sourceRun) checkContinuousSplits(t T) {
	_, streams := r.fullRead(t)
	if !r.spec.GetSource().GetSplits() {
		t.Skipf("the source doesn't split streams")
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var follow []*protocolv1.ConfiguredStream
	for _, cs := range streams {
		if !changelog(cs.GetSyncMode()) {
			continue
		}
		stream := &protocolv1.ConfiguredStream{Name: cs.GetName(), SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor()}
		resp, err := protocolv1.NewSourceServiceClient(r.proc.Conn()).Splits(ctx, &protocolv1.SplitsRequest{Config: r.proc.Config(), Stream: stream})
		if err != nil {
			err = rpcerr.FromStatus(err)
			t.Fatalf("Splits(%s): %s: %v", cs.GetName(), codeOf(err), err)
		}
		if len(resp.GetSplits()) > 1 {
			stream.Splits = resp.GetSplits()
			follow = append(follow, stream)
		}
	}
	if len(follow) == 0 {
		t.Skipf("the source splits no stream the suite reads with cdc or stream")
	}
	// What's upstream now, which the checks before may have added to.
	var whole []*protocolv1.ConfiguredStream
	for _, cs := range follow {
		whole = append(whole, &protocolv1.ConfiguredStream{Name: cs.GetName(), SyncMode: cs.GetSyncMode(), Cursor: cs.GetCursor()})
	}
	upstream := r.readUnder(t, r.pipeline(), whole...)
	l := startRead(ctx, r.proc.Conn(), &protocolv1.ReadRequest{Config: r.proc.Config(), Streams: r.declare(t, follow), Pipeline: r.pipeline(), Continuous: true, Lanes: 1})
	defer l.stop()
	if !l.wait(r.readTimeout(), func(events []event) bool { return caughtUp(upstream, follow, events) }) {
		r.liveFailed(t, l, "every split's records, on one lane")
	}
	// Time to end, for a read that ends once it has caught up.
	l.wait(2*time.Second, func([]event) bool { return false })
	if _, ended, err := l.state(); ended && err != nil {
		t.Fatalf("the continuous read failed after it read every split's records: %s: %v", codeOf(err), err)
	} else if ended {
		t.Fatalf("the continuous read ended after it read every split's records")
	}
	r.addAndFollow(t, l, follow)
}

// caughtUp reports whether events hold every record the bounded read had of
// each stream.
func caughtUp(full *readResult, streams []*protocolv1.ConfiguredStream, events []event) bool {
	for _, cs := range streams {
		if len(countRows(allRows(full.of(cs.GetName()))).minus(countRows(allRows(eventsOf(events, cs.GetName()))))) > 0 {
			return false
		}
	}
	return true
}

// liveFailed fails a check whose continuous read didn't read what it
// waited for.
func (r *sourceRun) liveFailed(t T, l *liveRead, what string) {
	t.Helper()
	_, ended, err := l.state()
	switch {
	case ended && err != nil:
		t.Fatalf("the continuous read failed before it read %s: %s: %v", what, codeOf(err), err)
	case ended:
		t.Fatalf("the continuous read ended before it read %s", what)
	}
	t.Fatalf("within %s, the continuous read didn't read %s", r.readTimeout(), what)
}

// addAndFollow adds records upstream to each stream, and checks the read
// gets them, and in each split that gets some, a resume point after them.
func (r *sourceRun) addAndFollow(t T, l *liveRead, streams []*protocolv1.ConfiguredStream) {
	t.Helper()
	if r.s.AddRecords == nil {
		return
	}
	for _, cs := range streams {
		name := cs.GetName()
		before, _, _ := l.state()
		seen := countRows(allRows(eventsOf(before, name)))
		n, err := r.s.AddRecords(t.Context(), name)
		if err != nil {
			t.Fatalf("AddRecords(%s): %v", name, err)
		}
		if n == 0 {
			continue
		}
		arrived := l.wait(r.readTimeout(), func(events []event) bool {
			ev := eventsOf(events, name)
			fresh := novel(allRows(ev), seen)
			return len(fresh) >= n && unresumed(ev, seen) == nil
		})
		if !arrived {
			events, ended, err := l.state()
			ev := eventsOf(events, name)
			fresh := novel(allRows(ev), seen)
			switch {
			case ended && err != nil:
				t.Fatalf("%s: the continuous read failed after records were added: %s: %v", name, codeOf(err), err)
			case len(fresh) < n:
				t.Errorf("%s: %d records were added upstream, and within %s the continuous read read %d of them", name, n, r.readTimeout(), len(fresh))
			default:
				t.Errorf("%s: no resume point of %s followed the records added upstream within %s, so the platform can't commit them", name, splitNames(unresumed(ev, seen)), r.readTimeout())
			}
		}
	}
}

// unresumed are the splits of a stream's events with records not in seen
// whose last batch no resume point of theirs follows; the whole stream is
// the empty split.
func unresumed(events []event, seen multiset) []string {
	fresh, open := map[string]bool{}, map[string]bool{}
	for _, e := range events {
		switch {
		case e.resume:
			open[e.split] = false
		case e.batch && len(e.rows) > 0:
			open[e.split] = true
			fresh[e.split] = fresh[e.split] || len(novel(e.rows, seen)) > 0
		}
	}
	var out []string
	for split, f := range fresh {
		if f && open[split] {
			out = append(out, split)
		}
	}
	slices.Sort(out)
	return out
}

func splitNames(splits []string) string {
	names := make([]string, len(splits))
	for i, s := range splits {
		names[i] = "split " + strconv.Quote(s)
		if s == "" {
			names[i] = "the whole stream"
		}
	}
	return strings.Join(names, ", ")
}
