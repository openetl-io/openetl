package conformance

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// The most lanes a check opens.
const kitLanes = 3

// lanes is how many lanes the destination's checks open. It skips a
// destination that takes one.
func (r *destinationRun) lanes(t T) int {
	t.Helper()
	r.process(t)
	if r.s.Read == nil {
		t.Fatalf("the suite has no Read")
	}
	most := int(r.spec.GetDestination().GetMaxWriterLanes())
	if most < 2 {
		t.Skipf("the destination takes one writer lane")
	}
	return min(kitLanes, most)
}

// eachLanes runs a check on lanes in every commit scope the destination has.
func (r *destinationRun) eachLanes(check func(T, *session, int)) func(T) {
	return func(t T) {
		n := r.lanes(t)
		for _, scope := range r.spec.GetDestination().GetScopes() {
			check(t, r.session(t, scope, protocolv1.WriteMode_WRITE_MODE_APPEND), n)
		}
	}
}

// attempt is one generation's writers, by lane.
type attempt struct {
	s     *session
	lanes []*writer
}

// recovered opens generation gen on n lanes as an attempt does: lane 0 first,
// which recovers e with its committables, and the others behind its fence.
func (s *session) recovered(gen uint64, n int, e *protocolv1.Epoch, staged [][]byte) (*attempt, protocolv1.Recovery) {
	s.t.Helper()
	w := s.openLane(gen, 0, n)
	if e != nil {
		if err := s.applySchemas(w); err != nil {
			s.t.Fatalf("%s generation %d's apply_schema: %s: %v", s.where(), gen, codeOf(err), err)
		}
		s.handOver(w, e)
	}
	rec, err := w.recoverLanes(e, staged)
	if err != nil {
		s.t.Fatalf("%s generation %d's recover of %s on %d committables: %s: %v", s.where(), gen, epochString(e), len(staged), codeOf(err), err)
	}
	a := &attempt{s: s, lanes: []*writer{w}}
	for lane := 1; lane < n; lane++ {
		a.lanes = append(a.lanes, s.openLane(gen, lane, n))
	}
	return a, rec
}

// attempt is recovered, with every stream read from the session's start.
func (s *session) attempt(gen uint64, n int, e *protocolv1.Epoch, staged [][]byte) (*attempt, protocolv1.Recovery) {
	s.t.Helper()
	a, rec := s.recovered(gen, n, e, staged)
	a.startReads(kitStreams, s.start)
	return a, rec
}

// startReads goes to every lane.
func (a *attempt) startReads(streams []string, at time.Time) {
	a.s.t.Helper()
	for _, w := range a.lanes {
		if err := w.startReads(streams, at); err != nil {
			a.s.t.Fatalf("%s start_reads on lane %d: %v", a.s.where(), w.lane, err)
		}
	}
}

// newRead starts every stream's next read.
func (a *attempt) newRead() {
	a.s.t.Helper()
	a.s.start = a.s.start.Add(time.Second)
	a.startReads(kitStreams, a.s.start)
}

// laneWork is what a lane does in an epoch before its pre-commit: its
// batches, then the ends of the reads it knows of.
type laneWork struct {
	batches []laneBatch
	ended   []string
}

type laneBatch struct {
	stream string
	patch  bool
	// Built as it's sent, so that a lane's later batches have later sequences.
	build func() arrow.RecordBatch
}

func dataBatch(stream string, data []arrow.Field, recs []record) laneBatch {
	return laneBatch{stream: stream, build: func() arrow.RecordBatch { return buildData(stream, data, recs, "insert") }}
}

// onLane is one lane's work: recs, in a batch per stream.
func onLane(recs map[string][]record, ended ...string) laneWork {
	work := laneWork{ended: ended}
	for _, name := range kitStreams {
		if len(recs[name]) > 0 {
			work.batches = append(work.batches, dataBatch(name, dataFields, recs[name]))
		}
	}
	return work
}

// byLanes is the work of n lanes: each version's records on the lane of their
// id, so that a key's versions stay on one lane, in order.
func byLanes(n int, versions ...map[string][]record) []laneWork {
	works := make([]laneWork, n)
	for _, recs := range versions {
		for _, name := range kitStreams {
			byLane := make([][]record, n)
			for _, rec := range recs[name] {
				l := int(rec.id) % n //nolint:gosec // small
				byLane[l] = append(byLane[l], rec)
			}
			for l, rs := range byLane {
				if len(rs) > 0 {
					works[l].batches = append(works[l].batches, dataBatch(name, dataFields, rs))
				}
			}
		}
	}
	return works
}

// stage runs each lane's work on epoch e at once, and returns the lanes'
// committables. Lanes past works do nothing but pre-commit.
func (a *attempt) stage(e *protocolv1.Epoch, works ...laneWork) [][]byte {
	a.s.t.Helper()
	staged, errs := a.tryStage(e, works)
	for i, err := range errs {
		if err != nil {
			a.s.t.Fatalf("%s lane %d's part of epoch %s: %s: %v", a.s.where(), i, epochString(e), codeOf(err), err)
		}
	}
	return staged
}

func (a *attempt) tryStage(e *protocolv1.Epoch, works []laneWork) ([][]byte, []error) {
	staged := make([][]byte, len(a.lanes))
	errs := make([]error, len(a.lanes))
	var wg sync.WaitGroup
	for i, w := range a.lanes {
		var work laneWork
		if i < len(works) {
			work = works[i]
		}
		wg.Go(func() { staged[i], errs[i] = a.s.stageLane(w, e, work) })
	}
	wg.Wait()
	return staged, errs
}

func (s *session) stageLane(w *writer, e *protocolv1.Epoch, work laneWork) ([]byte, error) {
	for i, lb := range work.batches {
		if i > 0 {
			time.Sleep(time.Millisecond)
		}
		b := lb.build()
		var err error
		if lb.patch {
			err = w.patch(e, lb.stream, b)
		} else {
			err = s.write(w, e, lb.stream, b)
		}
		b.Release()
		if err != nil {
			return nil, err
		}
	}
	for _, name := range work.ended {
		if err := w.endRead(e, name); err != nil {
			return nil, err
		}
	}
	return s.preCommit(w, e)
}

// commit applies the schemas on lane 0, and commits e there with every
// lane's committable.
func (a *attempt) commit(e *protocolv1.Epoch, staged [][]byte) {
	a.s.t.Helper()
	if err := a.tryCommit(e, staged); err != nil {
		a.s.t.Fatalf("%s commit %s: %s: %v", a.s.where(), epochString(e), codeOf(err), err)
	}
}

func (a *attempt) tryCommit(e *protocolv1.Epoch, staged [][]byte) error {
	if err := a.s.applySchemas(a.lanes[0]); err != nil {
		return fmt.Errorf("apply_schema: %w", err)
	}
	return a.lanes[0].commitLanes(e, staged)
}

func (a *attempt) crash() {
	for _, w := range a.lanes {
		w.crash()
	}
}

func (a *attempt) close() {
	a.s.t.Helper()
	for _, w := range a.lanes {
		a.s.close(w)
	}
}

// wantRedone is what recovering an epoch that was recorded and not committed
// answers on lanes.
func (s *session) wantRedone(gen uint64, rec protocolv1.Recovery) {
	s.t.Helper()
	if s.r.staged() || s.r.class() == protocolv1.SinkClass_SINK_CLASS_STAGE_AND_APPLY {
		s.wantRecovery(gen, rec, protocolv1.Recovery_RECOVERY_REDONE)
	} else {
		s.wantRecovery(gen, rec, protocolv1.Recovery_RECOVERY_REDONE, protocolv1.Recovery_RECOVERY_APPLIED)
	}
}

// fewer is a lane count other than n, for a next attempt.
func fewer(n int) int { return max(n-1, 1) }

// Lanes: commit: lanes write an epoch concurrently and pre-commit it apart.
// Nothing is visible before Commit, which takes every committable and makes
// the epoch visible once, and a second Commit of it changes nothing.
func (r *destinationRun) checkLanesCommit(_ T, s *session, n int) {
	a, rec := s.attempt(1, n, nil, nil)
	s.wantRecovery(1, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	e1 := map[string][]record{"alpha": records(0, 7, "a"), "beta": records(0, 5, "b")}
	staged := a.stage(epoch(1, 1), byLanes(n, e1)...)
	if r.class() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		s.expect("after every lane's pre-commit, before commit", nil)
	}
	a.commit(epoch(1, 1), staged)
	s.expect("after commit", e1)
	a.commit(epoch(1, 1), staged)
	s.expect("after committing the epoch again", e1)
	e2 := map[string][]record{"alpha": records(7, 12, "a")}
	a.commit(epoch(1, 2), a.stage(epoch(1, 2), byLanes(n, e2)...))
	s.expect("after a second epoch", merged(e1, e2))
	a.close()
}

// Lanes: empty: a lane can be empty in an epoch, a stream can be only on some
// lanes, and every lane can be empty. Recovering such an epoch, on other
// lanes, finds it applied.
func (r *destinationRun) checkLanesEmpty(_ T, s *session, n int) {
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 4, "a")}
	works := make([]laneWork, n)
	works[n-1] = onLane(e1)
	a.commit(epoch(1, 1), a.stage(epoch(1, 1), works...))
	s.expect("after an epoch on one lane", e1)

	e2 := map[string][]record{"alpha": records(4, 6, "a"), "beta": records(0, 2, "b")}
	works = make([]laneWork, n)
	works[0] = onLane(map[string][]record{"alpha": e2["alpha"]})
	works[n-1].batches = append(works[n-1].batches, dataBatch("beta", dataFields, e2["beta"]))
	a.commit(epoch(1, 2), a.stage(epoch(1, 2), works...))
	want := merged(e1, e2)
	s.expect("after an epoch with each stream on its own lane", want)

	staged := a.stage(epoch(1, 3))
	a.commit(epoch(1, 3), staged)
	s.expect("after an epoch with every lane empty", want)
	a.crash()

	b, rec := s.attempt(2, fewer(n), epoch(1, 3), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED, protocolv1.Recovery_RECOVERY_REDONE)
	s.expect("after recovering an epoch with every lane empty", want)
	e4 := map[string][]record{"beta": records(2, 5, "b")}
	b.commit(epoch(2, 4), b.stage(epoch(2, 4), byLanes(fewer(n), e4)...))
	want = merged(want, e4)
	s.expect("after the next attempt's epoch", want)

	e5 := map[string][]record{"alpha": records(6, 8, "a")}
	works = make([]laneWork, fewer(n))
	works[fewer(n)-1] = onLane(e5)
	staged = b.stage(epoch(2, 5), works...)
	b.crash()
	c, rec := s.attempt(3, n, epoch(2, 5), staged)
	s.wantRedone(3, rec)
	s.expect("after redoing an epoch on one lane", merged(want, e5))
	c.close()
}

// Lanes: overwrite: a read's rows on any lane replace the table when it ends,
// which any one lane may know, including one with no rows in the epoch, and in
// an epoch with no rows at all. A read without rows empties the table.
func (r *destinationRun) checkLanesOverwrite(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
	n := r.lanes(t)
	for _, scope := range r.spec.GetDestination().GetScopes() {
		s := r.session(t, scope, protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
		a, _ := s.attempt(1, n, nil, nil)

		p1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 1, "b")}
		a.commit(epoch(1, 1), a.stage(epoch(1, 1), onLane(p1)))
		s.expect("while the first read runs", nil)
		p2 := map[string][]record{"alpha": records(3, 4, "a")}
		works := make([]laneWork, n)
		works[0] = onLane(p2)
		works[n-1].ended = kitStreams
		a.commit(epoch(1, 2), a.stage(epoch(1, 2), works...))
		first := merged(p1, p2)
		s.expect("after the first read ended, on a lane that never had its rows", first)

		a.newRead()
		p3 := map[string][]record{"alpha": records(10, 16, "a"), "beta": records(10, 14, "b")}
		a.commit(epoch(1, 3), a.stage(epoch(1, 3), byLanes(n, p3)...))
		s.expect("while the second read runs", first)
		p4 := map[string][]record{"alpha": records(16, 19, "a"), "beta": records(14, 15, "b")}
		works = byLanes(n, p4)
		works[n-1].ended = kitStreams
		a.commit(epoch(1, 4), a.stage(epoch(1, 4), works...))
		second := merged(p3, p4)
		s.expect("after the second read ended, on the last lane alone", second)

		a.newRead()
		third := map[string][]record{"alpha": records(20, 22, "a"), "beta": records(20, 21, "b")}
		a.commit(epoch(1, 5), a.stage(epoch(1, 5), byLanes(n, third)...))
		s.expect("while the third read runs", second)
		works = make([]laneWork, n)
		works[0].ended = kitStreams
		a.commit(epoch(1, 6), a.stage(epoch(1, 6), works...))
		s.expect("after the third read ended, in an epoch without rows", third)

		a.newRead()
		works = make([]laneWork, n)
		works[n-1].ended = kitStreams
		a.commit(epoch(1, 7), a.stage(epoch(1, 7), works...))
		s.expect("after a read without rows", nil)
		a.close()
	}
}

// expectLatest reports how each stream's records differ from want's, the
// later of each id's: one row each, with its latest v.
func (s *session) expectLatest(when string, want map[string][]record) {
	s.t.Helper()
	for _, name := range kitStreams {
		versions := map[string]string{}
		for _, rec := range want[name] {
			versions[fmt.Sprint(rec.id)] = rec.v
		}
		got := map[string]string{}
		for _, row := range s.read(name) {
			id := fmt.Sprint(row["id"])
			if _, dup := got[id]; dup {
				s.t.Errorf("%s %s: %s has id %s twice", s.where(), when, name, id)
			}
			got[id] = fmt.Sprint(row["v"])
		}
		for id, v := range versions {
			if got[id] != v {
				s.t.Errorf("%s %s: %s's id %s has v %q, not %q", s.where(), when, name, id, got[id], v)
			}
		}
		if len(got) != len(versions) {
			s.t.Errorf("%s %s: %s has %d records, not %d", s.where(), when, name, len(got), len(versions))
		}
	}
}

// Lanes: merge: a key's versions stay on one lane while lanes write
// concurrently, its latest wins, and later epochs supersede earlier ones, from
// whichever lane has the key's rows.
func (r *destinationRun) checkLanesMerge(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_MERGE)
	n := r.lanes(t)
	for _, scope := range r.spec.GetDestination().GetScopes() {
		s := r.session(t, scope, protocolv1.WriteMode_WRITE_MODE_MERGE)
		a, _ := s.attempt(1, n, nil, nil)
		v1 := map[string][]record{"alpha": records(0, 6, "old"), "beta": records(0, 3, "old")}
		v2 := map[string][]record{"alpha": records(2, 5, "mid")}
		v3 := map[string][]record{"alpha": records(3, 4, "new")}
		a.commit(epoch(1, 1), a.stage(epoch(1, 1), byLanes(n, v1, v2, v3)...))
		want := merged(v1, v2, v3)
		s.expectLatest("after an epoch with several versions of a key", want)
		time.Sleep(time.Millisecond)
		e2 := map[string][]record{"alpha": records(4, 8, "later"), "beta": records(1, 2, "later")}
		a.commit(epoch(1, 2), a.stage(epoch(1, 2), byLanes(n, e2)...))
		want = merged(want, e2)
		s.expectLatest("after a later epoch", want)
		a.close()
	}
}

// Lanes: crash after pre-commit: every lane staged an epoch that was never
// recorded. The next attempt, on other lanes, finds the one before applied,
// writes the lost one's records again, and each lands once.
func (r *destinationRun) checkLanesCrashAfterPreCommit(_ T, s *session, n int) {
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 5, "a"), "beta": records(0, 3, "b")}
	staged := a.stage(epoch(1, 1), byLanes(n, e1)...)
	a.commit(epoch(1, 1), staged)
	e2 := map[string][]record{"alpha": records(5, 9, "a"), "beta": records(3, 5, "b")}
	a.stage(epoch(1, 2), byLanes(n, e2)...)
	a.crash()

	m := fewer(n)
	b, rec := s.attempt(2, m, epoch(1, 1), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	if r.class() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		s.expect("after recovering the epoch before the crash", e1)
	}
	b.commit(epoch(2, 2), b.stage(epoch(2, 2), byLanes(m, e2)...))
	s.expect("after writing the unrecorded epoch again", merged(e1, e2))
	b.close()
}

// Lanes: crash after the record: an epoch was recorded but not committed, and
// the next attempt has other lanes than the one that staged it, more or fewer.
// Recover takes every committable, and answers for the epoch as a whole.
func (r *destinationRun) checkLanesCrashAfterRecord(_ T, s *session, n int) {
	counts := []int{n, fewer(n), n, 1}
	var want map[string][]record
	var a *attempt
	var e *protocolv1.Epoch
	var staged [][]byte
	var at int64
	for i, m := range counts {
		gen := uint64(i + 1) //nolint:gosec // small
		var rec protocolv1.Recovery
		a, rec = s.attempt(gen, m, e, staged)
		if e != nil {
			s.wantRedone(gen, rec)
			s.expect(fmt.Sprintf("after recovering %d lanes' epoch on %d", counts[i-1], m), want)
		}
		recs := map[string][]record{"alpha": records(at, at+5, "a"), "beta": records(at, at+2, "b")}
		at += 5
		want = merged(want, recs)
		e = epoch(gen, gen)
		staged = a.stage(e, byLanes(m, recs)...)
		a.crash()
	}
	last := uint64(len(counts) + 1) //nolint:gosec // small
	a, rec := s.attempt(last, n, e, staged)
	s.wantRedone(last, rec)
	s.expect("after the last recovery", want)
	a.close()
}

// Lanes: crash during commit: a runner died while lane 0 committed. The epoch
// lands once, whichever of its streams and lanes the commit had applied.
func (r *destinationRun) checkLanesCrashDuringCommit(t T, s *session, n int) {
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 9, "a"), "beta": records(0, 6, "b")}
	staged := a.stage(epoch(1, 1), byLanes(n, e1)...)
	if err := s.applySchemas(a.lanes[0]); err != nil {
		t.Fatalf("%s apply_schema: %s: %v", s.where(), codeOf(err), err)
	}
	if err := a.lanes[0].send(&protocolv1.WriteRequest{Msg: &protocolv1.WriteRequest_Commit{Commit: &protocolv1.WriteCommit{Epoch: epoch(1, 1), Staged: staged}}}); err != nil {
		t.Fatalf("%s sending the commit: %v", s.where(), err)
	}
	time.Sleep(time.Millisecond)
	a.crash()

	b, rec := s.attempt(2, fewer(n), epoch(1, 1), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED, protocolv1.Recovery_RECOVERY_REDONE)
	s.expect("after recovering an epoch cut off in its commit", e1)
	b.close()
}

// Lanes: crash after commit: recovering an epoch that was committed, on other
// lanes, finds it applied and applies nothing again.
func (r *destinationRun) checkLanesCrashAfterCommit(_ T, s *session, n int) {
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 5, "a"), "beta": records(0, 3, "b")}
	staged := a.stage(epoch(1, 1), byLanes(n, e1)...)
	a.commit(epoch(1, 1), staged)
	a.crash()

	m := fewer(n)
	b, rec := s.attempt(2, m, epoch(1, 1), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after recovering a committed epoch", e1)
	e2 := map[string][]record{"beta": records(3, 7, "b")}
	b.commit(epoch(2, 2), b.stage(epoch(2, 2), byLanes(m, e2)...))
	s.expect("after the next epoch", merged(e1, e2))
	b.close()
}

// Lanes: fence: a runner that was replaced can still commit the epoch its
// replacement is recovering, and its lanes can go on writing. The epoch lands
// once, what they wrote never lands, and a commit the destination refuses
// fails with FENCED.
func (r *destinationRun) checkLanesFence(t T, s *session, n int) {
	if r.class() == protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		t.Skipf("deliver-and-acknowledge destinations have no fence")
	}
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 5, "a"), "beta": records(0, 3, "b")}
	staged := a.stage(epoch(1, 1), byLanes(n, e1)...)
	m := fewer(n)
	b, rec := s.attempt(2, m, epoch(1, 1), staged)
	s.wantRedone(2, rec)
	switch err := a.tryCommit(epoch(1, 1), staged); {
	case err == nil:
		flagf(t, "%s generation 1 committed after generation 2 recovered; that's safe only if applying an epoch twice is, as copying to deterministic names is", s.where())
	case codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_FENCED:
		t.Errorf("%s generation 1's commit failed with %s, not FENCED: %v", s.where(), codeOf(err), err)
	}
	stale := map[string][]record{"alpha": records(100, 104, "z"), "beta": records(100, 102, "z")}
	want := e1
	if later, errs := a.tryStage(epoch(1, 2), byLanes(n, stale)); errors.Join(errs...) == nil {
		switch err := a.tryCommit(epoch(1, 2), later); {
		case err == nil:
			flagf(t, "%s generation 1 committed a later epoch after generation 2 recovered; that's safe only if the checkpoint log is enough to stop it, since an epoch is committed once it's recorded", s.where())
			want = merged(e1, stale)
		case codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_FENCED:
			t.Errorf("%s generation 1's commit of its lanes' later epoch failed with %s, not FENCED: %v", s.where(), codeOf(err), err)
		}
	}
	a.crash()
	s.expect("after the replaced runner's lanes wrote", want)
	e2 := map[string][]record{"alpha": records(5, 8, "a")}
	b.commit(epoch(2, 2), b.stage(epoch(2, 2), byLanes(m, e2)...))
	s.expect("after the next epoch", merged(want, e2))
	b.close()
}

// Lanes: overwrite and crashes: a read goes on across attempts on other lanes,
// and its rows on lanes the last attempt doesn't have still replace the table.
// A read that ended on one lane stays, though the others' later committables
// know nothing of its end and the next attempt doesn't read the stream. A read
// the next attempt starts over is dropped.
func (r *destinationRun) checkLanesOverwriteCrash(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
	n := r.lanes(t)
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
	a, _ := s.attempt(1, n, nil, nil)
	first := map[string][]record{"alpha": records(0, 4, "a"), "beta": records(0, 2, "b")}
	works := byLanes(n, first)
	works[n-1].ended = kitStreams
	a.commit(epoch(1, 1), a.stage(epoch(1, 1), works...))
	s.expect("after the first read", first)

	a.newRead()
	p1 := map[string][]record{"alpha": records(10, 14, "a"), "beta": records(10, 12, "b")}
	a.commit(epoch(1, 2), a.stage(epoch(1, 2), byLanes(n, p1)...))
	p2 := map[string][]record{"alpha": records(14, 18, "a"), "beta": records(12, 13, "b")}
	st3 := a.stage(epoch(1, 3), byLanes(n, p2)...)
	a.crash()

	m := fewer(n)
	b, rec := s.attempt(2, m, epoch(1, 3), st3)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_REDONE, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after recovering an epoch of the second read", first)
	p3 := map[string][]record{"alpha": records(18, 20, "a")}
	b.stage(epoch(2, 4), byLanes(m, p3)...)
	b.crash()

	c, rec := s.attempt(3, 1, epoch(1, 3), st3)
	s.wantRecovery(3, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after an epoch of the second read wasn't recorded", first)
	st4 := c.stage(epoch(3, 4), onLane(p3, kitStreams...))
	c.commit(epoch(3, 4), st4)
	second := merged(p1, p2, p3)
	s.expect("after the second read ended, on one lane of the third attempt", second)
	c.crash()

	d, rec := s.attempt(4, n, epoch(3, 4), st4)
	s.wantRecovery(4, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after recovering the epoch the second read ended in", second)

	d.newRead()
	third := map[string][]record{"alpha": records(20, 24, "a"), "beta": records(20, 22, "b")}
	d.commit(epoch(4, 5), d.stage(epoch(4, 5), byLanes(n, third)...))
	works = make([]laneWork, n)
	works[0].ended = kitStreams
	d.commit(epoch(4, 6), d.stage(epoch(4, 6), works...))
	s.expect("after the third read ended, on lane 0 alone", third)
	st7 := d.stage(epoch(4, 7))
	d.commit(epoch(4, 7), st7)
	d.crash()

	f, rec := s.recovered(5, m, epoch(4, 7), st7)
	s.wantRecovery(5, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.start = s.start.Add(time.Second)
	f.startReads(kitStreams[1:], s.start)
	s.expect("after an attempt that doesn't read alpha again", third)
	dropped := map[string][]record{"beta": records(30, 33, "z")}
	st8 := f.stage(epoch(5, 8), byLanes(m, dropped)...)
	f.commit(epoch(5, 8), st8)
	f.crash()

	s.start = s.start.Add(time.Second)
	g, rec := s.attempt(6, n, epoch(5, 8), st8)
	s.wantRecovery(6, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after an attempt starts the read over", third)
	last := map[string][]record{"alpha": records(40, 41, "a"), "beta": records(40, 43, "b")}
	works = byLanes(n, last)
	works[0].ended = kitStreams
	g.commit(epoch(6, 9), g.stage(epoch(6, 9), works...))
	s.expect("after the read that started over ended", last)
	g.close()
}

// Lanes: merge and crashes: an epoch redone on other lanes keeps each key's
// latest, and an attempt on other lanes moves keys between them.
func (r *destinationRun) checkLanesMergeCrash(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_MERGE)
	n := r.lanes(t)
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_MERGE)
	a, _ := s.attempt(1, n, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 6, "one"), "beta": records(0, 3, "one")}
	a.commit(epoch(1, 1), a.stage(epoch(1, 1), byLanes(n, e1)...))
	time.Sleep(time.Millisecond)
	e2a, e2b := map[string][]record{"alpha": records(1, 5, "two")}, map[string][]record{"alpha": records(2, 4, "three")}
	st2 := a.stage(epoch(1, 2), byLanes(n, e2a, e2b)...)
	a.crash()

	m := fewer(n)
	b, rec := s.attempt(2, m, epoch(1, 2), st2)
	s.wantRedone(2, rec)
	want := merged(e1, e2a, e2b)
	s.expectLatest("after redoing an epoch", want)
	time.Sleep(time.Millisecond)
	e3 := map[string][]record{"alpha": records(3, 8, "four"), "beta": records(1, 3, "four")}
	st3 := b.stage(epoch(2, 3), byLanes(m, e3)...)
	b.commit(epoch(2, 3), st3)
	want = merged(want, e3)
	s.expectLatest("after an epoch on other lanes", want)
	e4 := map[string][]record{"alpha": records(0, 2, "five")}
	b.stage(epoch(2, 4), byLanes(m, e4)...)
	b.crash()

	c, rec := s.attempt(3, n, epoch(2, 3), st3)
	s.wantRecovery(3, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expectLatest("after an epoch wasn't recorded", want)
	time.Sleep(time.Millisecond)
	c.commit(epoch(3, 4), c.stage(epoch(3, 4), byLanes(n, e4)...))
	s.expectLatest("after writing it again", merged(want, e4))
	c.close()
}

// expectColumn reports how a column's values differ from want's, by id; ids
// not in want have none.
func (s *session) expectColumn(when, stream, column string, want map[int64]string) {
	s.t.Helper()
	for _, row := range s.read(stream) {
		var id int64
		if _, err := fmt.Sscan(fmt.Sprint(row["id"]), &id); err != nil {
			s.t.Fatalf("%s %s's id %v: %v", s.where(), stream, row["id"], err)
		}
		v, ok := want[id]
		switch got := fmt.Sprint(row[column]); {
		case ok && got != v:
			s.t.Errorf("%s %s: %s's id %d has %s %q, not %q", s.where(), when, stream, id, column, got, v)
		case !ok && row[column] != nil:
			s.t.Errorf("%s %s: %s's id %d has %s %q, and its batch didn't", s.where(), when, stream, id, column, got)
		}
	}
}

// Lanes: schemas: lanes aren't in step. One lane's batches can have a newer
// schema than another's, and a lane can get a batch with an older one after a
// newer one, with fewer columns or narrower types. Every lane's rows land, each
// with its own columns' values, in tables that hold the widest.
func (r *destinationRun) checkLanesSchemas(t T) {
	n := r.lanes(t)
	scope := r.spec.GetDestination().GetScopes()[0]
	columns := func(fields ...arrow.Field) []arrow.Field {
		return append(append([]arrow.Field{}, dataFields...), fields...)
	}
	n32 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true}
	n64 := arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	byID := func(prefix string, from, to int64) map[int64]string {
		out := map[int64]string{}
		for i := from; i < to; i++ {
			out[i] = fmt.Sprintf("%s%d", prefix, i)
		}
		return out
	}

	// Added columns, in tables that don't exist yet.
	s := r.session(t, scope, protocolv1.WriteMode_WRITE_MODE_APPEND)
	a, _ := s.attempt(1, n, nil, nil)
	s.data = columns(wField)
	works := make([]laneWork, n)
	works[0].batches = []laneBatch{dataBatch("alpha", dataFields, records(0, 2, "a"))}
	works[n-1].batches = append(works[n-1].batches,
		dataBatch("alpha", s.data, records(2, 4, "a")),
		dataBatch("alpha", dataFields, records(4, 6, "a")),
		dataBatch("beta", s.data, records(0, 2, "b")))
	a.commit(epoch(1, 1), a.stage(epoch(1, 1), works...))
	want := map[string][]record{"alpha": records(0, 6, "a"), "beta": records(0, 2, "b")}
	s.expect("after lanes on different schemas", want)
	s.expectColumn("after lanes on different schemas", "alpha", "w", byID("w", 2, 4))
	works = make([]laneWork, n)
	works[n-1].batches = []laneBatch{dataBatch("alpha", dataFields, records(6, 8, "a"))}
	a.commit(epoch(1, 2), a.stage(epoch(1, 2), works...))
	s.expect("after an older schema in a later epoch", merged(want, map[string][]record{"alpha": records(6, 8, "a")}))
	s.expectColumn("after an older schema in a later epoch", "alpha", "w", byID("w", 2, 4))
	a.close()

	// Narrower types, in tables that hold the widest.
	full := columns(wField, n64)
	narrow := columns(n32)
	modes := r.spec.GetDestination().GetWriteModes()
	if len(modes) == 0 {
		modes = []protocolv1.WriteMode{protocolv1.WriteMode_WRITE_MODE_APPEND}
	}
	for _, mode := range modes {
		s := r.session(t, scope, mode)
		a, _ := s.attempt(1, n, nil, nil)
		s.data = full
		if err := s.applySchemas(a.lanes[0]); err != nil {
			t.Fatalf("%s apply_schema: %s: %v", s.where(), codeOf(err), err)
		}
		works := make([]laneWork, n)
		works[0].batches = []laneBatch{dataBatch("alpha", narrow, records(0, 2, "a"))}
		a.commit(epoch(1, 1), a.stage(epoch(1, 1), works...))
		works = make([]laneWork, n)
		works[0].batches = []laneBatch{dataBatch("alpha", narrow, records(2, 4, "a"))}
		works[n-1].batches = append(works[n-1].batches,
			dataBatch("alpha", full, records(4, 6, "a")),
			dataBatch("alpha", narrow, records(6, 8, "a")),
			dataBatch("alpha", dataFields, records(8, 10, "a")))
		works[n-1].ended = kitStreams
		a.commit(epoch(1, 2), a.stage(epoch(1, 2), works...))
		want := map[string][]record{"alpha": records(0, 10, "a")}
		when := fmt.Sprintf("after narrower types on lanes, writing %s", mode)
		s.expect(when, want)
		s.expectColumn(when, "alpha", "n", func() map[int64]string {
			out := map[int64]string{}
			for i := range int64(8) {
				out[i] = fmt.Sprint(i)
			}
			return out
		}())
		s.expectColumn(when, "alpha", "w", byID("w", 4, 6))
		a.close()
	}
}

func heldRows(schema *arrow.Schema, recs []held, seq time.Time) laneBatch {
	return laneBatch{stream: "alpha", build: func() arrow.RecordBatch { return heldBatch(schema, "alpha", recs, seq) }}
}

func patches(ps []patch) laneBatch {
	return laneBatch{stream: "alpha", patch: true, build: func() arrow.RecordBatch { return patchBatch("alpha", ps) }}
}

// Lanes: holds: a patch can reach another lane than its row, and applies after
// every lane's rows of the epoch.
func (r *destinationRun) checkLanesHolds(t T) {
	h := r.holds(t)
	n := r.lanes(t)
	mode := protocolv1.WriteMode_WRITE_MODE_APPEND
	if h.GetByKey() && r.supportsMode(protocolv1.WriteMode_WRITE_MODE_MERGE) {
		mode = protocolv1.WriteMode_WRITE_MODE_MERGE
	} else if !h.GetById() {
		t.Skipf("the destination patches only merge, which it doesn't write")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], mode)
	a, _ := s.attempt(1, n, nil, nil)
	s.data = append(append([]arrow.Field{}, dataFields...), heldField)
	schema := heldSchema("alpha", nil)
	t0 := time.Now()
	old1, old2, old3 := record{id: 1, v: "old1"}, record{id: 2, v: "old2"}, record{id: 3, v: "old3"}
	works := make([]laneWork, n)
	works[0].batches = []laneBatch{heldRows(schema, []held{{record: record{id: 0, v: "a0"}}, {record: old1, held: true}}, t0)}
	works[n-1].batches = []laneBatch{heldRows(schema, []held{{record: old2, held: true}}, t0)}
	a.commit(epoch(1, 1), a.stage(epoch(1, 1), works...))
	s.expectVs("with held cells on lanes", map[string]string{"0": "a0", "1": "<null>", "2": "<null>"})

	works = make([]laneWork, n)
	works[0].batches = []laneBatch{heldRows(schema, []held{{record: old3, held: true}}, t0), patches([]patch{{old2, "p2"}})}
	works[n-1].batches = []laneBatch{patches([]patch{{old1, "p1"}, {old3, "p3"}})}
	a.commit(epoch(1, 2), a.stage(epoch(1, 2), works...))
	s.expectVs("after patches on other lanes than their rows", map[string]string{"0": "a0", "1": "p1", "2": "p2", "3": "p3"})
	a.close()
}
