package conformance

import (
	"context"
	"fmt"
	"math"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// DestinationSuite checks a destination connector by writing through it as
// the platform does, crashing where ADR-0013 says a runner can, and reading
// back what it made visible.
type DestinationSuite struct {
	Launch Launcher
	Config []byte
	// A config with credentials the destination rejects. Nil skips that part
	// of the errors check.
	BadCredentials []byte
	// A config whose destination doesn't answer, such as a closed port. Nil
	// skips that part of the errors check.
	Unreachable []byte
	// Read returns the records the destination has made visible for stream
	// in namespace, as readers of the destination see them, each by field
	// name; none for a stream it hasn't created. The checks compare the
	// fields "id", an int64, "v", a string, a soft-deleted row's
	// "_openetl_deleted", a bool, and the columns schemas add, "w", a string,
	// and "n", an int64, by their text, so any Go value that prints the same
	// will do. A cell without a value is nil, or not there.
	Read func(ctx context.Context, namespace, stream string) ([]map[string]any, error)
	// Namespace returns a new namespace for one check. Nil: conformance_
	// and random letters.
	Namespace func() string
	// Drop removes what the checks wrote in namespace, when the suite ends.
	// Nil: nothing.
	Drop func(ctx context.Context, namespace string) error
	// NotNull makes column of stream's table in namespace refuse nulls, as
	// a user's constraint would. Nil skips holds/not-null.
	NotNull func(ctx context.Context, namespace, stream, column string) error
	// How long after a commit the destination's readers can take to see it,
	// as Kafka's consumers can. Zero: they see it at once.
	Settle time.Duration
	// The destination keeps no column types, as a topic of JSON records
	// doesn't, so write/type-changes checks only that a widened column
	// lands.
	Untyped bool
}

// The kit's streams.
var kitStreams = []string{"alpha", "beta"}

// Test runs the suite's checks as subtests of t.
func (s DestinationSuite) Test(t *testing.T) {
	r := newDestinationRun(s)
	t.Cleanup(r.close)
	test(t, r.checks())
}

// Run runs the suite's checks outside go test, reporting each one's result.
func (s DestinationSuite) Run(ctx context.Context, report func(Result)) {
	r := newDestinationRun(s)
	defer r.close()
	run(ctx, r.checks(), report)
}

type destinationRun struct {
	*common
	s          DestinationSuite
	mu         sync.Mutex
	namespaces []string
}

func newDestinationRun(s DestinationSuite) *destinationRun {
	r := &destinationRun{s: s, common: &common{
		launch: s.Launch, kind: protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION,
		config: s.Config, badCredentials: s.BadCredentials, unreachable: s.Unreachable,
		unreachableCodes: []protocolv1.ErrorCode{protocolv1.ErrorCode_ERROR_CODE_DESTINATION_UNAVAILABLE, protocolv1.ErrorCode_ERROR_CODE_TRANSIENT},
	}}
	r.closers = append(r.closers, r.drop)
	return r
}

func (r *destinationRun) checks() []Check {
	return append(r.common.checks(),
		Check{"write/commit", r.each(r.checkCommit)},
		Check{"write/crash-after-precommit", r.each(r.checkCrashAfterPreCommit)},
		Check{"write/crash-after-record", r.each(r.checkCrashAfterRecord)},
		Check{"write/crash-after-commit", r.each(r.checkCrashAfterCommit)},
		Check{"write/fence", r.each(r.checkFence)},
		Check{"write/staged-resume", r.each(r.checkStagedResume)},
		Check{"write/overwrite", r.checkOverwrite},
		Check{"write/merge", r.checkMerge},
		Check{"write/evolution", r.checkEvolution},
		Check{"write/type-changes", r.checkTypeChanges},
		Check{"holds/patch", r.checkHoldsPatch},
		Check{"holds/last", r.checkHoldsLast},
		Check{"holds/soft-delete", r.checkHoldsSoftDelete},
		Check{"holds/not-null", r.checkHoldsNotNull},
		Check{"lanes/commit", r.eachLanes(r.checkLanesCommit)},
		Check{"lanes/empty", r.eachLanes(r.checkLanesEmpty)},
		Check{"lanes/overwrite", r.checkLanesOverwrite},
		Check{"lanes/merge", r.checkLanesMerge},
		Check{"lanes/crash-after-precommit", r.eachLanes(r.checkLanesCrashAfterPreCommit)},
		Check{"lanes/crash-after-record", r.eachLanes(r.checkLanesCrashAfterRecord)},
		Check{"lanes/crash-during-commit", r.eachLanes(r.checkLanesCrashDuringCommit)},
		Check{"lanes/crash-after-commit", r.eachLanes(r.checkLanesCrashAfterCommit)},
		Check{"lanes/fence", r.eachLanes(r.checkLanesFence)},
		Check{"lanes/overwrite-crash", r.checkLanesOverwriteCrash},
		Check{"lanes/merge-crash", r.checkLanesMergeCrash},
		Check{"lanes/schemas", r.checkLanesSchemas},
		Check{"lanes/holds", r.checkLanesHolds},
	)
}

func (r *destinationRun) drop() {
	if r.s.Drop == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	for _, ns := range r.namespaces {
		_ = r.s.Drop(ctx, ns)
	}
}

func (r *destinationRun) class() protocolv1.SinkClass {
	return r.spec.GetDestination().GetSinkClass()
}

func (r *destinationRun) staged() bool {
	return r.spec.GetDestination().GetDelivery() == protocolv1.Delivery_DELIVERY_STAGED
}

// area is where the kit stages a session's epochs for a staged
// destination, served by its process's host services until the next
// session's replaces it, as each pipeline has staging of its own.
func (r *destinationRun) area(t T) *stagingArea {
	t.Helper()
	a, err := attachStaging(r.proc)
	if err != nil {
		t.Fatalf("staging: %v", err)
	}
	r.common.mu.Lock()
	r.closers = append(r.closers, a.close)
	r.common.mu.Unlock()
	return a
}

// each runs a write check in every commit scope the destination has.
func (r *destinationRun) each(check func(T, *session)) func(T) {
	return func(t T) {
		r.process(t)
		if r.s.Read == nil {
			t.Fatalf("the suite has no Read")
		}
		for _, scope := range r.spec.GetDestination().GetScopes() {
			check(t, r.session(t, scope, protocolv1.WriteMode_WRITE_MODE_APPEND))
		}
	}
}

// session is one pipeline writing to a fresh namespace.
type session struct {
	t        T
	r        *destinationRun
	ns       string
	pipeline string
	scope    protocolv1.CommitScope
	mode     protocolv1.WriteMode
	data     []arrow.Field
	// The streams let the destination change a column's type.
	allowTypeChanges bool
	// When reads start, for attempts on lanes.
	start time.Time
	// For a staged destination: where its epochs are staged, the batches of
	// the epoch being written by lane, and each epoch's files by stream, every
	// lane's.
	area    *stagingArea
	mu      sync.Mutex
	pending map[int]map[string][]arrow.RecordBatch
	files   map[string]map[string][]*protocolv1.StagedFile
}

func (r *destinationRun) session(t T, scope protocolv1.CommitScope, mode protocolv1.WriteMode) *session {
	ns := randomName()
	if r.s.Namespace != nil {
		ns = r.s.Namespace()
	}
	r.mu.Lock()
	r.namespaces = append(r.namespaces, ns)
	r.mu.Unlock()
	s := &session{t: t, r: r, ns: ns, pipeline: newID(), scope: scope, mode: mode, data: dataFields, start: time.Now()}
	if r.staged() {
		s.area, s.pending, s.files = r.area(t), map[int]map[string][]arrow.RecordBatch{}, map[string]map[string][]*protocolv1.StagedFile{}
		t.Cleanup(func() {
			for _, streams := range s.pending {
				for _, bs := range streams {
					for _, b := range bs {
						b.Release()
					}
				}
			}
		})
	}
	return s
}

func (s *session) where() string {
	return fmt.Sprintf("[%s scope]", scopeName(s.scope))
}

func scopeName(s protocolv1.CommitScope) string {
	if s == protocolv1.CommitScope_COMMIT_SCOPE_STREAM {
		return "stream"
	}
	return "pipeline"
}

func (s *session) open(gen uint64) *writer {
	s.t.Helper()
	return s.openLane(gen, 0, 1)
}

// openLane opens lane of an attempt's lanes.
func (s *session) openLane(gen uint64, lane, lanes int) *writer {
	s.t.Helper()
	open := &protocolv1.WriteOpen{
		Config: s.r.proc.Config(), Pipeline: s.pipeline, Namespace: s.ns, Generation: gen, Scope: s.scope,
		CommitInterval: durationpb.New(time.Second),
	}
	if lane > 0 && lane <= math.MaxUint32 {
		open.Lane = uint32(lane)
	}
	for _, name := range kitStreams {
		open.Streams = append(open.Streams, &protocolv1.WriteStream{Name: name, Mode: s.mode, PrimaryKey: []string{"id"}, AllowTypeChanges: s.allowTypeChanges})
	}
	w, err := openWriter(s.t.Context(), s.r.proc.Conn(), open)
	if err != nil {
		s.t.Fatalf("%s open generation %d, lane %d: %s: %v", s.where(), gen, lane, codeOf(err), err)
	}
	w.lane, w.lanes = lane, lanes
	s.t.Cleanup(w.crash)
	return w
}

// begin opens generation gen and recovers e, as an attempt does before its
// first batch.
func (s *session) begin(gen uint64, e *protocolv1.Epoch, staged []byte) (*writer, protocolv1.Recovery) {
	s.t.Helper()
	w := s.open(gen)
	if e != nil {
		// A redone epoch needs its schemas in place.
		if err := s.applySchemas(w); err != nil {
			s.t.Fatalf("%s generation %d's apply_schema: %s: %v", s.where(), gen, codeOf(err), err)
		}
		// A staged epoch's files are handed over again, as the runner does.
		s.handOver(w, e)
	}
	rec, err := w.recover(e, staged)
	if err != nil {
		s.t.Fatalf("%s generation %d's recover of %s: %s: %v", s.where(), gen, epochString(e), codeOf(err), err)
	}
	if err := w.startReads(kitStreams, time.Now()); err != nil {
		s.t.Fatalf("%s start_reads: %v", s.where(), err)
	}
	return w, rec
}

// stage writes epoch e's records and pre-commits it. Streams in ended end
// their reads in e.
func (s *session) stage(w *writer, e *protocolv1.Epoch, recs map[string][]record, ended ...string) []byte {
	s.t.Helper()
	for _, name := range kitStreams {
		if len(recs[name]) == 0 {
			continue
		}
		b := buildBatch(name, recs[name], "insert")
		err := s.write(w, e, name, b)
		b.Release()
		if err != nil {
			s.t.Fatalf("%s a batch of epoch %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
		}
	}
	for _, name := range ended {
		if err := w.endRead(e, name); err != nil {
			s.t.Fatalf("%s end_read: %v", s.where(), err)
		}
	}
	staged, err := s.preCommit(w, e)
	if err != nil {
		s.t.Fatalf("%s pre-commit %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
	}
	return staged
}

// write sends a batch of epoch e, or for a staged destination, keeps it for
// e's files.
func (s *session) write(w *writer, e *protocolv1.Epoch, stream string, b arrow.RecordBatch) error {
	if s.area == nil {
		return w.batch(e, stream, b)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pending[w.lane] == nil {
		s.pending[w.lane] = map[string][]arrow.RecordBatch{}
	}
	for i := int64(0); i < b.NumRows(); i += recordsPerFile {
		s.pending[w.lane][stream] = append(s.pending[w.lane][stream], b.NewSlice(i, min(i+recordsPerFile, b.NumRows())))
	}
	return nil
}

// preCommit pre-commits e. For a staged destination, it first stages what
// write kept as e's files, and hands them over.
func (s *session) preCommit(w *writer, e *protocolv1.Epoch) ([]byte, error) {
	if s.area != nil {
		s.mu.Lock()
		pending := s.pending[w.lane]
		s.mu.Unlock()
		files := map[string][]*protocolv1.StagedFile{}
		for _, name := range kitStreams {
			bs := pending[name]
			if len(bs) == 0 {
				continue
			}
			fs, err := s.area.stage(e, name, w.lane, w.lanes, bs)
			for _, b := range bs {
				b.Release()
			}
			delete(pending, name)
			if err != nil {
				return nil, err
			}
			files[name] = fs
		}
		s.mu.Lock()
		if s.files[epochString(e)] == nil {
			s.files[epochString(e)] = map[string][]*protocolv1.StagedFile{}
		}
		for name, fs := range files {
			s.files[epochString(e)][name] = append(s.files[epochString(e)][name], fs...)
		}
		s.mu.Unlock()
		if err := handFiles(w, e, files); err != nil {
			return nil, err
		}
	}
	return w.preCommit(e)
}

// handOver sends e's staged files, every lane's, stream by stream.
func (s *session) handOver(w *writer, e *protocolv1.Epoch) {
	s.t.Helper()
	if s.area == nil {
		return
	}
	s.mu.Lock()
	files := s.files[epochString(e)]
	s.mu.Unlock()
	if err := handFiles(w, e, files); err != nil {
		s.t.Fatalf("%s staged_files of epoch %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
	}
}

func handFiles(w *writer, e *protocolv1.Epoch, files map[string][]*protocolv1.StagedFile) error {
	for _, name := range kitStreams {
		if len(files[name]) == 0 {
			continue
		}
		if err := w.stagedFiles(e, name, files[name]); err != nil {
			return err
		}
	}
	return nil
}

// visible counts the ids each kit stream's records have in the
// destination.
func (s *session) visible(ctx context.Context) (map[string]multiset, error) {
	out := map[string]multiset{}
	for _, name := range kitStreams {
		rows, err := s.r.s.Read(ctx, s.ns, name)
		if err != nil {
			return nil, err
		}
		out[name] = multiset{}
		for _, row := range rows {
			out[name][fmt.Sprint(row["id"])]++
		}
	}
	return out, nil
}

// commit applies the schemas, as the runner does before each commit, and
// commits e.
func (s *session) commit(w *writer, e *protocolv1.Epoch, staged []byte) {
	s.t.Helper()
	if err := s.tryCommit(w, e, staged); err != nil {
		s.t.Fatalf("%s commit %s: %s: %v", s.where(), epochString(e), codeOf(err), err)
	}
}

func (s *session) tryCommit(w *writer, e *protocolv1.Epoch, staged []byte) error {
	if err := s.applySchemas(w); err != nil {
		return fmt.Errorf("apply_schema: %w", err)
	}
	return w.commit(e, staged)
}

func (s *session) applySchemas(w *writer) error {
	if !w.opened.GetAppliesSchema() {
		return nil
	}
	var schemas []*protocolv1.StreamSchema
	for _, name := range kitStreams {
		b, err := arrowipc.EncodeSchema(streamSchema(name, s.data))
		if err != nil {
			return err
		}
		schemas = append(schemas, &protocolv1.StreamSchema{Name: name, ArrowSchema: b})
	}
	return w.applySchema(schemas)
}

func (s *session) close(w *writer) {
	s.t.Helper()
	if err := w.close(); err != nil {
		s.t.Errorf("%s closing the write stream: %v", s.where(), err)
	}
}

func (s *session) read(stream string) []map[string]any {
	s.t.Helper()
	rows, err := s.r.s.Read(s.t.Context(), s.ns, stream)
	if err != nil {
		s.t.Fatalf("%s Read(%s, %s): %v", s.where(), s.ns, stream, err)
	}
	return rows
}

// expect reports how the destination's records differ from want, by id. A
// deliver-and-acknowledge destination may repeat records.
func (s *session) expect(when string, want map[string][]record) {
	s.t.Helper()
	deadline := time.Now().Add(s.r.s.Settle)
	diffs := s.diff(want)
	waited := false
	for len(diffs) > 0 && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
		diffs, waited = s.diff(want), true
	}
	if waited && len(diffs) == 0 {
		// Late arrivals could still be repeats.
		time.Sleep(min(time.Second, s.r.s.Settle))
		diffs = s.diff(want)
	}
	for _, d := range diffs {
		s.t.Errorf("%s %s: %s", s.where(), when, d)
	}
}

func (s *session) diff(want map[string][]record) []string {
	s.t.Helper()
	repeats := s.r.class() == protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK
	var out []string
	for _, name := range kitStreams {
		got := multiset{}
		for _, row := range s.read(name) {
			got[fmt.Sprint(row["id"])]++
		}
		w := multiset{}
		for _, rec := range want[name] {
			w[fmt.Sprint(rec.id)]++
		}
		if missing := w.minus(got); len(missing) > 0 {
			out = append(out, fmt.Sprintf("%s is missing ids %v", name, sample(missing)))
		}
		extra := got.minus(w)
		if repeats {
			extra = slices.DeleteFunc(extra, func(id string) bool { return w[id] > 0 })
		}
		if len(extra) > 0 {
			out = append(out, fmt.Sprintf("%s has ids %v too many times", name, sample(extra)))
		}
	}
	return out
}

func epoch(gen, n uint64) *protocolv1.Epoch { return &protocolv1.Epoch{Generation: gen, N: n} }

func merged(ms ...map[string][]record) map[string][]record {
	out := map[string][]record{}
	for _, m := range ms {
		for k, v := range m {
			out[k] = append(out[k], v...)
		}
	}
	return out
}

func (s *session) wantRecovery(gen uint64, got protocolv1.Recovery, want ...protocolv1.Recovery) {
	s.t.Helper()
	if !slices.Contains(want, got) {
		s.t.Errorf("%s generation %d's recover reported %s, not %v", s.where(), gen, got, want)
	}
}

// Write: commit: nothing is visible before Commit, Commit makes the epoch
// visible once, and a second Commit of it changes nothing.
func (r *destinationRun) checkCommit(_ T, s *session) {
	w, rec := s.begin(1, nil, nil)
	s.wantRecovery(1, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	e1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 2, "b")}
	staged := s.stage(w, epoch(1, 1), e1)
	if r.class() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		s.expect("after pre-commit, before commit", nil)
	}
	s.commit(w, epoch(1, 1), staged)
	s.expect("after commit", e1)
	s.commit(w, epoch(1, 1), staged)
	s.expect("after committing the epoch again", e1)
	e2 := map[string][]record{"alpha": records(3, 5, "a")}
	s.commit(w, epoch(1, 2), s.stage(w, epoch(1, 2), e2))
	s.expect("after a second epoch", merged(e1, e2))
	s.close(w)
}

// Write: crash after pre-commit: a runner died before it recorded the
// epoch. The next attempt recovers the epoch before it, then writes the
// lost one's records again, and each lands once.
func (r *destinationRun) checkCrashAfterPreCommit(_ T, s *session) {
	w, _ := s.begin(1, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 1, "b")}
	staged1 := s.stage(w, epoch(1, 1), e1)
	s.commit(w, epoch(1, 1), staged1)
	e2 := map[string][]record{"alpha": records(3, 6, "a"), "beta": records(1, 2, "b")}
	s.stage(w, epoch(1, 2), e2)
	w.crash()

	w2, rec := s.begin(2, epoch(1, 1), staged1)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	if r.class() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		s.expect("after recovering the epoch before the crash", e1)
	}
	s.commit(w2, epoch(2, 2), s.stage(w2, epoch(2, 2), e2))
	s.expect("after writing the unrecorded epoch again", merged(e1, e2))
	s.close(w2)
}

// Write: crash after the record: a runner died after it recorded the epoch
// but before Commit. Stage-and-apply destinations apply what was staged; a
// transactional stream's transaction died with its producer, so the epoch
// is lost and written again.
func (r *destinationRun) checkCrashAfterRecord(_ T, s *session) {
	w, _ := s.begin(1, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 2, "b")}
	staged := s.stage(w, epoch(1, 1), e1)
	w.crash()

	w2, rec := s.begin(2, epoch(1, 1), staged)
	switch {
	case r.staged():
		// The platform's staging outlives the runner.
		s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_REDONE)
	case r.class() == protocolv1.SinkClass_SINK_CLASS_STAGE_AND_APPLY:
		s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_REDONE)
	case r.class() == protocolv1.SinkClass_SINK_CLASS_TRANSACTIONAL_STREAM:
		s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_LOST)
	}
	if rec == protocolv1.Recovery_RECOVERY_LOST {
		if r.class() != protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
			s.expect("after recovery reported the epoch lost", nil)
		}
		s.commit(w2, epoch(2, 1), s.stage(w2, epoch(2, 1), e1))
	}
	s.expect("after recovery", e1)
	s.close(w2)
}

// Write: crash after commit: recovering an epoch that was committed finds
// it applied and applies nothing again.
func (r *destinationRun) checkCrashAfterCommit(_ T, s *session) {
	w, _ := s.begin(1, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 2, "b")}
	staged := s.stage(w, epoch(1, 1), e1)
	s.commit(w, epoch(1, 1), staged)
	w.crash()

	w2, rec := s.begin(2, epoch(1, 1), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after recovering a committed epoch", e1)
	e2 := map[string][]record{"beta": records(2, 4, "b")}
	s.commit(w2, epoch(2, 2), s.stage(w2, epoch(2, 2), e2))
	s.expect("after the next epoch", merged(e1, e2))
	s.close(w2)
}

// Write: staged resume: a destination that delivers a staged epoch in Commit,
// and is cut off just after it saves a delivery cursor, resumes from that
// cursor. The next attempt delivers every record, and none of those it had
// delivered when it saved the cursor.
func (r *destinationRun) checkStagedResume(t T, s *session) {
	if s.area == nil {
		t.Skipf("the destination doesn't take staged epochs")
	}
	w, _ := s.begin(1, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 6, "a"), "beta": records(0, 4, "b")}
	staged := s.stage(w, epoch(1, 1), e1)
	saved := make(chan map[string]multiset, 1)
	var once sync.Once
	s.area.mu.Lock()
	s.area.onCursor = func(e *protocolv1.Epoch) {
		if e.GetGeneration() != 1 || e.GetN() != 1 {
			return
		}
		once.Do(func() {
			v, err := s.visible(context.WithoutCancel(t.Context()))
			if err != nil {
				v = nil
			}
			w.crash()
			// The destination's commit is cancelled before it hears back.
			time.Sleep(200 * time.Millisecond)
			saved <- v
		})
	}
	s.area.mu.Unlock()
	defer func() {
		s.area.mu.Lock()
		s.area.onCursor = nil
		s.area.mu.Unlock()
	}()
	err := s.tryCommit(w, epoch(1, 1), staged)
	var delivered map[string]multiset
	select {
	case delivered = <-saved:
	case <-time.After(func() time.Duration {
		if err != nil {
			return 10 * time.Second
		}
		return 0
	}()):
	}
	switch {
	case delivered == nil && err == nil:
		flagf(t, "%s the commit saved no delivery cursor, so a delivery cut off partway is sent again whole", s.where())
		s.close(w)
		return
	case delivered == nil:
		t.Fatalf("%s commit %s failed before it saved a delivery cursor: %s: %v", s.where(), epochString(epoch(1, 1)), codeOf(err), err)
	}

	w2, rec := s.begin(2, epoch(1, 1), staged)
	s.wantRecovery(2, rec, protocolv1.Recovery_RECOVERY_REDONE, protocolv1.Recovery_RECOVERY_APPLIED)
	s.expect("after resuming the delivery", e1)
	now, err := s.visible(t.Context())
	if err != nil {
		t.Fatalf("%s Read: %v", s.where(), err)
	}
	for _, name := range kitStreams {
		var again []string
		for id, n := range delivered[name] {
			if now[name][id] > n {
				again = append(again, id)
			}
		}
		if len(again) > 0 {
			slices.Sort(again)
			t.Errorf("%s %s's ids %v were delivered before the cursor was saved, and again after the resume", s.where(), name, sample(again))
		}
	}
	s.close(w2)
}

// Write: fence: a runner that was replaced can still commit the epoch its
// replacement is recovering. The epoch lands once either way, and a commit
// the destination refuses fails with FENCED, which stops the old attempt.
func (r *destinationRun) checkFence(t T, s *session) {
	if r.class() == protocolv1.SinkClass_SINK_CLASS_DELIVER_AND_ACK {
		t.Skipf("deliver-and-acknowledge destinations have no fence")
	}
	w1, _ := s.begin(1, nil, nil)
	e1 := map[string][]record{"alpha": records(0, 3, "a"), "beta": records(0, 1, "b")}
	staged := s.stage(w1, epoch(1, 1), e1)
	w2, rec := s.begin(2, epoch(1, 1), staged)
	err := s.tryCommit(w1, epoch(1, 1), staged)
	switch {
	case err == nil:
		flagf(t, "%s generation 1 committed after generation 2 recovered; that's safe only if applying an epoch twice is, as copying to deterministic names is", s.where())
	case codeOf(err) != protocolv1.ErrorCode_ERROR_CODE_FENCED:
		t.Errorf("%s generation 1's commit failed with %s, not FENCED: %v", s.where(), codeOf(err), err)
	}
	w1.crash()
	if rec == protocolv1.Recovery_RECOVERY_LOST {
		s.expect("after the stale commit of an epoch recovery lost", nil)
		s.commit(w2, epoch(2, 1), s.stage(w2, epoch(2, 1), e1))
	}
	s.expect("after the stale commit", e1)
	s.close(w2)
}

func (r *destinationRun) supportsMode(mode protocolv1.WriteMode) bool {
	return slices.Contains(r.spec.GetDestination().GetWriteModes(), mode)
}

func (r *destinationRun) supports(t T, mode protocolv1.WriteMode) {
	t.Helper()
	r.process(t)
	if r.s.Read == nil {
		t.Fatalf("the suite has no Read")
	}
	if !slices.Contains(r.spec.GetDestination().GetWriteModes(), mode) {
		t.Skipf("the destination doesn't write %s", mode)
	}
}

// Write: overwrite: a new read replaces what the stream's last read wrote
// once its last epoch commits.
func (r *destinationRun) checkOverwrite(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_OVERWRITE)
	w, _ := s.begin(1, nil, nil)
	first := map[string][]record{"alpha": records(0, 4, "a"), "beta": records(0, 2, "b")}
	s.commit(w, epoch(1, 1), s.stage(w, epoch(1, 1), first, kitStreams...))
	s.expect("after the first read", first)
	if err := w.startReads(kitStreams, time.Now().Add(time.Second)); err != nil {
		t.Fatalf("start_reads: %v", err)
	}
	second := map[string][]record{"alpha": records(10, 12, "a"), "beta": records(10, 11, "b")}
	part := map[string][]record{"alpha": second["alpha"][:1], "beta": second["beta"]}
	s.commit(w, epoch(1, 2), s.stage(w, epoch(1, 2), part))
	s.expect("while the second read runs", first)
	rest := map[string][]record{"alpha": second["alpha"][1:]}
	s.commit(w, epoch(1, 3), s.stage(w, epoch(1, 3), rest, kitStreams...))
	s.expect("after the second read ended", second)
	s.close(w)
}

// Write: merge: records upsert on the primary key, the newer version
// winning.
func (r *destinationRun) checkMerge(t T) {
	r.supports(t, protocolv1.WriteMode_WRITE_MODE_MERGE)
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_MERGE)
	w, _ := s.begin(1, nil, nil)
	s.commit(w, epoch(1, 1), s.stage(w, epoch(1, 1), map[string][]record{"alpha": records(0, 3, "old")}))
	time.Sleep(time.Millisecond)
	s.commit(w, epoch(1, 2), s.stage(w, epoch(1, 2), map[string][]record{"alpha": records(2, 4, "new")}))
	want := map[string]string{"0": "old0", "1": "old1", "2": "new2", "3": "new3"}
	got := map[string]string{}
	for _, row := range s.read("alpha") {
		id := fmt.Sprint(row["id"])
		if _, dup := got[id]; dup {
			t.Errorf("alpha has id %s twice after merging", id)
		}
		got[id] = fmt.Sprint(row["v"])
	}
	for id, v := range want {
		if got[id] != v {
			t.Errorf("after merging, alpha's id %s has v %q, not %q", id, got[id], v)
		}
	}
	if len(got) != len(want) {
		t.Errorf("after merging, alpha has %d records, not %d", len(got), len(want))
	}
	s.close(w)
}

// Write: evolution: an added column applies, applying it again changes
// nothing, and its values land.
func (r *destinationRun) checkEvolution(t T) {
	r.process(t)
	if r.s.Read == nil {
		t.Fatalf("the suite has no Read")
	}
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_APPEND)
	w, _ := s.begin(1, nil, nil)
	if !w.opened.GetAppliesSchema() {
		t.Skipf("the destination's writer applies no schemas")
	}
	e1 := map[string][]record{"alpha": records(0, 2, "a")}
	s.commit(w, epoch(1, 1), s.stage(w, epoch(1, 1), e1))
	s.data = append(append([]arrow.Field{}, dataFields...), arrow.Field{Name: "w", Type: arrow.BinaryTypes.String, Nullable: true})
	b, err := arrowipc.EncodeSchema(streamSchema("alpha", s.data))
	if err != nil {
		t.Fatalf("%v", err)
	}
	for i := range 2 {
		if err := w.applySchema([]*protocolv1.StreamSchema{{Name: "alpha", ArrowSchema: b}}); err != nil {
			t.Fatalf("applying an added column, time %d: %s: %v", i+1, codeOf(err), err)
		}
	}
	rec := buildWideBatch("alpha", records(2, 4, "a"))
	err = s.write(w, epoch(1, 2), "alpha", rec)
	rec.Release()
	if err != nil {
		t.Fatalf("a batch with the added column: %s: %v", codeOf(err), err)
	}
	staged, err := s.preCommit(w, epoch(1, 2))
	if err != nil {
		t.Fatalf("pre-commit with the added column: %s: %v", codeOf(err), err)
	}
	s.commit(w, epoch(1, 2), staged)
	rows := s.read("alpha")
	ws := map[string]string{}
	for _, row := range rows {
		ws[fmt.Sprint(row["id"])] = fmt.Sprint(row["w"])
	}
	for _, id := range []string{"2", "3"} {
		if ws[id] != "w"+id {
			t.Errorf("record %s's added column is %q, not %q", id, ws[id], "w"+id)
		}
	}
	s.expect("after the added column", merged(e1, map[string][]record{"alpha": records(2, 4, "a")}))
	s.close(w)
}

// Write: type changes: a column whose type widens is refused, by apply_schema
// or by the write, unless its stream allows type changes; then the
// destination takes the wider type and keeps the rows it has.
func (r *destinationRun) checkTypeChanges(t T) {
	r.process(t)
	if r.s.Read == nil {
		t.Fatalf("the suite has no Read")
	}
	narrow := append(append([]arrow.Field{}, dataFields...), arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int32, Nullable: true})
	wide := append(append([]arrow.Field{}, dataFields...), arrow.Field{Name: "n", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	small, large := records(0, 2, "a"), records(1<<33, 1<<33+2, "a")
	s := r.session(t, r.spec.GetDestination().GetScopes()[0], protocolv1.WriteMode_WRITE_MODE_APPEND)
	s.data = narrow
	w, _ := s.begin(1, nil, nil)
	b := buildData("alpha", narrow, small, "insert")
	err := s.write(w, epoch(1, 1), "alpha", b)
	b.Release()
	if err != nil {
		t.Fatalf("%s a batch with an int32 column: %s: %v", s.where(), codeOf(err), err)
	}
	staged, err := s.preCommit(w, epoch(1, 1))
	if err != nil {
		t.Fatalf("%s pre-commit with an int32 column: %s: %v", s.where(), codeOf(err), err)
	}
	s.commit(w, epoch(1, 1), staged)

	s.data = wide
	refused, err := s.widen(w, epoch(1, 2), large)
	switch {
	case err != nil:
		t.Fatalf("%s the column widened to int64: %s: %v", s.where(), codeOf(err), err)
	case !refused && r.s.Untyped:
		s.expect("after the column widened", map[string][]record{"alpha": append(small, large...)})
		s.close(w)
		return
	case !refused:
		t.Fatalf("%s the destination took a column's change from int32 to int64, which its stream doesn't allow", s.where())
	}
	w.crash()
	s.expect("after the change was refused", map[string][]record{"alpha": small})

	s.allowTypeChanges = true
	w, _ = s.begin(2, epoch(1, 1), staged)
	if refused, err = s.widen(w, epoch(2, 2), large); err != nil || refused {
		t.Fatalf("%s the column widened to int64, which its stream allows: refused %v: %s: %v", s.where(), refused, codeOf(err), err)
	}
	s.expect("after the allowed change", map[string][]record{"alpha": append(small, large...)})
	ns := map[int64]string{}
	for _, rec := range append(small, large...) {
		ns[rec.id] = fmt.Sprint(rec.id)
	}
	s.expectColumn("after the allowed change", "alpha", "n", ns)
	s.close(w)
}

// widen writes epoch e's records with the session's columns and commits it,
// and reports whether the destination refused n's type: in apply_schema's
// answer, or with SCHEMA_DRIFT.
func (s *session) widen(w *writer, e *protocolv1.Epoch, recs []record) (bool, error) {
	if w.opened.GetAppliesSchema() {
		b, err := arrowipc.EncodeSchema(streamSchema("alpha", s.data))
		if err != nil {
			return false, err
		}
		a, err := w.schemaApplied([]*protocolv1.StreamSchema{{Name: "alpha", ArrowSchema: b}})
		if err != nil {
			return false, err
		}
		if slices.ContainsFunc(a.GetRefused(), func(c *protocolv1.RefusedColumn) bool { return c.GetStream() == "alpha" && c.GetColumn() == "n" }) {
			return true, nil
		}
	}
	b := buildData("alpha", s.data, recs, "insert")
	err := s.write(w, e, "alpha", b)
	b.Release()
	var staged []byte
	if err == nil {
		staged, err = s.preCommit(w, e)
	}
	if err == nil {
		err = s.tryCommit(w, e, staged)
	}
	if codeOf(err) == protocolv1.ErrorCode_ERROR_CODE_SCHEMA_DRIFT {
		return true, nil
	}
	return false, err
}
