//go:build integration

package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
	"github.com/openetl-io/openetl/internal/store/storetest"
)

type fakeWaker struct {
	mu    sync.Mutex
	woken []string
	err   error
}

func (w *fakeWaker) Wake(_ context.Context, _, id string) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.woken = append(w.woken, id)
	return nil
}

func (w *fakeWaker) has(id string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Contains(w.woken, id)
}

type fixture struct {
	t         *testing.T
	d         *Dispatcher
	waker     *fakeWaker
	org       uuid.UUID
	workspace uuid.UUID
	url       string
}

func setup(t *testing.T, o Options) *fixture {
	t.Helper()
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.Bootstrap(ctx, "default", func(*db.Queries, uuid.UUID) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	waker := &fakeWaker{}
	return &fixture{t: t, d: &Dispatcher{Store: s, Waker: waker, Options: o}, waker: waker, org: tenant.Org, workspace: tenant.Workspace, url: url}
}

// report has a runner report what it runs, and returns what to stop.
func (f *fixture) report(id string, capacity resources.Size, running ...string) []string {
	f.t.Helper()
	r := Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: id, Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64",
		Pool: "default", CapacityCpu: capacity.CPU, CapacityMemory: capacity.Memory,
	}}
	for _, w := range running {
		r.Running = append(r.Running, RunningWork{ID: w, Epoch: 3, Size: resources.S})
	}
	stop, _, err := f.d.Report(f.t.Context(), r)
	if err != nil {
		f.t.Fatal(err)
	}
	return stop
}

func (f *fixture) attempt(id string) {
	f.t.Helper()
	err := f.d.Enqueue(f.t.Context(), contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{"k": id},
		Size: resources.S, Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
	}, nil)
	if err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) light(id, light string, pickup time.Duration) {
	f.t.Helper()
	err := f.d.Enqueue(f.t.Context(), contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.CheckConnector, Light: light, Input: map[string]string{},
		Heartbeat: time.Minute, Timeout: time.Hour, Pickup: pickup,
	}, nil)
	if err != nil {
		f.t.Fatal(err)
	}
}

// poll takes what's there for the runner, without waiting long.
func (f *fixture) poll(runner string, ops, logs int32) []string {
	f.t.Helper()
	work, err := f.d.claim(f.t.Context(), f.org, "default", runner, ops, logs)
	if err != nil {
		f.t.Fatal(err)
	}
	ids := make([]string, len(work))
	for i, w := range work {
		ids[i] = w.ID
	}
	slices.Sort(ids)
	return ids
}

func (f *fixture) tick() {
	f.t.Helper()
	if err := f.d.tick(f.t.Context(), f.org, false); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) status(id string) contract.WorkStatus {
	f.t.Helper()
	st, err := f.d.Status(f.t.Context(), contract.WorkRef{OrgID: f.org.String(), ID: id})
	if err != nil {
		f.t.Fatal(err)
	}
	return st
}

func TestAttemptPlacedPolledReportedFinished(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("small", resources.M)
	f.report("big", resources.XL)
	f.attempt("a")
	// Enqueuing again is what a retried step does.
	f.attempt("a")
	if got := f.poll("big", 0, 0); len(got) != 0 {
		t.Fatalf("big got %v; best fit is small", got)
	}
	if got := f.poll("small", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("small got %v", got)
	}
	if got := f.poll("small", 0, 0); len(got) != 0 {
		t.Fatalf("handed over twice: %v", got)
	}
	if st := f.status("a"); st.Done {
		t.Fatalf("done already: %+v", st)
	}
	// Its report keeps it, and stops nothing.
	if stop := f.report("small", resources.M, "a"); len(stop) != 0 {
		t.Fatalf("stop %v", stop)
	}
	out := contract.WorkOutcome{Result: []byte(`{"Committed":1}`)}
	ok, err := f.d.Result(ctx, f.org, "default", "small", "a", out, false, nil)
	if err != nil || !ok {
		t.Fatalf("result: %v, %v", ok, err)
	}
	if !f.waker.has("a") {
		t.Error("the workflow wasn't woken")
	}
	st := f.status("a")
	var res contract.AttemptResult
	if !st.Done || json.Unmarshal(st.Outcome.Result, &res) != nil || res.Committed != 1 {
		t.Fatalf("status %+v", st)
	}
	// A result sent again is fine; another runner's isn't.
	if ok, err := f.d.Result(ctx, f.org, "default", "small", "a", out, false, nil); err != nil || !ok {
		t.Errorf("sent again: %v, %v", ok, err)
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "big", "a", out, false, nil); err != nil || ok {
		t.Errorf("another runner's: %v, %v", ok, err)
	}
}

// A runner that doesn't start what's placed on it loses it, and it keeps its
// place; the next placement finds another runner, and the first can't start it
// late.
func TestAttemptNotStartedInTimeIsPlacedAgain(t *testing.T) {
	f := setup(t, Options{StartTimeout: 50 * time.Millisecond, ReportLapse: 80 * time.Millisecond})
	f.report("dead", resources.M)
	f.attempt("a")
	time.Sleep(100 * time.Millisecond)
	f.tick()
	if got := f.poll("dead", 0, 0); len(got) != 0 {
		t.Fatalf("a late start: %v", got)
	}
	if st := f.status("a"); st.Done {
		t.Fatalf("the workflow would see %+v", st)
	}
	f.report("alive", resources.M)
	f.tick()
	if got := f.poll("alive", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("alive got %v", got)
	}
}

// A runner whose reports lapse takes nothing.
func TestLapsedRunnerTakesNothing(t *testing.T) {
	f := setup(t, Options{ReportLapse: 20 * time.Millisecond})
	f.report("r", resources.M)
	time.Sleep(50 * time.Millisecond)
	f.attempt("a")
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
}

// A lease ends when the runner stops listing the work; the workflow hears, and
// the runner's report and result find it's gone.
func TestLeaseEnds(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("r", resources.M)
	if err := f.d.Enqueue(ctx, contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: "a", WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{},
		Size: resources.S, Heartbeat: time.Second, Timeout: time.Hour,
	}, nil); err != nil {
		t.Fatal(err)
	}
	f.poll("r", 0, 0)
	// A report keeps it past its first deadline.
	time.Sleep(600 * time.Millisecond)
	f.report("r", resources.M, "a")
	time.Sleep(600 * time.Millisecond)
	f.tick()
	if st := f.status("a"); st.Done {
		t.Fatalf("the report didn't extend the lease: %+v", st)
	}
	// Without another, it ends.
	time.Sleep(time.Second)
	f.tick()
	st := f.status("a")
	if !st.Done || st.Outcome.Code != errs.Transient.String() || st.Outcome.NonRetryable {
		t.Fatalf("status %+v", st)
	}
	if !f.waker.has("a") {
		t.Error("the workflow wasn't woken")
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "a", contract.WorkOutcome{}, false, nil); err != nil || !ok {
		// It was this runner's, and it's finished, so a late result is taken
		// as already in, and changes nothing.
		t.Errorf("late result: %v, %v", ok, err)
	}
	if st := f.status("a"); st.Outcome.Code != errs.Transient.String() {
		t.Errorf("a late result changed it: %+v", st)
	}
	if stop := f.report("r", resources.M, "a"); !slices.Equal(stop, []string{"a"}) {
		t.Errorf("stop %v, want a", stop)
	}
}

// Work can't outrun its timeout, however its runner reports it.
func TestTimeoutEndsALease(t *testing.T) {
	f := setup(t, Options{})
	f.report("r", resources.M)
	if err := f.d.Enqueue(t.Context(), contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: "a", WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{},
		Size: resources.S, Heartbeat: time.Hour, Timeout: 100 * time.Millisecond,
	}, nil); err != nil {
		t.Fatal(err)
	}
	f.poll("r", 0, 0)
	f.report("r", resources.M, "a")
	time.Sleep(150 * time.Millisecond)
	f.report("r", resources.M, "a")
	f.tick()
	if st := f.status("a"); !st.Done {
		t.Fatalf("status %+v", st)
	}
}

// A report that lists what isn't the runner's any more holds its room until
// it stops, which the next placement sees.
func TestOrphansUseRoom(t *testing.T) {
	f := setup(t, Options{})
	stop := f.report("r", resources.S, "gone")
	if !slices.Equal(stop, []string{"gone"}) {
		t.Fatalf("stop %v", stop)
	}
	f.attempt("a")
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Fatalf("placed over an orphan: %v", got)
	}
	f.report("r", resources.S)
	f.tick()
	if got := f.poll("r", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("got %v", got)
	}
}

func TestRoomIsNeverOverplaced(t *testing.T) {
	f := setup(t, Options{})
	f.report("a", resources.M)
	f.report("b", resources.M)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() { f.attempt(fmt.Sprintf("w%d", i)) })
	}
	wg.Wait()
	// Passes that found another at work left theirs to the next tick.
	f.tick()
	a, b := f.poll("a", 0, 0), f.poll("b", 0, 0)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("a got %v, b got %v, want two each", a, b)
	}
	// The rest wait, and get room as it frees up.
	if ok, err := f.d.Result(t.Context(), f.org, "default", "a", a[0], contract.WorkOutcome{}, false, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	f.report("a", resources.M, a[1])
	f.report("b", resources.M, b...)
	f.tick()
	if got := f.poll("a", 0, 0); len(got) != 1 {
		t.Errorf("a got %v", got)
	}
	if got := f.poll("b", 0, 0); len(got) != 0 {
		t.Errorf("b got %v", got)
	}
}

// A pass that finds another at work on the data plane doesn't wait for it,
// holding a connection, and fails nothing; the next tick places what it left.
func TestABusyPassLeavesItsWorkToTheNextTick(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("r", resources.M)
	conn, err := pgx.Connect(ctx, f.url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	key := planeKey(f.org, "default")
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock(hashtextextended($1, 0))", key); err != nil {
		t.Fatal(err)
	}
	enqueued := make(chan struct{})
	go func() {
		defer close(enqueued)
		f.attempt("a")
	}()
	select {
	case <-enqueued:
	case <-time.After(5 * time.Second):
		t.Fatal("the pass waited for the other")
	}
	f.tick()
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Fatalf("placed by a pass that wasn't let: %v", got)
	}
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", key); err != nil {
		t.Fatal(err)
	}
	f.tick()
	if got := f.poll("r", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("got %v", got)
	}
}

// Manual syncs go ahead of what's been waiting.
func TestPriorityInAPass(t *testing.T) {
	f := setup(t, Options{})
	f.attempt("old")
	if err := f.d.Enqueue(t.Context(), contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: "manual", WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{},
		Size: resources.S, Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityManual,
	}, nil); err != nil {
		t.Fatal(err)
	}
	f.report("r", resources.S)
	f.tick()
	if got := f.poll("r", 0, 0); !slices.Equal(got, []string{"manual"}) {
		t.Fatalf("got %v", got)
	}
}

func TestNeverFitsFails(t *testing.T) {
	f := setup(t, Options{ReserveAfter: time.Millisecond})
	f.report("tiny", resources.XS)
	f.attempt("a")
	time.Sleep(10 * time.Millisecond)
	f.tick()
	st := f.status("a")
	if !st.Done || st.Outcome.Code != errs.ConfigInvalid.String() || !st.Outcome.NonRetryable || !strings.Contains(st.Outcome.Message, "the largest has 0.5 vCPU, 1 GiB") {
		t.Fatalf("status %+v", st)
	}
	if !f.waker.has("a") {
		t.Error("the workflow wasn't woken")
	}
}

func TestNoCapacityPlacesAgain(t *testing.T) {
	f := setup(t, Options{})
	f.report("stale", resources.S)
	f.attempt("a")
	if got := f.poll("stale", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("got %v", got)
	}
	if ok, err := f.d.Result(t.Context(), f.org, "default", "stale", "a", contract.WorkOutcome{}, true, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if st := f.status("a"); st.Done {
		t.Fatalf("status %+v", st)
	}
	f.report("stale", resources.Size{})
	f.report("other", resources.S)
	f.tick()
	if got := f.poll("other", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("got %v", got)
	}
}

// A runner that says it's draining gets nothing new, and what was placed on
// it goes elsewhere.
func TestDrainingReturnsPlacements(t *testing.T) {
	f := setup(t, Options{})
	f.report("r", resources.M)
	f.attempt("a")
	if _, _, err := f.d.Report(t.Context(), Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: "r", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64",
		Pool: "default", CapacityCpu: resources.M.CPU, CapacityMemory: resources.M.Memory, Draining: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	f.report("next", resources.M)
	f.tick()
	if got := f.poll("next", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("got %v", got)
	}
}

func TestCancel(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	ref := func(id string) contract.WorkRef { return contract.WorkRef{OrgID: f.org.String(), ID: id} }

	// Waiting for room, or placed, it ends at once.
	f.attempt("pending")
	if err := f.d.Cancel(ctx, ref("pending")); err != nil {
		t.Fatal(err)
	}
	if st := f.status("pending"); !st.Done || !st.Outcome.Cancelled {
		t.Errorf("pending: %+v", st)
	}
	f.report("r", resources.M)
	f.attempt("placed")
	if err := f.d.Cancel(ctx, ref("placed")); err != nil {
		t.Fatal(err)
	}
	if st := f.status("placed"); !st.Done || !st.Outcome.Cancelled {
		t.Errorf("placed: %+v", st)
	}
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Errorf("handed over after cancelling: %v", got)
	}

	// Running, its runner hears on its next report, and it runs till it stops.
	f.attempt("running")
	f.poll("r", 0, 0)
	if err := f.d.Cancel(ctx, ref("running")); err != nil {
		t.Fatal(err)
	}
	if st := f.status("running"); st.Done {
		t.Errorf("running: %+v", st)
	}
	if stop := f.report("r", resources.M, "running"); !slices.Equal(stop, []string{"running"}) {
		t.Errorf("stop %v", stop)
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "running", contract.WorkOutcome{Cancelled: true}, false, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if st := f.status("running"); !st.Outcome.Cancelled {
		t.Errorf("running: %+v", st)
	}
	// Nothing cancelled uses room.
	f.report("r", resources.M)
	f.attempt("a")
	f.attempt("b")
	if got := f.poll("r", 0, 0); len(got) != 2 {
		t.Errorf("got %v", got)
	}
}

// A cancel and a runner's "no capacity" can land in either order. Either way
// the work finishes as cancelled, its workflow hears, and it's not placed again.
func TestCancelRacingNoCapacity(t *testing.T) {
	running := func(t *testing.T) (*fixture, contract.WorkRef) {
		f := setup(t, Options{})
		f.report("r", resources.S)
		f.attempt("a")
		if got := f.poll("r", 0, 0); !slices.Equal(got, []string{"a"}) {
			t.Fatalf("got %v", got)
		}
		return f, contract.WorkRef{OrgID: f.org.String(), ID: "a"}
	}
	finished := func(t *testing.T, f *fixture) {
		t.Helper()
		if st := f.status("a"); !st.Done || !st.Outcome.Cancelled {
			t.Fatalf("status %+v", st)
		}
		f.tick()
		if !f.waker.has("a") {
			t.Error("the workflow wasn't woken")
		}
		f.report("r", resources.S)
		f.tick()
		if got := f.poll("r", 0, 0); len(got) != 0 {
			t.Errorf("handed over after cancelling: %v", got)
		}
	}
	t.Run("cancelled first", func(t *testing.T) {
		f, ref := running(t)
		if err := f.d.Cancel(t.Context(), ref); err != nil {
			t.Fatal(err)
		}
		if ok, err := f.d.Result(t.Context(), f.org, "default", "r", "a", contract.WorkOutcome{}, true, nil); err != nil || !ok {
			t.Fatal(ok, err)
		}
		finished(t, f)
	})
	t.Run("returned while cancelling", func(t *testing.T) {
		f, ref := running(t)
		ctx := t.Context()
		returned, commit, replaced := make(chan struct{}), make(chan struct{}), make(chan error, 1)
		go func() {
			replaced <- f.d.Store.Tx(ctx, func(q *db.Queries) error {
				runner := "r"
				_, err := q.ReplaceRunning(ctx, db.ReplaceRunningParams{Cancelled: cancelled(), OrgID: f.org, DataPlaneID: "default", ID: "a", Runner: &runner})
				close(returned)
				<-commit
				return err
			})
		}()
		<-returned
		cancelDone := make(chan error, 1)
		go func() { cancelDone <- f.d.Cancel(ctx, ref) }()
		// Cancel reaches the row while the runner's answer holds it.
		time.Sleep(200 * time.Millisecond)
		close(commit)
		if err := <-replaced; err != nil {
			t.Fatal(err)
		}
		if err := <-cancelDone; err != nil {
			t.Fatal(err)
		}
		finished(t, f)
	})
}

func TestWorkThatsGoneCountsAsLost(t *testing.T) {
	f := setup(t, Options{})
	if st := f.status("never"); !st.Done || st.Outcome.Code != errs.Transient.String() {
		t.Fatalf("status %+v", st)
	}
}

func TestLightOperations(t *testing.T) {
	f := setup(t, Options{})
	f.light("o1", contract.LightOps, time.Minute)
	f.light("o2", contract.LightOps, time.Minute)
	f.light("o3", contract.LightOps, time.Minute)
	f.light("l1", contract.LightLogs, time.Minute)

	if got := f.poll("a", 0, 1); !slices.Equal(got, []string{"l1"}) {
		t.Fatalf("a's log read: %v", got)
	}
	if got := f.poll("a", 2, 0); !slices.Equal(got, []string{"o1", "o2"}) {
		t.Fatalf("a's two operations: %v", got)
	}
	if got := f.poll("b", 4, 4); !slices.Equal(got, []string{"o3"}) {
		t.Fatalf("b: %v", got)
	}
	if ok, err := f.d.Result(t.Context(), f.org, "default", "b", "o3", contract.WorkOutcome{Result: []byte(`{}`)}, false, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if st := f.status("o3"); !st.Done {
		t.Fatalf("status %+v", st)
	}

	// Its lease works as an attempt's does.
	if stop := f.report("a", resources.M, "o1", "o2", "l1"); len(stop) != 0 {
		t.Errorf("stop %v", stop)
	}

	// Nobody takes it in time.
	f.light("late", contract.LightOps, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if got := f.poll("b", 4, 4); len(got) != 0 {
		t.Fatalf("got %v", got)
	}
	f.tick()
	st := f.status("late")
	if !st.Done || st.Outcome.Code != contract.NoRunner || !st.Outcome.NonRetryable {
		t.Fatalf("status %+v", st)
	}
	if !f.waker.has("late") {
		t.Error("the workflow wasn't woken")
	}
}

// Two runners never take the same operation.
func TestLightOperationsAreTakenOnce(t *testing.T) {
	f := setup(t, Options{})
	for i := range 40 {
		f.light(fmt.Sprintf("o%d", i), contract.LightOps, time.Minute)
	}
	var mu sync.Mutex
	seen := map[string]int{}
	var wg sync.WaitGroup
	for r := range 8 {
		wg.Go(func() {
			for range 10 {
				for _, id := range f.poll(fmt.Sprintf("r%d", r), 2, 0) {
					mu.Lock()
					seen[id]++
					mu.Unlock()
				}
			}
		})
	}
	wg.Wait()
	if len(seen) != 40 {
		t.Errorf("%d taken, want 40", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("%s taken %d times", id, n)
		}
	}
}

// A wake that fails is sent again until it gets through.
func TestWakesAreRetried(t *testing.T) {
	f := setup(t, Options{})
	f.report("r", resources.M)
	f.attempt("a")
	f.poll("r", 0, 0)
	f.waker.err = fmt.Errorf("temporal is down")
	ok, err := f.d.Result(t.Context(), f.org, "default", "r", "a", contract.WorkOutcome{Result: []byte(`{}`)}, false, nil)
	if err != nil || !ok {
		t.Fatalf("the result should stand: %v, %v", ok, err)
	}
	if err := f.d.tick(t.Context(), f.org, false); err == nil {
		t.Error("the tick should report it")
	}
	f.waker.err = nil
	f.tick()
	if !f.waker.has("a") {
		t.Fatal("not woken")
	}
	// Only once it's through.
	f.waker.woken = nil
	f.tick()
	if f.waker.has("a") {
		t.Error("woken twice")
	}
}

// Finished work goes after a while, once its workflow knows; a log read's
// result, which is a page of a log, sooner.
func TestFinishedWorkGoes(t *testing.T) {
	f := setup(t, Options{History: time.Hour, LightHistory: time.Millisecond})
	ctx := t.Context()
	f.report("r", resources.M)
	f.attempt("a")
	f.light("l", contract.LightLogs, time.Minute)
	f.light("unwoken", contract.LightLogs, time.Minute)
	f.poll("r", 0, 8)
	for _, id := range []string{"a", "l"} {
		if id == "a" {
			f.poll("r", 0, 0)
		}
		if ok, err := f.d.Result(ctx, f.org, "default", "r", id, contract.WorkOutcome{Result: []byte(`{}`)}, false, nil); err != nil || !ok {
			t.Fatal(id, ok, err)
		}
	}
	f.waker.err = fmt.Errorf("temporal is down")
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "unwoken", contract.WorkOutcome{Result: []byte(`{}`)}, false, nil); err != nil || !ok {
		t.Fatal(ok, err)
	}
	time.Sleep(5 * time.Millisecond)
	before := time.Now().Add(-time.Hour)
	lightBefore := time.Now()
	if err := f.d.Store.DeleteFinishedDispatches(ctx, db.DeleteFinishedDispatchesParams{OrgID: f.org, Before: &before, LightBefore: &lightBefore}); err != nil {
		t.Fatal(err)
	}
	exists := func(id string) bool {
		_, err := f.d.Store.GetDispatchWorkflow(ctx, db.GetDispatchWorkflowParams{OrgID: f.org, ID: id})
		return err == nil
	}
	if exists("l") || !exists("a") || !exists("unwoken") {
		t.Errorf("light gone: %v, attempt kept: %v, unwoken kept: %v", !exists("l"), exists("a"), exists("unwoken"))
	}
}

// A runner whose poll drops, as when it dies, takes nothing more, and what was
// placed on it goes elsewhere at once. If it's alive, its next report brings it
// back.
func TestADroppedPollTakesTheRunnerOutOfPlacement(t *testing.T) {
	f := setup(t, Options{PollWait: time.Minute})
	f.report("r", resources.M)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		_, err := f.d.Poll(ctx, f.org, "default", "r", 0, 0)
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("poll ended with %v", err)
	}
	f.attempt("a")
	if got := f.poll("r", 0, 0); len(got) != 0 {
		t.Fatalf("a disconnected runner got %v", got)
	}
	f.report("r", resources.M)
	f.tick()
	if got := f.poll("r", 0, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after its next report it got %v", got)
	}
}

// A long poll answers as soon as there's work, without waiting for its next
// look.
func TestLongPollWakes(t *testing.T) {
	f := setup(t, Options{PollWait: 10 * time.Second})
	ctx, cancel := context.WithCancel(t.Context())
	listened := make(chan error, 1)
	go func() { listened <- f.d.Listen(ctx) }()
	// Let LISTEN happen.
	time.Sleep(200 * time.Millisecond)
	f.report("r", resources.M)

	got := make(chan []db.Dispatch, 1)
	start := time.Now()
	go func() {
		work, err := f.d.Poll(ctx, f.org, "default", "r", 0, 0)
		if err != nil {
			t.Error(err)
		}
		got <- work
	}()
	time.Sleep(100 * time.Millisecond)
	f.attempt("a")
	select {
	case work := <-got:
		if len(work) != 1 || work[0].ID != "a" {
			t.Fatalf("got %+v", work)
		}
		if took := time.Since(start); took > 2*time.Second {
			t.Errorf("took %s", took)
		}
	case <-time.After(pollFallback + time.Second):
		t.Fatal("it didn't answer")
	}

	// A server stopping ends its polls at once.
	polling := make(chan struct{})
	go func() {
		defer close(polling)
		if work, err := f.d.Poll(context.WithoutCancel(ctx), f.org, "default", "r", 0, 0); err != nil || len(work) != 0 {
			t.Errorf("%+v, %v", work, err)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-polling:
	case <-time.After(2 * time.Second):
		t.Fatal("the poll outlasted the server")
	}
	if err := <-listened; err != nil {
		t.Error(err)
	}
}

// The mark a manual sync leaves is taken once, and an old one doesn't count.
func TestManualSyncMark(t *testing.T) {
	ctx := t.Context()
	url := storetest.NewDatabase(t)
	s, err := store.Open(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	if err := s.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := s.Bootstrap(ctx, "default", func(*db.Queries, uuid.UUID) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	// A pipeline's source and destination don't matter here.
	id := uuid.New()
	if _, err := conn.Exec(ctx, "SET session_replication_role = replica"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO pipelines (org_id, workspace_id, id, name, source_id, destination_id, data_plane_id, version, revision)
		VALUES ($1, $2, $3, 'p', $2, $2, 'default', 'v1', 1)`, tenant.Org, uuid.New(), id); err != nil {
		t.Fatal(err)
	}
	take := func() (bool, error) { return s.TakeManualSync(ctx, db.TakeManualSyncParams{OrgID: tenant.Org, ID: id}) }
	if _, err := take(); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("no mark: %v", err)
	}
	if err := s.MarkManualSync(ctx, db.MarkManualSyncParams{OrgID: tenant.Org, ID: id}); err != nil {
		t.Fatal(err)
	}
	if manual, err := take(); err != nil || !manual {
		t.Fatalf("marked: %v, %v", manual, err)
	}
	if _, err := take(); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("taken twice: %v", err)
	}
	if _, err := conn.Exec(ctx, "UPDATE pipelines SET manual_sync_time = now() - interval '1 hour' WHERE id = $1", id); err != nil {
		t.Fatal(err)
	}
	if manual, err := take(); err != nil || manual {
		t.Fatalf("an old mark: %v, %v", manual, err)
	}
}

// An attempt keeps its size, why it was given it, and what its runner says it
// used, from its reports and its result, which sizing reads back newest first.
func TestAttemptUsage(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	pipeline, sync := f.pipelineAndSync()
	f.report("r", resources.L)
	for n, id := range []string{"first", "second"} {
		err := f.d.Enqueue(ctx, contract.WorkInput{
			OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{"a": id},
			Size: resources.M, SizeReason: "history", Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
			PipelineID: pipeline.String(), Version: "v", SyncID: sync.String(), Attempt: int32(n + 1),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		// Enqueuing again changes nothing.
		err = f.d.Enqueue(ctx, contract.WorkInput{
			OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{"a": id},
			Size: resources.XL, SizeReason: "spec", Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
			PipelineID: pipeline.String(), Version: "v", SyncID: sync.String(), Attempt: int32(n + 1),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if work, err := f.d.Poll(ctx, f.org, "default", "r", 0, 0); err != nil || len(work) != 1 || work[0].ID != id {
			t.Fatalf("poll: %v, %v", work, err)
		}
		used := &AttemptUsage{Usage: resources.Usage{CPU: 10, Memory: 100, RunnerCPU: 1, RunnerMemory: 10, Seconds: 60}, Rows: 5, Bytes: 500}
		report := Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
			ID: "r", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64",
			Pool: "default", CapacityCpu: resources.L.CPU, CapacityMemory: resources.L.Memory,
		}, Running: []RunningWork{{ID: id, Epoch: 1, Size: resources.M, Usage: used}}}
		if _, _, err := f.d.Report(ctx, report); err != nil {
			t.Fatal(err)
		}
		// A report from a runner it isn't on says nothing about it.
		stale := report
		stale.Runner.ID, stale.Runner.Draining = "other", true
		stale.Running = []RunningWork{{ID: id, Epoch: 1, Size: resources.M, Usage: &AttemptUsage{Usage: resources.Usage{Memory: 1 << 40}}}}
		if _, _, err := f.d.Report(ctx, stale); err != nil {
			t.Fatal(err)
		}
		more := &AttemptUsage{Usage: resources.Usage{CPU: 20, Memory: 90, RunnerCPU: 2, RunnerMemory: 5, Seconds: 120}, Rows: 7, Bytes: 700}
		code := ""
		if n == 1 {
			code = errs.ResourceExhausted.String()
		}
		if ok, err := f.d.Result(ctx, f.org, "default", "r", id, contract.WorkOutcome{Code: code}, false, more); err != nil || !ok {
			t.Fatalf("result: %v, %v", ok, err)
		}
	}
	got, err := sizing.History(ctx, f.d.Store.Queries, f.org, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	want := sizing.Attempt{Size: resources.M, CPU: 22, Memory: 110, Seconds: 120}
	if len(got) != 2 {
		t.Fatalf("history %+v", got)
	}
	if got[1] != want {
		t.Errorf("first attempt %+v, want %+v", got[1], want)
	}
	want.Code = errs.ResourceExhausted.String()
	if got[0] != want {
		t.Errorf("second attempt %+v, want %+v", got[0], want)
	}
	rows, err := f.d.Store.SyncAttempts(ctx, db.SyncAttemptsParams{OrgID: f.org, SyncIds: []uuid.UUID{sync}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ID != "first" || rows[0].SizeReason != "history" || rows[0].Attempt != 1 || rows[0].Rows != 7 || rows[0].Bytes != 700 {
		t.Errorf("attempts %+v", rows)
	}
}

// A job goes only to runners of its pool.
func TestPools(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("general", resources.L)
	r := Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: "batch", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64",
		Pool: "batch", CapacityCpu: resources.L.CPU, CapacityMemory: resources.L.Memory,
	}}
	if _, _, err := f.d.Report(ctx, r); err != nil {
		t.Fatal(err)
	}
	err := f.d.Enqueue(ctx, contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: "a", WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{"a": "a"},
		Size: resources.S, Pool: "batch", Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	work, err := f.d.Poll(ctx, f.org, "default", "batch", 0, 0)
	if err != nil || len(work) != 1 {
		t.Fatalf("the batch runner got %v, %v", work, err)
	}
	if got, err := f.d.Capacities(ctx, f.org, "default", "batch"); err != nil || len(got) != 1 || got[0] != resources.L {
		t.Errorf("capacities %v, %v", got, err)
	}
	if got, err := f.d.Capacities(ctx, f.org, "default", "none"); err != nil || len(got) != 0 {
		t.Errorf("capacities of a pool with no runners %v, %v", got, err)
	}
}

// pipelineAndSync makes the rows an attempt's usage hangs off.
func (f *fixture) pipelineAndSync() (pipeline, sync uuid.UUID) {
	f.t.Helper()
	ctx, q := f.t.Context(), f.d.Store.Queries
	src, dst := uuid.New(), uuid.New()
	pipeline, sync = uuid.New(), uuid.New()
	err := errors.Join(
		q.InsertSource(ctx, db.InsertSourceParams{OrgID: f.org, WorkspaceID: f.workspace, ID: src, Name: "s", Spec: []byte("{}"), Version: "v", Secrets: []string{}}),
		q.InsertDestination(ctx, db.InsertDestinationParams{OrgID: f.org, WorkspaceID: f.workspace, ID: dst, Name: "d", Spec: []byte("{}"), Version: "v", Secrets: []string{}}),
		q.InsertPipeline(ctx, db.InsertPipelineParams{OrgID: f.org, WorkspaceID: f.workspace, ID: pipeline, Name: "p", SourceID: src, DestinationID: dst, DataPlaneID: "default", Version: "v"}),
	)
	if err != nil {
		f.t.Fatal(err)
	}
	if _, err := q.StartSync(ctx, db.StartSyncParams{OrgID: f.org, ID: sync, PipelineID: pipeline, Version: "v", WorkflowID: "wf", RunID: "run"}); err != nil {
		f.t.Fatal(err)
	}
	return pipeline, sync
}

// A rescale drains the pipeline's running attempts through their runners'
// reports, and ends those not started as drained at once. Reports keep each
// attempt's lanes, which the last sync's attempts give finishWithin.
func TestRescale(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	pipeline, sync := f.pipelineAndSync()
	f.report("r", resources.M)
	for n, id := range []string{"running", "waiting"} {
		err := f.d.Enqueue(ctx, contract.WorkInput{
			OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.RunStreamJob, Input: map[string]string{"a": id},
			Size: resources.M, Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
			PipelineID: pipeline.String(), Version: "v", SyncID: sync.String(), Attempt: int32(n + 1),
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	if work, err := f.d.Poll(ctx, f.org, "default", "r", 0, 0); err != nil || len(work) != 1 || work[0].ID != "running" {
		t.Fatalf("poll: %v, %v", work, err)
	}
	lanes := resources.Lanes{Count: 2, Limit: 4, Busy: 50, Backpressured: 5, Idle: 65, Rows: 1000}
	report := Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: "r", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64",
		Pool: "default", CapacityCpu: resources.M.CPU, CapacityMemory: resources.M.Memory,
	}, Running: []RunningWork{{ID: "running", Epoch: 1, Size: resources.M, Usage: &AttemptUsage{Usage: resources.Usage{Seconds: 60}, Bytes: 6000, Lanes: lanes}}}}
	if stop, drain, err := f.d.Report(ctx, report); err != nil || len(stop)+len(drain) != 0 {
		t.Fatalf("report: stop %v, drain %v, %v", stop, drain, err)
	}
	if err := f.d.Store.Tx(ctx, func(q *db.Queries) error {
		if _, err := q.RescaleWaiting(ctx, db.RescaleWaitingParams{OrgID: f.org, PipelineID: pipeline, Outcome: marshalOutcome(contract.WorkOutcome{Code: errs.Draining.String()})}); err != nil {
			return err
		}
		_, err := q.RescaleRunning(ctx, db.RescaleRunningParams{OrgID: f.org, PipelineID: pipeline})
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if st, err := f.d.Status(ctx, contract.WorkRef{OrgID: f.org.String(), ID: "waiting"}); err != nil || !st.Done || st.Outcome.Code != errs.Draining.String() {
		t.Errorf("the attempt not started: %+v, %v", st, err)
	}
	if stop, drain, err := f.d.Report(ctx, report); err != nil || len(stop) != 0 || !slices.Equal(drain, []string{"running"}) {
		t.Fatalf("report: stop %v, drain %v, %v", stop, drain, err)
	}
	rows, err := f.d.Store.SyncAttempts(ctx, db.SyncAttemptsParams{OrgID: f.org, SyncIds: []uuid.UUID{sync}})
	if err != nil {
		t.Fatal(err)
	}
	if r := rows[0]; r.Lanes != 2 || r.LaneLimit != 4 || r.BusySeconds != 50 || r.BackpressuredSeconds != 5 || r.IdleSeconds != 65 || r.LaneRows != 1000 {
		t.Errorf("the running attempt's lanes: %+v", r)
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "running", contract.WorkOutcome{}, false, report.Running[0].Usage); err != nil || !ok {
		t.Fatalf("result: %v, %v", ok, err)
	}
	if _, err := f.d.Store.FinishSync(ctx, db.FinishSyncParams{OrgID: f.org, ID: sync, Status: "succeeded"}); err != nil {
		t.Fatal(err)
	}
	last, err := sizing.LastSync(ctx, f.d.Store.Queries, f.org, pipeline)
	if err != nil {
		t.Fatal(err)
	}
	want := []sizing.SyncAttempt{{Bytes: 6000, Seconds: 60, Lanes: 2, Limit: 4}, {}}
	if !slices.Equal(last, want) {
		t.Errorf("the last sync's attempts %+v, want %+v", last, want)
	}
}

// launcher has a pod launcher report its pods, and returns which to delete.
func (f *fixture) launcher(id string, pods Pods, launched ...Launch) []Launch {
	f.t.Helper()
	if _, _, err := f.d.Report(f.t.Context(), Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: id, Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64", Pool: "default",
		Launcher: true, PodMinCpu: pods.Min.CPU, PodMinMemory: pods.Min.Memory, PodMaxCpu: pods.Max.CPU, PodMaxMemory: pods.Max.Memory,
		MaxPods: int32(pods.MaxPods), MaxPodCpu: pods.MaxCPU,
	}}); err != nil {
		f.t.Fatal(err)
	}
	reap, err := f.d.Reap(f.t.Context(), f.org, launched)
	if err != nil {
		f.t.Fatal(err)
	}
	return reap
}

func (f *fixture) sized(id string, size resources.Size) {
	f.t.Helper()
	err := f.d.Enqueue(f.t.Context(), contract.WorkInput{
		OrgID: f.org.String(), DataPlane: "default", ID: id, WorkflowID: "wf", Kind: contract.RunSyncAttempt, Input: map[string]string{},
		Size: size, Heartbeat: time.Minute, Timeout: time.Hour, Priority: contract.PriorityScheduled,
	}, nil)
	if err != nil {
		f.t.Fatal(err)
	}
}

// launches is what the launcher's poll hands it: the attempts' IDs and
// placements.
func (f *fixture) launches(launcher string) []Launch {
	f.t.Helper()
	work, err := f.d.claim(f.t.Context(), f.org, "default", launcher, 0, 0)
	if err != nil {
		f.t.Fatal(err)
	}
	var out []Launch
	for _, w := range work {
		if w.State != "placed" || !w.Launched {
			f.t.Fatalf("the launcher got %s as %s, launched %v", w.ID, w.State, w.Launched)
		}
		out = append(out, Launch{Attempt: w.ID, Placement: w.Placements})
	}
	return out
}

// claimAs has a pod take its attempt, and reports whether it got it.
func (f *fixture) claimAs(pod string, l Launch) bool {
	f.t.Helper()
	work, err := f.d.ClaimAttempt(f.t.Context(), f.org, "default", pod, l.Attempt, l.Placement)
	if err != nil {
		f.t.Fatal(err)
	}
	return len(work) == 1
}

// A large attempt goes to the pod launcher, which takes it once; its pod, and
// only a pod of that placement, runs it; it counts against the launcher's caps
// until it ends; and then the launcher deletes the pod.
func TestAttemptInAPod(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("shared", resources.XL)
	f.launcher("pods", Pods{Min: resources.M, MaxPods: 1})
	f.sized("small", resources.S)
	f.sized("large", resources.L)
	f.sized("next", resources.L)
	if got := f.poll("shared", 0, 0); !slices.Equal(got, []string{"small"}) {
		t.Fatalf("shared got %v, want small", got)
	}
	got := f.launches("pods")
	if len(got) != 1 || got[0] != (Launch{"large", 1}) {
		t.Fatalf("the launcher got %v, want large's first placement only: one pod at most", got)
	}
	if again := f.launches("pods"); len(again) != 0 {
		t.Fatalf("launched twice: %v", again)
	}
	if f.claimAs("stale", Launch{"large", 2}) {
		t.Fatal("a pod of another placement took it")
	}
	if !f.claimAs("pod-1", got[0]) {
		t.Fatal("its pod didn't get it")
	}
	if f.claimAs("pod-2", got[0]) {
		t.Fatal("a second pod got it too")
	}
	// The pod's reports keep it; it still holds the launcher's one pod.
	stop, _, err := f.d.Report(ctx, Report{Org: f.org, DataPlane: "default", Running: []RunningWork{{ID: "large", Size: resources.L}}, Runner: db.ReportRunnerParams{
		ID: "pod-1", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64", Pool: "default",
		CapacityCpu: resources.L.CPU, CapacityMemory: resources.L.Memory, Attempt: "large",
	}})
	if err != nil || len(stop) != 0 {
		t.Fatalf("stop %v (%v)", stop, err)
	}
	f.tick()
	if got := f.launches("pods"); len(got) != 0 {
		t.Fatalf("the launcher got %v while its one pod runs", got)
	}
	if reap := f.launcher("pods", Pods{Min: resources.M, MaxPods: 1}, Launch{"large", 1}); len(reap) != 0 {
		t.Fatalf("reap %v while it runs", reap)
	}
	// A pod's report doesn't make it a runner.
	if c, err := f.d.Capacities(ctx, f.org, "default", "default"); err != nil || len(c) != 2 || !slices.Contains(c, resources.XL) {
		t.Fatalf("capacities %v (%v), want shared's and the launcher's", c, err)
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "pod-1", "large", contract.WorkOutcome{Result: []byte(`{}`)}, false, nil); err != nil || !ok {
		t.Fatalf("result: %v %v", ok, err)
	}
	if st := f.status("large"); !st.Done || !f.waker.has("large") {
		t.Fatalf("status %+v", st)
	}
	if reap := f.launcher("pods", Pods{Min: resources.M, MaxPods: 1}, Launch{"large", 1}); !slices.Equal(reap, []Launch{{"large", 1}}) {
		t.Fatalf("reap %v, want large's pod", reap)
	}
	f.tick()
	if got := f.launches("pods"); len(got) != 1 || got[0].Attempt != "next" {
		t.Fatalf("the launcher got %v, want next", got)
	}
}

// A pod that doesn't start in time is replaced: the attempt is placed again,
// and the first pod can neither take it nor stay. A launcher that can't make a
// pod for now places it again; one whose pod died says how.
func TestLaunchesThatFail(t *testing.T) {
	f := setup(t, Options{PodStartTimeout: time.Second})
	ctx := t.Context()
	f.launcher("pods", Pods{Min: resources.M})
	f.sized("a", resources.L)
	first := f.launches("pods")
	if len(first) != 1 {
		t.Fatalf("launches %v", first)
	}
	time.Sleep(1100 * time.Millisecond)
	f.tick()
	second := f.launches("pods")
	if len(second) != 1 || second[0] != (Launch{"a", 2}) {
		t.Fatalf("launches %v, want a's second placement", second)
	}
	if f.claimAs("late", first[0]) {
		t.Fatal("the first pod took it late")
	}
	if reap := f.launcher("pods", Pods{Min: resources.M}, first[0], second[0]); !slices.Equal(reap, []Launch{first[0]}) {
		t.Fatalf("reap %v, want the first pod", reap)
	}

	// No room for a pod: placed again at once, as a third placement.
	if ok, err := f.d.LaunchResult(ctx, f.org, "default", "pods", "a", 2, contract.WorkOutcome{}, true); err != nil || !ok {
		t.Fatalf("no capacity: %v %v", ok, err)
	}
	third := f.launches("pods")
	if len(third) != 1 || third[0] != (Launch{"a", 3}) {
		t.Fatalf("launches %v, want a's third placement", third)
	}
	// Late word of an earlier placement changes nothing.
	if ok, err := f.d.LaunchResult(ctx, f.org, "default", "pods", "a", 2, contract.WorkOutcome{Code: "TRANSIENT"}, false); err != nil || ok {
		t.Fatalf("a stale placement's failure: %v %v", ok, err)
	}
	if !f.claimAs("pod-3", third[0]) {
		t.Fatal("its pod didn't get it")
	}
	oom := contract.WorkOutcome{Code: errs.ResourceExhausted.String(), Message: "its pod went over its memory limit"}
	if ok, err := f.d.LaunchResult(ctx, f.org, "default", "pods", "a", 3, oom, false); err != nil || !ok {
		t.Fatalf("its pod's failure: %v %v", ok, err)
	}
	if st := f.status("a"); !st.Done || st.Outcome.Code != errs.ResourceExhausted.String() || !f.waker.has("a") {
		t.Fatalf("status %+v", st)
	}
}

// The pool's runners are too small for a job below the launcher's threshold,
// so it goes to a pod; and a draining launcher keeps the pods it's making.
func TestPodPoolAndDrainingLauncher(t *testing.T) {
	f := setup(t, Options{})
	ctx := t.Context()
	f.report("tiny", resources.XS)
	f.launcher("pods", Pods{Min: resources.XL})
	f.sized("s", resources.S)
	got := f.launches("pods")
	if len(got) != 1 {
		t.Fatalf("launches %v, want s, which tiny can't take", got)
	}
	f.sized("t", resources.S)
	if _, _, err := f.d.Report(ctx, Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
		ID: "pods", Release: "dev", Protocol: "v1", Features: []string{}, Launchers: []string{LauncherSubprocess}, Platform: "linux/amd64", Pool: "default",
		Launcher: true, PodMinCpu: resources.XL.CPU, PodMinMemory: resources.XL.Memory, Draining: true,
	}}); err != nil {
		t.Fatal(err)
	}
	if !f.claimAs("pod", got[0]) {
		t.Fatal("the pod being made lost its attempt when its launcher drained")
	}
	if st, err := f.d.Store.GetDispatchOutcome(ctx, db.GetDispatchOutcomeParams{OrgID: f.org, ID: "t"}); err != nil || st.State != "pending" {
		t.Fatalf("t is %v (%v), want pending", st.State, err)
	}
}

// The placer's metrics count what a launcher's pods run, or will, apart from
// runners' demand, and add up what's placed and what's running.
func TestMetricsSplitPodsFromRunners(t *testing.T) {
	f := setup(t, Options{})
	f.report("shared", resources.M)
	f.launcher("pods", Pods{Min: resources.L, MaxPods: 1})
	f.sized("running", resources.S)
	f.poll("shared", 0, 0)
	f.sized("placed", resources.S)
	f.sized("waiting", resources.S)
	f.sized("pod", resources.L)
	f.sized("next-pod", resources.L)
	capacity, demands, err := f.d.readMetrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(capacity) != 1 || capacity[0].Cpu != resources.M.CPU {
		t.Errorf("capacity %+v, want shared's alone", capacity)
	}
	got := map[demandKey]int64{}
	for _, d := range demands {
		got[d.demandKey] = d.attempts
	}
	key := func(size resources.Size, pending, pod bool) demandKey {
		return demandKey{dataPlane: "default", pool: "default", size: size, pending: pending, pod: pod}
	}
	want := map[demandKey]int64{
		key(resources.S, false, false): 2, key(resources.S, true, false): 1,
		key(resources.L, false, true): 1, key(resources.L, true, true): 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("demand %v, want %v", got, want)
	}
}
