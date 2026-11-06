//go:build integration

package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/internal/orchestration/contract"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/resources"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/db"
	"github.com/unsuitable001/openetl/internal/store/storetest"
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
	t     *testing.T
	d     *Dispatcher
	waker *fakeWaker
	org   uuid.UUID
}

func setup(t *testing.T, o Options) *fixture {
	t.Helper()
	ctx := t.Context()
	s, err := store.Open(ctx, storetest.NewDatabase(t))
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
	return &fixture{t: t, d: &Dispatcher{Store: s, Waker: waker, Options: o}, waker: waker, org: tenant.Org}
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
	stop, err := f.d.Report(f.t.Context(), r)
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
	ok, err := f.d.Result(ctx, f.org, "default", "small", "a", out, false)
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
	if ok, err := f.d.Result(ctx, f.org, "default", "small", "a", out, false); err != nil || !ok {
		t.Errorf("sent again: %v, %v", ok, err)
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "big", "a", out, false); err != nil || ok {
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
		Size: resources.S, Heartbeat: 150 * time.Millisecond, Timeout: time.Hour,
	}, nil); err != nil {
		t.Fatal(err)
	}
	f.poll("r", 0, 0)
	// A report keeps it past its first deadline.
	time.Sleep(100 * time.Millisecond)
	f.report("r", resources.M, "a")
	time.Sleep(100 * time.Millisecond)
	f.tick()
	if st := f.status("a"); st.Done {
		t.Fatalf("the report didn't extend the lease: %+v", st)
	}
	// Without another, it ends.
	time.Sleep(200 * time.Millisecond)
	f.tick()
	st := f.status("a")
	if !st.Done || st.Outcome.Code != errs.Transient.String() || st.Outcome.NonRetryable {
		t.Fatalf("status %+v", st)
	}
	if !f.waker.has("a") {
		t.Error("the workflow wasn't woken")
	}
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "a", contract.WorkOutcome{}, false); err != nil || !ok {
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
	a, b := f.poll("a", 0, 0), f.poll("b", 0, 0)
	if len(a) != 2 || len(b) != 2 {
		t.Fatalf("a got %v, b got %v, want two each", a, b)
	}
	// The rest wait, and get room as it frees up.
	if ok, err := f.d.Result(t.Context(), f.org, "default", "a", a[0], contract.WorkOutcome{}, false); err != nil || !ok {
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
	if ok, err := f.d.Result(t.Context(), f.org, "default", "stale", "a", contract.WorkOutcome{}, true); err != nil || !ok {
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
	if _, err := f.d.Report(t.Context(), Report{Org: f.org, DataPlane: "default", Runner: db.ReportRunnerParams{
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
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "running", contract.WorkOutcome{Cancelled: true}, false); err != nil || !ok {
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
	if ok, err := f.d.Result(t.Context(), f.org, "default", "b", "o3", contract.WorkOutcome{Result: []byte(`{}`)}, false); err != nil || !ok {
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
	ok, err := f.d.Result(t.Context(), f.org, "default", "r", "a", contract.WorkOutcome{Result: []byte(`{}`)}, false)
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
		if ok, err := f.d.Result(ctx, f.org, "default", "r", id, contract.WorkOutcome{Result: []byte(`{}`)}, false); err != nil || !ok {
			t.Fatal(id, ok, err)
		}
	}
	f.waker.err = fmt.Errorf("temporal is down")
	if ok, err := f.d.Result(ctx, f.org, "default", "r", "unwoken", contract.WorkOutcome{Result: []byte(`{}`)}, false); err != nil || !ok {
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
