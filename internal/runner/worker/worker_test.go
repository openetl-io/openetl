package worker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"

	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner/pods"
)

// fakeAPI hands out work, and answers reports with what stop says.
type fakeAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler

	mu      sync.Mutex
	work    []*dataplanev1.Work
	polls   []*dataplanev1.PollWorkRequest
	reports []*dataplanev1.ReportRunnerRequest
	results []*dataplanev1.ReportResultRequest
	stop    []string
	drain   []string
	reap    []*dataplanev1.Launch
	// Results fail until this many have been tried.
	failResults int
	refused     bool
}

func (f *fakeAPI) ReportRunner(_ context.Context, req *connect.Request[dataplanev1.ReportRunnerRequest]) (*connect.Response[dataplanev1.ReportRunnerResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reports = append(f.reports, req.Msg)
	return connect.NewResponse(&dataplanev1.ReportRunnerResponse{Stop: f.stop, Drain: f.drain, Reap: f.reap}), nil
}

func (f *fakeAPI) PollWork(ctx context.Context, req *connect.Request[dataplanev1.PollWorkRequest]) (*connect.Response[dataplanev1.PollWorkResponse], error) {
	f.mu.Lock()
	f.polls = append(f.polls, req.Msg)
	work := f.work
	f.work = nil
	f.mu.Unlock()
	if len(work) == 0 {
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return connect.NewResponse(&dataplanev1.PollWorkResponse{Work: work}), nil
}

func (f *fakeAPI) ReportResult(_ context.Context, req *connect.Request[dataplanev1.ReportResultRequest]) (*connect.Response[dataplanev1.ReportResultResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failResults > 0 {
		f.failResults--
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("down"))
	}
	f.results = append(f.results, req.Msg)
	return connect.NewResponse(&dataplanev1.ReportResultResponse{Accepted: !f.refused}), nil
}

func (f *fakeAPI) give(w ...*dataplanev1.Work) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.work = append(f.work, w...)
}

func (f *fakeAPI) tell(stop ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stop = stop
}

func (f *fakeAPI) result(id string) *dataplanev1.ReportResultRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range f.results {
		if r.GetId() == id {
			return r
		}
	}
	return nil
}

func (f *fakeAPI) lastReport() *dataplanev1.ReportRunnerRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.reports) == 0 {
		return nil
	}
	return f.reports[len(f.reports)-1]
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("waited too long for %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// start runs a worker against a fake API until the returned stop is called.
func start(t *testing.T, kinds map[string]Kind, capacity resources.Size) (api *fakeAPI, w *Worker, stop func()) {
	t.Helper()
	return startWith(t, kinds, capacity, nil)
}

// startWith is start with these cgroups.
func startWith(t *testing.T, kinds map[string]Kind, capacity resources.Size, cgroups *resources.Cgroups) (api *fakeAPI, w *Worker, stop func()) {
	t.Helper()
	api = &fakeAPI{}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	w = &Worker{
		API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), ID: "r1", Release: "dev", Protocol: "v1", Platform: "linux/amd64",
		Pool: "default", Capacity: capacity, Cgroups: cgroups, ReportEvery: 5 * time.Millisecond, Kinds: kinds,
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Error(err)
				}
			case <-time.After(30 * time.Second):
				t.Error("the worker didn't stop")
			}
		})
	}
	t.Cleanup(stop)
	return api, w, stop
}

func work(id, kind string, size *dataplanev1.Resources, input any) *dataplanev1.Work {
	b, _ := json.Marshal(input)
	return &dataplanev1.Work{Id: id, Kind: kind, Input: b, Size: size, HeartbeatTimeout: durationpb.New(time.Minute), Timeout: durationpb.New(time.Hour)}
}

var small = &dataplanev1.Resources{CpuMillis: 1000, MemoryBytes: 1 << 30}

func TestResultsReachTheAPI(t *testing.T) {
	type in struct{ N int }
	type out struct{ Doubled int }
	api, _, _ := start(t, map[string]Kind{
		"double": {Handler: Handle(func(_ context.Context, i in) (out, error) { return out{i.N * 2}, nil })},
		"fail":   {Handler: Handle(func(context.Context, in) (out, error) { return out{}, errs.New(errs.ConfigInvalid, "bad") })},
		"retry": {Handler: Handle(func(context.Context, in) (out, error) {
			return out{}, &errs.Error{Code: errs.RateLimited, Msg: "slow down", RetryAfter: time.Minute}
		})},
		"own": {Handler: Handle(func(context.Context, in) (out, error) {
			return out{}, &contract.Failure{Code: "DBT_FAILED", Message: "boom"}
		})},
		"panic": {Handler: Handle(func(context.Context, in) (out, error) { panic("oh no") })},
	}, resources.XL)
	api.give(work("a", "double", small, in{21}), work("b", "fail", small, in{}), work("c", "retry", small, in{}), work("d", "own", small, in{}), work("e", "panic", small, in{}), work("f", "unknown", small, in{}))
	eventually(t, "results", func() bool {
		return api.result("f") != nil && api.result("e") != nil && api.result("d") != nil && api.result("c") != nil && api.result("b") != nil && api.result("a") != nil
	})
	if r := api.result("a"); string(r.GetResult()) != `{"Doubled":42}` || r.GetCode() != "" {
		t.Errorf("a: %v", r)
	}
	if r := api.result("b"); r.GetCode() != "CONFIG_INVALID" || !r.GetNonRetryable() || r.GetMessage() != "CONFIG_INVALID: bad" {
		t.Errorf("b: %v", r)
	}
	if r := api.result("c"); r.GetCode() != "RATE_LIMITED" || r.GetNonRetryable() || r.GetRetryAfter().AsDuration() != time.Minute {
		t.Errorf("c: %v", r)
	}
	if r := api.result("d"); r.GetCode() != "DBT_FAILED" || !r.GetNonRetryable() || r.GetMessage() != "boom" {
		t.Errorf("d: %v", r)
	}
	if r := api.result("e"); r.GetCode() != "INTERNAL" || r.GetNonRetryable() {
		t.Errorf("e: %v", r)
	}
	if r := api.result("f"); r.GetCode() != "INTERNAL" {
		t.Errorf("f: %v", r)
	}
}

func TestReportsListWhatRuns(t *testing.T) {
	release := make(chan struct{})
	api, w, _ := start(t, map[string]Kind{"wait": {Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
		From(ctx).SetEpoch(7)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return struct{}{}, nil
	})}}, resources.L)
	defer close(release)
	api.give(work("a", "wait", small, struct{}{}))
	eventually(t, "a report of it", func() bool {
		r := api.lastReport()
		return r != nil && len(r.GetRunning()) == 1 && r.GetRunning()[0].GetEpoch() == 7
	})
	r := api.lastReport()
	if r.GetRunner() != "r1" || r.GetPool() != "default" || r.GetCapacity().GetCpuMillis() != w.Capacity.CPU || r.GetReserved().GetCpuMillis() != small.GetCpuMillis() || r.GetDraining() {
		t.Errorf("report %v", r)
	}
	if got := r.GetRunning()[0]; got.GetId() != "a" || got.GetSize().GetMemoryBytes() != small.GetMemoryBytes() {
		t.Errorf("running %v", got)
	}
}

// An attempt has its use and what it moved in reports and its result, and its
// share of the runner goes by what it moved. A light operation has none.
func TestUsage(t *testing.T) {
	release := make(chan struct{})
	moved := make(chan struct{}, 2)
	api, _, _ := start(t, map[string]Kind{
		"attempt": {Handler: Handle(func(ctx context.Context, n int64) (struct{}, error) {
			From(ctx).Meter.Moved(n, 1000*n)
			moved <- struct{}{}
			select {
			case <-release:
			case <-ctx.Done():
			}
			return struct{}{}, nil
		})},
		"light": {Handler: Handle(func(ctx context.Context, _ int64) (struct{}, error) {
			if From(ctx).Meter != nil {
				t.Error("a light operation has a meter")
			}
			return struct{}{}, nil
		})},
	}, resources.L)
	api.give(work("big", "attempt", small, int64(9)), work("little", "attempt", small, int64(1)), work("op", "light", nil, int64(0)))
	<-moved
	<-moved
	eventually(t, "a report of both", func() bool {
		r := api.lastReport()
		if r == nil || len(r.GetRunning()) != 2 {
			return false
		}
		for _, w := range r.GetRunning() {
			if w.GetUsage() == nil || w.GetBytes() == 0 {
				return false
			}
		}
		return true
	})
	for _, w := range api.lastReport().GetRunning() {
		want := uint64(1)
		if w.GetId() == "big" {
			want = 9
		}
		if w.GetRows() != want || w.GetBytes() != 1000*want {
			t.Errorf("%s moved %d rows, %d bytes, want %d rows", w.GetId(), w.GetRows(), w.GetBytes(), want)
		}
		if w.GetUsage().GetSeconds() <= 0 {
			t.Errorf("%s: usage %v", w.GetId(), w.GetUsage())
		}
	}
	close(release)
	eventually(t, "the results", func() bool { return api.result("big") != nil && api.result("little") != nil && api.result("op") != nil })
	big, little := api.result("big"), api.result("little")
	if big.GetRows() != 9 || big.GetUsage().GetSeconds() <= 0 || little.GetRows() != 1 {
		t.Errorf("results %v and %v", big, little)
	}
	if b, l := big.GetUsage().GetRunnerMemoryBytes(), little.GetUsage().GetRunnerMemoryBytes(); b <= 0 || l <= 0 {
		t.Errorf("shares of the runner's memory are %d and %d, want some of each", b, l)
	}
	// What the process used before these attempts isn't theirs.
	if self, _ := resources.Self(); big.GetUsage().GetRunnerCpuSeconds() >= self {
		t.Errorf("an attempt has %v s of the runner's CPU, of %v s it has used in all", big.GetUsage().GetRunnerCpuSeconds(), self)
	}
	if op := api.result("op"); op.GetUsage() != nil {
		t.Errorf("a light operation's result has usage: %v", op.GetUsage())
	}
}

// balloonEnv has this test binary hold that many MiB until it's killed.
const balloonEnv = "OPENETL_TEST_BALLOON_MIB"

func TestMain(m *testing.M) {
	if v := os.Getenv(balloonEnv); v != "" {
		n, _ := strconv.Atoi(v)
		held := make([]byte, n<<20)
		for i := 0; i < len(held); i += 4096 {
			held[i] = 1
		}
		time.Sleep(time.Minute)
		_ = held[0]
		return
	}
	os.Exit(m.Run())
}

// An attempt runs a process holding the MiB it's given, and waits for its
// context to end.
func balloonKind(t *testing.T) Kind {
	return Kind{Checkpoints: true, Handler: Handle(func(ctx context.Context, mib int) (struct{}, error) {
		cmd := exec.Command(os.Args[0])
		cmd.Env = append(os.Environ(), balloonEnv+"="+strconv.Itoa(mib))
		meter := From(ctx).Meter
		meter.Confine(cmd)
		if err := cmd.Start(); err != nil {
			t.Error(err)
			return struct{}{}, err
		}
		done := meter.Track(cmd.Process)
		exited := make(chan struct{})
		go func() { _ = cmd.Wait(); close(exited) }()
		select {
		case <-exited:
			done(cmd.ProcessState)
			return struct{}{}, errs.New(errs.Internal, "the process exited: %v", cmd.ProcessState)
		case <-ctx.Done():
			_ = cmd.Process.Kill()
			<-exited
			done(cmd.ProcessState)
			return struct{}{}, ctx.Err()
		case <-From(ctx).Stopping():
			_ = cmd.Process.Kill()
			<-exited
			done(cmd.ProcessState)
			return struct{}{}, ErrCancelled
		}
	})}
}

func TestWatchdogStopsAJobOverItsSize(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("reading a process's memory needs /proc")
	}
	api, _, _ := start(t, map[string]Kind{"attempt": balloonKind(t)}, resources.L)
	tiny := &dataplanev1.Resources{CpuMillis: 1000, MemoryBytes: 32 * resources.MiB}
	api.give(work("over", "attempt", tiny, 128), work("within", "attempt", small, 8))

	var res *dataplanev1.ReportResultRequest
	eventually(t, "the job over its size to stop", func() bool { res = api.result("over"); return res != nil })
	if res.GetCode() != errs.ResourceExhausted.String() || res.GetNonRetryable() {
		t.Errorf("result %v, want a retryable RESOURCE_EXHAUSTED", res)
	}
	if res.GetUsage().GetPeakMemoryBytes() < 32*resources.MiB {
		t.Errorf("usage %v", res.GetUsage())
	}
	// Its neighbour runs on.
	time.Sleep(2 * memoryWatchEvery)
	if got := api.result("within"); got != nil {
		t.Errorf("the job within its size stopped: %v", got)
	}
}

// With OPENETL_TEST_CGROUP_ROOT, as in resources' TestCgroupForReal: the kernel
// kills the job's processes, whatever the attempt makes of that.
func TestCgroupStopsAJobOverItsSize(t *testing.T) {
	root := os.Getenv("OPENETL_TEST_CGROUP_ROOT")
	if root == "" {
		t.Skip("set OPENETL_TEST_CGROUP_ROOT to a delegated cgroup v2 directory")
	}
	cgroups, err := resources.OpenCgroups(root)
	if err != nil {
		t.Fatal(err)
	}
	api, _, stop := startWith(t, map[string]Kind{"attempt": balloonKind(t)}, resources.L, cgroups)
	tiny := &dataplanev1.Resources{CpuMillis: 1000, MemoryBytes: 32 * resources.MiB}
	api.give(work("over", "attempt", tiny, 128), work("within", "attempt", small, 8))

	var res *dataplanev1.ReportResultRequest
	eventually(t, "the job over its size to stop", func() bool { res = api.result("over"); return res != nil })
	if res.GetCode() != errs.ResourceExhausted.String() || !strings.Contains(res.GetMessage(), "cgroup") {
		t.Errorf("result %v, want RESOURCE_EXHAUSTED from its cgroup", res)
	}
	if _, err := os.Stat(filepath.Join(root, "job-over")); !os.IsNotExist(err) {
		t.Errorf("its cgroup is still there: %v", err)
	}
	if got := api.result("within"); got != nil {
		t.Errorf("the job within its size stopped: %v", got)
	}
	stop()
	if _, err := os.Stat(filepath.Join(root, "job-within")); !os.IsNotExist(err) {
		t.Errorf("its cgroup is still there: %v", err)
	}
}

// Attempts stop themselves at a checkpoint, and report themselves cancelled;
// other work has its context cancelled.
func TestStopFromAReport(t *testing.T) {
	api, _, _ := start(t, map[string]Kind{
		"checkpoints": {Checkpoints: true, Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
			select {
			case <-From(ctx).Stopping():
				if ctx.Err() != nil {
					t.Error("its context ended before it stopped")
				}
				return struct{}{}, ErrCancelled
			case <-ctx.Done():
				return struct{}{}, ctx.Err()
			}
		})},
		"plain": {Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
			<-ctx.Done()
			return struct{}{}, ctx.Err()
		})},
	}, resources.L)
	api.give(work("a", "checkpoints", small, struct{}{}), work("b", "plain", nil, struct{}{}))
	eventually(t, "both running", func() bool { r := api.lastReport(); return r != nil && len(r.GetRunning()) == 2 })
	api.tell("a", "b")
	eventually(t, "both results", func() bool { return api.result("a") != nil && api.result("b") != nil })
	for _, id := range []string{"a", "b"} {
		if r := api.result(id); !r.GetCancelled() || r.GetCode() != "" {
			t.Errorf("%s: %v", id, r)
		}
	}
}

// A rescale drains an attempt, as the runner stopping does, without
// cancelling it; it says what its lanes did until then.
func TestDrainFromAReport(t *testing.T) {
	api, _, _ := start(t, map[string]Kind{"attempt": {Checkpoints: true, Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
		From(ctx).Meter.SetLanes(2, 0)
		From(ctx).Meter.AddLanes(3*time.Second, time.Second, 2*time.Second, 10)
		<-From(ctx).Stopping()
		if From(ctx).Cancelled() {
			t.Error("it thinks it was cancelled")
		}
		return struct{}{}, errs.New(errs.Draining, "drained")
	})}}, resources.L)
	api.give(work("a", "attempt", small, struct{}{}))
	eventually(t, "it running", func() bool { r := api.lastReport(); return r != nil && len(r.GetRunning()) == 1 })
	api.mu.Lock()
	api.drain = []string{"a"}
	api.mu.Unlock()
	eventually(t, "its result", func() bool { return api.result("a") != nil })
	r := api.result("a")
	if r.GetCode() != "DRAINING" || r.GetCancelled() {
		t.Fatalf("result %v", r)
	}
	if l := r.GetLanes(); l.GetLanes() != 2 || l.GetBusySeconds() != 3 || l.GetBackpressuredSeconds() != 1 || l.GetIdleSeconds() != 2 || l.GetRows() != 10 {
		t.Errorf("lanes %v", l)
	}
}

// When the runner stops, it stops polling, says it's draining, and work drains
// instead of being cancelled.
func TestDraining(t *testing.T) {
	api, _, stop := start(t, map[string]Kind{"attempt": {Checkpoints: true, StopTimeout: time.Minute, Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-From(ctx).Stopping()
		if From(ctx).Cancelled() {
			t.Error("it thinks it was cancelled")
		}
		return struct{}{}, errs.New(errs.Draining, "drained")
	})}}, resources.L)
	api.give(work("a", "attempt", small, struct{}{}))
	eventually(t, "it running", func() bool { r := api.lastReport(); return r != nil && len(r.GetRunning()) == 1 })
	stop()
	if r := api.result("a"); r == nil || r.GetCode() != "DRAINING" {
		t.Fatalf("result %v", r)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	draining := false
	for _, r := range api.reports {
		draining = draining || r.GetDraining()
	}
	if !draining {
		t.Error("no report said it was draining")
	}
}

// Work that ignores a stop has its context ended after its stop timeout.
func TestWorkThatDoesntStopIsCutOff(t *testing.T) {
	cut := make(chan struct{})
	api, _, stop := start(t, map[string]Kind{"stuck": {Checkpoints: true, StopTimeout: 10 * time.Millisecond, Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
		<-ctx.Done()
		close(cut)
		return struct{}{}, ctx.Err()
	})}}, resources.L)
	api.give(work("a", "stuck", small, struct{}{}))
	eventually(t, "it running", func() bool { r := api.lastReport(); return r != nil && len(r.GetRunning()) == 1 })
	stop()
	<-cut
	if r := api.result("a"); r == nil || r.GetCode() != "DRAINING" {
		t.Errorf("a context cut off by a drain is a drain: %v", r)
	}
}

// What doesn't fit, because a report was stale, goes back at once.
func TestNoCapacity(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var ran sync.Map
	api, _, _ := start(t, map[string]Kind{"wait": {Handler: Handle(func(ctx context.Context, in struct{ ID string }) (struct{}, error) {
		ran.Store(in.ID, true)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return struct{}{}, nil
	})}}, resources.Size{CPU: 1500, Memory: 2 << 30})
	api.give(work("a", "wait", small, struct{ ID string }{"a"}))
	eventually(t, "a running", func() bool { _, ok := ran.Load("a"); return ok })
	api.give(work("b", "wait", small, struct{ ID string }{"b"}))
	eventually(t, "b's answer", func() bool { return api.result("b") != nil })
	if !api.result("b").GetNoCapacity() {
		t.Errorf("b: %v", api.result("b"))
	}
	if _, ok := ran.Load("b"); ok {
		t.Error("b ran")
	}
}

// Light operations have no size, and take a fixed number of slots, which
// polls say are free.
func TestLightOperationsTakeSlots(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	wait := Kind{Handler: Handle(func(ctx context.Context, _ struct{}) (struct{}, error) {
		select {
		case <-release:
		case <-ctx.Done():
		}
		return struct{}{}, nil
	})}
	api, _, _ := start(t, map[string]Kind{"op": wait, contract.ReadLogs: wait}, resources.Size{})
	api.give(work("op", "op", nil, struct{}{}), work("logs", contract.ReadLogs, nil, struct{}{}))
	eventually(t, "a poll that says so", func() bool {
		api.mu.Lock()
		defer api.mu.Unlock()
		if len(api.polls) == 0 {
			return false
		}
		last := api.polls[len(api.polls)-1]
		return last.GetFreeOperations() == maxOps-1 && last.GetFreeLogReads() == maxLogReads-1
	})
}

// A report lost in a partition doesn't hold up the ones after it, which keep
// leases alive once the partition heals.
func TestALostReportDoesntHoldUpTheNext(t *testing.T) {
	first := make(chan struct{})
	var once sync.Once
	var reports atomic.Int32
	api := &hangingAPI{report: func(ctx context.Context) {
		reports.Add(1)
		hang := false
		once.Do(func() { hang = true })
		if hang {
			close(first)
			<-ctx.Done()
		}
	}}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	defer srv.Close()
	w := &Worker{API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), ID: "r1", ReportEvery: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	w.running = map[string]*Work{}
	w.drain = make(chan struct{})
	go w.reports(ctx)
	<-first
	eventually(t, "the reports after the lost one", func() bool { return reports.Load() >= 3 })
}

type hangingAPI struct {
	dataplanev1connect.UnimplementedDataPlaneServiceHandler
	report func(context.Context)
}

func (h *hangingAPI) ReportRunner(ctx context.Context, _ *connect.Request[dataplanev1.ReportRunnerRequest]) (*connect.Response[dataplanev1.ReportRunnerResponse], error) {
	h.report(ctx)
	return connect.NewResponse(&dataplanev1.ReportRunnerResponse{}), nil
}

// A result that can't get through is sent again.
func TestResultsAreRetried(t *testing.T) {
	api, _, _ := start(t, map[string]Kind{"ok": {Handler: Handle(func(context.Context, struct{}) (struct{}, error) { return struct{}{}, nil })}}, resources.L)
	api.mu.Lock()
	api.failResults = 2
	api.mu.Unlock()
	api.give(work("a", "ok", small, struct{}{}))
	eventually(t, "the result", func() bool { return api.result("a") != nil })
}

func TestLateStart(t *testing.T) {
	wk := NewWork(Spec{HeartbeatTimeout: time.Minute, Received: time.Now().Add(-30 * time.Second)}, nil)
	if err := wk.LateStart(); err != nil {
		t.Errorf("30 s late: %v", err)
	}
	wk = NewWork(Spec{HeartbeatTimeout: time.Minute, Received: time.Now().Add(-2 * time.Minute)}, nil)
	if err := wk.LateStart(); errs.CodeOf(err) != errs.Transient {
		t.Errorf("2 min late: %v, want TRANSIENT", err)
	}
	wk = NewWork(Spec{Received: time.Now().Add(-2 * time.Minute)}, nil)
	if err := wk.LateStart(); err != nil {
		t.Errorf("no heartbeat timeout: %v", err)
	}
}

func TestWorkWithoutAWorkerNeverStops(t *testing.T) {
	wk := From(t.Context())
	select {
	case <-wk.Stopping():
		t.Error("it's stopping")
	case <-wk.Draining():
		t.Error("it's draining")
	case <-time.After(5 * time.Millisecond):
	}
	if wk.Cancelled() {
		t.Error("it's cancelled")
	}
}

// fakeLauncher keeps pods in memory.
type fakeLauncher struct {
	mu       sync.Mutex
	launched []pods.Launch
	pods     map[string]*pods.Pod
	deleted  []string
	fail     error
	tries    int
}

func (f *fakeLauncher) Launch(_ context.Context, l pods.Launch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tries++
	if f.fail != nil {
		return f.fail
	}
	f.launched = append(f.launched, l)
	if f.pods == nil {
		f.pods = map[string]*pods.Pod{}
	}
	name := pods.Name(l.Attempt, l.Placement)
	f.pods[name] = &pods.Pod{Name: name, Attempt: l.Attempt, Placement: l.Placement}
	return nil
}

func (f *fakeLauncher) Pods(context.Context) ([]pods.Pod, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []pods.Pod
	for _, p := range f.pods {
		out = append(out, *p)
	}
	return out, nil
}

func (f *fakeLauncher) Delete(_ context.Context, p pods.Pod) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.pods, p.Name)
	f.deleted = append(f.deleted, p.Name)
	return nil
}

func (f *fakeLauncher) end(name string, failure error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pods[name].Ended, f.pods[name].Failure = true, failure
}

func (f *fakeAPI) launchResults(placement uint32) []*dataplanev1.ReportResultRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []*dataplanev1.ReportResultRequest
	for _, r := range f.results {
		if r.GetPlacement() == placement {
			out = append(out, r)
		}
	}
	return out
}

// A launcher makes a pod for each launch it's given, reports its pods, says
// once how one that died ended, and deletes those the API says to.
func TestLauncherMakesPods(t *testing.T) {
	l := &fakeLauncher{}
	api, stop := startLauncher(t, l)
	launch := work("sync/1/1", contract.RunSyncAttempt, small, map[string]string{"PipelineID": "p1"})
	launch.Placement = 3
	api.give(launch)
	eventually(t, "the pod", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.launched) == 1
	})
	if got := l.launched[0]; got.Attempt != "sync/1/1" || got.Placement != 3 || got.Pipeline != "p1" || got.Size != (resources.Size{CPU: 1000, Memory: 1 << 30}) || got.StopTimeout != 120 {
		t.Errorf("launched %+v", got)
	}
	name := pods.Name("sync/1/1", 3)
	eventually(t, "a report of the pod", func() bool {
		r := api.lastReport()
		return r != nil && len(r.GetLaunched()) == 1 && r.GetLaunched()[0].GetPlacement() == 3
	})
	if p := api.lastReport().GetPods(); p.GetMinSize().GetCpuMillis() != 2000 || p.GetMaxPods() != 5 || p.GetMaxSize() != nil {
		t.Errorf("pods %v", p)
	}
	if len(api.launchResults(3)) != 0 {
		t.Fatal("a result for a pod that's running")
	}

	l.end(name, errs.New(errs.ResourceExhausted, "its pod went over its memory limit"))
	eventually(t, "the launcher's result", func() bool { return len(api.launchResults(3)) == 1 })
	time.Sleep(50 * time.Millisecond)
	if rs := api.launchResults(3); len(rs) != 1 || rs[0].GetCode() != "RESOURCE_EXHAUSTED" || rs[0].GetNonRetryable() || rs[0].GetId() != "sync/1/1" {
		t.Errorf("results %v, want one RESOURCE_EXHAUSTED", rs)
	}
	api.mu.Lock()
	api.reap = []*dataplanev1.Launch{{Attempt: "sync/1/1", Placement: 3}}
	api.mu.Unlock()
	eventually(t, "the pod deleted", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return len(l.deleted) == 1 && l.deleted[0] == name
	})

	// A pod that can't ever be made fails its attempt; one that can't be made
	// for now is placed again when the launcher stops.
	l.mu.Lock()
	l.fail = errs.New(errs.ConfigInvalid, "pods: forbidden")
	l.mu.Unlock()
	bad := work("sync/2/1", contract.RunSyncAttempt, small, nil)
	bad.Placement = 4
	api.give(bad)
	eventually(t, "the failure", func() bool { return len(api.launchResults(4)) == 1 })
	if r := api.launchResults(4)[0]; r.GetCode() != "CONFIG_INVALID" || !r.GetNonRetryable() {
		t.Errorf("result %v", r)
	}
	l.mu.Lock()
	l.fail = errs.New(errs.Transient, "pods: the API server is down")
	tries := l.tries
	l.mu.Unlock()
	later := work("sync/3/1", contract.RunSyncAttempt, small, nil)
	later.Placement = 5
	api.give(later)
	eventually(t, "the launch tried", func() bool {
		l.mu.Lock()
		defer l.mu.Unlock()
		return l.tries > tries
	})
	stop()
	if rs := api.launchResults(5); len(rs) != 1 || !rs[0].GetNoCapacity() {
		t.Errorf("results %v, want it placed again", rs)
	}
}

// startLauncher runs a launcher against a fake API until the returned stop is
// called.
func startLauncher(t *testing.T, l pods.Launcher) (api *fakeAPI, stop func()) {
	t.Helper()
	api, w := fakeWorker(t)
	w.Launcher, w.Pods = l, PodLimits{Min: resources.M, MaxPods: 5}
	w.Kinds = map[string]Kind{contract.RunSyncAttempt: {StopTimeout: 2 * time.Minute, Checkpoints: true}}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			if err := <-done; err != nil {
				t.Error(err)
			}
		})
	}
	t.Cleanup(stop)
	return api, stop
}

func fakeWorker(t *testing.T) (*fakeAPI, *Worker) {
	t.Helper()
	api := &fakeAPI{}
	mux := http.NewServeMux()
	mux.Handle(dataplanev1connect.NewDataPlaneServiceHandler(api))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return api, &Worker{
		API: dataplanev1connect.NewDataPlaneServiceClient(srv.Client(), srv.URL), ID: "r1", Release: "dev", Protocol: "v1", Platform: "linux/amd64",
		Pool: "default", ReportEvery: 5 * time.Millisecond,
	}
}

// runPod runs a pod's worker until it stops of itself.
func runPod(t *testing.T, w *Worker) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- w.Run(t.Context()) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the pod didn't stop")
	}
}

// A pod takes its one attempt, runs it, reports it, and stops of itself.
func TestPodRunsItsAttempt(t *testing.T) {
	api, w := fakeWorker(t)
	api.give(work("sync/1/1", "run", small, nil))
	ran := false
	w.Attempt = &Attempt{ID: "sync/1/1", Placement: 2}
	w.Kinds = map[string]Kind{"run": {Handler: func(context.Context, []byte) (any, error) {
		ran = true
		time.Sleep(30 * time.Millisecond)
		return map[string]int{"N": 1}, nil
	}}}
	runPod(t, w)
	if !ran {
		t.Fatal("the attempt didn't run")
	}
	if r := api.result("sync/1/1"); r == nil || string(r.GetResult()) != `{"N":1}` {
		t.Errorf("result %v", r)
	}
	api.mu.Lock()
	defer api.mu.Unlock()
	if len(api.polls) != 1 || api.polls[0].GetAttempt().GetAttempt() != "sync/1/1" || api.polls[0].GetAttempt().GetPlacement() != 2 {
		t.Errorf("polls %v, want one for its attempt", api.polls)
	}
	if len(api.reports) == 0 || api.reports[0].GetAttempt() != "sync/1/1" {
		t.Errorf("reports %v, want them to name the attempt", api.reports)
	}
}

// A pod whose attempt has moved on stops at once.
func TestPodWithoutItsAttemptStops(t *testing.T) {
	api, w := fakeWorker(t)
	w.Attempt = &Attempt{ID: "gone", Placement: 1}
	runPod(t, w)
	if len(api.results) != 0 {
		t.Errorf("results %v, want none", api.results)
	}
}
