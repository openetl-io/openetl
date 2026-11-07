// Package worker is a runner's side of dispatch (ADR-0028 §1): it reports the
// runner to the API, long-polls for work, runs it, and reports its results.
package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/gen/openetl/dataplane/v1/dataplanev1connect"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/runner/pods"
)

// A runner takes this many light operations at a time, of each kind.
const (
	maxOps      = 4
	maxLogReads = 8
)

var tracer = otel.Tracer("github.com/openetl-io/openetl/internal/runner/worker")

// ErrCancelled is what work returns when it stopped because it was asked to.
var ErrCancelled = errors.New("worker: stopped when asked to")

// Handler runs one piece of work. Its context carries the Work.
type Handler func(ctx context.Context, input []byte) (any, error)

// Handle is a Handler for a function of the work's input and result.
func Handle[In, Out any](f func(context.Context, In) (Out, error)) Handler {
	return func(ctx context.Context, input []byte) (any, error) {
		var in In
		if err := json.Unmarshal(input, &in); err != nil {
			return nil, errs.Wrap(errs.Internal, err, "the work's input")
		}
		return f(ctx, in)
	}
}

// Kind is how a runner runs a kind of work.
type Kind struct {
	Handler Handler
	// How long it has to stop once it's asked to, or the runner is stopping,
	// before its context ends.
	StopTimeout time.Duration
	// It stops at its next checkpoint when Work.Stopping says to, so its
	// context doesn't end when it's asked to stop.
	Checkpoints bool
}

// Worker runs a runner's work.
type Worker struct {
	API dataplanev1connect.DataPlaneServiceClient
	// Unique to this process.
	ID       string
	Release  string
	Protocol string
	Features []string
	// How it starts connectors.
	Launchers []string
	// os/arch.
	Platform string
	Pool     string
	// What it can place on itself.
	Capacity resources.Size
	// Makes a cgroup for each attempt. Nil: a watchdog holds them to their size.
	Cgroups *resources.Cgroups
	// How often it reports. Zero: 10 s.
	ReportEvery time.Duration
	Kinds       map[string]Kind
	Logger      *slog.Logger
	// Set for a pod launcher: it makes a pod for each attempt placed on it,
	// within Pods, and runs only light operations itself.
	Launcher pods.Launcher
	Pods     PodLimits
	// Set for a pod: it runs this attempt and stops.
	Attempt *Attempt

	mu       sync.Mutex
	running  map[string]*Work
	reserved resources.Size
	ops      int
	logReads int
	// What the runner had used when its work last got its share.
	sharedCPU float64
	drain     chan struct{}
	handlers  sync.WaitGroup
	results   sync.WaitGroup
	// A launcher's pods it has said ended badly, or logged as stuck.
	told map[string]bool
}

// PodLimits are the jobs a pod launcher takes, and its caps.
type PodLimits struct {
	// Jobs with this much CPU or memory, or more.
	Min resources.Size
	// The largest pod; zero: any.
	Max resources.Size
	// Zero: no limit.
	MaxPods int
	MaxCPU  int64
}

// Attempt is a pod's attempt, and which placement of it the pod is for.
type Attempt struct {
	ID        string
	Placement int32
}

func (w *Worker) log() *slog.Logger {
	if w.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return w.Logger
}

// Run reports, polls and runs work until ctx ends. Then it stops taking work,
// asks what's running to stop, and returns once it has, and its results have
// gone out.
func (w *Worker) Run(ctx context.Context) error {
	w.running = map[string]*Work{}
	w.told = map[string]bool{}
	w.drain = make(chan struct{})
	w.sharedCPU, _ = resources.Self()
	base := context.WithoutCancel(ctx)
	reportCtx, stopReports := context.WithCancel(base)
	defer stopReports()
	resultCtx, stopResults := context.WithCancel(base)
	defer stopResults()

	reported := make(chan struct{})
	go func() {
		defer close(reported)
		w.reports(reportCtx)
	}()
	polled := make(chan struct{})
	var pollErr error
	go func() {
		defer close(polled)
		if w.Attempt != nil {
			pollErr = w.claim(ctx, base, resultCtx)
			return
		}
		w.poll(ctx, base, resultCtx)
	}()
	if w.Attempt != nil {
		// A pod stops once its attempt has, or it has none.
		idle := make(chan struct{})
		go func() {
			<-polled
			w.handlers.Wait()
			close(idle)
		}()
		select {
		case <-ctx.Done():
		case <-idle:
		}
	} else {
		<-ctx.Done()
	}
	<-polled
	close(w.drain)
	if err := w.report(base); err != nil {
		w.log().Warn("telling the API this runner is stopping failed", "err", err)
	}
	w.handlers.Wait()
	// Results that can't get through don't hold the runner up for long; their
	// leases end, and the work runs again.
	sent := make(chan struct{})
	go func() {
		w.results.Wait()
		close(sent)
	}()
	select {
	case <-sent:
	case <-time.After(15 * time.Second):
		stopResults()
		<-sent
	}
	stopReports()
	<-reported
	return pollErr
}

func (w *Worker) reports(ctx context.Context) {
	t := time.NewTicker(w.reportEvery())
	defer t.Stop()
	var failing, warned time.Time
	for {
		err := w.report(ctx)
		switch {
		case err == nil:
			if !failing.IsZero() {
				w.log().Info("reports reach the API again", "after", time.Since(failing).Round(time.Second))
				failing = time.Time{}
			}
		case ctx.Err() != nil:
		default:
			if failing.IsZero() {
				failing = time.Now()
			}
			if time.Since(warned) >= time.Minute {
				w.log().Warn("reporting to the API failed; running work carries on, and is fenced if it's replaced", "failing", time.Since(failing).Round(time.Second), "err", err)
				warned = time.Now()
			}
		}
		select {
		case <-t.C:
		case <-ctx.Done():
			return
		}
	}
}

func (w *Worker) reportEvery() time.Duration { return cmp.Or(w.ReportEvery, 10*time.Second) }

// report tells the API what the runner runs, which extends those leases, and
// stops what the answer says to.
func (w *Worker) report(ctx context.Context) error {
	w.mu.Lock()
	req := &dataplanev1.ReportRunnerRequest{
		Runner: w.ID, Release: w.Release, Protocol: w.Protocol, Features: w.Features, Launchers: w.Launchers, Platform: w.Platform,
		Pool: w.Pool, Capacity: protoSize(w.Capacity), Reserved: protoSize(w.reserved), Draining: w.draining(),
	}
	if w.Attempt != nil {
		req.Attempt = w.Attempt.ID
	}
	w.share()
	running := slices.Collect(maps.Values(w.running))
	w.mu.Unlock()
	launched := w.reportPods(ctx, req)
	for _, wk := range running {
		r := &dataplanev1.RunningWork{Id: wk.ID, Epoch: wk.epoch.Load(), Size: protoSize(wk.Size), Lanes: APILanes(wk.Meter.Lanes())}
		r.Usage, r.Rows, r.Bytes = wk.usage()
		req.Running = append(req.Running, r)
	}
	// A slow answer still counts, but one lost in a partition mustn't hold up
	// the reports after it, which keep leases alive once the partition heals.
	ctx, cancel := context.WithTimeout(ctx, 2*w.reportEvery())
	defer cancel()
	res, err := w.API.ReportRunner(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	w.mu.Lock()
	for _, id := range res.Msg.GetStop() {
		if wk, ok := w.running[id]; ok {
			wk.requestStop()
		}
	}
	for _, id := range res.Msg.GetDrain() {
		if wk, ok := w.running[id]; ok {
			wk.requestDrain()
		}
	}
	w.mu.Unlock()
	w.tend(ctx, launched, res.Msg.GetReap())
	return nil
}

// reportPods adds a launcher's limits and pods to its report, and returns
// the pods.
func (w *Worker) reportPods(ctx context.Context, req *dataplanev1.ReportRunnerRequest) []pods.Pod {
	if w.Launcher == nil {
		return nil
	}
	req.Pods = &dataplanev1.PodLauncher{MinSize: protoSize(w.Pods.Min), MaxPods: count32(w.Pods.MaxPods), MaxCpuMillis: w.Pods.MaxCPU}
	if w.Pods.Max != (resources.Size{}) {
		req.Pods.MaxSize = protoSize(w.Pods.Max)
	}
	launched, err := w.Launcher.Pods(ctx)
	if err != nil {
		// Reaping waits for the next report.
		w.log().Warn("listing the launcher's pods failed", "err", err)
		return nil
	}
	for _, p := range launched {
		req.Launched = append(req.Launched, &dataplanev1.Launch{Attempt: p.Attempt, Placement: uint32(max(0, p.Placement))})
	}
	return launched
}

// tend deletes a launcher's pods whose placements have ended, and says how
// the others that ended did, as their runners couldn't.
func (w *Worker) tend(ctx context.Context, launched []pods.Pod, reap []*dataplanev1.Launch) {
	gone := map[string]bool{}
	for _, l := range reap {
		gone[pods.Name(l.GetAttempt(), int32(min(l.GetPlacement(), math.MaxInt32)))] = true
	}
	for _, p := range launched {
		if gone[p.Name] {
			if err := w.Launcher.Delete(ctx, p); err != nil {
				w.log().Warn("deleting an attempt's pod failed; the next report tries again", "pod", p.Name, "err", err)
				continue
			}
			w.mu.Lock()
			delete(w.told, p.Name)
			w.mu.Unlock()
			continue
		}
		w.mu.Lock()
		told := w.told[p.Name]
		w.told[p.Name] = told || p.Ended || p.Stuck != ""
		w.mu.Unlock()
		switch {
		case told:
		case p.Ended:
			w.log().Warn("an attempt's pod ended without its result", "pod", p.Name, "attempt", p.Attempt, "err", p.Failure)
			w.send(context.WithoutCancel(ctx), launchFailure(w.ID, p.Attempt, p.Placement, p.Failure))
		case p.Stuck != "":
			w.log().Warn("an attempt's pod hasn't started, and is placed again if it doesn't start in time", "pod", p.Name, "attempt", p.Attempt, "why", p.Stuck)
		}
	}
}

// launchFailure is a launcher's result for a placement whose pod failed.
func launchFailure(runner, attempt string, placement int32, err error) *dataplanev1.ReportResultRequest {
	code := errs.CodeOf(err)
	return &dataplanev1.ReportResultRequest{
		Runner: runner, Id: attempt, Placement: uint32(max(0, placement)),
		Code: code.String(), Message: err.Error(), NonRetryable: !code.Retryable(),
	}
}

// share gives the attempts running their share of what the runner has used
// since the last time: in proportion to what each moved since then, and evenly
// if none moved anything. Call it with mu held.
func (w *Worker) share() {
	cpu, heap := resources.Self()
	used := max(0, cpu-w.sharedCPU)
	w.sharedCPU = cpu
	var attempts []*Work
	var moved int64
	for _, wk := range w.running {
		if wk.Meter != nil {
			attempts = append(attempts, wk)
			_, b := wk.Meter.Progress()
			moved += b - wk.sharedBytes
		}
	}
	for _, wk := range attempts {
		_, b := wk.Meter.Progress()
		part := 1 / float64(len(attempts))
		if moved > 0 {
			part = float64(b-wk.sharedBytes) / float64(moved)
		}
		wk.sharedBytes = b
		wk.Meter.Share(used*part, int64(float64(heap)*part))
	}
}

func (w *Worker) draining() bool {
	select {
	case <-w.drain:
		return true
	default:
		return false
	}
}

func protoSize(s resources.Size) *dataplanev1.Resources {
	return &dataplanev1.Resources{CpuMillis: s.CPU, MemoryBytes: s.Memory}
}

// poll takes work until ctx ends. Handlers run on base.
func (w *Worker) poll(ctx, base, results context.Context) {
	backoff := time.Second
	var warned time.Time
	for ctx.Err() == nil {
		w.mu.Lock()
		req := &dataplanev1.PollWorkRequest{Runner: w.ID, FreeOperations: uint32(maxOps - w.ops), FreeLogReads: uint32(maxLogReads - w.logReads)} //nolint:gosec // bounded by the constants
		w.mu.Unlock()
		// A little longer than the API waits.
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := w.API.PollWork(pctx, connect.NewRequest(req))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if time.Since(warned) >= time.Minute {
				w.log().Warn("polling the API for work failed", "err", err)
				warned = time.Now()
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			backoff = min(2*backoff, 10*time.Second)
			continue
		}
		backoff = time.Second
		for _, m := range res.Msg.GetWork() {
			if m.GetPlacement() > 0 && w.Launcher != nil {
				w.launch(base, results, m)
				continue
			}
			w.start(base, results, m)
		}
	}
}

// claim takes a pod's attempt and starts it. It tries for a while if the API
// can't be reached; a pod whose attempt has moved on has nothing to do.
func (w *Worker) claim(ctx, base, results context.Context) error {
	req := &dataplanev1.PollWorkRequest{Runner: w.ID, Attempt: &dataplanev1.Launch{Attempt: w.Attempt.ID, Placement: uint32(max(0, w.Attempt.Placement))}}
	backoff, deadline := time.Second, time.Now().Add(2*time.Minute)
	for {
		pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		res, err := w.API.PollWork(pctx, connect.NewRequest(req))
		cancel()
		switch {
		case err == nil && len(res.Msg.GetWork()) == 0:
			w.log().Info("the attempt isn't this pod's to run any more", "attempt", w.Attempt.ID, "placement", w.Attempt.Placement)
			return nil
		case err == nil:
			for _, m := range res.Msg.GetWork() {
				w.start(base, results, m)
			}
			return nil
		case ctx.Err() != nil:
			return nil
		case time.Now().After(deadline):
			return fmt.Errorf("worker: taking attempt %s from the API: %w", w.Attempt.ID, err)
		}
		w.log().Warn("taking the attempt from the API failed; trying again", "err", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}
		backoff = min(2*backoff, 10*time.Second)
	}
}

// launch makes a pod for an attempt placed on the launcher. If it can't for
// now, the attempt is placed again; if it never can, the attempt fails.
func (w *Worker) launch(base, results context.Context, m *dataplanev1.Work) {
	l := pods.Launch{
		Attempt: m.GetId(), Placement: int32(min(m.GetPlacement(), math.MaxInt32)), Kind: m.GetKind(),
		Size:        resources.Size{CPU: m.GetSize().GetCpuMillis(), Memory: m.GetSize().GetMemoryBytes()},
		Pipeline:    pipelineOf(m.GetInput()),
		StopTimeout: int64(cmp.Or(w.Kinds[m.GetKind()].StopTimeout, 2*time.Minute) / time.Second),
	}
	w.handlers.Go(func() {
		wait := time.Second
		for {
			ctx, cancel := context.WithTimeout(base, 30*time.Second)
			err := w.Launcher.Launch(ctx, l)
			cancel()
			if err == nil {
				w.log().Info("launched a pod for an attempt", "attempt", l.Attempt, "placement", l.Placement, "size", l.Size, "pod", pods.Name(l.Attempt, l.Placement))
				return
			}
			if errs.CodeOf(err) == errs.ConfigInvalid {
				w.log().Warn("the attempt's pod can't be made", "attempt", l.Attempt, "err", err)
				w.send(results, launchFailure(w.ID, l.Attempt, l.Placement, err))
				return
			}
			again := &dataplanev1.ReportResultRequest{Runner: w.ID, Id: l.Attempt, Placement: uint32(max(0, l.Placement)), NoCapacity: true}
			if wait > time.Minute {
				w.log().Warn("the attempt's pod couldn't be made for now; it's placed again", "attempt", l.Attempt, "err", err)
				w.send(results, again)
				return
			}
			select {
			case <-time.After(wait):
			case <-w.drain:
				w.send(results, again)
				return
			}
			wait *= 2
		}
	})
}

// pipelineOf is the pipeline an attempt's input names, if it names one.
func pipelineOf(input []byte) string {
	var in struct{ PipelineID string }
	_ = json.Unmarshal(input, &in)
	return in.PipelineID
}

func (w *Worker) start(base, results context.Context, m *dataplanev1.Work) {
	wk := newWork(m, w.drain)
	kind, known := w.Kinds[wk.Kind]
	light := m.GetSize() == nil
	w.mu.Lock()
	// A pod was made for its attempt.
	if !light && w.Attempt == nil && !wk.Size.Fits(w.Capacity.Sub(w.reserved)) {
		w.mu.Unlock()
		w.log().Warn("no room for what was placed here, whose report must have been stale", "work", wk.ID, "size", wk.Size)
		w.send(results, &dataplanev1.ReportResultRequest{Runner: w.ID, Id: wk.ID, NoCapacity: true})
		return
	}
	w.running[wk.ID] = wk
	w.reserved = w.reserved.Add(wk.Size)
	logRead := wk.Kind == contract.ReadLogs
	switch {
	case !light:
	case logRead:
		w.logReads++
	default:
		w.ops++
	}
	w.mu.Unlock()

	w.handlers.Go(func() {
		ctx, cancel := context.WithCancel(base)
		defer cancel()
		if wk.Timeout > 0 {
			var stop context.CancelFunc
			ctx, stop = context.WithTimeout(ctx, wk.Timeout)
			defer stop()
		}
		ctx = NewContext(ctx, wk)
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(m.GetTrace()))
		ctx, span := tracer.Start(ctx, "RunWork:"+wk.Kind, trace.WithSpanKind(trace.SpanKindServer))
		defer span.End()
		finished := make(chan struct{})
		defer close(finished)
		go w.cutOff(wk, kind, cancel, finished)

		release, limitErr := w.limit(wk, cancel, finished)
		var res any
		var err error
		switch {
		case !known:
			err = errs.New(errs.Internal, "this runner doesn't run %s", wk.Kind)
		case limitErr != nil:
			err = limitErr
		default:
			res, err = run(ctx, kind.Handler, m.GetInput())
		}
		// However the attempt came to fail: killing its processes can show up
		// as anything from a broken pipe to a cancelled context.
		if over := wk.overMemory(); err != nil && over != nil {
			err = over
		}
		release()
		w.mu.Lock()
		w.share()
		delete(w.running, wk.ID)
		w.reserved = w.reserved.Sub(wk.Size)
		switch {
		case !light:
		case logRead:
			w.logReads--
		default:
			w.ops--
		}
		w.mu.Unlock()
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		w.send(results, outcome(w.ID, wk, res, err))
	})
}

func run(ctx context.Context, h Handler, input []byte) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = errs.New(errs.Internal, "panic: %v\n%s", r, debug.Stack())
		}
	}()
	return h(ctx, input)
}

// limit holds an attempt to its size: with a cgroup if the runner has them, and
// otherwise with a watchdog. What it returns ends that, once the attempt has.
func (w *Worker) limit(wk *Work, stop context.CancelFunc, finished <-chan struct{}) (func(), error) {
	if wk.Meter == nil || wk.Size.Memory == 0 {
		return func() {}, nil
	}
	if w.Cgroups == nil {
		go w.watchMemory(wk, stop, finished)
		return func() {}, nil
	}
	g, err := w.Cgroups.Job(wk.ID, wk.Size)
	if err != nil {
		return func() {}, errs.Wrap(errs.Transient, err, "make the job's cgroup")
	}
	wk.group = g
	wk.Meter.Contain(g)
	return func() {
		if err := g.Close(); err != nil {
			w.log().Warn("removing a job's cgroup failed", "work", wk.ID, "err", err)
		}
	}, nil
}

// watchMemory stops an attempt whose processes hold more than its size allows,
// so it can't push out its neighbours. There's no drain: it would be growing
// meanwhile, and the attempt resumes from its last checkpoint.
func (w *Worker) watchMemory(wk *Work, stop context.CancelFunc, finished <-chan struct{}) {
	ceiling := wk.Size.MemoryCeiling()
	t := time.NewTicker(memoryWatchEvery)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-finished:
			return
		}
		held := wk.Meter.Resident()
		if held <= ceiling {
			continue
		}
		over := errs.New(errs.ResourceExhausted, "the job's processes held %d MiB, over the %d MiB its size allows", held/resources.MiB, ceiling/resources.MiB)
		wk.over.Store(&over)
		w.log().Warn("stopping a job over its size", "work", wk.ID, "size", wk.Size, "err", over)
		wk.Meter.Kill()
		stop()
		return
	}
}

// memoryWatchEvery is how often the watchdog reads a job's processes. A burst
// between readings is what oom_score_adj is for.
const memoryWatchEvery = time.Second

// cutOff ends the work's context once it has been asked to stop: at once if
// the work doesn't stop itself at a checkpoint and was cancelled, and otherwise
// after its stop timeout.
func (w *Worker) cutOff(wk *Work, kind Kind, cancel context.CancelFunc, finished <-chan struct{}) {
	select {
	case <-wk.stop:
		if !kind.Checkpoints {
			cancel()
			return
		}
	case <-wk.drain:
	case <-finished:
		return
	}
	t := time.NewTimer(cmp.Or(kind.StopTimeout, 2*time.Minute))
	defer t.Stop()
	select {
	case <-t.C:
		w.log().Warn("the work didn't stop in time", "work", wk.ID, "kind", wk.Kind)
		cancel()
	case <-finished:
	}
}

// outcome is the result report of work that returned res and err.
func outcome(runner string, wk *Work, res any, err error) *dataplanev1.ReportResultRequest {
	out := &dataplanev1.ReportResultRequest{Runner: runner, Id: wk.ID, Lanes: APILanes(wk.Meter.Lanes())}
	out.Usage, out.Rows, out.Bytes = wk.usage()
	var failure *contract.Failure
	switch {
	case err == nil:
		b, merr := json.Marshal(res)
		if merr != nil {
			return outcome(runner, wk, nil, errs.Wrap(errs.Internal, merr, "the work's result"))
		}
		out.Result = b
	case errors.Is(err, ErrCancelled):
		out.Cancelled = true
	case errors.As(err, &failure):
		out.Code, out.Message, out.NonRetryable = failure.Code, failure.Message, true
	case errors.Is(err, context.Canceled) && wk.Cancelled():
		out.Cancelled = true
	case errors.Is(err, context.Canceled):
		out.Code, out.Message = errs.Draining.String(), errs.New(errs.Draining, "the runner is stopping").Error()
	default:
		code := errs.CodeOf(err)
		out.Code, out.Message, out.NonRetryable = code.String(), err.Error(), !code.Retryable()
		if d := errs.RetryAfterOf(err); d > 0 {
			out.RetryAfter = durationpb.New(d)
		}
	}
	return out
}

// send reports a result until it gets an answer, for as long as the work's
// lease could still be alive.
func (w *Worker) send(ctx context.Context, req *dataplanev1.ReportResultRequest) {
	w.results.Go(func() {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		wait := time.Second
		for {
			cctx, stop := context.WithTimeout(ctx, 30*time.Second)
			res, err := w.API.ReportResult(cctx, connect.NewRequest(req))
			stop()
			if err == nil {
				if !res.Msg.GetAccepted() {
					w.log().Info("the work's result came too late; something else has replaced it", "work", req.GetId())
				}
				return
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				w.log().Warn("the work's result didn't reach the API; its lease ends and it runs again", "work", req.GetId(), "err", err)
				return
			}
			wait = min(2*wait, 15*time.Second)
		}
	})
}

// Spec is what the API says of a piece of work.
type Spec struct {
	ID, Kind string
	// How long it may go unreported.
	HeartbeatTimeout time.Duration
	Timeout          time.Duration
	Size             resources.Size
	// When the API handed it over, on this runner's wall clock.
	Received time.Time
}

// Work is one piece of work on this runner.
type Work struct {
	Spec
	// What an attempt uses. Nil for a light operation.
	Meter *resources.Meter
	group *resources.Cgroup
	// Why the watchdog stopped it.
	over atomic.Pointer[error]

	// How much of what it moved has had its share of the runner's work, under
	// the worker's lock.
	sharedBytes int64
	epoch       atomic.Uint64
	stop        chan struct{}
	stopOnce    sync.Once
	// A rescale: it stops at a checkpoint as if the runner were stopping.
	rescale     chan struct{}
	rescaleOnce sync.Once
	drain       <-chan struct{}
	stopping    func() <-chan struct{}
}

func newWork(m *dataplanev1.Work, drain <-chan struct{}) *Work {
	wk := NewWork(Spec{
		ID: m.GetId(), Kind: m.GetKind(), HeartbeatTimeout: m.GetHeartbeatTimeout().AsDuration(), Timeout: m.GetTimeout().AsDuration(),
		Size:     resources.Size{CPU: m.GetSize().GetCpuMillis(), Memory: m.GetSize().GetMemoryBytes()},
		Received: time.Now().Round(0),
	}, drain)
	if m.GetSize() != nil {
		wk.Meter = resources.NewMeter()
	}
	return wk
}

// overMemory is the failure of an attempt stopped for going over its size; nil
// if it wasn't.
func (wk *Work) overMemory() error {
	if over := wk.over.Load(); over != nil {
		return *over
	}
	if wk.group != nil && wk.group.OutOfMemory() {
		return errs.New(errs.ResourceExhausted, "the job went over the %d MiB its size allows, and its cgroup killed it", wk.Size.MemoryCeiling()/resources.MiB)
	}
	return nil
}

// usage is what an attempt has used and moved, for a report; nil for a light
// operation.
func (wk *Work) usage() (*apiv1.Usage, uint64, uint64) {
	if wk.Meter == nil {
		return nil, 0, 0
	}
	rows, bytes := wk.Meter.Progress()
	return APIUsage(wk.Meter.Usage()), uint64(max(0, rows)), uint64(max(0, bytes))
}

// APILanes is l as the API says it; nil before an attempt settles its lanes.
func APILanes(l resources.Lanes) *apiv1.LaneUsage {
	if l.Count == 0 {
		return nil
	}
	return &apiv1.LaneUsage{
		Lanes: count32(l.Count), Limit: count32(l.Limit),
		BusySeconds: l.Busy, BackpressuredSeconds: l.Backpressured, IdleSeconds: l.Idle, Rows: uint64(max(0, l.Rows)),
	}
}

func count32(n int) uint32 {
	if n > 0 && n <= math.MaxUint32 {
		return uint32(n)
	}
	return 0
}

// APIUsage is u as the API says it.
func APIUsage(u resources.Usage) *apiv1.Usage {
	return &apiv1.Usage{
		CpuSeconds: u.CPU, PeakMemoryBytes: u.Memory, RunnerCpuSeconds: u.RunnerCPU, RunnerMemoryBytes: u.RunnerMemory, Seconds: u.Seconds,
	}
}

// NewWork is work ready to run, on a runner that stops when drain closes.
func NewWork(s Spec, drain <-chan struct{}) *Work {
	wk := &Work{Spec: s, stop: make(chan struct{}), rescale: make(chan struct{}), drain: drain}
	wk.stopping = sync.OnceValue(func() <-chan struct{} {
		c := make(chan struct{})
		go func() {
			defer close(c)
			select {
			case <-wk.stop:
			case <-wk.rescale:
			case <-wk.drain:
			}
		}()
		return c
	})
	return wk
}

// RequestStop asks it to stop, as a report's answer does.
func (wk *Work) RequestStop() { wk.requestStop() }

func (wk *Work) requestStop() { wk.stopOnce.Do(func() { close(wk.stop) }) }

// RequestDrain asks it to stop at its next checkpoint and end with DRAINING,
// as a rescale's report answer does.
func (wk *Work) RequestDrain() { wk.requestDrain() }

func (wk *Work) requestDrain() { wk.rescaleOnce.Do(func() { close(wk.rescale) }) }

// Cancelled reports whether it was asked to stop: it was cancelled, or
// something else may have replaced it.
func (wk *Work) Cancelled() bool {
	select {
	case <-wk.stop:
		return true
	default:
		return false
	}
}

// Draining is closed once the runner is stopping.
func (wk *Work) Draining() <-chan struct{} { return wk.drain }

// Stopping is closed once it was asked to stop, or the runner is stopping.
func (wk *Work) Stopping() <-chan struct{} { return wk.stopping() }

// SetEpoch records the last epoch it committed, for reports.
func (wk *Work) SetEpoch(n uint64) { wk.epoch.Store(n) }

type workKey struct{}

// NewContext returns ctx carrying wk.
func NewContext(ctx context.Context, wk *Work) context.Context {
	return context.WithValue(ctx, workKey{}, wk)
}

// From returns the work ctx carries, or idle work that never stops.
func From(ctx context.Context) *Work {
	if wk, ok := ctx.Value(workKey{}).(*Work); ok {
		return wk
	}
	return NewWork(Spec{}, nil)
}

// LateStart fails if the work starts more than its heartbeat timeout after
// the API handed it over, as when the runner was frozen. Its lease has ended,
// and what replaced it may be running: claiming the pipeline would fence that.
func (wk *Work) LateStart() error {
	if late := time.Since(wk.Received); wk.HeartbeatTimeout > 0 && late > wk.HeartbeatTimeout {
		return errs.New(errs.Transient, "the attempt starts %s after it was handed out, past its heartbeat timeout, so another may have replaced it", late.Round(time.Second))
	}
	return nil
}
