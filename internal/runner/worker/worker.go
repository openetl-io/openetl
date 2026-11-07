// Package worker is a runner's side of dispatch (ADR-0028 §1): it reports the
// runner to the API, long-polls for work, runs it, and reports its results.
package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
	go func() {
		defer close(polled)
		w.poll(ctx, base, resultCtx)
	}()
	<-ctx.Done()
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
	return nil
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
	w.share()
	running := slices.Collect(maps.Values(w.running))
	w.mu.Unlock()
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
	defer w.mu.Unlock()
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
	return nil
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
			w.start(base, results, m)
		}
	}
}

func (w *Worker) start(base, results context.Context, m *dataplanev1.Work) {
	wk := newWork(m, w.drain)
	kind, known := w.Kinds[wk.Kind]
	light := m.GetSize() == nil
	w.mu.Lock()
	if !light && !wk.Size.Fits(w.Capacity.Sub(w.reserved)) {
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
