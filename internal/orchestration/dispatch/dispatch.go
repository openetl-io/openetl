package dispatch

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/orchestration/sizing"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/resources"
	"github.com/openetl-io/openetl/internal/store"
	"github.com/openetl-io/openetl/internal/store/db"
)

// Waker tells a workflow that work it sent has finished. It returns nil if
// the workflow is gone.
type Waker interface {
	Wake(ctx context.Context, workflowID, workID string) error
}

// Options are the dispatcher's timings; zero means production's.
type Options struct {
	// How long a runner's report counts for placing work on it. Zero: 30 s.
	ReportLapse time.Duration
	// How long a runner has to start an attempt placed on it. Zero: 30 s.
	StartTimeout time.Duration
	// How long a request waits before it reserves a runner, or fails if none
	// could take it. Zero: DefaultReserveAfter.
	ReserveAfter time.Duration
	// How often Run looks at leases, placements and wakes. Zero: 1 s.
	Every time.Duration
	// How long a poll waits for work. Zero: 20 s.
	PollWait time.Duration
	// How long finished attempts are kept, and light operations, whose results
	// a workflow has by then. Zero: a day, and 10 minutes.
	History, LightHistory time.Duration
}

// Dispatcher is the control plane's side of dispatch.
type Dispatcher struct {
	Store   *store.Store
	Waker   Waker
	Options Options
	Logger  *slog.Logger

	hub      hub
	stopOnce sync.Once
	stopped  chan struct{}
	initOnce sync.Once
}

func (d *Dispatcher) init() {
	d.initOnce.Do(func() { d.stopped = make(chan struct{}) })
}

func (d *Dispatcher) log() *slog.Logger {
	if d.Logger == nil {
		return slog.New(slog.DiscardHandler)
	}
	return d.Logger
}

func (d *Dispatcher) lapse() time.Duration {
	return cmp.Or(d.Options.ReportLapse, 30*time.Second)
}

func (d *Dispatcher) reserveAfter() time.Duration {
	return cmp.Or(d.Options.ReserveAfter, DefaultReserveAfter)
}

func planeKey(org uuid.UUID, dataPlane string) string { return org.String() + "/" + dataPlane }

func marshalOutcome(o contract.WorkOutcome) []byte {
	b, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return b
}

// Enqueue records work a workflow sends, once per ID, and places it if it's
// an attempt with room waiting.
func (d *Dispatcher) Enqueue(ctx context.Context, in contract.WorkInput, needs []Need) error {
	org, err := uuid.Parse(in.OrgID)
	if err != nil {
		return err
	}
	input, err := json.Marshal(in.Input)
	if err != nil {
		return err
	}
	needed, err := json.Marshal(needs)
	if err != nil {
		return err
	}
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	trace, err := json.Marshal(carrier)
	if err != nil {
		return err
	}
	start := in.Pickup
	if in.Light == "" {
		start = cmp.Or(d.Options.StartTimeout, 30*time.Second)
	}
	err = d.Store.Tx(ctx, func(q *db.Queries) error {
		_, err := q.EnqueueDispatch(ctx, db.EnqueueDispatchParams{
			OrgID: org, ID: in.ID, DataPlaneID: in.DataPlane, WorkflowID: in.WorkflowID, Kind: in.Kind, Input: input,
			Light: in.Light, Pool: cmp.Or(in.Pool, defaultPool), SizeCpu: in.Size.CPU, SizeMemory: in.Size.Memory, Priority: int16(in.Priority), //nolint:gosec // a few priorities
			Needs: needed, Trace: trace, HeartbeatMs: in.Heartbeat.Milliseconds(), StartTimeoutMs: start.Milliseconds(), TimeoutMs: in.Timeout.Milliseconds(),
		})
		if err != nil {
			return err
		}
		if in.SyncID != "" {
			pipeline, err := uuid.Parse(in.PipelineID)
			if err != nil {
				return err
			}
			sync, err := uuid.Parse(in.SyncID)
			if err != nil {
				return err
			}
			err = q.InsertAttempt(ctx, db.InsertAttemptParams{
				OrgID: org, ID: in.ID, PipelineID: pipeline, SyncID: sync, Attempt: in.Attempt,
				SizeCpu: in.Size.CPU, SizeMemory: in.Size.Memory, SizeReason: in.SizeReason,
			})
			if err != nil {
				return err
			}
		}
		if in.Light != "" {
			return q.NotifyDispatch(ctx, planeKey(org, in.DataPlane))
		}
		return nil
	})
	if err != nil {
		return err
	}
	if in.Light != "" {
		return nil
	}
	return d.pass(ctx, org, in.DataPlane)
}

const defaultPool = sizing.DefaultPool

// Capacities are the capacities of the pool's runners reporting now, that
// aren't stopping.
func (d *Dispatcher) Capacities(ctx context.Context, org uuid.UUID, dataPlane, pool string) ([]resources.Size, error) {
	rows, err := d.Store.PlacementRunners(ctx, db.PlacementRunnersParams{OrgID: org, DataPlaneID: dataPlane, LapseMs: d.lapse().Milliseconds()})
	if err != nil {
		return nil, err
	}
	var out []resources.Size
	for _, r := range rows {
		if r.Pool == pool && !r.Draining {
			out = append(out, resources.Size{CPU: r.CapacityCpu, Memory: r.CapacityMemory})
		}
	}
	return out, nil
}

// Status reports whether the work finished, and how. Work that's gone counts
// as lost, so the workflow moves on.
func (d *Dispatcher) Status(ctx context.Context, ref contract.WorkRef) (contract.WorkStatus, error) {
	org, err := uuid.Parse(ref.OrgID)
	if err != nil {
		return contract.WorkStatus{}, err
	}
	row, err := d.Store.GetDispatchOutcome(ctx, db.GetDispatchOutcomeParams{OrgID: org, ID: ref.ID})
	if errors.Is(err, pgx.ErrNoRows) {
		return contract.WorkStatus{Done: true, Outcome: lost()}, nil
	}
	if err != nil {
		return contract.WorkStatus{}, err
	}
	if row.State != stateFinished {
		return contract.WorkStatus{}, nil
	}
	st := contract.WorkStatus{Done: true}
	if err := json.Unmarshal(row.Outcome, &st.Outcome); err != nil {
		return st, fmt.Errorf("dispatch: outcome of %s: %w", ref.ID, err)
	}
	return st, nil
}

const stateFinished = "finished"

func lost() contract.WorkOutcome {
	return contract.WorkOutcome{Code: errs.Transient.String(), Message: errs.New(errs.Transient, "its runner stopped reporting it, or it ran out of time").Error()}
}

// Cancel stops work: at once if no runner has started it, and otherwise on
// its runner's next report.
func (d *Dispatcher) Cancel(ctx context.Context, ref contract.WorkRef) error {
	org, err := uuid.Parse(ref.OrgID)
	if err != nil {
		return err
	}
	n, err := d.Store.CancelWaiting(ctx, db.CancelWaitingParams{OrgID: org, ID: ref.ID, Outcome: marshalOutcome(contract.WorkOutcome{Cancelled: true})})
	if err != nil || n > 0 {
		return err
	}
	return d.Store.CancelRunning(ctx, db.CancelRunningParams{OrgID: org, ID: ref.ID})
}

// Report is a runner's report.
type Report struct {
	Org       uuid.UUID
	DataPlane string
	Runner    db.ReportRunnerParams
	Running   []RunningWork
}

// RunningWork is what a report says of one running piece of work.
type RunningWork struct {
	ID    string
	Epoch int64
	Size  resources.Size
	// Nil for a light operation.
	Usage *AttemptUsage
}

// AttemptUsage is what an attempt has used so far, and what its source has
// emitted.
type AttemptUsage struct {
	resources.Usage
	Rows, Bytes int64
}

// recordUsage records what attempts have used, of those its caller has checked
// are its runner's.
func recordUsage(ctx context.Context, q *db.Queries, org uuid.UUID, works []RunningWork) error {
	var p db.RecordAttemptUsageParams
	for _, w := range works {
		if u := w.Usage; u != nil {
			p.Ids = append(p.Ids, w.ID)
			p.CpuSeconds = append(p.CpuSeconds, u.CPU)
			p.PeakMemory = append(p.PeakMemory, u.Memory)
			p.RunnerCpuSeconds = append(p.RunnerCpuSeconds, u.RunnerCPU)
			p.RunnerMemory = append(p.RunnerMemory, u.RunnerMemory)
			p.Seconds = append(p.Seconds, u.Seconds)
			p.Rows = append(p.Rows, u.Rows)
			p.Bytes = append(p.Bytes, u.Bytes)
		}
	}
	if len(p.Ids) == 0 {
		return nil
	}
	p.OrgID = org
	return q.RecordAttemptUsage(ctx, p)
}

// Report records a runner's report and extends the leases of what it runs.
// It returns the work the runner should stop.
func (d *Dispatcher) Report(ctx context.Context, r Report) (stop []string, err error) {
	ids, epochs := make([]string, len(r.Running)), make([]int64, len(r.Running))
	for i, w := range r.Running {
		ids[i], epochs[i] = w.ID, w.Epoch
	}
	err = d.Store.Tx(ctx, func(q *db.Queries) error {
		id := r.Runner.ID
		live, err := q.ExtendLeases(ctx, db.ExtendLeasesParams{Epochs: epochs, Ids: ids, OrgID: r.Org, DataPlaneID: r.DataPlane, Runner: &id})
		if err != nil {
			return err
		}
		keep := map[string]bool{}
		for _, l := range live {
			keep[l.ID] = true
			if l.Cancel {
				stop = append(stop, l.ID)
			}
		}
		params := r.Runner
		params.OrgID, params.DataPlaneID = r.Org, r.DataPlane
		var own []RunningWork
		for _, w := range r.Running {
			if keep[w.ID] {
				own = append(own, w)
				continue
			}
			stop = append(stop, w.ID)
			params.OrphanCpu += w.Size.CPU
			params.OrphanMemory += w.Size.Memory
		}
		if err := recordUsage(ctx, q, r.Org, own); err != nil {
			return err
		}
		if params.Draining {
			if err := q.ReturnPlaced(ctx, db.ReturnPlacedParams{OrgID: r.Org, DataPlaneID: r.DataPlane, Runner: &id}); err != nil {
				return err
			}
		}
		return q.ReportRunner(ctx, params)
	})
	slices.Sort(stop)
	return stop, err
}

// Poll waits for work for the runner, and hands it over.
func (d *Dispatcher) Poll(ctx context.Context, org uuid.UUID, dataPlane, runner string, freeOps, freeLogs int32) ([]db.Dispatch, error) {
	d.init()
	timer := time.NewTimer(cmp.Or(d.Options.PollWait, 20*time.Second))
	defer timer.Stop()
	key := planeKey(org, dataPlane)
	for {
		// Before claiming, so what's enqueued after the claim wakes it.
		wake := d.hub.wait(key)
		work, err := d.claim(ctx, org, dataPlane, runner, freeOps, freeLogs)
		if ctx.Err() != nil {
			d.disconnected(context.WithoutCancel(ctx), org, dataPlane, runner)
			return nil, ctx.Err()
		}
		if err != nil || len(work) > 0 {
			return work, err
		}
		select {
		case <-wake:
		// NOTIFY doesn't reach everywhere, such as through a transaction pooler.
		case <-time.After(pollFallback):
		case <-timer.C:
			return nil, nil
		case <-d.stopped:
			return nil, nil
		case <-ctx.Done():
			d.disconnected(context.WithoutCancel(ctx), org, dataPlane, runner)
			return nil, ctx.Err()
		}
	}
}

const pollFallback = 5 * time.Second

// disconnected takes a runner whose poll was dropped out of placement, without
// waiting for its reports to lapse, and sends what's placed on it elsewhere. A
// dropped connection usually means the runner died; if it didn't, its next
// report brings it back.
func (d *Dispatcher) disconnected(ctx context.Context, org uuid.UUID, dataPlane, runner string) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	err := d.Store.Tx(ctx, func(q *db.Queries) error {
		if err := q.RunnerDisconnected(ctx, db.RunnerDisconnectedParams{LapseMs: d.lapse().Milliseconds(), OrgID: org, DataPlaneID: dataPlane, ID: runner}); err != nil {
			return err
		}
		return q.ReturnPlaced(ctx, db.ReturnPlacedParams{OrgID: org, DataPlaneID: dataPlane, Runner: &runner})
	})
	if err != nil {
		d.log().Warn("taking a disconnected runner out of placement failed; its reports lapse", "runner", runner, "err", err)
	}
}

func (d *Dispatcher) claim(ctx context.Context, org uuid.UUID, dataPlane, runner string, freeOps, freeLogs int32) ([]db.Dispatch, error) {
	id := &runner
	work, err := d.Store.ClaimPlaced(ctx, db.ClaimPlacedParams{OrgID: org, DataPlaneID: dataPlane, Runner: id})
	if err != nil {
		return nil, err
	}
	for _, l := range []struct {
		light string
		free  int32
	}{{contract.LightOps, freeOps}, {contract.LightLogs, freeLogs}} {
		if l.free <= 0 {
			continue
		}
		rows, err := d.Store.ClaimLight(ctx, db.ClaimLightParams{Runner: id, OrgID: org, DataPlaneID: dataPlane, Light: l.light, MaxRows: l.free})
		if err != nil {
			return work, err
		}
		work = append(work, rows...)
	}
	return work, nil
}

// Result records what a runner says of its work, and wakes the workflow
// waiting for it. It returns false if the work isn't the runner's any more.
func (d *Dispatcher) Result(ctx context.Context, org uuid.UUID, dataPlane, runner, id string, out contract.WorkOutcome, noCapacity bool, usage *AttemptUsage) (bool, error) {
	who := &runner
	if noCapacity {
		n, err := d.Store.ReplaceRunning(ctx, db.ReplaceRunningParams{OrgID: org, DataPlaneID: dataPlane, ID: id, Runner: who})
		return n > 0, err
	}
	var n int64
	err := d.Store.Tx(ctx, func(q *db.Queries) error {
		var err error
		n, err = q.FinishRunning(ctx, db.FinishRunningParams{Outcome: marshalOutcome(out), OrgID: org, DataPlaneID: dataPlane, ID: id, Runner: who})
		if err != nil || n == 0 {
			return err
		}
		if err := recordUsage(ctx, q, org, []RunningWork{{ID: id, Usage: usage}}); err != nil {
			return err
		}
		return q.SetAttemptCode(ctx, db.SetAttemptCodeParams{OrgID: org, ID: id, Code: out.Code})
	})
	if err != nil {
		return false, err
	}
	if n == 0 {
		return d.Store.IsFinishedByRunner(ctx, db.IsFinishedByRunnerParams{OrgID: org, DataPlaneID: dataPlane, ID: id, Runner: who})
	}
	row, err := d.Store.GetDispatchWorkflow(ctx, db.GetDispatchWorkflowParams{OrgID: org, ID: id})
	if err != nil {
		return true, err
	}
	if err := d.wake(ctx, org, id, row); err != nil {
		// Run tries again.
		d.log().Warn("waking the workflow failed", "work", id, "workflow", row, "err", err)
	}
	return true, nil
}

func (d *Dispatcher) wake(ctx context.Context, org uuid.UUID, id, workflow string) error {
	if d.Waker == nil {
		return nil
	}
	if err := d.Waker.Wake(ctx, workflow, id); err != nil {
		return err
	}
	return d.Store.MarkWoken(ctx, db.MarkWokenParams{OrgID: org, ID: id})
}

// pass gives the room on a data plane's runners to the attempts waiting.
func (d *Dispatcher) pass(ctx context.Context, org uuid.UUID, dataPlane string) error {
	return d.Store.Tx(ctx, func(q *db.Queries) error {
		key := planeKey(org, dataPlane)
		if err := q.LockPlacement(ctx, key); err != nil {
			return err
		}
		pending, err := q.PendingAttempts(ctx, db.PendingAttemptsParams{OrgID: org, DataPlaneID: dataPlane, MaxRows: maxPending})
		if err != nil || len(pending) == 0 {
			return err
		}
		rows, err := q.PlacementRunners(ctx, db.PlacementRunnersParams{OrgID: org, DataPlaneID: dataPlane, LapseMs: d.lapse().Milliseconds()})
		if err != nil {
			return err
		}
		runners := make([]Runner, len(rows))
		reserved := int64(0)
		for i, r := range rows {
			reserved += r.UsedCpu
			runners[i] = Runner{
				RunnerFacts: RunnerFacts{ID: r.ID, Release: r.Release, Protocol: r.Protocol, Platform: r.Platform, Features: r.Features, Launchers: r.Launchers},
				Pool:        r.Pool, Draining: r.Draining, Attempts: int(r.Attempts),
				Capacity: resources.Size{CPU: r.CapacityCpu, Memory: r.CapacityMemory},
				Used:     resources.Size{CPU: r.UsedCpu + r.OrphanCpu, Memory: r.UsedMemory + r.OrphanMemory},
			}
		}
		requests := make([]Request, len(pending))
		for i, p := range pending {
			var needs []Need
			if err := json.Unmarshal(p.Needs, &needs); err != nil {
				return fmt.Errorf("dispatch: needs of %s: %w", p.ID, err)
			}
			requests[i] = Request{
				ID: p.ID, Org: org.String(), Pool: p.Pool, Size: resources.Size{CPU: p.SizeCpu, Memory: p.SizeMemory},
				Priority: int(p.Priority), Waited: time.Duration(p.WaitedMs) * time.Millisecond, Needs: needs,
			}
		}
		decisions := Plan(runners, requests, map[string]int64{org.String(): reserved}, d.reserveAfter())
		for _, dec := range decisions {
			if dec.Runner == "" {
				out := contract.WorkOutcome{Code: errs.ConfigInvalid.String(), Message: errs.New(errs.ConfigInvalid, "%s", dec.Failure).Error(), NonRetryable: true}
				if err := q.FinishDispatch(ctx, db.FinishDispatchParams{OrgID: org, ID: dec.ID, Outcome: marshalOutcome(out)}); err != nil {
					return err
				}
				continue
			}
			runner := dec.Runner
			if err := q.PlaceDispatch(ctx, db.PlaceDispatchParams{OrgID: org, ID: dec.ID, Runner: &runner}); err != nil {
				return err
			}
		}
		if len(decisions) == 0 {
			return nil
		}
		return q.NotifyDispatch(ctx, key)
	})
}

// maxPending bounds the attempts one pass looks at, the first by priority and
// age; a pass places far fewer than this.
const maxPending = 1000

// Run looks after what no request does: work whose deadlines pass, attempts
// that can have room now, and workflows not yet told their work finished. Any
// number of them can run, as each step is safe to repeat.
func (d *Dispatcher) Run(ctx context.Context) error {
	t := time.NewTicker(cmp.Or(d.Options.Every, time.Second))
	defer t.Stop()
	var cleaned, warned time.Time
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		orgs, err := d.Store.ListOrgs(ctx)
		if err == nil {
			clean := time.Since(cleaned) >= time.Minute
			for _, org := range orgs {
				err = errors.Join(err, d.tick(ctx, org, clean))
			}
			if clean {
				cleaned = time.Now()
			}
		}
		if err != nil && ctx.Err() == nil && time.Since(warned) >= time.Minute {
			d.log().Warn("looking after dispatched work failed; trying again", "err", err)
			warned = time.Now()
		}
	}
}

func (d *Dispatcher) tick(ctx context.Context, org uuid.UUID, clean bool) error {
	if _, err := d.Store.ReturnUnstarted(ctx, org); err != nil {
		return err
	}
	noRunner := contract.WorkOutcome{Code: contract.NoRunner, Message: "no runner took it in time", NonRetryable: true}
	if _, err := d.Store.ExpireUnclaimed(ctx, db.ExpireUnclaimedParams{OrgID: org, Outcome: marshalOutcome(noRunner)}); err != nil {
		return err
	}
	if _, err := d.Store.ExpireLeases(ctx, db.ExpireLeasesParams{OrgID: org, Outcome: marshalOutcome(lost())}); err != nil {
		return err
	}
	planes, err := d.Store.PendingDataPlanes(ctx, org)
	if err != nil {
		return err
	}
	for _, p := range planes {
		if err := d.pass(ctx, org, p); err != nil {
			return err
		}
	}
	unwoken, err := d.Store.UnwokenDispatches(ctx, db.UnwokenDispatchesParams{OrgID: org, MaxRows: 100})
	if err != nil {
		return err
	}
	for _, u := range unwoken {
		if err := d.wake(ctx, org, u.ID, u.WorkflowID); err != nil {
			return err
		}
	}
	if clean {
		before, light := time.Now().Add(-cmp.Or(d.Options.History, 24*time.Hour)), time.Now().Add(-cmp.Or(d.Options.LightHistory, 10*time.Minute))
		return d.Store.DeleteFinishedDispatches(ctx, db.DeleteFinishedDispatchesParams{OrgID: org, Before: &before, LightBefore: &light})
	}
	return nil
}

// hub wakes long polls of a data plane when there may be work for them.
type hub struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

// wait returns a channel closed by the next wake of key.
func (h *hub) wait(key string) <-chan struct{} {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.waiters == nil {
		h.waiters = map[string]chan struct{}{}
	}
	c, ok := h.waiters[key]
	if !ok {
		c = make(chan struct{})
		h.waiters[key] = c
	}
	return c
}

func (h *hub) wake(key string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.waiters[key]; ok {
		close(c)
		delete(h.waiters, key)
	}
}

// Listen wakes this server's long polls when the database says there's work,
// until ctx ends, and then ends the polls. Polls also look now and then, so a
// lost connection only slows them.
func (d *Dispatcher) Listen(ctx context.Context) error {
	d.init()
	defer d.stopOnce.Do(func() { close(d.stopped) })
	var warned time.Time
	for ctx.Err() == nil {
		err := d.Store.Listen(ctx, notifyChannel, d.hub.wake)
		if ctx.Err() != nil {
			break
		}
		if time.Since(warned) >= time.Minute {
			d.log().Warn("listening for dispatched work failed; long polls look every few seconds meanwhile", "err", err)
			warned = time.Now()
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second):
		}
	}
	return nil
}

const notifyChannel = "openetl_dispatch"
