package sim

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/internal/runner/flinktest"
	"github.com/openetl-io/openetl/sdk/go/destination"
)

// The flink kind runs a continuous pipeline with a Flink job
// before one of the other sinks: flinktest's fake job passes each stream to
// the output of its name. Each attempt drains a while after its source has
// read everything; a drained attempt ends the seed only if every record is
// visible, otherwise the next attempt goes on, as a continuous run does.

// flinkRun numbers the fake job's runs of tasks for the scheduler, apart
// from attempts: a crash at one of its points is a TaskManager failure.
const flinkRun = 1000

// flinkWorld is what the flink kind adds to the world.
type flinkWorld struct {
	bridge  *flinktest.Bridge
	cluster *flinktest.Cluster
	// Delays from an attempt's source finishing to its drain, by attempt.
	drainAfter []time.Duration
	// Closed once the seed's attempts are done.
	ended chan struct{}

	mu           sync.Mutex
	declines     map[int64]bool
	byHand       map[int64]bool
	fails        map[int64]bool
	noHA         int  // restarts left that restore without HA
	usedNoHA     bool // a restart restored without HA
	positionLost bool
	problems     []string
	stats        map[string]int
}

var errNotDelivered = errors.New("sim: drained before every record was visible")

// flinkOutcome is an attempt's outcome as the scheduler takes it: a drain
// ends the seed once every record is visible, and a job restored where the
// records can't follow ends it as a pass, if a restart without HA caused it.
func (w *world) flinkOutcome(err error) error {
	f := w.flink
	switch {
	case errors.Is(err, runner.ErrDrained):
		f.count("drained attempts")
		if w.delivered() {
			return nil
		}
		return errNotDelivered
	case errs.CodeOf(err) == errs.PositionLost && f.noHAUsed():
		f.mu.Lock()
		f.positionLost = true
		f.mu.Unlock()
		f.count("position lost")
		return nil
	}
	return err
}

func (f *flinkWorld) count(stat string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stats[stat]++
}

// endFlink stops the cluster's jobs once the seed's attempts are done.
func (w *world) endFlink(stats map[string]int) {
	f := w.flink
	close(f.ended)
	w.sched.crashAll()
	_ = f.cluster.CancelAll(context.Background())
	f.mu.Lock()
	defer f.mu.Unlock()
	w.positionLost = f.positionLost
	for k, v := range f.stats {
		stats[k] += v
	}
	stats["flink deploys"] += f.cluster.Deploys()
}

func newFlinkWorld(rng *rand.Rand, sched *scheduler) *flinkWorld {
	f := &flinkWorld{bridge: flinktest.NewBridge(), declines: map[int64]bool{}, byHand: map[int64]bool{}, fails: map[int64]bool{}, stats: map[string]int{}, ended: make(chan struct{})}
	for range maxAttempts + 1 {
		f.drainAfter = append(f.drainAfter, time.Duration(rng.IntN(120))*time.Second)
	}
	pick := func(m map[int64]bool, n int) {
		for range n {
			m[int64(1+rng.IntN(30))] = true
		}
	}
	pick(f.declines, rng.IntN(3))
	pick(f.byHand, rng.IntN(4))
	pick(f.fails, rng.IntN(3))
	if rng.IntN(8) == 0 {
		f.noHA = 1
	}
	c := flinktest.NewCluster(f.bridge)
	// Each fault fires once: checkpoint IDs repeat across jobs.
	once := func(m map[int64]bool, stat string) func(int64) bool {
		return func(id int64) bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			hit := m[id]
			delete(m, id)
			if hit {
				f.stats[stat]++
			}
			return hit
		}
	}
	c.MaxRestarts = 12
	c.FailOnceMore = rng.IntN(2) == 0
	c.Faults = flinktest.Faults{
		AbortOnStop:        rng.IntN(3) == 0,
		DeclineAfterRecord: once(f.declines, "flink declines"),
		SavepointByHand:    once(f.byHand, "flink savepoints by hand"),
		FailTasks:          once(f.fails, "flink task failures"),
		NoHA: func() bool {
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.noHA == 0 {
				return false
			}
			f.noHA--
			f.usedNoHA = true
			return true
		},
	}
	c.Logf = sched.tracef
	c.Point = func(ctx context.Context, run int, op string, apply func() error) error {
		return outcome(sched.point(ctx, flinkRun+run, op), apply)
	}
	f.cluster = c
	return f
}

func (f *flinkWorld) problem(format string, args ...any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.problems = append(f.problems, fmt.Sprintf(format, args...))
}

func (f *flinkWorld) noHAUsed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.usedNoHA
}

// simFlink is one attempt's view of the cluster: each call is a yield
// point of the attempt.
type simFlink struct {
	w       *world
	attempt int
}

var _ runner.Flink = (*simFlink)(nil)

func (s *simFlink) point(ctx context.Context, op string, apply func() error) error {
	return outcome(s.w.sched.point(ctx, s.attempt, op), apply)
}

func (s *simFlink) CancelAll(ctx context.Context) error {
	return s.point(ctx, "flink.cancelAll", func() error { return s.w.flink.cluster.CancelAll(ctx) })
}

func (s *simFlink) Completed(ctx context.Context, job string) (cs []runner.FlinkCheckpoint, err error) {
	err = s.point(ctx, "flink.completed", func() error { cs, err = s.w.flink.cluster.Completed(ctx, job); return err })
	return cs, err
}

func (s *simFlink) SavepointDir(name string) string { return s.w.flink.cluster.SavepointDir(name) }

func (s *simFlink) Prune(ctx context.Context, keep []string) error {
	return s.point(ctx, "flink.prune", func() error { return s.w.flink.cluster.Prune(ctx, keep) })
}

func (s *simFlink) Savepoint(ctx context.Context, dir string) (p string, err error) {
	err = s.point(ctx, "flink.savepoint", func() error { p, err = s.w.flink.cluster.Savepoint(ctx, dir); return err })
	return p, err
}

func (s *simFlink) Deploy(ctx context.Context, d runner.FlinkDeploy) (runner.FlinkJob, error) {
	var j runner.FlinkJob
	err := s.point(ctx, "flink.deploy", func() error {
		var err error
		j, err = s.w.flink.cluster.Deploy(ctx, d)
		return err
	})
	if err != nil {
		return nil, err
	}
	return &simFlinkJob{FlinkJob: j, s: s}, nil
}

type simFlinkJob struct {
	runner.FlinkJob
	s *simFlink
}

func (j *simFlinkJob) Stop(ctx context.Context, dir string) error {
	if err := j.s.point(ctx, "flink.stop", nop); err != nil {
		return err
	}
	return j.FlinkJob.Stop(ctx, dir)
}

func (j *simFlinkJob) Cancel(ctx context.Context) error {
	return j.s.point(ctx, "flink.cancel", func() error { return j.FlinkJob.Cancel(ctx) })
}

// flinkFlow is the engine of a Flink attempt: the first dataflow sends the
// inputs to the job, and the second computes each output row's _openetl_id
// again, as the engine's output lane does.
type flinkFlow struct {
	first, second runner.Passthrough
}

func (f flinkFlow) Run(ctx context.Context, plan runner.Plan, in <-chan runner.Frame, out chan<- runner.Frame) error {
	if plan.ReportSchemas {
		return f.first.Run(ctx, plan, in, out)
	}
	return sparkFlow{second: f.second}.Run(ctx, plan, in, out)
}

// flinkJob makes an attempt's job a Flink pipeline's.
func (w *world) flinkJob(j *runner.Job, attempt int, src *simSource) {
	j.Sync, j.Continuous = "", true
	j.Flink, j.Bridge = &simFlink{w: w, attempt: attempt}, w.flink.bridge
	// The scheduler holds actors at its points for long stretches of
	// virtual time; a job's connections aren't late.
	j.FlinkConnectTimeout = 365 * 24 * time.Hour
	j.Dataflow = flinkFlow{first: runner.Passthrough{Convert: toArrow}, second: j.Dataflow.(runner.Passthrough)}
	drain := make(chan struct{})
	j.Drain = drain
	var once sync.Once
	delay := w.flink.drainAfter[min(attempt, len(w.flink.drainAfter)-1)]
	src.onDone = func() {
		once.Do(func() {
			go func() {
				select {
				case <-time.After(delay):
					close(drain)
				case <-w.flink.ended:
				}
			}()
		})
	}
	last := uint64(0)
	j.OnCommit = func(n uint64) {
		if n <= last {
			w.flink.problem("a%d committed epoch %d after %d", attempt, n, last)
		}
		last = n
	}
}

// delivered says whether every record is visible, so a drained attempt
// ends the seed.
func (w *world) delivered() bool {
	for _, s := range w.cfg.streams {
		u := w.upstreams[s.name]
		if w.cfg.mode == destination.Merge {
			latest := map[string]record{}
			for _, r := range u.records {
				latest[r.Key] = r
			}
			w.wh.mu.Lock()
			got := w.wh.merged[s.name]
			ok := len(got) == len(latest)
			for k, r := range latest {
				ok = ok && got[k].Seq == r.Seq
			}
			w.wh.mu.Unlock()
			if !ok {
				return false
			}
			continue
		}
		seen := map[string]bool{}
		for _, r := range w.visibleLocked(s.name) {
			seen[r.ID] = true
		}
		for _, r := range u.records {
			if !seen[recordID(s.name, r.Key, r.Seq)] {
				return false
			}
		}
	}
	return true
}

// checkFlink returns problems with the records the runner wrote: one for a
// checkpoint that never completed, which the runner didn't rewind past, or
// for a savepoint taken by hand.
func (w *world) checkFlink(written []*checkpoint.Record) []string {
	f := w.flink
	f.mu.Lock()
	problems := append([]string(nil), f.problems...)
	f.mu.Unlock()
	for _, rec := range written {
		if fc := rec.Flink; fc != nil && f.cluster.TakenByHand(fc.Job, fc.Checkpoint) {
			problems = append(problems, fmt.Sprintf("record %d-g%d is for %s's savepoint %d, taken by hand", rec.Epoch, rec.Generation, fc.Job, fc.Checkpoint))
		}
	}
	// The chain HEAD ends, as far as pruning left it.
	ctx := context.Background()
	data, _, err := w.store.Get(ctx, "org/pipe/_checkpoints/HEAD")
	if err != nil {
		return append(problems, fmt.Sprintf("read HEAD: %v", err))
	}
	var head checkpoint.Head
	if err := json.Unmarshal(data, &head); err != nil {
		return append(problems, fmt.Sprintf("decode HEAD: %v", err))
	}
	log := checkpoint.NewLog(w.store, "org", "pipe")
	for ref := head.Record; ref != nil; {
		rec, err := log.Record(ctx, *ref)
		if err != nil {
			break
		}
		// A refused restore leaves HEAD's epoch for the attempt after the
		// new snapshot to settle.
		if fc := rec.Flink; fc != nil && (!w.positionLost || ref != head.Record) {
			done := f.cluster.EverCompleted(fc.Job, fc.Checkpoint)
			if fc.Savepoint != "" {
				p, _ := f.cluster.Savepoint(ctx, fc.Savepoint)
				done = p != ""
			}
			if !done {
				problems = append(problems, fmt.Sprintf("record %d-g%d stands at %s's checkpoint %d, which never completed", rec.Epoch, rec.Generation, fc.Job, fc.Checkpoint))
			}
		}
		ref = rec.Previous
	}
	return problems
}

func isRecordKey(key string) bool {
	return strings.Contains(key, "/_checkpoints/") && strings.HasSuffix(key, ".json")
}

func (w *world) visibleLocked(stream string) []row {
	switch w.cfg.kind {
	case "kafka":
		w.broker.mu.Lock()
		defer w.broker.mu.Unlock()
	case "webhook", "staged":
		w.recv.mu.Lock()
		defer w.recv.mu.Unlock()
	default:
		w.wh.mu.Lock()
		defer w.wh.mu.Unlock()
	}
	return slices.Clone(w.visible(stream))
}
