// Package sim is a seeded simulation of the commit protocol. Each seed runs
// the real runner against fake sources and a fake sink under injected faults,
// crashes and zombie attempts, then checks no loss, no duplicates where
// exactly-once, and the right final rows where effectively-once. Streams
// without a primary key hold some rows twice, and check what collapses.
// Overwrite tables start with an earlier read's rows, which the sync's read
// must replace. The spark kind puts a Spark transform before one of the
// other sinks, and checks the runner keeps the transform's contract. The
// flink kind runs a continuous pipeline with a fake Flink job before one of
// them, and checks the records against what the job did.
//
//	go test ./test/sim -seeds=10000         # more seeds per sink class
//	go test ./test/sim -run 'TestSimulation/kafka' -seed=42 -v   # one seed, with its trace
package sim

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var (
	seedsFlag = flag.Int("seeds", 300, "seeds to run per sink kind")
	seedFlag  = flag.Uint64("seed", 0, "run only this seed")
	traceFlag = flag.Bool("simtrace", false, "print the scheduler trace of every seed")
)

// parquet is stage-and-apply like warehouse, but can't dedupe or commit
// across tables. spark is one of the others with a Spark transform before
// it, whose outputs take their inputs' names and settings; flink is the
// same with a Flink job, in a continuous pipeline. holds is the warehouse
// taking holds, as Postgres does.
var sinkKinds = []string{"warehouse", "parquet", "kafka", "webhook", "staged", "spark", "flink", "holds"}

// The sinks a transform goes before.
var transformedKinds = sinkKinds[:5]

func TestSimulation(t *testing.T) {
	for _, kind := range sinkKinds {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			first, last := uint64(1), uint64(*seedsFlag)
			if *seedFlag != 0 {
				first, last = *seedFlag, *seedFlag
			}
			stats := map[string]int{}
			for seed := first; seed <= last; seed++ {
				var res seedResult
				synctest.Test(t, func(*testing.T) { res = runSeed(kind, seed) })
				for k, v := range res.stats {
					stats[k] += v
				}
				if res.failure != "" || *traceFlag {
					t.Logf("%s seed %d: %s\n%s", kind, seed, res.config, res.trace)
				}
				if res.failure != "" {
					t.Fatalf("%s seed %d: %s\nrerun: go test ./test/sim -run 'TestSimulation/%s$' -seed=%d -v", kind, seed, res.failure, kind, seed)
				}
			}
			t.Logf("%s, %d seeds: %v", kind, last-first+1, stats)
		})
	}
}

type streamCfg struct {
	name     string
	replay   source.Replay
	keyed    bool
	require  delivery.Level
	collapse *bool
	// The output of its name, which is exact when every input is.
	transformed bool
	outReplay   source.Replay
	// Nil: holds nothing.
	holds *holdCfg
}

// delivery is what the destination gets: the stream, or the transform's
// output of its name.
func (s streamCfg) delivery(mode destination.WriteMode) delivery.Stream {
	if s.transformed {
		return delivery.Stream{Name: s.name, Replay: s.outReplay, Mode: mode, Keyed: s.keyed, Require: s.require, Transformed: true}
	}
	return delivery.Stream{Name: s.name, Replay: s.replay, Mode: mode, Keyed: s.keyed, Require: s.require, Collapse: s.collapse}
}

type config struct {
	kind     string
	spark    bool
	flink    bool
	holds    bool
	streams  []streamCfg
	mode     destination.WriteMode
	settings delivery.Settings
	faults   int
	zombies  int
	// Checkpoint records each attempt keeps.
	keep int
	// Holds: held values a replay reads at a time, how likely a retry takes
	// a release of each stream's holds, and whether releases come without
	// fixing what held them.
	chunk, releasePct int
	unfixed           bool
}

func (c config) String() string {
	var b strings.Builder
	if c.spark {
		b.WriteString("spark ")
	}
	if c.flink {
		b.WriteString("flink ")
	}
	fmt.Fprintf(&b, "sink=%s mode=%d scope=%d require=%v maxBytes=%d interval=%v resumeTimeout=%v faults=%d zombies=%d keep=%d streams=",
		c.kind, c.mode, c.settings.Scope, c.settings.Require, c.settings.CommitMaxBytes,
		c.settings.CommitInterval, c.settings.ResumePointTimeout, c.faults, c.zombies, c.keep)
	for _, s := range c.streams {
		fmt.Fprintf(&b, "%s(%v, keyed=%v", s.name, s.replay, s.keyed)
		if s.require != 0 {
			fmt.Fprintf(&b, ", require=%v", s.require)
		}
		if s.collapse != nil {
			fmt.Fprintf(&b, ", collapse=%v", *s.collapse)
		}
		if h := s.holds; h != nil {
			fmt.Fprintf(&b, ", cells=%v, held=%d %s, wideAt=%d, fixedType=%v", h.cells, h.held.Mode, h.held.Value, h.wideAt, h.fixedType)
		}
		b.WriteString(") ")
	}
	if c.holds {
		fmt.Fprintf(&b, "chunk=%d release=%d%% unfixed=%v", c.chunk, c.releasePct, c.unfixed)
	}
	return b.String()
}

func randomConfig(rng *rand.Rand, kind string) config {
	spark, flink, holds := kind == "spark", kind == "flink", kind == "holds"
	if spark || flink {
		kind = transformedKinds[rng.IntN(len(transformedKinds))]
	}
	if holds {
		kind = "warehouse"
	}
	c := config{kind: kind, spark: spark, flink: flink, holds: holds, mode: destination.Append, faults: rng.IntN(9), zombies: rng.IntN(3)}
	switch {
	case holds:
		// Postgres patches append streams by _openetl_id and merge streams
		// by key. Retries take releases, so more of them.
		c.mode = []destination.WriteMode{destination.Append, destination.Merge}[rng.IntN(2)]
		c.faults += 2 + rng.IntN(8)
		c.chunk, c.releasePct, c.unfixed = 1+rng.IntN(6), 40+rng.IntN(61), rng.IntN(6) == 0
	case kind == "warehouse" && flink:
		// A continuous read never ends, so never replaces a table.
		c.mode = []destination.WriteMode{destination.Append, destination.Merge}[rng.IntN(2)]
	case kind == "warehouse":
		c.mode = []destination.WriteMode{destination.Append, destination.Overwrite, destination.Merge}[rng.IntN(3)]
	}
	for i := range 1 + rng.IntN(3) {
		r := source.Exact
		// A Flink job's restores need streams that replay exactly.
		if rng.IntN(2) == 0 && !flink {
			r = source.Superset
		}
		s := streamCfg{name: fmt.Sprintf("s%d", i), replay: r, keyed: c.mode == destination.Merge || rng.IntN(3) != 0}
		if rng.IntN(3) == 0 {
			s.require = []delivery.Level{delivery.AtLeastOnce, delivery.ExactlyOnce}[rng.IntN(2)]
		}
		// Only a sink that can dedupe can collapse, and a transform's
		// outputs never do.
		if kind == "warehouse" && !spark && !flink && c.mode != destination.Merge && rng.IntN(2) == 0 {
			s.collapse = new(rng.IntN(2) == 0)
		}
		if holds && s.keyed {
			s.holds = randomHolds(rng, c.mode, 40)
		}
		c.streams = append(c.streams, s)
	}
	if spark || flink {
		out := source.Exact
		for _, s := range c.streams {
			if s.replay != source.Exact {
				out = source.Superset
			}
		}
		for i := range c.streams {
			c.streams[i].transformed, c.streams[i].outReplay = true, out
		}
	}
	c.settings = delivery.Settings{
		Require:        delivery.ExactlyOnce,
		Scope:          destination.ScopeStream,
		CommitMaxBytes: int64(40 + rng.IntN(400)),
	}
	switch kind {
	case "warehouse":
		if rng.IntN(2) == 0 {
			c.settings.Scope = destination.ScopePipeline
		}
	case "kafka":
		c.settings.Scope = destination.ScopePipeline
	}
	if rng.IntN(3) == 0 {
		c.settings.Require = delivery.AtLeastOnce
	}
	if rng.IntN(3) == 0 || flink {
		c.settings.CommitInterval = time.Duration(5+rng.IntN(55)) * time.Second
	}
	if rng.IntN(2) == 0 {
		c.settings.ResumePointTimeout = time.Duration(1+rng.IntN(20)) * time.Second
	}
	c.keep = 2 + rng.IntN(3)
	return c
}

// world is what survives an attempt.
type world struct {
	cfg       config
	sched     *scheduler
	store     *checkpoint.MemStore
	upstreams map[string]*upstream
	wh        *warehouse
	broker    *broker
	recv      *receiver
	staged    *stagedStore
	spark     *sparkCluster
	flink     *flinkWorld
	lost      error
	// Streams forced in any record ever written, pruned ones included.
	forced map[string]bool
	// Every checkpoint record ever written.
	written []*checkpoint.Record
	// The Flink job restored where the pipeline's records can't follow,
	// which ends the seed: only a new snapshot recovers.
	positionLost bool
	// Holds: the releases taken so far, and whether they fixed what held
	// the values; the streams whose cell holds were discarded.
	relRng    *rand.Rand
	released  map[releaseKey]bool
	releases  []runner.Release
	override  bool
	discarded map[string]bool
	// The sync being run: holds seeds run a second.
	sync int
}

func (w *world) markLost(err error) {
	if errors.Is(err, errDataLost) && w.lost == nil {
		w.lost = err
	}
}

func (w *world) destination(attempt int) destination.Destination {
	switch w.cfg.kind {
	case "kafka":
		return &kafkaDest{w: w, b: w.broker, attempt: attempt}
	case "webhook":
		return &webhookDest{w: w, r: w.recv, attempt: attempt}
	case "staged":
		return &stagedAPIDest{w: w, r: w.recv, attempt: attempt}
	default:
		return &warehouseDest{w: w, wh: w.wh, attempt: attempt}
	}
}

func (w *world) job(attempt int) runner.Job {
	src := &simSource{w: w, attempt: attempt, continuous: w.cfg.flink}
	j := runner.Job{
		Owner:       fmt.Sprintf("%c%d", 'a'+w.sync, attempt),
		Sync:        fmt.Sprintf("sync-%d", w.sync),
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(&simStore{w: w, attempt: attempt}, "org", "pipe"),
		Source:      src,
		Destination: w.destination(attempt),
		Dataflow:    runner.Passthrough{Convert: toArrow},
		Settings:    w.cfg.settings,
		KeepRecords: w.cfg.keep,
	}
	if w.cfg.holds {
		j.HeldURL = "sim://held"
		j.Dataflow = &holdFlow{w: w, attempt: attempt, override: w.override, chunk: w.cfg.chunk}
		j.Releases = slices.Clone(w.releases)
	}
	if d, ok := j.Destination.(*stagedAPIDest); ok {
		st := newSimStaging(w, attempt)
		d.st, j.Staging = st, st
		j.Dataflow = runner.Passthrough{Convert: toArrow, Stage: st.stage}
	}
	for _, s := range w.cfg.streams {
		st := runner.Stream{Name: s.name, Replay: s.replay, Mode: w.cfg.mode, Require: s.require, Collapse: s.collapse, Holds: s.holds.streamHolds()}
		if s.keyed {
			st.PrimaryKey = []string{"key"}
		}
		j.Streams = append(j.Streams, st)
		if s.transformed {
			out := runner.Stream{Name: s.name, Replay: s.outReplay, Mode: w.cfg.mode, PrimaryKey: st.PrimaryKey, Require: s.require, Transformed: true}
			j.Outputs = append(j.Outputs, out)
		}
	}
	if w.cfg.spark {
		app := &sparkApp{w: w, attempt: attempt}
		j.Transform = app
		second := j.Dataflow.(runner.Passthrough)
		j.Dataflow = sparkFlow{first: runner.Passthrough{Convert: toArrow, Stage: app.stage}, second: second}
	}
	if w.cfg.flink {
		w.flinkJob(&j, attempt, src)
	}
	return j
}

type seedResult struct {
	config, trace, failure string
	stats                  map[string]int
	steps                  []string
}

func runSeed(kind string, seed uint64) (res seedResult) {
	rng := rand.New(rand.NewPCG(seed, uint64(len(kind))))
	cfg := randomConfig(rng, kind)
	w := &world{
		cfg:       cfg,
		store:     checkpoint.NewMemStore(),
		upstreams: map[string]*upstream{},
		wh:        newWarehouse(cfg.kind == "warehouse"),
		broker:    newBroker(),
		recv:      &receiver{applies: map[string]int{}},
		staged:    newStagedStore(),
		spark:     newSparkCluster(),
		forced:    map[string]bool{},
	}
	if cfg.holds {
		w.wh.holds = newHeldTables(cfg.streams)
		w.relRng, w.released, w.discarded = rand.New(rand.NewPCG(seed, 7)), map[releaseKey]bool{}, map[string]bool{}
	}
	for _, s := range cfg.streams {
		u := newUpstream(rng, s.name, s.replay, !s.keyed)
		if s.holds != nil {
			markHolds(rng, u, s.holds, 0)
		}
		u.cdc = cfg.holds
		if cfg.mode == destination.Overwrite {
			w.wh.tables[s.name] = []row{{ID: "stale-" + s.name, Key: "stale", Value: "an earlier read"}}
			if rng.IntN(5) == 0 {
				u.records, u.resumeAt = nil, []bool{true}
			}
		}
		w.upstreams[s.name] = u
	}
	// Each stream requires no more than it can get, so the plan passes and a
	// forced barrier may never lower a stream below it.
	caps := w.destination(0).Capabilities()
	var planned []delivery.Stream
	for i, s := range w.cfg.streams {
		if p := delivery.PlanStream(caps, s.delivery(cfg.mode), cfg.settings.Require); p.Level < cmp.Or(s.require, cfg.settings.Require) {
			w.cfg.streams[i].require = p.Level
		}
		planned = append(planned, w.cfg.streams[i].delivery(cfg.mode))
	}
	res.config = w.cfg.String()
	if err := delivery.Check(caps, w.cfg.settings, planned); err != nil {
		res.failure = "the generated config fails the plan: " + err.Error()
		return res
	}
	w.sched = newScheduler(rng, cfg.faults, cfg.zombies)
	if cfg.flink {
		w.flink = newFlinkWorld(rng, w.sched)
	}
	res.stats = map[string]int{"write:" + []string{"", "append", "overwrite", "merge"}[cfg.mode]: 1}

	attempts := 0
	start := func(attempt int) error {
		attempts = max(attempts, attempt)
		if cfg.holds && (attempt > 1 || w.sync > 0) {
			w.release(attempt)
		}
		j := w.job(attempt)
		j.OnCheckpoint = func(_ *checkpoint.Record, collapsed map[string]uint64) {
			for _, n := range collapsed {
				res.stats["collapsed rows"] += int(n)
			}
		}
		r, err := runner.RunAttempt(context.Background(), j)
		if app, ok := j.Transform.(*sparkApp); ok {
			app.mu.Lock()
			if app.started && !app.stopped {
				w.spark.problem("a%d returned without stopping its Spark application", attempt)
			}
			app.mu.Unlock()
		}
		if errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) {
			res.stats["fenced attempts"]++
		}
		if cfg.flink {
			err = w.flinkOutcome(err)
		}
		if err == nil {
			w.sched.tracef("a%d: gen %d, %s, restored %d, committed %d epochs, last %d", attempt, r.Generation, r.Recovery, r.Restored, r.Committed, r.Last)
			res.stats["recovery:"+r.Recovery.String()]++
		}
		return err
	}
	err := w.sched.run(start)
	zombies := cfg.zombies - w.sched.zombies
	if cfg.holds && err == nil && w.lost == nil {
		// The next sync, with the records added since, takes the releases.
		for _, s := range w.cfg.streams {
			extendUpstream(rng, w.upstreams[s.name], s.holds)
		}
		first, firstAttempts, z := w.sched, attempts, rng.IntN(3)
		w.sync, attempts = 1, 0
		w.sched = newScheduler(rng, rng.IntN(9), z)
		err = w.sched.run(start)
		w.sched.trace, w.sched.steps = append(first.trace, w.sched.trace...), append(first.steps, w.sched.steps...)
		attempts += firstAttempts
		zombies += z - w.sched.zombies
	}
	if cfg.flink {
		w.endFlink(res.stats)
	}
	res.trace = strings.Join(lastN(w.sched.trace, 400), "\n")
	res.steps = w.sched.steps
	res.stats["attempts"] += attempts
	res.stats["zombies"] += zombies
	if cfg.spark {
		res.stats["spark generations ended by a newer one"] += len(w.spark.dropped)
	}
	switch {
	case len(w.spark.problems) > 0:
		res.failure = "the Spark transform's contract: " + strings.Join(lastN(w.spark.problems, 8), "; ")
	case w.lost != nil:
		res.failure = w.lost.Error()
	case err != nil:
		res.failure = err.Error()
	default:
		res.failure = w.check(caps, res.stats)
	}
	return res
}

// check returns "" if every stream got its guarantee.
func (w *world) check(caps destination.Capabilities, stats map[string]int) string {
	forced := w.forced
	problems := w.checkLog()
	if w.cfg.flink {
		problems = append(problems, w.checkFlink(w.written)...)
	}
	for _, s := range w.cfg.streams {
		u := w.upstreams[s.name]
		ds := s.delivery(w.cfg.mode)
		require := cmp.Or(s.require, w.cfg.settings.Require)
		p := delivery.PlanStream(caps, ds, w.cfg.settings.Require)
		level, forcedDedupe := p.Level, destination.DedupeNone
		// A barrier forced on any input makes every output superset.
		if forced[s.name] || s.transformed && len(forced) > 0 {
			stats["forced streams"]++
			f := delivery.PlanStream(caps, ds.Forced(w.cfg.settings.Require), w.cfg.settings.Require)
			if f.Level < require {
				problems = append(problems, fmt.Sprintf("%s: a barrier was forced although it lowers the stream to %v, below the required %v", s.name, f.Level, require))
			} else {
				level, forcedDedupe = min(level, f.Level), f.Dedupe
			}
		}
		stats["level:"+level.String()]++
		if p.Collapse {
			stats["collapsing streams"]++
		}
		expected := map[string]record{}
		copies := map[string]int{}
		for _, r := range u.records {
			id := recordID(s.name, r.Key, r.Seq)
			expected[id] = r
			copies[id]++
		}
		for k := range w.wh.reads {
			if k.stream == s.name {
				problems = append(problems, fmt.Sprintf("%s: the rows of read %d never replaced the table", s.name, k.start))
			}
		}
		switch {
		case w.cfg.mode == destination.Merge && w.positionLost:
			// What landed, landed once; the rest needs a new snapshot.
			for id, r := range expected {
				if n := w.wh.applies[id]; n > 1 && level == delivery.ExactlyOnce {
					problems = append(problems, fmt.Sprintf("%s: %s@%d applied %d times (exactly-once)", s.name, r.Key, r.Seq, n))
				}
			}
		case w.cfg.mode == destination.Merge:
			problems = append(problems, w.checkMerge(s.name, u, expected, level)...)
		default:
			lo, hi := appendBounds(level, p.Dedupe, forcedDedupe)
			if w.positionLost {
				lo = func(int) int { return 0 }
			}
			problems = append(problems, checkAppend(s.name, w.visible(s.name), expected, copies, lo, hi)...)
		}
	}
	if w.cfg.holds {
		problems = append(problems, w.checkHolds(stats)...)
	}
	if len(problems) > 8 {
		problems = append(problems[:8], fmt.Sprintf("… and %d more", len(problems)-8))
	}
	return strings.Join(problems, "; ")
}

func (w *world) visible(stream string) []row {
	switch w.cfg.kind {
	case "kafka":
		var out []row
		for _, r := range w.broker.visible {
			if r.stream == stream {
				out = append(out, r.row)
			}
		}
		return out
	case "webhook", "staged":
		var out []row
		for _, r := range w.recv.delivered {
			if r.stream == stream {
				out = append(out, r.row)
			}
		}
		return out
	default:
		return w.wh.tables[stream]
	}
}

// appendBounds returns how many times a row the source holds m times may
// land in an append table, as functions of m: the plan's dedupe, and the
// dedupe after a restore from a forced barrier, if one was forced.
func appendBounds(level delivery.Level, dedupe, forced destination.Dedupe) (lo, hi func(m int) int) {
	same := func(m int) int { return m }
	one := func(int) int { return 1 }
	many := func(int) int { return 1 << 30 }
	lo = same
	if dedupe != destination.DedupeNone || forced != destination.DedupeNone {
		// Collapsing loses the source's second copies.
		lo = one
	}
	switch {
	case level != delivery.ExactlyOnce:
		return lo, many
	case dedupe == destination.DedupeTarget:
		return one, one
	default:
		// Exact: every copy lands once, unless a restore from a forced
		// barrier collapsed the rest of an attempt.
		return lo, same
	}
}

func checkAppend(stream string, rows []row, expected map[string]record, copies map[string]int, lo, hi func(int) int) []string {
	var problems []string
	count := map[string]int{}
	for _, r := range rows {
		if _, ok := expected[r.ID]; !ok {
			problems = append(problems, fmt.Sprintf("%s: unexpected row %+v", stream, r))
		}
		count[r.ID]++
	}
	for id, r := range expected {
		m := copies[id]
		switch n := count[id]; {
		case n == 0 && lo(m) > 0:
			problems = append(problems, fmt.Sprintf("%s: LOST %s@%d", stream, r.Key, r.Seq))
		case n < lo(m):
			problems = append(problems, fmt.Sprintf("%s: LOST a copy of %s@%d: %d of the source's %d", stream, r.Key, r.Seq, n, m))
		case n > hi(m):
			problems = append(problems, fmt.Sprintf("%s: DUPLICATED %s@%d ×%d, the source holds %d (exactly-once)", stream, r.Key, r.Seq, n, m))
		}
	}
	return problems
}

func (w *world) checkMerge(stream string, u *upstream, expected map[string]record, level delivery.Level) []string {
	var problems []string
	latest := map[string]record{}
	for _, r := range u.records {
		latest[r.Key] = r // records of a key are in increasing seq order
	}
	got := w.wh.merged[stream]
	for k, r := range latest {
		// checkHolds checks held cells.
		if g, ok := got[k]; !ok || g.Seq != r.Seq || g.HeldBy == "" && g.Value != r.Value {
			problems = append(problems, fmt.Sprintf("%s: key %s is %+v, want %s@%d", stream, k, g, r.Key, r.Seq))
		}
	}
	for k := range got {
		if _, ok := latest[k]; !ok {
			problems = append(problems, fmt.Sprintf("%s: unexpected key %s", stream, k))
		}
	}
	if level == delivery.ExactlyOnce {
		for id, r := range expected {
			if n := w.wh.applies[id]; n != 1 {
				problems = append(problems, fmt.Sprintf("%s: %s@%d applied %d times (exactly-once)", stream, r.Key, r.Seq, n))
			}
		}
	}
	return problems
}

// checkLog returns problems a next attempt would hit restoring: HEAD's
// record, or the one before it, missing.
func (w *world) checkLog() []string {
	ctx := context.Background()
	data, _, err := w.store.Get(ctx, "org/pipe/_checkpoints/HEAD")
	if err != nil {
		return []string{fmt.Sprintf("read HEAD: %v", err)}
	}
	var head checkpoint.Head
	if err := json.Unmarshal(data, &head); err != nil {
		return []string{fmt.Sprintf("decode HEAD: %v", err)}
	}
	log := checkpoint.NewLog(w.store, "org", "pipe")
	for ref, i := head.Record, 0; ref != nil && i < 2; i++ {
		rec, err := log.Record(ctx, *ref)
		if err != nil {
			return []string{fmt.Sprintf("restoring would fail: %v", err)}
		}
		ref = rec.Previous
	}
	return nil
}

func lastN(lines []string, n int) []string {
	if len(lines) > n {
		return append([]string{fmt.Sprintf("… %d earlier lines", len(lines)-n)}, lines[len(lines)-n:]...)
	}
	return lines
}
