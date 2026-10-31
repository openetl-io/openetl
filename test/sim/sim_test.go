// Package sim is a seeded simulation of the commit protocol. Each seed runs
// the real runner against fake sources and a fake sink under injected faults,
// crashes and zombie attempts, then checks no loss, no duplicates where
// exactly-once, and the right final rows where effectively-once.
//
//	go test ./test/sim -seeds=10000         # more seeds per sink class
//	go test ./test/sim -run 'TestSimulation/kafka' -seed=42 -v   # one seed, with its trace
package sim

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
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
// across tables.
var sinkKinds = []string{"warehouse", "parquet", "kafka", "webhook"}

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
	name   string
	replay source.Replay
}

type config struct {
	kind     string
	streams  []streamCfg
	mode     destination.WriteMode
	settings delivery.Settings
	faults   int
	zombies  int
}

func (c config) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "sink=%s mode=%d scope=%d dedupe=%d require=%v maxBytes=%d interval=%v resumeTimeout=%v faults=%d zombies=%d streams=",
		c.kind, c.mode, c.settings.Scope, c.settings.Dedupe, c.settings.Require, c.settings.CommitMaxBytes,
		c.settings.CommitInterval, c.settings.ResumePointTimeout, c.faults, c.zombies)
	for _, s := range c.streams {
		fmt.Fprintf(&b, "%s(%v) ", s.name, s.replay)
	}
	return b.String()
}

func randomConfig(rng *rand.Rand, kind string) config {
	c := config{kind: kind, mode: destination.Append, faults: rng.IntN(9), zombies: rng.IntN(3)}
	for i := range 1 + rng.IntN(3) {
		r := source.Exact
		if rng.IntN(2) == 0 {
			r = source.Superset
		}
		c.streams = append(c.streams, streamCfg{name: fmt.Sprintf("s%d", i), replay: r})
	}
	if kind == "warehouse" && rng.IntN(2) == 0 {
		c.mode = destination.Merge
	}
	c.settings = delivery.Settings{
		Scope:          destination.ScopeStream,
		Dedupe:         delivery.DedupeAuto,
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
		c.settings.Dedupe = delivery.DedupeNone
	}
	if rng.IntN(3) == 0 {
		c.settings.CommitInterval = time.Duration(5+rng.IntN(55)) * time.Second
	}
	if rng.IntN(2) == 0 {
		c.settings.ResumePointTimeout = time.Duration(1+rng.IntN(20)) * time.Second
	}
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
	lost      error
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
	default:
		return &warehouseDest{w: w, wh: w.wh, attempt: attempt}
	}
}

func (w *world) job(attempt int) runner.Job {
	j := runner.Job{
		Owner:       fmt.Sprintf("a%d", attempt),
		Pipeline:    "pipe",
		Log:         checkpoint.NewLog(&simStore{w: w, attempt: attempt}, "org", "pipe"),
		Source:      &simSource{w: w, attempt: attempt},
		Destination: w.destination(attempt),
		Dataflow:    runner.Passthrough{Convert: toArrow},
		Settings:    w.cfg.settings,
	}
	for _, s := range w.cfg.streams {
		j.Streams = append(j.Streams, runner.Stream{Name: s.name, Replay: s.replay, Mode: w.cfg.mode, PrimaryKey: []string{"key"}})
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
		wh:        newWarehouse(kind == "warehouse"),
		broker:    newBroker(),
		recv:      &receiver{applies: map[string]int{}},
	}
	for _, s := range cfg.streams {
		w.upstreams[s.name] = newUpstream(rng, s.name, s.replay)
	}
	// Require what the plan computes, so a forced barrier may never lower it.
	caps := w.destination(0).Capabilities()
	w.cfg.settings.Require = delivery.ExactlyOnce
	for _, s := range cfg.streams {
		w.cfg.settings.Require = min(w.cfg.settings.Require, delivery.StreamLevel(caps, s.replay, cfg.mode, cfg.settings.Dedupe))
	}
	w.sched = newScheduler(rng, cfg.faults, cfg.zombies)
	res.config = w.cfg.String()
	res.stats = map[string]int{}

	attempts := 0
	err := w.sched.run(func(attempt int) error {
		attempts = max(attempts, attempt)
		r, err := runner.RunAttempt(context.Background(), w.job(attempt))
		if errors.Is(err, checkpoint.ErrFenced) || errors.Is(err, destination.ErrFenced) {
			res.stats["fenced attempts"]++
		}
		if err == nil {
			w.sched.tracef("a%d: gen %d, %s, restored %d, committed %d epochs, last %d", attempt, r.Generation, r.Recovery, r.Restored, r.Committed, r.Last)
			res.stats["recovery:"+r.Recovery.String()]++
		}
		return err
	})
	res.trace = strings.Join(lastN(w.sched.trace, 400), "\n")
	res.steps = w.sched.steps
	res.stats["attempts"] += attempts
	res.stats["zombies"] += cfg.zombies - w.sched.zombies
	switch {
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
	forced := w.forcedStreams()
	var problems []string
	for _, s := range w.cfg.streams {
		u := w.upstreams[s.name]
		level := delivery.StreamLevel(caps, s.replay, w.cfg.mode, w.cfg.settings.Dedupe)
		if forced[s.name] {
			stats["forced streams"]++
			if lower := delivery.StreamLevel(caps, source.Superset, w.cfg.mode, w.cfg.settings.Dedupe); lower < w.cfg.settings.Require {
				problems = append(problems, fmt.Sprintf("%s: a barrier was forced although it lowers the stream to %v, below the required %v", s.name, lower, w.cfg.settings.Require))
			} else {
				level = min(level, lower)
			}
		}
		stats["level:"+level.String()]++
		expected := map[string]record{}
		for _, r := range u.records {
			expected[recordID(s.name, r.Key, r.Seq)] = r
		}
		if w.cfg.mode == destination.Merge {
			problems = append(problems, w.checkMerge(s.name, u, expected, level)...)
		} else {
			problems = append(problems, checkAppend(s.name, w.visible(s.name), expected, level)...)
		}
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
	case "webhook":
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

func checkAppend(stream string, rows []row, expected map[string]record, level delivery.Level) []string {
	var problems []string
	count := map[string]int{}
	for _, r := range rows {
		if _, ok := expected[r.ID]; !ok {
			problems = append(problems, fmt.Sprintf("%s: unexpected row %+v", stream, r))
		}
		count[r.ID]++
	}
	for id, r := range expected {
		switch n := count[id]; {
		case n == 0:
			problems = append(problems, fmt.Sprintf("%s: LOST %s@%d", stream, r.Key, r.Seq))
		case n > 1 && level == delivery.ExactlyOnce:
			problems = append(problems, fmt.Sprintf("%s: DUPLICATED %s@%d ×%d (exactly-once)", stream, r.Key, r.Seq, n))
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
		if g, ok := got[k]; !ok || g.Seq != r.Seq || g.Value != r.Value {
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

// forcedStreams returns the streams forced in any record ever written.
func (w *world) forcedStreams() map[string]bool {
	out := map[string]bool{}
	ctx := context.Background()
	keys, _ := w.store.List(ctx, "org/pipe/_checkpoints/")
	for _, k := range keys {
		if strings.HasSuffix(k, "HEAD") {
			continue
		}
		data, _, err := w.store.Get(ctx, k)
		if err != nil {
			continue
		}
		var rec checkpoint.Record
		if json.Unmarshal(data, &rec) == nil {
			for _, s := range rec.Forced {
				out[s] = true
			}
		}
	}
	return out
}

func lastN(lines []string, n int) []string {
	if len(lines) > n {
		return append([]string{fmt.Sprintf("… %d earlier lines", len(lines)-n)}, lines[len(lines)-n:]...)
	}
	return lines
}
