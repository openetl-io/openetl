//go:build e2e && chaos

package e2e

// The chaos suite runs randomized pipelines through the whole stack while
// runners, engines and connectors die, runners are partitioned from
// Temporal and the API, and the object store fails, then checks that every
// row landed once. Runners die at the commit protocol's crash points too,
// injected by their object-store proxies.
//
//	make chaos CHAOS_FLAGS='-chaos.runs=200 -chaos.parallel=6'
//	go test -tags 'e2e chaos' ./test/e2e -run 'TestChaos/runs/0042-' -chaos.seed=7   # one run again

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

var (
	chaosRuns     = flag.Int("chaos.runs", 12, "randomized runs")
	chaosParallel = flag.Int("chaos.parallel", 4, "runs at a time")
	chaosSeed     = flag.Uint64("chaos.seed", 0, "the suite's seed, which draws each run's config and faults; 0 picks one")
	chaosRunners  = flag.Int("chaos.runners", 3, "runners")
	chaosWrite    = flag.Duration("chaos.write", 40*time.Second, "how long each run changes its source, under faults")
	chaosSettle   = flag.Duration("chaos.settle", 5*time.Minute, "how long a run's destination may take to match its source afterwards")
	chaosOnly     = flag.String("chaos.only", "", "run only configs whose name contains one of these, comma-separated, such as cdc-continuous,kafka")
	chaosLogs     = flag.String("chaos.logs", "", "a directory to keep every runner's output in")
	chaosTemporal = flag.Duration("chaos.temporal-restart", 0, "restart compose's Temporal about this often; 0: never")
)

type chaosConfig struct {
	i    int
	seed uint64
	// cdc, incremental or kafka.
	source     string
	continuous bool
	// merge or append.
	write    string
	scope    string
	interval string
	maxBytes string
	every    string
	faults   []string
	noise    float64
	// Unset: -chaos.write.
	writeFor time.Duration
	plant    string
}

// The faults a run can draw besides crash points.
const (
	faultKill        = "kill runner"
	faultDrain       = "drain runner"
	faultEngine      = "kill engine"
	faultSource      = "kill source"
	faultDestination = "kill destination"
	faultPartition   = "partition runner"
)

func drawConfig(seed uint64, i int) chaosConfig {
	rng := rand.New(rand.NewPCG(seed, uint64(i)))
	pick := func(xs ...string) string { return xs[rng.IntN(len(xs))] }
	c := chaosConfig{i: i, seed: seed, source: pick("cdc", "incremental", "kafka")}
	c.continuous = c.source != "incremental" && rng.IntN(2) == 0
	c.write = "append"
	if c.source != "kafka" && rng.IntN(2) == 0 {
		c.write = "merge"
	}
	c.scope = pick("pipeline", "stream")
	c.interval = pick("1s", "2s", "3s")
	if !c.continuous {
		c.every = pick("5s", "10s", "20s")
		c.maxBytes = pick("", "8KB", "64KB")
	}
	for range rng.IntN(4) {
		c.faults = append(c.faults, "crash "+pick(crashPoints...))
	}
	for range rng.IntN(3) {
		c.faults = append(c.faults, pick(faultKill, faultKill, faultDrain, faultEngine, faultSource, faultDestination))
	}
	if rng.IntN(4) == 0 {
		c.faults = append(c.faults, faultPartition)
	}
	c.noise = []float64{0, 0, 0.01, 0.03}[rng.IntN(4)]
	return c
}

func (c chaosConfig) name() string {
	mode := "scheduled"
	if c.continuous {
		mode = "continuous"
	}
	return fmt.Sprintf("%s-%s-%s-%s", c.source, mode, c.write, c.scope)
}

func (c chaosConfig) String() string {
	commit := "interval=" + c.interval
	if c.maxBytes != "" {
		commit += " maxBytes=" + c.maxBytes
	}
	if c.every != "" {
		commit += " every=" + c.every
	}
	return fmt.Sprintf("%s %s noise=%v faults=[%s]", c.name(), commit, c.noise, strings.Join(c.faults, ", "))
}

func (c chaosConfig) pipelineDoc(name, pipeSource, pipeDest string) string {
	src, dst := map[string]any{}, map[string]any{}
	_ = json.Unmarshal([]byte(pipeSource), &src)
	_ = json.Unmarshal([]byte(pipeDest), &dst)
	src["ref"], dst["ref"] = name, name
	commit := map[string]string{"interval": c.interval, "scope": c.scope}
	if c.maxBytes != "" {
		commit["maxBytes"] = c.maxBytes
	}
	// A superset stream merged by key is effectively once at most.
	require := "exactly_once"
	if c.source == "incremental" && c.write == "merge" {
		require = "effectively_once"
	}
	spec := map[string]any{"source": src, "destination": dst, "delivery": map[string]any{"require": require, "commit": commit}}
	if c.continuous {
		spec["mode"] = "continuous"
	} else {
		spec["schedule"] = map[string]string{"every": c.every}
	}
	return mustJSON(map[string]any{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": map[string]string{"name": name}, "spec": spec})
}

type chaosSuite struct {
	pool *chaosPool
	book *faultBook

	mu       sync.Mutex
	results  []runResult
	restarts int
}

type runResult struct {
	c       chaosConfig
	failed  string
	counts  map[string]int
	resumes []time.Duration
	settle  time.Duration
	changes int
}

func TestChaos(t *testing.T) {
	seed := *chaosSeed
	if seed == 0 {
		seed = rand.Uint64() >> 12
	}
	t.Logf("chaos seed %d (rerun with -chaos.seed=%d)", seed, seed)
	s := newChaosSuite(t, seed, *chaosRunners)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if *chaosTemporal > 0 {
		go s.restartTemporal(ctx, t, seed)
	}
	sem := make(chan struct{}, *chaosParallel)
	t.Run("runs", func(t *testing.T) {
		for i := range *chaosRuns {
			cfg := drawConfig(seed, i)
			if *chaosOnly != "" && !slices.ContainsFunc(strings.Split(*chaosOnly, ","), func(s string) bool { return strings.Contains(cfg.name(), s) }) {
				continue
			}
			t.Run(fmt.Sprintf("%04d-%s", i, cfg.name()), func(t *testing.T) {
				t.Parallel()
				sem <- struct{}{}
				defer func() { <-sem }()
				s.run(t, cfg)
			})
		}
	})
	cancel()
	s.report(t, seed)
}

// TestChaosChecks runs a pipeline of each kind without faults, then takes a
// row from its destination or adds one, and fails unless the run's check
// notices.
func TestChaosChecks(t *testing.T) {
	s := newChaosSuite(t, 1, 1)
	kinds := []chaosConfig{
		{source: "cdc", continuous: true, write: "merge"},
		{source: "cdc", continuous: true, write: "append"},
		{source: "incremental", write: "merge"},
		{source: "incremental", write: "append"},
		{source: "kafka", write: "append"},
	}
	for i, c := range kinds {
		for _, plant := range []string{plantLoss, plantExtra} {
			c := c
			c.i, c.seed, c.plant, c.scope, c.interval, c.writeFor = i, 1, plant, "stream", "1s", 5*time.Second
			if !c.continuous {
				c.every = "5s"
			}
			if *chaosOnly != "" && !slices.ContainsFunc(strings.Split(*chaosOnly, ","), func(s string) bool { return strings.Contains(c.name(), s) }) {
				continue
			}
			t.Run(c.name()+"-"+plant, func(t *testing.T) {
				t.Parallel()
				s.run(t, c)
			})
		}
	}
}

// newChaosSuite starts runners in their slots, whose checkpoints go to a
// bucket of the suite's own.
func newChaosSuite(t *testing.T, seed uint64, runners int) *chaosSuite {
	t.Helper()
	bucket := "chaos-" + strings.TrimPrefix(unique("x"), "x-")
	c, err := objectstore.S3Client(mustParse(t, "s3://"+bucket+"?endpoint="+s3Endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &bucket}); err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	t.Cleanup(func() { dropBucket(t, c, bucket) })
	s := &chaosSuite{book: newFaultBook(seed)}
	ctx, stop := context.WithCancel(context.Background())
	s.pool = startPool(ctx, t, runners, bucket, *chaosLogs, s.book)
	t.Cleanup(stop)
	return s
}

func (s *chaosSuite) run(t *testing.T, c chaosConfig) {
	t.Logf("config: %s", c)
	rng := rand.New(rand.NewPCG(c.seed, uint64(c.i)<<32|0xc4a05))
	name := unique("chaos-" + c.source)
	var w workload
	if c.source == "kafka" {
		w = newKafkaWork(t, name)
	} else {
		w = newPGWork(t, c, name)
	}
	src, dst, pipeSource, pipeDest := w.docs()
	apply(t,
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		c.pipelineDoc(name, pipeSource, pipeDest))
	pipeline := mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	s.book.add(pipeline, c.noise)
	res := runResult{c: c}
	defer func() {
		res.counts, res.resumes = s.book.remove(pipeline)
		t.Logf("faults: %v; resumed after: %v", res.counts, res.resumes)
		if t.Failed() {
			res.failed = cmp.Or(res.failed, "failed")
		}
		s.mu.Lock()
		s.results = append(s.results, res)
		s.mu.Unlock()
	}()
	t.Cleanup(func() {
		_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
		if !t.Failed() {
			w.drop(t)
		} else {
			t.Logf("kept the run's schemas and topics: %s", name)
		}
	})

	writeCtx, stopWrite := context.WithTimeout(t.Context(), cmp.Or(c.writeFor, *chaosWrite))
	defer stopWrite()
	var faults sync.WaitGroup
	faults.Go(func() { s.inject(writeCtx, t.Context(), c, rng, pipeline) })
	for !w.loaded(t) {
		if writeCtx.Err() != nil {
			// The faults went on without changes to the source; its
			// preloaded rows are still checked.
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err := w.write(writeCtx, rand.New(rand.NewPCG(c.seed, uint64(c.i)))); err != nil {
		res.failed = "write: " + err.Error()
		t.Fatalf("writing to the source: %v", err)
	}
	<-writeCtx.Done()
	faults.Wait()
	s.book.quiet(pipeline)
	stopped := mustQuery[time.Time](t, "SELECT now()")

	start := time.Now()
	var missing, extra int
	var detail string
	for {
		synced := c.continuous || syncedSince(t, name, stopped)
		missing, extra, detail = w.check(t)
		if extra > 0 || (missing == 0 && synced) || time.Since(start) > *chaosSettle {
			break
		}
		time.Sleep(2 * time.Second)
	}
	res.settle, res.changes = time.Since(start), w.changes(t)
	if c.plant != "" {
		if extra > 0 || missing > 0 {
			t.Fatalf("before the plant: %d missing, %d extra: %s", missing, extra, detail)
		}
		w.plant(t, c.plant)
		missing, extra, detail = w.check(t)
		if (c.plant == plantLoss || c.write == "merge") && missing == 0 || c.plant == plantExtra && c.write != "merge" && extra == 0 {
			t.Errorf("the check missed a planted %s: %d missing, %d extra", c.plant, missing, extra)
		}
		t.Logf("planted %s: %s", c.plant, detail)
		return
	}
	switch {
	case extra > 0:
		res.failed = "duplicates or phantom rows: " + detail
	case missing > 0:
		res.failed = fmt.Sprintf("rows missing after %v: %s (last sync: %s)", *chaosSettle, detail, lastSyncError(t, name))
	}
	if res.failed != "" {
		t.Errorf("%s\nconfig: %s\nrerun: go test -tags 'e2e chaos' ./test/e2e -run 'TestChaos/runs/%04d-' -chaos.seed=%d", res.failed, c, c.i, c.seed)
	}
}

// syncedSince reports whether a sync of the pipeline that started after
// since has succeeded.
func syncedSince(t *testing.T, pipeline string, since time.Time) bool {
	t.Helper()
	ok, err := query[bool](t, `SELECT EXISTS (SELECT 1 FROM syncs s JOIN pipelines p ON p.id = s.pipeline_id
		WHERE p.name = $1 AND s.status = 'succeeded' AND s.start_time > $2)`, pipeline, since)
	return err == nil && ok
}

// inject arms or makes the run's faults at random times while ctx lasts.
// Partitions heal by their own clock, within base.
func (s *chaosSuite) inject(ctx, base context.Context, c chaosConfig, rng *rand.Rand, pipeline string) {
	type event struct {
		at    time.Duration
		fault string
	}
	var events []event
	for _, f := range c.faults {
		write := cmp.Or(c.writeFor, *chaosWrite)
		events = append(events, event{at: write/10 + upTo(rng, write*8/10), fault: f})
	}
	slices.SortFunc(events, func(a, b event) int { return cmp.Compare(a.at, b.at) })
	start := time.Now()
	var partitions sync.WaitGroup
	defer partitions.Wait()
	for _, e := range events {
		pause(ctx, time.Until(start.Add(e.at)))
		if ctx.Err() != nil {
			s.book.count(pipeline, "missed "+e.fault)
			continue
		}
		if point, ok := strings.CutPrefix(e.fault, "crash "); ok {
			s.book.arm(pipeline, point)
			continue
		}
		sl, engine := s.locate(base, pipeline)
		if sl == nil {
			s.book.count(pipeline, "missed "+e.fault)
			continue
		}
		done := true
		switch e.fault {
		case faultKill:
			done = sl.signal(syscall.SIGKILL)
		case faultDrain:
			done = sl.signal(syscall.SIGTERM)
		case faultEngine:
			done = kill(engine)
		case faultSource, faultDestination:
			pids := sl.children(strings.TrimPrefix(e.fault, "kill ") + "-")
			done = len(pids) > 0 && kill(pids[rng.IntN(len(pids))])
		case faultPartition:
			d := 5*time.Second + upTo(rng, 15*time.Second)
			partitions.Go(func() { sl.partition(base, d) })
		}
		if done {
			s.book.count(pipeline, e.fault)
		} else {
			s.book.count(pipeline, "missed "+e.fault)
		}
	}
}

func kill(pid int) bool { return pid > 0 && syscall.Kill(pid, syscall.SIGKILL) == nil }

// locate waits up to 30 seconds, long enough for a scheduled pipeline's
// next sync, for a runner with a live engine for the pipeline, and returns
// its slot and the engine's PID.
func (s *chaosSuite) locate(ctx context.Context, pipeline string) (*slot, int) {
	for deadline := time.Now().Add(30 * time.Second); ctx.Err() == nil && time.Now().Before(deadline); pause(ctx, 250*time.Millisecond) {
		for _, sl := range s.pool.slots {
			if pid := sl.engine(pipeline); pid > 0 && syscall.Kill(pid, 0) == nil {
				return sl, pid
			}
		}
	}
	return nil, 0
}

// restartTemporal restarts compose's Temporal now and then, with jitter.
func (s *chaosSuite) restartTemporal(ctx context.Context, t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0x7e3))
	for {
		pause(ctx, *chaosTemporal/2+upTo(rng, *chaosTemporal))
		if ctx.Err() != nil {
			return
		}
		id, err := exec.Command("docker", "ps", "-q", "--filter", "publish=7233").Output()
		if err != nil || len(strings.TrimSpace(string(id))) == 0 {
			t.Logf("chaos: no Temporal container to restart: %v", err)
			return
		}
		if out, err := exec.Command("docker", "restart", strings.TrimSpace(string(id))).CombinedOutput(); err != nil {
			t.Logf("chaos: restart Temporal: %v\n%s", err, out)
			continue
		}
		s.mu.Lock()
		s.restarts++
		s.mu.Unlock()
	}
}

func (s *chaosSuite) report(t *testing.T, seed uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	fmt.Fprintf(&b, "chaos seed %d: %d runs, %d Temporal restarts\n", seed, len(s.results), s.restarts)
	byName := map[string][2]int{}
	faults := map[string]int{}
	var resumes, settles []time.Duration
	var failed []string
	changes := 0
	for _, r := range s.results {
		changes += r.changes
		n := byName[r.c.name()]
		n[0]++
		if r.failed != "" {
			n[1]++
			failed = append(failed, fmt.Sprintf("  %04d %s: %s", r.c.i, r.c, r.failed))
		}
		byName[r.c.name()] = n
		for k, v := range r.counts {
			faults[k] += v
		}
		if r.c.continuous {
			resumes = append(resumes, r.resumes...)
		}
		settles = append(settles, r.settle)
	}
	for _, name := range slices.Sorted(maps.Keys(byName)) {
		fmt.Fprintf(&b, "  %-40s %4d runs, %d failed\n", name, byName[name][0], byName[name][1])
	}
	b.WriteString("faults:\n")
	for _, k := range slices.Sorted(maps.Keys(faults)) {
		fmt.Fprintf(&b, "  %-30s %d\n", k, faults[k])
	}
	started := 0
	for _, sl := range s.pool.slots {
		sl.mu.Lock()
		started += sl.started
		sl.mu.Unlock()
	}
	fmt.Fprintf(&b, "changes checked: %d\nrunners started: %d in %d slots\n", changes, started, len(s.pool.slots))
	fmt.Fprintf(&b, "continuous jobs resuming after their runner was killed: %s\n", quantiles(resumes))
	fmt.Fprintf(&b, "settling after the writes stopped: %s\n", quantiles(settles))
	if len(failed) > 0 {
		b.WriteString("failed:\n" + strings.Join(failed, "\n") + "\n")
	}
	t.Log(b.String())
	if path := os.Getenv("OPENETL_CHAOS_REPORT"); path != "" {
		_ = os.WriteFile(path, []byte(b.String()), 0o600)
	}
}

func quantiles(ds []time.Duration) string {
	if len(ds) == 0 {
		return "none"
	}
	slices.Sort(ds)
	q := func(p float64) time.Duration {
		return ds[min(len(ds)-1, int(p*float64(len(ds))))].Round(100 * time.Millisecond)
	}
	return fmt.Sprintf("%d, p50 %v, p95 %v, max %v", len(ds), q(0.5), q(0.95), ds[len(ds)-1].Round(100*time.Millisecond))
}
