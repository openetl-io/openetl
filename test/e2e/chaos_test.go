//go:build e2e && chaos

package e2e

// The chaos suite runs randomized pipelines through the whole stack while
// runners, engines and connectors die, runners are partitioned from the API,
// and the object store fails, then checks that every row landed once. Runners die at the commit protocol's crash points too,
// injected by their object-store proxies. Some runs into Postgres add a
// column mid-run, which block holds, and release it at random times.
//
//	make chaos CHAOS_FLAGS='-chaos.runs=200 -chaos.parallel=6'
//	go test -tags 'e2e chaos' ./test/e2e -run 'TestChaos/runs/0042-' -chaos.seed=7   # one run again

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
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
	"github.com/jackc/pgx/v5"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
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
	chaosPostgres = flag.Duration("chaos.postgres-restart", 0, "restart or kill compose's Postgres about this often; 0: never")
	chaosKills    = flag.Duration("chaos.orchestrator-kill", 0, "on DBOS, run two more orchestrators, and kill one about this often, which starts again; 0: neither")
)

type chaosConfig struct {
	i    int
	seed uint64
	// cdc, incremental or kafka.
	source string
	// postgres, iceberg or kafka.
	dest       string
	continuous bool
	// "", flink or spark.
	transform   string
	parallelism int
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
	// A column added to the source mid-run is held under block, and
	// released.
	holds bool
}

// The faults a run can draw besides crash points.
const (
	faultKill        = "kill runner"
	faultDrain       = "drain runner"
	faultEngine      = "kill engine"
	faultSource      = "kill source"
	faultDestination = "kill destination"
	faultPartition   = "partition runner"
	faultResults     = "drop results"
	faultDriver      = "kill Spark driver"
	faultTaskManager = "kill Flink TaskManager"
	faultJobManager  = "kill Flink JobManager"
)

func drawConfig(seed uint64, i int) chaosConfig {
	rng := rand.New(rand.NewPCG(seed, uint64(i)))
	pick := func(xs ...string) string { return xs[rng.IntN(len(xs))] }
	c := chaosConfig{i: i, seed: seed, source: pick("cdc", "incremental", "kafka"), dest: pick("postgres", "postgres", "iceberg", "kafka")}
	c.continuous = c.source != "incremental" && rng.IntN(2) == 0
	// A transform takes the mode it needs: Flink's is continuous, over
	// sources that replay exactly; Spark's scheduled. A run drawn with one
	// that isn't set up keeps the mode and goes without.
	switch pick("", "", "", "", "flink", "spark") {
	case "flink":
		c.transform, c.continuous, c.parallelism = "flink", true, 1+rng.IntN(2)
		if c.source == "incremental" {
			c.source = "cdc"
		}
	case "spark":
		c.transform, c.continuous = "spark", false
	}
	// Kafka only appends, and neither Kafka nor Iceberg dedupes, so a
	// superset stream is merged into Iceberg.
	if c.source == "incremental" && c.dest == "kafka" {
		c.dest = "iceberg"
	}
	c.write = "append"
	if c.source != "kafka" && c.dest != "kafka" && rng.IntN(2) == 0 {
		c.write = "merge"
	}
	if c.source == "incremental" && (c.dest == "iceberg" || c.transform == "spark") {
		c.write = "merge"
	}
	c.scope = pick("pipeline", "stream")
	if c.dest == "kafka" {
		c.scope = "pipeline"
	}
	c.interval = pick("1s", "2s", "3s")
	if !c.continuous {
		c.every = pick("5s", "10s", "20s")
		c.maxBytes = pick("", "8KB", "64KB")
	}
	// Spark's runners keep their checkpoints in a directory, without the
	// proxy that crashes them at the protocol's steps and adds noise.
	for range rng.IntN(4) {
		if p := pick(crashPoints...); c.transform != "spark" {
			c.faults = append(c.faults, "crash "+p)
		}
	}
	faults := []string{faultKill, faultKill, faultDrain, faultEngine, faultSource, faultDestination}
	switch c.transform {
	case "spark":
		faults = append(faults, faultDriver, faultDriver, faultDriver)
	case "flink":
		faults = append(faults, faultTaskManager, faultTaskManager, faultJobManager)
	}
	n := rng.IntN(3)
	if c.transform != "" {
		n++
	}
	for range n {
		c.faults = append(c.faults, pick(faults...))
	}
	if rng.IntN(4) == 0 {
		c.faults = append(c.faults, faultPartition)
	}
	if rng.IntN(4) == 0 {
		c.faults = append(c.faults, faultResults)
	}
	if c.noise = []float64{0, 0, 0.01, 0.03}[rng.IntN(4)]; c.transform == "spark" {
		c.noise = 0
	}
	// Postgres takes holds, on streams no transform reads.
	c.holds = c.source != "kafka" && c.dest == "postgres" && c.transform == "" && rng.IntN(3) == 0
	return c
}

func (c chaosConfig) name() string {
	mode := "scheduled"
	if c.continuous {
		mode = "continuous"
	}
	name := fmt.Sprintf("%s-%s-%s-%s-%s", c.source, mode, c.write, c.scope, c.dest)
	if c.transform != "" {
		name += "-" + c.transform
	}
	if c.holds {
		name += "-holds"
	}
	return name
}

// writing is how long the run changes its source. A transform's first sync
// takes a while, and the run writes nothing until it has landed.
func (c chaosConfig) writing() time.Duration {
	if c.writeFor > 0 {
		return c.writeFor
	}
	if c.transform != "" {
		return 2 * *chaosWrite
	}
	return *chaosWrite
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

func (c chaosConfig) pipelineDoc(name, pipeSource, pipeDest string, transform map[string]any, dataPlane string) string {
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
	if c.holds {
		spec["schemaPolicy"] = "block"
	}
	if c.continuous {
		spec["mode"] = "continuous"
	} else {
		spec["schedule"] = map[string]string{"every": c.every}
	}
	if transform != nil {
		spec["transform"] = transform
	}
	if dataPlane != "" {
		spec["dataPlane"] = dataPlane
	}
	return mustJSON(map[string]any{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": map[string]string{"name": name}, "spec": spec})
}

type chaosSuite struct {
	pool *chaosPool
	book *faultBook
	// Set up when compose runs them. Spark's runs go to a pool of their own.
	flink, spark *transformSetup
	sparkPool    *chaosPool
	// Runs with a transform wait for one of these. The compose Spark worker
	// fits one application, and the Flink TaskManager four slots.
	sparkRuns, flinkRuns chan struct{}
	// Held while a Flink container is down.
	cluster sync.Mutex

	pg pgGate

	mu       sync.Mutex
	results  []runResult
	restarts int
	// Orchestrators killed.
	orchestratorKills int
}

// pgGate keeps Postgres restarts out of the suite's own statements, so
// they cut off only the platform's: a run holds it for reading around its
// setup, each transaction it writes and each check, and a restart holds it
// for writing until Postgres takes connections again.
type pgGate struct {
	mu sync.RWMutex
	// Restarts so far, and how many were kills; a writer whose connection
	// is from before the last one connects again.
	restarts, kills int
}

func (g *pgGate) do(f func()) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	f()
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
	if *chaosPostgres > 0 {
		go s.restartPostgres(ctx, t, seed)
	}
	if *chaosKills > 0 && env.orchestrator == "dbos" {
		go s.killOrchestrators(ctx, t, seed)
	}
	sem := make(chan struct{}, *chaosParallel)
	t.Run("runs", func(t *testing.T) {
		for i := range *chaosRuns {
			cfg := s.setUp(drawConfig(seed, i))
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
		{source: "cdc", continuous: true, write: "merge", dest: "postgres"},
		{source: "cdc", continuous: true, write: "append", dest: "postgres"},
		{source: "incremental", write: "merge", dest: "postgres"},
		{source: "incremental", write: "append", dest: "postgres"},
		{source: "kafka", write: "append", dest: "postgres"},
		{source: "cdc", continuous: true, write: "merge", dest: "iceberg"},
		{source: "cdc", continuous: true, write: "append", dest: "kafka"},
		{source: "incremental", write: "merge", dest: "iceberg"},
		{source: "kafka", write: "append", dest: "iceberg"},
	}
	// Runs that hold a column added mid-run, and release it.
	kinds = append(kinds,
		chaosConfig{source: "cdc", continuous: true, write: "append", dest: "postgres", holds: true},
		chaosConfig{source: "incremental", write: "merge", dest: "postgres", holds: true})
	for i, c := range kinds {
		plants := []string{plantLoss, plantExtra}
		if c.holds {
			plants = append(plants, plantHeld)
		}
		for _, plant := range plants {
			c := c
			c.i, c.seed, c.plant, c.scope, c.interval, c.writeFor = i, 1, plant, "stream", "1s", 5*time.Second
			if c.dest == "kafka" {
				c.scope = "pipeline"
			}
			if !c.continuous {
				c.every = "5s"
			}
			// Its plant needs the column, and writes wait for the first sync,
			// which can take this long on a machine this busy.
			if plant == plantHeld {
				c.writeFor = 15 * time.Second
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
	s := &chaosSuite{book: newFaultBook(seed), sparkRuns: make(chan struct{}, 1), flinkRuns: make(chan struct{}, 2)}
	s.flink, s.spark = setupFlink(t), setupSpark(t)
	ctx, stop := context.WithCancel(context.Background())
	s.pool = startPool(ctx, t, poolConfig{runners: runners, bucket: bucket, dataPlane: "default", token: env.runnerToken, bridge: s.flink != nil}, *chaosLogs, s.book)
	if s.spark != nil {
		s.sparkPool = s.startSparkPool(ctx, t, runners)
	}
	t.Cleanup(stop)
	return s
}

// poolFor is the pool whose runners run c's pipeline.
func (s *chaosSuite) poolFor(c chaosConfig) *chaosPool {
	if c.transform == "spark" {
		return s.sparkPool
	}
	return s.pool
}

// setUp drops a transform the suite has no cluster for.
func (s *chaosSuite) setUp(c chaosConfig) chaosConfig {
	if c.transform == "flink" && s.flink == nil || c.transform == "spark" && s.spark == nil {
		c.transform = ""
		c.faults = slices.DeleteFunc(c.faults, func(f string) bool {
			return f == faultDriver || f == faultTaskManager || f == faultJobManager
		})
	}
	return c
}

func (s *chaosSuite) run(t *testing.T, c chaosConfig) {
	t.Logf("config: %s", c)
	rng := rand.New(rand.NewPCG(c.seed, uint64(c.i)<<32|0xc4a05))
	name := unique("chaos-" + c.source)
	switch c.transform {
	case "spark":
		s.sparkRuns <- struct{}{}
		defer func() { <-s.sparkRuns }()
	case "flink":
		s.flinkRuns <- struct{}{}
		defer func() { <-s.flinkRuns }()
	}
	var w workload
	var pipeline string
	s.pg.do(func() {
		if c.source == "kafka" {
			w = newKafkaWork(t, c, name, &s.pg)
		} else {
			w = newPGWork(t, c, name, &s.pg)
		}
		src, dst, pipeSource, pipeDest := w.docs()
		transform, dataPlane := s.transformSpec(c)
		apply(t,
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
			fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
			c.pipelineDoc(name, pipeSource, pipeDest, transform, dataPlane))
		pipeline = mustQuery[string](t, "SELECT id::text FROM pipelines WHERE name = $1", name)
	})
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
		s.pg.do(func() {
			_, _ = pipelines().DeletePipeline(context.Background(), connect.NewRequest(&apiv1.DeletePipelineRequest{Name: "workspaces/default/pipelines/" + name}))
			if !t.Failed() {
				w.drop(t)
			} else {
				t.Logf("kept the run's schemas and topics: %s", name)
			}
		})
	})

	writeCtx, stopWrite := context.WithTimeout(t.Context(), c.writing())
	defer stopWrite()
	var faults sync.WaitGroup
	faults.Go(func() { s.inject(writeCtx, t.Context(), c, rng, pipeline) })
	if c.holds {
		releaseCtx, stopReleases := context.WithCancel(t.Context())
		var releases sync.WaitGroup
		releases.Go(func() { releaseHolds(releaseCtx, t, name, rand.New(rand.NewPCG(c.seed, uint64(c.i)<<32|0x401d5))) })
		defer releases.Wait()
		defer stopReleases()
	}
	for {
		var loaded bool
		s.pg.do(func() { loaded = w.loaded(t) })
		if loaded {
			break
		}
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
	var stopped time.Time
	s.pg.do(func() { stopped = mustQuery[time.Time](t, "SELECT now()") })

	start := time.Now()
	var missing, extra int
	var detail string
	for {
		var synced bool
		s.pg.do(func() {
			synced = c.continuous || syncedSince(t, name, stopped)
			missing, extra, detail = w.check(t)
		})
		if extra > 0 || (missing == 0 && synced) || time.Since(start) > *chaosSettle {
			break
		}
		time.Sleep(2 * time.Second)
	}
	res.settle = time.Since(start)
	s.pg.do(func() { res.changes = w.changes(t) })
	if c.plant != "" {
		if extra > 0 || missing > 0 {
			t.Fatalf("before the plant: %d missing, %d extra: %s", missing, extra, detail)
		}
		s.pg.do(func() {
			w.plant(t, c.plant)
			missing, extra, detail = w.check(t)
		})
		if (c.plant == plantLoss || c.plant == plantHeld || c.write == "merge") && missing == 0 || c.plant == plantExtra && c.write != "merge" && extra == 0 {
			t.Errorf("the check missed a planted %s: %d missing, %d extra", c.plant, missing, extra)
		}
		t.Logf("planted %s: %s", c.plant, detail)
		return
	}
	switch {
	case extra > 0:
		res.failed = "duplicates or phantom rows: " + detail
	case missing > 0:
		var last string
		s.pg.do(func() { last = lastSyncError(t, name) })
		res.failed = fmt.Sprintf("rows missing after %v: %s (last sync: %s)", *chaosSettle, detail, last)
	}
	if res.failed != "" {
		t.Errorf("%s\nconfig: %s\nrerun: go test -tags 'e2e chaos' ./test/e2e -run 'TestChaos/runs/%04d-' -chaos.seed=%d", res.failed, c, c.i, c.seed)
	}
}

// releaseHolds releases the pipeline's holds as they show up, each at a
// random time, until ctx ends. The API may be down while Postgres
// restarts, and the next try does.
func releaseHolds(ctx context.Context, t *testing.T, pipeline string, rng *rand.Rand) {
	parent := "workspaces/default/pipelines/" + pipeline
	for ctx.Err() == nil {
		pause(ctx, time.Second+upTo(rng, 2*time.Second))
		resp, err := pipelines().ListHolds(ctx, connect.NewRequest(&apiv1.ListHoldsRequest{Parent: parent}))
		if err != nil {
			continue
		}
		for _, h := range resp.Msg.GetHolds() {
			if h.GetPending() || h.GetReleasing() || h.GetDiscarded() || rng.IntN(3) != 0 {
				continue
			}
			_, err := pipelines().ReleaseHold(ctx, connect.NewRequest(&apiv1.ReleaseHoldRequest{Parent: parent, Stream: h.GetStream(), Column: h.GetColumn()}))
			if err == nil {
				t.Logf("released %s.%s, held since %d-g%d", h.GetStream(), h.GetColumn(), h.GetSinceEpoch(), h.GetSinceGeneration())
			}
		}
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
		write := c.writing()
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
		switch e.fault {
		case faultDriver, faultTaskManager, faultJobManager:
			if s.clusterFault(base, &partitions, e.fault, rng, pipeline) {
				s.book.count(pipeline, e.fault)
			} else {
				s.book.count(pipeline, "missed "+e.fault)
			}
			continue
		}
		sl, engine := s.locate(base, s.poolFor(c), pipeline)
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
		case faultResults:
			sl.results.dropFor(5*time.Second + upTo(rng, 15*time.Second))
		}
		if done {
			s.book.count(pipeline, e.fault)
		} else {
			s.book.count(pipeline, "missed "+e.fault)
		}
	}
}

func kill(pid int) bool { return pid > 0 && syscall.Kill(pid, syscall.SIGKILL) == nil }

// clusterFault kills the pipeline's Spark driver, or takes a Flink
// container down for a few seconds in the background. A driver waits up to
// 30 seconds for the pipeline's next sync.
func (s *chaosSuite) clusterFault(ctx context.Context, bg *sync.WaitGroup, fault string, rng *rand.Rand, pipeline string) bool {
	switch fault {
	case faultDriver:
		for deadline := time.Now().Add(30 * time.Second); ctx.Err() == nil && time.Now().Before(deadline); pause(ctx, 500*time.Millisecond) {
			if killDriver(pipeline) {
				return true
			}
		}
		return false
	case faultTaskManager, faultJobManager:
		service := "flink-taskmanager"
		if fault == faultJobManager {
			service = "flink-jobmanager"
		}
		if !s.cluster.TryLock() {
			return false
		}
		seed := rng.Uint64()
		bg.Go(func() {
			defer s.cluster.Unlock()
			downFor(ctx, service, rand.New(rand.NewPCG(seed, 0)))
		})
		return true
	}
	return false
}

// locate waits up to 30 seconds, long enough for a scheduled pipeline's
// next sync, for a runner of the pool with a live engine for the pipeline,
// and returns its slot and the engine's PID.
func (s *chaosSuite) locate(ctx context.Context, pool *chaosPool, pipeline string) (*slot, int) {
	for deadline := time.Now().Add(30 * time.Second); ctx.Err() == nil && time.Now().Before(deadline); pause(ctx, 250*time.Millisecond) {
		for _, sl := range pool.slots {
			if pid := sl.engine(pipeline); pid > 0 && syscall.Kill(pid, 0) == nil {
				return sl, pid
			}
		}
	}
	return nil, 0
}

// killOrchestrators kills an orchestrator now and then, with jitter. Half the
// time it starts again under its own name, and recovers its own workflows when
// it launches; otherwise under another, as a Deployment's pod does, and another
// recovers them when its reports lapse.
func (s *chaosSuite) killOrchestrators(ctx context.Context, t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0x0c5))
	start := func(id string) *runner {
		return startOpenetld(t, nil, "orchestrator", "--database-url", env.dbURL, "--executor-id", id, "--bootstrap-key-file", env.keyFile)
	}
	names := []string{"chaos-0", "chaos-1"}
	replicas := []*runner{start(names[0]), start(names[1])}
	for n := 2; ; n++ {
		pause(ctx, *chaosKills/2+upTo(rng, *chaosKills))
		if ctx.Err() != nil {
			return
		}
		i := rng.IntN(len(replicas))
		replicas[i].kill(t)
		s.mu.Lock()
		s.orchestratorKills++
		s.mu.Unlock()
		pause(ctx, upTo(rng, 5*time.Second))
		if ctx.Err() != nil {
			return
		}
		if rng.IntN(2) == 0 {
			names[i] = fmt.Sprintf("chaos-%d", n)
		}
		replicas[i] = start(names[i])
	}
}

// restartTemporal restarts compose's Temporal now and then, with jitter.
func (s *chaosSuite) restartTemporal(ctx context.Context, t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0x7e3))
	for {
		pause(ctx, *chaosTemporal/2+upTo(rng, *chaosTemporal))
		if ctx.Err() != nil {
			return
		}
		id, err := publishing("7233")
		if err != nil {
			t.Logf("chaos: no Temporal container to restart: %v", err)
			return
		}
		if out, err := exec.Command("docker", "restart", id).CombinedOutput(); err != nil {
			t.Logf("chaos: restart Temporal: %v\n%s", err, out)
			continue
		}
		s.mu.Lock()
		s.restarts++
		s.mu.Unlock()
	}
}

// restartPostgres restarts compose's Postgres now and then, with jitter,
// between the suite's own statements: cleanly, or killed, so it recovers
// from its WAL. The control plane's store goes with it, and every source and
// destination.
func (s *chaosSuite) restartPostgres(ctx context.Context, t *testing.T, seed uint64) {
	rng := rand.New(rand.NewPCG(seed, 0x9e5))
	for {
		pause(ctx, *chaosPostgres/2+upTo(rng, *chaosPostgres))
		if ctx.Err() != nil {
			return
		}
		id, err := publishing("5432")
		if err != nil {
			t.Logf("chaos: no Postgres container to restart: %v", err)
			return
		}
		kill := rng.IntN(2) == 0
		s.pg.mu.Lock()
		err = restartPostgres(ctx, id, kill)
		s.pg.restarts++
		if kill {
			s.pg.kills++
		}
		s.pg.mu.Unlock()
		if err != nil {
			t.Logf("chaos: restart Postgres: %v", err)
		}
	}
}

// restartPostgres restarts or kills and starts container id, and waits
// until Postgres takes connections.
func restartPostgres(ctx context.Context, id string, kill bool) error {
	cmds := [][]string{{"docker", "restart", id}}
	if kill {
		cmds = [][]string{{"docker", "kill", id}, {"docker", "start", id}}
	}
	for _, c := range cmds {
		if out, err := exec.Command(c[0], c[1:]...).CombinedOutput(); err != nil {
			return fmt.Errorf("%s: %w\n%s", strings.Join(c, " "), err, out)
		}
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		conn, err := pgx.Connect(ctx, env.dbURL)
		if err == nil {
			err = conn.Ping(ctx)
			_ = conn.Close(context.Background())
		}
		if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
			return err
		}
		pause(ctx, 500*time.Millisecond)
	}
}

// publishing is the compose container publishing port.
func publishing(port string) (string, error) {
	out, err := exec.Command("docker", "ps", "-q", "--filter", "publish="+port).Output()
	id := strings.TrimSpace(string(out))
	if err == nil && id == "" {
		err = errors.New("none publishes " + port)
	}
	return id, err
}

func (s *chaosSuite) report(t *testing.T, seed uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var b strings.Builder
	s.pg.mu.RLock()
	fmt.Fprintf(&b, "chaos seed %d: %d runs on %s, %d Temporal restarts, %d orchestrator kills, %d Postgres restarts (%d killed)\n", seed, len(s.results), env.orchestrator, s.restarts, s.orchestratorKills, s.pg.restarts, s.pg.kills)
	s.pg.mu.RUnlock()
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
		fmt.Fprintf(&b, "  %-52s %4d runs, %d failed\n", name, byName[name][0], byName[name][1])
	}
	b.WriteString("faults:\n")
	for _, k := range slices.Sorted(maps.Keys(faults)) {
		fmt.Fprintf(&b, "  %-30s %d\n", k, faults[k])
	}
	started := 0
	slots := s.pool.slots
	if s.sparkPool != nil {
		slots = append(slices.Clone(slots), s.sparkPool.slots...)
	}
	for _, sl := range slots {
		sl.mu.Lock()
		started += sl.started
		sl.mu.Unlock()
	}
	fmt.Fprintf(&b, "changes checked: %d\nrunners started: %d in %d slots\n", changes, started, len(slots))
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
