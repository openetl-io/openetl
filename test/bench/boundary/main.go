// Command boundary benchmarks the runner/engine boundary: Postgres snapshots
// through the engine into Parquet (one crossing) and into
// destination-postgres (two crossings), the CPU the crossings cost, and the
// engine's idle memory. It needs the compose Postgres and a release engine;
// make bench-boundary runs it.
//
// Boundary CPU is measured by replaying the same batches from memory through
// the engine and through an in-process passthrough. The difference per GB is
// what crossing costs, which is then compared with the full jobs' CPU per GB.
package main

import (
	"bufio"
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/unsuitable001/openetl/connectors/native/postgres"
	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/delivery"
	"github.com/unsuitable001/openetl/internal/engineclient"
	"github.com/unsuitable001/openetl/internal/enginesink"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/source"
)

var (
	dsnFlag    = flag.String("dsn", cmp.Or(os.Getenv("OPENETL_TEST_POSTGRES_DSN"), "postgres://openetl:openetl@localhost:5432/openetl"), "Postgres for the dataset and for destination-postgres")
	engineFlag = flag.String("engine", "target/release/openetl-engine", "the openetl-engine binary")
	rowsFlag   = flag.Int("rows", 5_000_000, "rows in the dataset")
	runsFlag   = flag.Int("runs", 3, "runs of each phase; the report shows medians")
	replayFlag = flag.Int("replay-mb", 512, "MB of batches held in memory for the replay phases")
	profile    = flag.String("cpuprofile", "", "write the runner's CPU profile to this file")
)

const (
	schema    = "bench_boundary"
	table     = "items"
	outSchema = "bench_boundary_out"
	// Changing the table's definition must change this, so a cached
	// dataset is rebuilt.
	datasetVersion = "bench-boundary v1"
)

func main() {
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	err := profiled(func() error { return run(ctx) })
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench-boundary:", err)
		os.Exit(1)
	}
}

func profiled(f func() error) error {
	if *profile == "" {
		return f()
	}
	out, err := os.Create(*profile)
	if err != nil {
		return err
	}
	if err := pprof.StartCPUProfile(out); err != nil {
		_ = out.Close()
		return err
	}
	err = f()
	pprof.StopCPUProfile()
	return errors.Join(err, out.Close())
}

func run(ctx context.Context) error {
	bin, err := filepath.Abs(*engineFlag)
	if err != nil {
		return err
	}
	version, err := exec.CommandContext(ctx, bin, "--version").Output() //nolint:gosec // the -engine flag
	if err != nil {
		return fmt.Errorf("run %s (make engine builds it): %w", bin, err)
	}
	conn, err := pgx.Connect(ctx, *dsnFlag)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var pgVersion string
	if err := conn.QueryRow(ctx, "SHOW server_version").Scan(&pgVersion); err != nil {
		return err
	}
	if err := prepare(ctx, conn, *rowsFlag); err != nil {
		return fmt.Errorf("dataset: %w", err)
	}
	idle, err := idleRSS(ctx, bin)
	if err != nil {
		return fmt.Errorf("idle engine: %w", err)
	}

	srcConfig := fmt.Sprintf(`{"dsn": %q, "schema": %q, "tables": [%q]}`, *dsnFlag, schema, table)
	newSource := func() (*postgres.Source, error) { return postgres.NewSource([]byte(srcConfig)) }
	src, err := newSource()
	if err != nil {
		return err
	}
	cat, err := src.Discover(ctx)
	if err != nil {
		return err
	}
	var streams []runner.Stream
	for _, s := range cat.Streams {
		streams = append(streams, runner.Stream{Name: s.Name, Replay: s.Replay, Mode: destination.Append, PrimaryKey: s.PrimaryKey, Schema: s.Schema})
	}
	readAll := source.ReadRequest{Streams: []source.ConfiguredStream{{Name: table}}}
	b := &bench{runs: *runsFlag}
	engine := engineclient.Dataflow{Options: engineclient.Options{Binary: bin, PeakMemory: b.enginePeak}}
	if _, err := b.add(ctx, "postgres wire, no Arrow", step{run: func(ctx context.Context) (int64, error) { return wire(ctx, conn) }}); err != nil {
		return err
	}
	sourceOnly, err := b.add(ctx, "source → discard", step{run: func(ctx context.Context) (int64, error) {
		src, err := newSource()
		if err != nil {
			return 0, err
		}
		var d discardEmitter
		err = src.Read(ctx, readAll, &d)
		return d.bytes, err
	}})
	if err != nil {
		return err
	}
	var dir string
	shape1, err := b.add(ctx, "shape 1: source → engine → Parquet", step{
		setup: func(context.Context) (err error) {
			dir, err = os.MkdirTemp("", "bench-parquet-")
			return err
		},
		run: func(ctx context.Context) (int64, error) {
			src, err := newSource()
			if err != nil {
				return 0, err
			}
			dst, err := enginesink.NewParquet(fmt.Appendf(nil, `{"url": %q}`, "file://"+filepath.ToSlash(dir)))
			if err != nil {
				return 0, err
			}
			return attempt(ctx, "shape1", src, streams, dst, &engine, "")
		},
		cleanup: func(context.Context) { _ = os.RemoveAll(dir) },
	})
	if err != nil {
		return err
	}
	dropOut := func(ctx context.Context) error {
		_, err := conn.Exec(ctx, "DROP SCHEMA IF EXISTS "+outSchema+" CASCADE")
		return err
	}
	shape2, err := b.add(ctx, "shape 2: source → engine → postgres", step{
		setup: dropOut,
		run: func(ctx context.Context) (int64, error) {
			src, err := newSource()
			if err != nil {
				return 0, err
			}
			dst, err := postgres.NewDestination(fmt.Appendf(nil, `{"dsn": %q}`, *dsnFlag))
			if err != nil {
				return 0, err
			}
			return attempt(ctx, "shape2", src, streams, dst, &engine, outSchema)
		},
		cleanup: func(ctx context.Context) { _ = dropOut(ctx) },
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "holding %d MB of batches for the replays\n", *replayFlag)
	rec := &recorder{limit: int64(*replayFlag) * 1e6}
	if err := src.Read(ctx, readAll, rec); err != nil && !errors.Is(err, errEnough) {
		rec.release()
		return fmt.Errorf("capture: %w", err)
	}
	defer rec.release()
	if rec.bytes == 0 {
		return errors.New("capture: the source emitted nothing")
	}
	dataset := int64(sourceOnly.median(func(s sample) float64 { return float64(s.bytes) }))
	replayed := func(name string, dst destination.Destination, flow runner.Dataflow) step {
		return step{run: func(ctx context.Context) (int64, error) {
			return attempt(ctx, name, &replay{stream: table, items: rec.items, bytes: dataset}, streams, dst, flow, "")
		}}
	}
	ref, err := b.add(ctx, "replay → in process → discard", replayed("ref", discardDest{}, runner.Passthrough{}))
	if err != nil {
		return err
	}
	one, err := b.add(ctx, "replay → engine, discarded there", replayed("one", engineDiscard{}, &engine))
	if err != nil {
		return err
	}
	two, err := b.add(ctx, "replay → engine → runner → discard", replayed("two", discardDest{}, &engine))
	if err != nil {
		return err
	}

	fmt.Printf("dataset   %s.%s: %d rows, %.0f MB of Arrow\n", schema, table, *rowsFlag, float64(dataset)/1e6)
	fmt.Printf("host      %s/%s, %d CPUs; Postgres %s; %s\n", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), pgVersion, strings.TrimSpace(string(version)))
	fmt.Printf("runs      %d per phase, medians shown\n\n", b.runs)
	b.print()

	perGB := func(p *phase) float64 { return p.median(sample.cpuPerGB) }
	oneCost := perGB(one) - perGB(ref)
	twoCost := perGB(two) - perGB(ref)
	share1 := oneCost / perGB(shape1)
	share2 := twoCost / perGB(shape2)
	mbps1, mbpsSrc := shape1.median(sample.mbps), sourceOnly.median(sample.mbps)

	fmt.Println("\ngate")
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  shape 1 throughput\t%.0f MB/s (%.0f%% of the source alone, %.0f MB/s)\t≥ 100 MB/s\t%s\n", mbps1, 100*mbps1/mbpsSrc, mbpsSrc, verdict(mbps1 >= 100))
	fmt.Fprintf(w, "  one crossing\t%.3f CPU s/GB = %.1f%% of shape 1's CPU\t≤ 10%%\t%s\n", oneCost, 100*share1, verdict(share1 <= 0.10))
	fmt.Fprintf(w, "  two crossings\t%.3f CPU s/GB = %.1f%% of shape 2's CPU\t≤ 20%%\t%s\n", twoCost, 100*share2, verdict(share2 <= 0.20))
	fmt.Fprintf(w, "  idle engine RSS\t%.1f MB\t≤ 50 MB\t%s\n", float64(idle)/1e6, verdict(idle <= 50e6))
	_ = w.Flush()
	fmt.Println("\nMB is 10^6 bytes of Arrow buffers (value bytes on the wire for the first phase).")
	fmt.Println("Engine MB is the engine's peak resident memory.")
	fmt.Println("CPU is this process (the runner) plus the engines it started; Postgres's own CPU isn't counted.")
	return nil
}

func verdict(ok bool) string {
	if ok {
		return "pass"
	}
	return "FAIL"
}

// The defaults for scheduled pipelines.
func settings(caps destination.Capabilities) delivery.Settings {
	s := delivery.Settings{
		Require:            delivery.AtLeastOnce,
		CommitInterval:     5 * time.Minute,
		CommitMaxBytes:     256e6,
		Scope:              destination.ScopeStream,
		ResumePointTimeout: time.Minute,
	}
	if caps.SupportsScope(destination.ScopePipeline) {
		s.Scope = destination.ScopePipeline
	}
	return s
}

// attempt runs one sync attempt and returns the Arrow bytes the source
// emitted.
func attempt(ctx context.Context, name string, src source.Reader, streams []runner.Stream, dst destination.Destination, flow runner.Dataflow, ns string) (int64, error) {
	c := &counted{Reader: src}
	_, err := runner.RunAttempt(ctx, runner.Job{
		Owner:       rand.Text(),
		Pipeline:    name,
		Log:         checkpoint.NewLog(checkpoint.NewMemStore(), "bench", name),
		Source:      c,
		Streams:     streams,
		Destination: dst,
		Namespace:   ns,
		Dataflow:    flow,
		Settings:    settings(dst.Capabilities()),
	})
	return c.bytes.Load(), err
}

// prepare creates the dataset, unless the table already holds it.
func prepare(ctx context.Context, conn *pgx.Conn, rows int) error {
	want := fmt.Sprintf("%s, %d rows", datasetVersion, rows)
	var have *string
	if err := conn.QueryRow(ctx, "SELECT obj_description(to_regclass($1), 'pg_class')", schema+"."+table).Scan(&have); err != nil {
		return err
	}
	if have != nil && *have == want {
		return nil
	}
	fmt.Fprintf(os.Stderr, "creating %s.%s with %d rows\n", schema, table, rows)
	ident := pgx.Identifier{schema, table}.Sanitize()
	for _, q := range []string{
		"CREATE SCHEMA IF NOT EXISTS " + schema,
		"DROP TABLE IF EXISTS " + ident,
		"CREATE TABLE " + ident + ` (
			id bigint PRIMARY KEY,
			customer_id integer NOT NULL,
			sku text NOT NULL,
			description text,
			quantity smallint NOT NULL,
			price numeric(12, 2) NOT NULL,
			discount double precision,
			active boolean NOT NULL,
			created_at timestamptz NOT NULL,
			ref uuid NOT NULL)`,
		"INSERT INTO " + ident + ` SELECT i, (i * 7919) % 1000000, 'SKU-' || lpad((i % 100000)::text, 8, '0'),
			CASE WHEN i % 10 = 0 THEN NULL ELSE md5(i::text) || md5((i * 31)::text) END,
			i % 50 + 1, (i % 100000) / 100.0,
			CASE WHEN i % 7 = 0 THEN NULL ELSE (i % 1000) / 1000.0 END,
			i % 3 <> 0, timestamptz '2026-01-01 00:00:00+00' + i * interval '1 second', md5(i::text)::uuid
			FROM generate_series(1::bigint, ` + strconv.Itoa(rows) + `) i`,
		"VACUUM (ANALYZE) " + ident,
		"COMMENT ON TABLE " + ident + " IS '" + want + "'",
	} {
		if _, err := conn.Exec(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// wire reads the table in one query with binary values, building nothing:
// what Postgres and the connection deliver. It returns the value bytes.
func wire(ctx context.Context, conn *pgx.Conn) (int64, error) {
	rr := conn.PgConn().ExecParams(ctx, "SELECT * FROM "+pgx.Identifier{schema, table}.Sanitize(), nil, nil, nil, []int16{pgx.BinaryFormatCode})
	var n int64
	for rr.NextRow() {
		for _, v := range rr.Values() {
			n += int64(len(v))
		}
	}
	_, err := rr.Close()
	return n, err
}

// idleRSS starts an engine and returns its resident memory once it is ready
// for a job.
func idleRSS(ctx context.Context, bin string) (int64, error) {
	dir, err := os.MkdirTemp("", "oe-")
	if err != nil {
		return 0, err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cmd := exec.CommandContext(ctx, bin, "serve", "--control", filepath.Join(dir, "c.sock"), "--data", filepath.Join(dir, "d.sock"), "--watch-stdin") //nolint:gosec // the -engine flag
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return 0, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return 0, err
	}
	if err := cmd.Start(); err != nil {
		return 0, err
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("read the engine's ready line: %w", err)
	}
	if strings.TrimSpace(line) != "ready" {
		return 0, fmt.Errorf("the engine printed %q, not ready", line)
	}
	time.Sleep(time.Second)
	out, err := exec.CommandContext(ctx, "ps", "-o", "rss=", "-p", strconv.Itoa(cmd.Process.Pid)).Output() //nolint:gosec // a PID
	if err != nil {
		return 0, err
	}
	kb, err := strconv.ParseInt(strings.TrimSpace(string(out)), 10, 64)
	return kb << 10, err
}

func cpu(who int) time.Duration {
	var ru syscall.Rusage
	if err := syscall.Getrusage(who, &ru); err != nil {
		return 0
	}
	return time.Duration(ru.Utime.Nano() + ru.Stime.Nano())
}

type step struct {
	// setup and cleanup aren't measured.
	setup   func(context.Context) error
	run     func(context.Context) (int64, error)
	cleanup func(context.Context)
}

type sample struct {
	wall  time.Duration
	bytes int64
	// This process, and the children that exited during the run.
	runner, engine time.Duration
	enginePeak     int64
}

func (s sample) mbps() float64 { return float64(s.bytes) / 1e6 / s.wall.Seconds() }

func (s sample) cpuPerGB() float64 {
	return (s.runner + s.engine).Seconds() / (float64(s.bytes) / 1e9)
}

type phase struct {
	name    string
	samples []sample
}

func (p *phase) median(f func(sample) float64) float64 {
	xs := make([]float64, len(p.samples))
	for i, s := range p.samples {
		xs[i] = f(s)
	}
	slices.Sort(xs)
	n := len(xs)
	if n%2 == 1 {
		return xs[n/2]
	}
	return (xs[n/2-1] + xs[n/2]) / 2
}

type bench struct {
	runs   int
	phases []*phase
	peak   atomic.Int64
}

// enginePeak keeps the largest engine peak of the current run.
func (b *bench) enginePeak(n int64) {
	for {
		old := b.peak.Load()
		if n <= old || b.peak.CompareAndSwap(old, n) {
			return
		}
	}
}

func (b *bench) add(ctx context.Context, name string, s step) (*phase, error) {
	p := &phase{name: name}
	for i := range b.runs {
		if s.setup != nil {
			if err := s.setup(ctx); err != nil {
				return nil, fmt.Errorf("%s: %w", name, err)
			}
		}
		runtime.GC()
		b.peak.Store(0)
		self, kids := cpu(syscall.RUSAGE_SELF), cpu(syscall.RUSAGE_CHILDREN)
		start := time.Now()
		n, err := s.run(ctx)
		wall := time.Since(start)
		runtime.GC()
		smp := sample{wall: wall, bytes: n, runner: cpu(syscall.RUSAGE_SELF) - self, engine: cpu(syscall.RUSAGE_CHILDREN) - kids, enginePeak: b.peak.Load()}
		if s.cleanup != nil {
			s.cleanup(ctx)
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		fmt.Fprintf(os.Stderr, "%-40s run %d: %5.2fs %5.0f MB/s, engine peak %.0f MB\n", name, i+1, wall.Seconds(), smp.mbps(), float64(smp.enginePeak)/1e6)
		p.samples = append(p.samples, smp)
	}
	b.phases = append(b.phases, p)
	return p, nil
}

func (b *bench) print() {
	const row = "%-38s %7s %6s %11s %11s %9s %9s\n"
	fmt.Printf(row, "phase", "wall s", "MB/s", "runner CPU", "engine CPU", "CPU s/GB", "engine MB")
	for _, p := range b.phases {
		peak := "—"
		if mb := p.median(func(s sample) float64 { return float64(s.enginePeak) / 1e6 }); mb > 0 {
			peak = fmt.Sprintf("%.0f", mb)
		}
		fmt.Printf(row, p.name,
			fmt.Sprintf("%.2f", p.median(func(s sample) float64 { return s.wall.Seconds() })),
			fmt.Sprintf("%.0f", p.median(sample.mbps)),
			fmt.Sprintf("%.2f", p.median(func(s sample) float64 { return s.runner.Seconds() })),
			fmt.Sprintf("%.2f", p.median(func(s sample) float64 { return s.engine.Seconds() })),
			fmt.Sprintf("%.3f", p.median(sample.cpuPerGB)),
			peak)
	}
}
