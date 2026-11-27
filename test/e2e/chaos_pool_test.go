//go:build e2e && chaos

package e2e

import (
	"bufio"
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/test/e2e/procs"
)

// chaosPool keeps a runner going in each slot. Each slot reaches the API
// through a proxy of its own, which partitions it, and a filter that drops its
// results, and an S3 store through another, which injects the faults runs arm.
// A runner that dies is started again after a pause.
type chaosPool struct {
	cfg   poolConfig
	store string
	logs  string
	slots []*slot
	book  *faultBook
	wg    sync.WaitGroup
	mu    sync.Mutex
	// The one pipeline a run's own pool runs, once it's applied; empty for
	// a pool runs share.
	only string
	// Faults at a launcher's pod, a worker's pod or a pod's connector, and
	// those of them that found what they kill.
	podTried, podLanded atomic.Int64
}

// dedicate notes that the pool runs pipeline and no other.
func (p *chaosPool) dedicate(pipeline string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.only = pipeline
}

func (p *chaosPool) dedicated() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.only
}

type poolConfig struct {
	runners int
	// How many of the slots run a pod launcher: jobs of m and up run in pods
	// of their own, started as processes, and the other slots take the rest.
	launchers int
	// The first slot's index. Indices are unique across a suite's pools.
	first int
	// Checkpoints go to bucket, through each slot's proxy, or else to the
	// directory dir.
	bucket, dir      string
	dataPlane, token string
	// Runners take Flink jobs' bridge connections.
	bridge      bool
	sparkSubmit string
}

type slot struct {
	i    int
	pool *chaosPool
	s3   *s3Proxy
	api  *proxy
	// Drops its results, between the two.
	results *apiFilter
	// The port the runner's bridge listens on, or 0.
	bridge int
	// It runs a pod launcher.
	launcher bool
	mu       sync.Mutex
	cur      *runner
	started  int
	// What its launchers' output says of their pods, and the process group
	// of each launcher it has run.
	pods   procs.Pods
	groups []int
}

func startPool(ctx context.Context, t *testing.T, cfg poolConfig, logs string, book *faultBook) *chaosPool {
	t.Helper()
	p := &chaosPool{cfg: cfg, logs: logs, book: book}
	target, err := url.Parse(s3Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	for i := range cfg.runners {
		s := &slot{i: cfg.first + i, pool: p, launcher: i < cfg.launchers, results: startAPIFilter(t, env.apiURL)}
		s.api = startProxy(t, strings.TrimPrefix(s.results.url(), "http://"))
		if cfg.bucket != "" {
			s.s3 = startS3Proxy(t, s, target)
		}
		if cfg.bridge {
			s.bridge = bridgePort(t)
		}
		p.slots = append(p.slots, s)
	}
	p.store = "s3://" + cfg.bucket + "?anonymous=true&endpoint="
	for _, s := range p.slots {
		p.wg.Go(func() { s.supervise(ctx) })
	}
	t.Cleanup(p.wg.Wait)
	return p
}

// Runners' bridges listen on ports from 21000 to 22999, below those the
// kernel gives listeners on port 0 (32768 and up on Linux, 49152 and up on
// macOS). A port it gave and took back could be one of the suite's own
// proxies' before the runner listens on it, and every start of the runner
// would fail.
const (
	firstBridgePort = 21000
	bridgePorts     = 2000
)

var bridgePortsTaken atomic.Int64

// bridgePort is a port for a slot's runner's bridge that no other slot has
// had, and that's free now.
func bridgePort(t *testing.T) int {
	t.Helper()
	host := cmp.Or(os.Getenv("OPENETL_TEST_BRIDGE_HOST"), "0.0.0.0")
	for range bridgePorts {
		port := firstBridgePort + int(bridgePortsTaken.Add(1)%bridgePorts)
		var lc net.ListenConfig
		if ln, err := lc.Listen(t.Context(), "tcp", net.JoinHostPort(host, strconv.Itoa(port))); err == nil {
			_ = ln.Close()
			return port
		}
	}
	t.Fatalf("no port from %d to %d is free for a runner's bridge", firstBridgePort, firstBridgePort+bridgePorts-1)
	return 0
}

// supervise starts the slot's runner, and starts it again 1 to 5 seconds
// after it dies, until ctx ends.
func (s *slot) supervise(ctx context.Context) {
	rng := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), uint64(s.i)))
	for ctx.Err() == nil {
		r, err := s.start()
		if err != nil {
			fmt.Fprintf(os.Stderr, "chaos: slot %d: %v\n", s.i, err)
		} else {
			select {
			case <-r.done:
			case <-ctx.Done():
				_ = r.cmd.Process.Kill()
				<-r.done
			}
			s.mu.Lock()
			s.cur = nil
			s.mu.Unlock()
			if !s.launcher {
				s.save(r)
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(time.Second + upTo(rng, 4*time.Second)):
		}
	}
	// The pods of launchers that were killed are still running.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range s.groups {
		_ = syscall.Kill(-g, syscall.SIGKILL)
	}
}

func (s *slot) start() (*runner, error) {
	cfg := s.pool.cfg
	store := cfg.dir
	if s.s3 != nil {
		store = s.pool.store + url.QueryEscape("http://"+s.s3.addr())
	}
	args := []string{"runner",
		"--api-url", "http://" + s.api.addr(), "--report-interval", reportInterval(), "--capacity", runnerCapacity,
		"--data-plane", cfg.dataPlane, "--checkpoints", store,
		"--engine", env.engine, "--cache", env.cache, "--connectors", env.noConnectors, "--egress-allow", egressAllow, "--log-format", "text"}
	if s.bridge > 0 {
		host := cmp.Or(os.Getenv("OPENETL_TEST_BRIDGE_HOST"), "0.0.0.0")
		args = append(args, "--bridge-listen", fmt.Sprintf("%s:%d", host, s.bridge), "--bridge-advertise", fmt.Sprintf("host.docker.internal:%d", s.bridge))
	}
	if cfg.sparkSubmit != "" {
		args = append(args, "--spark-submit", cfg.sparkSubmit)
	}
	if s.launcher {
		args = append(args, "--launcher", "process", "--pod-min-size", "m")
	}
	cmd := exec.Command(env.openetld, args...)
	cmd.Env = append(os.Environ(), "OPENETL_RUNNER_TOKEN="+cfg.token)
	r := &runner{cmd: cmd, out: &syncBuffer{}, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = r.out, r.out
	var logs *os.File
	if s.launcher {
		// A pipe of exec's would keep Wait going until the last pod a killed
		// launcher left had ended, and closing it would end them by SIGPIPE.
		out, in, err := os.Pipe()
		if err != nil {
			return nil, err
		}
		defer func() { _ = in.Close() }()
		logs = out
		cmd.Stdout, cmd.Stderr, cmd.SysProcAttr = in, in, &syscall.SysProcAttr{Setpgid: true}
	}
	if err := cmd.Start(); err != nil {
		if logs != nil {
			_ = logs.Close()
		}
		return nil, err
	}
	go func() {
		_ = cmd.Wait()
		close(r.done)
	}()
	s.mu.Lock()
	s.cur = r
	s.started++
	if logs != nil {
		s.groups = append(s.groups, cmd.Process.Pid)
	}
	s.mu.Unlock()
	if logs != nil {
		s.pool.wg.Go(func() { s.follow(r, logs) })
	}
	return r, nil
}

// follow keeps what a launcher and its pods write until the last of them has
// gone, and notes what it says of the pods: see procs.Pods.
func (s *slot) follow(r *runner, logs *os.File) {
	defer func() { _ = logs.Close() }()
	br := bufio.NewReader(logs)
	for {
		line, err := br.ReadString('\n')
		_, _ = r.out.Write([]byte(line))
		s.mu.Lock()
		s.pods.Line(line)
		s.mu.Unlock()
		if err != nil {
			s.save(r)
			return
		}
	}
}

// save writes a runner's output to the logs directory, if there is one.
func (s *slot) save(r *runner) {
	if s.pool.logs == "" {
		return
	}
	s.mu.Lock()
	n := s.started
	s.mu.Unlock()
	name := filepath.Join(s.pool.logs, fmt.Sprintf("runner-%d-%03d-%d.log", s.i, n, r.cmd.Process.Pid))
	_ = os.WriteFile(name, []byte(r.out.String()), 0o600)
}

func (s *slot) runner() *runner {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cur
}

// signal sends sig to the slot's runner, and with SIGKILL waits for it to
// die. It reports whether there was a runner.
func (s *slot) signal(sig syscall.Signal) bool {
	r := s.runner()
	if r == nil {
		return false
	}
	if sig == syscall.SIGKILL {
		s.pool.book.died(s.i)
	}
	if err := r.cmd.Process.Signal(sig); err != nil {
		return false
	}
	if sig == syscall.SIGKILL {
		<-r.done
	}
	return true
}

// stop sends sig to what runs the pipeline: the slot's runner, or a launcher's
// pod, and with SIGKILL waits for it to die. It reports whether there was one.
func (s *slot) stop(pipeline string, sig syscall.Signal) bool {
	if !s.launcher {
		return s.signal(sig)
	}
	pod := s.pod(pipeline)
	if pod == 0 {
		return false
	}
	if syscall.Kill(pod, sig) != nil {
		return false
	}
	if sig == syscall.SIGKILL {
		for deadline := time.Now().Add(2 * time.Second); syscall.Kill(pod, 0) == nil && time.Now().Before(deadline); {
			time.Sleep(5 * time.Millisecond)
		}
	}
	return true
}

// workerPIDs are the PIDs of the pipeline's latest workers' pods on this
// slot's launcher that still run, by their number.
func (s *slot) workerPIDs(pipeline string) []int {
	s.mu.Lock()
	pids := s.pods.Workers(pipeline)
	s.mu.Unlock()
	var out []int
	for _, n := range slices.Sorted(maps.Keys(pids)) {
		if openetld(pids[n]) {
			out = append(out, pids[n])
		}
	}
	return out
}

// stopWorker kills a worker's pod of the pipeline, and waits for it to die.
func (s *slot) stopWorker(pid int) bool {
	if syscall.Kill(pid, syscall.SIGKILL) != nil {
		return false
	}
	for deadline := time.Now().Add(2 * time.Second); syscall.Kill(pid, 0) == nil && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// crashWorker kills the pipeline's worker that commits, worker 1, and
// reports whether there was one.
func (s *slot) crashWorker(pipeline string) bool {
	s.mu.Lock()
	pid := s.pods.Workers(pipeline)[1]
	s.mu.Unlock()
	return openetld(pid) && s.stopWorker(pid)
}

// openetld reports whether pid is an openetld process: the name guards
// against a reused PID.
func openetld(pid int) bool {
	if pid <= 0 {
		return false
	}
	name, err := exec.Command("ps", "-o", "comm=", "-p", strconv.Itoa(pid)).Output()
	return err == nil && filepath.Base(strings.TrimSpace(string(name))) == "openetld"
}

// crash kills what's writing the pipeline's checkpoints through this slot,
// and reports whether there was something to kill.
func (s *slot) crash(pipeline string) bool { return s.stop(pipeline, syscall.SIGKILL) }

// killLauncher kills the slot's launcher and leaves its pods running, as when
// a launcher's own pod dies on Kubernetes.
func (s *slot) killLauncher() bool {
	r := s.runner()
	if !s.launcher || r == nil || r.cmd.Process.Kill() != nil {
		return false
	}
	<-r.done
	return true
}

// pod is the PID of the pipeline's latest pod on this slot's launcher, or 0
// if it has gone. It can't be found through the engine, which may have exited
// at the end of a sync while its pod still runs. The name guards against a
// reused PID.
func (s *slot) pod(pipeline string) int {
	s.mu.Lock()
	pod := s.pods.Pod(pipeline)
	s.mu.Unlock()
	if !openetld(pod) {
		return 0
	}
	return pod
}

// engine is the PID of the engine the slot's runner, or one of its launcher's
// pods, last started for pipeline, or 0.
func (s *slot) engine(pipeline string) int {
	if s.launcher {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pods.Engine(pipeline)
	}
	r := s.runner()
	if r == nil {
		return 0
	}
	return procs.Engine(r.out.String(), pipeline)
}

// bound reports whether one of the pool's launchers ever ran a pod of the
// pipeline that it could find: see procs.Pods.
func (p *chaosPool) bound(pipeline string) bool {
	for _, s := range p.slots {
		s.mu.Lock()
		b := s.pods.Bound(pipeline)
		s.mu.Unlock()
		if b {
			return true
		}
	}
	return false
}

// podFault notes a fault at one of the slot's launcher's pods, workers'
// pods or a pod's connectors, which landed if it found what it kills, and
// returns landed. On a runner it notes nothing.
func (s *slot) podFault(landed bool) bool {
	if s.launcher {
		s.pool.podTried.Add(1)
		if landed {
			s.pool.podLanded.Add(1)
		}
	}
	return landed
}

// children are the PIDs of the pipeline's connectors whose names start
// with prefix: a launcher's pod's children, or the runner's in a pool the
// pipeline has to itself. A runner other pipelines may share has none, as
// its connectors can't be told apart.
func (s *slot) children(ctx context.Context, pipeline, prefix string) []int {
	h := procs.Host{Pod: s.launcher, Only: s.pool.dedicated()}
	if s.launcher {
		h.PID = s.pod(pipeline)
	} else if r := s.runner(); r != nil {
		h.PID = r.cmd.Process.Pid
	}
	return procs.Children(ctx, procs.Owner(h, pipeline), prefix)
}

// partition cuts the slot off from the API for d.
func (s *slot) partition(ctx context.Context, d time.Duration) {
	s.api.cut(true)
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
	s.api.cut(false)
}

// s3Proxy forwards a slot's object-store requests, and asks the fault book
// what to do with each.
type s3Proxy struct {
	slot   *slot
	target *url.URL
	tr     *http.Transport
	srv    *httptest.Server
}

func startS3Proxy(t *testing.T, s *slot, target *url.URL) *s3Proxy {
	t.Helper()
	p := &s3Proxy{slot: s, target: target, tr: &http.Transport{MaxIdleConnsPerHost: 64}}
	p.srv = httptest.NewServer(p)
	t.Cleanup(p.srv.Close)
	return p
}

func (p *s3Proxy) addr() string { return strings.TrimPrefix(p.srv.URL, "http://") }

// The kinds of request the fault book tells apart.
const (
	reqOther = iota
	reqRecord
	reqHead
)

var recordName = regexp.MustCompile(`^\d{20}-g(\d{20})\.json$`)

// A checkpoint write's generations: of the attempt that writes, and for a
// HEAD, of the record it names; 0 where unknown or none.
type gens struct{ writer, record int64 }

// classify returns the pipeline a path-style request is under, whether it
// writes a checkpoint record or HEAD, and its generations: a record's from
// its name, a HEAD's from its body, which it reads and puts back.
func classify(r *http.Request) (pipeline string, kind int, g gens) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 4)
	if len(parts) < 4 {
		return "", reqOther, g
	}
	pipeline, rest := parts[2], parts[3]
	if r.Method != http.MethodPut {
		return pipeline, reqOther, g
	}
	name, ok := strings.CutPrefix(rest, "_checkpoints/")
	if ok && name == "HEAD" {
		return pipeline, reqHead, headGens(r)
	}
	if m := recordName.FindStringSubmatch(name); ok && m != nil {
		gen, err := strconv.ParseInt(m[1], 10, 64)
		if err == nil {
			return pipeline, reqRecord, gens{writer: gen, record: gen}
		}
	}
	return pipeline, reqOther, g
}

// headGens reads a HEAD write's generations from its body, the JSON of a
// checkpoint.Head, inside an aws-chunked body too, and puts the body back.
func headGens(r *http.Request) gens {
	body, err := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	if err != nil {
		return gens{}
	}
	from, to := bytes.IndexByte(body, '{'), bytes.LastIndexByte(body, '}')
	if from < 0 || to < from {
		return gens{}
	}
	var h checkpoint.Head
	if json.Unmarshal(body[from:to+1], &h) != nil || h.Generation > math.MaxInt64 {
		return gens{}
	}
	g := gens{writer: int64(h.Generation)}
	if h.Record != nil && h.Record.Generation <= math.MaxInt64 {
		g.record = int64(h.Record.Generation)
	}
	return g
}

func (p *s3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pipeline, kind, gen := classify(r)
	act := p.slot.pool.book.decide(p.slot.i, pipeline, kind, gen.writer)
	if act.delay > 0 {
		time.Sleep(act.delay)
	}
	switch act.kind {
	case actKillBefore:
		if p.kill(pipeline, act) {
			panic(http.ErrAbortHandler)
		}
		// With nothing to kill, the write goes on as if no point were armed.
		act = action{}
	case actStatus:
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(act.status)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected</Message></Error>`, act.code)
		return
	}
	killAfter := act.kind == actKillAfter || act.kind == actKillLater
	res, err := p.forward(r)
	if err != nil {
		switch {
		case killAfter && r.Context().Err() != nil:
			// What wrote hung up, as it does when it dies mid-write.
			p.slot.pool.book.missed(pipeline, act, "the writer went away")
		case killAfter:
			p.slot.pool.book.count(pipeline, "missed crash "+act.point+": the write failed")
		}
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = res.Body.Close() }()
	ok := res.StatusCode/100 == 2
	if ok {
		p.slot.pool.book.wrote(p.slot.i, pipeline, kind, gen.record)
	} else if killAfter {
		p.slot.pool.book.count(pipeline, "missed crash "+act.point+": the write failed")
	}
	switch {
	case act.kind == actKillAfter && ok:
		// What wrote dies before it sees the response, but the worker that
		// commits as its coordinator sees it. With nothing to kill, the
		// response goes back as if no point were armed.
		if p.kill(pipeline, act) && !act.worker {
			panic(http.ErrAbortHandler)
		}
	case act.kind == actDrop:
		_, _ = io.Copy(io.Discard, res.Body)
		panic(http.ErrAbortHandler)
	case act.kind == actKillLater && ok:
		time.AfterFunc(act.later, func() { p.kill(pipeline, act) })
	}
	for k, vs := range res.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, res.Body)
}

// kill carries out the crash point act fired for, the coordinator or the
// pod or runner that wrote, or the worker that commits, and reports whether
// it killed. If there's nothing to kill, the fault book arms the point
// again: see faultBook.missed.
func (p *s3Proxy) kill(pipeline string, act action) bool {
	sl := p.slot
	var killed bool
	why := "nothing to kill"
	switch {
	case act.worker:
		killed, why = sl.podFault(sl.crashWorker(pipeline)), "no worker to kill"
	case sl.launcher:
		killed, why = sl.podFault(sl.crash(pipeline)), "no pod to kill"
	default:
		killed = sl.crash(pipeline)
	}
	if killed {
		sl.pool.book.killed(pipeline, act)
	} else {
		sl.pool.book.missed(pipeline, act, why)
	}
	return killed
}

func (p *s3Proxy) forward(r *http.Request) (*http.Response, error) {
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL.Scheme, out.URL.Host = p.target.Scheme, p.target.Host
	out.Host = p.target.Host
	return p.tr.RoundTrip(out)
}
