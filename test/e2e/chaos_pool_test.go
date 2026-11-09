//go:build e2e && chaos

package e2e

import (
	"bufio"
	"cmp"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
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
	// For each pipeline, a launcher's pods' latest engine and latest pod; and
	// the process group of each launcher it has run.
	engines, pods map[string]int
	groups        []int
}

func startPool(ctx context.Context, t *testing.T, cfg poolConfig, logs string, book *faultBook) *chaosPool {
	t.Helper()
	p := &chaosPool{cfg: cfg, logs: logs, book: book}
	target, err := url.Parse(s3Endpoint())
	if err != nil {
		t.Fatal(err)
	}
	for i := range cfg.runners {
		s := &slot{i: cfg.first + i, pool: p, launcher: i < cfg.launchers, engines: map[string]int{}, pods: map[string]int{}, results: startAPIFilter(t, env.apiURL)}
		s.api = startProxy(t, strings.TrimPrefix(s.results.url(), "http://"))
		if cfg.bucket != "" {
			s.s3 = startS3Proxy(t, s, target)
		}
		if cfg.bridge {
			s.bridge = freePort(t)
		}
		p.slots = append(p.slots, s)
	}
	p.store = "s3://" + cfg.bucket + "?endpoint="
	for _, s := range p.slots {
		p.wg.Go(func() { s.supervise(ctx) })
	}
	t.Cleanup(p.wg.Wait)
	return p
}

func freePort(t *testing.T) int {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port
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
// gone, and notes each pipeline's latest engine and pod.
func (s *slot) follow(r *runner, logs *os.File) {
	defer func() { _ = logs.Close() }()
	br := bufio.NewReader(logs)
	for {
		line, err := br.ReadString('\n')
		_, _ = r.out.Write([]byte(line))
		if m := engineReady.FindStringSubmatch(line); m != nil {
			pid, _ := strconv.Atoi(m[2])
			s.mu.Lock()
			s.engines[m[1]] = pid
			s.mu.Unlock()
		}
		if m := podStarted.FindStringSubmatch(line); m != nil {
			pid, _ := strconv.Atoi(m[1])
			s.mu.Lock()
			s.pods[m[2]] = pid
			s.mu.Unlock()
		}
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
	if sig == syscall.SIGKILL {
		s.pool.book.podDied(pipeline)
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

// crash kills what's writing the pipeline's checkpoints through this slot.
func (s *slot) crash(pipeline string) {
	if !s.stop(pipeline, syscall.SIGKILL) {
		s.pool.book.count(pipeline, "crash with nothing to kill")
	}
}

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
	pod := s.pods[pipeline]
	s.mu.Unlock()
	if pod <= 0 {
		return 0
	}
	name, err := exec.Command("ps", "-o", "comm=", "-p", strconv.Itoa(pod)).Output()
	if err != nil || filepath.Base(strings.TrimSpace(string(name))) != "openetld" {
		return 0
	}
	return pod
}

var (
	engineReady = regexp.MustCompile(`msg="engine ready" .*pipeline=(\S+) .* pid=(\d+)`)
	podStarted  = regexp.MustCompile(`msg="runner running one attempt" .*id=\S+:(\d+) attempt=(?:sync|stream)/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})`)
)

// engine is the PID of the engine the slot's runner, or one of its launcher's
// pods, last started for pipeline, or 0.
func (s *slot) engine(pipeline string) int {
	if s.launcher {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.engines[pipeline]
	}
	r := s.runner()
	if r == nil {
		return 0
	}
	pid := 0
	for _, m := range engineReady.FindAllStringSubmatch(r.out.String(), -1) {
		if m[1] == pipeline {
			pid, _ = strconv.Atoi(m[2])
		}
	}
	return pid
}

// children are the PIDs of the child processes, whose names start with prefix,
// of what runs the pipeline: the slot's runner, or a launcher's pod.
func (s *slot) children(pipeline, prefix string) []int {
	parent := 0
	if s.launcher {
		parent = s.pod(pipeline)
	} else if r := s.runner(); r != nil {
		parent = r.cmd.Process.Pid
	}
	if parent == 0 {
		return nil
	}
	out, _ := exec.Command("pgrep", "-P", strconv.Itoa(parent), "^"+prefix).Output()
	var pids []int
	for _, f := range strings.Fields(string(out)) {
		if pid, err := strconv.Atoi(f); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
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

var recordName = regexp.MustCompile(`^\d{20}-g\d{20}\.json$`)

// classify returns the pipeline a path-style request is under, and whether
// it writes a checkpoint record or HEAD.
func classify(r *http.Request) (pipeline string, kind int) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 4)
	if len(parts) < 4 {
		return "", reqOther
	}
	pipeline, rest := parts[2], parts[3]
	if r.Method != http.MethodPut {
		return pipeline, reqOther
	}
	switch name, ok := strings.CutPrefix(rest, "_checkpoints/"); {
	case ok && name == "HEAD":
		return pipeline, reqHead
	case ok && recordName.MatchString(name):
		return pipeline, reqRecord
	}
	return pipeline, reqOther
}

func (p *s3Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pipeline, kind := classify(r)
	act := p.slot.pool.book.decide(p.slot.i, pipeline, kind)
	if act.delay > 0 {
		time.Sleep(act.delay)
	}
	switch act.kind {
	case actKillBefore:
		p.slot.crash(pipeline)
		panic(http.ErrAbortHandler)
	case actStatus:
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(act.status)
		_, _ = fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><Error><Code>%s</Code><Message>injected</Message></Error>`, act.code)
		return
	}
	res, err := p.forward(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = res.Body.Close() }()
	ok := res.StatusCode/100 == 2
	if ok {
		p.slot.pool.book.wrote(p.slot.i, pipeline, kind)
	}
	switch {
	case act.kind == actKillAfter && ok:
		p.slot.crash(pipeline)
		panic(http.ErrAbortHandler)
	case act.kind == actDrop:
		_, _ = io.Copy(io.Discard, res.Body)
		panic(http.ErrAbortHandler)
	case act.kind == actKillLater && ok:
		time.AfterFunc(act.later, func() { p.slot.crash(pipeline) })
	}
	for k, vs := range res.Header {
		w.Header()[k] = vs
	}
	w.WriteHeader(res.StatusCode)
	_, _ = io.Copy(w, res.Body)
}

func (p *s3Proxy) forward(r *http.Request) (*http.Response, error) {
	out := r.Clone(r.Context())
	out.RequestURI = ""
	out.URL.Scheme, out.URL.Host = p.target.Scheme, p.target.Host
	out.Host = p.target.Host
	return p.tr.RoundTrip(out)
}
