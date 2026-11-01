// Package engineclient starts openetl-engine processes and drives them: the
// control client, and the framed Arrow IPC edges.
package engineclient

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/unsuitable001/openetl/gen/openetl/engine/v1"
	"github.com/unsuitable001/openetl/internal/platform/errs"
)

type Options struct {
	// The openetl-engine executable. Empty: next to this executable, then on
	// PATH.
	Binary string
	Logger *slog.Logger
	// Allocates batches read from the engine. Nil: memory.DefaultAllocator.
	Allocator memory.Allocator
	// PeakMemory, if set, gets each engine's peak resident memory in bytes
	// once it has stopped, when that can be read.
	PeakMemory func(bytes int64)
}

const (
	readyTimeout = 10 * time.Second
	stopTimeout  = 2 * time.Second
	exitTimeout  = 5 * time.Second
	// sun_path is 104 bytes on darwin, including the terminating NUL.
	maxSocketPath = 103
	// Unix sockets default to small buffers (8 KiB on darwin), which costs a
	// context switch every few KiB of a batch. The engine uses the same size.
	edgeSocketBuffer = 1 << 20
)

func (o Options) binary() (string, error) {
	if o.Binary != "" {
		return o.Binary, nil
	}
	if exe, err := os.Executable(); err == nil {
		p := filepath.Join(filepath.Dir(exe), "openetl-engine")
		if _, err := os.Stat(p); err == nil {
			return p, nil
		}
	}
	return exec.LookPath("openetl-engine")
}

// process is one running engine.
type process struct {
	cmd    *exec.Cmd
	dir    string
	data   string
	stdin  io.WriteCloser
	conn   *grpc.ClientConn
	client enginev1.EngineServiceClient
	log    *slog.Logger
	peak   func(int64)
	// Closed once the process has exited and its output is drained.
	exited chan struct{}
	killed atomic.Bool
}

func start(ctx context.Context, opts Options) (*process, error) {
	bin, err := opts.binary()
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "engineclient: find openetl-engine")
	}
	dir, err := os.MkdirTemp("", "oe-")
	if err != nil {
		return nil, errs.Wrap(errs.Internal, err, "engineclient: make the socket directory")
	}
	control := filepath.Join(dir, "control.sock")
	p := &process{dir: dir, data: filepath.Join(dir, "data.sock"), peak: opts.PeakMemory, exited: make(chan struct{})}
	if len(control) > maxSocketPath {
		_ = os.RemoveAll(dir)
		return nil, errs.New(errs.ConfigInvalid, "engineclient: socket path %s is too long; set a shorter TMPDIR", control)
	}

	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	p.cmd = exec.Command(bin, "serve", "--control", control, "--data", p.data, "--watch-stdin") //nolint:gosec,noctx // the operator configures the binary; close stops it
	stdin, err1 := p.cmd.StdinPipe()
	stdout, err2 := p.cmd.StdoutPipe()
	stderr, err3 := p.cmd.StderrPipe()
	if err := errors.Join(err1, err2, err3); err != nil {
		_ = os.RemoveAll(dir)
		return nil, errs.Wrap(errs.Internal, err, "engineclient: pipes")
	}
	p.stdin = stdin
	if err := p.cmd.Start(); err != nil {
		_ = os.RemoveAll(dir)
		return nil, errs.Wrap(errs.Internal, err, "engineclient: start "+bin)
	}
	p.log = log.With("component", "engine", "pid", p.cmd.Process.Pid)

	// Wait closes the pipes, so it runs once both are drained.
	ready := make(chan struct{})
	var drained sync.WaitGroup
	drained.Go(func() {
		sc := bufio.NewScanner(stdout)
		if sc.Scan() && sc.Text() == "ready" {
			close(ready)
		}
		_, _ = io.Copy(io.Discard, stdout)
	})
	drained.Go(func() { p.relayLogs(stderr) })
	go func() {
		drained.Wait()
		_ = p.cmd.Wait()
		close(p.exited)
	}()

	t := time.NewTimer(readyTimeout)
	defer t.Stop()
	select {
	case <-ready:
	case <-p.exited:
		_ = os.RemoveAll(dir)
		return nil, p.exitError()
	case <-t.C:
		p.close()
		return nil, errs.New(errs.Internal, "engineclient: the engine wasn't ready after %v", readyTimeout)
	case <-ctx.Done():
		p.close()
		return nil, ctx.Err()
	}

	p.conn, err = grpc.NewClient("unix://"+control, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		p.close()
		return nil, errs.Wrap(errs.Internal, err, "engineclient: control client")
	}
	p.client = enginev1.NewEngineServiceClient(p.conn)
	return p, nil
}

// dialEdge connects one edge of the job's plan.
func (p *process) dialEdge(ctx context.Context, id string) (*net.UnixConn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", p.data)
	if err != nil {
		return nil, fmt.Errorf("engineclient: connect edge %s: %w", id, err)
	}
	conn := c.(*net.UnixConn)
	if err := errors.Join(conn.SetReadBuffer(edgeSocketBuffer), conn.SetWriteBuffer(edgeSocketBuffer)); err != nil {
		p.log.Warn("sizing an edge's socket buffers failed", "edge", id, "err", err)
	}
	hello, err := proto.Marshal(&enginev1.Hello{Edge: id})
	if err == nil {
		fw := newFrameWriter(conn)
		if err = fw.write(enginev1.FrameKind_FRAME_KIND_HELLO, 0, hello); err == nil {
			err = fw.flush()
		}
	}
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("engineclient: open edge %s: %w", id, err)
	}
	return conn, nil
}

// close stops the engine and waits for it to exit. It is safe to call after
// the engine has died.
func (p *process) close() {
	var hwm int64
	if p.peak != nil {
		hwm = p.highWater()
	}
	if p.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		_, _ = p.client.Stop(ctx, &enginev1.StopRequest{})
		cancel()
	}
	if p.conn != nil {
		_ = p.conn.Close()
	}
	_ = p.stdin.Close()
	t := time.NewTimer(exitTimeout)
	select {
	case <-p.exited:
	case <-t.C:
		p.log.Warn("the engine didn't exit after Stop; killing it")
		p.kill()
		<-p.exited
	}
	t.Stop()
	_ = os.RemoveAll(p.dir)
	if p.peak != nil {
		if n := p.peakMemory(hwm); n > 0 {
			p.peak(n)
		}
	}
}

// highWater reads the running engine's peak resident memory on Linux. Its
// rusage won't do there: Go starts processes with CLONE_VM, so execve carries
// the runner's own peak into the engine's maxrss.
func (p *process) highWater() int64 {
	if runtime.GOOS != "linux" {
		return 0
	}
	b, err := os.ReadFile("/proc/" + strconv.Itoa(p.cmd.Process.Pid) + "/status")
	if err != nil {
		return 0
	}
	for line := range strings.Lines(string(b)) {
		if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(v), " kB"), 10, 64)
			if err != nil {
				return 0
			}
			return kb << 10
		}
	}
	return 0
}

// peakMemory is the exited engine's peak resident memory in bytes, given
// what highWater read before it stopped; 0 if unknown.
func (p *process) peakMemory(hwm int64) int64 {
	if runtime.GOOS == "linux" {
		return hwm
	}
	ru, ok := p.cmd.ProcessState.SysUsage().(*syscall.Rusage)
	if !ok {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return ru.Maxrss
	}
	return ru.Maxrss << 10
}

func (p *process) kill() {
	p.killed.Store(true)
	_ = p.cmd.Process.Kill()
}

// waitExit returns why the engine exited, if it exits within d.
func (p *process) waitExit(d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-p.exited:
		return p.exitError()
	case <-t.C:
		return nil
	}
}

// exitError classifies an exit nobody asked for. Call it after exited.
func (p *process) exitError() error {
	st := p.cmd.ProcessState
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL && !p.killed.Load() {
		return errs.New(errs.ResourceExhausted, "engineclient: the engine was killed, likely for running out of memory")
	}
	return errs.New(errs.Internal, "engineclient: the engine exited unexpectedly: %v", st)
}

// relayLogs re-logs the engine's JSON log lines.
func (p *process) relayLogs(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			p.log.Info(sc.Text())
			continue
		}
		msg, _ := m["message"].(string)
		lvl, _ := m["level"].(string)
		var attrs []slog.Attr
		for k, v := range m {
			switch k {
			case "message", "level", "timestamp", "target":
			default:
				attrs = append(attrs, slog.Any(k, v))
			}
		}
		slices.SortFunc(attrs, func(a, b slog.Attr) int { return strings.Compare(a.Key, b.Key) })
		p.log.LogAttrs(context.Background(), level(lvl), msg, attrs...)
	}
}

func level(s string) slog.Level {
	switch s {
	case "ERROR":
		return slog.LevelError
	case "WARN":
		return slog.LevelWarn
	case "DEBUG", "TRACE":
		return slog.LevelDebug
	default:
		return slog.LevelInfo
	}
}
