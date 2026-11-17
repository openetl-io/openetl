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
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	enginev1 "github.com/openetl-io/openetl/gen/openetl/engine/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/ints"
	"github.com/openetl-io/openetl/internal/platform/logging"
	"github.com/openetl-io/openetl/internal/platform/resources"
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
	// Counts what each engine uses. Nil: nothing does.
	Meter *resources.Meter
	// The job's size, which sets each engine's threads and memory limit. Zero:
	// one thread per core and no limit.
	Size resources.Size
	// What each engine may keep on local disk, in bytes, in a directory of
	// its own in the system's temporary directory that goes when it stops:
	// half of it for its state's disk cache. Zero: none, and no disk cache.
	Scratch int64
	// Batches reach the sink as they came, without the engine's coercion,
	// naming or metadata columns, and rows fail the job. For measuring the
	// boundary.
	Passthrough bool
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
	// The engine's exit code on reaching its memory limit, as Limits says.
	exitOverLimit = 75
	// The file in an engine's directory that its runner holds locked while
	// the engine may run, so that no other runner's sweep removes it.
	dirLock = "lock"
	// The engine's scratch, in its directory.
	scratchDir = "scratch"
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
	lock   *os.File
	data   string
	stdin  io.WriteCloser
	conn   *grpc.ClientConn
	client enginev1.EngineServiceClient
	log    *slog.Logger
	peak   func(int64)
	meter  *resources.Meter
	size   resources.Size
	// Closed once the process has exited and its output is drained.
	exited chan struct{}
	killed atomic.Bool
}

func start(ctx context.Context, opts Options) (*process, error) {
	bin, err := opts.binary()
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "engineclient: find openetl-engine")
	}
	sweep()
	dir, err := os.MkdirTemp("", "oe-")
	if err != nil {
		return nil, errs.Wrap(errs.Internal, err, "engineclient: make the socket directory")
	}
	lock, err := lockDir(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, errs.Wrap(errs.Internal, err, "engineclient: lock the engine's directory")
	}
	control := filepath.Join(dir, "control.sock")
	p := &process{dir: dir, lock: lock, data: filepath.Join(dir, "data.sock"), peak: opts.PeakMemory, meter: opts.Meter, size: opts.Size, exited: make(chan struct{})}
	if len(control) > maxSocketPath {
		p.removeDir()
		return nil, errs.New(errs.ConfigInvalid, "engineclient: socket path %s is too long; set a shorter TMPDIR", control)
	}

	log := opts.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	args := []string{"serve", "--control", control, "--data", p.data, "--watch-stdin"}
	if opts.Size != (resources.Size{}) {
		args = append(args, "--threads", strconv.Itoa(opts.Size.Threads()))
	}
	// close stops it, not ctx's end.
	p.cmd = exec.CommandContext(context.WithoutCancel(ctx), bin, args...)
	p.cmd.Env = engineEnv()
	p.meter.Confine(p.cmd)
	stdin, err1 := p.cmd.StdinPipe()
	stdout, err2 := p.cmd.StdoutPipe()
	stderr, err3 := p.cmd.StderrPipe()
	if err := errors.Join(err1, err2, err3); err != nil {
		p.removeDir()
		return nil, errs.Wrap(errs.Internal, err, "engineclient: pipes")
	}
	p.stdin = stdin
	if err := p.cmd.Start(); err != nil {
		p.removeDir()
		return nil, errs.Wrap(errs.Internal, err, "engineclient: start "+bin)
	}
	p.log = log.With("component", "engine", "pid", p.cmd.Process.Pid)
	resources.KillFirst(p.cmd.Process.Pid)
	metered := p.meter.Track(p.cmd.Process)
	engines.Add(ctx, 1)

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
		metered(p.cmd.ProcessState)
		ctx := context.WithoutCancel(ctx)
		engines.Add(ctx, -1)
		if st := p.cmd.ProcessState; st != nil {
			engineCPU.Add(ctx, st.UserTime().Seconds(), userCPU)
			engineCPU.Add(ctx, st.SystemTime().Seconds(), systemCPU)
		}
		close(p.exited)
	}()

	t := time.NewTimer(readyTimeout)
	defer t.Stop()
	select {
	case <-ready:
	case <-p.exited:
		p.removeDir()
		return nil, p.exitError()
	case <-t.C:
		p.close()
		return nil, errs.New(errs.Internal, "engineclient: the engine wasn't ready after %v", readyTimeout)
	case <-ctx.Done():
		p.close()
		return nil, ctx.Err()
	}

	p.conn, err = grpc.NewClient("unix://"+control, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithStatsHandler(otelgrpc.NewClientHandler()))
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
	p.meter.Sample()
	hwm := resources.HighWater(p.cmd.Process.Pid)
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
	p.removeDir()
	if n := resources.ExitPeak(p.cmd.ProcessState, hwm); n > 0 {
		enginePeak.Record(context.Background(), n)
		if p.peak != nil {
			p.peak(n)
		}
	}
}

// removeDir removes the engine's directory, its scratch with it, and then
// lets go of its lock.
func (p *process) removeDir() {
	_ = os.RemoveAll(p.dir)
	_ = p.lock.Close()
}

// limits are what the engine may use: its memory, and its scratch. Nil for
// neither.
func (p *process) limits(o Options) *enginev1.Limits {
	var l enginev1.Limits
	if o.Size != (resources.Size{}) {
		l.MemoryBytes = ints.Uint64(o.Size.EngineMemory())
	}
	if o.Scratch > 0 {
		l.ScratchDir, l.ScratchBytes = filepath.Join(p.dir, scratchDir), ints.Uint64(o.Scratch)
	}
	if l.MemoryBytes == 0 && l.ScratchDir == "" {
		return nil
	}
	return &l
}

// lockDir locks a new engine directory for as long as the file it gives is
// open: the lock is taken before the file has its name, so a sweep never
// finds it unlocked.
func lockDir(dir string) (*os.File, error) {
	f, err := os.CreateTemp(dir, dirLock+".")
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := os.Rename(f.Name(), filepath.Join(dir, dirLock)); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// sweep removes the engine directories, with their scratch, that runners
// which died left in the system's temporary directory: those whose lock no
// process holds. One being made has no lock yet, and stays.
func sweep() {
	dirs, _ := filepath.Glob(filepath.Join(os.TempDir(), "oe-*"))
	for _, d := range dirs {
		root, err := os.OpenRoot(d)
		if err != nil {
			continue
		}
		f, err := root.Open(dirLock)
		_ = root.Close()
		if err != nil {
			continue
		}
		if syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
			_ = os.RemoveAll(d)
		}
		_ = f.Close()
	}
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
	if st.ExitCode() == exitOverLimit {
		return errs.New(errs.ResourceExhausted, "engineclient: the engine reached its memory limit of %d MiB; run the pipeline with a larger resources.size", p.size.EngineMemory()/resources.MiB)
	}
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL && !p.killed.Load() {
		return errs.New(errs.ResourceExhausted, "engineclient: the engine was killed, likely for running out of memory")
	}
	return errs.New(errs.Internal, "engineclient: the engine exited unexpectedly: %v", st)
}

// engineEnv is the runner's environment, so the engine exports telemetry with
// its OTEL_* settings, under its own service name.
func engineEnv() []string {
	env := slices.DeleteFunc(os.Environ(), func(kv string) bool { return strings.HasPrefix(kv, "OTEL_SERVICE_NAME=") })
	return append(env, "OTEL_SERVICE_NAME=openetl-engine")
}

// relayLogs re-logs the engine's JSON log lines.
func (p *process) relayLogs(r io.Reader) {
	sc := logging.Lines(r)
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
	if err := sc.Err(); err != nil {
		p.log.Warn("reading the engine's logs failed", "err", err)
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
