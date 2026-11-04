// Package external runs external connectors: executables that speak the
// connector protocol over a Unix socket, with host services on a second one.
// The runner starts each as a subprocess, so only trusted connectors run
// this way.
package external

import (
	"bufio"
	"bytes"
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
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/unsuitable001/openetl/internal/connectorhost/hostservices"
	"github.com/unsuitable001/openetl/internal/netguard"
	"github.com/unsuitable001/openetl/internal/platform/errs"
	connspec "github.com/unsuitable001/openetl/internal/spec"
	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

const (
	readyTimeout = 10 * time.Second
	exitTimeout  = 5 * time.Second
	specTimeout  = 10 * time.Second
	// sun_path is 104 bytes on darwin, including the terminating NUL.
	maxSocketPath = 103
	socketBuffer  = 1 << 20
	// gRPC's own defaults, 32 KiB buffers and 64 KiB windows, cost a syscall
	// or a window update every few KiB of a batch.
	flowWindow = 16 << 20
)

// The runner's environment holds its own credentials, so a connector gets
// only these.
var passEnv = []string{"PATH", "HOME", "TMPDIR", "TZ", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR"}

// Options is what a connector's process gets from the runner.
type Options struct {
	// Which addresses host services may reach besides public ones.
	Egress netguard.Policy
	// Nil: slog's default.
	Logger *slog.Logger
	// The spec the connector must return: its descriptor's. Nil: any spec of
	// the right kind.
	Spec *protocolv1.SpecResponse
	// Serves a staged destination's files and cursors (SetStaging).
	Staging hostservices.Staging
}

// process is one running connector.
type process struct {
	cmd   *exec.Cmd
	dir   string
	stdin io.WriteCloser
	conn  *grpc.ClientConn
	spec  *protocolv1.SpecResponse
	// The config, with secret names in place of the secrets.
	config []byte
	// Set once the spec is read; logs relayed before then aren't scrubbed.
	hs   atomic.Pointer[hostservices.Services]
	host *grpc.Server
	log  *slog.Logger
	// Closed once the process has exited and its output is drained.
	exited chan struct{}
	killed atomic.Bool
}

// start runs exe, reads its spec and serves its host services, which hold
// the config's secrets.
func start(ctx context.Context, exe string, kind protocolv1.ConnectorKind, config []byte, opts Options) (*process, error) {
	dir, err := os.MkdirTemp("", "oc-")
	if err != nil {
		return nil, errs.Wrap(errs.Internal, err, "connector: make the socket directory")
	}
	socket, hostSocket := filepath.Join(dir, "c.sock"), filepath.Join(dir, "h.sock")
	if len(hostSocket) > maxSocketPath {
		_ = os.RemoveAll(dir)
		return nil, errs.New(errs.ConfigInvalid, "connector: socket path %s is too long; set a shorter TMPDIR", hostSocket)
	}
	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}
	p := &process{dir: dir, exited: make(chan struct{}), log: log.With("connector", filepath.Base(exe))}
	var lc net.ListenConfig
	hostLn, err := lc.Listen(ctx, "unix", hostSocket)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, errs.Wrap(errs.Internal, err, "connector: host services socket")
	}
	if err := p.run(ctx, exe, socket, hostSocket); err != nil {
		_ = hostLn.Close()
		return nil, err
	}

	sctx, cancel := context.WithTimeout(ctx, specTimeout)
	spec, err := protocolv1.NewConnectorServiceClient(p.conn).Spec(sctx, &protocolv1.SpecRequest{})
	cancel()
	if err != nil {
		err = p.failure(errs.FromStatus(err))
		_ = hostLn.Close()
		p.close()
		return nil, fmt.Errorf("connector: spec: %w", err)
	}
	if kind != protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED && spec.GetKind() != kind {
		_ = hostLn.Close()
		p.close()
		return nil, errs.New(errs.ConfigInvalid, "connector %s is a %s, not a %s", filepath.Base(exe), spec.GetKind(), kind)
	}
	if opts.Spec != nil {
		if diff := connspec.SameSpec(spec, opts.Spec); diff != "" {
			_ = hostLn.Close()
			p.close()
			return nil, errs.New(errs.ConfigInvalid, "connector %s's spec isn't its descriptor's: %s", filepath.Base(exe), diff)
		}
	}
	p.spec = spec
	cfg, secrets, err := splitSecrets(spec.GetConfigSchema(), config)
	if err != nil {
		_ = hostLn.Close()
		p.close()
		return nil, err
	}
	p.config = cfg
	tier := spec.GetEgress().GetTier()
	hs := hostservices.New(hostservices.Options{
		Policy:        opts.Egress,
		Secrets:       secrets,
		Logger:        p.log,
		RevealSecrets: tier == protocolv1.EgressTier_EGRESS_TIER_PROXY || tier == protocolv1.EgressTier_EGRESS_TIER_RAW,
	})
	if opts.Staging != nil {
		hs.SetStaging(opts.Staging)
	}
	p.hs.Store(hs)
	p.host = grpc.NewServer(grpc.MaxRecvMsgSize(sdk.MaxMessageSize), grpc.MaxSendMsgSize(sdk.MaxMessageSize))
	protocolv1.RegisterHostServiceServer(p.host, hostservices.NewServer(hs))
	go func() { _ = p.host.Serve(hostLn) }()
	return p, nil
}

// ReadSpec runs exe just long enough to read its spec.
func ReadSpec(ctx context.Context, exe string, opts Options) (*protocolv1.SpecResponse, error) {
	p, err := start(ctx, exe, protocolv1.ConnectorKind_CONNECTOR_KIND_UNSPECIFIED, nil, opts)
	if err != nil {
		return nil, err
	}
	defer p.close()
	return p.spec, nil
}

// run starts the executable and connects to it once it's ready.
func (p *process) run(ctx context.Context, exe, socket, hostSocket string) error {
	p.cmd = exec.Command(exe) //nolint:noctx // close stops it
	for _, k := range passEnv {
		if v, ok := os.LookupEnv(k); ok {
			p.cmd.Env = append(p.cmd.Env, k+"="+v)
		}
	}
	p.cmd.Env = append(p.cmd.Env, sdk.EnvSocket+"="+socket, sdk.EnvHostSocket+"="+hostSocket, sdk.EnvWatchStdin+"=1")
	stdin, err1 := p.cmd.StdinPipe()
	stdout, err2 := p.cmd.StdoutPipe()
	stderr, err3 := p.cmd.StderrPipe()
	if err := errors.Join(err1, err2, err3); err != nil {
		_ = os.RemoveAll(p.dir)
		return errs.Wrap(errs.Internal, err, "connector: pipes")
	}
	p.stdin = stdin
	if err := p.cmd.Start(); err != nil {
		_ = os.RemoveAll(p.dir)
		return errs.Wrap(errs.ConfigInvalid, err, "connector: start "+exe)
	}
	p.log = p.log.With("pid", p.cmd.Process.Pid)

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
		_ = os.RemoveAll(p.dir)
		return p.exitError()
	case <-t.C:
		p.close()
		return errs.New(errs.Internal, "connector: %s wasn't ready after %v", filepath.Base(exe), readyTimeout)
	case <-ctx.Done():
		p.close()
		return ctx.Err()
	}
	var err error
	p.conn, err = grpc.NewClient("passthrough:///"+socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
		grpc.WithContextDialer(dialSized),
		grpc.WithReadBufferSize(socketBuffer), grpc.WithWriteBufferSize(socketBuffer),
		grpc.WithInitialWindowSize(flowWindow), grpc.WithInitialConnWindowSize(flowWindow),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(sdk.MaxMessageSize), grpc.MaxCallSendMsgSize(sdk.MaxMessageSize)))
	if err != nil {
		p.close()
		return errs.Wrap(errs.Internal, err, "connector: client")
	}
	return nil
}

func dialSized(ctx context.Context, addr string) (net.Conn, error) {
	var d net.Dialer
	c, err := d.DialContext(ctx, "unix", addr)
	if err != nil {
		return nil, err
	}
	if uc, ok := c.(*net.UnixConn); ok {
		_ = uc.SetReadBuffer(socketBuffer)
		_ = uc.SetWriteBuffer(socketBuffer)
	}
	return c, nil
}

// close stops the connector and waits for it to exit. It's safe to call
// after the connector has died.
func (p *process) close() {
	if p.conn != nil {
		_ = p.conn.Close()
	}
	_ = p.stdin.Close()
	t := time.NewTimer(exitTimeout)
	select {
	case <-p.exited:
	case <-t.C:
		p.log.Warn("the connector didn't exit after its stdin closed; killing it")
		p.kill()
		<-p.exited
	}
	t.Stop()
	if p.host != nil {
		p.host.Stop()
	}
	if hs := p.hs.Load(); hs != nil {
		hs.Close()
	}
	_ = os.RemoveAll(p.dir)
}

func (p *process) kill() {
	p.killed.Store(true)
	_ = p.cmd.Process.Kill()
}

// failure is why the connector exited, if it exits soon, or else err. A
// call can fail before the exit is seen.
func (p *process) failure(err error) error {
	t := time.NewTimer(100 * time.Millisecond)
	defer t.Stop()
	select {
	case <-p.exited:
		return p.exitError()
	case <-t.C:
		return err
	}
}

// exitError classifies an exit nobody asked for. Call it after exited.
func (p *process) exitError() error {
	st := p.cmd.ProcessState
	if ws, ok := st.Sys().(syscall.WaitStatus); ok && ws.Signaled() && ws.Signal() == syscall.SIGKILL && !p.killed.Load() {
		return errs.New(errs.ResourceExhausted, "connector: killed, likely for running out of memory")
	}
	return errs.New(errs.Internal, "connector: exited unexpectedly: %v", st)
}

// relayLogs logs the connector's stderr through its host services, which
// scrub the secrets they hold. JSON lines from slog keep their level.
func (p *process) relayLogs(r io.Reader) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 1<<20)
	for sc.Scan() {
		e := host.LogEntry{Level: slog.LevelInfo, Message: sc.Text()}
		var m map[string]any
		if line := sc.Bytes(); bytes.HasPrefix(line, []byte("{")) && json.Unmarshal(line, &m) == nil {
			if msg, ok := m["msg"].(string); ok {
				e.Message = msg
				var lvl slog.Level
				if s, ok := m["level"].(string); ok && lvl.UnmarshalText([]byte(s)) == nil {
					e.Level = lvl
				}
				e.Attrs = map[string]string{}
				for k, v := range m {
					switch k {
					case "msg", "level", "time":
					default:
						e.Attrs[k] = fmt.Sprint(v)
					}
				}
			}
		}
		if hs := p.hs.Load(); hs != nil {
			_ = hs.Log(context.Background(), []host.LogEntry{e})
		} else {
			p.log.Log(context.Background(), e.Level, e.Message)
		}
	}
}
