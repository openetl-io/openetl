package conformance

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/configschema"
	"github.com/unsuitable001/openetl/sdk/go/internal/launch"
	"github.com/unsuitable001/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

const (
	readyTimeout = 30 * time.Second
	// sun_path is 104 bytes on darwin, including the terminating NUL.
	maxSocketPath = 103
	maxOutput     = 1 << 20
)

// Host is what the kit tells a connector about its platform in Spec. A
// platform's own tools set theirs.
var Host = &protocolv1.Host{Version: "dev", Protocol: "v1"}

// The runner passes a connector only these of its environment.
var passEnv = []string{"PATH", "HOME", "TMPDIR", "TZ", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR"}

// Process is a running connector, bound to one config.
type Process interface {
	Conn() grpc.ClientConnInterface
	// The config as the connector gets it: each secret's value replaced by
	// its name, which host services resolve.
	Config() []byte
	// What the connector logged, for a failed check's report.
	Output() string
	Close() error
}

// Launcher starts the connector with config's secrets in its host
// services. The process outlives ctx, which bounds only starting it.
type Launcher func(ctx context.Context, config []byte) (Process, error)

// Options are how a launcher starts a connector, besides what it runs.
type Options struct {
	// KEY=value pairs added to the connector's environment.
	Env []string
	// source or destination, for an executable or image with both roles.
	Role string
	// Reach the connector, and let it reach host services, over TCP with a
	// token rather than Unix sockets: as a container launcher does where the
	// runtime doesn't carry sockets into containers.
	TCP bool
	// Serves the connector's host services in place of the kit's, once its
	// spec is read, such as a platform's own, which make HTTP requests.
	// secrets are the config's, by dotted path.
	HostServices func(spec *protocolv1.SpecResponse, secrets map[string]string) (protocolv1.HostServiceServer, error)
	// Also gets what the connector writes on stderr.
	Logs io.Writer
}

// Exec starts the executable at exe as the runner does: with only the
// runner's environment and env's KEY=value pairs, and with the kit's host
// services, which resolve secrets and keep logs but make no HTTP requests.
func Exec(exe string, env ...string) Launcher { return ExecWith(exe, Options{Env: env}) }

// ExecWith starts the executable at exe as the runner does, with o.
func ExecWith(exe string, o Options) Launcher {
	return func(ctx context.Context, config []byte) (Process, error) {
		p, err := newProc(o)
		if err != nil {
			return nil, err
		}
		cmd := exec.Command(exe) //nolint:noctx // Close stops it
		for _, k := range passEnv {
			if v, ok := os.LookupEnv(k); ok {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
		}
		cmd.Env = append(cmd.Env, o.Env...)
		cmd.Env = append(cmd.Env, p.env()...)
		cmd.Env = append(cmd.Env, sdk.EnvWatchStdin+"=1")
		stdin, err1 := cmd.StdinPipe()
		stdout, err2 := cmd.StdoutPipe()
		cmd.Stderr = p.out
		if err := errors.Join(err1, err2); err != nil {
			p.cleanup()
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			p.cleanup()
			return nil, fmt.Errorf("start %s: %w", exe, err)
		}
		exited, ready := make(chan struct{}), readyLine(stdout)
		go func() {
			_ = cmd.Wait()
			close(exited)
		}()
		p.stop = func() {
			_ = stdin.Close()
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				_ = cmd.Process.Kill()
				<-exited
			}
		}
		if err := p.awaitReady(ctx, filepath.Base(exe), ready, exited); err != nil {
			return nil, err
		}
		if err := p.bind(ctx, config); err != nil {
			_ = p.Close()
			return nil, err
		}
		return p, nil
	}
}

// InProcess runs c in this process, with the kit's host services.
func InProcess(c sdk.Connector) Launcher {
	return func(ctx context.Context, config []byte) (Process, error) {
		p, err := newProc(Options{})
		if err != nil {
			return nil, err
		}
		cctx, cancel := context.WithCancel(context.Background())
		ready := make(chan struct{})
		done := make(chan struct{})
		var runErr error
		go func() {
			defer close(done)
			runErr = c.Run(cctx, sdk.Launch{Addr: p.addr, HostAddr: p.hostAddr}, func() { close(ready) })
		}()
		p.stop = func() {
			cancel()
			<-done
		}
		select {
		case <-ready:
		case <-done:
			_ = p.Close()
			return nil, fmt.Errorf("%s: %w", c.Name, runErr)
		case <-ctx.Done():
			_ = p.Close()
			return nil, ctx.Err()
		}
		if err := p.bind(ctx, config); err != nil {
			_ = p.Close()
			return nil, err
		}
		return p, nil
	}
}

// Attach serves host services and waits, until ctx ends, for a connector
// someone else starts at addr, unix:///path or tcp://host:port: under a
// debugger, say. announce gets the environment to start it with. Closing
// the process leaves the connector running.
func Attach(addr string, o Options, announce func(env []string)) Launcher {
	return func(ctx context.Context, config []byte) (Process, error) {
		a, err := launch.Parse(addr)
		if err != nil {
			return nil, err
		}
		o.TCP = a.Network == "tcp"
		p, err := newProc(o)
		if err != nil {
			return nil, err
		}
		p.addr, p.dial = a.String(), a
		announce(append(append([]string{}, o.Env...), p.env()...))
		if err := p.connect(); err != nil {
			_ = p.Close()
			return nil, err
		}
		for {
			sctx, cancel := context.WithTimeout(ctx, time.Second)
			_, err := protocolv1.NewConnectorServiceClient(p.conn).Spec(sctx, &protocolv1.SpecRequest{Host: Host})
			cancel()
			if err == nil {
				break
			}
			if status.Code(err) == codes.Unauthenticated {
				_ = p.Close()
				return nil, fmt.Errorf("the connector at %s refused this launch's token; start it again with the environment above", addr)
			}
			select {
			case <-ctx.Done():
				_ = p.Close()
				return nil, ctx.Err()
			case <-time.After(250 * time.Millisecond):
			}
		}
		if err := p.bind(ctx, config); err != nil {
			_ = p.Close()
			return nil, err
		}
		return p, nil
	}
}

// Build builds the main package pkg with the go command and returns the
// executable's path, for Exec.
func Build(t T, pkg string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "conformance-")
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	exe := filepath.Join(dir, filepath.Base(pkg))
	cmd := exec.CommandContext(t.Context(), "go", "build", "-o", exe, pkg) //nolint:gosec // the test names the package
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, out)
	}
	return exe
}

// proc is what the launchers share: the addresses, the connection and the
// host services, which start serving once the connector's spec is read, as
// the runner's do.
type proc struct {
	opts Options
	dir  string
	// Where the connector listens and finds host services, as it's told;
	// a container's differ from where the kit dials and listens.
	addr, hostAddr string
	dial           launch.Addr
	token          string
	hostLn         net.Listener
	host           *grpc.Server
	hs             *hostServer
	conn           *grpc.ClientConn
	config         []byte
	out            *output
	stop           func()
	once           sync.Once
}

// newProc listens for host services: on a Unix socket in a directory of its
// own, or on TCP loopback with a token.
func newProc(o Options) (*proc, error) {
	dir, err := os.MkdirTemp("", "oc-")
	if err != nil {
		return nil, err
	}
	p := &proc{opts: o, dir: dir, out: &output{tee: o.Logs}}
	p.hs = &hostServer{out: p.out, kv: map[string]kvEntry{}}
	host := launch.Addr{Network: "unix", Address: filepath.Join(dir, "h.sock")}
	p.dial = launch.Addr{Network: "unix", Address: filepath.Join(dir, "c.sock")}
	if o.TCP {
		port, err := freePort()
		if err != nil {
			_ = os.RemoveAll(dir)
			return nil, err
		}
		host = launch.Addr{Network: "tcp", Address: "127.0.0.1:0"}
		p.dial = launch.Addr{Network: "tcp", Address: net.JoinHostPort("127.0.0.1", strconv.Itoa(port))}
		p.token = launch.NewToken()
	} else if len(host.Address) > maxSocketPath {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("socket path %s is too long; set a shorter TMPDIR", host.Address)
	}
	if p.hostLn, err = launch.Listen(context.Background(), host); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	if o.TCP {
		host.Address = p.hostLn.Addr().String()
	}
	p.addr, p.hostAddr = p.dial.String(), host.String()
	return p, nil
}

func freePort() (int, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer func() { _ = ln.Close() }()
	return ln.Addr().(*net.TCPAddr).Port, nil
}

// env is the launch contract's variables for the connector.
func (p *proc) env() []string {
	env := []string{sdk.EnvConnectorAddr + "=" + p.addr, sdk.EnvHostAddr + "=" + p.hostAddr}
	if p.token != "" {
		env = append(env, sdk.EnvToken+"="+p.token)
	}
	if p.opts.Role != "" {
		env = append(env, sdk.EnvRole+"="+p.opts.Role)
	}
	return env
}

// readyLine is closed once r's first line is "ready", and drains the rest.
func readyLine(r io.Reader) chan struct{} {
	ready := make(chan struct{})
	go func() {
		sc := bufio.NewScanner(r)
		if sc.Scan() && sc.Text() == "ready" {
			close(ready)
		}
		_, _ = io.Copy(io.Discard, r)
	}()
	return ready
}

// awaitReady waits for the connector to say it's ready, and closes p if it
// doesn't.
func (p *proc) awaitReady(ctx context.Context, name string, ready, exited chan struct{}) error {
	var err error
	select {
	case <-ready:
		return nil
	case <-exited:
		err = fmt.Errorf("%s exited before it was ready: %s", name, p.Output())
	case <-ctx.Done():
		err = ctx.Err()
	case <-time.After(readyTimeout):
		err = fmt.Errorf("%s wasn't ready after %s: %s", name, readyTimeout, p.Output())
	}
	_ = p.Close()
	return err
}

func (p *proc) connect() error {
	if p.conn != nil {
		return nil
	}
	conn, err := grpc.NewClient(p.dial.Target(), append(launch.DialOptions(p.token),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(sdk.MaxMessageSize), grpc.MaxCallSendMsgSize(sdk.MaxMessageSize)))...)
	if err != nil {
		return err
	}
	p.conn = conn
	return nil
}

// bind connects, reads the spec, moves config's secrets into host services
// and starts serving them.
func (p *proc) bind(ctx context.Context, config []byte) error {
	if err := p.connect(); err != nil {
		return err
	}
	spec, err := protocolv1.NewConnectorServiceClient(p.conn).Spec(ctx, &protocolv1.SpecRequest{Host: Host})
	if err != nil {
		return fmt.Errorf("spec: %w", rpcerr.FromStatus(err))
	}
	cfg, secrets, err := splitSecrets(spec.GetConfigSchema(), config)
	if err != nil {
		return err
	}
	p.config = cfg
	p.hs.setSecrets(secrets)
	var hs protocolv1.HostServiceServer = p.hs
	if p.opts.HostServices != nil {
		if hs, err = p.opts.HostServices(spec, secrets); err != nil {
			return err
		}
	}
	p.host = grpc.NewServer(append(launch.ServerOptions(p.token), grpc.MaxRecvMsgSize(sdk.MaxMessageSize), grpc.MaxSendMsgSize(sdk.MaxMessageSize))...)
	protocolv1.RegisterHostServiceServer(p.host, hs)
	go func() { _ = p.host.Serve(p.hostLn) }()
	return nil
}

func (p *proc) Conn() grpc.ClientConnInterface { return p.conn }
func (p *proc) Config() []byte                 { return p.config }
func (p *proc) Output() string                 { return p.out.String() }

func (p *proc) Close() error {
	p.once.Do(func() {
		if p.conn != nil {
			_ = p.conn.Close()
		}
		if p.stop != nil {
			p.stop()
		}
		if p.host != nil {
			p.host.Stop()
		}
		p.cleanup()
	})
	return nil
}

func (p *proc) cleanup() {
	_ = p.hostLn.Close()
	_ = os.RemoveAll(p.dir)
}

// splitSecrets replaces each property the schema marks x-secret, at any
// depth, with its dotted path, and returns the values by path, as the runner
// does.
func splitSecrets(schema, config []byte) ([]byte, map[string]string, error) {
	secrets := map[string]string{}
	if len(schema) == 0 || len(bytes.TrimSpace(config)) == 0 {
		return config, secrets, nil
	}
	paths, err := configschema.SecretPaths(schema)
	if err != nil {
		return nil, nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, nil, fmt.Errorf("the config isn't a JSON object: %w", err)
	}
	for _, k := range paths {
		v, ok := configschema.Lookup(m, k)
		if !ok {
			continue
		}
		value, ok := v.(string)
		if !ok {
			return nil, nil, fmt.Errorf("config.%s is a secret, so a string", k)
		}
		secrets[k] = value
		configschema.Replace(m, k, k)
	}
	out, err := json.Marshal(m)
	return out, secrets, err
}

type output struct {
	mu  sync.Mutex
	b   bytes.Buffer
	tee io.Writer
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.b.Len() < maxOutput {
		o.b.Write(p[:min(len(p), maxOutput-o.b.Len())])
	}
	if o.tee != nil {
		_, _ = o.tee.Write(p)
	}
	return len(p), nil
}

func (o *output) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.String()
}

// hostServer is the kit's host services: enough for connectors that reach
// their upstream themselves.
type hostServer struct {
	protocolv1.UnimplementedHostServiceServer
	out     *output
	mu      sync.Mutex
	secrets map[string]string
	kv      map[string]kvEntry
	staging *stagingArea
}

type kvEntry struct {
	value   []byte
	expires time.Time
}

func (h *hostServer) setSecrets(s map[string]string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.secrets = s
}

func (h *hostServer) GetSecret(_ context.Context, req *protocolv1.GetSecretRequest) (*protocolv1.GetSecretResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	v, ok := h.secrets[req.GetName()]
	if !ok {
		return nil, rpcerr.Status(sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, fmt.Errorf("no secret %q", req.GetName())))
	}
	return &protocolv1.GetSecretResponse{Value: []byte(v)}, nil
}

func (h *hostServer) HTTP(*protocolv1.HTTPRequest, grpc.ServerStreamingServer[protocolv1.HTTPResponse]) error {
	return status.Error(codes.Unimplemented, "the conformance kit's host makes no HTTP requests; test HTTP connectors with openetl connector test")
}

func (h *hostServer) Log(_ context.Context, req *protocolv1.LogRequest) (*protocolv1.LogResponse, error) {
	for _, e := range req.GetEntries() {
		fmt.Fprintf(h.out, "%s %s %v\n", e.GetLevel(), e.GetMessage(), e.GetAttrs())
	}
	return &protocolv1.LogResponse{}, nil
}

func (h *hostServer) KVGet(_ context.Context, req *protocolv1.KVGetRequest) (*protocolv1.KVGetResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.kv[req.GetKey()]
	if !ok || (!e.expires.IsZero() && time.Now().After(e.expires)) {
		return &protocolv1.KVGetResponse{}, nil
	}
	return &protocolv1.KVGetResponse{Value: e.value}, nil
}

func (h *hostServer) KVPut(_ context.Context, req *protocolv1.KVPutRequest) (*protocolv1.KVPutResponse, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e := kvEntry{value: req.GetValue()}
	if req.GetTtl() != nil {
		e.expires = time.Now().Add(req.GetTtl().AsDuration())
	}
	h.kv[req.GetKey()] = e
	return &protocolv1.KVPutResponse{}, nil
}

func (h *hostServer) Now(context.Context, *protocolv1.NowRequest) (*protocolv1.NowResponse, error) {
	return &protocolv1.NowResponse{Now: timestamppb.Now()}, nil
}

// newID is a random UUID, for pipelines.
func newID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
