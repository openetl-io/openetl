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
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdk "github.com/unsuitable001/openetl/sdk/go"
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

// Exec starts the executable at exe as the runner does: with only the
// runner's environment and env's KEY=value pairs, and with the kit's host
// services, which resolve secrets and keep logs but make no HTTP requests.
func Exec(exe string, env ...string) Launcher {
	return func(ctx context.Context, config []byte) (Process, error) {
		p, err := newProc()
		if err != nil {
			return nil, err
		}
		cmd := exec.Command(exe) //nolint:noctx // Close stops it
		for _, k := range passEnv {
			if v, ok := os.LookupEnv(k); ok {
				cmd.Env = append(cmd.Env, k+"="+v)
			}
		}
		cmd.Env = append(cmd.Env, env...)
		cmd.Env = append(cmd.Env, sdk.EnvSocket+"="+p.socket, sdk.EnvHostSocket+"="+p.hostSocket, sdk.EnvWatchStdin+"=1")
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
		exited := make(chan struct{})
		ready := make(chan struct{})
		go func() {
			sc := bufio.NewScanner(stdout)
			if sc.Scan() && sc.Text() == "ready" {
				close(ready)
			}
			_, _ = io.Copy(io.Discard, stdout)
		}()
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
		select {
		case <-ready:
		case <-exited:
			_ = p.Close()
			return nil, fmt.Errorf("%s exited before it was ready: %s", filepath.Base(exe), p.Output())
		case <-ctx.Done():
			_ = p.Close()
			return nil, ctx.Err()
		case <-time.After(readyTimeout):
			_ = p.Close()
			return nil, fmt.Errorf("%s wasn't ready after %s: %s", filepath.Base(exe), readyTimeout, p.Output())
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
		p, err := newProc()
		if err != nil {
			return nil, err
		}
		cctx, cancel := context.WithCancel(context.Background())
		ready := make(chan struct{})
		done := make(chan struct{})
		var runErr error
		go func() {
			defer close(done)
			runErr = c.Run(cctx, p.socket, p.hostSocket, func() { close(ready) })
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

// proc is what both launchers share: the sockets, the connection and the
// kit's host services.
type proc struct {
	dir                string
	socket, hostSocket string
	hostLn             net.Listener
	host               *grpc.Server
	hs                 *hostServer
	conn               *grpc.ClientConn
	config             []byte
	out                *output
	stop               func()
	once               sync.Once
}

func newProc() (*proc, error) {
	dir, err := os.MkdirTemp("", "oc-")
	if err != nil {
		return nil, err
	}
	p := &proc{dir: dir, socket: filepath.Join(dir, "c.sock"), hostSocket: filepath.Join(dir, "h.sock"), out: &output{}}
	if len(p.hostSocket) > maxSocketPath {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("socket path %s is too long; set a shorter TMPDIR", p.hostSocket)
	}
	var lc net.ListenConfig
	if p.hostLn, err = lc.Listen(context.Background(), "unix", p.hostSocket); err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	p.hs = &hostServer{out: p.out, kv: map[string]kvEntry{}}
	p.host = grpc.NewServer(grpc.MaxRecvMsgSize(sdk.MaxMessageSize), grpc.MaxSendMsgSize(sdk.MaxMessageSize))
	protocolv1.RegisterHostServiceServer(p.host, p.hs)
	go func() { _ = p.host.Serve(p.hostLn) }()
	return p, nil
}

// bind connects, reads the spec and moves config's secrets into host
// services.
func (p *proc) bind(ctx context.Context, config []byte) error {
	conn, err := grpc.NewClient("unix://"+p.socket,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(sdk.MaxMessageSize), grpc.MaxCallSendMsgSize(sdk.MaxMessageSize)))
	if err != nil {
		return err
	}
	p.conn = conn
	spec, err := protocolv1.NewConnectorServiceClient(conn).Spec(ctx, &protocolv1.SpecRequest{Host: Host})
	if err != nil {
		return fmt.Errorf("spec: %w", rpcerr.FromStatus(err))
	}
	cfg, secrets, err := splitSecrets(spec.GetConfigSchema(), config)
	if err != nil {
		return err
	}
	p.config = cfg
	p.hs.setSecrets(secrets)
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
		p.host.Stop()
		p.cleanup()
	})
	return nil
}

func (p *proc) cleanup() {
	_ = p.hostLn.Close()
	_ = os.RemoveAll(p.dir)
}

// splitSecrets replaces each top-level property the schema marks x-secret
// with its name, and returns the values by name, as the runner does.
func splitSecrets(schema, config []byte) ([]byte, map[string]string, error) {
	secrets := map[string]string{}
	if len(schema) == 0 || len(bytes.TrimSpace(config)) == 0 {
		return config, secrets, nil
	}
	var s struct {
		Properties map[string]struct {
			Secret bool `json:"x-secret"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil, nil, fmt.Errorf("config schema: %w", err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(config, &m); err != nil {
		return nil, nil, fmt.Errorf("the config isn't a JSON object: %w", err)
	}
	for k, prop := range s.Properties {
		v, ok := m[k]
		if !prop.Secret || !ok {
			continue
		}
		var value string
		if err := json.Unmarshal(v, &value); err != nil {
			return nil, nil, fmt.Errorf("config.%s is a secret, so a string", k)
		}
		secrets[k] = value
		m[k], _ = json.Marshal(k)
	}
	out, err := json.Marshal(m)
	return out, secrets, err
}

type output struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.b.Len() < maxOutput {
		o.b.Write(p[:min(len(p), maxOutput-o.b.Len())])
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
