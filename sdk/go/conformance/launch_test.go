package conformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/conformance"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// readLaunch reads the both connector's one record.
func readLaunch(t *testing.T, p conformance.Process) map[string]string {
	t.Helper()
	stream, err := protocolv1.NewSourceServiceClient(p.Conn()).Read(t.Context(), &protocolv1.ReadRequest{
		Config: p.Config(), Streams: []*protocolv1.ConfiguredStream{{Name: "launch", SyncMode: protocolv1.SyncMode_SYNC_MODE_FULL_REFRESH}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var rec map[string]string
	for {
		m, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return rec
		}
		if err != nil {
			t.Fatalf("read: %v\n%s", err, p.Output())
		}
		if rows := m.GetBatch().GetRows().GetRows(); len(rows) > 0 {
			if err := json.Unmarshal(rows[0], &rec); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func kind(t *testing.T, p conformance.Process) protocolv1.ConnectorKind {
	t.Helper()
	s, err := protocolv1.NewConnectorServiceClient(p.Conn()).Spec(t.Context(), &protocolv1.SpecRequest{})
	if err != nil {
		t.Fatal(err)
	}
	return s.GetKind()
}

type kv struct {
	protocolv1.UnimplementedHostServiceServer
	secrets map[string]string
}

func (h *kv) KVGet(context.Context, *protocolv1.KVGetRequest) (*protocolv1.KVGetResponse, error) {
	return &protocolv1.KVGetResponse{Value: []byte("custom")}, nil
}

// An executable with both roles serves the one it's given, over Unix
// sockets or TCP, and the platform's host services can stand in for the
// kit's.
func TestLaunchContract(t *testing.T) {
	exe := conformance.Build(t, "./testdata/both")
	for _, tcp := range []bool{false, true} {
		h := &kv{}
		p, err := conformance.ExecWith(exe, conformance.Options{Role: "source", TCP: tcp,
			HostServices: func(_ *protocolv1.SpecResponse, secrets map[string]string) (protocolv1.HostServiceServer, error) {
				h.secrets = secrets
				return h, nil
			},
		})(t.Context(), []byte(`{"token": "s3cr3t"}`))
		if err != nil {
			t.Fatal(err)
		}
		rec := readLaunch(t, p)
		if want := map[bool]string{false: "unix://", true: "tcp://127.0.0.1:"}[tcp]; !strings.HasPrefix(rec["addr"], want) || rec["k"] != "custom" {
			t.Errorf("tcp %v: the connector saw %v", tcp, rec)
		}
		if h.secrets["token"] != "s3cr3t" || strings.Contains(string(p.Config()), "s3cr3t") {
			t.Errorf("tcp %v: secrets %v, config %s", tcp, h.secrets, p.Config())
		}
		_ = p.Close()
	}
	p, err := conformance.ExecWith(exe, conformance.Options{Role: "destination"})(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if k := kind(t, p); k != protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
		t.Errorf("as a destination, its kind is %s", k)
	}
	_ = p.Close()
	if _, err := conformance.Exec(exe)(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "both roles") {
		t.Errorf("without a role: %v", err)
	}
}

// Attach waits for a connector started by hand with the environment it
// announces, and leaves it running.
func TestAttach(t *testing.T) {
	exe := conformance.Build(t, "./testdata/both")
	for _, addr := range []string{"tcp://127.0.0.1:" + freePort(t), "unix://" + filepath.Join(shortTemp(t), "c.sock")} {
		var cmd *exec.Cmd
		started := make(chan error, 1)
		p, err := conformance.Attach(addr, conformance.Options{Role: "source"}, func(env []string) {
			cmd = exec.CommandContext(t.Context(), exe)
			cmd.Env = append(os.Environ(), env...)
			go func() {
				time.Sleep(300 * time.Millisecond)
				started <- cmd.Start()
			}()
		})(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := <-started; err != nil {
			t.Fatal(err)
		}
		if rec := readLaunch(t, p); rec["addr"] != addr {
			t.Errorf("the connector saw %v", rec)
		}
		_ = p.Close()
		if cmd.ProcessState != nil {
			t.Error("closing the process stopped the connector")
		}
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}

	// A connector started with another launch's token is refused.
	addr := "tcp://127.0.0.1:" + freePort(t)
	cmd := exec.CommandContext(t.Context(), exe)
	cmd.Env = append(os.Environ(), "OPENETL_CONNECTOR_ADDR="+addr, "OPENETL_HOST_ADDR=tcp://127.0.0.1:1", "OPENETL_TOKEN=stale", "OPENETL_CONNECTOR_ROLE=source")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	if _, err := conformance.Attach(addr, conformance.Options{}, func([]string) {})(t.Context(), nil); err == nil || !strings.Contains(err.Error(), "token") {
		t.Errorf("a stale token: %v", err)
	}
}

// Container runs an image under the image contract. It needs Docker or
// Podman, and OPENETL_TEST_CONTAINER=1.
func TestContainer(t *testing.T) {
	if os.Getenv("OPENETL_TEST_CONTAINER") != "1" {
		t.Skip("set OPENETL_TEST_CONTAINER=1 to build and run an image")
	}
	engine, err := conformance.ContainerEngine()
	if err != nil {
		t.Fatal(err)
	}
	image := conformance.BuildImage(t, engine, "./testdata/both")
	for _, role := range []string{"source", "destination"} {
		p, err := conformance.Container(engine, image, nil, conformance.Resources{Memory: "64Mi", CPU: "500m"}, conformance.Options{Role: role})(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		if role == "source" {
			rec := readLaunch(t, p)
			if want := map[bool]string{true: "unix:///run/openetl/", false: "tcp://0.0.0.0:"}[runtime.GOOS == "linux"]; !strings.HasPrefix(rec["addr"], want) {
				t.Errorf("the connector saw %v", rec)
			}
		} else if k := kind(t, p); k != protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION {
			t.Errorf("as a destination, its kind is %s", k)
		}
		_ = p.Close()
	}
}

func freePort(t *testing.T) string {
	t.Helper()
	var lc net.ListenConfig
	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	_, port, _ := strings.Cut(ln.Addr().String(), ":")
	return port
}

// shortTemp is a directory whose socket paths fit in sun_path.
func shortTemp(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "oc-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}
