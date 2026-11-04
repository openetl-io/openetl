package conformance

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/unsuitable001/openetl/sdk/go/internal/launch"
)

// EnvContainerEngine names the container engine Container uses when one
// isn't given: docker or podman.
const EnvContainerEngine = "OPENETL_CONTAINER_ENGINE"

// ContainerEngine is EnvContainerEngine's engine, or else docker or podman,
// whichever is on PATH.
func ContainerEngine() (string, error) {
	if e := os.Getenv(EnvContainerEngine); e != "" {
		return e, nil
	}
	for _, e := range []string{"docker", "podman"} {
		if _, err := exec.LookPath(e); err == nil {
			return e, nil
		}
	}
	return "", fmt.Errorf("neither docker nor podman is on PATH; set %s", EnvContainerEngine)
}

// Resources are a container's limits, as Kubernetes quantities, as a
// descriptor's resources are.
type Resources struct {
	CPU, Memory string
	// The size of TMPDIR; empty: 1Gi.
	Disk string
}

// The port a connector listens on in its container, when the kit reaches
// it over TCP.
const containerPort = 7700

// Container runs a connector's image with engine, docker or podman, under
// the image contract: its entrypoint, or command when set, as a non-root
// user with a read-only root filesystem and a TMPDIR in memory, given only
// the launch contract's variables and TZ and LANG. On Linux the addresses
// are Unix sockets in a directory mounted into the container; elsewhere, or
// with o.TCP, they're TCP with a token. The image is pulled first if it
// isn't there, which the ready timeout doesn't count.
func Container(engine, image string, command []string, res Resources, o Options) Launcher {
	return func(ctx context.Context, config []byte) (Process, error) {
		if err := exec.CommandContext(ctx, engine, "image", "inspect", image).Run(); err != nil { //nolint:gosec // the caller names the engine and image
			if out, err := exec.CommandContext(ctx, engine, "pull", image).CombinedOutput(); err != nil { //nolint:gosec // as above
				return nil, fmt.Errorf("%s pull %s: %w: %s", engine, image, err, strings.TrimSpace(string(out)))
			}
		}
		o.TCP = o.TCP || runtime.GOOS != "linux"
		p, err := newProc(o)
		if err != nil {
			return nil, err
		}
		name := "openetl-conformance-" + launch.NewToken()[:12]
		args := []string{"run", "--rm", "--name", name, "--label", "openetl.io/launcher=conformance",
			"--read-only", "--tmpfs", "/tmp:rw,size=" + dockerBytes(cmp.Or(res.Disk, "1Gi")),
			"--cap-drop", "ALL", "--security-opt", "no-new-privileges", "-e", "TMPDIR=/tmp"}
		args = append(args, limits(res)...)
		for _, k := range []string{"TZ", "LANG"} {
			if _, ok := os.LookupEnv(k); ok {
				args = append(args, "-e", k)
			}
		}
		// Values go through the engine's environment, so ps doesn't show
		// the token.
		env := os.Environ()
		uid, gid := os.Getuid(), os.Getgid()
		switch {
		case !o.TCP:
			if uid == 0 {
				uid, gid = 65532, 65532
				_ = os.Chmod(p.dir, 0o777) //nolint:gosec // the container's user isn't ours
				_ = os.Chmod(p.hostAddr[len("unix://"):], 0o666)
			}
			if engine == "podman" {
				args = append(args, "--userns=keep-id")
			}
			args = append(args, "-v", p.dir+":/run/openetl")
			p.addr, p.hostAddr = "unix:///run/openetl/c.sock", "unix:///run/openetl/h.sock"
		case runtime.GOOS == "linux":
			uid, gid = 65532, 65532
			args = append(args, "--network", "host")
		default:
			uid, gid = 65532, 65532
			_, port, _ := net.SplitHostPort(p.hostAddr[len("tcp://"):])
			p.addr, p.hostAddr = "tcp://0.0.0.0:"+strconv.Itoa(containerPort), "tcp://host.docker.internal:"+port
			args = append(args, "-p", "127.0.0.1::"+strconv.Itoa(containerPort))
		}
		args = append(args, "--user", fmt.Sprintf("%d:%d", uid, gid))
		for _, kv := range append(append([]string{}, o.Env...), p.env()...) {
			k, _, _ := strings.Cut(kv, "=")
			args = append(args, "-e", k)
			env = append(env, kv)
		}
		if len(command) > 0 {
			args = append(args, "--entrypoint", command[0], image)
			args = append(args, command[1:]...)
		} else {
			args = append(args, image)
		}
		cmd := exec.Command(engine, args...) //nolint:gosec,noctx // the caller names the engine and image; Close stops it
		cmd.Env = env
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			p.cleanup()
			return nil, err
		}
		cmd.Stderr = p.out
		if err := cmd.Start(); err != nil {
			p.cleanup()
			return nil, fmt.Errorf("start %s: %w", engine, err)
		}
		exited, ready := make(chan struct{}), readyLine(stdout)
		go func() {
			_ = cmd.Wait()
			close(exited)
		}()
		p.stop = func() {
			sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			_ = exec.CommandContext(sctx, engine, "stop", "--time", "5", name).Run() //nolint:gosec // as above
			select {
			case <-exited:
			case <-time.After(10 * time.Second):
				_ = exec.CommandContext(sctx, engine, "rm", "--force", name).Run() //nolint:gosec // as above
				_ = cmd.Process.Kill()
				<-exited
			}
		}
		if err := p.awaitReady(ctx, image, ready, exited); err != nil {
			return nil, err
		}
		if o.TCP && runtime.GOOS != "linux" {
			out, err := exec.CommandContext(ctx, engine, "port", name, strconv.Itoa(containerPort)+"/tcp").Output() //nolint:gosec // as above
			hostPort := ""
			if err == nil {
				hostPort, _, _ = strings.Cut(strings.TrimSpace(string(out)), "\n")
			}
			a, perr := launch.Parse("tcp://" + strings.TrimSpace(hostPort))
			if perr != nil {
				_ = p.Close()
				return nil, errors.Join(fmt.Errorf("the container's published port: %w", perr), err)
			}
			p.dial = a
		}
		if err := p.bind(ctx, config); err != nil {
			_ = p.Close()
			return nil, err
		}
		return p, nil
	}
}

// BuildImage builds the main package pkg for Linux on this machine's
// architecture into an image with nothing else in it, for Container, and
// removes the image when the test ends.
func BuildImage(t T, engine, pkg string) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "conformance-")
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	cmd := exec.CommandContext(t.Context(), "go", "build", "-trimpath", "-o", filepath.Join(dir, "connector"), pkg) //nolint:gosec // the test names the package
	cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH="+runtime.GOARCH, "CGO_ENABLED=0")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s: %v\n%s", pkg, err, out)
	}
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), []byte("FROM scratch\nCOPY connector /connector\nENTRYPOINT [\"/connector\"]\n"), 0o600); err != nil {
		t.Fatalf("%v", err)
	}
	image := "localhost/openetl-conformance:" + launch.NewToken()[:12]
	if out, err := exec.CommandContext(t.Context(), engine, "build", "-q", "-t", image, dir).CombinedOutput(); err != nil { //nolint:gosec // the caller names the engine
		t.Fatalf("%s build: %v\n%s", engine, err, out)
	}
	t.Cleanup(func() { _ = exec.Command(engine, "rmi", "--force", image).Run() }) //nolint:gosec,noctx // as above; the test's context has ended
	return image
}

// limits are a container engine's flags for res.
func limits(res Resources) []string {
	var args []string
	if res.Memory != "" {
		args = append(args, "--memory", dockerBytes(res.Memory))
	}
	if c := res.CPU; c != "" {
		if m, ok := strings.CutSuffix(c, "m"); ok {
			n, _ := strconv.Atoi(m)
			c = strconv.FormatFloat(float64(n)/1000, 'f', -1, 64)
		}
		args = append(args, "--cpus", c)
	}
	return args
}

var quantity = regexp.MustCompile(`^([0-9]+)(Ki|Mi|Gi|Ti|k|M|G|T)?$`)

// dockerBytes is a Kubernetes size in bytes, which every engine reads.
func dockerBytes(q string) string {
	m := quantity.FindStringSubmatch(q)
	if m == nil {
		return q
	}
	n, _ := strconv.ParseInt(m[1], 10, 64)
	mult := map[string]int64{"": 1, "k": 1e3, "M": 1e6, "G": 1e9, "T": 1e12, "Ki": 1 << 10, "Mi": 1 << 20, "Gi": 1 << 30, "Ti": 1 << 40}[m[2]]
	return strconv.FormatInt(n*mult, 10)
}
