package dbt

import (
	"bufio"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"
)

// Commands a step can run.
var Commands = []string{"build", "run", "test"}

// Invocation is one dbt command on a bundle.
type Invocation struct {
	// Empty: dbt on PATH.
	Executable string
	// build, run or test.
	Command string
	// dbt's --select and --exclude: space-separated selectors.
	Select, Exclude string
	Target          *Target
	// dbt's output goes here, a line at a time.
	Logger *slog.Logger
}

// Result is what dbt reported in run_results.json.
type Result struct {
	// Nodes by status, such as success, error, skipped, pass and fail.
	Nodes   map[string]int
	Elapsed time.Duration
}

// Failure is a dbt command that ran and failed.
type Failure struct {
	Command string
	Exit    int
	// The nodes that failed, with dbt's messages. Empty if dbt stopped
	// before it ran any, as on a compilation or connection error.
	Failed []string
	// The end of dbt's output, for when nothing failed by node.
	Output []string
}

func (f *Failure) Error() string {
	detail := f.Failed
	if len(detail) > 3 {
		detail = append(detail[:3:3], fmt.Sprintf("and %d more", len(f.Failed)-3))
	}
	if len(detail) == 0 {
		detail = f.Output
	}
	return fmt.Sprintf("dbt %s exited with %d: %s", f.Command, f.Exit, strings.Join(detail, "; "))
}

// Passed to dbt from the runner's environment; nothing else is, as for
// connectors.
var passEnv = []string{"PATH", "HOME", "TMPDIR", "TZ", "LANG", "SSL_CERT_FILE", "SSL_CERT_DIR"}

const (
	outputTail = 10
	// How long dbt gets to stop after an interrupt.
	stopGrace = 30 * time.Second
)

// ErrStart is dbt not starting, as when there's no executable.
var ErrStart = errors.New("dbt didn't start")

// Exec unpacks bundle under work and runs one dbt command on it. A failed
// command returns its result with a *Failure.
func Exec(ctx context.Context, bundle []byte, work string, inv Invocation) (*Result, error) {
	dir := filepath.Join(work, "project")
	project, err := Unpack(bundle, dir)
	if err != nil {
		return nil, fmt.Errorf("unpack the project: %w", err)
	}
	profiles := filepath.Join(work, "profiles")
	conf, err := Profiles(project.Profile, inv.Target)
	if err != nil {
		return nil, err
	}
	if err := os.Mkdir(profiles, 0o700); err != nil {
		return nil, err
	}
	if err := os.WriteFile(filepath.Join(profiles, "profiles.yml"), conf, 0o600); err != nil {
		return nil, err
	}

	args := []string{inv.Command, "--profiles-dir", profiles}
	if sel := strings.Fields(inv.Select); len(sel) > 0 {
		args = append(append(args, "--select"), sel...)
	}
	if exc := strings.Fields(inv.Exclude); len(exc) > 0 {
		args = append(append(args, "--exclude"), exc...)
	}
	cmd := exec.CommandContext(ctx, cmp.Or(inv.Executable, "dbt"), args...) //nolint:gosec // the operator names dbt; the step's selectors are arguments, not a shell
	cmd.Dir = dir
	for _, k := range passEnv {
		if v, ok := os.LookupEnv(k); ok {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, inv.Target.Env...)
	cmd.Env = append(cmd.Env, "OPENETL_NAMESPACE="+inv.Target.Namespace, "DBT_SEND_ANONYMOUS_USAGE_STATS=false", "DBT_USE_COLORS=false")
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGINT) }
	cmd.WaitDelay = stopGrace
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrStart, err)
	}
	tail := logLines(out, inv.Logger)
	err = cmd.Wait()

	res := &Result{Nodes: map[string]int{}}
	failed, rerr := readResults(filepath.Join(dir, "target", "run_results.json"), res)
	var exit *exec.ExitError
	switch {
	case ctx.Err() != nil:
		return res, ctx.Err()
	case errors.As(err, &exit):
		return res, &Failure{Command: inv.Command, Exit: exit.ExitCode(), Failed: failed, Output: tail}
	case err != nil:
		return res, err
	case rerr != nil:
		return res, fmt.Errorf("dbt %s succeeded, but its results: %w", inv.Command, rerr)
	}
	return res, nil
}

var timestamp = regexp.MustCompile(`^\d\d:\d\d:\d\d\s+`)

// logLines logs each line of r that isn't empty, without dbt's timestamp,
// until r ends. It returns the last few.
func logLines(r io.Reader, log *slog.Logger) []string {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	var tail []string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := strings.TrimRight(timestamp.ReplaceAllString(sc.Text(), ""), " \r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		log.Info(line)
		tail = append(tail, strings.TrimSpace(line))
		if len(tail) > outputTail {
			tail = tail[1:]
		}
	}
	// A line longer than the buffer ends the scan; drain the rest so dbt
	// doesn't block on a full pipe.
	_, _ = io.Copy(io.Discard, r)
	return tail
}

// readResults counts run_results.json's nodes into res, and returns the
// failed ones with their messages.
func readResults(path string, res *Result) ([]string, error) {
	b, err := os.ReadFile(path) //nolint:gosec // in the project dbt ran
	if err != nil {
		return nil, err
	}
	var rr struct {
		Results []struct {
			UniqueID string  `json:"unique_id"`
			Status   string  `json:"status"`
			Message  *string `json:"message"`
		} `json:"results"`
		Elapsed float64 `json:"elapsed_time"`
	}
	if err := json.Unmarshal(b, &rr); err != nil {
		return nil, err
	}
	res.Elapsed = time.Duration(rr.Elapsed * float64(time.Second))
	var failed []string
	for _, r := range rr.Results {
		res.Nodes[r.Status]++
		if r.Status == "error" || r.Status == "fail" || r.Status == "runtime error" {
			msg := r.Status
			if r.Message != nil {
				msg = strings.Join(strings.Fields(*r.Message), " ")
			}
			if r := []rune(msg); len(r) > 300 {
				msg = string(r[:300]) + "…"
			}
			failed = append(failed, r.UniqueID+": "+msg)
		}
	}
	return failed, nil
}
