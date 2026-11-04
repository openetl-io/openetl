package spark

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"regexp"
	"strings"

	"github.com/unsuitable001/openetl/internal/platform/errs"
)

// submit runs spark-submit. Spark 3.5's lines are what it parses, which is
// one reason only Spark 3.5 is supported.
type submit struct {
	bin, master string
	log         *slog.Logger
}

// run runs spark-submit with args, logs each line it prints, and returns
// them. A spark-submit that can't start is CONFIG_INVALID.
func (s *submit) run(ctx context.Context, args ...string) ([]string, error) {
	cmd := exec.CommandContext(ctx, s.bin, args...) //nolint:gosec // the operator's spark-submit, with arguments from the pipeline's spec
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: install Spark 3.5 on the runner, or point --spark-submit at its spark-submit")
	}
	var lines []string
	sc := bufio.NewScanner(out)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		line := sc.Text()
		lines = append(lines, line)
		s.log.Info(line, "component", "spark-submit")
	}
	err = cmd.Wait()
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		err = fmt.Errorf("spark-submit exited with %d: %s", exit.ExitCode(), lastLines(lines, 3))
	}
	return lines, err
}

// failed is a spark-submit that failed, TRANSIENT unless it couldn't start.
func failed(err error, msg string) error {
	if errs.CodeOf(err) == errs.ConfigInvalid {
		return err
	}
	return errs.Wrap(errs.Transient, err, msg)
}

func find(lines []string, re *regexp.Regexp) []string {
	for _, l := range lines {
		if m := re.FindStringSubmatch(l); m != nil {
			return m
		}
	}
	return nil
}

func lastLines(lines []string, n int) string {
	return strings.Join(lines[max(0, len(lines)-n):], " | ")
}

// standalone submits to a standalone master, whose drivers can be found only
// by the ID it assigns.
type standalone struct{ *submit }

var (
	standaloneSubmitted = regexp.MustCompile(`Driver successfully submitted as (driver-\S+)`)
	standaloneState     = regexp.MustCompile(`State of (?:driver )?(driver-\S+) is ([A-Z_]+)`)
	standaloneException = regexp.MustCompile(`Exception from cluster was: (.*)`)
	standaloneUnknown   = regexp.MustCompile(`did not recognize (driver-\S+)`)
)

func (s *standalone) Kind() string { return "standalone" }

func (s *standalone) Submit(ctx context.Context, app Application) (string, error) {
	lines, err := s.run(ctx, submitArgs(s.master, app, nil)...)
	if m := find(lines, standaloneSubmitted); m != nil {
		return m[1], nil
	}
	if err == nil {
		err = errors.New("spark-submit didn't say which driver it submitted: " + lastLines(lines, 3))
	}
	return "", failed(err, "spark: submit to "+s.master)
}

func (s *standalone) Find(context.Context, string) (string, error) { return "", nil }

func (s *standalone) Status(ctx context.Context, id string) (State, error) {
	lines, err := s.run(ctx, "--master", s.master, "--status", id)
	if find(lines, standaloneUnknown) != nil {
		return State{Done: true, Failed: true, State: "UNKNOWN", Message: "the master no longer knows driver " + id + ", so whether it ran is unknown"}, nil
	}
	m := find(lines, standaloneState)
	if m == nil {
		if err == nil {
			err = errors.New("spark-submit didn't say the driver's state: " + lastLines(lines, 3))
		}
		return State{}, failed(err, "spark: driver "+id)
	}
	st := State{State: m[2]}
	switch st.State {
	case "FINISHED":
		st.Done = true
	case "FAILED", "ERROR", "KILLED":
		st.Done, st.Failed = true, true
		st.Message = "driver " + id + " ended " + st.State
		if e := find(lines, standaloneException); e != nil {
			st.Message += ": " + e[1]
		}
	}
	return st, nil
}

func (s *standalone) Kill(ctx context.Context, id string) error {
	_, err := s.run(ctx, "--master", s.master, "--kill", id)
	return err
}

// kubernetes names each driver pod for its submission, so a second
// submission fails as a duplicate.
type kubernetes struct {
	*submit
	namespace string
}

var (
	podExists   = regexp.MustCompile(`(?i)already ?exists`)
	podPhase    = regexp.MustCompile(`phase: (\w+)`)
	podExitCode = regexp.MustCompile(`exit code: (-?\d+)`)
	podReason   = regexp.MustCompile(`termination reason: (\S+)`)
)

func (k *kubernetes) Kind() string { return "kubernetes" }

func (k *kubernetes) id(name string) string { return k.namespace + ":" + name }

func (k *kubernetes) Submit(ctx context.Context, app Application) (string, error) {
	lines, err := k.run(ctx, submitArgs(k.master, app, map[string]string{
		"spark.kubernetes.driver.pod.name":              app.Name,
		"spark.kubernetes.submission.waitAppCompletion": "false",
	})...)
	if err != nil && find(lines, podExists) == nil {
		return "", failed(err, "spark: submit to "+k.master)
	}
	return k.id(app.Name), nil
}

func (k *kubernetes) Find(ctx context.Context, name string) (string, error) {
	st, err := k.Status(ctx, k.id(name))
	if err != nil || st.State == "" {
		return "", err
	}
	return k.id(name), nil
}

func (k *kubernetes) Status(ctx context.Context, id string) (State, error) {
	lines, err := k.run(ctx, "--master", k.master, "--status", id)
	m := find(lines, podPhase)
	if m == nil {
		if err != nil {
			return State{}, failed(err, "spark: driver pod "+id)
		}
		return State{}, nil
	}
	st := State{State: m[1]}
	switch st.State {
	case "Succeeded":
		st.Done = true
	case "Failed":
		st.Done, st.Failed = true, true
		st.Message = "driver pod " + id + " failed"
		if c := find(lines, podExitCode); c != nil {
			st.Message += " with exit code " + c[1]
		}
		if r := find(lines, podReason); r != nil {
			st.Message += " (" + r[1] + ")"
		}
	}
	return st, nil
}

func (k *kubernetes) Kill(ctx context.Context, id string) error {
	_, err := k.run(ctx, "--master", k.master, "--kill", id)
	return err
}
