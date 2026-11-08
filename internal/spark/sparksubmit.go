package spark

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os/exec"
	"regexp"
	"strings"

	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/platform/logging"
	"github.com/openetl-io/openetl/internal/platform/resources"
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
	cmd := exec.CommandContext(ctx, s.bin, args...)
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = cmd.Stdout
	if err := cmd.Start(); err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: install Spark 3.5 on the runner, or point --spark-submit at its spark-submit")
	}
	resources.KillFirst(cmd.Process.Pid)
	var lines []string
	sc := logging.Lines(out)
	for sc.Scan() {
		line := sc.Text()
		lines = append(lines, line)
		s.log.Info(line, "component", "spark-submit")
	}
	if err := sc.Err(); err != nil {
		s.log.Warn("reading spark-submit's output failed", "err", err)
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

// standalone submits to a standalone master with spark-submit, then watches
// and kills drivers through the master's web UI: spark-submit's --status
// and --kill need its REST server, which is off by default. Drivers can be
// found only by the ID the master assigns.
type standalone struct {
	*submit
	ui   string
	http *http.Client
}

var standaloneSubmitted = regexp.MustCompile(`Driver successfully submitted as (driver-\S+)`)

func (s *standalone) Kind() string { return "standalone" }

func (s *standalone) Submit(ctx context.Context, app Application) (string, error) {
	lines, err := s.run(ctx, submitArgs(s.master, app, nil)...)
	if m := find(lines, standaloneSubmitted); m != nil {
		return m[1], nil
	}
	if err == nil {
		// A standalone driver can be found only by this ID, so a retry would
		// submit a second one.
		return "", errs.New(errs.ConfigInvalid, "spark: spark-submit to %s exited without saying which driver it submitted, so the first driver may still be running untracked: look for it in the master's UI, and set spark-submit's client log level to INFO, where it says: %s", s.master, lastLines(lines, 3))
	}
	return "", failed(err, "spark: submit to "+s.master)
}

func (s *standalone) Find(context.Context, string) (string, error) { return "", nil }

type standaloneDriver struct {
	ID    string `json:"id"`
	State string `json:"state"`
}

func (s *standalone) Status(ctx context.Context, id string) (State, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.ui+"/json/", nil)
	if err != nil {
		return State{}, err
	}
	var master struct {
		Active    []standaloneDriver `json:"activedrivers"`
		Completed []standaloneDriver `json:"completeddrivers"`
	}
	if err := s.call(req, &master); err != nil {
		return State{}, err
	}
	st := State{URL: s.ui}
	for _, d := range append(master.Active, master.Completed...) {
		if d.ID == id {
			st.State = d.State
		}
	}
	switch st.State {
	case "":
		return State{Done: true, Failed: true, State: "UNKNOWN", URL: s.ui, Message: "the master no longer knows driver " + id + ", so whether it ran is unknown"}, nil
	case "FINISHED":
		st.Done = true
	case "FAILED", "ERROR", "KILLED":
		st.Done, st.Failed = true, true
		st.Message = "driver " + id + " ended " + st.State + "; its log is on the worker that ran it, in the master's UI"
	}
	return st, nil
}

// Kill asks the master's UI to kill the driver, as its kill link does.
func (s *standalone) Kill(ctx context.Context, id string) error {
	form := url.Values{"id": {id}, "terminate": {"true"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.ui+"/driver/kill/", strings.NewReader(form.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return s.call(req, nil)
}

func (s *standalone) call(req *http.Request, out any) error {
	res, err := s.http.Do(req)
	if err != nil {
		return errs.Wrap(errs.Transient, err, "spark: the master's UI at "+s.ui)
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(res.Body, 16<<20))
	switch {
	case err != nil:
		return errs.Wrap(errs.Transient, err, "spark: the master's UI at "+s.ui)
	case res.StatusCode/100 != 2:
		return errs.New(errs.Transient, "spark: the master's UI at %s: %s %s: %s", s.ui, req.Method, req.URL.Path, res.Status)
	case out == nil:
		return nil
	}
	return json.Unmarshal(b, out)
}

// standaloneUI is the UI of a master spark://host:port: port 8080 on its
// host, unless the cluster names another.
func standaloneUI(master, ui string) string {
	if ui != "" {
		return strings.TrimSuffix(ui, "/")
	}
	host := strings.TrimPrefix(master, "spark://")
	// A master URL can list several masters, the first of which is tried
	// first.
	host, _, _ = strings.Cut(host, ",")
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	return "http://" + net.JoinHostPort(host, "8080")
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
