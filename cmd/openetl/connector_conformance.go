package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/openetl-io/openetl/sdk/go/conformance"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

const testUsage = `
Runs the conformance kit's checks against TARGET, a source or a destination,
the way the platform drives a connector, and reports each: PASS, FAIL or SKIP,
with what a failed or skipped check found, and the flags a passing one raised.
It fails if any check fails. A descriptor's or reference's binary must return
the spec the descriptor says, field for field, before any check runs.

--config is the config the checks use. --bad-credentials and --unreachable add
the errors check's configs: credentials the upstream rejects, and an upstream
that doesn't answer. A source's checks read every stream, or the --stream ones;
--add-records-command lets the incremental check add records upstream. A
destination's checks that write need --read-command to see what it made
visible, and skip without it.

The commands run with sh -c, with what they act on in the environment:
  --add-records-command   OPENETL_STREAM; prints how many records it added.
  --read-command          OPENETL_NAMESPACE and OPENETL_STREAM; prints the
                          stream's records as JSON objects, one a line, and
                          nothing for a stream the destination hasn't created.
  --drop-command          OPENETL_NAMESPACE; removes what the checks wrote.
`

var syncModes = map[string]source.SyncMode{
	"full_refresh": source.FullRefresh, "incremental": source.Incremental, "cdc": source.CDC, "stream": source.Stream,
}

func runConnectorTest(ctx context.Context, args []string, out io.Writer) error {
	f := newRunFlags("test", "TARGET")
	usage := f.fs.Usage
	f.fs.Usage = func() {
		usage()
		fmt.Fprint(f.fs.Output(), testUsage)
	}
	badCredentials := f.fs.String("bad-credentials", "", "a config with credentials the upstream rejects, for the errors check")
	unreachable := f.fs.String("unreachable", "", "a config whose upstream doesn't answer, such as a closed port, for the errors check")
	var streams files
	f.fs.Var(&streams, "stream", "a source's stream to read, NAME[:MODE[:CURSOR]], MODE full_refresh, incremental, cdc or stream; repeatable (default: every stream, each read the way the source reads it first)")
	resumePoints := f.fs.Int("resume-points", 0, "how many of a stream's resume points the resume check restarts from (default 5)")
	readTimeout := f.fs.Duration("read-timeout", 0, "how long one read may take (default 2m)")
	addRecords := f.fs.String("add-records-command", "", "a command that adds records upstream to $OPENETL_STREAM and prints how many")
	readCmd := f.fs.String("read-command", "", "a command that prints the records a destination made visible in $OPENETL_NAMESPACE for $OPENETL_STREAM, as JSON lines")
	dropCmd := f.fs.String("drop-command", "", "a command that removes what the checks wrote in $OPENETL_NAMESPACE")
	settle := f.fs.Duration("settle", 0, "how long after a commit a destination's readers can take to see it, as Kafka's consumers can")
	untyped := f.fs.Bool("untyped", false, "the destination keeps no column types, as a topic of JSON records doesn't")
	var lossy files
	f.fs.Var(&lossy, "lossy-type", "TYPE=WHAT: a logical type the destination's mapping table doesn't keep exactly, such as interval, and what the table says of it; the types check leaves it out; repeatable")
	runPattern := f.fs.String("run", "", "run only the checks whose names match this regular expression")
	asJSON := f.fs.Bool("json", false, "print a JSON object a check, with its check, result (pass, fail or skip) and log")
	if err := f.parse(args); err != nil {
		return err
	}
	var match func(string) bool
	if *runPattern != "" {
		re, err := regexp.Compile(*runPattern)
		if err != nil {
			return fmt.Errorf("--run: %w", err)
		}
		match = re.MatchString
	}
	config, err := f.readConfig()
	if err != nil {
		return err
	}
	bad, err := f.optionalConfig("bad-credentials", *badCredentials)
	if err != nil {
		return err
	}
	down, err := f.optionalConfig("unreachable", *unreachable)
	if err != nil {
		return err
	}
	t, err := f.target(ctx, "")
	if err != nil {
		return err
	}
	defer t.close()
	p, s, err := t.start(ctx, config)
	if err != nil {
		return err
	}
	var run func(context.Context, func(string) bool, func(conformance.Result))
	switch s.GetKind() {
	case protocolv1.ConnectorKind_CONNECTOR_KIND_SOURCE:
		if *readCmd != "" || *dropCmd != "" || *settle != 0 || *untyped || len(lossy) > 0 {
			_ = p.Close()
			return errors.New("--read-command, --drop-command, --settle, --untyped and --lossy-type are for destinations")
		}
		suite := conformance.SourceSuite{Launch: t.launch, Config: config, BadCredentials: bad, Unreachable: down,
			ResumePoints: *resumePoints, ReadTimeout: *readTimeout}
		suite.Streams, err = testStreams(ctx, p, streams)
		if err != nil {
			_ = p.Close()
			return err
		}
		if *addRecords != "" {
			suite.AddRecords = func(ctx context.Context, stream string) (int, error) {
				b, err := shell(ctx, *addRecords, "OPENETL_STREAM="+stream)
				if err != nil {
					return 0, err
				}
				n, err := strconv.Atoi(strings.TrimSpace(string(b)))
				if err != nil {
					return 0, fmt.Errorf("--add-records-command printed %q, not a number of records", b)
				}
				return n, nil
			}
		}
		run = suite.RunMatching
	case protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION:
		if len(streams) > 0 || *addRecords != "" {
			_ = p.Close()
			return errors.New("--stream and --add-records-command are for sources")
		}
		suite := conformance.DestinationSuite{Launch: t.launch, Config: config, BadCredentials: bad, Unreachable: down,
			Settle: *settle, Untyped: *untyped}
		for _, l := range lossy {
			name, what, ok := strings.Cut(l, "=")
			if !ok || name == "" {
				_ = p.Close()
				return fmt.Errorf("--lossy-type %s: want TYPE=WHAT", l)
			}
			if suite.LossyTypes == nil {
				suite.LossyTypes = map[string]string{}
			}
			suite.LossyTypes[name] = what
		}
		if *readCmd != "" {
			suite.Read = func(ctx context.Context, namespace, stream string) ([]map[string]any, error) {
				b, err := shell(ctx, *readCmd, "OPENETL_NAMESPACE="+namespace, "OPENETL_STREAM="+stream)
				if err != nil {
					return nil, err
				}
				return jsonRecords(b)
			}
		}
		if *dropCmd != "" {
			suite.Drop = func(ctx context.Context, namespace string) error {
				_, err := shell(ctx, *dropCmd, "OPENETL_NAMESPACE="+namespace)
				return err
			}
		}
		run = suite.RunMatching
	default:
		_ = p.Close()
		return fmt.Errorf("%s is a %s, which the kit doesn't check", t.name, s.GetKind())
	}
	_ = p.Close()

	rep := &testReport{out: out, json: *asJSON}
	if s.GetKind() == protocolv1.ConnectorKind_CONNECTOR_KIND_DESTINATION && *readCmd == "" {
		rep.note("No --read-command, so the checks that write skip: they can't see what %s made visible.", t.name)
	}
	run(ctx, match, rep.result)
	return rep.end(t.name, *runPattern)
}

// optionalConfig reads a config flag's file, or nil when the flag isn't set.
func (f *runFlags) optionalConfig(flag, path string) ([]byte, error) {
	if path == "" {
		return nil, nil
	}
	return f.readConfigFile(flag, path)
}

// testStreams reads --stream's NAME[:MODE[:CURSOR]], with a stream's first
// mode from p's catalog where MODE is left out.
func testStreams(ctx context.Context, p conformance.Process, given []string) ([]conformance.Stream, error) {
	var out []conformance.Stream
	var cat map[string]*protocolv1.CatalogStream
	for _, g := range given {
		parts := strings.SplitN(g, ":", 3)
		st := conformance.Stream{Name: parts[0]}
		if len(parts) > 2 {
			st.Cursor = parts[2]
		}
		if len(parts) > 1 && parts[1] != "" {
			m, ok := syncModes[parts[1]]
			if !ok {
				return nil, fmt.Errorf("--stream %s: mode %s isn't full_refresh, incremental, cdc or stream", g, parts[1])
			}
			st.Mode = m
			out = append(out, st)
			continue
		}
		if cat == nil {
			resp, err := protocolv1.NewSourceServiceClient(p.Conn()).Discover(ctx, &protocolv1.DiscoverRequest{Config: p.Config()})
			if err != nil {
				return nil, fmt.Errorf("discover, for --stream %s's mode: %w", g, err)
			}
			cat = map[string]*protocolv1.CatalogStream{}
			for _, s := range resp.GetCatalog().GetStreams() {
				cat[s.GetName()] = s
			}
		}
		st.Mode = source.FullRefresh
		if s := cat[st.Name]; s != nil && len(s.GetSyncModes()) > 0 {
			st.Mode = source.SyncMode(s.GetSyncModes()[0])
		}
		out = append(out, st)
	}
	return out, nil
}

// shell runs a command with sh -c, with env added to the environment, and
// returns what it printed. The command goes through the environment too, so
// the argv is fixed.
func shell(ctx context.Context, command string, env ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "sh", "-c", `eval "$OPENETL_COMMAND"`)
	cmd.Env = append(append(os.Environ(), "OPENETL_COMMAND="+command), env...)
	var errs bytes.Buffer
	cmd.Stderr = &errs
	b, err := cmd.Output()
	if err != nil {
		if msg := strings.TrimSpace(errs.String()); msg != "" {
			err = fmt.Errorf("%w: %s", err, msg)
		}
		return nil, fmt.Errorf("%s: %w", command, err)
	}
	return b, nil
}

// jsonRecords reads JSON objects, one a line; numbers keep their text.
func jsonRecords(b []byte) ([]map[string]any, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var out []map[string]any
	for {
		var rec map[string]any
		err := dec.Decode(&rec)
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return nil, fmt.Errorf("--read-command's records: %w", err)
		}
		if rec == nil {
			return nil, errors.New("--read-command printed null, not a record")
		}
		out = append(out, rec)
	}
}

// testReport prints each check's result as it ends.
type testReport struct {
	out                     io.Writer
	json                    bool
	passed, failed, skipped int
}

func (r *testReport) note(format string, args ...any) {
	w := r.out
	if r.json {
		w = stderr
	}
	fmt.Fprintf(w, format+"\n", args...)
}

func (r *testReport) result(res conformance.Result) {
	result := "pass"
	switch {
	case res.Failed:
		result = "fail"
		r.failed++
	case res.Skipped:
		result = "skip"
		r.skipped++
	default:
		r.passed++
	}
	if r.json {
		b, _ := json.Marshal(struct {
			Check  string   `json:"check"`
			Result string   `json:"result"`
			Log    []string `json:"log"`
		}{res.Check, result, res.Log})
		fmt.Fprintf(r.out, "%s\n", b)
		return
	}
	fmt.Fprintf(r.out, "%s %s\n", strings.ToUpper(result), res.Check)
	for _, l := range res.Log {
		if result != "pass" || strings.HasPrefix(l, "flag:") {
			fmt.Fprintf(r.out, "    %s\n", strings.ReplaceAll(l, "\n", "\n    "))
		}
	}
}

func (r *testReport) end(name, pattern string) error {
	total := r.passed + r.failed + r.skipped
	if total == 0 {
		return fmt.Errorf("no check matches --run %s", pattern)
	}
	r.note("%s: %d checks, %d passed, %d failed, %d skipped", name, total, r.passed, r.failed, r.skipped)
	if r.failed > 0 {
		return fmt.Errorf("%d of %d checks failed", r.failed, total)
	}
	return nil
}
