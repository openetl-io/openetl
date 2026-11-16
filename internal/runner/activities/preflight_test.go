package activities

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	dataplanev1 "github.com/openetl-io/openetl/gen/openetl/dataplane/v1"
	"github.com/openetl-io/openetl/internal/connectorhost"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/internal/runner"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/destination"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// tableName names as the engine does: the vectors are the engine's own
// tests', read from its source.
func TestTableName(t *testing.T) {
	b, err := os.ReadFile("../../../crates/normalize/src/naming.rs")
	if err != nil {
		t.Fatal(err)
	}
	test := func(name string) string {
		_, body, ok := strings.Cut(string(b), "fn "+name+"() {")
		if !ok {
			t.Fatalf("naming.rs has no test %s", name)
		}
		body, _, _ = strings.Cut(body, "\n    }\n")
		return body
	}
	str := `("(?:[^"\\]|\\.)*")`
	unquote := func(s string) string {
		u, err := strconv.Unquote(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		return u
	}
	n := 0
	for _, m := range regexp.MustCompile(`\(`+str+`, `+str+`\)`).FindAllStringSubmatch(test("normalizes_names"), -1) {
		if got, want := tableName(unquote(m[1]), false), unquote(m[2]); got != want {
			t.Errorf("tableName(%s) = %q, want %q", m[1], got, want)
		}
		n++
	}
	for _, m := range regexp.MustCompile(`n\.base\(`+str+`\), `+str+`\)`).FindAllStringSubmatch(test("keeps_source_names_when_asked"), -1) {
		if got, want := tableName(unquote(m[1]), true), unquote(m[2]); got != want {
			t.Errorf("tableName(%s, kept) = %q, want %q", m[1], got, want)
		}
		n++
	}
	if n < 10 {
		t.Fatalf("read %d vectors from naming.rs", n)
	}
	// Rust lowercases İ to two characters.
	for in, want := range map[string]string{"public.Orders": "public_orders", "İd": "i_d", "2fa": "_2fa", "\u212Aelvin": "kelvin"} {
		if got := tableName(in, false); got != want {
			t.Errorf("tableName(%q) = %q, want %q", in, got, want)
		}
	}
}

// interval follows the scheduler, ends for a cron that never fires, and
// finds the longest gap of schedules that vary.
func TestInterval(t *testing.T) {
	berlin, err := time.LoadLocation("Europe/Berlin")
	if err != nil {
		t.Skip(err)
	}
	// The week Berlin leaves summer time, at 2026-10-25 03:00.
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, berlin)
	day := 24 * time.Hour
	cron := func(c string, jitter time.Duration) *apiv1.Schedule {
		return &apiv1.Schedule{Trigger: &apiv1.Schedule_Cron{Cron: c}, Jitter: durationpb.New(jitter)}
	}
	for _, c := range []struct {
		name string
		s    *apiv1.Schedule
		want time.Duration
	}{
		{"every", &apiv1.Schedule{Trigger: &apiv1.Schedule_Every{Every: durationpb.New(time.Hour)}, Jitter: durationpb.New(time.Minute)}, time.Hour + time.Minute},
		{"daily, in UTC", cron("0 6 * * *", 0), day},
		{"daily, in Berlin", cron("CRON_TZ=Europe/Berlin 0 6 * * *", 0), 25 * time.Hour},
		{"daily, with jitter", cron("0 6 * * *", 30*time.Minute), day + 30*time.Minute},
		{"weekdays", cron("0 6 * * 1-5", 0), 3 * day},
		{"weekdays, with jitter", cron("0 6 * * 1-5", 30*time.Minute), 3*day + 30*time.Minute},
		{"each minute of weekdays", cron("* * * * 1-5", 0), 2*day + time.Minute},
		{"monthly", cron("0 0 1 * *", 0), 31 * day},
		{"on the 31st", cron("0 0 31 * *", 0), 61 * day},
		{"quarterly", cron("0 0 1 */3 *", 0), 92 * day},
		// More than 10,000 firings before the first long gap.
		{"each minute of a month's first week", cron("* * 1-7 * *", 0), 24*day + time.Minute},
		{"each minute of a month's first week, in Berlin", cron("CRON_TZ=Europe/Berlin * * 1-7 * *", 0), 24*day + time.Hour + time.Minute},
		{"each minute of January and July", cron("* * * 1,7 *", 0), 153*day + time.Minute},
		{"each minute", cron("* * * * *", 0), time.Minute},
		{"every 90 minutes", cron("@every 90m", time.Minute), 91 * time.Minute},
		{"never", cron("0 0 31 2 *", time.Minute), 0},
		{"unparsable", cron("every day", 0), 0},
		{"none", nil, 0},
	} {
		got := make(chan time.Duration, 1)
		go func() { got <- interval(c.s, now) }()
		select {
		case d := <-got:
			if d != c.want {
				t.Errorf("%s: %v, want %v", c.name, d, c.want)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("%s: interval didn't return", c.name)
		}
	}
}

// longestGap finds what stepping through each firing finds, across changes
// of UTC offset: Berlin's at 2:00, Santiago's at midnight and Lord Howe's by
// half an hour.
func TestLongestGap(t *testing.T) {
	stepped := func(s cron.Schedule, first time.Time) time.Duration {
		var longest time.Duration
		for f, end := first, first.Add(cronHorizon); !f.After(end); {
			n := s.Next(f)
			if n.IsZero() {
				break
			}
			longest, f = max(longest, n.Sub(f)), n
		}
		return longest
	}
	// Few firings each: stepping through them is slow under the race
	// detector. A year from mid-day holds each zone's changes.
	crons := []string{"*/30 0-2 * * *", "*/20 2 * * 0", "0 0,12 * * 1", "0,59 23 * * *", "0 * 1,31 * *", "0 0 1,15 * *", "0 */6 * 2 *", "0 0 29 2 *"}
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	for _, zone := range []string{"Europe/Berlin", "America/Santiago", "Australia/Lord_Howe"} {
		if _, err := time.LoadLocation(zone); err != nil {
			t.Skip(err)
		}
		for _, c := range crons {
			c = "CRON_TZ=" + zone + " " + c
			s, err := cron.ParseStandard(c)
			if err != nil {
				t.Fatal(err)
			}
			first := s.Next(now)
			if got, want := longestGap(s, first), stepped(s, first); got != want {
				t.Errorf("%s: %v, want %v", c, got, want)
			}
		}
	}
}

func TestDestinationPreflight(t *testing.T) {
	job := runner.Job{Namespace: "cdc", Streams: []runner.Stream{
		{Name: "public.Orders", Mode: destination.Append, PrimaryKey: []string{"id"}},
		{Name: "users", Table: "people", Mode: destination.Merge},
	}}
	req := destinationPreflight("p1", job)
	if req.Pipeline != "p1" || req.Namespace != "cdc" || len(req.Streams) != 2 || req.Streams[1].Mode != destination.Merge {
		t.Fatalf("request %+v", req)
	}
	for stream, want := range map[string]string{"public.Orders": "public_orders", "users": "people"} {
		if got, _ := req.Schemas[stream].Metadata().GetValue(tableKey); got != want {
			t.Errorf("%s's table %q, want %q", stream, got, want)
		}
	}
}

type remotePreflight struct{}

func (remotePreflight) PreflightReport(_ context.Context, req source.PreflightRequest) (check.Report, error) {
	return check.FromError(errors.New("remote " + req.Pipeline)), nil
}

type builtInPreflight struct{}

func (builtInPreflight) Preflight(req destination.PreflightRequest) []check.Check {
	if req.Pipeline == "panics" {
		panic("listing")
	}
	return []check.Check{{Name: "stream.a", Title: "Write a", Run: func(context.Context, *check.Probe) error { return nil }}}
}

// An external connector's preflight report is its own; a built-in one's
// checks run here, a panic listing them failing the report; and a connector
// without a preflight passes.
func TestRunPreflight(t *testing.T) {
	if r, err := runPreflight(t.Context(), remotePreflight{}, source.PreflightRequest{Pipeline: "p"}); err != nil || r.Err() == nil || r.Err().Error() != "remote p" {
		t.Errorf("an external connector's: %v, %v", r.Err(), err)
	}
	if r, _ := runPreflight(t.Context(), builtInPreflight{}, destination.PreflightRequest{Pipeline: "p"}); !r.OK() || len(r.Items) != 1 || r.Items[0].Status != check.Passed {
		t.Errorf("a built-in connector's: %+v, %v", r.Items, r.Err())
	}
	if r, _ := runPreflight(t.Context(), builtInPreflight{}, destination.PreflightRequest{Pipeline: "panics"}); r.OK() || errs.CodeOf(r.Err()) != errs.Internal {
		t.Errorf("a panic listing a built-in connector's: %v", r.Err())
	}
	if r, _ := runPreflight(t.Context(), builtInPreflight{}, source.PreflightRequest{}); !r.OK() || len(r.Items) != 0 {
		t.Errorf("a destination's for a source's request: %v", r.Err())
	}
}

// parts is a preflight's parts, each as passed, failed with its code and
// its items' count, or skipped with why.
func parts(gs []contract.CheckGroup) []string {
	var out []string
	for _, g := range gs {
		s := g.Role + " " + g.Stage + ": "
		switch {
		case g.Skipped != "":
			s += "skipped, " + g.Skipped
		case g.Code != "":
			s += fmt.Sprintf("%s, %d items", g.Code, len(g.Items))
		default:
			s += "passed"
		}
		out = append(out, s)
	}
	return out
}

// Each part runs only after the parts it needs; one whose connector didn't
// start or answer fails, and a part is skipped only after one failed.
func TestPreflightParts(t *testing.T) {
	up := func(context.Context) error { return nil }
	down := func(context.Context) error { return errs.New(errs.Internal, "connector: exited before it was ready") }
	pass := func(context.Context) (check.Report, error) {
		return check.FromProto(&protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED}}}), nil
	}
	fail := func(context.Context) (check.Report, error) { return check.Of(context.Background(), builtIn{}), nil }
	dead := func(context.Context) (check.Report, error) {
		return check.Report{}, errs.New(errs.Internal, "connector: exited unexpectedly")
	}
	settled := func(context.Context) error { return nil }
	unsettled := func(context.Context) error { return errs.New(errs.ConfigInvalid, "the source has no stream \"s\"") }
	all := func() preflight {
		return preflight{startSource: up, startDestination: up, checkSource: pass, checkDestination: pass, settle: settled,
			preflightSource: pass, preflightDestination: pass, checkTime: time.Minute, preflightTime: time.Minute}
	}
	for _, c := range []struct {
		name   string
		change func(*preflight)
		want   []string
	}{
		{"all pass", func(*preflight) {}, []string{"source check: passed", "destination check: passed", "sync plan: passed", "source preflight: passed", "destination preflight: passed"}},
		{"the source doesn't start", func(p *preflight) { p.startSource = down }, []string{"source check: INTERNAL, 0 items", "destination check: passed",
			"sync plan: skipped, the source didn't start", "source preflight: skipped, the source didn't start", "destination preflight: skipped, a sync can't be planned"}},
		{"the source's check fails", func(p *preflight) { p.checkSource = fail }, []string{"source check: AUTH_INVALID, 1 items", "destination check: passed",
			"sync plan: skipped, the source's check failed", "source preflight: skipped, the source's check failed", "destination preflight: skipped, a sync can't be planned"}},
		{"the source's check doesn't answer", func(p *preflight) { p.checkSource = dead }, []string{"source check: INTERNAL, 0 items", "destination check: passed",
			"sync plan: skipped, the source's check failed", "source preflight: skipped, the source's check failed", "destination preflight: skipped, a sync can't be planned"}},
		{"the destination doesn't start", func(p *preflight) { p.startDestination = down }, []string{"source check: passed", "destination check: INTERNAL, 0 items",
			"sync plan: skipped, the destination didn't start", "source preflight: skipped, a sync can't be planned", "destination preflight: skipped, the destination didn't start"}},
		{"the destination's check fails", func(p *preflight) { p.checkDestination = fail }, []string{"source check: passed", "destination check: AUTH_INVALID, 1 items",
			"sync plan: passed", "source preflight: passed", "destination preflight: skipped, the destination's check failed"}},
		{"a sync can't be planned", func(p *preflight) { p.settle = unsettled }, []string{"source check: passed", "destination check: passed",
			"sync plan: CONFIG_INVALID, 0 items", "source preflight: skipped, a sync can't be planned", "destination preflight: skipped, a sync can't be planned"}},
		{"the source's preflight doesn't answer", func(p *preflight) { p.preflightSource = dead }, []string{"source check: passed", "destination check: passed",
			"sync plan: passed", "source preflight: INTERNAL, 0 items", "destination preflight: passed"}},
	} {
		p := all()
		c.change(&p)
		gs, err := p.run(t.Context(), "s1", "d1")
		if got := parts(gs); err != nil || !slices.Equal(got, c.want) {
			t.Errorf("%s: %v\n got %q\nwant %q", c.name, err, got, c.want)
		}
		if gs[0].Version != "s1" || gs[1].Version != "d1" || gs[3].Version != "s1" || gs[4].Version != "d1" {
			t.Errorf("%s: versions %+v", c.name, gs)
		}
	}

	var calls []string
	p := all()
	call := func(name string) func(context.Context) error {
		return func(context.Context) error { calls = append(calls, name); return nil }
	}
	report := func(name string) func(context.Context) (check.Report, error) {
		return func(ctx context.Context) (check.Report, error) { return check.Report{}, call(name)(ctx) }
	}
	p.startSource, p.checkSource, p.startDestination, p.checkDestination = call("start source"), report("check source"), call("start destination"), report("check destination")
	p.settle, p.preflightSource, p.preflightDestination = call("settle"), report("preflight source"), report("preflight destination")
	if _, err := p.run(t.Context(), "", ""); err != nil || !slices.Equal(calls, []string{"start source", "check source", "start destination", "check destination", "settle", "preflight source", "preflight destination"}) {
		t.Errorf("ran %q, %v", calls, err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := all().run(ctx, "", ""); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled op: %v", err)
	}
}

// A part that runs out of time fails saying how long it had, keeping the
// items it has, and the parts that don't need it still run.
func TestPreflightPartTime(t *testing.T) {
	hang := func(ctx context.Context) (check.Report, error) {
		<-ctx.Done()
		return check.Report{}, errs.Wrap(errs.Internal, ctx.Err(), "")
	}
	slow := func(ctx context.Context) (check.Report, error) {
		return check.RunList(ctx, func() []check.Check {
			return []check.Check{{Name: "slow", Title: "Slow", Run: func(ctx context.Context, _ *check.Probe) error { <-ctx.Done(); return ctx.Err() }}}
		}), nil
	}
	pass := func(context.Context) (check.Report, error) { return check.Report{}, nil }
	up := func(context.Context) error { return nil }
	p := preflight{startSource: up, startDestination: up, checkSource: hang, checkDestination: pass, settle: func(context.Context) error { return nil },
		preflightSource: pass, preflightDestination: pass, checkTime: time.Second, preflightTime: time.Second}
	gs, err := p.run(t.Context(), "", "")
	if err != nil || gs[0].Code != "TRANSIENT" || gs[0].Message != "the source's check didn't finish within 1s" || gs[1].Code != "" || gs[2].Skipped == "" {
		t.Errorf("a source's check that hangs: %v, %q, %+v", err, parts(gs), gs[0])
	}

	p.checkSource, p.preflightDestination = pass, slow
	gs, err = p.run(t.Context(), "", "")
	if err != nil || gs[3].Code != "" || gs[4].Message != "the destination's preflight didn't finish within 1s" || len(gs[4].Items) != 1 {
		t.Errorf("a destination's preflight that runs out of time: %v, %q, %+v", err, parts(gs), gs[4])
	}

	// With less time than the SDK's slack, what a part answers in time or why
	// its connector failed is kept.
	p.preflightDestination = pass
	p.checkSource = func(context.Context) (check.Report, error) { return check.Of(context.Background(), builtIn{}), nil }
	p.checkDestination = func(context.Context) (check.Report, error) {
		return check.Report{}, errs.New(errs.Internal, "connector: exited unexpectedly")
	}
	gs, err = p.run(t.Context(), "", "")
	if err != nil || gs[0].Code != "AUTH_INVALID" || len(gs[0].Items) != 1 || gs[1].Code != "INTERNAL" || gs[1].Message != "connector: exited unexpectedly" {
		t.Errorf("parts that fail in time: %v, %q, %+v, %+v", err, parts(gs), gs[0], gs[1])
	}

	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	if _, cancel, had := within(ctx, 2*time.Minute); had > time.Minute-closeMargin || had < time.Minute-closeMargin-time.Second {
		t.Errorf("within a minute, a part has %v", had)
	} else {
		cancel()
	}
}

// A preflight checks the connectors before it settles a sync, which a
// failed check stops, and a destination that doesn't start fails its part.
func TestPreflightPipeline(t *testing.T) {
	dir := t.TempDir()
	testConnector(t, dir, "source-locked", "locked")
	if err := os.WriteFile(filepath.Join(dir, "destination-broken"), []byte("#!/bin/sh\nexit 3\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "discovered")
	cfg, err := structpb.NewStruct(map[string]any{"marker": marker})
	if err != nil {
		t.Fatal(err)
	}
	spec := &dataplanev1.GetSyncSpecResponse{
		OrgId: "org", PipelineId: "p", Version: "v1", Pipeline: &apiv1.PipelineSpec{},
		Source: &apiv1.ConnectorConfig{Connector: "locked", Config: cfg}, Destination: &apiv1.ConnectorConfig{Connector: "stdout", Config: &structpb.Struct{}},
	}
	a := &Ops{Connectors: connectorhost.Env{Connectors: dir}, API: opsAPI{spec: spec}}
	in := contract.ConnectorOpInput{OpID: "op", OrgID: "org", PipelineID: "p", Version: "v1"}
	res, err := a.PreflightPipeline(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"source check: AUTH_INVALID, 1 items", "destination check: passed", "sync plan: skipped, the source's check failed",
		"source preflight: skipped, the source's check failed", "destination preflight: skipped, a sync can't be planned"}
	if got := parts(res.Preflight); !slices.Equal(got, want) {
		t.Errorf("parts\n %q\nwant\n %q", got, want)
	}
	if it := res.Preflight[0].Items; len(it) != 1 || it[0].Fix != "Unlock the account." {
		t.Errorf("the source's check: %+v", it)
	}
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Errorf("the source discovered after its check failed: %v", err)
	}

	spec.Destination.Connector = "broken"
	if res, err = a.PreflightPipeline(t.Context(), in); err != nil {
		t.Fatal(err)
	}
	want[1], want[4] = "destination check: INTERNAL, 0 items", "destination preflight: skipped, the destination didn't start"
	if got := parts(res.Preflight); !slices.Equal(got, want) || res.Preflight[1].Message == "" {
		t.Errorf("parts\n %q\nwant\n %q\n%+v", got, want, res.Preflight[1])
	}
}
