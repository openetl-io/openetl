package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"unicode"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
	"github.com/openetl-io/openetl/sdk/go/check"
	"github.com/openetl-io/openetl/sdk/go/check/netcheck"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// runCLI runs the CLI and returns what it printed, and reported on stderr.
func runCLI(t *testing.T, args ...string) (string, string, error) {
	t.Helper()
	var out, errs bytes.Buffer
	old := stderr
	stderr = &errs
	defer func() { stderr = old }()
	err := run(t.Context(), args, &out)
	return out.String(), errs.String(), err
}

// databaseCheck is a database's check as the SDK reports it, with the SDK's
// own titles and reasons, carried to the API as the runner carries it:
// templates and their params, unfilled.
func databaseCheck(t *testing.T) *apiv1.CheckResult {
	t.Helper()
	const address = "db.internal:5432"
	reach := netcheck.Dial(address)
	reach.Run = func(_ context.Context, p *check.Probe) error {
		p.Param("version", "16.2")
		p.Found("PostgreSQL {version}")
		return nil
	}
	agree := netcheck.TLS(address, nil)
	agree.Run = func(_ context.Context, p *check.Probe) error {
		p.Param("tls_version", "TLS 1.3")
		p.Param("expires", "2026-11-28")
		return check.Warn(netcheck.CertificateExpiring.Fail())
	}
	schema := check.Params{"schema": "public"}
	r := check.Run(t.Context(), []check.Check{reach, agree,
		{Name: "schema.usage", Kind: check.Permission, Title: "Use schema {schema}", Field: "schema", Params: schema, Run: func(context.Context, *check.Probe) error {
			return check.PrivilegeMissing.Wrap(errors.New("ERROR: permission denied for schema public (SQLSTATE 42501)\nHINT: ask the schema's owner"),
				"principal", "etl", "privilege", "USAGE", "object", "schema public")
		}},
		{Name: "tables.read", Kind: check.Permission, Title: "Read every table in {schema}", Params: schema, Needs: []string{"schema.usage"},
			Run: func(context.Context, *check.Probe) error { return nil }},
	})
	res := &apiv1.CheckResult{Version: "0123456789abcdef", Ok: r.OK()}
	ps, _ := r.Proto()
	for _, p := range ps {
		it := &apiv1.CheckItem{Name: p.GetName(), Title: p.GetTitle(), Status: check.Status(p.GetStatus()).String(), Message: p.GetMessage(),
			Fix: p.GetFix(), Field: p.GetField(), Description: p.GetDescription(), Reason: p.GetReason(),
			Kind: check.Kind(p.GetKind()).String(), Params: p.GetParams(), Detail: p.GetDetail()}
		if e := p.GetError(); e != nil {
			it.Code = errs.Code(e.GetCode()).String()
		}
		res.Items = append(res.Items, it)
	}
	if err := r.Err(); err != nil {
		res.Code, res.Message = errs.CodeOf(err).String(), errs.ToProto(err).GetMessage()
	}
	return res
}

// One line an item, its templates filled in; a failed or warning item's
// message, detail, fix, field, and code and reason under it; a passed
// item's message, and a skipped one's reason.
func TestChecklist(t *testing.T) {
	f := &fakeChecks{destination: databaseCheck(t)}
	url := checksServer(t, f)
	out, _, err := runCLI(t, "check", "--api-url", url, "destination", "lake")
	want := "workspaces/default/destinations/lake failed its check (version 0123456789ab)\n" +
		"  passed   Reach db.internal:5432\n" +
		"           PostgreSQL 16.2\n" +
		"  warning  Agree on TLS with db.internal:5432\n" +
		"           The certificate of db.internal:5432 expires on 2026-11-28.\n" +
		"           fix: Renew the certificate.\n" +
		"           code: TRANSIENT (certificate_expiring)\n" +
		"  failed   Use schema public\n" +
		"           etl has no USAGE on schema public.\n" +
		"           detail: ERROR: permission denied for schema public (SQLSTATE 42501)\n" +
		"                   HINT: ask the schema's owner\n" +
		"           fix: Grant USAGE on schema public to etl.\n" +
		"           field: schema\n" +
		"           code: PERMISSION_DENIED (privilege_missing)\n" +
		"  skipped  Read every table in public\n" +
		"           needs schema.usage, which failed\n"
	if out != want {
		t.Errorf("printed\n%s\nwant\n%s", out, want)
	}
	if err == nil || err.Error() != "workspaces/default/destinations/lake failed its check: PERMISSION_DENIED: Use schema public: etl has no USAGE on schema public: "+
		"ERROR: permission denied for schema public (SQLSTATE 42501) HINT: ask the schema's owner" {
		t.Errorf("error: %v", err)
	}
	t.Logf("a check that fails:\n%s\nopenetl: %v", out, err)

	// A check that passes with a warning.
	f.destination = databaseCheck(t)
	f.destination.Ok, f.destination.Code, f.destination.Message, f.destination.Items = true, "", "", f.destination.Items[:2]
	out, _, err = runCLI(t, "check", "--api-url", url, "destination", "lake")
	if err != nil || !strings.HasPrefix(out, "workspaces/default/destinations/lake passed its check (version 0123456789ab)\n  passed   Reach db.internal:5432\n") {
		t.Errorf("a warning: %v\n%s", err, out)
	}
	t.Logf("a check that passes with a warning:\n%s", out)

	// What failed besides the items, such as running out of time, is on
	// the check's line, not hidden behind items that passed.
	f.destination = &apiv1.CheckResult{Code: "TRANSIENT", Message: "the check ran out of time", Version: "0123456789abcdef",
		Items: []*apiv1.CheckItem{{Name: "a", Title: "Reach db", Status: "passed"}}}
	out, _, err = runCLI(t, "check", "--api-url", url, "destination", "lake")
	if want := "workspaces/default/destinations/lake failed its check (version 0123456789ab): TRANSIENT: the check ran out of time\n  passed   Reach db\n"; out != want || exitStatus(err) != 1 {
		t.Errorf("a failure without a failed item: %v\n%q\nwant\n%q", err, out, want)
	}
}

// A preflight's parts in the order they ran, each with its items as a
// check's.
func TestPreflightChecklist(t *testing.T) {
	res := databaseCheck(t)
	f := &fakeChecks{preflight: preflight(res.GetCode(), res.GetMessage(), res.GetItems()...)}
	url := checksServer(t, f)
	out, _, err := runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
	want := "source shop's check: passed\n" +
		"destination lake's check: failed: PERMISSION_DENIED: Use schema public: etl has no USAGE on schema public: " +
		"ERROR: permission denied for schema public (SQLSTATE 42501) HINT: ask the schema's owner\n" +
		"  passed   Reach db.internal:5432\n" +
		"           PostgreSQL 16.2\n" +
		"  warning  Agree on TLS with db.internal:5432\n" +
		"           The certificate of db.internal:5432 expires on 2026-11-28.\n" +
		"           fix: Renew the certificate.\n" +
		"           code: TRANSIENT (certificate_expiring)\n" +
		"  failed   Use schema public\n" +
		"           etl has no USAGE on schema public.\n" +
		"           detail: ERROR: permission denied for schema public (SQLSTATE 42501)\n" +
		"                   HINT: ask the schema's owner\n" +
		"           fix: Grant USAGE on schema public to etl.\n" +
		"           field: schema\n" +
		"           code: PERMISSION_DENIED (privilege_missing)\n" +
		"  skipped  Read every table in public\n" +
		"           needs schema.usage, which failed\n" +
		"the sync's plan: passed\n" +
		"  passed   The plan\n" +
		"           2 streams, no problems\n" +
		"source shop's preflight: passed\n" +
		"  passed   Read orders\n" +
		"destination lake's preflight: skipped, the destination's check failed\n" +
		"\nworkspaces/default/pipelines/shop-to-lake (version abababababab)\n"
	if out != want {
		t.Errorf("printed\n%s\nwant\n%s", out, want)
	}
	if exitStatus(err) != 1 {
		t.Errorf("status %d: %v", exitStatus(err), err)
	}
	t.Logf("a preflight with a failed part and a skipped one:\n%s\nopenetl: %v", out, err)

	// A part whose connector didn't answer fails with its code and message,
	// and no items.
	f.preflight = preflight("TRANSIENT", "the destination's check didn't finish within 1m0s")
	if out, _, _ := runCLI(t, "preflight", "--api-url", url, "shop-to-lake"); !strings.Contains(out, "destination lake's check: failed: TRANSIENT: the destination's check didn't finish within 1m0s\nthe sync's plan: passed\n") {
		t.Errorf("a part's own failure:\n%s", out)
	}

	// A sync that can't be planned fails the plan's part, without items, and
	// skips both preflights. The plan printed after the parts is the API's.
	f.preflight = preflight("", "")
	f.preflight.Ok = false
	f.preflight.Parts[partPlan] = &apiv1.PreflightPart{Role: "sync", Stage: "plan", Code: "CONFIG_INVALID", Message: `the source has no stream "s"`}
	for _, i := range []int{partSourcePreflight, partDestinationPreflight} {
		f.preflight.Parts[i].Ok, f.preflight.Parts[i].Items, f.preflight.Parts[i].Skipped = false, nil, "a sync can't be planned"
	}
	out, _, err = runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
	want = "source shop's check: passed\n" +
		"destination lake's check: passed\n" +
		"the sync's plan: failed: CONFIG_INVALID: the source has no stream \"s\"\n" +
		"source shop's preflight: skipped, a sync can't be planned\n" +
		"destination lake's preflight: skipped, a sync can't be planned\n" +
		"\nworkspaces/default/pipelines/shop-to-lake (version abababababab)\n"
	if out != want || exitStatus(err) != 1 ||
		err.Error() != `workspaces/default/pipelines/shop-to-lake failed its preflight: the sync's plan failed: CONFIG_INVALID: the source has no stream "s"` {
		t.Errorf("a sync that can't be planned: %v\n%s\nwant\n%s", err, out, want)
	}
	t.Logf("a sync that can't be planned:\n%s\nopenetl: %v", out, err)

	// A part skipped with none failed, as from an older runner whose
	// destination didn't start, with the plan's part the API adds last,
	// leaves the preflight unfinished: the error names it, in the words of
	// its line.
	older := preflight("", "")
	plan := older.Parts[partPlan]
	older.Parts = append(slices.Delete(older.Parts, partPlan, partPlan+1), plan)
	older.Ok, older.Parts[partDestinationCheck].Ok, older.Parts[partDestinationCheck].Skipped = false, false, "the connector didn't start"
	f.preflight = older
	out, _, err = runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
	if exitStatus(err) != 1 || !strings.Contains(out, "destination lake's check: skipped, the connector didn't start\n") ||
		err.Error() != "workspaces/default/pipelines/shop-to-lake failed its preflight: destination lake's check was skipped: the connector didn't start" {
		t.Errorf("a skipped part: %v\n%s", err, out)
	}
	// So does one the result calls ok.
	f.preflight.Ok = true
	if _, _, err := runCLI(t, "preflight", "--api-url", url, "shop-to-lake"); exitStatus(err) != 1 || !strings.Contains(err.Error(), "destination lake's check was skipped") {
		t.Errorf("a skipped part of a result that's ok: %v", err)
	}

	// A result that isn't ok with nothing to explain it says what to do.
	f.preflight = preflight("", "")
	f.preflight.Ok = false
	if _, _, err := runCLI(t, "preflight", "--api-url", url, "shop-to-lake"); exitStatus(err) != 1 ||
		err.Error() != "workspaces/default/pipelines/shop-to-lake failed its preflight: it failed, though none of its parts did; run it again" {
		t.Errorf("a failure without a part: %v", err)
	}
}

// The plan after a preflight's parts is of the version it checked. A plan
// that can't be fetched leaves the preflight's verdict, and says so.
func TestPreflightPlan(t *testing.T) {
	const name = "workspaces/default/pipelines/shop-to-lake"
	f := &fakeChecks{preflight: preflight("", ""), plan: &apiv1.PipelinePlan{Pipeline: name, Version: "cdcdcdcdcdcdcdcd", Problems: []string{"the current version's"}}}
	url := checksServer(t, f)
	out, _, err := runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
	want := "destination lake's preflight: passed\n" +
		"\n" + name + " changed after its preflight: version abababababab was checked, and version cdcdcdcdcdcd is current\n"
	if err != nil || !strings.HasSuffix(out, want) || strings.Contains(out, "the current version's") {
		t.Errorf("a pipeline applied since: %v\n%s\nwant it to end\n%s", err, out, want)
	}
	t.Logf("a pipeline applied since:\n%s", out)

	f.plan, f.planErr = nil, connect.NewError(connect.CodeUnavailable, errors.New("the API went away"))
	out, _, err = runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
	if exitStatus(err) != 2 || !strings.HasSuffix(out, "destination lake's preflight: passed\n") ||
		err.Error() != name+" passed its preflight, but its plan couldn't be fetched: unavailable: the API went away" {
		t.Errorf("a passed preflight without its plan: %v, status %d\n%s", err, exitStatus(err), out)
	}
	f.preflight = preflight("AUTH_INVALID", "no")
	if _, _, err := runCLI(t, "preflight", "--api-url", url, "shop-to-lake"); exitStatus(err) != 1 ||
		err.Error() != name+" failed its preflight: destination lake's check failed: AUTH_INVALID: no; and its plan couldn't be fetched: unavailable: the API went away" {
		t.Errorf("a failed preflight without its plan: %v, status %d", err, exitStatus(err))
	}
}

// An item that isn't passed, warning or skipped fails its check, its part
// and its preflight, whatever their ok and code say.
func TestPassOrFailFromItems(t *testing.T) {
	failed := []*apiv1.CheckItem{{Name: "a", Title: "Reach db", Status: "failed", Message: "refused"}}
	unknown := []*apiv1.CheckItem{{Name: "a", Title: "Reach db", Status: "bogus"}}
	for name, items := range map[string][]*apiv1.CheckItem{"failed": failed, "an unknown status": unknown} {
		t.Run(name, func(t *testing.T) {
			f := &fakeChecks{destination: &apiv1.CheckResult{Ok: true, Items: items}}
			url := checksServer(t, f)
			out, _, err := runCLI(t, "check", "--api-url", url, "destination", "lake")
			if exitStatus(err) != 1 || !strings.HasPrefix(out, "workspaces/default/destinations/lake failed its check\n  failed   Reach db\n") {
				t.Errorf("check: %v\n%s", err, out)
			}

			f.preflight = preflight("", "")
			f.preflight.Parts[partSourcePreflight].Items = items
			out, _, err = runCLI(t, "preflight", "--api-url", url, "shop-to-lake")
			if exitStatus(err) != 1 || !strings.Contains(out, "source shop's preflight: failed\n  failed   Reach db\n") ||
				!strings.Contains(err.Error(), "source shop's preflight failed: Reach db") {
				t.Errorf("preflight: %v\n%s", err, out)
			}

			// apply reports it as a failure, not as checked.
			f.applied = createdPipeline()
			out, _, err = runCLI(t, "apply", "--api-url", url, "-f", documentFile(t))
			if err != nil || !strings.Contains(out, "  pipeline shop-to-lake: source shop's preflight failed: Reach db") || strings.Contains(out, "checked") {
				t.Errorf("apply: %v\n%s", err, out)
			}
		})
	}
	if got := apiCheckItems(unknown)[0].detail; got != `the item's status is "bogus", which counts as failed` {
		t.Errorf("an unknown status's detail: %q", got)
	}
}

// evil is text a connector could send to rewrite the terminal: the
// scratch proof's title clearing a line, an OSC 8 hyperlink, a
// right-to-left override, a C1 CSI and DEL.
const evil = "pub\x1b]0;pwned\x07\x1b[2K\rPASS  schema.usage  Use schema ok\nlic" +
	" \x1b]8;;https://evil.example/\x1b\\click me\x1b]8;;\x1b\\" +
	" invoice\u202egpj.exe \u2066x\u2069 \u009b31mred\x7f"

func unprintable(s string) []rune {
	var bad []rune
	for _, r := range s {
		if r != '\n' && (unicode.IsControl(r) || bidi(r)) {
			bad = append(bad, r)
		}
	}
	return bad
}

func TestNothingDrivesTheTerminal(t *testing.T) {
	item := func(status string) *apiv1.CheckItem {
		return &apiv1.CheckItem{Name: evil, Title: "Use {schema} " + evil, Status: status, Message: evil + " {schema}", Code: evil, Reason: evil,
			Fix: evil + " {schema}", Field: evil, Detail: "line one " + evil + "\nline two\r\n" + evil, Params: map[string]string{"schema": evil}}
	}
	f := &fakeChecks{destination: &apiv1.CheckResult{Code: evil, Message: evil, Version: evil, Items: []*apiv1.CheckItem{item("failed"), item("warning"), item("passed"), item("skipped")}}}
	f.preflight = preflight(evil, evil, item("failed"))
	f.preflight.Parts[partDestinationPreflight].Skipped = evil
	url := checksServer(t, f)
	for _, args := range [][]string{
		{"check", "--api-url", url, "destination", "lake"},
		{"preflight", "--api-url", url, "shop-to-lake"},
	} {
		out, _, err := runCLI(t, args...)
		if bad := unprintable(out); len(bad) > 0 {
			t.Errorf("%s printed %q in\n%q", args[0], bad, out)
		}
		if err == nil || len(unprintable(err.Error())) > 0 || strings.Contains(err.Error(), "\n") {
			t.Errorf("%s's error: %q", args[0], err)
		}
		// A detail keeps its lines.
		if !strings.Contains(out, "detail: line one pub]0;pwned[2K PASS  schema.usage  Use schema ok\n") || !strings.Contains(out, "\n                   line two\n") {
			t.Errorf("%s's detail:\n%s", args[0], out)
		}
	}

	f.applied = createdPipeline()
	out, _, err := runCLI(t, "apply", "--api-url", url, "-f", documentFile(t))
	if bad := unprintable(out); err != nil || len(bad) > 0 {
		t.Errorf("apply printed %q: %v\n%q", bad, err, out)
	}

	// The connector's own answer, run on its own.
	var b bytes.Buffer
	err = printConnectorCheck(&b, formatText, "acme", &protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{{
		Name: "a", Title: evil, Status: protocolv1.CheckStatus_CHECK_STATUS_FAILED, Message: evil, Fix: evil, Field: evil, Reason: evil,
		Detail: evil + "\n" + evil, Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, Message: evil},
	}}})
	if bad := unprintable(b.String()); len(bad) > 0 || err == nil || len(unprintable(err.Error())) > 0 {
		t.Errorf("connector check printed %q, error %q:\n%q", bad, err, b.String())
	}

	// JSON keeps the text, escaped.
	b.Reset()
	if err := printJSON(&b, &apiv1.CheckItem{Title: evil}); err != nil {
		t.Fatal(err)
	}
	var back apiv1.CheckItem
	if bad := unprintable(strings.ReplaceAll(b.String(), "\n", "")); len(bad) > 0 || protojson.Unmarshal(b.Bytes(), &back) != nil || back.GetTitle() != evil {
		t.Errorf("JSON printed %q:\n%s", bad, b.String())
	}
}

// --format json prints the API's result as it is, its templates unfilled,
// and nothing else on stdout.
func TestCheckFormatJSON(t *testing.T) {
	f := &fakeChecks{destination: databaseCheck(t), preflight: preflight("PERMISSION_DENIED", "no", databaseCheck(t).GetItems()...)}
	url := checksServer(t, f)
	out, errOut, err := runCLI(t, "check", "--api-url", url, "--format", "json", "destination", "lake")
	var res apiv1.CheckResult
	if exitStatus(err) != 1 || protojson.Unmarshal([]byte(out), &res) != nil || !proto.Equal(&res, f.destination) || errOut != "" {
		t.Errorf("check: %v\n%s", err, out)
	}
	t.Logf("a check that fails, as JSON:\n%s", out)

	out, _, err = runCLI(t, "preflight", "--api-url", url, "--format", "json", "shop-to-lake")
	var pre apiv1.PreflightResult
	if exitStatus(err) != 1 || protojson.Unmarshal([]byte(out), &pre) != nil || !proto.Equal(&pre, f.preflight) {
		t.Errorf("preflight: %v\n%s", err, out)
	}

	for _, args := range [][]string{
		{"check", "--api-url", url, "--format", "yaml", "destination", "lake"},
		{"preflight", "--api-url", url, "--format", "yaml", "shop-to-lake"},
	} {
		if out, _, err := runCLI(t, args...); exitStatus(err) != 2 || out != "" || !strings.Contains(err.Error(), "want text or json") {
			t.Errorf("%s --format yaml: %v, %q", args[0], err, out)
		}
	}
}

// 0 passed, warnings included; 1 failed; 2 couldn't run.
func TestCheckExitStatus(t *testing.T) {
	warned := databaseCheck(t)
	warned.Ok, warned.Code, warned.Message, warned.Items = true, "", "", warned.Items[:2]
	f := &fakeChecks{destination: warned, preflight: preflight("", "")}
	url := checksServer(t, f)
	// An API that drops every connection: a closed server's port could be
	// taken by another listener, which might never answer.
	gone := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(http.ErrAbortHandler) }))
	t.Cleanup(gone.Close)
	for _, c := range []struct {
		args   []string
		status int
	}{
		{[]string{"check", "--api-url", url, "destination", "lake"}, 0},
		{[]string{"preflight", "--api-url", url, "shop-to-lake"}, 0},
		{[]string{"check", "--api-url", gone.URL, "destination", "lake"}, 2},
		{[]string{"preflight", "--api-url", gone.URL, "shop-to-lake"}, 2},
		{[]string{"check", "--api-url", url, "destination"}, 2},
		{[]string{"check", "--no-such-flag", "destination", "lake"}, 2},
		{[]string{"preflight", "--api-url", url}, 2},
		// Another command's status is as it was.
		{[]string{"pause", "--api-url", url}, 1},
		{[]string{"pause", "--api-url", gone.URL, "shop-to-lake"}, 1},
		{[]string{"apply", "--api-url", url}, 1},
	} {
		_, _, err := runCLI(t, c.args...)
		got := 0
		if err != nil {
			got = exitStatus(err)
		}
		if got != c.status {
			t.Errorf("%v: status %d, want %d: %v", c.args, got, c.status, err)
		}
	}
	if _, _, err := runCLI(t, "check", "-h"); !errors.Is(err, flag.ErrHelp) {
		t.Errorf("-h: %v", err)
	}
	f.destination = &apiv1.CheckResult{Code: "AUTH_INVALID", Message: "no"}
	if _, _, err := runCLI(t, "check", "--api-url", url, "destination", "lake"); exitStatus(err) != 1 {
		t.Errorf("a failed check: %v", err)
	}
}

// main exits with the status run's error gives.
func TestMainExitStatus(t *testing.T) {
	if args, ok := os.LookupEnv("OPENETL_TEST_MAIN"); ok {
		os.Args = append([]string{"openetl"}, strings.Fields(args)...)
		main()
		os.Exit(0)
	}
	t.Setenv("OPENETL_API_KEY", "oetl_test")
	for args, status := range map[string]int{
		"check -h":                    0,
		"check destination":           2,
		"check --format xml source s": 2,
		"check --api-url http://127.0.0.1:1 destination lake": 2,
		"pause":           1,
		"no-such-command": 1,
	} {
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestMainExitStatus$")
		cmd.Env = append(os.Environ(), "OPENETL_TEST_MAIN="+args)
		err := cmd.Run()
		got := 0
		if e := (*exec.ExitError)(nil); errors.As(err, &e) {
			got = e.ExitCode()
		} else if err != nil {
			t.Fatal(err)
		}
		if got != status {
			t.Errorf("openetl %s: exit status %d, want %d", args, got, status)
		}
	}
}

// check pipeline is preflight, with a note on stderr.
func TestCheckPipelineIsPreflight(t *testing.T) {
	f := &fakeChecks{preflight: preflight("PERMISSION_DENIED", "no PutRecords")}
	url := checksServer(t, f)
	want, _, wantErr := runCLI(t, "preflight", "--api-url", url, "--workspace", "default", "shop-to-lake")
	for _, args := range [][]string{
		{"check", "--api-url", url, "pipeline", "--workspace", "default", "shop-to-lake"},
		{"check", "--api-url", url, "--workspace", "default", "pipeline", "shop-to-lake"},
		// What check pipeline took before; the pipeline's own data plane
		// runs a preflight.
		{"check", "--api-url", url, "--data-plane", "dp1", "pipeline", "shop-to-lake"},
		{"check", "--api-url=" + url, "-data-plane=dp1", "--format", "text", "pipeline", "shop-to-lake"},
	} {
		out, errOut, err := runCLI(t, args...)
		if out != want || err == nil || err.Error() != wantErr.Error() || exitStatus(err) != 1 {
			t.Errorf("%v: %v\n%s", args, err, out)
		}
		if errOut != "openetl: check pipeline is deprecated; use openetl preflight NAME\n" {
			t.Errorf("%v: stderr %q", args, errOut)
		}
	}
	t.Logf("the deprecation note: %q", "openetl: check pipeline is deprecated; use openetl preflight NAME\n")
	out, _, err := runCLI(t, "check", "--api-url", url, "pipeline", "--format", "json", "shop-to-lake")
	if exitStatus(err) != 1 || !isPreflightJSON(out) {
		t.Errorf("check pipeline --format json: %v\n%s", err, out)
	}
	if _, _, err := runCLI(t, "check", "--api-url", url, "pipeline"); exitStatus(err) != 2 {
		t.Errorf("check pipeline without a name: %v", err)
	}
}

func isPreflightJSON(s string) bool {
	return protojson.Unmarshal([]byte(s), &apiv1.PreflightResult{}) == nil
}

// After an apply, warnings show as well as failures, each with its fix.
func TestApplyShowsWarnings(t *testing.T) {
	applied := createdPipeline()
	applied.Resources = append(applied.Resources, &apiv1.AppliedResource{Name: "workspaces/default/destinations/other", Action: apiv1.ApplyAction_APPLY_ACTION_UPDATED, Version: "cdcdcdcdcdcdcdcd"})
	warned := databaseCheck(t)
	warned.Ok, warned.Code, warned.Message, warned.Items = true, "", "", warned.Items[:2]
	res := databaseCheck(t)
	f := &fakeChecks{applied: applied, destination: warned, preflight: preflight(res.GetCode(), res.GetMessage(), res.GetItems()...)}
	url := checksServer(t, f)
	out, _, err := runCLI(t, "apply", "--api-url", url, "-f", documentFile(t))
	if err != nil {
		t.Fatal(err)
	}
	block := "checked destination other\n" +
		"\nthe apply stands, but its checks found:\n" +
		"  pipeline shop-to-lake: destination lake's check failed: PERMISSION_DENIED: Use schema public: etl has no USAGE on schema public: " +
		"ERROR: permission denied for schema public (SQLSTATE 42501) HINT: ask the schema's owner\n" +
		"    warning  Agree on TLS with db.internal:5432: The certificate of db.internal:5432 expires on 2026-11-28.\n" +
		"             fix: Renew the certificate.\n" +
		"    failed   Use schema public: etl has no USAGE on schema public.\n" +
		"             fix: Grant USAGE on schema public to etl.\n" +
		"  destination other: passed, with warnings\n" +
		"    warning  Agree on TLS with db.internal:5432: The certificate of db.internal:5432 expires on 2026-11-28.\n" +
		"             fix: Renew the certificate.\n"
	if !strings.HasSuffix(out, block) {
		t.Errorf("printed\n%s\nwant it to end\n%s", out, block)
	}
	t.Logf("apply:\n%s", out)
}

// Each command's -h says what it takes, and its exit statuses.
func TestCheckUsage(t *testing.T) {
	for _, args := range [][]string{{"check", "-h"}, {"preflight", "-h"}, {"connector", "check", "-h"}} {
		_, errOut, err := runCLI(t, args...)
		if !errors.Is(err, flag.ErrHelp) || !strings.Contains(errOut, "-format string") || !strings.Contains(errOut, "Exit status: 0 when it passed") {
			t.Errorf("%v: %v\n%s", args, err, errOut)
		}
		t.Logf("openetl %s:\n%s", strings.Join(args, " "), errOut)
	}
}
