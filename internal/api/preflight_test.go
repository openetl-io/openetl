package api

import (
	"context"
	"errors"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/orchestration"
	"github.com/openetl-io/openetl/internal/orchestration/contract"
)

// Every field of an item reaches the API as it was, templates unfilled.
func TestAPIItems(t *testing.T) {
	in := contract.CheckItem{
		Name: "schema", Title: "Use {schema}", Status: "failed", Message: "{user} has no USAGE on {schema}.", Code: "PERMISSION_DENIED",
		Fix: "GRANT USAGE ON SCHEMA {schema} TO {user}", Field: "schema", Kind: "permission", Description: "Read from {schema}",
		Reason: "privilege_missing", Params: map[string]string{"schema": "public", "user": "etl"}, Detail: "pq: denied", Duration: 3 * time.Second,
	}
	v := reflect.ValueOf(in)
	for i := range v.NumField() {
		if v.Field(i).IsZero() {
			t.Fatalf("set %s in the test's item", v.Type().Field(i).Name)
		}
	}
	got := apiItems([]contract.CheckItem{in, {Name: "connect", Title: "Connect", Status: "passed"}})
	want := []*apiv1.CheckItem{{
		Name: "schema", Title: "Use {schema}", Status: "failed", Message: "{user} has no USAGE on {schema}.", Code: "PERMISSION_DENIED",
		Fix: "GRANT USAGE ON SCHEMA {schema} TO {user}", Field: "schema", Kind: "permission", Description: "Read from {schema}",
		Reason: "privilege_missing", Params: map[string]string{"schema": "public", "user": "etl"}, Detail: "pq: denied", Duration: durationpb.New(3 * time.Second),
	}, {Name: "connect", Title: "Connect", Status: "passed"}}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Errorf("item %d\n %v\nwant\n %v", i, got[i], want[i])
		}
	}
}

// The plan's items have the shape a connector's have: a kind, a reason for a
// failure, and a stream's name as a param. Its text is kept as written, so
// braces in it stay.
func TestPlanItems(t *testing.T) {
	part := &apiv1.PreflightPart{}
	planItems(part, &apiv1.PipelinePlan{
		Problems: []string{"orders: no cursor; set {cursor}"},
		Warnings: []*apiv1.PlanWarning{{Stream: "users", Message: "collapses rows"}, {Stream: "logs", Message: "keys by {stream}"}, {Message: "see {docs}"}},
	})
	settle(part)
	want := []*apiv1.CheckItem{
		{Name: "plan.problem.1", Title: "The plan", Status: "failed", Message: "orders: no cursor; set {cursor}", Code: "CONFIG_INVALID", Kind: "configuration", Reason: "config_invalid"},
		{Name: "plan.warning.1", Title: "Stream {stream}", Status: "warning", Message: "collapses rows", Kind: "configuration", Params: map[string]string{"stream": "users"}},
		{Name: "plan.warning.2", Title: "Stream logs", Status: "warning", Message: "keys by {stream}", Kind: "configuration"},
		{Name: "plan.warning.3", Title: "The plan", Status: "warning", Message: "see {docs}", Kind: "configuration"},
	}
	if len(part.GetItems()) != len(want) {
		t.Fatalf("items %v", part.GetItems())
	}
	for i := range want {
		if !proto.Equal(part.GetItems()[i], want[i]) {
			t.Errorf("item %d\n %v\nwant\n %v", i, part.GetItems()[i], want[i])
		}
	}
	if part.GetOk() || part.GetCode() != "CONFIG_INVALID" || part.GetMessage() != "The plan: orders: no cursor; set {cursor}" {
		t.Errorf("part %v", part)
	}
	for _, it := range part.GetItems() {
		if got := itemError(it); strings.Contains(it.GetMessage(), "{") && !strings.Contains(got, it.GetMessage()) {
			t.Errorf("%s renders as %q", it.GetName(), got)
		}
	}

	clean := &apiv1.PreflightPart{}
	planItems(clean, &apiv1.PipelinePlan{Streams: []*apiv1.StreamPlan{{Name: "a"}}})
	settle(clean)
	if !clean.GetOk() || len(clean.GetItems()) != 1 || clean.GetItems()[0].GetStatus() != "passed" || clean.GetItems()[0].GetKind() != "configuration" {
		t.Errorf("a clean plan %v", clean)
	}
	// A part the runner failed gets no item that says the plan passed.
	failed := &apiv1.PreflightPart{Code: "CONFIG_INVALID", Message: "no stream orders"}
	planItems(failed, &apiv1.PipelinePlan{})
	if len(failed.GetItems()) != 0 {
		t.Errorf("a failed part's items %v", failed.GetItems())
	}
}

// A failed item fails a check whatever its code says, and gives the result
// its code and its text as the connector's error has it; an item fails
// closed.
func TestCheckResultFromItems(t *testing.T) {
	failed := contract.CheckItem{
		Name: "connect", Title: "Connect to {host}", Status: "failed", Message: "{host} refused the connection.", Code: "TRANSIENT",
		Params: map[string]string{"host": "db:5432"}, Detail: "dial tcp: refused",
	}
	for _, c := range []struct {
		name          string
		in            contract.ConnectorOpResult
		ok            bool
		code, message string
	}{
		{"passed", contract.ConnectorOpResult{Checks: []contract.CheckItem{{Status: "passed"}, {Status: "warning"}, {Status: "skipped"}}}, true, "", ""},
		{"no items", contract.ConnectorOpResult{}, true, "", ""},
		{"a code", contract.ConnectorOpResult{Code: "AUTH_INVALID", Message: "denied", Checks: []contract.CheckItem{{Status: "passed"}}}, false, "AUTH_INVALID", "denied"},
		{"a failed item without a code", contract.ConnectorOpResult{Checks: []contract.CheckItem{{Status: "passed"}, failed}}, false, "TRANSIENT", "Connect to db:5432: db:5432 refused the connection: dial tcp: refused"},
		{"its own code wins", contract.ConnectorOpResult{Code: "AUTH_INVALID", Message: "denied", Checks: []contract.CheckItem{failed}}, false, "AUTH_INVALID", "denied"},
		{"an item without a code", contract.ConnectorOpResult{Checks: []contract.CheckItem{{Title: "Read", Status: "failed", Message: "No rows."}}}, false, "INTERNAL", "Read: No rows."},
		{"a status it doesn't know", contract.ConnectorOpResult{Checks: []contract.CheckItem{{Title: "Read", Status: "unspecified"}}}, false, "INTERNAL", "Read: The check failed."},
	} {
		r := checkResult(c.in)
		if r.GetOk() != c.ok || r.GetCode() != c.code || r.GetMessage() != c.message {
			t.Errorf("%s: ok %v, code %q, message %q; want %v, %q, %q", c.name, r.GetOk(), r.GetCode(), r.GetMessage(), c.ok, c.code, c.message)
		}
	}
}

// A preflight is ok only if every part is; one that failed, didn't start,
// or was skipped leaves it not ok. The runner's parts come through as it
// sent them, in its order, and the plan runs only after the runner's plan
// part passed, or, from a runner that sends none, the source's check.
func TestPreflightResult(t *testing.T) {
	passed := []contract.CheckItem{{Name: "connect", Title: "Connect", Status: "passed"}}
	part := func(role, stage string, items ...contract.CheckItem) contract.CheckGroup {
		return contract.CheckGroup{Role: role, Stage: stage, Items: items}
	}
	failed := func(role, stage, message string) contract.CheckGroup {
		return contract.CheckGroup{Role: role, Stage: stage, Code: "INTERNAL", Message: message}
	}
	skipped := func(role, stage, why string) contract.CheckGroup {
		return contract.CheckGroup{Role: role, Stage: stage, Skipped: why}
	}
	// The runner's five parts, in its order.
	runner := func(change map[int]contract.CheckGroup) []contract.CheckGroup {
		gs := []contract.CheckGroup{
			part("source", "check", passed...), part("destination", "check", passed...), part("sync", "plan"),
			part("source", "preflight", passed...), part("destination", "preflight", passed...),
		}
		for i, g := range change {
			gs[i] = g
		}
		return gs
	}
	// An older runner's four, without a plan part.
	older := func(change map[int]contract.CheckGroup) []contract.CheckGroup {
		gs := slices.Delete(runner(nil), 2, 3)
		for i, g := range change {
			gs[i] = g
		}
		return gs
	}
	cleanPlan := &apiv1.PipelinePlan{Streams: []*apiv1.StreamPlan{{Name: "orders"}}}
	for _, c := range []struct {
		name    string
		groups  []contract.CheckGroup
		plan    *apiv1.PipelinePlan
		ok      bool
		planned bool
	}{
		{name: "all passed", groups: runner(nil), plan: cleanPlan, ok: true, planned: true},
		{name: "warnings pass", groups: runner(map[int]contract.CheckGroup{4: part("destination", "preflight", contract.CheckItem{Name: "w", Status: "warning"})}), plan: cleanPlan, ok: true, planned: true},
		{name: "the source didn't start", groups: runner(map[int]contract.CheckGroup{
			0: failed("source", "check", "it exited"), 2: skipped("sync", "plan", "the source didn't start"),
			3: skipped("source", "preflight", "the source didn't start"), 4: skipped("destination", "preflight", "a sync can't be planned"),
		})},
		{name: "the source's check failed", groups: runner(map[int]contract.CheckGroup{
			0: part("source", "check", contract.CheckItem{Name: "connect", Status: "failed"}), 2: skipped("sync", "plan", "the source's check failed"),
			3: skipped("source", "preflight", "the source's check failed"), 4: skipped("destination", "preflight", "a sync can't be planned"),
		})},
		{name: "the source's check didn't finish", groups: runner(map[int]contract.CheckGroup{
			0: {Role: "source", Stage: "check", Code: "TRANSIENT", Message: "the source's check didn't finish within 1m0s"}, 2: skipped("sync", "plan", "the source's check failed"),
			3: skipped("source", "preflight", "the source's check failed"), 4: skipped("destination", "preflight", "a sync can't be planned"),
		})},
		{name: "the destination didn't start", groups: runner(map[int]contract.CheckGroup{
			1: failed("destination", "check", "it exited"), 2: skipped("sync", "plan", "the destination didn't start"),
			3: skipped("source", "preflight", "a sync can't be planned"), 4: skipped("destination", "preflight", "the destination didn't start"),
		})},
		{name: "the destination's check failed", groups: runner(map[int]contract.CheckGroup{
			1: part("destination", "check", contract.CheckItem{Name: "connect", Status: "failed"}), 4: skipped("destination", "preflight", "the destination's check failed"),
		}), plan: cleanPlan, planned: true},
		{name: "settling failed, the source's Discover included", groups: runner(map[int]contract.CheckGroup{
			2: {Role: "sync", Stage: "plan", Code: "AUTH_INVALID", Message: "settling what a sync reads and writes: denied"},
			3: skipped("source", "preflight", "a sync can't be planned"), 4: skipped("destination", "preflight", "a sync can't be planned"),
		})},
		{name: "a preflight that didn't answer", groups: runner(map[int]contract.CheckGroup{3: failed("source", "preflight", "the source's preflight didn't finish within 5m0s")}), plan: cleanPlan, planned: true},
		{name: "a preflight's item of a status it doesn't know", groups: runner(map[int]contract.CheckGroup{3: part("source", "preflight", contract.CheckItem{Name: "read", Status: "unspecified"})}), plan: cleanPlan, planned: true},
		{name: "the plan's problems", groups: runner(nil), plan: &apiv1.PipelinePlan{Problems: []string{"no cursor"}}, planned: true},
		{name: "an older runner", groups: older(nil), plan: cleanPlan, ok: true, planned: true},
		{name: "an older runner's skipped part", groups: older(map[int]contract.CheckGroup{1: skipped("destination", "check", "the connector didn't start")}), plan: cleanPlan, planned: true},
		{name: "an older runner's source that didn't start", groups: older(map[int]contract.CheckGroup{0: skipped("source", "check", "the connector didn't start")})},
		{name: "no parts", plan: cleanPlan},
	} {
		t.Run(c.name, func(t *testing.T) {
			planned := false
			out, err := preflightResult("v1", contract.ConnectorOpResult{Preflight: c.groups}, nil, func() (*apiv1.PipelinePlan, error) {
				planned = true
				return c.plan, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if out.GetOk() != c.ok || planned != c.planned || out.GetVersion() != "v1" {
				t.Fatalf("ok %v, planned %v, want %v, %v: %v", out.GetOk(), planned, c.ok, c.planned, out)
			}
			for i, p := range out.GetParts() {
				failedItem := slices.ContainsFunc(p.GetItems(), func(it *apiv1.CheckItem) bool { return failing(it.GetStatus()) })
				if p.GetOk() && (p.GetCode() != "" || p.GetSkipped() != "" || failedItem) {
					t.Errorf("part %s %s is ok: %v", p.GetRole(), p.GetStage(), p)
				}
				if !p.GetOk() && p.GetSkipped() == "" && p.GetCode() == "" {
					t.Errorf("part %s %s failed without a code: %v", p.GetRole(), p.GetStage(), p)
				}
				if i >= len(c.groups) {
					continue
				}
				g := c.groups[i]
				if p.GetRole() != g.Role || p.GetStage() != g.Stage || p.GetSkipped() != g.Skipped || g.Code != "" && (p.GetCode() != g.Code || p.GetMessage() != g.Message) {
					t.Errorf("part %d is %v, the runner sent %+v", i, p, g)
				}
			}
			n := len(c.groups)
			if !slices.ContainsFunc(c.groups, func(g contract.CheckGroup) bool { return g.Role == "sync" }) {
				n++
				if last := out.GetParts()[n-1]; last.GetRole() != "sync" || last.GetStage() != "plan" || !c.planned && last.GetSkipped() == "" {
					t.Errorf("the plan part an older runner didn't send: %v", last)
				}
			}
			if len(out.GetParts()) != n {
				t.Errorf("%d parts, want %d", len(out.GetParts()), n)
			}
		})
	}
}

// When the discover a preflight runs for its plan can't run, the preflight
// can't, and its error says so; no result is given.
func TestPreflightResultWhenTheCatalogCantBeRefreshed(t *testing.T) {
	s := &Server{Logger: slog.New(slog.DiscardHandler)}
	in := contract.ConnectorOpInput{Op: contract.OpDiscover, DataPlane: "dp1"}
	refresh := s.opError(catalogNotRefreshed, in, &orchestration.Failure{Code: "AUTH_INVALID", Message: "password authentication failed"})
	groups := []contract.CheckGroup{
		{Role: "source", Stage: "check"}, {Role: "destination", Stage: "check"}, {Role: "sync", Stage: "plan"},
		{Role: "source", Stage: "preflight"}, {Role: "destination", Stage: "preflight"},
	}
	out, err := preflightResult("v1", contract.ConnectorOpResult{Preflight: groups}, nil, func() (*apiv1.PipelinePlan, error) { return nil, refresh })
	if out != nil || !errors.Is(err, refresh) {
		t.Fatalf("%v, %v", out, err)
	}
	want := "the preflight couldn't run: the source's catalog couldn't be refreshed: AUTH_INVALID: password authentication failed"
	if ce, ok := errors.AsType[*connect.Error](err); !ok || ce.Code() != connect.CodeFailedPrecondition || ce.Message() != want {
		t.Errorf("%v, want %s", err, want)
	}
}

// An operation that couldn't run is an error that says so, never a failed
// check.
func TestOpError(t *testing.T) {
	s := &Server{Logger: slog.New(slog.DiscardHandler)}
	in := contract.ConnectorOpInput{Op: contract.OpCheck, DataPlane: "dp1"}
	for _, c := range []struct {
		err     error
		code    connect.Code
		message string
	}{
		{&orchestration.Failure{Code: contract.NoRunner, Message: "no runner took it in time"}, connect.CodeUnavailable, `the check couldn't run: no runner of data plane "dp1" took it within 1m0s`},
		{&orchestration.Failure{Code: "INTERNAL", Message: "the source didn't start: exit status 1"}, connect.CodeUnavailable, "the check couldn't run: INTERNAL: the source didn't start: exit status 1"},
		{&orchestration.Failure{Code: "TRANSIENT", Message: "TRANSIENT: its runner stopped reporting it, or it ran out of time"}, connect.CodeUnavailable, "the check couldn't run: TRANSIENT: its runner stopped reporting it, or it ran out of time"},
		{&orchestration.Failure{Code: "CONFIG_INVALID", Message: "no host"}, connect.CodeInvalidArgument, "the check couldn't run: CONFIG_INVALID: no host"},
		{&orchestration.Failure{Code: "PERMISSION_DENIED", Message: "denied"}, connect.CodeFailedPrecondition, "the check couldn't run: PERMISSION_DENIED: denied"},
		{orchestration.ErrStopped, connect.CodeUnavailable, "the check couldn't run: cancelled"},
		{errors.New("the workflow ran out of time"), connect.CodeUnavailable, "the check couldn't run: the workflow ran out of time"},
	} {
		err := s.opError("the check couldn't run", in, c.err)
		if ce, ok := errors.AsType[*connect.Error](err); !ok || ce.Code() != c.code || ce.Message() != c.message {
			t.Errorf("%v: %v, want %v: %s", c.err, err, c.code, c.message)
		}
	}
	if err := s.opError("the check couldn't run", in, context.Canceled); !errors.Is(err, context.Canceled) {
		t.Errorf("a cancelled request: %v", err)
	}
}
