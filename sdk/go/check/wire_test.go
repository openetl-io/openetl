package check

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

func TestProtoRoundTrip(t *testing.T) {
	r := Run(t.Context(), []Check{
		{Name: "connect", Kind: Connection, Title: "Reach {address}", Description: "The port answers.", Field: "host", Params: Params{"address": "db:5432"},
			Run: func(_ context.Context, p *Probe) error {
				p.Param("version", "17")
				p.Found("PostgreSQL {version}")
				return nil
			}},
		{Name: "auth", Kind: Authentication, Title: "Sign in as {user}", Params: Params{"user": "etl"},
			Run: fails(func() *Failure {
				f := RateLimited.Wrap(&codedErr{after: time.Second}, "wait", "1s")
				f.Fix = "Wait {wait}."
				return f
			}())},
		{Name: "skip", Title: "Skip", Needs: []string{"auth"}, Run: pass},
		{Name: "lag", Title: "Lag", Advisory: true, Run: fails(errors.New("behind"))},
	})
	items, rerr := r.Proto()
	resp := &protocolv1.CheckResponse{Items: items, Error: rerr}
	connect, auth, skip := items[0], items[1], items[2]
	if connect.GetTitle() != "Reach {address}" || connect.GetMessage() != "PostgreSQL {version}" || connect.GetError() != nil || connect.GetKind() != protocolv1.CheckKind_CHECK_KIND_CONNECTION || connect.GetField() != "host" {
		t.Errorf("connect, which keeps its templates: %v", connect)
	}
	// The item's error has its message filled in, and its detail apart.
	if auth.GetMessage() != RateLimited.Message || auth.GetReason() != "rate_limited" || auth.GetFix() != "Wait {wait}." || auth.GetDetail() != "coded" ||
		auth.GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED || auth.GetError().GetMessage() != "The upstream is limiting requests." ||
		auth.GetError().GetRetryAfter().AsDuration() != time.Second || auth.GetKind() != protocolv1.CheckKind_CHECK_KIND_AUTHENTICATION {
		t.Errorf("auth: %v", auth)
	}
	if skip.GetMessage() != "needs auth, which failed" || skip.GetError() != nil || skip.GetKind() != protocolv1.CheckKind_CHECK_KIND_CUSTOM || skip.GetDuration() != nil {
		t.Errorf("skip: %v", skip)
	}
	if items[3].GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_WARNING || items[3].GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL ||
		items[3].GetError().GetMessage() != unknownWarning || items[3].GetDetail() != "behind" {
		t.Errorf("a warning: %v", items[3])
	}
	// The answer's error is the first failed item's, as "title: message",
	// then the detail.
	if rerr.GetCode() != protocolv1.ErrorCode_ERROR_CODE_RATE_LIMITED || rerr.GetMessage() != "Sign in as etl: The upstream is limiting requests: coded" || rerr.GetRetryAfter().AsDuration() != time.Second {
		t.Errorf("the answer's error: %v", rerr)
	}

	back := FromProto(resp)
	if statuses(back) != statuses(r) {
		t.Fatalf("statuses %s, want %s", statuses(back), statuses(r))
	}
	for i := range r.Items {
		want, got := r.Items[i].Rendered(), back.Items[i].Rendered()
		got.Duration, want.Duration = 0, 0
		if want.Failure != nil {
			w := *want.Failure
			w.cause = nil
			want.Failure = &w
		}
		if !equalItems(got, want) {
			t.Errorf("item %d:\n got %+v %+v\nwant %+v %+v", i, got, got.Failure, want, want.Failure)
		}
	}
	if back.Err() == nil || back.Err().Error() != r.Err().Error() || codeOf(back.Err()) != codeOf(r.Err()) {
		t.Errorf("Err read back: %v, want %v", back.Err(), r.Err())
	}
}

func equalItems(a, b Item) bool {
	if a.Name != b.Name || a.Kind != b.Kind || a.Title != b.Title || a.Description != b.Description || a.Field != b.Field ||
		a.Status != b.Status || a.Message != b.Message || len(a.Params) != len(b.Params) || (a.Failure == nil) != (b.Failure == nil) {
		return false
	}
	for k, v := range a.Params {
		if b.Params[k] != v {
			return false
		}
	}
	if a.Failure == nil {
		return true
	}
	f, g := *a.Failure, *b.Failure
	return f.Reason == g.Reason && f.Code == g.Code && f.Message == g.Message && f.Fix == g.Fix && f.Detail == g.Detail && f.After == g.After
}

// An answer without items is its error alone: nothing is made up for it.
func TestFromProtoWithoutItems(t *testing.T) {
	if r := FromProto(&protocolv1.CheckResponse{}); len(r.Items) != 0 || !r.OK() {
		t.Errorf("a passing answer: %s, %v", statuses(r), r.Err())
	}
	r := FromProto(&protocolv1.PreflightSourceResponse{Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, Message: "bad password", RetryAfter: nil}})
	if len(r.Items) != 0 || r.OK() || r.Err().Error() != "bad password" || codeOf(r.Err()) != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID {
		t.Errorf("a failing answer: %s, %v", statuses(r), r.Err())
	}
	orig := errors.New("pg: no route")
	if r := FromError(orig); len(r.Items) != 0 || !errors.Is(r.Err(), orig) {
		t.Errorf("FromError: %s, %v", statuses(r), r.Err())
	}
}

// What a connector's answer leaves out or gets wrong fails the report.
func TestFromProtoFailsClosed(t *testing.T) {
	failed := protocolv1.CheckStatus_CHECK_STATUS_FAILED
	for name, resp := range map[string]*protocolv1.CheckResponse{
		"an unknown status": {Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: 9}}},
		"no status":         {Items: []*protocolv1.CheckItem{{Name: "a", Title: "A"}}},
		"failed, no error":  {Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: failed}}},
		"an error, no item failed": {
			Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_CONFIG_INVALID, Message: "bad"},
			Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: protocolv1.CheckStatus_CHECK_STATUS_PASSED}},
		},
	} {
		r := FromProto(resp)
		if r.OK() || r.Err() == nil || r.OK() != (r.Err() == nil) {
			t.Errorf("%s: OK %v, Err %v", name, r.OK(), r.Err())
		}
		if it := r.Items[0]; it.Status != Passed && (it.Status != Failed || it.Failure == nil || it.Failure.Code == protocolv1.ErrorCode_ERROR_CODE_UNSPECIFIED) {
			t.Errorf("%s: %s, %+v", name, it.Status, it.Failure)
		}
	}
	r := FromProto(&protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{{Name: "a", Title: "A", Status: failed, Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_PERMISSION_DENIED, Message: "denied"}, Detail: "pq: no"}}})
	if f := r.Items[0].Failure; f.Message != "denied" || f.Detail != "pq: no" || f.Reason != "unknown" || r.Items[0].Message != "denied" || r.Err().Error() != "A: denied: pq: no" {
		t.Errorf("an item with only an error: %+v, %v", f, r.Err())
	}
}

// A warning that arrives with Unknown's reason and no message of its own
// reads as Run's does; a failed item, and a warning with a message, don't
// change.
func TestFromProtoUnknownWarning(t *testing.T) {
	internal := &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_INTERNAL}
	warning, failed := protocolv1.CheckStatus_CHECK_STATUS_WARNING, protocolv1.CheckStatus_CHECK_STATUS_FAILED
	for name, c := range map[string]struct {
		p    *protocolv1.CheckItem
		want string
	}{
		"a warning without a message": {&protocolv1.CheckItem{Status: warning, Error: internal, Detail: "one disk"}, unknownWarning},
		"an older SDK's warning":      {&protocolv1.CheckItem{Status: warning, Error: internal, Message: Unknown.Message, Reason: "unknown"}, unknownWarning},
		"a warning with a message":    {&protocolv1.CheckItem{Status: warning, Error: internal, Message: "One disk."}, "One disk."},
		"a failed item":               {&protocolv1.CheckItem{Status: failed, Error: internal, Detail: "one disk"}, Unknown.Message},
	} {
		c.p.Name, c.p.Title = "a", "A"
		r := FromProto(&protocolv1.CheckResponse{Items: []*protocolv1.CheckItem{c.p}})
		it := r.Items[0]
		if it.Status != Status(c.p.GetStatus()) || it.Message != c.want || it.Failure.Message != c.want || it.Failure.Reason != "unknown" || r.OK() != (it.Status == Warning) {
			t.Errorf("%s: %s %q, %+v", name, it.Status, it.Message, it.Failure)
		}
	}
}

// Every string goes on the wire as valid UTF-8.
func TestProtoUTF8(t *testing.T) {
	r := Run(t.Context(), []Check{
		{Name: "schema", Title: "Use schema", Run: func(_ context.Context, p *Probe) error {
			p.Param("owner", "M\xfcller")
			return errors.New("FEHLER: Zugriff verweigert f\xfcr Schema")
		}},
	})
	it := r.Items[0]
	if !utf8.ValidString(it.Failure.Detail) || !utf8.ValidString(it.Params["owner"]) || !utf8.ValidString(r.Err().Error()) {
		t.Errorf("Run kept invalid UTF-8: %q, %q", it.Failure.Detail, it.Params["owner"])
	}
	r.Items = append(r.Items, Item{Name: "x\xff", Title: "T\xff", Status: Failed, Params: Params{"k\xff": "v\xff"}, Failure: &Failure{Detail: "d\xff", Fix: "f\xff"}})
	items, rerr := r.Proto()
	if _, err := proto.Marshal(&protocolv1.CheckResponse{Items: items, Error: rerr}); err != nil {
		t.Errorf("the answer doesn't marshal: %v", err)
	}
}

func TestProtoStatusOutsideTheFour(t *testing.T) {
	items, rerr := Report{Items: []Item{{Name: "a", Title: "A", Status: 7}}}.Proto()
	if items[0].GetStatus() != protocolv1.CheckStatus_CHECK_STATUS_FAILED || items[0].GetError().GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || rerr == nil {
		t.Errorf("%v, %v", items[0], rerr)
	}
}

// A connector that gives only an error answers with no items and that
// error, without a URL's secrets.
func TestProtoFromError(t *testing.T) {
	items, rerr := FromError(&codedErr{code: protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID, after: time.Minute}).Proto()
	if len(items) != 0 || rerr.GetCode() != protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID || rerr.GetMessage() != "coded" || rerr.GetRetryAfter().AsDuration() != time.Minute {
		t.Errorf("%v, %v", items, rerr)
	}
	if items, rerr := FromError(nil).Proto(); items != nil || rerr != nil {
		t.Errorf("a passing check: %v, %v", items, rerr)
	}
	_, rerr = FromError(Classify(errors.New("x"))).Proto()
	if rerr.GetCode() != protocolv1.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("an error without a code: %v", rerr)
	}
}

// An item's error has its message filled in, and nothing more.
func TestProtoItemError(t *testing.T) {
	r := Run(t.Context(), []Check{{Name: "schema", Title: "Use {schema}", Params: Params{"schema": "public"}, Run: fails(
		PrivilegeMissing.Wrap(errors.New("pq: permission denied for schema public"), "principal", "etl", "privilege", "USAGE", "object", "schema public"))}})
	items, rerr := r.Proto()
	it := items[0]
	if it.GetMessage() != PrivilegeMissing.Message || it.GetError().GetMessage() != "etl has no USAGE on schema public." || it.GetDetail() != "pq: permission denied for schema public" {
		t.Errorf("the item: %v", it)
	}
	if rerr.GetMessage() != "Use public: etl has no USAGE on schema public: pq: permission denied for schema public" {
		t.Errorf("the answer's error: %v", rerr)
	}
}

// Every text of an answer loses each secret, in each form a URL gives it;
// the item's name and reason, which a secret isn't, stay.
func TestRedact(t *testing.T) {
	const secret = "p@ss word/1"
	forms := []string{secret, "p%40ss+word%2F1", "p@ss%20word%2F1", "p%40ss%20word%2F1"}
	text := strings.Join(forms, " | ")
	want := "[redacted] | [redacted] | [redacted] | [redacted]"
	items := []*protocolv1.CheckItem{{
		Name: "auth", Reason: "credentials_rejected", Title: text, Description: text, Message: text, Fix: text, Detail: text, Field: text,
		Params: map[string]string{secret: text, "dsn": "pg://etl:" + forms[3] + "@db"}, Error: &protocolv1.Error{Message: text},
	}, {Name: "plain", Title: "Reach db"}}
	e := &protocolv1.Error{Message: "Sign in: " + text}
	Redact([]string{"", secret}, items, e)
	it := items[0]
	for name, got := range map[string]string{
		"title": it.GetTitle(), "description": it.GetDescription(), "message": it.GetMessage(), "fix": it.GetFix(), "detail": it.GetDetail(),
		"field": it.GetField(), "the item's error": it.GetError().GetMessage(), "the param named the secret": it.GetParams()["[redacted]"],
	} {
		if got != want {
			t.Errorf("%s: %q", name, got)
		}
	}
	if it.GetName() != "auth" || it.GetReason() != "credentials_rejected" || it.GetParams()["dsn"] != "pg://etl:[redacted]@db" || len(it.GetParams()) != 2 {
		t.Errorf("the item: %v", it)
	}
	if e.GetMessage() != "Sign in: "+want || items[1].GetTitle() != "Reach db" {
		t.Errorf("the answer's error %q, the other item %v", e.GetMessage(), items[1])
	}
	Redact(nil, items, nil)
	if it.GetTitle() != want {
		t.Errorf("no secrets changed the title: %q", it.GetTitle())
	}
}

// Redact finds a secret in an item's name and reason, and quoted as Go and
// JSON quote it and with lowercase percent escapes, in one pass, so a short
// secret isn't found in what replaced a longer one.
func TestRedactMoreForms(t *testing.T) {
	const secret = `p"s<s\1/`
	// Go's quoting, JSON's with < escaped, and percent escapes in lower and
	// upper case.
	forms := []string{`p\"s<s\\1/`, `p\"s` + `\` + `u003cs\\1/`, "p%22s%3cs%5c1%2f", "p%22s%3Cs%5C1%2F"}
	text := strings.Join(forms, " | ")
	want := "[redacted] | [redacted] | [redacted] | [redacted]"
	items := []*protocolv1.CheckItem{{Name: "auth." + secret, Reason: "rejected_" + secret, Title: text, Detail: secret, Error: &protocolv1.Error{Message: "red"}}}
	e := &protocolv1.Error{Message: fmt.Sprintf("Sign in: %q", secret)}
	Redact([]string{secret, "red"}, items, e)
	it := items[0]
	if it.GetName() != "auth.[redacted]" || it.GetReason() != "rejected_[redacted]" || it.GetError().GetMessage() != "[redacted]" || it.GetDetail() != "[redacted]" {
		t.Errorf("the item: %v", it)
	}
	if it.GetTitle() != want {
		t.Errorf("the title %q", it.GetTitle())
	}
	if e.GetMessage() != `Sign in: "[redacted]"` {
		t.Errorf("the answer's error %q", e.GetMessage())
	}
}

// A report's method is a host's, which Of doesn't call.
type reporting struct{ listed }

func (reporting) CheckReport(context.Context) Report { return Report{Items: []Item{{Name: "remote"}}} }

type listed struct {
	checks []Check
	err    error
}

func (l listed) Checks() []Check             { return l.checks }
func (l listed) Check(context.Context) error { return l.err }

type panicking struct{}

func (panicking) Checks() []Check             { panic("listing") }
func (panicking) Check(context.Context) error { return nil }

type checkPanics struct{}

func (checkPanics) Check(context.Context) error { panic("checking") }

type lister struct{}

func (lister) Checks() []Check { return nil }

type checkOnly struct{}

func (checkOnly) Check(context.Context) error { return errors.New("legacy") }

type nothing struct{}

func TestOf(t *testing.T) {
	legacy := errors.New("legacy")
	for _, tc := range []struct {
		name     string
		c        any
		statuses string
		err      string
	}{
		{"named checks", listed{checks: []Check{{Name: "named", Title: "N", Run: pass}}, err: legacy}, "named=passed", ""},
		{"a host's report", reporting{listed{checks: []Check{{Name: "named", Title: "N", Run: pass}}}}, "named=passed", ""},
		{"an empty list falls back to Check", listed{err: legacy}, "", "legacy"},
		{"Check alone", checkOnly{}, "", "legacy"},
		{"an empty list alone", lister{}, "", ""},
		{"no check at all", nothing{}, "", ""},
		{"a panic listing", panicking{}, "", "the connector panicked listing its checks: listing"},
		{"a panic checking", checkPanics{}, "", "the connector's check panicked: checking"},
	} {
		r := Of(t.Context(), tc.c)
		if got := statuses(r); got != tc.statuses {
			t.Errorf("%s: %s", tc.name, got)
		}
		if got := r.Err(); tc.err == "" && got != nil || tc.err != "" && (got == nil || got.Error() != tc.err) {
			t.Errorf("%s: Err %v, want %q", tc.name, got, tc.err)
		}
	}
	r := RunList(t.Context(), func() []Check { panic("preflight") })
	if len(r.Items) != 0 || r.Err() == nil || codeOf(r.Err()) != protocolv1.ErrorCode_ERROR_CODE_INTERNAL || !strings.Contains(r.Err().Error(), "preflight") {
		t.Errorf("a panic listing a preflight: %v", r.Err())
	}
	if r := RunList(t.Context(), func() []Check { return []Check{{Name: "a", Title: "A", Run: pass}} }); statuses(r) != "a=passed" {
		t.Errorf("RunList: %s", statuses(r))
	}
}

func TestRendered(t *testing.T) {
	it := Item{Title: "Use {schema}", Description: "On {host}.", Status: Failed, Message: "{schema} is {state}.", Params: Params{"schema": "public", "host": "db"},
		Failure: &Failure{Message: "{schema} is {state}.", Fix: "GRANT USAGE ON SCHEMA {schema}", Params: Params{"state": "locked", "schema": "other"}}}
	got := it.Rendered()
	if got.Title != "Use public" || got.Description != "On db." || got.Message != "public is locked." || got.Failure.Message != "public is locked." || got.Failure.Fix != "GRANT USAGE ON SCHEMA public" {
		t.Errorf("%+v %+v", got, got.Failure)
	}
	if it.Failure.Fix != "GRANT USAGE ON SCHEMA {schema}" || it.Title != "Use {schema}" {
		t.Error("Rendered changed the item")
	}
	// A value with braces is shown as it is, not filled in again.
	it = Item{Title: "Find {resource}", Status: Failed, Params: Params{"resource": "{host}", "host": "db"}, Failure: NotFound.Fail()}
	if got := it.Rendered(); got.Title != "Find {host}" || got.Message != "" || got.Failure.Message != "{host} doesn't exist." || got.Failure.Error() != "{host} doesn't exist." {
		t.Errorf("%q, %q, %q", got.Title, got.Failure.Message, got.Failure.Error())
	}
}
