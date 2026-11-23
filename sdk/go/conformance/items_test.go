package conformance

import (
	"context"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/openetl-io/openetl/sdk/go/check"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

const (
	passed  = protocolv1.CheckStatus_CHECK_STATUS_PASSED
	warning = protocolv1.CheckStatus_CHECK_STATUS_WARNING
	failed  = protocolv1.CheckStatus_CHECK_STATUS_FAILED
	skipped = protocolv1.CheckStatus_CHECK_STATUS_SKIPPED
)

var itemSchema = []byte(`{"type": "object", "properties": {
	"host": {"type": "string"},
	"password": {"type": "string", "x-secret": true},
	"auth": {"$ref": "#/$defs/auth"},
	"login": {"$ref": "#/$defs/auth/oneOf/0"},
	"tables": {"type": "array", "items": {"type": "object", "properties": {"name": {"type": "string"}}}}},
	"dependentSchemas": {"host": {"properties": {"port": {"type": "integer"}}}},
	"$defs": {"auth": {"type": "object", "oneOf": [{"properties": {"token": {"type": "string"}}}]}}}`)

// wellFormed is an answer every status of which holds to the protocol.
func wellFormed() *protocolv1.CheckResponse {
	params := func() map[string]string { return map[string]string{"host": "db"} }
	authInvalid := protocolv1.ErrorCode_ERROR_CODE_AUTH_INVALID
	return &protocolv1.CheckResponse{
		Error: &protocolv1.Error{Code: authInvalid, Message: "Sign in to db: db said no: pq: auth failed"},
		Items: []*protocolv1.CheckItem{
			{Name: "connect", Kind: protocolv1.CheckKind_CHECK_KIND_CONNECTION, Title: "Reach {host}", Status: passed, Message: "PostgreSQL 17", Params: params(), Field: "host"},
			{Name: "auth", Kind: protocolv1.CheckKind_CHECK_KIND_AUTHENTICATION, Title: "Sign in to {host}", Description: "{host} takes the token.", Params: params(),
				Status: failed, Message: "{host} said no.", Error: &protocolv1.Error{Code: authInvalid, Message: "db said no."}, Reason: "credentials_rejected",
				Fix: "Ask {host}.", Detail: "pq: auth failed", Field: "auth.token"},
			{Name: "quota", Title: "Room on {host}", Params: params(), Status: warning, Message: "{host} is full.", Reason: "quota_exceeded",
				Error: &protocolv1.Error{Code: protocolv1.ErrorCode_ERROR_CODE_RESOURCE_EXHAUSTED, Message: "db is full."}},
			{Name: "schema.usage", Kind: protocolv1.CheckKind_CHECK_KIND_PERMISSION, Title: "Use the schema", Status: skipped, Message: "needs auth, which failed"},
		},
	}
}

// Each break of the protocol fails, saying what's wrong; a placeholder
// without a param is only flagged; an answer without items and a
// well-formed one pass.
func TestCheckItems(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(r *protocolv1.CheckResponse)
		// What the log says; "" when nothing is, and a flag doesn't fail.
		says string
	}{
		{"well formed", func(*protocolv1.CheckResponse) {}, ""},
		// The checks couldn't run at all.
		{"an error and no items", func(r *protocolv1.CheckResponse) { r.Items = nil }, ""},
		{"no name", func(r *protocolv1.CheckResponse) { r.Items[0].Name = "" }, "with x, item 1 has no name"},
		{"malformed name", func(r *protocolv1.CheckResponse) { r.Items[0].Name = "Connect" },
			`with x, item "Connect" (Reach db)'s name isn't lowercase letters and digits in words joined by`},
		{"repeated name", func(r *protocolv1.CheckResponse) { r.Items[2].Name = "connect" },
			`with x, items 1 and 3 have the same name "connect": each needs its own`},
		{"no title", func(r *protocolv1.CheckResponse) { r.Items[0].Title = "" }, `with x, item "connect" () has no title`},
		{"unknown kind", func(r *protocolv1.CheckResponse) { r.Items[0].Kind = 99 }, `with x, item "connect" (Reach db) has the kind 99, which CheckKind doesn't have`},
		{"no status", func(r *protocolv1.CheckResponse) { r.Items[0].Status = 0 }, `with x, item "connect" (Reach db) has no status`},
		{"unknown status", func(r *protocolv1.CheckResponse) { r.Items[0].Status = 7 },
			`with x, item "connect" (Reach db) has the status 7, which CheckStatus doesn't have: a host counts it as FAILED`},
		{"malformed reason", func(r *protocolv1.CheckResponse) { r.Items[2].Reason = "quota-exceeded" },
			`with x, item "quota" (Room on db)'s reason "quota-exceeded" isn't lowercase words joined by "_"`},
		{"failed without an error", func(r *protocolv1.CheckResponse) { r.Items[1].Error = nil },
			`with x, item "auth" (Sign in to db) failed without an error`},
		{"passed with an error", func(r *protocolv1.CheckResponse) {
			r.Items[0].Error = &protocolv1.Error{Code: 1, Message: "PostgreSQL 17"}
		},
			`with x, item "connect" (Reach db) passed with an error: only a failed or warning item has one`},
		{"skipped with an error", func(r *protocolv1.CheckResponse) {
			r.Items[3].Error = &protocolv1.Error{Code: 1, Message: "needs auth, which failed"}
		}, `with x, item "schema.usage" (Use the schema) was skipped with an error`},
		{"an error without a code", func(r *protocolv1.CheckResponse) { r.Items[2].Error.Code = 0 },
			`with x, item "quota" (Room on db)'s error has no code`},
		{"an error's message with the detail", func(r *protocolv1.CheckResponse) { r.Items[1].Error.Message = "db said no: pq: auth failed" },
			`with x, item "auth" (Sign in to db)'s error message is "db said no: pq: auth failed", not its message with its params filled in, "db said no."`},
		{"an error's message unfilled", func(r *protocolv1.CheckResponse) { r.Items[2].Error.Message = "{host} is full." },
			`with x, item "quota" (Room on db)'s error message is "{host} is full."`},
		// The checks couldn't finish: those they didn't reach are skipped.
		{"an error with a skipped item and none failed", func(r *protocolv1.CheckResponse) {
			r.Items[1].Status, r.Items[1].Error = passed, nil
		}, ""},
		{"an error with every item passed or warned", func(r *protocolv1.CheckResponse) {
			r.Items[1].Status, r.Items[1].Error, r.Items[3].Status = passed, nil, passed
		}, "with x, the response has an error (ERROR_CODE_AUTH_INVALID: Sign in to db: db said no: pq: auth failed) but every item passed or warned: set it when an item failed, or when the checks couldn't finish"},
		{"a failed item without an error", func(r *protocolv1.CheckResponse) { r.Error = nil },
			`with x, item "auth" (Sign in to db) failed but the response has no error`},
		{"an unknown status without an error", func(r *protocolv1.CheckResponse) { r.Items[0].Status, r.Error = 7, nil },
			`with x, item "connect" (Reach db) failed but the response has no error`},
		{"an error of another code", func(r *protocolv1.CheckResponse) { r.Error.Code = protocolv1.ErrorCode_ERROR_CODE_TRANSIENT },
			`with x, the response's error is ERROR_CODE_TRANSIENT, not ERROR_CODE_AUTH_INVALID of item "auth" (Sign in to db), the first that failed`},
		{"an error without the title", func(r *protocolv1.CheckResponse) { r.Error.Message = "db said no." },
			`with x, the response's error message "db said no." isn't "Sign in to db: db said no: pq: auth failed", the title, message and detail of item "auth" (Sign in to db)`},
		{"an error of the title alone", func(r *protocolv1.CheckResponse) { r.Error.Message = "Sign in to db" },
			`with x, the response's error message "Sign in to db" isn't "Sign in to db: db said no: pq: auth failed"`},
		{"an error that keeps the message's period", func(r *protocolv1.CheckResponse) { r.Error.Message = "Sign in to db: db said no.: pq: auth failed" }, ""},
		{"an error of an item without a detail", func(r *protocolv1.CheckResponse) {
			r.Items[1].Detail, r.Error.Message = "", "Sign in to db: db said no."
		}, ""},
		{"a field in an array's items", func(r *protocolv1.CheckResponse) { r.Items[1].Field = "tables.0.name" }, ""},
		{"a field of a dependent schema", func(r *protocolv1.CheckResponse) { r.Items[1].Field = "port" }, ""},
		{"a secret in a failed item's title", func(r *protocolv1.CheckResponse) { r.Items[1].Title = "Sign in as hunter22-s3cr3t" },
			"with x, item 2's title shows the value of a secret in the config"},
		{"a secret in an item's error", func(r *protocolv1.CheckResponse) { r.Items[1].Error.Message = "hunter22-s3cr3t said no." },
			"with x, item 2's error shows the value of a secret in the config"},
		{"a placeholder without a param in a fix", func(r *protocolv1.CheckResponse) { r.Items[1].Fix = "Raise {limit}." },
			`flag: with x, item "auth" (Sign in to db)'s fix uses {limit} without a param of that name`},
		{"what looks like a placeholder in a description", func(r *protocolv1.CheckResponse) { r.Items[1].Description = "{Host} takes it." },
			`with x, item "auth" (Sign in to db)'s description has {Host}, which looks like a placeholder but isn't one`},
		{"what looks like a placeholder in a title", func(r *protocolv1.CheckResponse) { r.Items[0].Title = "Reach {db-host}" },
			`with x, item "connect" (Reach {db-host})'s title has {db-host}, which looks like a placeholder but isn't one`},
		{"a field outside the schema", func(r *protocolv1.CheckResponse) { r.Items[1].Field = "auth.user" },
			`with x, item "auth" (Sign in to db)'s field "auth.user" isn't a property of the config schema`},
		{"a secret in a param", func(r *protocolv1.CheckResponse) { r.Items[0].Params["dsn"] = "pg://etl:hunter22-s3cr3t@db" },
			"with x, item 1's param shows the value of a secret in the config"},
		{"a secret in a detail", func(r *protocolv1.CheckResponse) { r.Items[1].Detail = "password hunter22-s3cr3t rejected" },
			"with x, item 2's detail shows the value of a secret in the config"},
		{"a secret in the response's error", func(r *protocolv1.CheckResponse) { r.Error.Message += " (hunter22-s3cr3t)" },
			"with x, the response's error shows the value of a secret in the config"},
		{"a secret in an answer without items", func(r *protocolv1.CheckResponse) { r.Items, r.Error.Message = nil, "hunter22-s3cr3t rejected" },
			"with x, the response's error shows the value of a secret in the config"},
	}
	fields := fieldPaths(itemSchema)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := wellFormed()
			tc.mutate(resp)
			r := &reporter{ctx: t.Context()}
			checkItems(r, "with x", resp, []string{"hunter22-s3cr3t"}, fields)
			said := slices.ContainsFunc(r.log, func(l string) bool { return strings.HasPrefix(l, tc.says) })
			switch {
			case tc.says == "" && (r.failed || len(r.log) > 0):
				t.Errorf("failed %v, logged %q; want neither", r.failed, r.log)
			case tc.says != "" && (!said || r.failed == strings.HasPrefix(tc.says, "flag:")):
				t.Errorf("failed %v, logged %q; want it to say %q, and fail unless that's a flag", r.failed, r.log, tc.says)
			}
			if slices.ContainsFunc(r.log, func(l string) bool {
				return strings.Contains(l, "hunter22-s3cr3t") && !strings.Contains(tc.says, "hunter22-s3cr3t")
			}) {
				t.Errorf("the log shows the secret: %q", r.log)
			}
		})
	}
}

// The SDK's answers when its checks couldn't finish hold to the protocol:
// one whose time ran out between two checks, so the second is skipped with
// none failed, and one whose list Validate refuses, with no items. A check
// can't pass once the time is up, so an advisory one uses it up, and warns.
func TestUnfinishedChecks(t *testing.T) {
	pass := func(context.Context, *check.Probe) error { return nil }
	for _, tc := range []struct {
		name     string
		checks   []check.Check
		statuses []protocolv1.CheckStatus
	}{
		{"out of time between two checks", []check.Check{
			{Name: "connect", Title: "Reach the server", Run: pass},
			{Name: "lag", Title: "Keep up", Advisory: true, Run: func(ctx context.Context, _ *check.Probe) error {
				<-ctx.Done()
				return ctx.Err()
			}},
			{Name: "write", Title: "Write a row", Run: pass},
		}, []protocolv1.CheckStatus{passed, warning, skipped}},
		{"a list Validate refuses", []check.Check{
			{Name: "connect", Title: "Reach the server", Run: pass},
			{Name: "connect", Title: "Reach it again", Run: pass},
		}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
			defer cancel()
			items, rerr := check.Run(ctx, tc.checks).Proto()
			var statuses []protocolv1.CheckStatus
			for _, it := range items {
				statuses = append(statuses, it.GetStatus())
			}
			if !slices.Equal(statuses, tc.statuses) || rerr == nil {
				t.Fatalf("the run gave %v and the error %v; want %v and an error", statuses, rerr, tc.statuses)
			}
			r := &reporter{ctx: t.Context()}
			checkItems(r, "with x", &protocolv1.CheckResponse{Items: items, Error: rerr}, nil, nil)
			if r.failed || len(r.log) > 0 {
				t.Errorf("failed %v, logged %q; want neither", r.failed, r.log)
			}
		})
	}
}

// With the good config, an answer with an error fails, even with no item
// failed.
func TestPassed(t *testing.T) {
	c := &common{spec: &protocolv1.SpecResponse{ConfigSchema: itemSchema}, config: []byte(`{"password": "hunter22-s3cr3t"}`)}
	for _, tc := range []struct {
		name   string
		mutate func(r *protocolv1.CheckResponse)
		says   string
	}{
		{"passed", func(r *protocolv1.CheckResponse) { r.Error, r.Items = nil, r.Items[:1] }, ""},
		{"a failed item", func(*protocolv1.CheckResponse) {},
			`with the good config, Check's item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID: db said no: pq: auth failed`},
		{"out of time, with the rest skipped", func(r *protocolv1.CheckResponse) { r.Items[1].Status, r.Items[1].Error = passed, nil },
			"Check failed with the good config: ERROR_CODE_AUTH_INVALID: Sign in to db"},
		{"an error and no items", func(r *protocolv1.CheckResponse) { r.Items = nil },
			"Check failed with the good config: ERROR_CODE_AUTH_INVALID: Sign in to db"},
		{"a secret in a failed item's detail", func(r *protocolv1.CheckResponse) { r.Items[1].Detail = "hunter22-s3cr3t rejected" },
			`with the good config, Check's item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID: ` + hidden},
		{"a secret in the error, with no items", func(r *protocolv1.CheckResponse) { r.Items, r.Error.Message = nil, "hunter22-s3cr3t rejected" },
			"Check failed with the good config: ERROR_CODE_AUTH_INVALID: " + hidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := wellFormed()
			tc.mutate(resp)
			r := &reporter{ctx: t.Context()}
			c.passed(r, "Check", resp)
			if said := slices.ContainsFunc(r.log, func(l string) bool { return strings.HasPrefix(l, tc.says) }); r.failed != (tc.says != "") || tc.says != "" && !said {
				t.Errorf("failed %v, logged %q; want %q", r.failed, r.log, tc.says)
			}
			if slices.ContainsFunc(r.log, func(l string) bool { return strings.Contains(l, "hunter22-s3cr3t") }) {
				t.Errorf("the log shows the secret: %q", r.log)
			}
		})
	}
}

func TestFieldPaths(t *testing.T) {
	got := fieldPaths(itemSchema)
	for _, p := range []string{"host", "password", "auth", "auth.token", "login", "login.token", "tables", "tables.name", "port"} {
		if !got[p] {
			t.Errorf("%s isn't among %v", p, got)
		}
	}
	if fieldPaths(nil) != nil || fieldPaths([]byte(`{"type": "object"}`)) != nil {
		t.Error("a schema without properties has field paths")
	}
}

func TestFailedWith(t *testing.T) {
	transient := []protocolv1.ErrorCode{protocolv1.ErrorCode_ERROR_CODE_TRANSIENT}
	c := &common{}
	for _, tc := range []struct {
		name   string
		mutate func(r *protocolv1.CheckResponse)
		says   string
	}{
		{"the right code", func(r *protocolv1.CheckResponse) { r.Error.Code, r.Items[1].Error.Code = transient[0], transient[0] }, ""},
		{"another code", func(*protocolv1.CheckResponse) {},
			`with x, item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]`},
		// Without the response's error, the first failed item's code is
		// compared.
		{"another code without the response's error", func(r *protocolv1.CheckResponse) { r.Error = nil },
			`with x, item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]: db said no: pq: auth failed`},
		{"the right code without the response's error", func(r *protocolv1.CheckResponse) { r.Error, r.Items[1].Error.Code = nil, transient[0] }, ""},
		{"passed", func(r *protocolv1.CheckResponse) { r.Error, r.Items = nil, r.Items[:1] }, "with x, Check passed"},
		{"an error alone", func(r *protocolv1.CheckResponse) { r.Items = nil }, "with x, Check failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]"},
		{"a secret in the item's detail, without the response's error", func(r *protocolv1.CheckResponse) { r.Error, r.Items[1].Detail = nil, "hunter22-s3cr3t rejected" },
			`with x, item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]: ` + hidden},
		{"a secret in the response's error", func(r *protocolv1.CheckResponse) { r.Error.Message = "hunter22-s3cr3t rejected" },
			`with x, item "auth" (Sign in to db) failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]: ` + hidden},
		{"a secret in the error alone", func(r *protocolv1.CheckResponse) { r.Items, r.Error.Message = nil, "hunter22-s3cr3t rejected" },
			"with x, Check failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]: " + hidden},
		{"a secret in the item's title", func(r *protocolv1.CheckResponse) { r.Items[1].Title = "Sign in as hunter22-s3cr3t" },
			"with x, item 2 failed with ERROR_CODE_AUTH_INVALID, not [ERROR_CODE_TRANSIENT]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			resp := wellFormed()
			tc.mutate(resp)
			r := &reporter{ctx: t.Context()}
			c.failedWith(r, "with x", resp, transient, []string{"hunter22-s3cr3t"})
			if said := slices.ContainsFunc(r.log, func(l string) bool { return strings.HasPrefix(l, tc.says) }); r.failed != (tc.says != "") || tc.says != "" && !said {
				t.Errorf("failed %v, logged %q; want %q", r.failed, r.log, tc.says)
			}
			if slices.ContainsFunc(r.log, func(l string) bool { return strings.Contains(l, "hunter22-s3cr3t") }) {
				t.Errorf("the log shows the secret: %q", r.log)
			}
		})
	}
}

// A secret too short, or one another value holds at any depth, is one the
// leak check can't test; an empty one, and a distinctive one, are tested.
func TestUntestableSecretValues(t *testing.T) {
	short := "is shorter than 12 characters"
	held := "appears in another of the config's values"
	for _, tc := range []struct {
		config string
		want   map[string]string
	}{
		{`{"password": "hunter22-s3cr3t"}`, nil},
		{`{"password": ""}`, nil},
		{`{"host": "db"}`, nil},
		{`{"password": "openetl"}`, map[string]string{"password": short}},
		{`{"password": "ünïcödé-12ch"}`, nil},
		{`{"password": "hunter22-s3cr3t", "host": "hunter22-s3cr3t.db"}`, map[string]string{"password": held}},
		{`{"password": "hunter22-s3cr3t", "tables": [{"name": "x-hunter22-s3cr3t"}]}`, map[string]string{"password": held}},
		{`{"password": "123456789012", "port": 1234567890123}`, map[string]string{"password": held}},
	} {
		if got := untestableSecrets(itemSchema, []byte(tc.config)); !maps.Equal(got, tc.want) {
			t.Errorf("%s: %v, want %v", tc.config, got, tc.want)
		}
	}
}

// A check that answers without the spec check before it, as go test -run
// can, refuses a secret it can't test itself, and only once a suite; its
// leak check leaves that secret out.
func TestCheckAnswerRefusesUntestableSecrets(t *testing.T) {
	c := &common{spec: &protocolv1.SpecResponse{ConfigSchema: itemSchema}, config: []byte(`{"password": "openetl"}`)}
	resp := wellFormed()
	resp.Items[1].Detail = "wrote _openetl/check-1"
	resp.Error.Message = "Sign in to db: db said no: wrote _openetl/check-1"
	for i, want := range []int{1, 0} {
		r := &reporter{ctx: t.Context()}
		c.checkAnswer(r, "with x", c.config, resp)
		if len(r.log) != want || want > 0 && !strings.HasPrefix(r.log[0], "Config's secret config.password is shorter than 12 characters") {
			t.Errorf("answer %d logged %q", i+1, r.log)
		}
	}
}
