package heal

import (
	"strings"
	"testing"

	"github.com/google/uuid"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/platform/errs"
)

func TestNormalize(t *testing.T) {
	for in, want := range map[string]string{
		`INTERNAL: write epoch 12 of 9f86d081-884c-4d63-a5d6-1d14f8b9c1a2: dial tcp 10.0.0.7:5432: connection refused`: "internal: write epoch <n> of <id>: dial tcp <n>.<n>.<n>.<n>:<n>: connection refused",
		`DATA_INVALID: column "price" got 'abc'`:       "data_invalid: column <value> got <value>",
		`digest sha256:0a1b2c3d4e5f6a7b8c9d not found`: "digest sha<n>:<hex> not found",
		"  two\n lines  ":           "two lines",
		"deadbeefcafe stays a word": "deadbeefcafe stays a word",
	} {
		if got := Normalize(in); got != want {
			t.Errorf("Normalize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestFingerprint(t *testing.T) {
	a := Fingerprint(SyncFailed, "INTERNAL", "write epoch 12: connection reset", "sha256:s", "sha256:d")
	if b := Fingerprint(SyncFailed, "INTERNAL", "write epoch 13: connection reset", "sha256:s", "sha256:d"); a != b {
		t.Error("the same fault with another epoch has another fingerprint")
	}
	for what, other := range map[string]string{
		"error":       Fingerprint(SyncFailed, "INTERNAL", "write epoch 12: disk full", "sha256:s", "sha256:d"),
		"code":        Fingerprint(SyncFailed, "TRANSIENT", "write epoch 12: connection reset", "sha256:s", "sha256:d"),
		"source":      Fingerprint(SyncFailed, "INTERNAL", "write epoch 12: connection reset", "sha256:t", "sha256:d"),
		"destination": Fingerprint(SyncFailed, "INTERNAL", "write epoch 12: connection reset", "sha256:s", "sha256:e"),
		"class":       Fingerprint(Auth, "INTERNAL", "write epoch 12: connection reset", "sha256:s", "sha256:d"),
	} {
		if other == a {
			t.Errorf("another %s has the same fingerprint", what)
		}
	}
	if Fingerprint(Auth, "AUTH_INVALID", "user alice", "s", "d") != Fingerprint(Auth, "AUTH_INVALID", "role bob", "s", "d") {
		t.Error("an auth failure's message changes its fingerprint")
	}
	if Fingerprint(CrashLoop, "INTERNAL", "x", "s", "d") != Fingerprint(CrashLoop, "TRANSIENT", "y", "s", "d") {
		t.Error("a crash loop's latest error changes its fingerprint")
	}
}

func TestClassOf(t *testing.T) {
	for code, want := range map[errs.Code]Class{
		errs.AuthInvalid: Auth, errs.AuthExpired: Auth, errs.SchemaDrift: SchemaBlocked, errs.RateLimited: RateLimited,
		errs.PositionLost: PositionLost, errs.Internal: SyncFailed, errs.ConfigInvalid: SyncFailed,
	} {
		if got := ClassOf(code.String()); got != want {
			t.Errorf("ClassOf(%s) = %s, want %s", code, got, want)
		}
	}
	if ClassOf("") != SyncFailed || ClassOf("DBT_FAILED") != SyncFailed {
		t.Error("unknown codes should be sync_failed")
	}
}

func TestStreamOf(t *testing.T) {
	msg := "SCHEMA_DRIFT: engine: stream public.items: a new column price (decimal(10, 2)), which schema policy block doesn't take"
	if got := StreamOf(SchemaBlocked, msg); got != "public.items" {
		t.Errorf("StreamOf = %q", got)
	}
	if StreamOf(SyncFailed, msg) != "" {
		t.Error("only schema_blocked incidents are a stream's")
	}
}

// Every class has its own summary and next steps, which name the pipeline
// and the commands to run.
func TestDescribe(t *testing.T) {
	sync := uuid.MustParse("019a4a4e-0000-7000-8000-000000000001")
	tg := target{workspace: "analytics", pipeline: "shop", sourceSecrets: []string{"shop-dsn"}, mode: apiv1.HealMode_HEAL_MODE_UNSPECIFIED}
	seen := map[string]Class{}
	for _, c := range []Class{SyncFailed, Auth, SchemaBlocked, RateLimited, PositionLost, CrashLoop, Lagging, RetentionAtRisk, Quarantine} {
		o := Observation{Class: c, Code: "X", Stream: "items", Sync: &sync, Details: map[string]string{"used": "62%", "share": "4.0%"}}
		summary, next := describe(o, tg)
		if !strings.Contains(summary, "analytics/shop") || next == "" {
			t.Errorf("%s: summary %q, next steps %q", c, summary, next)
		}
		if other, ok := seen[summary]; ok {
			t.Errorf("%s and %s have the same summary", c, other)
		}
		seen[summary] = c
	}
	_, next := describe(Observation{Class: Auth, Code: "AUTH_INVALID"}, tg)
	if !strings.Contains(next, "openetl secret set shop-dsn") || !strings.Contains(next, "openetl resume --workspace analytics shop") {
		t.Errorf("auth's next steps %q", next)
	}
	_, next = describe(Observation{Class: SyncFailed, Sync: &sync}, target{workspace: "default", pipeline: "shop"})
	if !strings.Contains(next, "openetl logs --sync "+sync.String()+" shop") {
		t.Errorf("sync_failed's next steps %q", next)
	}
}
