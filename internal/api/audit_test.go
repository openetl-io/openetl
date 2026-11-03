package api

import (
	"context"
	"slices"
	"strings"
	"testing"

	apiv1 "github.com/unsuitable001/openetl/gen/openetl/api/v1"
)

// Reads aren't audited, everything else on the public API is, and so are
// the runner calls that get secrets' values.
func TestAudited(t *testing.T) {
	for proc, r := range rules {
		name := proc[strings.LastIndex(proc, "/")+1:]
		want := !strings.HasPrefix(name, "Get") && !strings.HasPrefix(name, "List") && !strings.HasPrefix(name, "Read")
		if r.runner {
			want = name == "GetSyncSpec" || name == "GetConnectorConfig"
		}
		// Sign-in's handlers write their own entries.
		if r.public() {
			want = false
		}
		if got := audited(proc, r); got != want {
			t.Errorf("%s: audited %v, want %v", proc, got, want)
		}
	}
}

func TestNamed(t *testing.T) {
	cases := []struct {
		req  any
		want string
	}{
		{&apiv1.SyncPipelineRequest{Name: "workspaces/w/pipelines/p"}, "workspaces/w/pipelines/p"},
		{&apiv1.CreateKeyRequest{Parent: "principals/p"}, "principals/p"},
		{&apiv1.UpdateSecretRequest{Secret: &apiv1.Secret{Name: "workspaces/w/secrets/s", Value: "v"}}, "workspaces/w/secrets/s"},
		{&apiv1.ApplyRequest{Workspace: "workspaces/w"}, ""},
	}
	for _, c := range cases {
		if got := named(c.req); got != c.want {
			t.Errorf("%T: %q, want %q", c.req, got, c.want)
		}
	}
}

func TestEntry(t *testing.T) {
	e := &entry{}
	ctx := context.WithValue(context.Background(), entryKey{}, e)
	audit(ctx).touched("a", "", "b", "a")
	audit(ctx).set("k", 1)
	audit(context.Background()).touched("dropped")
	if !slices.Equal(e.resources, []string{"a", "b"}) || e.details["k"] != 1 {
		t.Errorf("entry %+v", e)
	}
	if got := truncate("abé", 3); got != "ab" {
		t.Errorf("truncate cut a rune: %q", got)
	}
}
