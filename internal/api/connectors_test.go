package api

import (
	"testing"

	"github.com/unsuitable001/openetl/internal/store/db"
)

func TestPickVersion(t *testing.T) {
	rows := []db.ConnectorVersion{
		{Version: "0.9.0", Digest: "sha256:09"},
		{Version: "0.10.0", Digest: "sha256:10"},
		{Version: "1.0.0-rc.1", Digest: "sha256:rc"},
		{Version: "0.2.0", Digest: "sha256:02"},
	}
	for ref, want := range map[string]string{
		"":           "0.10.0",
		"0.9.0":      "0.9.0",
		"1.0.0-rc.1": "1.0.0-rc.1",
		"sha256:02":  "0.2.0",
	} {
		if got := pickVersion(rows, ref); got == nil || got.Version != want {
			t.Errorf("pickVersion(%q) = %v, want %s", ref, got, want)
		}
	}
	for _, ref := range []string{"1.0.0", "sha256:11"} {
		if got := pickVersion(rows, ref); got != nil {
			t.Errorf("pickVersion(%q) = %v, want none", ref, got)
		}
	}
	if got := pickVersion(rows[2:3], ""); got == nil || got.Version != "1.0.0-rc.1" {
		t.Errorf("only a prerelease: %v", got)
	}
	if got := pickVersion(nil, ""); got != nil {
		t.Errorf("no versions: %v", got)
	}
}
