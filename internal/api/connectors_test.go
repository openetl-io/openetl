package api

import (
	"testing"

	"github.com/unsuitable001/openetl/internal/store/db"
)

func TestPickVersion(t *testing.T) {
	rows := []db.ListConnectorRolesRow{
		{Version: "0.9.0", Digest: "sha256:09"},
		{Version: "0.10.0", Digest: "sha256:10"},
		{Version: "1.0.0-rc.1", Digest: "sha256:rc"},
		{Version: "0.2.0", Digest: "sha256:02"},
		{Version: "1.2.3", Digest: "sha256:123"},
		{Version: "1.10.0", Digest: "sha256:1100"},
		{Version: "1.2.10", Digest: "sha256:1210"},
		{Version: "2.0.0-beta.1", Digest: "sha256:2b"},
	}
	for ref, want := range map[string]string{
		"":           "1.10.0",
		"0.9.0":      "0.9.0",
		"1.0.0-rc.1": "1.0.0-rc.1",
		"sha256:02":  "0.2.0",
		"0":          "0.10.0",
		"0.9":        "0.9.0",
		"1":          "1.10.0",
		"1.2":        "1.2.10",
	} {
		if got, err := pickVersion(rows, ref); err != nil || got == nil || got.Version != want {
			t.Errorf("pickVersion(%q) = %v, %v, want %s", ref, got, err, want)
		}
	}
	for _, ref := range []string{"1.0.0", "sha256:11", "2", "1.3", "3"} {
		if got, err := pickVersion(rows, ref); err != nil || got != nil {
			t.Errorf("pickVersion(%q) = %v, %v, want none", ref, got, err)
		}
	}
	for _, ref := range []string{"v1", "1.x", "latest", "1.2.3.4", "01"} {
		if _, err := pickVersion(rows, ref); err == nil {
			t.Errorf("pickVersion(%q) took it", ref)
		}
	}
	if got, _ := pickVersion(rows[2:3], ""); got == nil || got.Version != "1.0.0-rc.1" {
		t.Errorf("only a prerelease: %v", got)
	}
	if got, _ := pickVersion(nil, ""); got != nil {
		t.Errorf("no versions: %v", got)
	}
}
