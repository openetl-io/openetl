package events

import "testing"

func TestPatterns(t *testing.T) {
	for _, p := range []string{"*", "sync.*", "sync.failed", "*.lagging", "schema.*"} {
		if err := CheckPattern(p); err != nil {
			t.Errorf("%q: %v", p, err)
		}
	}
	for _, p := range []string{"", "sync", "incident.*", "sync.faild", "[", "Sync.*"} {
		if err := CheckPattern(p); err == nil {
			t.Errorf("%q passed", p)
		}
	}
	for _, c := range []struct {
		patterns []string
		typ      string
		want     bool
	}{
		{[]string{"sync.*"}, SyncFailed, true},
		{[]string{"sync.*"}, SchemaChanged, false},
		{[]string{"schema.blocked", "pipeline.*"}, PipelineLagging, true},
		{[]string{"*"}, WebhookTest, true},
		{nil, SyncFailed, false},
	} {
		if got := Matches(c.patterns, c.typ); got != c.want {
			t.Errorf("Matches(%q, %s) = %v", c.patterns, c.typ, got)
		}
	}
}
