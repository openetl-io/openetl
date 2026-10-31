package activities

import (
	"path/filepath"
	"testing"
)

func TestQuarantineURL(t *testing.T) {
	abs, _ := filepath.Abs("cp")
	for storage, want := range map[string]string{
		"/var/openetl":                          "file:///var/openetl/o/p/_quarantine",
		"cp":                                    "file://" + filepath.ToSlash(abs) + "/o/p/_quarantine",
		"s3://b?region=r&endpoint=http://h:1":   "s3://b/o/p/_quarantine?region=r&endpoint=http://h:1",
		"s3://b/base?endpoint=https://s3.x.com": "s3://b/base/o/p/_quarantine?endpoint=https://s3.x.com",
	} {
		got, err := quarantineURL(storage, "o", "p")
		if err != nil || got != want {
			t.Errorf("quarantineURL(%q) = %q, %v; want %q", storage, got, err, want)
		}
	}
}
