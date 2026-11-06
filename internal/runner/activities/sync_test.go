package activities

import (
	"path/filepath"
	"testing"

	"github.com/unsuitable001/openetl/internal/checkpoint"
)

func TestQuarantineURL(t *testing.T) {
	abs, _ := filepath.Abs("cp")
	for storage, want := range map[string]string{
		"/var/openetl":                          "file:///var/openetl/o/p/_quarantine",
		"cp":                                    "file://" + filepath.ToSlash(abs) + "/o/p/_quarantine",
		"s3://b?region=r&endpoint=http://h:1":   "s3://b/o/p/_quarantine?region=r&endpoint=http://h:1",
		"s3://b/base?endpoint=https://s3.x.com": "s3://b/base/o/p/_quarantine?endpoint=https://s3.x.com",
		"gs://b":                                "gs://b/o/p/_quarantine",
		"azblob://c?account=a":                  "azblob://c/o/p/_quarantine?account=a",
	} {
		got, err := pipelineURL(storage, "o", "p", checkpoint.QuarantineDir)
		if err != nil || got != want {
			t.Errorf("pipelineURL(%q) = %q, %v; want %q", storage, got, err, want)
		}
	}
}
