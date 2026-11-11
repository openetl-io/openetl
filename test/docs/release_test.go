package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The release the docs install: install.sh's default, which every pinned
// reference to a release of this repository in docs/, the README and the
// deploy files' defaults must name, so that a release changes them together.
var (
	installDefault = regexp.MustCompile(`(?m)^version=\$\{OPENETL_VERSION:-([^}]+)\}$`)
	releaseRefs    = []*regexp.Regexp{
		regexp.MustCompile(`ghcr\.io/openetl-io/openetl:(` + version + `)`),
		regexp.MustCompile(`github(?:usercontent)?\.com/openetl-io/openetl/(?:releases/download/)?v(` + version + `)/`),
		regexp.MustCompile(`openetl-io/openetl//deploy/[a-z/]+\?ref=v(` + version + `)`),
		regexp.MustCompile(`openetl/sdk/go@v(` + version + `)`),
		regexp.MustCompile(`openetl-(?:spark|flink)-(` + version + `?)\.jar`),
		regexp.MustCompile(`newTag: (` + version + `)`),
		regexp.MustCompile(`OPENETL_VERSION=v?(` + version + `)`),
	}
)

// A version's characters, as semantic versions have them.
const version = `[0-9][0-9A-Za-z.+-]*`

func TestReleaseVersion(t *testing.T) {
	script, err := os.ReadFile("../../install.sh")
	if err != nil {
		t.Fatal(err)
	}
	m := installDefault.FindSubmatch(script)
	if m == nil {
		t.Fatal("install.sh has no line version=${OPENETL_VERSION:-VERSION}")
	}
	want := string(m[1])
	files := []string{"../../README.md", "../../deploy/compose/compose.yml", "../../deploy/kubernetes/kustomization.yaml"}
	err = filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".md") {
			files = append(files, path)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(b), "\n") {
			for _, re := range releaseRefs {
				for _, m := range re.FindAllStringSubmatch(line, -1) {
					found++
					if m[1] != want {
						t.Errorf("%s:%d: %q names release %s, and install.sh installs %s", f, i+1, m[0], m[1], want)
					}
				}
			}
		}
	}
	if found == 0 {
		t.Error("no page names a release; the docs should say how to install one")
	}
}
