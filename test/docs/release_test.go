package docs

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// A release's version lives only in its tag. These name a release of this
// repository; docs pages write {{version}} instead, which the docs site fills
// in, and the release's own files are stamped when it's built.
var releaseRefs = []*regexp.Regexp{
	regexp.MustCompile(`ghcr\.io/openetl-io/openetl:` + version),
	regexp.MustCompile(`github(?:usercontent)?\.com/openetl-io/openetl/(?:releases/download/)?v` + version),
	regexp.MustCompile(`(?i)\bopenetl ` + version),
	regexp.MustCompile(`release is ` + version),
	regexp.MustCompile(`openetl-io/openetl//deploy/[a-z/]+\?ref=v` + version),
	regexp.MustCompile(`openetl/sdk/go@v` + version),
	regexp.MustCompile(`openetl[-_](?:spark|flink)-` + version + `[.-]`),
	regexp.MustCompile(`newTag: ` + version),
	regexp.MustCompile(`OPENETL_VERSION(?:=|:-)v?` + version),
	regexp.MustCompile(`--branch v` + version),
	regexp.MustCompile(`-Dopenetl\.version=` + version),
}

// A version's characters, as semantic versions have them.
const version = `[0-9][0-9A-Za-z.+-]*`

// What users install from, and where it's described.
var releaseFiles = []string{"../../README.md", "../../CONTRIBUTING.md", "../../install.sh", "../../deploy", "../../examples", root}

func TestNoPinnedRelease(t *testing.T) {
	walk(t, func(path string, line int, text string) {
		for _, re := range releaseRefs {
			for _, m := range re.FindAllString(text, -1) {
				t.Errorf("%s:%d: %q names a release; write {{version}} in docs, and leave other files without one", path, line, m)
			}
		}
	})
}

// An official connector's version, which goes stale as the connector is
// released on its own. A reference without one applies the newest.
var connectorRefs = []*regexp.Regexp{
	regexp.MustCompile(`ghcr\.io/openetl-io/openetl-connectors/[a-z0-9-]+:` + version),
	regexp.MustCompile(`github\.com/openetl-io/openetl-connectors(?:/[a-z0-9-]+)?@` + version),
}

func TestNoPinnedConnector(t *testing.T) {
	walk(t, func(path string, line int, text string) {
		for _, re := range connectorRefs {
			for _, m := range re.FindAllString(text, -1) {
				t.Errorf("%s:%d: %q pins a connector's version; leave it out, and connector apply takes the newest", path, line, m)
			}
		}
	})
}

var placeholder = regexp.MustCompile(`\{\{[^{}]*[Vv]ersion[^{}]*\}\}`)

// The site fills in {{version}} exactly, and leaves {{ version }} or
// {{.Version}} as they are.
func TestVersionPlaceholder(t *testing.T) {
	walk(t, func(path string, line int, text string) {
		for _, m := range placeholder.FindAllString(text, -1) {
			if m != "{{version}}" {
				t.Errorf("%s:%d: %q: the docs site fills in {{version}} only", path, line, m)
			}
		}
	})
}

// walk calls f with each line of the text files in releaseFiles.
func walk(t *testing.T, f func(path string, line int, text string)) {
	t.Helper()
	for _, top := range releaseFiles {
		err := filepath.WalkDir(top, func(path string, d fs.DirEntry, err error) error {
			if err != nil || d.IsDir() {
				return err
			}
			switch filepath.Ext(path) {
			case ".md", ".sh", ".yml", ".yaml", ".json", ".xml", ".toml", ".py", "":
			default:
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			for i, text := range strings.Split(string(b), "\n") {
				f(path, i+1, text)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}
