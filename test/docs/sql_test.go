package docs

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The functions SQL transforms can call, as the engine records them, and
// the reference page that lists them.
const (
	functionsFile = "../../crates/sql/functions.txt"
	sqlPage       = "reference/sql.md"
)

type sqlFunction struct {
	aliases       []string
	deterministic bool
}

// recordedFunctions reads functions.txt: a line per function, its name,
// then its aliases in parentheses, then "not deterministic" if it isn't.
func recordedFunctions(t *testing.T) map[string]sqlFunction {
	b, err := os.ReadFile(functionsFile)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]sqlFunction{}
	for l := range strings.Lines(string(b)) {
		l = strings.TrimSpace(l)
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		f := sqlFunction{deterministic: true}
		if rest, ok := strings.CutSuffix(l, " not deterministic"); ok {
			l, f.deterministic = rest, false
		}
		name, aliases, _ := strings.Cut(l, " ")
		if aliases != "" {
			f.aliases = strings.Split(strings.Trim(aliases, "()"), ", ")
		}
		out[name] = f
	}
	return out
}

// TestSQLFunctions checks that the SQL reference's function tables list
// every function functions.txt records, with its aliases and whether it's
// deterministic, and nothing else.
func TestSQLFunctions(t *testing.T) {
	want := recordedFunctions(t)
	if len(want) == 0 {
		t.Fatalf("%s lists no functions", functionsFile)
	}
	b, err := os.ReadFile(filepath.Join(root, sqlPage))
	if err != nil {
		t.Fatal(err)
	}
	// The rows of tables whose header starts | Function | Other names |.
	got := map[string]sqlFunction{}
	inTable := false
	for l := range strings.Lines(string(b)) {
		l = strings.TrimSpace(l)
		switch {
		case strings.HasPrefix(l, "| Function | Other names |"):
			inTable = true
			continue
		case !strings.HasPrefix(l, "|"):
			inTable = false
			continue
		case !inTable || strings.HasPrefix(l, "|---"):
			continue
		}
		cells := strings.Split(strings.Trim(l, "|"), "|")
		if len(cells) != 3 {
			t.Errorf("%s: a function's row has %d cells, want 3: %s", sqlPage, len(cells), l)
			continue
		}
		name := strings.Trim(strings.TrimSpace(cells[0]), "`")
		if _, dup := got[name]; dup {
			t.Errorf("%s lists %s twice", sqlPage, name)
		}
		f := sqlFunction{deterministic: !strings.Contains(cells[2], "Not deterministic")}
		for _, m := range codeSpan.FindAllStringSubmatch(cells[1], -1) {
			f.aliases = append(f.aliases, m[1])
		}
		got[name] = f
	}
	for name, w := range want {
		g, ok := got[name]
		switch {
		case !ok:
			t.Errorf("%s has no row for %s, which %s records", sqlPage, name, functionsFile)
		case !slices.Equal(g.aliases, w.aliases):
			t.Errorf("%s: %s's other names are %v, want %v", sqlPage, name, g.aliases, w.aliases)
		case g.deterministic != w.deterministic:
			t.Errorf("%s: %s says it's deterministic: %t, want %t", sqlPage, name, g.deterministic, w.deterministic)
		}
	}
	for name := range got {
		if _, ok := want[name]; !ok {
			t.Errorf("%s lists %s, which %s doesn't record", sqlPage, name, functionsFile)
		}
	}
}
