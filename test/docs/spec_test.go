package docs

import (
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/spec"
)

// specPages are the reference's pages of the kinds internal/spec reads
// field by field.
var specPages = map[string]string{
	spec.KindPipeline:    "reference/specs/pipeline.md",
	spec.KindSource:      "reference/specs/source.md",
	spec.KindDestination: "reference/specs/destination.md",
}

// envelope is a document's fields outside its spec, which a spec page
// lists too.
var envelope = []string{"apiVersion", "kind", "metadata.name", "metadata.workspace", "spec"}

var codeSpan = regexp.MustCompile("`([^`]+)`")

// TestSpecFields checks that a spec page has a row for every field its kind
// takes and for nothing else, in its tables whose first column is Field,
// and that every document in its yaml blocks parses.
func TestSpecFields(t *testing.T) {
	for kind, file := range specPages {
		b, err := os.ReadFile(filepath.Join(root, file))
		if err != nil {
			t.Error(err)
			continue
		}
		lines := strings.Split(string(b), "\n")
		fields := append(spec.FieldPaths(kind), envelope...)
		rows := map[string]int{}
		for _, r := range fieldRows(lines) {
			if _, ok := rows[r.field]; ok {
				t.Errorf("%s:%d: %s has a second row", file, r.line, r.field)
			}
			rows[r.field] = r.line
			if !slices.Contains(fields, r.field) {
				t.Errorf("%s:%d: %s isn't a field of a %s document", file, r.line, r.field, kind)
			}
		}
		for _, f := range fields {
			if _, ok := rows[f]; !ok {
				t.Errorf("%s: no row for %s, which a %s document takes", file, f, kind)
			}
		}
		for _, d := range documents(lines, "yaml") {
			if _, err := spec.Parse([]byte(d.text)); err != nil {
				t.Errorf("%s:%d: %v", file, d.line, err)
			}
		}
	}
}

type row struct {
	field string
	line  int
}

// fieldRows are the code spans in the first column of each table whose
// header's first column is Field, outside code blocks.
func fieldRows(lines []string) []row {
	var rows []row
	fenced, table := false, false
	for i, l := range lines {
		switch {
		case strings.HasPrefix(l, "```"):
			fenced, table = !fenced, false
		case fenced:
		case !strings.HasPrefix(l, "|"):
			table = false
		case strings.HasPrefix(l, "| Field |"):
			table = true
		case table && !strings.HasPrefix(l, "|---"):
			first, _, _ := strings.Cut(strings.TrimPrefix(l, "|"), "|")
			for _, m := range codeSpan.FindAllStringSubmatch(first, -1) {
				rows = append(rows, row{m[1], i + 1})
			}
		}
	}
	return rows
}

type document struct {
	text string
	line int
}

// documents are the resource documents in the code blocks of a language:
// each block with a kind, split on ---.
func documents(lines []string, lang string) []document {
	var docs []document
	var cur []string
	in, start := false, 0
	flush := func() {
		if text := strings.Join(cur, "\n"); strings.Contains(text, "\nkind:") || strings.HasPrefix(text, "kind:") {
			docs = append(docs, document{text, start})
		}
		cur = nil
	}
	for i, l := range lines {
		switch {
		case !in && l == "```"+lang:
			in, start = true, i+2
		case in && l == "```":
			flush()
			in = false
		case in && l == "---":
			flush()
			start = i + 2
		case in:
			cur = append(cur, l)
		}
	}
	return docs
}
