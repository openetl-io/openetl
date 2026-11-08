package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPagesAreCurrent(t *testing.T) {
	pages, err := render()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range pages {
		path := filepath.Join("..", "..", "docs", "reference", p.file)
		b, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("%s: %v; run %s from the repository's root", path, err, command)
			continue
		}
		if string(b) != p.body {
			t.Errorf("docs/reference/%s is stale; run %s from the repository's root", p.file, command)
		}
	}
}

func TestRendersEveryRule(t *testing.T) {
	for _, tc := range []struct {
		name, schema, want string
	}{
		{"unknown keyword", `{"type": "object", "properties": {"a": {"type": "string", "format": "uri", "description": "A."}}}`, "format"},
		{"no description", `{"type": "object", "properties": {"a": {"type": "string"}}}`, "no description"},
		{"required but undefined", `{"type": "object", "required": ["b"], "properties": {"a": {"type": "string", "description": "A."}}}`, "doesn't define"},
		{"a not of two at once", `{"type": "object", "properties": {"a": {"type": "string", "description": "A."}}, "not": {"required": ["a", "b"]}}`, "more than which fields"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, err := newSchemaDoc([]byte(tc.schema))
			if err != nil {
				t.Fatal(err)
			}
			_, err = d.render()
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want an error with %q", err, tc.want)
			}
		})
	}
}

func TestRules(t *testing.T) {
	d, err := newSchemaDoc([]byte(`{
		"type": "object",
		"additionalProperties": false,
		"properties": {
			"x": {
				"description": "X.",
				"type": "object",
				"additionalProperties": false,
				"anyOf": [{"required": ["a"]}, {"required": ["b", "c"]}],
				"dependentRequired": {"c": ["a"]},
				"not": {"anyOf": [{"required": ["d"]}]},
				"properties": {
					"a": {"description": "A.", "type": "string", "minLength": 1},
					"b": {"description": "B.", "enum": ["one", "two"]},
					"c": {"description": "C.", "type": "integer", "minimum": 1, "maximum": 9},
					"d": {"description": "D.", "type": "array", "items": {"type": "string", "pattern": "^[a-z]+$"}, "maxItems": 3}
				}
			}
		}
	}`))
	if err != nil {
		t.Fatal(err)
	}
	got, err := d.render()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"| `x` | [object](#x) | No | X. |",
		"| `a` | string | No | A. Isn't empty. |",
		"| `b` | string | No | B. Is one of `one` or `two`. |",
		"| `c` | integer | No | C. Is from 1 to 9. |",
		"| `d` | array of string | No | D. Has at most 3 items. Each item matches `^[a-z]+$`. |",
		"- Has at least one of these: `a`; or `b` and `c`.",
		"- Can't have `d`.",
		"- `c` needs `a`.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("no %q in:\n%s", want, got)
		}
	}
}
