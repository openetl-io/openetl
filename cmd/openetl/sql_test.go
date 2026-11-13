package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/spec"
)

// openetl apply and openetl plan send a SQL file's text, from a path
// relative to the file that names it.
func TestApplyInlinesSQLFiles(t *testing.T) {
	dir := t.TempDir()
	text := "  INSERT INTO \"Out\" SELECT 'a: b' -- é\n\tFROM orders;  \n\nCREATE VIEW v AS SELECT 1"
	pipeline := "apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: shop}\n" +
		"spec:\n  # the schedule\n  schedule: {every: 1h}\n  source: {ref: s, streams: [{name: orders, eventTime: at}]}\n  destination: {ref: d}\n" +
		"  types: {infinity: quarantine}\n  transform:\n    passthrough: none\n    sql: {file: FILE}\n"
	for name, content := range map[string]string{
		"sql/shop.sql":  text,
		"sql/empty.sql": " \n",
		"sql/bytes.sql": "SELECT '\xff'",
	} {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	base := filepath.Join(dir, "openetl")
	inline := func(file string) (*apiv1.Document, error) {
		d := &apiv1.Document{Origin: "p.yaml#1", Content: []byte(strings.Replace(pipeline, "FILE", file, 1))}
		return d, inlineSQL(d, base)
	}
	for _, file := range []string{"../sql/shop.sql", filepath.Join(dir, "sql", "shop.sql")} {
		d, err := inline(file)
		if err != nil {
			t.Fatal(err)
		}
		doc, err := spec.Parse(d.GetContent())
		if err != nil {
			t.Fatalf("%v\n%s", err, d.GetContent())
		}
		p := doc.Pipeline
		if p.GetTransform().GetSql().GetSql() != text || p.GetTransform().GetSql().GetPassthrough() != apiv1.Passthrough_PASSTHROUGH_NONE ||
			p.GetTypes().GetInfinity() != apiv1.Infinity_INFINITY_QUARANTINE || p.GetSource().GetStreams()[0].GetEventTime() != "at" {
			t.Errorf("%s: sent %s", file, d.GetContent())
		}
	}
	for file, want := range map[string]string{
		"../sql/missing.sql": "no such file",
		"../sql/empty.sql":   "is empty",
		"../sql/bytes.sql":   "isn't UTF-8",
	} {
		if _, err := inline(file); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v, want %q", file, err, want)
		}
	}

	for _, content := range []string{
		strings.Replace(pipeline, "{file: FILE}", "'SELECT 1'", 1),
		strings.Replace(pipeline, "{file: FILE}", "{file: x.sql, other: y}", 1),
		strings.Replace(pipeline, "kind: Pipeline", "kind: Source", 1),
	} {
		d := &apiv1.Document{Content: []byte(content)}
		if err := inlineSQL(d, base); err != nil || string(d.GetContent()) != content {
			t.Errorf("changed %s: %v, %s", content, err, d.GetContent())
		}
	}
}
