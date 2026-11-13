package api

import (
	"testing"

	"github.com/openetl-io/openetl/internal/spec"
)

// A pipeline's version is its SQL's text: the same text gives the same
// version, and other text another.
func TestVersionTakesSQL(t *testing.T) {
	version := func(sql string) string {
		t.Helper()
		doc, err := spec.Parse([]byte("apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nmetadata: {name: p}\nspec:\n  schedule: {every: 1h}\n" +
			"  source: {ref: s}\n  destination: {ref: d}\n  transform: {sql: '" + sql + "'}\n"))
		if err != nil {
			t.Fatal(err)
		}
		v, err := (&pinned{spec: doc.Pipeline}).version()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	a := version("INSERT INTO a SELECT * FROM orders")
	if a != version("INSERT INTO a SELECT * FROM orders") {
		t.Error("the same SQL gave another version")
	}
	if a == version("INSERT INTO a SELECT id FROM orders") {
		t.Error("other SQL gave the same version")
	}
}
