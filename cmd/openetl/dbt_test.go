package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/unsuitable001/openetl/internal/dbt"
	"github.com/unsuitable001/openetl/internal/spec"
)

// openetl apply sends a DbtProject's files, from a directory relative to
// the file that names it.
func TestApplyBundlesDbtProjects(t *testing.T) {
	dir := t.TempDir()
	for name, content := range map[string]string{
		"dbt/dbt_project.yml":  "name: shop\nprofile: shop\n",
		"dbt/models/a.sql":     "select 1",
		"dbt/target/stale.sql": "select 0",
		"openetl/project.yaml": "apiVersion: openetl.io/v1alpha1\nkind: DbtProject\nmetadata: {name: shop}\nspec: {path: ../dbt}\n",
	} {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	file := filepath.Join(dir, "openetl", "project.yaml")
	docs, err := readDocuments(file)
	if err != nil {
		t.Fatal(err)
	}
	if err := bundleDbtProject(docs[0], filepath.Dir(file)); err != nil {
		t.Fatal(err)
	}
	doc, err := spec.Parse(docs[0].GetContent())
	if err != nil {
		t.Fatal(err)
	}
	want, err := dbt.Pack(filepath.Join(dir, "dbt"))
	if err != nil {
		t.Fatal(err)
	}
	if doc.Kind != spec.KindDbtProject || string(doc.Bundle) != string(want) {
		t.Errorf("sent %s", docs[0].GetContent())
	}

	pipeline := []byte("apiVersion: openetl.io/v1alpha1\nkind: Pipeline\nspec: {path: x}\n")
	docs[0].Content = pipeline
	if err := bundleDbtProject(docs[0], dir); err != nil || string(docs[0].GetContent()) != string(pipeline) {
		t.Errorf("changed another kind: %v, %s", err, docs[0].GetContent())
	}
	docs[0].Content = []byte("apiVersion: openetl.io/v1alpha1\nkind: DbtProject\nmetadata: {name: shop}\nspec: {path: missing}\n")
	if err := bundleDbtProject(docs[0], dir); err == nil {
		t.Error("bundled a missing directory")
	}
}
