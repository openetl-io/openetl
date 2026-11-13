package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"unicode/utf8"

	"go.yaml.in/yaml/v3"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/spec"
)

// inlineSQL replaces a Pipeline's spec.transform.sql of {file: PATH}, a path
// relative to base, with the file's text, so the version holds the SQL
// itself. Other documents, and SQL given as text, are left as they are.
func inlineSQL(d *apiv1.Document, base string) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(d.GetContent(), &doc); err != nil || len(doc.Content) == 0 {
		return nil
	}
	root := doc.Content[0]
	if kind := mappingValue(root, "kind"); kind == nil || kind.Value != spec.KindPipeline {
		return nil
	}
	sql := mappingValue(mappingValue(mappingValue(root, "spec"), "transform"), "sql")
	file := mappingValue(sql, "file")
	if file == nil || file.Kind != yaml.ScalarNode || file.Value == "" || len(sql.Content) != 2 {
		return nil
	}
	path := file.Value
	if !filepath.IsAbs(path) {
		path = filepath.Join(base, path)
	}
	text, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return fmt.Errorf("%s: transform.sql.file: %w", d.GetOrigin(), err)
	}
	switch {
	case !utf8.Valid(text):
		return fmt.Errorf("%s: transform.sql.file: %s isn't UTF-8 text", d.GetOrigin(), path)
	case len(bytes.TrimSpace(text)) == 0:
		return fmt.Errorf("%s: transform.sql.file: %s is empty", d.GetOrigin(), path)
	}
	*sql = yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: string(text), Style: yaml.LiteralStyle}
	b, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	d.Content = b
	return nil
}

// mappingValue is a mapping's value for key; nil if n isn't a mapping
// or hasn't the key.
func mappingValue(n *yaml.Node, key string) *yaml.Node {
	if n == nil || n.Kind != yaml.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		if n.Content[i].Value == key {
			return n.Content[i+1]
		}
	}
	return nil
}
