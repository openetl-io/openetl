package main

import (
	"encoding/base64"
	"fmt"
	"path/filepath"

	"go.yaml.in/yaml/v3"

	apiv1 "github.com/openetl-io/openetl/gen/openetl/api/v1"
	"github.com/openetl-io/openetl/internal/dbt"
	"github.com/openetl-io/openetl/internal/spec"
)

// bundleDbtProject replaces a DbtProject's spec.path, a directory relative
// to base, with spec.bundle: the project's files. Other documents are left
// as they are.
func bundleDbtProject(d *apiv1.Document, base string) error {
	var doc map[string]any
	if err := yaml.Unmarshal(d.GetContent(), &doc); err != nil {
		return nil
	}
	s, _ := doc["spec"].(map[string]any)
	dir, _ := s["path"].(string)
	if doc["kind"] != spec.KindDbtProject || dir == "" {
		return nil
	}
	if !filepath.IsAbs(dir) {
		dir = filepath.Join(base, dir)
	}
	bundle, err := dbt.Pack(dir)
	if err != nil {
		return fmt.Errorf("%s: dbt project %s: %w", d.GetOrigin(), dir, err)
	}
	delete(s, "path")
	s["bundle"] = base64.StdEncoding.EncodeToString(bundle)
	b, err := yaml.Marshal(doc)
	if err != nil {
		return err
	}
	d.Content = b
	return nil
}
