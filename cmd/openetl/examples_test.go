package main

import (
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/spec"
)

// Every example parses, and its connectors accept their configs.
func TestExamples(t *testing.T) {
	files, err := filepath.Glob("../../examples/*.yaml")
	if err != nil || len(files) == 0 {
		t.Fatalf("examples: %v, %v", files, err)
	}
	for _, f := range files {
		docs, err := readDocuments(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range docs {
			doc, err := spec.Parse(d.GetContent())
			if err != nil {
				t.Errorf("%s: %v", d.GetOrigin(), err)
				continue
			}
			if doc.Connector == nil {
				continue
			}
			cfg, err := protojson.Marshal(doc.Connector.GetConfig())
			if err != nil {
				t.Fatal(err)
			}
			switch doc.Kind {
			case spec.KindSource:
				_, err = connectorhost.NewSource(doc.Connector.GetConnector(), cfg)
			case spec.KindDestination:
				_, err = connectorhost.NewDestination(doc.Connector.GetConnector(), cfg)
			}
			if err != nil {
				t.Errorf("%s: %v", d.GetOrigin(), err)
			}
		}
	}
}
