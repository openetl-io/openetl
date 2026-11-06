package connectorhost

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestConnectorsDirectory(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "acme"), 0o750); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"source-pg", "acme/source-crm", "acme/destination-crm"} {
		if err := os.WriteFile(filepath.Join(dir, p), []byte("#!/bin/sh\n"), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	env := Env{Connectors: dir}
	for _, name := range []string{"pg", "openetl/pg", "acme/crm"} {
		if _, err := NewSource(name, nil, env); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := NewDestination("acme/crm", nil, env); err != nil {
		t.Errorf("acme/crm's destination: %v", err)
	}
	if _, err := NewSource("crm", nil, env); err == nil || !strings.Contains(err.Error(), "known: acme/crm, faker, pg") {
		t.Errorf("crm is openetl's, not acme's: %v", err)
	}
	if _, err := NewSource("acme/../pg", nil, env); err == nil {
		t.Error("a name that leaves the directory")
	}
	if ExecutablePath("source", "pg") != "source-pg" || ExecutablePath("source", "openetl/pg") != "source-pg" || ExecutablePath("destination", "acme/crm") != filepath.Join("acme", "destination-crm") {
		t.Error("executable paths")
	}
	if !Builtin("source", "faker") || !Builtin("source", "openetl/faker") || Builtin("source", "acme/faker") || Builtin("destination", "faker") {
		t.Error("built-ins")
	}
}
