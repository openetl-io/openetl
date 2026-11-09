package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseGoListSkipsTheStandardLibraryAndOurModules(t *testing.T) {
	got, err := parseGoList(strings.NewReader(`{}
{"Module":{"Path":"github.com/openetl-io/openetl","Main":true}}
{"Module":{"Path":"github.com/openetl-io/openetl/sdk/go","Version":"v0.0.0"}}
{"Module":{"Path":"example.com/a","Version":"v1.2.3","Dir":"/m/a"}}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].name != "example.com/a v1.2.3" || got[0].dir != "/m/a" {
		t.Fatalf("got %+v", got)
	}
}

func TestEngineCratesFollowsOnlyNormalDependencies(t *testing.T) {
	build := "build"
	normal := []cargoKind{{}}
	m := &cargoMetadata{}
	for _, p := range []struct{ id, name string }{{"e", "openetl-engine"}, {"f", "openetl-formats"}, {"n", "normal"}, {"t", "transitive"}, {"b", "build-only"}, {"u", "unused"}} {
		m.Packages = append(m.Packages, cargoPackage{ID: p.id, Name: p.name, Version: "1.0.0", License: "MIT", ManifestPath: "/c/" + p.name + "/Cargo.toml"})
	}
	m.Resolve.Nodes = []cargoNode{
		{ID: "e", Deps: []cargoDep{{Pkg: "f", DepKinds: normal}, {Pkg: "n", DepKinds: normal}, {Pkg: "b", DepKinds: []cargoKind{{Kind: &build}}}}},
		{ID: "f", Deps: []cargoDep{{Pkg: "t", DepKinds: normal}}},
	}
	got, err := engineCrates(m)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, c := range got {
		names[c.name] = true
	}
	if len(got) != 2 || !names["normal 1.0.0"] || !names["transitive 1.0.0"] {
		t.Fatalf("got %+v", got)
	}
}

func TestParsePnpmLicensesTakesEachVersionsPath(t *testing.T) {
	got, err := parsePnpmLicenses([]byte(`{"MIT":[{"name":"a","versions":["1.0.0","2.0.0"],"paths":["/p/1","/p/2"]}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[1].name != "a 2.0.0" || got[1].dir != "/p/2" || got[1].declared != "MIT" {
		t.Fatalf("got %+v", got)
	}
}

func TestRenderGroupsIdenticalTermsInAStableOrder(t *testing.T) {
	dir := t.TempDir()
	for name, text := range map[string]string{"LICENSE": "the terms", "NOTICE.md": "a notice", "main.go": "not terms"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	first := []component{{name: "z 1", dir: dir}, {name: "a 1", dir: dir}, {name: "bare 1", declared: "MIT"}}
	b, err := render(first)
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, "a 1\nz 1\n") || !strings.Contains(got, "the terms\n\na notice") || !strings.Contains(got, "License: MIT") {
		t.Fatalf("got:\n%s", got)
	}
	if strings.Contains(got, "not terms") {
		t.Fatalf("a source file was taken for a license:\n%s", got)
	}
	again, err := render([]component{first[2], first[1], first[0]})
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != got {
		t.Fatalf("the order of components changed the file:\n%s\n---\n%s", again, got)
	}
}
