// Command notices writes a release's third-party notices: every Go module,
// Rust crate and console package its binaries hold, with their license and
// notice files. It needs them downloaded, and pnpm install run in web/. Run
// it from the repository's root:
//
//	go run ./internal/notices -out THIRD-PARTY-NOTICES.txt
//
// The official connectors' modules are included with -connectors, a checkout
// of openetl-connectors, and -connectors-work, the go.work that builds it.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// A file at a package's root that carries its terms.
var licenseFile = regexp.MustCompile(`(?i)^(licen[cs]e|copying|notice|unlicense|copyright)([-._].*)?$`)

// The binaries' targets. A dependency can be on one platform only.
var (
	goOSes      = []string{"linux", "darwin"}
	rustTargets = []string{"x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu", "x86_64-apple-darwin", "aarch64-apple-darwin"}
)

// component is one third-party package. dir is where its license files are;
// declared is its license's name, for when it ships no file.
type component struct {
	name, dir, declared string
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "notices:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	out := flag.String("out", "THIRD-PARTY-NOTICES.txt", "the file to write")
	connectors := flag.String("connectors", "", "a checkout of openetl-connectors")
	work := flag.String("connectors-work", "", "the go.work that builds -connectors")
	web := flag.String("web", "web", "the console's directory")
	flag.Parse()

	found := map[string]component{}
	if err := goModules(ctx, found, "", nil, "./cmd/openetld", "./cmd/openetl"); err != nil {
		return err
	}
	if *connectors != "" {
		if err := goModules(ctx, found, *connectors, []string{"GOWORK=" + *work}, "./cmd/..."); err != nil {
			return fmt.Errorf("the connectors: %w", err)
		}
	}
	goroot, err := output(ctx, "", nil, "go", "env", "GOROOT")
	if err != nil {
		return err
	}
	found["go:std"] = component{name: "Go standard library", dir: strings.TrimSpace(string(goroot))}
	found["rust:std"] = component{name: "Rust standard library", declared: "MIT OR Apache-2.0"}
	if err := rustCrates(ctx, found); err != nil {
		return err
	}
	if err := webPackages(ctx, found, *web); err != nil {
		return err
	}
	text, err := render(slices.Collect(maps.Values(found)))
	if err != nil {
		return err
	}
	return os.WriteFile(*out, text, 0o600)
}

// output runs a command in dir and returns its standard output.
func output(ctx context.Context, dir string, env []string, name string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	b, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, stderr.Bytes())
	}
	return b, nil
}

func goModules(ctx context.Context, found map[string]component, dir string, env []string, patterns ...string) error {
	for _, goos := range goOSes {
		args := append([]string{"list", "-deps", "-json=Module"}, patterns...)
		b, err := output(ctx, dir, append(env, "GOOS="+goos, "CGO_ENABLED=0"), "go", args...)
		if err != nil {
			return err
		}
		mods, err := parseGoList(bytes.NewReader(b))
		if err != nil {
			return err
		}
		for _, m := range mods {
			found["go:"+m.name] = m
		}
	}
	return nil
}

// parseGoList reads go list -json's stream of packages. The standard library
// has no module, and this repository's modules are the main ones or ours.
func parseGoList(r io.Reader) ([]component, error) {
	var out []component
	for dec := json.NewDecoder(r); ; {
		var p struct {
			Module *struct {
				Path, Version, Dir string
				Main               bool
			}
		}
		if err := dec.Decode(&p); errors.Is(err, io.EOF) {
			return out, nil
		} else if err != nil {
			return nil, err
		}
		if p.Module == nil || p.Module.Main || strings.HasPrefix(p.Module.Path, "github.com/openetl-io/") {
			continue
		}
		out = append(out, component{name: p.Module.Path + " " + p.Module.Version, dir: p.Module.Dir,
			declared: "see https://pkg.go.dev/" + p.Module.Path + "@" + p.Module.Version + "?tab=licenses"})
	}
}

type cargoPackage struct {
	ID, Name, Version, License string
	ManifestPath               string `json:"manifest_path"`
}

// A dependency's kind is null for a normal one.
type cargoKind struct{ Kind *string }

type cargoDep struct {
	Pkg      string
	DepKinds []cargoKind `json:"dep_kinds"`
}

type cargoNode struct {
	ID   string
	Deps []cargoDep
}

type cargoMetadata struct {
	Packages []cargoPackage
	Resolve  struct{ Nodes []cargoNode }
}

func rustCrates(ctx context.Context, found map[string]component) error {
	for _, target := range rustTargets {
		b, err := output(ctx, "", nil, "cargo", "metadata", "--format-version=1", "--locked", "--filter-platform", target)
		if err != nil {
			return err
		}
		var m cargoMetadata
		if err := json.Unmarshal(b, &m); err != nil {
			return err
		}
		crates, err := engineCrates(&m)
		if err != nil {
			return err
		}
		for _, c := range crates {
			found["rust:"+c.name] = c
		}
	}
	return nil
}

// engineCrates are what openetl-engine links: its normal dependencies, not
// build or dev ones, without the workspace's own crates, which have no license
// of their own to show.
func engineCrates(m *cargoMetadata) ([]component, error) {
	var root string
	byID := map[string]int{}
	for i, p := range m.Packages {
		byID[p.ID] = i
		if p.Name == "openetl-engine" {
			root = p.ID
		}
	}
	if root == "" {
		return nil, errors.New("cargo metadata has no openetl-engine")
	}
	edges := map[string][]string{}
	for _, n := range m.Resolve.Nodes {
		for _, d := range n.Deps {
			if slices.ContainsFunc(d.DepKinds, func(k cargoKind) bool { return k.Kind == nil }) {
				edges[n.ID] = append(edges[n.ID], d.Pkg)
			}
		}
	}
	seen := map[string]bool{root: true}
	var out []component
	for stack := []string{root}; len(stack) > 0; {
		id := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		for _, d := range edges[id] {
			if seen[d] {
				continue
			}
			seen[d] = true
			stack = append(stack, d)
			p := m.Packages[byID[d]]
			if !strings.HasPrefix(p.Name, "openetl-") {
				out = append(out, component{name: p.Name + " " + p.Version, dir: filepath.Dir(p.ManifestPath), declared: p.License})
			}
		}
	}
	return out, nil
}

func webPackages(ctx context.Context, found map[string]component, dir string) error {
	b, err := output(ctx, dir, nil, "pnpm", "licenses", "list", "--prod", "--json")
	if err != nil {
		return err
	}
	pkgs, err := parsePnpmLicenses(b)
	if err != nil {
		return err
	}
	for _, c := range pkgs {
		found["web:"+c.name] = c
	}
	return nil
}

// parsePnpmLicenses reads pnpm licenses list --json: packages by license, each
// with a path per version.
func parsePnpmLicenses(b []byte) ([]component, error) {
	var byLicense map[string][]struct {
		Name     string
		Versions []string
		Paths    []string
	}
	if err := json.Unmarshal(b, &byLicense); err != nil {
		return nil, err
	}
	var out []component
	for license, pkgs := range byLicense {
		for _, p := range pkgs {
			for i, v := range p.Versions {
				c := component{name: p.Name + " " + v, declared: license}
				if i < len(p.Paths) {
					c.dir = p.Paths[i]
				}
				out = append(out, c)
			}
		}
	}
	return out, nil
}

// terms is a component's license and notice files, or its declared license
// when it has none.
func terms(c component) (string, error) {
	var parts []string
	if c.dir != "" {
		root, err := os.OpenRoot(c.dir)
		if err != nil {
			return "", fmt.Errorf("%s: %w", c.name, err)
		}
		defer func() { _ = root.Close() }()
		entries, err := os.ReadDir(c.dir)
		if err != nil {
			return "", fmt.Errorf("%s: %w", c.name, err)
		}
		for _, e := range entries {
			if e.IsDir() || !licenseFile.MatchString(e.Name()) {
				continue
			}
			b, err := root.ReadFile(e.Name())
			if err != nil {
				return "", fmt.Errorf("%s: %w", c.name, err)
			}
			parts = append(parts, strings.TrimSpace(string(b)))
		}
	}
	if len(parts) == 0 {
		return "License: " + cmp.Or(c.declared, "not stated by the package"), nil
	}
	return strings.Join(parts, "\n\n"), nil
}

// render groups components under identical terms, sorted so that the same
// dependencies always give the same file.
func render(components []component) ([]byte, error) {
	type group struct {
		names []string
		text  string
	}
	groups := map[[sha256.Size]byte]*group{}
	for _, c := range components {
		text, err := terms(c)
		if err != nil {
			return nil, err
		}
		key := sha256.Sum256([]byte(text))
		if groups[key] == nil {
			groups[key] = &group{text: text}
		}
		groups[key].names = append(groups[key].names, c.name)
	}
	sorted := slices.Collect(maps.Values(groups))
	for _, g := range sorted {
		slices.Sort(g.names)
	}
	slices.SortFunc(sorted, func(a, b *group) int { return strings.Compare(a.names[0], b.names[0]) })

	var out bytes.Buffer
	out.WriteString("OpenETL includes the following third-party software, under these terms.\n")
	for _, g := range sorted {
		out.WriteString("\n" + strings.Repeat("=", 78) + "\n")
		for _, n := range g.names {
			out.WriteString(n + "\n")
		}
		out.WriteString(strings.Repeat("-", 78) + "\n" + g.text + "\n")
	}
	return out.Bytes(), nil
}
