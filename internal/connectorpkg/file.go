// Package connectorpkg reads openetl-connector.yaml, the file a connector's
// author writes, and builds and packages what it describes.
package connectorpkg

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"go.yaml.in/yaml/v3"

	"github.com/unsuitable001/openetl/internal/spec"
)

// FileName is the authored file, at the root of a package's directory.
const FileName = "openetl-connector.yaml"

//go:embed openetl-connector.schema.json
var schemaJSON []byte

// Schema is the file's JSON Schema.
func Schema() []byte { return bytes.Clone(schemaJSON) }

var fileSchema = sync.OnceValues(func() (*jsonschema.Schema, error) {
	doc, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
	if err != nil {
		return nil, err
	}
	c := jsonschema.NewCompiler()
	if err := c.AddResource(FileName, doc); err != nil {
		return nil, err
	}
	return c.Compile(FileName)
})

// DefaultPlatforms are built when a file names none.
var DefaultPlatforms = []string{"linux/amd64", "linux/arm64", "darwin/amd64", "darwin/arm64"}

// File is an openetl-connector.yaml.
type File struct {
	// The directory it's in, which its paths are relative to.
	Dir string `yaml:"-"`

	Name        string           `yaml:"name"`
	Description string           `yaml:"description"`
	License     string           `yaml:"license"`
	Repository  string           `yaml:"repository"`
	Docs        string           `yaml:"docs"`
	Logo        string           `yaml:"logo"`
	Author      string           `yaml:"author"`
	Tags        []string         `yaml:"tags"`
	Platforms   []string         `yaml:"platforms"`
	Roles       map[string]*Role `yaml:"roles"`
}

// Role is how one role is built or run.
type Role struct {
	Build    *Build   `yaml:"build"`
	Image    string   `yaml:"image"`
	Command  []string `yaml:"command"`
	Manifest string   `yaml:"manifest"`
	Requires *struct {
		Platform string `yaml:"platform"`
	} `yaml:"requires"`
	Resources *struct {
		CPU    string `yaml:"cpu"`
		Memory string `yaml:"memory"`
		Disk   string `yaml:"disk"`
		Hint   string `yaml:"hint"`
	} `yaml:"resources"`
	State *struct {
		Version uint32 `yaml:"version"`
		Reads   uint32 `yaml:"reads"`
	} `yaml:"state"`
}

// Build is how a role's executable is built for each platform.
type Build struct {
	// A Go main package, relative to the file.
	Go string `yaml:"go"`
	// Run once with sh in the file's directory.
	Command string `yaml:"command"`
	// Where Command leaves each platform's executable, with {os} and {arch}.
	Output string `yaml:"output"`
}

// Info is the file's metadata, as a descriptor holds it.
func (f *File) Info() spec.Info {
	return spec.Info{
		Description: f.Description, License: f.License, Repository: f.Repository, Docs: f.Docs,
		Logo: f.Logo, Author: f.Author, Tags: f.Tags,
	}
}

// FullName is the package's name with its owner.
func (f *File) FullName() string { return spec.FullName(f.Name) }

// ShortName is the package's name without its owner, which names its
// release assets and tags.
func (f *File) ShortName() string {
	_, short := spec.SplitName(f.Name)
	return short
}

// Builds reports whether any role is built.
func (f *File) Builds() bool {
	for _, r := range f.Roles {
		if r.Build != nil {
			return true
		}
	}
	return false
}

// Load reads and checks dir's openetl-connector.yaml.
func Load(dir string) (*File, error) {
	path := filepath.Join(dir, FileName)
	b, err := os.ReadFile(path) //nolint:gosec // the author's own file
	if err != nil {
		return nil, err
	}
	f, err := Parse(b)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	f.Dir = dir
	for name, r := range f.Roles {
		if r.Build != nil && r.Build.Go != "" {
			if fi, err := os.Stat(filepath.Join(dir, r.Build.Go)); err != nil || !fi.IsDir() {
				return nil, fmt.Errorf("%s: roles.%s.build.go: %s isn't a directory", path, name, r.Build.Go)
			}
		}
		if r.Manifest != "" {
			if _, err := os.Stat(filepath.Join(dir, r.Manifest)); err != nil {
				return nil, fmt.Errorf("%s: roles.%s.manifest: %w", path, name, err)
			}
		}
	}
	return f, nil
}

// Parse checks an openetl-connector.yaml against its JSON Schema and the
// rules the schema can't say.
func Parse(b []byte) (*File, error) {
	var doc any
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	j, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	inst, err := jsonschema.UnmarshalJSON(bytes.NewReader(j))
	if err != nil {
		return nil, err
	}
	sch, err := fileSchema()
	if err != nil {
		return nil, err
	}
	if err := sch.Validate(inst); err != nil {
		return nil, err
	}
	var f File
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(&f); err != nil {
		return nil, err
	}
	if err := spec.CheckInfo(f.Info()); err != nil {
		return nil, err
	}
	if len(f.Platforms) == 0 {
		f.Platforms = DefaultPlatforms
	}
	for name, r := range f.Roles {
		if err := r.check(name, len(f.Platforms)); err != nil {
			return nil, fmt.Errorf("roles.%s: %w", name, err)
		}
	}
	return &f, nil
}

func (r *Role) check(role string, platforms int) error {
	switch {
	case r.Manifest != "" && role != spec.RoleSource:
		return errors.New("manifest: only a source runs from a manifest")
	case r.Manifest != "" && (r.Build != nil || r.Image != ""):
		return errors.New("manifest is the role's whole runtime, with no build or image")
	case r.Manifest != "" && r.Resources != nil && r.Resources.CPU+r.Resources.Memory+r.Resources.Disk != "":
		return errors.New("a manifest runs in the platform, so of resources it takes only a hint")
	case r.Manifest != "" && len(r.Command) > 0:
		return errors.New("a manifest runs in the platform, so it takes no command")
	case r.Resources != nil && spec.CheckHint(r.Resources.Hint) != nil:
		return spec.CheckHint(r.Resources.Hint)
	case len(r.Command) > 0 && r.Image == "":
		return errors.New("command is an image's argv, and the role has no image")
	case r.State != nil && r.State.Reads > r.State.Version:
		return fmt.Errorf("state: reads %d is more than version %d", r.State.Reads, r.State.Version)
	case r.Build != nil && r.Build.Command != "" && platforms > 1 && (!strings.Contains(r.Build.Output, "{os}") || !strings.Contains(r.Build.Output, "{arch}")):
		return errors.New("build.output needs {os} and {arch}, to name each platform's executable")
	}
	return nil
}

// CheckManifests loads each role's manifest, as package does.
func (f *File) CheckManifests() error {
	for name, r := range f.Roles {
		if r.Manifest == "" {
			continue
		}
		if _, err := inlineManifest(filepath.Join(f.Dir, r.Manifest)); err != nil {
			return fmt.Errorf("roles.%s.manifest: %w", name, err)
		}
	}
	return nil
}

// Require checks that the file sets each of fields, such as author and logo,
// which a repository can ask of its packages.
func (f *File) Require(fields []string) error {
	set := map[string]bool{
		"description": f.Description != "", "license": f.License != "", "docs": f.Docs != "",
		"logo": f.Logo != "", "author": f.Author != "", "tags": len(f.Tags) > 0,
	}
	var missing []string
	for _, field := range fields {
		ok, known := set[field]
		if !known {
			return fmt.Errorf("can't require %q: only description, license, docs, logo, author and tags", field)
		}
		if !ok {
			missing = append(missing, field)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s has no %s", f.Name, strings.Join(missing, " or "))
	}
	return nil
}
