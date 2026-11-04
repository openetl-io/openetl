package connectorpkg

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"

	"github.com/unsuitable001/openetl/internal/connectorhost"
	"github.com/unsuitable001/openetl/internal/declarative"
	"github.com/unsuitable001/openetl/internal/spec"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Options are what a release adds to the file.
type Options struct {
	Version string
	// The release's git tag; empty: v{Version}.
	Tag string
	// Where executables and the descriptor are written.
	Out string
	// Where runners download each executable, with {tag}, {asset},
	// {version}, {name}, {role}, {os} and {arch} in it. Empty: the
	// release's assets, for a repository on GitHub.
	URLTemplate string
	// Overrides the file's.
	Platforms []string
	// Resolves images' tags; nil: a role with an image fails.
	Registry *Registry
	// Reads the spec of a role that only has an image; nil: such a role
	// fails.
	ImageSpec func(ctx context.Context, image, role string, command []string) (*protocolv1.SpecResponse, error)
	// Builds' output; nil: discarded.
	Log io.Writer
}

// Result is what Package wrote, below Options.Out.
type Result struct {
	Descriptor  string
	Executables []string
}

// Asset is the release asset of a role's executable for a platform.
func Asset(short, role, platform string) string {
	goos, arch, _ := strings.Cut(platform, "/")
	a := short + "-" + role + "-" + goos + "-" + arch
	if goos == "windows" {
		a += ".exe"
	}
	return a
}

// DescriptorAsset is the release asset of a package's descriptor.
func DescriptorAsset(short string) string { return short + ".connector.yaml" }

var githubRepo = regexp.MustCompile(`^https://github\.com/[^/]+/[^/]+$`)

// ReleaseURL is a GitHub repository's page for tag; "" elsewhere.
func ReleaseURL(repository, tag string) string {
	repo := strings.TrimSuffix(strings.TrimSuffix(repository, "/"), ".git")
	if !githubRepo.MatchString(repo) {
		return ""
	}
	return repo + "/releases/tag/" + tag
}

// Package builds f's roles and writes its descriptor, for a release.
func Package(ctx context.Context, f *File, o Options) (*Result, error) {
	if !spec.ValidVersion(o.Version) {
		return nil, fmt.Errorf("version %q isn't a semantic version, such as 1.2.0", o.Version)
	}
	tag := cmp.Or(o.Tag, "v"+o.Version)
	platforms := o.Platforms
	if len(platforms) == 0 {
		platforms = f.Platforms
	}
	for _, p := range platforms {
		if !spec.ValidPlatform(p) {
			return nil, fmt.Errorf("%q isn't a platform, such as linux/amd64", p)
		}
	}
	if f.Builds() && !slices.Contains(platforms, connectorhost.Platform()) {
		return nil, fmt.Errorf("the platforms must include %s, whose executable is run to read the spec", connectorhost.Platform())
	}
	urls := o.URLTemplate
	if urls == "" && f.Builds() {
		repo := strings.TrimSuffix(strings.TrimSuffix(f.Repository, "/"), ".git")
		if !githubRepo.MatchString(repo) {
			return nil, fmt.Errorf("%s isn't on GitHub, so give --url-template, where runners download each executable", f.Repository)
		}
		urls = repo + "/releases/download/{tag}/{asset}"
	}
	if err := os.MkdirAll(o.Out, 0o750); err != nil {
		return nil, err
	}
	res := &Result{}
	roles := map[string]connectorhost.PackageRole{}
	for _, role := range slices.Sorted(maps.Keys(f.Roles)) {
		r := f.Roles[role]
		pr := connectorhost.PackageRole{}
		if r.Requires != nil {
			pr.Platform = r.Requires.Platform
		}
		if r.Resources != nil {
			pr.Resources = spec.Resources{CPU: r.Resources.CPU, Memory: r.Resources.Memory, Disk: r.Resources.Disk}
		}
		if r.State != nil {
			pr.State = &spec.State{Version: r.State.Version, Reads: r.State.Reads}
		}
		if r.Build != nil {
			exes, err := build(ctx, f, role, r.Build, platforms, o)
			if err != nil {
				return nil, fmt.Errorf("build the %s role: %w", role, err)
			}
			for _, e := range exes {
				asset := filepath.Base(e.Path)
				goos, arch, _ := strings.Cut(e.Platform, "/")
				e.URL = strings.NewReplacer("{tag}", tag, "{asset}", asset, "{version}", o.Version, "{name}", f.ShortName(),
					"{role}", role, "{os}", goos, "{arch}", arch).Replace(urls)
				pr.Executables = append(pr.Executables, e)
				res.Executables = append(res.Executables, e.Path)
			}
		}
		if r.Image != "" {
			if o.Registry == nil {
				return nil, fmt.Errorf("the %s role's image can't be resolved here", role)
			}
			digest, plats, err := o.Registry.Resolve(ctx, r.Image, o.Version)
			if err != nil {
				return nil, fmt.Errorf("the %s role's image: %w", role, err)
			}
			pr.Image = &spec.Image{Ref: r.Image, Digest: digest, Platforms: plats, Command: r.Command}
			if r.Build == nil {
				if o.ImageSpec == nil {
					return nil, fmt.Errorf("the %s role has only an image, and reading its spec needs Docker or Podman", role)
				}
				if pr.Spec, err = o.ImageSpec(ctx, r.Image+"@"+digest, role, r.Command); err != nil {
					return nil, fmt.Errorf("read the %s role's spec from its image: %w", role, err)
				}
			}
		}
		if r.Manifest != "" {
			m, err := inlineManifest(filepath.Join(f.Dir, r.Manifest))
			if err != nil {
				return nil, fmt.Errorf("the %s role's manifest: %w", role, err)
			}
			pr.Manifest = m
		}
		roles[role] = pr
	}
	info := f.Info()
	info.ReleaseTag, info.ReleaseURL = tag, ReleaseURL(f.Repository, tag)
	doc, err := connectorhost.Package(ctx, f.FullName(), o.Version, info, roles)
	if err != nil {
		return nil, err
	}
	res.Descriptor = filepath.Join(o.Out, DescriptorAsset(f.ShortName()))
	return res, os.WriteFile(res.Descriptor, doc, 0o644) //nolint:gosec // a descriptor, not a secret
}

// build builds a role for each platform into o.Out, named as its release
// asset.
func build(ctx context.Context, f *File, role string, b *Build, platforms []string, o Options) ([]connectorhost.Executable, error) {
	out, err := filepath.Abs(o.Out)
	if err != nil {
		return nil, err
	}
	log := o.Log
	if log == nil {
		log = io.Discard
	}
	if b.Command != "" {
		cmd := exec.CommandContext(ctx, "sh", "-c", b.Command) //nolint:gosec // the author's own build
		cmd.Dir, cmd.Stdout, cmd.Stderr = f.Dir, log, log
		cmd.Env = append(os.Environ(), "OPENETL_VERSION="+o.Version, "OPENETL_PLATFORMS="+strings.Join(platforms, ","))
		if err := cmd.Run(); err != nil {
			return nil, fmt.Errorf("%s: %w", b.Command, err)
		}
	}
	var exes []connectorhost.Executable
	for _, p := range platforms {
		goos, arch, _ := strings.Cut(p, "/")
		dst := filepath.Join(out, Asset(f.ShortName(), role, p))
		if b.Go != "" {
			cmd := exec.CommandContext(ctx, "go", "build", "-trimpath", "-o", dst, "./"+filepath.ToSlash(filepath.Clean(b.Go))) //nolint:gosec // the author's own build
			cmd.Dir, cmd.Stdout, cmd.Stderr = f.Dir, log, log
			cmd.Env = append(os.Environ(), "GOOS="+goos, "GOARCH="+arch, "CGO_ENABLED=0")
			if err := cmd.Run(); err != nil {
				return nil, fmt.Errorf("go build for %s: %w", p, err)
			}
		} else {
			src := filepath.Join(f.Dir, strings.NewReplacer("{os}", goos, "{arch}", arch).Replace(b.Output))
			if err := copyExecutable(src, dst); err != nil {
				return nil, fmt.Errorf("%s's output for %s: %w", b.Command, p, err)
			}
		}
		exes = append(exes, connectorhost.Executable{Platform: p, Path: dst})
	}
	return exes, nil
}

func copyExecutable(src, dst string) error {
	in, err := os.Open(src) //nolint:gosec // the author's build output
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755) //nolint:gosec // an executable
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

// inlineManifest reads a DeclarativeSource document and returns its spec,
// as canonical JSON, once the whole document loads.
func inlineManifest(path string) ([]byte, error) {
	b, err := os.ReadFile(path) //nolint:gosec // the author's own file
	if err != nil {
		return nil, err
	}
	if _, err := declarative.Load(b); err != nil {
		return nil, err
	}
	var doc struct {
		Kind string `yaml:"kind"`
		Spec any    `yaml:"spec"`
	}
	if err := yaml.Unmarshal(b, &doc); err != nil {
		return nil, err
	}
	if doc.Kind != spec.KindDeclarativeSource || doc.Spec == nil {
		return nil, errors.New("it isn't a DeclarativeSource document")
	}
	return json.Marshal(doc.Spec)
}
