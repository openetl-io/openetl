// Package dbt bundles dbt projects, and runs them against a pipeline's
// destination for its post-sync steps.
package dbt

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// MediaType is a bundle's, as an artifact.
const MediaType = "application/vnd.openetl.dbt.v1.tar+gzip"

// Limits on a bundle. ADR-0015 caps it at 16 MiB compressed.
const (
	MaxBundle   = 16 << 20
	maxUnpacked = 256 << 20
	maxFiles    = 20000
)

// Project is what openetl reads from dbt_project.yml.
type Project struct {
	Name string
	// The profile dbt looks for; the runner writes it.
	Profile string
}

// Pack bundles the dbt project in dir: its files, without hidden files,
// target/, logs/ and profiles.yml, which can hold credentials. Vendored
// packages in dbt_packages/ are kept, since runners don't run dbt deps.
func Pack(dir string) ([]byte, error) {
	files := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil || rel == "." {
			return err
		}
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(d.Name(), ".") || rel == "target" || rel == "logs" || rel == "profiles.yml" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		switch {
		case d.IsDir():
			return nil
		case !d.Type().IsRegular():
			return fmt.Errorf("%s isn't a regular file; bundles hold only files", rel)
		}
		b, err := os.ReadFile(p) //nolint:gosec // under the project the user names
		if err != nil {
			return err
		}
		files[rel] = b
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err := check(files); err != nil {
		return nil, err
	}
	return encode(files)
}

// Canonical checks a bundle and encodes its files again, so its digest
// depends only on them.
func Canonical(bundle []byte) ([]byte, *Project, error) {
	files, err := decode(bundle)
	if err != nil {
		return nil, nil, err
	}
	p, err := check(files)
	if err != nil {
		return nil, nil, err
	}
	b, err := encode(files)
	return b, p, err
}

// Unpack writes a bundle's files under dir.
func Unpack(bundle []byte, dir string) (*Project, error) {
	files, err := decode(bundle)
	if err != nil {
		return nil, err
	}
	p, err := check(files)
	if err != nil {
		return nil, err
	}
	for name, b := range files {
		dst := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(dst), 0o750); err != nil {
			return nil, err
		}
		if err := os.WriteFile(dst, b, 0o600); err != nil {
			return nil, err
		}
	}
	return p, nil
}

// check checks the files' names and sizes, and reads dbt_project.yml.
func check(files map[string][]byte) (*Project, error) {
	if len(files) > maxFiles {
		return nil, fmt.Errorf("the project has %d files, more than %d", len(files), maxFiles)
	}
	total := 0
	for name, b := range files {
		if !fs.ValidPath(name) || strings.Contains(name, `\`) {
			return nil, fmt.Errorf("%q isn't a relative path inside the project", name)
		}
		total += len(b)
	}
	if total > maxUnpacked {
		return nil, fmt.Errorf("the project's files hold %d bytes, more than %d", total, maxUnpacked)
	}
	conf, ok := files["dbt_project.yml"]
	if !ok {
		return nil, errors.New("the project has no dbt_project.yml at its root")
	}
	var p struct {
		Name    string `yaml:"name"`
		Profile string `yaml:"profile"`
	}
	if err := yaml.Unmarshal(conf, &p); err != nil {
		return nil, fmt.Errorf("dbt_project.yml: %w", err)
	}
	switch {
	case p.Name == "":
		return nil, errors.New("dbt_project.yml has no name")
	case p.Profile == "" || strings.ContainsAny(p.Profile, "{}%"):
		return nil, errors.New("dbt_project.yml needs a profile, a plain name: the runner writes a profile by that name for the pipeline's destination")
	}
	return &Project{Name: p.Name, Profile: p.Profile}, nil
}

// encode writes files as a gzipped tar: sorted, as regular files with no
// times, owners or modes, so the same files always give the same bytes.
func encode(files map[string][]byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, name := range slices.Sorted(maps.Keys(files)) {
		b := files[name]
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: name, Mode: 0o644, Size: int64(len(b))}); err != nil {
			return nil, err
		}
		if _, err := tw.Write(b); err != nil {
			return nil, err
		}
	}
	if err := tw.Close(); err != nil {
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	if buf.Len() > MaxBundle {
		return nil, fmt.Errorf("the bundle is %d bytes compressed, more than %d", buf.Len(), MaxBundle)
	}
	return buf.Bytes(), nil
}

// decode reads a gzipped tar of regular files and directories.
func decode(bundle []byte) (map[string][]byte, error) {
	if len(bundle) > MaxBundle {
		return nil, fmt.Errorf("the bundle is %d bytes, more than %d", len(bundle), MaxBundle)
	}
	zr, err := gzip.NewReader(bytes.NewReader(bundle))
	if err != nil {
		return nil, fmt.Errorf("the bundle isn't a gzipped tar: %w", err)
	}
	tr := tar.NewReader(zr)
	files := map[string][]byte{}
	total := int64(0)
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("the bundle isn't a gzipped tar: %w", err)
		}
		name := path.Clean(strings.TrimPrefix(h.Name, "./"))
		switch h.Typeflag {
		case tar.TypeDir:
			continue
		case tar.TypeReg:
		default:
			return nil, fmt.Errorf("%s in the bundle isn't a regular file", h.Name)
		}
		if _, dup := files[name]; dup {
			return nil, fmt.Errorf("%s is in the bundle twice", name)
		}
		if total += h.Size; total > maxUnpacked || len(files) >= maxFiles {
			return nil, fmt.Errorf("the bundle unpacks to more than %d bytes or %d files", maxUnpacked, maxFiles)
		}
		b, err := io.ReadAll(io.LimitReader(tr, h.Size))
		if err != nil {
			return nil, err
		}
		files[name] = b
	}
	return files, nil
}
