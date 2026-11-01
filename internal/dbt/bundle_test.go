package dbt

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const projectYAML = "name: shop\nprofile: shop\n"

func writeFiles(t *testing.T, dir string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func tarGz(t *testing.T, entries ...*tar.Header) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	for _, h := range entries {
		body := h.Linkname
		if h.Typeflag == tar.TypeReg {
			h.Linkname, h.Size = "", int64(len(body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			if _, err := tw.Write([]byte(body)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// file is a regular file's header; its content rides in Linkname until
// tarGz writes it.
func file(name, content string) *tar.Header {
	return &tar.Header{Typeflag: tar.TypeReg, Name: name, Linkname: content, Mode: 0o755, ModTime: time.Now(), Uname: "someone"}
}

func TestPackLeavesOutWhatIsNotTheProject(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{
		"dbt_project.yml":                 projectYAML,
		"models/orders.sql":               "select 1",
		"models/staging/stg.sql":          "select 2",
		"dbt_packages/utils/macros/a.sql": "{% macro a() %}{% endmacro %}",
		"profiles.yml":                    "shop: {password: secret}",
		"target/manifest.json":            "{}",
		"logs/dbt.log":                    "",
		".git/HEAD":                       "ref",
		".user.yml":                       "id: 1",
		"models/.hidden.sql":              "select 3",
		"models/profiles.yml":             "kept: true",
	})
	b, err := Pack(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := t.TempDir()
	if _, err := Unpack(b, out); err != nil {
		t.Fatal(err)
	}
	var got []string
	_ = filepath.WalkDir(out, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			rel, _ := filepath.Rel(out, p)
			got = append(got, filepath.ToSlash(rel))
		}
		return err
	})
	want := []string{"dbt_packages/utils/macros/a.sql", "dbt_project.yml", "models/orders.sql", "models/profiles.yml", "models/staging/stg.sql"}
	if !slices.Equal(got, want) {
		t.Errorf("bundled %v, want %v", got, want)
	}
	if b, _ := os.ReadFile(filepath.Join(out, "models", "orders.sql")); string(b) != "select 1" {
		t.Errorf("models/orders.sql = %q", b)
	}
}

// The same files give the same bundle, however they're stored.
func TestCanonical(t *testing.T) {
	dir := t.TempDir()
	writeFiles(t, dir, map[string]string{"dbt_project.yml": projectYAML, "models/a.sql": "select 1", "models/b.sql": "select 2"})
	packed, err := Pack(dir)
	if err != nil {
		t.Fatal(err)
	}
	other := tarGz(t,
		&tar.Header{Typeflag: tar.TypeDir, Name: "./models/", Mode: 0o755},
		file("./models/b.sql", "select 2"),
		file("models/a.sql", "select 1"),
		file("./dbt_project.yml", projectYAML),
	)
	got, p, err := Canonical(other)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, packed) {
		t.Error("the same files in another tar gave another bundle")
	}
	if p.Name != "shop" || p.Profile != "shop" {
		t.Errorf("project = %+v", p)
	}
	again, _, err := Canonical(got)
	if err != nil || !bytes.Equal(again, got) {
		t.Errorf("a canonical bundle changed: %v", err)
	}
}

func TestCanonicalRefuses(t *testing.T) {
	ok := file("dbt_project.yml", projectYAML)
	for name, b := range map[string][]byte{
		"not gzip":     []byte("plain text"),
		"parent":       tarGz(t, ok, file("../evil.sql", "")),
		"absolute":     tarGz(t, ok, file("/etc/evil.sql", "")),
		"backslash":    tarGz(t, ok, file(`models\evil.sql`, "")),
		"symlink":      tarGz(t, ok, &tar.Header{Typeflag: tar.TypeSymlink, Name: "models/a.sql", Linkname: "/etc/passwd"}),
		"hard link":    tarGz(t, ok, &tar.Header{Typeflag: tar.TypeLink, Name: "models/a.sql", Linkname: "dbt_project.yml"}),
		"twice":        tarGz(t, ok, file("./dbt_project.yml", projectYAML)),
		"no project":   tarGz(t, file("models/a.sql", "select 1")),
		"no profile":   tarGz(t, file("dbt_project.yml", "name: shop\n")),
		"jinja":        tarGz(t, file("dbt_project.yml", "name: shop\nprofile: \"{{ env_var('P') }}\"\n")),
		"no name":      tarGz(t, file("dbt_project.yml", "profile: shop\n")),
		"invalid yaml": tarGz(t, file("dbt_project.yml", "name: [\n")),
	} {
		if _, _, err := Canonical(b); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestBundleLimits(t *testing.T) {
	if _, _, err := Canonical(make([]byte, MaxBundle+1)); err == nil {
		t.Error("accepted a bundle over MaxBundle")
	}
	// Each compresses to little, but unpacks past a limit.
	big := func(tw *tar.Writer) error {
		if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: "seeds/big.csv", Mode: 0o644, Size: maxUnpacked + 1}); err != nil {
			return err
		}
		_, err := io.CopyN(tw, zeros{}, maxUnpacked+1)
		return err
	}
	many := func(tw *tar.Writer) error {
		for i := range maxFiles + 1 {
			if err := tw.WriteHeader(&tar.Header{Typeflag: tar.TypeReg, Name: fmt.Sprintf("models/m%d.sql", i), Mode: 0o644}); err != nil {
				return err
			}
		}
		return nil
	}
	for name, write := range map[string]func(*tar.Writer) error{"bytes": big, "files": many} {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		tw := tar.NewWriter(zw)
		if err := write(tw); err != nil {
			t.Fatal(err)
		}
		_ = tw.Close()
		_ = zw.Close()
		if _, _, err := Canonical(buf.Bytes()); err == nil || !strings.Contains(err.Error(), "unpacks to more than") {
			t.Errorf("%s: %v", name, err)
		}
	}
}

type zeros struct{}

func (zeros) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
