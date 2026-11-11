// Package install runs install.sh, the CLI's installer, against a release
// served from a directory.
package install

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const script = "../../install.sh"

func TestInstall(t *testing.T) {
	platform := runtime.GOOS + "_" + runtime.GOARCH
	switch platform {
	case "darwin_amd64", "darwin_arm64", "linux_amd64", "linux_arm64":
	default:
		t.Skipf("install.sh has no CLI for %s", platform)
	}
	archive := "openetl-cli_9.9.9-alpha_" + platform + ".tar.gz"

	t.Run("installs the version asked for", func(t *testing.T) {
		url := release(t, "v9.9.9-alpha", archive, "")
		bin := filepath.Join(t.TempDir(), "bin")
		out, err := install(t, url, "9.9.9-alpha", bin)
		if err != nil {
			t.Fatalf("install.sh failed: %v\n%s", err, out)
		}
		if !strings.Contains(out, "Installed openetl 9.9.9-alpha at "+filepath.Join(bin, "openetl")) {
			t.Errorf("install.sh said:\n%s", out)
		}
		if !strings.Contains(out, "is not on your PATH") {
			t.Errorf("install.sh didn't say %s isn't on PATH:\n%s", bin, out)
		}
		got, err := exec.Command(filepath.Join(bin, "openetl"), "version").Output()
		if err != nil || strings.TrimSpace(string(got)) != "9.9.9-alpha" {
			t.Errorf("the installed openetl says %q, %v", got, err)
		}
	})

	t.Run("takes a tag as the version, and replaces an older openetl", func(t *testing.T) {
		url := release(t, "v9.9.9-alpha", archive, "")
		bin := t.TempDir()
		if err := os.WriteFile(filepath.Join(bin, "openetl"), []byte("#!/bin/sh\necho old\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		if out, err := install(t, url, "v9.9.9-alpha", bin); err != nil {
			t.Fatalf("install.sh failed: %v\n%s", err, out)
		}
		got, err := exec.Command(filepath.Join(bin, "openetl"), "version").Output()
		if err != nil || strings.TrimSpace(string(got)) != "9.9.9-alpha" {
			t.Errorf("the installed openetl says %q, %v", got, err)
		}
	})

	for _, tc := range []struct {
		name, version, checksum, want string
	}{
		{"refuses an archive whose checksum differs", "9.9.9-alpha", strings.Repeat("0", 64), "but checksums.txt gives"},
		{"refuses a release with no checksum for the archive", "9.9.9-alpha", "none", "checksums.txt has no line for " + archive},
		{"fails on a version that isn't released", "9.9.8", "", "cannot download"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			url := release(t, "v9.9.9-alpha", archive, tc.checksum)
			bin := t.TempDir()
			out, err := install(t, url, tc.version, bin)
			if err == nil {
				t.Fatalf("install.sh succeeded:\n%s", out)
			}
			if !strings.Contains(out, tc.want) {
				t.Errorf("install.sh said %q, without %q", out, tc.want)
			}
			if _, err := os.Stat(filepath.Join(bin, "openetl")); !os.IsNotExist(err) {
				t.Errorf("install.sh left an openetl in %s: %v", bin, err)
			}
		})
	}
}

// install runs install.sh for version into dir, with releases at url, and
// returns what it wrote to stdout and stderr.
func install(t *testing.T, url, version, dir string) (string, error) {
	t.Helper()
	cmd := exec.Command("sh", script)
	cmd.Env = append(os.Environ(), "OPENETL_DOWNLOAD_URL="+url, "OPENETL_VERSION="+version, "OPENETL_INSTALL_DIR="+dir)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

// release serves a release with tag holding archive, a CLI that prints the
// version, and its checksums.txt. checksum is the archive's line in it: ""
// for its SHA-256, "none" for no line.
func release(t *testing.T, tag, archive, checksum string) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), tag)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	version := strings.TrimPrefix(tag, "v")
	top := strings.TrimSuffix(archive, ".tar.gz")
	content := tarball(t, map[string]string{
		top + "/openetl": "#!/bin/sh\necho " + version + "\n",
		top + "/LICENSE": "Apache License 2.0\n",
	})
	if err := os.WriteFile(filepath.Join(dir, archive), content, 0o600); err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(content)
	lines := "0000000000000000000000000000000000000000000000000000000000000000  openetl_" + version + "_linux_amd64.tar.gz\n"
	switch checksum {
	case "":
		lines += hex.EncodeToString(sum[:]) + "  " + archive + "\n"
	case "none":
	default:
		lines += checksum + "  " + archive + "\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "checksums.txt"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.FileServer(http.Dir(filepath.Dir(dir))))
	t.Cleanup(srv.Close)
	return srv.URL + "/"
}

// tarball is a gzipped tar of files, by path, each executable.
func tarball(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
