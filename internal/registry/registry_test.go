package registry

import (
	"context"
	"errors"
	"io"
	"log"
	"net/http/httptest"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/go-containerregistry/pkg/name"
	ggcr "github.com/google/go-containerregistry/pkg/registry"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	"github.com/unsuitable001/openetl/internal/netguard"
)

func testRegistry(t *testing.T) (*Client, string) {
	t.Helper()
	srv := httptest.NewServer(ggcr.New(ggcr.Logger(log.New(io.Discard, "", 0))))
	t.Cleanup(srv.Close)
	c := &Client{Policy: netguard.Policy{Allow: []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}}}
	return c, strings.TrimPrefix(srv.URL, "http://")
}

func writeExe(t *testing.T, dir, name, content string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o755); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestPushResolvePull(t *testing.T) {
	ctx := context.Background()
	c, host := testRegistry(t)
	tmp := t.TempDir()
	exes := []Executable{
		{Platform: "linux/amd64", Path: writeExe(t, filepath.Join(tmp, "amd64"), "source-x", "amd64")},
		{Platform: "darwin/arm64", Path: writeExe(t, filepath.Join(tmp, "arm64"), "source-x", "arm64")},
	}
	ref := host + "/connectors/source-x:0.1.0"
	pinned, err := c.Push(ctx, ref, exes)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(pinned, host+"/connectors/source-x@sha256:") {
		t.Fatalf("pinned %s", pinned)
	}
	again, err := c.Push(ctx, ref, exes)
	if err != nil || again != pinned {
		t.Fatalf("pushing the same executables again gave %s, %v; want %s", again, err, pinned)
	}

	img, err := c.Resolve(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if img.Ref != pinned || strings.Join(img.Platforms, ",") != "darwin/arm64,linux/amd64" {
		t.Fatalf("resolved %+v", img)
	}
	if img, err := c.Resolve(ctx, pinned); err != nil || img.Ref != pinned {
		t.Fatalf("resolving the pinned ref gave %+v, %v", img, err)
	}

	cache := filepath.Join(tmp, "cache")
	exe, err := c.Pull(ctx, pinned, "darwin/arm64", cache)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Base(exe) != "source-x" {
		t.Errorf("pulled %s; want the executable's own name", exe)
	}
	b, err := os.ReadFile(exe)
	if err != nil || string(b) != "arm64" {
		t.Fatalf("pulled %q, %v", b, err)
	}
	if fi, _ := os.Stat(exe); fi.Mode()&0o111 == 0 {
		t.Errorf("mode %v isn't executable", fi.Mode())
	}
	if exe2, err := c.Pull(ctx, pinned, "darwin/arm64", cache); err != nil || exe2 != exe {
		t.Fatalf("second pull gave %s, %v", exe2, err)
	}
	ents, _ := os.ReadDir(cache)
	if len(ents) != 1 {
		t.Errorf("cache has %d entries, want 1", len(ents))
	}

	var np *NoPlatformError
	if _, err := c.Pull(ctx, pinned, "windows/amd64", cache); !errors.As(err, &np) {
		t.Errorf("pull for a missing platform: %v", err)
	}
	if _, err := c.Pull(ctx, ref, "darwin/arm64", cache); err == nil {
		t.Error("pulled by tag")
	}
}

func TestSingleImage(t *testing.T) {
	ctx := context.Background()
	c, host := testRegistry(t)
	img, err := executableImage(writeExe(t, t.TempDir(), "destination-x", "x"), "linux", "arm64")
	if err != nil {
		t.Fatal(err)
	}
	tag, err := name.NewTag(host + "/destination-x:1")
	if err != nil {
		t.Fatal(err)
	}
	if err := remote.Write(tag, img, c.options(ctx)...); err != nil {
		t.Fatal(err)
	}
	got, err := c.Resolve(ctx, tag.String())
	if err != nil || strings.Join(got.Platforms, ",") != "linux/arm64" {
		t.Fatalf("resolved %+v, %v", got, err)
	}
	if _, err := c.Pull(ctx, got.Ref, "linux/arm64", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pull(ctx, got.Ref, "linux/amd64", t.TempDir()); err == nil {
		t.Error("pulled an image for another platform")
	}
}

func TestPolicy(t *testing.T) {
	c, host := testRegistry(t)
	c.Policy = netguard.Policy{}
	_, err := c.Resolve(context.Background(), host+"/x:1")
	var blocked *netguard.BlockedError
	if !errors.As(err, &blocked) {
		t.Fatalf("resolve on loopback: %v; want netguard to refuse", err)
	}
}

func TestStrictRefs(t *testing.T) {
	c := &Client{}
	for _, ref := range []string{"postgres", "ghcr.io/x/y", "x:1"} {
		if _, err := c.Resolve(context.Background(), ref); err == nil {
			t.Errorf("resolved %q, which names no registry or no tag", ref)
		}
	}
}
