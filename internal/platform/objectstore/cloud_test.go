//go:build integration

package objectstore_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// Runs against the compose fake-gcs-server, or STORAGE_EMULATOR_HOST.
func TestOpenGCS(t *testing.T) {
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:4443")
	}
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	u := &url.URL{Scheme: "gs", Host: name}
	c, err := objectstore.GCSClient(t.Context(), u)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Bucket(name).Create(t.Context(), "openetl", nil); err != nil {
		t.Fatalf("create bucket (is the GCS emulator running? make deps-up): %v", err)
	}
	b, err := objectstore.Open(t.Context(), u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = b.Delete(ctx, "a/b.txt")
		_ = b.Close()
		_ = c.Bucket(name).Delete(ctx)
	})
	roundTrip(t, b)
}

// Runs against the compose Azurite, or OPENETL_TEST_AZURE_ENDPOINT, with
// its well-known development account.
func TestOpenAzure(t *testing.T) {
	t.Setenv("AZURE_STORAGE_ACCOUNT", "devstoreaccount1")
	t.Setenv("AZURE_STORAGE_KEY", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), "http://127.0.0.1:10000/devstoreaccount1")
	loc := "azblob://openetl-test-" + strings.ToLower(rand.Text())[:16] + "?endpoint=" + url.QueryEscape(endpoint)
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := objectstore.AzureClient(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Create(t.Context(), nil); err != nil {
		t.Fatalf("create container (is Azurite running? make deps-up): %v", err)
	}
	b, err := objectstore.Open(t.Context(), loc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = b.Close()
		_, _ = c.Delete(context.Background(), nil)
	})
	roundTrip(t, b)
}
