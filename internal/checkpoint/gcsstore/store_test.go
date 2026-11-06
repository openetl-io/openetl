//go:build integration

package gcsstore_test

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"cloud.google.com/go/storage"
	"google.golang.org/api/option"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/checkpointtest"
	"github.com/openetl-io/openetl/internal/checkpoint/gcsstore"
)

// Runs against the compose fake-gcs-server (make deps-up), or the emulator
// STORAGE_EMULATOR_HOST names.
func TestStore(t *testing.T) {
	c := client(t, nil)
	checkpointtest.TestStore(t, func(t *testing.T) checkpoint.Store {
		return gcsstore.New(c, bucket(t, c))
	})
}

func TestLostResponseIsUnknownOutcome(t *testing.T) {
	ctx := t.Context()
	drop := &dropResponses{}
	c := client(t, &http.Client{Transport: drop})
	s := gcsstore.New(c, bucket(t, c))

	drop.next.Store(true)
	_, err := s.Create(ctx, "k", []byte("one"))
	if err == nil || errors.Is(err, checkpoint.ErrPrecondition) {
		t.Fatalf("Create with a lost response: err = %v, want an unknown outcome", err)
	}
	data, v, err := s.Get(ctx, "k")
	if err != nil || string(data) != "one" {
		t.Fatalf("Get = %q, %v; want the create to have landed", data, err)
	}

	drop.next.Store(true)
	_, err = s.Swap(ctx, "k", []byte("two"), v)
	if err == nil || errors.Is(err, checkpoint.ErrPrecondition) {
		t.Fatalf("Swap with a lost response: err = %v, want an unknown outcome", err)
	}
	if data, _, err := s.Get(ctx, "k"); err != nil || string(data) != "two" {
		t.Fatalf("Get = %q, %v; want the swap to have landed", data, err)
	}
}

// dropResponses applies the next upload and then reports a transport error.
type dropResponses struct {
	next atomic.Bool
}

func (d *dropResponses) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil || req.Method != http.MethodPost || !strings.HasPrefix(req.URL.Path, "/upload/") || !d.next.CompareAndSwap(true, false) {
		return resp, err
	}
	_ = resp.Body.Close()
	return nil, io.ErrUnexpectedEOF // the connection dropped before the response arrived
}

func client(t *testing.T, hc *http.Client) *storage.Client {
	t.Helper()
	if os.Getenv("STORAGE_EMULATOR_HOST") == "" {
		t.Setenv("STORAGE_EMULATOR_HOST", "127.0.0.1:4443")
	}
	var opts []option.ClientOption
	if hc != nil {
		opts = append(opts, option.WithHTTPClient(hc))
	}
	c, err := storage.NewClient(t.Context(), opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func bucket(t *testing.T, c *storage.Client) string {
	t.Helper()
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	if err := c.Bucket(name).Create(t.Context(), "openetl", nil); err != nil {
		t.Fatalf("create bucket (is the GCS emulator running? make deps-up): %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		keys, err := gcsstore.New(c, name).List(ctx, "")
		if err != nil {
			t.Errorf("list for cleanup: %v", err)
			return
		}
		for _, k := range keys {
			if err := c.Bucket(name).Object(k).Delete(ctx); err != nil {
				t.Errorf("delete %s: %v", k, err)
			}
		}
		if err := c.Bucket(name).Delete(ctx); err != nil {
			t.Errorf("delete bucket: %v", err)
		}
	})
	return name
}
