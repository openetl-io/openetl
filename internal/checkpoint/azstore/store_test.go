//go:build integration

package azstore_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/azstore"
	"github.com/openetl-io/openetl/internal/checkpoint/checkpointtest"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// Azurite's well-known development account.
const (
	account = "devstoreaccount1"
	key     = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)

// Runs against the compose Azurite (make deps-up), or the one
// OPENETL_TEST_AZURE_ENDPOINT names.
func TestStore(t *testing.T) {
	t.Setenv("AZURE_STORAGE_KEY", key)
	checkpointtest.TestStore(t, func(t *testing.T) checkpoint.Store {
		u, err := url.Parse("azblob://" + name() + "?account=" + account + "&endpoint=" + url.QueryEscape(endpoint()))
		if err != nil {
			t.Fatal(err)
		}
		c, err := objectstore.AzureClient(u)
		if err != nil {
			t.Fatal(err)
		}
		create(t, c)
		return azstore.New(c)
	})
}

func TestLostResponseIsUnknownOutcome(t *testing.T) {
	ctx := t.Context()
	drop := &dropResponses{}
	cred, err := container.NewSharedKeyCredential(account, key)
	if err != nil {
		t.Fatal(err)
	}
	c, err := container.NewClientWithSharedKeyCredential(endpoint()+"/"+name(), cred, &container.ClientOptions{
		ClientOptions: azcore.ClientOptions{Transport: drop},
	})
	if err != nil {
		t.Fatal(err)
	}
	create(t, c)
	s := azstore.New(c)

	drop.next.Store(true)
	_, err = s.Create(ctx, "k", []byte("one"))
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

func (d *dropResponses) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil || req.Method != http.MethodPut || req.URL.Query().Has("restype") || !d.next.CompareAndSwap(true, false) {
		return resp, err
	}
	_ = resp.Body.Close()
	return nil, io.ErrUnexpectedEOF // the connection dropped before the response arrived
}

func endpoint() string {
	return cmp.Or(os.Getenv("OPENETL_TEST_AZURE_ENDPOINT"), "http://127.0.0.1:10000/"+account)
}

func name() string { return "openetl-test-" + strings.ToLower(rand.Text())[:16] }

func create(t *testing.T, c *container.Client) {
	t.Helper()
	if _, err := c.Create(t.Context(), nil); err != nil {
		t.Fatalf("create container (is Azurite running? make deps-up): %v", err)
	}
	t.Cleanup(func() {
		if _, err := c.Delete(context.Background(), nil); err != nil {
			t.Errorf("delete container: %v", err)
		}
	})
}
