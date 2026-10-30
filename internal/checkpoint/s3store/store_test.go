//go:build integration

package s3store_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/checkpointtest"
	"github.com/unsuitable001/openetl/internal/checkpoint/s3store"
)

// The tests run against the SeaweedFS in deploy/compose/dev.yml (`make
// deps-up`), or the endpoint in OPENETL_TEST_S3_ENDPOINT, with anonymous
// access. Each store gets a new bucket.

func TestStore(t *testing.T) {
	checkpointtest.TestStore(t, func(t *testing.T) checkpoint.Store {
		c := client(nil)
		return s3store.New(c, bucket(t, c))
	})
}

// A conditional PUT that lands but whose response is lost must not be
// reported as a failed condition.
func TestLostResponseIsUnknownOutcome(t *testing.T) {
	ctx := t.Context()
	drop := &dropResponses{}
	c := client(drop)
	s := s3store.New(c, bucket(t, c))

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

// dropResponses sends the next PUT, then discards its response and reports a
// transport error, as if the connection broke after the server applied it.
type dropResponses struct {
	next atomic.Bool
}

func (d *dropResponses) Do(req *http.Request) (*http.Response, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil || req.Method != http.MethodPut || !d.next.CompareAndSwap(true, false) {
		return resp, err
	}
	_ = resp.Body.Close()
	return nil, errors.New("connection reset after the request was sent")
}

func client(hc s3.HTTPClient) *s3.Client {
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	return s3.New(s3.Options{
		Region:       "us-east-1",
		BaseEndpoint: &endpoint,
		UsePathStyle: true,
		Credentials:  aws.AnonymousCredentials{},
		HTTPClient:   hc,
	})
}

// bucket creates an empty bucket and removes it, with its objects, when t
// ends.
func bucket(t *testing.T, c *s3.Client) string {
	t.Helper()
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &name}); err != nil {
		t.Fatalf("create bucket (is the S3 store running? make deps-up): %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		keys, err := s3store.New(c, name).List(ctx, "")
		if err != nil {
			t.Errorf("list for cleanup: %v", err)
			return
		}
		for _, k := range keys {
			if _, err := c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &name, Key: &k}); err != nil {
				t.Errorf("delete %s: %v", k, err)
			}
		}
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &name}); err != nil {
			t.Errorf("delete bucket: %v", err)
		}
	})
	return name
}
