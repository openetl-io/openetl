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

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// Runs against the compose SeaweedFS (make deps-up) or
// OPENETL_TEST_S3_ENDPOINT, anonymously.
func TestOpenS3(t *testing.T) {
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	loc := "s3://" + name + "?anonymous=true&endpoint=" + url.QueryEscape(endpoint)
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := objectstore.S3Client(t.Context(), u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &name}); err != nil {
		t.Fatalf("create bucket (is the S3 store running? make deps-up): %v", err)
	}
	b, err := objectstore.Open(t.Context(), loc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = b.Delete(ctx, "a/b.txt")
		_ = b.Close()
		_, _ = c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &name})
	})
	roundTrip(t, b)
}
