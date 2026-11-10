//go:build integration

package engineclient

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

// Stages in the compose SeaweedFS (make deps-up) or OPENETL_TEST_S3_ENDPOINT,
// anonymously, so the destination reads presigned URLs with range requests.
func TestRunAttemptIntoAStagedDestinationOnS3(t *testing.T) {
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	loc := "s3://" + name + "/base?anonymous=true&endpoint=" + url.QueryEscape(endpoint)
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
	// SeaweedFS gives each bucket a volume, and caps their number.
	t.Cleanup(func() {
		ctx := context.Background()
		pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &name})
		for pages.HasMorePages() {
			page, err := pages.NextPage(ctx)
			if err != nil {
				break
			}
			for _, o := range page.Contents {
				_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &name, Key: o.Key})
			}
		}
		if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &name}); err != nil {
			t.Logf("delete bucket %s: %v", name, err)
		}
	})
	runStaged(t, loc)
}
