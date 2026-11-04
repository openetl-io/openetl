//go:build integration

package engineclient

import (
	"cmp"
	"crypto/rand"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// Stages in the compose SeaweedFS (make deps-up) or OPENETL_TEST_S3_ENDPOINT,
// anonymously, so the destination reads presigned URLs with range requests.
func TestRunAttemptIntoAStagedDestinationOnS3(t *testing.T) {
	endpoint := cmp.Or(os.Getenv("OPENETL_TEST_S3_ENDPOINT"), "http://127.0.0.1:8333")
	name := "openetl-test-" + strings.ToLower(rand.Text())[:16]
	loc := "s3://" + name + "/base?endpoint=" + url.QueryEscape(endpoint)
	u, err := url.Parse(loc)
	if err != nil {
		t.Fatal(err)
	}
	c, err := objectstore.S3Client(u)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.CreateBucket(t.Context(), &s3.CreateBucketInput{Bucket: &name}); err != nil {
		t.Fatalf("create bucket (is the S3 store running? make deps-up): %v", err)
	}
	runStaged(t, loc)
}
