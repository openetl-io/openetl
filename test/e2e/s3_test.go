//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5"
	"gocloud.dev/blob"

	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// dropBucket empties an S3 bucket and deletes it, for a test's cleanup.
// SeaweedFS gives each bucket a volume, and caps their number.
func dropBucket(t *testing.T, c *s3.Client, bucket string) {
	ctx := context.Background()
	pages := s3.NewListObjectsV2Paginator(c, &s3.ListObjectsV2Input{Bucket: &bucket})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			t.Logf("list bucket %s: %v", bucket, err)
			return
		}
		for _, o := range page.Contents {
			_, _ = c.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &bucket, Key: o.Key})
		}
	}
	if _, err := c.DeleteBucket(ctx, &s3.DeleteBucketInput{Bucket: &bucket}); err != nil {
		t.Logf("delete bucket %s: %v", bucket, err)
	}
}

// s3Dest returns a destination spec writing under a new prefix of the S3
// store's s3-destination bucket, and the bucket at that prefix. It skips the
// test without destination-s3.
func s3Dest(t *testing.T) (string, *blob.Bucket) {
	t.Helper()
	if _, err := os.Stat(filepath.Join(env.connectors, "destination-s3")); err != nil {
		t.Skip("OPENETL_CONNECTORS has no destination-s3")
	}
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPut, s3Endpoint()+"/s3-destination", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("create the bucket (make deps-up): %v", err)
	}
	_ = res.Body.Close()
	prefix := strings.ToLower(rand.Text())[:10]
	b, err := objectstore.Open(t.Context(), "s3://s3-destination/"+prefix+"?endpoint="+url.QueryEscape(s3Endpoint()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		it := b.List(nil)
		for {
			obj, err := it.Next(ctx)
			if err != nil {
				break
			}
			_ = b.Delete(ctx, obj.Key)
		}
		_ = b.Close()
	})
	key := setSecret(t, "openetl")
	return fmt.Sprintf(`{"connector": "s3", "config": {"url": %q, "endpoint": %q, "region": "us-east-1",
		"accessKeyId": {"secret": %q}, "secretAccessKey": {"secret": %q}}}`, "s3://s3-destination/"+prefix, s3Endpoint(), key, key), b
}

func s3Docs(name, src, dst string) []string {
	return []string{
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Source", "metadata": {"name": %q}, "spec": %s}`, name, src),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Destination", "metadata": {"name": %q}, "spec": %s}`, name, dst),
		fmt.Sprintf(`{"apiVersion": "openetl.io/v1alpha1", "kind": "Pipeline", "metadata": {"name": %q},
			"spec": {"schedule": {"every": "720h"}, "source": {"ref": %q}, "destination": {"ref": %q}, "delivery": {"commit": {"interval": "1s"}}}}`, name, name, name),
	}
}

// s3IDs returns the ids in a table's Parquet files.
func s3IDs(t *testing.T, b *blob.Bucket, table string) ([]int64, error) {
	t.Helper()
	var ids []int64
	it := b.List(&blob.ListOptions{Prefix: table + "/"})
	for {
		obj, err := it.Next(t.Context())
		if err != nil {
			break
		}
		data, err := b.ReadAll(t.Context(), obj.Key)
		if err != nil {
			return nil, err
		}
		pf, err := file.NewParquetReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: 1 << 16}, memory.DefaultAllocator)
		if err != nil {
			return nil, err
		}
		tbl, err := fr.ReadTable(t.Context())
		_ = pf.Close()
		if err != nil {
			return nil, err
		}
		col := tbl.Column(tbl.Schema().FieldIndices("id")[0])
		for _, c := range col.Data().Chunks() {
			ids = append(ids, c.(*array.Int64).Int64Values()...)
		}
		tbl.Release()
	}
	return ids, nil
}

// checkS3 fails unless each table has want rows, each id once.
func checkS3(t *testing.T, b *blob.Bucket, want int, tables ...string) {
	t.Helper()
	for _, name := range tables {
		ids, err := s3IDs(t, b, name)
		if err != nil {
			t.Fatal(err)
		}
		seen := map[int64]bool{}
		for _, id := range ids {
			seen[id] = true
		}
		if len(ids) != want || len(seen) != want {
			t.Errorf("%s: %d rows and %d distinct ids, want %d each", name, len(ids), len(seen), want)
		}
	}
}

func TestPostgresToS3(t *testing.T) {
	startRunner(t)
	name := unique("postgres-s3")
	ns := schema(name)
	dst, b := s3Dest(t)
	sch := pgx.Identifier{ns}.Sanitize()
	run(t, "CREATE SCHEMA "+sch,
		"CREATE TABLE "+sch+".items (id bigint PRIMARY KEY, name text, price numeric(10, 2), at timestamptz, uid uuid)",
		"INSERT INTO "+sch+".items SELECT i, 'item ' || i, i / 100.0, now(), gen_random_uuid() FROM generate_series(1, 5000) i",
		"CREATE TABLE "+sch+".events (id bigint, kind text)",
		"INSERT INTO "+sch+".events SELECT i, 'k' || (i % 3) FROM generate_series(1, 700) i")
	src := fmt.Sprintf(`{"connector": "postgres", "config": {"dsn": {"secret": "e2e-dsn"}, "schema": %q, "chunkRows": 1000}}`, ns)
	apply(t, s3Docs(name, src, dst)...)
	if status, stats := waitSync(t, name, 90*time.Second); status != "succeeded" {
		t.Fatalf("sync %s: %s", status, stats)
	}
	checkS3(t, b, 5000, "items")
	checkS3(t, b, 700, "events")
	if ok, err := b.Exists(t.Context(), "_openetl"); err == nil && ok {
		t.Error("an object is named _openetl")
	}
}

// Files that land while the runner dies, with its destination process.
func TestS3RunnerKilledMidSync(t *testing.T) {
	first := startRunner(t)
	name := unique("s3-runner-killed")
	dst, b := s3Dest(t)
	apply(t, s3Docs(name, icebergSlowFaker, dst)...)
	child := first.child(t, "destination-s3")
	eventually(t, 30*time.Second, "a committed epoch", func() bool {
		ids, err := s3IDs(t, b, "users")
		return err == nil && len(ids) > 0
	})
	first.kill(t)
	eventually(t, 10*time.Second, "the orphaned destination to exit", func() bool {
		return errors.Is(syscall.Kill(child, 0), syscall.ESRCH)
	})
	startRunner(t)
	status, stats := waitSync(t, name, 90*time.Second)
	finishedLater(t, status, stats)
	checkS3(t, b, 2000, "users", "purchases")
	staged := 0
	it := b.List(&blob.ListOptions{Prefix: "_openetl/"})
	for {
		obj, err := it.Next(context.Background())
		if err != nil {
			break
		}
		if strings.Contains(obj.Key, "/staging/") {
			staged++
		}
	}
	if staged != 0 {
		t.Errorf("%d staged files left", staged)
	}
}
