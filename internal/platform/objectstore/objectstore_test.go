package objectstore_test

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"gocloud.dev/blob"

	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

func TestOpen(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "missing")
	for _, loc := range []string{dir, "file://" + filepath.ToSlash(dir), "mem://"} {
		t.Run(loc, func(t *testing.T) {
			b, err := objectstore.Open(t.Context(), loc)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = b.Close() }()
			roundTrip(t, b)
		})
	}
	if _, err := os.Stat(filepath.Join(dir, "a", "b.txt")); err != nil {
		t.Errorf("the directory bucket didn't write under its directory: %v", err)
	}
	for _, loc := range []string{"", "gs://bucket", "file://host/dir", "s3://"} {
		if b, err := objectstore.Open(t.Context(), loc); err == nil {
			_ = b.Close()
			t.Errorf("opened %q", loc)
		}
	}
}

func roundTrip(t *testing.T, b *blob.Bucket) {
	t.Helper()
	ctx := context.Background()
	if err := b.WriteAll(ctx, "a/b.txt", []byte("hello"), nil); err != nil {
		t.Fatal(err)
	}
	got, err := b.ReadAll(ctx, "a/b.txt")
	if err != nil || string(got) != "hello" {
		t.Fatalf("ReadAll = %q, %v", got, err)
	}
	it := b.List(&blob.ListOptions{Prefix: "a/"})
	obj, err := it.Next(ctx)
	if err != nil || obj.Key != "a/b.txt" {
		t.Fatalf("List: %+v, %v", obj, err)
	}
	if _, err := it.Next(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("List: a second object, or %v", err)
	}
}
