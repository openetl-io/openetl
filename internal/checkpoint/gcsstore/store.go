// Package gcsstore is a checkpoint.Store on Google Cloud Storage. Versions
// are object generations: Create writes with ifGenerationMatch=0, and Swap
// with ifGenerationMatch on the generation read.
package gcsstore

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strconv"

	"cloud.google.com/go/storage"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

type Store struct {
	bucket *storage.BucketHandle
}

// New returns a Store on bucket. The bucket must exist.
func New(client *storage.Client, bucket string) *Store {
	return &Store{bucket: client.Bucket(bucket)}
}

var _ checkpoint.Store = (*Store)(nil)

func (s *Store) Get(ctx context.Context, key string) ([]byte, checkpoint.Version, error) {
	r, err := s.bucket.Object(key).NewReader(ctx)
	if errors.Is(err, storage.ErrObjectNotExist) {
		return nil, "", checkpoint.ErrNotFound
	}
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: gcs get %s: %w", key, err)
	}
	defer func() { _ = r.Close() }()
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: gcs get %s: %w", key, err)
	}
	return data, version(r.Attrs.Generation), nil
}

func (s *Store) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	return s.put(ctx, key, data, storage.Conditions{DoesNotExist: true})
}

func (s *Store) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	gen, err := strconv.ParseInt(string(prev), 10, 64)
	if err != nil || gen <= 0 {
		return "", checkpoint.ErrPrecondition // no object has that version
	}
	return s.put(ctx, key, data, storage.Conditions{GenerationMatch: gen})
}

// put sends a conditional upload exactly once, in a single request: a retry
// of an upload that landed would fail its own condition. Anything but a 412
// (GCS's answer for a missing object too) is an unknown outcome.
func (s *Store) put(ctx context.Context, key string, data []byte, cond storage.Conditions) (checkpoint.Version, error) {
	w := s.bucket.Object(key).If(cond).Retryer(storage.WithPolicy(storage.RetryNever)).NewWriter(ctx)
	w.ChunkSize = 0
	_, err := w.Write(data)
	if cerr := w.Close(); err == nil {
		err = cerr
	}
	if status(err) == http.StatusPreconditionFailed {
		return "", checkpoint.ErrPrecondition
	}
	if err != nil {
		return "", fmt.Errorf("checkpoint: gcs put %s: %w", key, err)
	}
	return version(w.Attrs().Generation), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	// A retried delete that already landed reads as not found.
	err := s.bucket.Object(key).Retryer(storage.WithPolicy(storage.RetryAlways)).Delete(ctx)
	if err != nil && !errors.Is(err, storage.ErrObjectNotExist) {
		return fmt.Errorf("checkpoint: gcs delete %s: %w", key, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	q := &storage.Query{Prefix: prefix}
	if err := q.SetAttrSelection([]string{"Name"}); err != nil {
		return nil, err
	}
	var keys []string
	it := s.bucket.Objects(ctx, q)
	for {
		o, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("checkpoint: gcs list %s: %w", prefix, err)
		}
		keys = append(keys, o.Name)
	}
	slices.Sort(keys)
	return keys, nil
}

func version(gen int64) checkpoint.Version {
	return checkpoint.Version(strconv.FormatInt(gen, 10))
}

// status returns 0 if err is nil or has no HTTP response.
func status(err error) int {
	if ge, ok := errors.AsType[*googleapi.Error](err); ok {
		return ge.Code
	}
	return 0
}
