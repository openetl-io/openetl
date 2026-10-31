// Package s3store is a checkpoint.Store on S3 and S3-compatible stores, using
// If-None-Match: * for Create and If-Match for Swap.
package s3store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/unsuitable001/openetl/internal/checkpoint"
)

type Store struct {
	client *s3.Client
	bucket string
}

// New returns a Store on bucket. The bucket must exist.
func New(client *s3.Client, bucket string) *Store {
	return &Store{client: client, bucket: bucket}
}

var _ checkpoint.Store = (*Store)(nil)

func (s *Store) Get(ctx context.Context, key string) ([]byte, checkpoint.Version, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{Bucket: &s.bucket, Key: &key})
	if status(err) == http.StatusNotFound {
		return nil, "", checkpoint.ErrNotFound
	}
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: s3 get %s: %w", key, err)
	}
	defer func() { _ = out.Body.Close() }()
	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: s3 get %s: %w", key, err)
	}
	return data, checkpoint.Version(aws.ToString(out.ETag)), nil
}

func (s *Store) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	return s.put(ctx, key, data, &s3.PutObjectInput{IfNoneMatch: aws.String("*")})
}

func (s *Store) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	return s.put(ctx, key, data, &s3.PutObjectInput{IfMatch: aws.String(string(prev))})
}

// put sends a conditional PUT exactly once: an SDK retry of a PUT that landed
// would fail its own condition. Anything but a 412 (including 409) is an
// unknown outcome.
func (s *Store) put(ctx context.Context, key string, data []byte, in *s3.PutObjectInput) (checkpoint.Version, error) {
	in.Bucket, in.Key, in.Body = &s.bucket, &key, bytes.NewReader(data)
	out, err := s.client.PutObject(ctx, in, func(o *s3.Options) { o.RetryMaxAttempts = 1 })
	switch status(err) {
	case 0:
	case http.StatusPreconditionFailed:
		return "", checkpoint.ErrPrecondition
	case http.StatusNotFound:
		// If-Match on a missing key. Real S3 and SeaweedFS answer 404.
		if in.IfMatch != nil {
			return "", checkpoint.ErrPrecondition
		}
	}
	if err != nil {
		return "", fmt.Errorf("checkpoint: s3 put %s: %w", key, err)
	}
	return checkpoint.Version(aws.ToString(out.ETag)), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	// S3 reports success when the key doesn't exist.
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &s.bucket, Key: &key}); err != nil {
		return fmt.Errorf("checkpoint: s3 delete %s: %w", key, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pages := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{Bucket: &s.bucket, Prefix: &prefix})
	for pages.HasMorePages() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: s3 list %s: %w", prefix, err)
		}
		for _, o := range page.Contents {
			keys = append(keys, aws.ToString(o.Key))
		}
	}
	// S3 lists in UTF-8 byte order already; compatible stores may not.
	slices.Sort(keys)
	return keys, nil
}

// status returns 0 if err is nil or has no HTTP response.
func status(err error) int {
	if re, ok := errors.AsType[*awshttp.ResponseError](err); ok {
		return re.HTTPStatusCode()
	}
	return 0
}
