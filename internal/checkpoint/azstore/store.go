// Package azstore is a checkpoint.Store on Azure Blob Storage, using
// If-None-Match: * for Create and If-Match on the ETag for Swap.
package azstore

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/streaming"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blockblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"

	"github.com/openetl-io/openetl/internal/checkpoint"
)

type Store struct {
	container *container.Client
}

// New returns a Store on a container, which must exist.
func New(c *container.Client) *Store {
	return &Store{container: c}
}

var _ checkpoint.Store = (*Store)(nil)

func (s *Store) Get(ctx context.Context, key string) ([]byte, checkpoint.Version, error) {
	resp, err := s.container.NewBlobClient(key).DownloadStream(ctx, nil)
	if bloberror.HasCode(err, bloberror.BlobNotFound) {
		return nil, "", checkpoint.ErrNotFound
	}
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: azure get %s: %w", key, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, "", fmt.Errorf("checkpoint: azure get %s: %w", key, err)
	}
	return data, version(resp.ETag), nil
}

func (s *Store) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	star := azcore.ETagAny
	v, err := s.put(ctx, key, data, &blob.ModifiedAccessConditions{IfNoneMatch: &star})
	// An existing blob is a 409 BlobAlreadyExists, not a 412.
	if bloberror.HasCode(err, bloberror.BlobAlreadyExists, bloberror.ConditionNotMet) {
		return "", checkpoint.ErrPrecondition
	}
	return v, err
}

func (s *Store) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	etag := azcore.ETag(prev)
	v, err := s.put(ctx, key, data, &blob.ModifiedAccessConditions{IfMatch: &etag})
	// A missing blob: Azurite answers 412, and Azure may answer 404.
	if bloberror.HasCode(err, bloberror.ConditionNotMet, bloberror.BlobNotFound) {
		return "", checkpoint.ErrPrecondition
	}
	return v, err
}

// put sends a conditional upload exactly once: a retry of an upload that
// landed would fail its own condition. The caller maps the errors that mean
// the condition didn't hold; any other is an unknown outcome.
func (s *Store) put(ctx context.Context, key string, data []byte, cond *blob.ModifiedAccessConditions) (checkpoint.Version, error) {
	ctx = policy.WithRetryOptions(ctx, policy.RetryOptions{MaxRetries: -1})
	resp, err := s.container.NewBlockBlobClient(key).Upload(ctx, streaming.NopCloser(bytes.NewReader(data)), &blockblob.UploadOptions{
		AccessConditions: &blob.AccessConditions{ModifiedAccessConditions: cond},
	})
	if err != nil {
		return "", fmt.Errorf("checkpoint: azure put %s: %w", key, err)
	}
	return version(resp.ETag), nil
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.container.NewBlobClient(key).Delete(ctx, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.BlobNotFound) {
		return fmt.Errorf("checkpoint: azure delete %s: %w", key, err)
	}
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]string, error) {
	var keys []string
	pages := s.container.NewListBlobsFlatPager(&container.ListBlobsFlatOptions{Prefix: &prefix})
	for pages.More() {
		page, err := pages.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("checkpoint: azure list %s: %w", prefix, err)
		}
		for _, b := range page.Segment.BlobItems {
			keys = append(keys, *b.Name)
		}
	}
	slices.Sort(keys)
	return keys, nil
}

func version(etag *azcore.ETag) checkpoint.Version {
	if etag == nil {
		return ""
	}
	return checkpoint.Version(*etag)
}
