// Package openstore opens the checkpoint store a URL names.
package openstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/azstore"
	"github.com/unsuitable001/openetl/internal/checkpoint/gcsstore"
	"github.com/unsuitable001/openetl/internal/checkpoint/s3store"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// Open takes a directory, or an s3://, gs:// or azblob:// URL as
// objectstore.S3Client, GCSClient and AzureClient describe.
func Open(ctx context.Context, location string) (checkpoint.Store, error) {
	if !strings.Contains(location, "://") {
		if location == "" {
			return nil, errors.New("openstore: no location")
		}
		return checkpoint.NewFSStore(location)
	}
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("openstore: %w", err)
	}
	switch u.Scheme {
	case "s3":
		c, err := objectstore.S3Client(u)
		if err != nil {
			return nil, err
		}
		return s3store.New(c, u.Host), nil
	case "gs":
		c, err := objectstore.GCSClient(ctx, u)
		if err != nil {
			return nil, err
		}
		return gcsstore.New(c, u.Host), nil
	case "azblob":
		c, err := objectstore.AzureClient(u)
		if err != nil {
			return nil, err
		}
		return azstore.New(c), nil
	}
	return nil, fmt.Errorf("openstore: %q is neither a directory nor an s3://, gs:// or azblob:// URL", u.Redacted())
}
