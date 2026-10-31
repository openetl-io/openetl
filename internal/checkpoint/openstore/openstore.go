// Package openstore opens the checkpoint store a URL names.
package openstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/unsuitable001/openetl/internal/checkpoint"
	"github.com/unsuitable001/openetl/internal/checkpoint/s3store"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// Open takes a directory, or an s3:// URL as objectstore.S3Client describes.
func Open(_ context.Context, location string) (checkpoint.Store, error) {
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
	if u.Scheme != "s3" || u.Host == "" {
		return nil, fmt.Errorf("openstore: %q is neither a directory nor s3://bucket", location)
	}
	c, err := objectstore.S3Client(u)
	if err != nil {
		return nil, err
	}
	return s3store.New(c, u.Host), nil
}
