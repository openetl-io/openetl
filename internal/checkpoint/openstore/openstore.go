// Package openstore opens the checkpoint store a URL names.
package openstore

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/azstore"
	"github.com/openetl-io/openetl/internal/checkpoint/gcsstore"
	"github.com/openetl-io/openetl/internal/checkpoint/s3store"
	"github.com/openetl-io/openetl/internal/platform/objectstore"
)

// IsLocal is whether location is a directory, which only processes on one
// machine can share.
func IsLocal(location string) bool { return !strings.Contains(location, "://") }

// Open takes a directory, or an s3://, gs:// or azblob:// URL as
// objectstore.S3Client, GCSClient and AzureClient describe. A URL's path puts
// every key under it, as it does the quarantine's.
func Open(ctx context.Context, location string) (checkpoint.Store, error) {
	if IsLocal(location) {
		if location == "" {
			return nil, errors.New("openstore: no location")
		}
		s, err := checkpoint.NewFSStore(location)
		if err != nil {
			return nil, err
		}
		return instrument(s, "file"), nil
	}
	u, err := url.Parse(location)
	if err != nil {
		return nil, fmt.Errorf("openstore: %w", err)
	}
	var s checkpoint.Store
	switch u.Scheme {
	case "s3":
		c, err := objectstore.S3Client(u)
		if err != nil {
			return nil, err
		}
		s = s3store.New(c, u.Host)
	case "gs":
		c, err := objectstore.GCSClient(ctx, u)
		if err != nil {
			return nil, err
		}
		s = gcsstore.New(c, u.Host)
	case "azblob":
		c, err := objectstore.AzureClient(u)
		if err != nil {
			return nil, err
		}
		s = azstore.New(c)
	default:
		return nil, fmt.Errorf("openstore: %q is neither a directory nor an s3://, gs:// or azblob:// URL", u.Redacted())
	}
	if p := objectstore.Prefix(u); p != "" {
		s = prefixed{s: s, prefix: p}
	}
	return instrument(s, u.Scheme), nil
}

type prefixed struct {
	s      checkpoint.Store
	prefix string
}

func (p prefixed) Get(ctx context.Context, key string) ([]byte, checkpoint.Version, error) {
	return p.s.Get(ctx, p.prefix+key)
}

func (p prefixed) Create(ctx context.Context, key string, data []byte) (checkpoint.Version, error) {
	return p.s.Create(ctx, p.prefix+key, data)
}

func (p prefixed) Swap(ctx context.Context, key string, data []byte, prev checkpoint.Version) (checkpoint.Version, error) {
	return p.s.Swap(ctx, p.prefix+key, data, prev)
}

func (p prefixed) Delete(ctx context.Context, key string) error {
	return p.s.Delete(ctx, p.prefix+key)
}

func (p prefixed) List(ctx context.Context, prefix string) ([]string, error) {
	keys, err := p.s.List(ctx, p.prefix+prefix)
	for i, k := range keys {
		keys[i] = strings.TrimPrefix(k, p.prefix)
	}
	return keys, err
}
