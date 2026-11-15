// Package checkpoint is the checkpoint log: immutable records plus a HEAD
// pointer in object storage, updated only by compare-and-swap, which also
// fences out replaced attempts.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"time"
)

var (
	ErrNotFound = errors.New("checkpoint: object not found")
	// ErrPrecondition: the object exists (Create), or its version changed
	// (Swap).
	ErrPrecondition = errors.New("checkpoint: precondition failed")
)

// Version is like an S3 ETag: it may be derived from the content.
type Version string

// Store is the object storage the log needs. A write that fails with any
// error other than ErrNotFound or ErrPrecondition may or may not have been
// applied.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, Version, error)
	// Create writes key only if it doesn't exist (If-None-Match: *).
	Create(ctx context.Context, key string, data []byte) (Version, error)
	// Swap replaces key only if its current version is prev (If-Match).
	Swap(ctx context.Context, key string, data []byte, prev Version) (Version, error)
	Delete(ctx context.Context, key string) error
	// List returns the keys that start with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// ListObjects is List, with when each key was last written.
	ListObjects(ctx context.Context, prefix string) ([]Object, error)
}

// Object is a key, and when it was last written.
type Object struct {
	Key      string
	Modified time.Time
}

// SortObjects sorts objects by key.
func SortObjects(objects []Object) {
	slices.SortFunc(objects, func(a, b Object) int { return strings.Compare(a.Key, b.Key) })
}

// Keys are objects' keys, in their order.
func Keys(objects []Object) []string {
	out := make([]string, len(objects))
	for i, o := range objects {
		out[i] = o.Key
	}
	return out
}

func contentVersion(data []byte) Version {
	sum := sha256.Sum256(data)
	return Version(hex.EncodeToString(sum[:16]))
}
