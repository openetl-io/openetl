// Package checkpoint implements the checkpoint log: one immutable record per
// checkpoint plus a HEAD pointer, kept in the data plane's object storage and
// updated only by compare-and-swap. The swap on HEAD is also how a replaced
// attempt is fenced out.
package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

var (
	// ErrNotFound is returned when an object doesn't exist.
	ErrNotFound = errors.New("checkpoint: object not found")
	// ErrPrecondition is returned when a conditional write's condition doesn't
	// hold: the object already exists (Create), or its version changed (Swap).
	ErrPrecondition = errors.New("checkpoint: precondition failed")
)

// Version identifies an object's content, like an S3 ETag. Stores may derive
// it from the content, so two writes of identical bytes can have the same
// version; the log never writes the same HEAD content twice.
type Version string

// Store is the object storage the checkpoint log needs.
//
// Any error other than ErrNotFound and ErrPrecondition from a write means its
// outcome is unknown: the write may or may not have been applied.
type Store interface {
	Get(ctx context.Context, key string) ([]byte, Version, error)
	// Create writes key only if it doesn't exist (If-None-Match: *).
	Create(ctx context.Context, key string, data []byte) (Version, error)
	// Swap replaces key only if its current version is prev (If-Match).
	Swap(ctx context.Context, key string, data []byte, prev Version) (Version, error)
	Delete(ctx context.Context, key string) error
	// List returns the keys that start with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
}

func contentVersion(data []byte) Version {
	sum := sha256.Sum256(data)
	return Version(hex.EncodeToString(sum[:16]))
}
