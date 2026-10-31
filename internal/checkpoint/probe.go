package checkpoint

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
)

// Probe checks that store honours conditional writes; a runner must refuse
// to start if it fails. It writes and deletes one object under prefix.
func Probe(ctx context.Context, store Store, prefix string) (err error) {
	key := prefix + "_probe/" + rand.Text()
	defer func() {
		if derr := store.Delete(ctx, key); err == nil && derr != nil {
			err = fmt.Errorf("checkpoint probe: delete: %w", derr)
		}
	}()

	v1, err := store.Create(ctx, key, []byte("probe-1"))
	if err != nil {
		return fmt.Errorf("checkpoint probe: create: %w", err)
	}
	switch _, err := store.Create(ctx, key, []byte("probe-2")); {
	case err == nil:
		return errors.New("checkpoint probe: the store accepted a second create of the same key; it ignores If-None-Match")
	case !errors.Is(err, ErrPrecondition):
		return fmt.Errorf("checkpoint probe: second create: %w", err)
	}
	if _, err := store.Swap(ctx, key, []byte("probe-3"), v1); err != nil {
		return fmt.Errorf("checkpoint probe: swap with the current version: %w", err)
	}
	switch _, err := store.Swap(ctx, key, []byte("probe-4"), v1); {
	case err == nil:
		return errors.New("checkpoint probe: the store accepted a swap with a stale version; it ignores If-Match")
	case !errors.Is(err, ErrPrecondition):
		return fmt.Errorf("checkpoint probe: stale swap: %w", err)
	}
	return nil
}
