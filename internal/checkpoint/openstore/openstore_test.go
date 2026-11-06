package openstore

import (
	"slices"
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/checkpointtest"
)

func TestPrefixed(t *testing.T) {
	checkpointtest.TestStore(t, func(*testing.T) checkpoint.Store {
		return prefixed{s: checkpoint.NewMemStore(), prefix: "base/"}
	})
}

func TestPrefixedKeys(t *testing.T) {
	ctx := t.Context()
	mem := checkpoint.NewMemStore()
	s := prefixed{s: mem, prefix: "base/"}
	for _, k := range []string{"base-other/o/p/x", "o/p/x"} {
		if _, err := mem.Create(ctx, k, nil); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Create(ctx, "o/p/y", nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := mem.Get(ctx, "base/o/p/y"); err != nil {
		t.Errorf("the key isn't under the prefix: %v", err)
	}
	for _, prefix := range []string{"", "o/", "o/p/y"} {
		if keys, err := s.List(ctx, prefix); err != nil || !slices.Equal(keys, []string{"o/p/y"}) {
			t.Errorf("List(%q) = %q, %v; want only o/p/y", prefix, keys, err)
		}
	}
}

func TestOpenRejects(t *testing.T) {
	for _, loc := range []string{"", "ftp://bucket", "s3://", "gs://", "azblob://"} {
		if _, err := Open(t.Context(), loc); err == nil {
			t.Errorf("opened %q", loc)
		}
	}
}
