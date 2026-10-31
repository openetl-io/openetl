//go:build integration

package store_test

import (
	"testing"

	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

func TestMigrateAndBootstrapAreIdempotent(t *testing.T) {
	ctx := t.Context()
	s, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for range 2 {
		if err := s.Migrate(ctx); err != nil {
			t.Fatal(err)
		}
	}
	a, err := s.Bootstrap(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Bootstrap(ctx, "default")
	if err != nil {
		t.Fatal(err)
	}
	if a != b {
		t.Errorf("second bootstrap gave %+v, first %+v", b, a)
	}
}
