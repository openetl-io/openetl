package checkpoint_test

import (
	"testing"

	"github.com/openetl-io/openetl/internal/checkpoint"
	"github.com/openetl-io/openetl/internal/checkpoint/checkpointtest"
)

func TestMemStore(t *testing.T) {
	checkpointtest.TestStore(t, func(*testing.T) checkpoint.Store { return checkpoint.NewMemStore() })
}

func TestFSStore(t *testing.T) {
	checkpointtest.TestStore(t, func(t *testing.T) checkpoint.Store {
		s, err := checkpoint.NewFSStore(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		return s
	})
}

func TestFSStoreRejectsEscapingKeys(t *testing.T) {
	s, err := checkpoint.NewFSStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"", "/abs", "a/../../etc"} {
		if _, err := s.Create(t.Context(), key, []byte("x")); err == nil {
			t.Errorf("Create(%q) succeeded, want an error", key)
		}
	}
}
