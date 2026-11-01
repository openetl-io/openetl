//go:build integration

package secrets_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/google/uuid"

	"github.com/unsuitable001/openetl/internal/secrets"
	"github.com/unsuitable001/openetl/internal/store"
	"github.com/unsuitable001/openetl/internal/store/storetest"
)

// Servers sealing their org's first secrets at once agree on one data key.
func TestKeyringOnTheStore(t *testing.T) {
	ctx := t.Context()
	st, err := store.Open(ctx, storetest.NewDatabase(t))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	tenant, err := st.Bootstrap(ctx, "default", nil)
	if err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(t.TempDir(), "secrets.key")
	open := func() *secrets.Keyring {
		t.Helper()
		orgs, err := st.ListOrgs(ctx)
		if err != nil {
			t.Fatal(err)
		}
		k, err := secrets.Open(ctx, key, st, orgs)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = k.Close() })
		return k
	}
	servers := []*secrets.Keyring{open(), open(), open(), open()}
	sealed := make([][]byte, len(servers))
	errs := make([]error, len(servers))
	var wg sync.WaitGroup
	for i, k := range servers {
		wg.Go(func() { sealed[i], errs[i] = k.Seal(ctx, tenant.Org, []byte("aad"), []byte("value")) })
	}
	wg.Wait()
	later := open()
	for i, s := range sealed {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if v, err := later.Open(ctx, tenant.Org, []byte("aad"), s); err != nil || string(v) != "value" {
			t.Errorf("server %d's value: %q, %v", i, v, err)
		}
	}
	if _, err := secrets.Open(ctx, filepath.Join(t.TempDir(), "new.key"), st, []uuid.UUID{tenant.Org}); err == nil {
		t.Error("a new key file opened, though the org's data key was wrapped with another")
	}
}
