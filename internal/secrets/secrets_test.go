package secrets

import (
	"bytes"
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/openetl-io/openetl/internal/store/db"
)

// memKeys is Keys in memory, with the store's ON CONFLICT DO NOTHING.
type memKeys struct {
	mu   sync.Mutex
	keys map[uuid.UUID][]byte
}

func (m *memKeys) GetOrgKey(_ context.Context, org uuid.UUID) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k, ok := m.keys[org]
	if !ok {
		return nil, pgx.ErrNoRows
	}
	return k, nil
}

func (m *memKeys) InsertOrgKey(_ context.Context, arg db.InsertOrgKeyParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.keys == nil {
		m.keys = map[uuid.UUID][]byte{}
	}
	if _, ok := m.keys[arg.OrgID]; !ok {
		m.keys[arg.OrgID] = arg.WrappedKey
	}
	return nil
}

func TestSealAndOpen(t *testing.T) {
	ctx := t.Context()
	keys := &memKeys{}
	k, err := Open(ctx, filepath.Join(t.TempDir(), "key"), keys, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = k.Close() }()
	org, other := uuid.New(), uuid.New()
	sealed, err := k.Seal(ctx, org, []byte("a"), []byte("hunter2"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(sealed, []byte("hunter2")) {
		t.Fatal("the sealed value holds the plaintext")
	}
	v, err := k.Open(ctx, org, []byte("a"), sealed)
	if err != nil || string(v) != "hunter2" {
		t.Fatalf("Open = %q, %v", v, err)
	}
	again, err := k.Seal(ctx, org, []byte("a"), []byte("hunter2"))
	if err != nil || bytes.Equal(again, sealed) {
		t.Errorf("sealing twice gave the same bytes (%v)", err)
	}
	if _, err := k.Open(ctx, org, []byte("b"), sealed); err == nil {
		t.Error("opened with another aad")
	}
	if _, err := k.Seal(ctx, other, []byte("a"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, err := k.Open(ctx, other, []byte("a"), sealed); err == nil {
		t.Error("opened with another org's key")
	}
	tampered := bytes.Clone(sealed)
	tampered[len(tampered)-1] ^= 1
	if _, err := k.Open(ctx, org, []byte("a"), tampered); err == nil {
		t.Error("opened a tampered value")
	}
	if _, err := k.Open(ctx, uuid.New(), []byte("a"), sealed); err == nil || !strings.Contains(err.Error(), "no data key") {
		t.Errorf("Open for an org without a key: %v", err)
	}
	if len(keys.keys) != 2 {
		t.Errorf("%d data keys, want one per org", len(keys.keys))
	}

	// A second server, with the same key file, opens what the first sealed.
	k2 := New(k.kek, keys)
	if v, err := k2.Open(ctx, org, []byte("a"), sealed); err != nil || string(v) != "hunter2" {
		t.Errorf("another keyring: %q, %v", v, err)
	}
}

func TestConcurrentFirstSealsShareOneKey(t *testing.T) {
	ctx := t.Context()
	keys := &memKeys{}
	kek, err := openKEK(ctx, filepath.Join(t.TempDir(), "key"), true)
	if err != nil {
		t.Fatal(err)
	}
	org := uuid.New()
	sealed := make([][]byte, 8)
	errs := make([]error, len(sealed))
	var wg sync.WaitGroup
	for i := range sealed {
		wg.Go(func() {
			// A keyring each, as servers have.
			sealed[i], errs[i] = New(kek, keys).Seal(ctx, org, nil, []byte("v"))
		})
	}
	wg.Wait()
	k := New(kek, keys)
	for i, s := range sealed {
		if errs[i] != nil {
			t.Fatal(errs[i])
		}
		if v, err := k.Open(ctx, org, nil, s); err != nil || string(v) != "v" {
			t.Errorf("value %d: %q, %v", i, v, err)
		}
	}
}

func TestKeyFiles(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "secrets.key")
	keys := &memKeys{}
	org := uuid.New()

	k, err := Open(ctx, path, keys, []uuid.UUID{org})
	if err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil || fi.Mode().Perm() != 0o600 {
		t.Fatalf("key file: %v, %v", fi, err)
	}
	sealed, err := k.Seal(ctx, org, nil, []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, path, keys, []uuid.UUID{org}); err != nil {
		t.Errorf("reopen: %v", err)
	}

	if _, err := Open(ctx, filepath.Join(dir, "lost.key"), keys, []uuid.UUID{org}); err == nil || !strings.Contains(err.Error(), "doesn't exist, but secrets were sealed") {
		t.Errorf("a missing key file with sealed secrets: %v", err)
	}
	other := filepath.Join(dir, "other.key")
	if err := os.WriteFile(other, []byte(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, 32))), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, other, keys, []uuid.UUID{org}); err == nil || !strings.Contains(err.Error(), "can't unwrap") {
		t.Errorf("another key: %v", err)
	}
	bad := filepath.Join(dir, "bad.key")
	if err := os.WriteFile(bad, []byte("short"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, bad, &memKeys{}, nil); err == nil || !strings.Contains(err.Error(), "32-byte") {
		t.Errorf("a malformed key file: %v", err)
	}

	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	url := "base64key://" + base64.URLEncoding.EncodeToString(raw)
	k3, err := Open(ctx, url, keys, []uuid.UUID{org})
	if err != nil {
		t.Fatalf("the same key as a URL: %v", err)
	}
	if v, err := k3.Open(ctx, org, nil, sealed); err != nil || string(v) != "v" {
		t.Errorf("open with the URL's key: %q, %v", v, err)
	}
}

func TestKEKURLs(t *testing.T) {
	ctx := t.Context()
	if _, err := openKEK(ctx, "base64key://", true); err == nil {
		t.Error("base64key:// without a key opened")
	}
	if _, err := openKEK(ctx, "base64key://bm90IDMyIGJ5dGVz", true); err == nil || strings.Contains(err.Error(), "bm90IDMyIGJ5dGVz") {
		t.Errorf("a short base64key://: %v", err)
	}
	if _, err := openKEK(ctx, "nosuch://key", true); err == nil || !strings.Contains(err.Error(), "no driver registered") {
		t.Errorf("an unknown scheme: %v", err)
	}
	// The KMS drivers are registered; opening needs no network, but some
	// need credentials.
	for _, u := range []string{"awskms://alias/openetl?region=us-east-1", "gcpkms://projects/p/locations/l/keyRings/r/cryptoKeys/k", "azurekeyvault://v.vault.azure.net/keys/k"} {
		k, err := openKEK(ctx, u, true)
		if err != nil && strings.Contains(err.Error(), "no driver registered") {
			t.Errorf("%s: %v", u, err)
		}
		if k != nil {
			_ = k.Close()
		}
	}
}
