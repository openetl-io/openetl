// Package secrets seals secrets' values for the metadata store. Each org has
// a data key, which seals its values with AES-256-GCM. The install's
// key-encryption key, a local key or a KMS key, wraps the data keys.
package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	gsecrets "gocloud.dev/secrets"

	// The KMS drivers for --secrets-key's URLs.
	_ "gocloud.dev/secrets/awskms"
	_ "gocloud.dev/secrets/azurekeyvault"
	_ "gocloud.dev/secrets/gcpkms"
	"gocloud.dev/secrets/localsecrets"

	"github.com/unsuitable001/openetl/internal/store/db"
)

// Keys stores each org's wrapped data key; store.Store implements it.
type Keys interface {
	GetOrgKey(ctx context.Context, orgID uuid.UUID) ([]byte, error)
	InsertOrgKey(ctx context.Context, arg db.InsertOrgKeyParams) error
}

type Keyring struct {
	kek  *gsecrets.Keeper
	keys Keys

	mu   sync.Mutex
	deks map[uuid.UUID]cipher.AEAD
}

// Open opens the key-encryption key ref names, and checks that it unwraps
// each org's data key. ref is a file holding a base64-encoded 32-byte key, or
// a base64key://, awskms://, gcpkms:// or azurekeyvault:// URL. A key file
// that doesn't exist is created, unless an org has a data key already.
func Open(ctx context.Context, ref string, keys Keys, orgs []uuid.UUID) (*Keyring, error) {
	wrapped := map[uuid.UUID][]byte{}
	for _, org := range orgs {
		w, err := keys.GetOrgKey(ctx, org)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		wrapped[org] = w
	}
	kek, err := openKEK(ctx, ref, len(wrapped) == 0)
	if err != nil {
		return nil, err
	}
	k := &Keyring{kek: kek, keys: keys, deks: map[uuid.UUID]cipher.AEAD{}}
	for org, w := range wrapped {
		if _, err := k.unwrap(ctx, org, w); err != nil {
			_ = kek.Close()
			return nil, err
		}
	}
	return k, nil
}

// New is a Keyring with the key-encryption key kek.
func New(kek *gsecrets.Keeper, keys Keys) *Keyring {
	return &Keyring{kek: kek, keys: keys, deks: map[uuid.UUID]cipher.AEAD{}}
}

func (k *Keyring) Close() error { return k.kek.Close() }

const sealVersion = 1

// AAD binds a secret's sealed value to the secret, so a value copied to
// another row doesn't open.
func AAD(org, workspace, id uuid.UUID) []byte {
	return []byte("openetl.secret.v1/" + org.String() + "/" + workspace.String() + "/" + id.String())
}

// Seal encrypts value with org's data key, creating the key if org has
// none. aad binds the result to its place: Open needs the same aad.
func (k *Keyring) Seal(ctx context.Context, org uuid.UUID, aad, value []byte) ([]byte, error) {
	a, err := k.dataKey(ctx, org, true)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, a.NonceSize())
	_, _ = rand.Read(nonce)
	out := make([]byte, 0, 1+len(nonce)+len(value)+a.Overhead())
	out = append(append(out, sealVersion), nonce...)
	return a.Seal(out, nonce, value, aad), nil
}

func (k *Keyring) Open(ctx context.Context, org uuid.UUID, aad, sealed []byte) ([]byte, error) {
	a, err := k.dataKey(ctx, org, false)
	if err != nil {
		return nil, err
	}
	n := 1 + a.NonceSize()
	if len(sealed) < n || sealed[0] != sealVersion {
		return nil, errors.New("secrets: not a sealed value")
	}
	v, err := a.Open(nil, sealed[1:n], sealed[n:], aad)
	if err != nil {
		return nil, fmt.Errorf("secrets: can't open a value of org %s: %w", org, err)
	}
	return v, nil
}

func (k *Keyring) dataKey(ctx context.Context, org uuid.UUID, create bool) (cipher.AEAD, error) {
	k.mu.Lock()
	a, ok := k.deks[org]
	k.mu.Unlock()
	if ok {
		return a, nil
	}
	w, err := k.keys.GetOrgKey(ctx, org)
	if errors.Is(err, pgx.ErrNoRows) && create {
		dek := make([]byte, 32)
		_, _ = rand.Read(dek)
		if w, err = k.kek.Encrypt(ctx, dek); err != nil {
			return nil, fmt.Errorf("secrets: wrap a data key: %w", err)
		}
		if err := k.keys.InsertOrgKey(ctx, db.InsertOrgKeyParams{OrgID: org, WrappedKey: w}); err != nil {
			return nil, err
		}
		// Another server may have stored its key first.
		w, err = k.keys.GetOrgKey(ctx, org)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("secrets: org %s has no data key", org)
	}
	if err != nil {
		return nil, err
	}
	return k.unwrap(ctx, org, w)
}

func (k *Keyring) unwrap(ctx context.Context, org uuid.UUID, wrapped []byte) (cipher.AEAD, error) {
	dek, err := k.kek.Decrypt(ctx, wrapped)
	if err != nil {
		return nil, fmt.Errorf("secrets: the key-encryption key can't unwrap org %s's data key; is --secrets-key the key its secrets were sealed with? %w", org, err)
	}
	b, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("secrets: org %s's data key: %w", org, err)
	}
	a, err := cipher.NewGCM(b)
	if err != nil {
		return nil, err
	}
	k.mu.Lock()
	k.deks[org] = a
	k.mu.Unlock()
	return a, nil
}

func openKEK(ctx context.Context, ref string, create bool) (*gsecrets.Keeper, error) {
	if scheme, rest, ok := strings.Cut(ref, "://"); ok {
		// Don't quote ref in errors: a base64key:// URL is the key.
		if scheme == localsecrets.Scheme && rest == "" {
			return nil, errors.New("secrets: base64key:// needs a key; without one it makes a new key each start")
		}
		k, err := gsecrets.OpenKeeper(ctx, ref)
		if err != nil {
			return nil, fmt.Errorf("secrets: open the %s:// key: %w", scheme, err)
		}
		return k, nil
	}
	b, err := os.ReadFile(ref) //nolint:gosec // the operator names the file
	if errors.Is(err, fs.ErrNotExist) {
		if !create {
			return nil, fmt.Errorf("secrets: the key file %s doesn't exist, but secrets were sealed with a key; restore it, or point --secrets-key at that key", ref)
		}
		b, err = createKeyFile(ref)
	}
	if err != nil {
		return nil, fmt.Errorf("secrets: %w", err)
	}
	key, err := localsecrets.Base64KeyStd(strings.TrimSpace(string(b)))
	if err != nil {
		return nil, fmt.Errorf("secrets: the key file %s doesn't hold a base64-encoded 32-byte key", ref)
	}
	return localsecrets.NewKeeper(key), nil
}

func createKeyFile(path string) ([]byte, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil { //nolint:gosec // the operator names the file
		return nil, err
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	b := []byte(base64.StdEncoding.EncodeToString(key) + "\n")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // the operator names the file
	if errors.Is(err, fs.ErrExist) {
		return os.ReadFile(path) //nolint:gosec // as above
	}
	if err != nil {
		return nil, err
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return nil, err
	}
	return b, f.Close()
}
