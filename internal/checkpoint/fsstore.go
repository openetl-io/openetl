package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
)

// FSStore is a Store on the local filesystem. Create hard-links, which fails
// if the target exists; Swap renames under an flock on a sidecar file, so it
// is safe across processes on one machine.
type FSStore struct {
	root string
}

func NewFSStore(dir string) (*FSStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("checkpoint: create store root: %w", err)
	}
	return &FSStore{root: dir}, nil
}

func (s *FSStore) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", fmt.Errorf("checkpoint: invalid key %q", key)
	}
	return filepath.Join(s.root, filepath.FromSlash(key)), nil
}

func (s *FSStore) Get(_ context.Context, key string) ([]byte, Version, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := os.ReadFile(p) //nolint:gosec // p is built from a validated key under the root
	if errors.Is(err, fs.ErrNotExist) {
		return nil, "", ErrNotFound
	}
	if err != nil {
		return nil, "", err
	}
	return data, contentVersion(data), nil
}

func (s *FSStore) Create(_ context.Context, key string, data []byte) (Version, error) {
	p, err := s.path(key)
	if err != nil {
		return "", err
	}
	tmp, err := s.writeTemp(p, data)
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp) // best effort; the link is what matters
	if err := os.Link(tmp, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", ErrPrecondition
		}
		return "", err
	}
	return contentVersion(data), nil
}

func (s *FSStore) Swap(_ context.Context, key string, data []byte, prev Version) (Version, error) {
	p, err := s.path(key)
	if err != nil {
		return "", err
	}
	unlock, err := lockFile(p + ".lock")
	if err != nil {
		return "", err
	}
	defer unlock()

	cur, err := os.ReadFile(p) //nolint:gosec // p is built from a validated key under the root
	if errors.Is(err, fs.ErrNotExist) {
		return "", ErrPrecondition
	}
	if err != nil {
		return "", err
	}
	if contentVersion(cur) != prev {
		return "", ErrPrecondition
	}
	tmp, err := s.writeTemp(p, data)
	if err != nil {
		return "", err
	}
	if err := os.Rename(tmp, p); err != nil {
		_ = os.Remove(tmp)
		return "", err
	}
	return contentVersion(data), nil
}

func (s *FSStore) Delete(_ context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (s *FSStore) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(s.root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasSuffix(p, ".lock") || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(s.root, p)
		if err != nil {
			return err
		}
		if key := filepath.ToSlash(rel); strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.Sort(keys)
	return keys, nil
}

func (s *FSStore) writeTemp(p string, data []byte) (string, error) {
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return "", err
	}
	name := f.Name()
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = os.Remove(name)
		return "", werr
	}
	return name, nil
}

func lockFile(p string) (unlock func(), err error) {
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_RDWR, 0o600) //nolint:gosec // path is built from a validated key
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		_ = f.Close()
		return nil, err
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}
