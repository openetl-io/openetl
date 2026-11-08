package checkpoint

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
)

// FSStore is a Store on the local filesystem. Create hard-links, which fails
// if the target exists; Swap renames under an flock on a sidecar file, so it
// is safe across processes on one machine. A write that returns is on disk,
// its directory entry included. It holds the directory open, and no key
// resolves outside it, through symbolic links either.
type FSStore struct {
	dir  string
	root *os.Root
	// Flushes a directory to disk.
	fsync func(*os.File) error
	// The directories below the root whose entries, up to the root, this
	// process has flushed.
	durable sync.Map
}

func NewFSStore(dir string) (*FSStore, error) { return newFSStore(dir, (*os.File).Sync) }

func newFSStore(dir string, fsync func(*os.File) error) (*FSStore, error) {
	made := missingDirs(dir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("checkpoint: create store root: %w", err)
	}
	for _, d := range made {
		parent, err := os.Open(filepath.Dir(d))
		if err == nil {
			err = errors.Join(fsync(parent), parent.Close())
		}
		if err != nil {
			return nil, fmt.Errorf("checkpoint: sync the store root's parent: %w", err)
		}
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: open store root: %w", err)
	}
	return &FSStore{dir: dir, root: root, fsync: fsync}, nil
}

// missingDirs are dir and those of its parents that don't exist, deepest
// first.
func missingDirs(dir string) []string {
	var out []string
	for d := filepath.Clean(dir); ; d = filepath.Dir(d) {
		if _, err := os.Stat(d); !errors.Is(err, fs.ErrNotExist) || d == filepath.Dir(d) {
			return out
		}
		out = append(out, d)
	}
}

// path is key's file, relative to the root.
func (s *FSStore) path(key string) (string, error) {
	if key == "" || strings.HasPrefix(key, "/") || strings.Contains(key, "..") {
		return "", fmt.Errorf("checkpoint: invalid key %q", key)
	}
	return filepath.FromSlash(key), nil
}

func (s *FSStore) Get(_ context.Context, key string) ([]byte, Version, error) {
	p, err := s.path(key)
	if err != nil {
		return nil, "", err
	}
	data, err := s.root.ReadFile(p)
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
	defer func() { _ = s.root.Remove(tmp) }() // best effort; the link is what matters
	if err := s.root.Link(tmp, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", ErrPrecondition
		}
		return "", err
	}
	if err := s.syncDir(filepath.Dir(p)); err != nil {
		return "", err
	}
	return contentVersion(data), nil
}

func (s *FSStore) Swap(_ context.Context, key string, data []byte, prev Version) (Version, error) {
	p, err := s.path(key)
	if err != nil {
		return "", err
	}
	unlock, err := s.lockFile(p + ".lock")
	if err != nil {
		return "", err
	}
	defer unlock()

	cur, err := s.root.ReadFile(p)
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
	if err := s.root.Rename(tmp, p); err != nil {
		_ = s.root.Remove(tmp)
		return "", err
	}
	if err := s.syncDir(filepath.Dir(p)); err != nil {
		return "", err
	}
	return contentVersion(data), nil
}

func (s *FSStore) Delete(_ context.Context, key string) error {
	p, err := s.path(key)
	if err != nil {
		return err
	}
	if err := s.root.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return nil
}

func (s *FSStore) List(_ context.Context, prefix string) ([]string, error) {
	start := s.dir
	if i := strings.LastIndex(prefix, "/"); i > 0 {
		p, err := s.path(prefix[:i])
		if err != nil {
			return nil, err
		}
		start = filepath.Join(s.dir, p)
	}
	var keys []string
	err := filepath.WalkDir(start, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			if p == start && errors.Is(err, fs.ErrNotExist) {
				return fs.SkipAll
			}
			return err
		}
		if d.IsDir() || strings.HasSuffix(p, ".lock") || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(s.dir, p)
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

// writeTemp writes data to a new .tmp- file beside p, and returns its path.
func (s *FSStore) writeTemp(p string, data []byte) (string, error) {
	dir := filepath.Dir(p)
	if err := s.makeDir(dir); err != nil {
		return "", err
	}
	name := filepath.Join(dir, ".tmp-"+rand.Text())
	f, err := s.root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		_ = s.root.Remove(name)
		return "", werr
	}
	return name, nil
}

// makeDir makes dir and its parents. A directory's entry is in its parent, so
// the first time this process uses dir it flushes each parent up to the root,
// whether the directory is new or an earlier process didn't get that far.
func (s *FSStore) makeDir(dir string) error {
	if err := s.root.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	if _, ok := s.durable.Load(dir); ok {
		return nil
	}
	for d := dir; d != "."; d = filepath.Dir(d) {
		if err := s.syncDir(filepath.Dir(d)); err != nil {
			return err
		}
	}
	s.durable.Store(dir, true)
	return nil
}

func (s *FSStore) syncDir(dir string) error {
	d, err := s.root.Open(dir)
	if err != nil {
		return err
	}
	if err := errors.Join(s.fsync(d), d.Close()); err != nil {
		return fmt.Errorf("checkpoint: sync %s: %w", dir, err)
	}
	return nil
}

func (s *FSStore) lockFile(p string) (unlock func(), err error) {
	if err := s.root.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		return nil, err
	}
	// Not plain O_CREATE: on macOS, openat with O_CREATE and without O_EXCL can
	// fail with ENOENT while another caller creates the same file. Lock files
	// are never removed, so one that exists can be opened.
	f, err := s.root.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	if errors.Is(err, fs.ErrExist) {
		f, err = s.root.OpenFile(p, os.O_RDWR, 0)
	}
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
