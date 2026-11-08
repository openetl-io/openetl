package checkpoint

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

// A directory's sync is what makes the entries in it survive a power loss,
// so it must come after the write that changes them. Each sync records the
// directory, and what the key read then.
func TestFSStoreSyncsDirectoriesAfterWriting(t *testing.T) {
	temp := t.TempDir()
	dir := filepath.Join(temp, "store", "root")
	key := filepath.Join(dir, "a", "b", "k")
	var synced []string
	var read string
	var fail error
	s, err := newFSStore(dir, func(f *os.File) error {
		synced = append(synced, filepath.Clean(f.Name()))
		b, _ := os.ReadFile(key)
		read = string(b)
		return fail
	})
	if err != nil {
		t.Fatal(err)
	}
	check := func(what, content string, want ...string) {
		t.Helper()
		if !slices.Equal(synced, want) {
			t.Errorf("%s synced %q, want %q", what, synced, want)
		}
		if read != content {
			t.Errorf("%s: the key read %q at the last sync, want %q", what, read, content)
		}
		synced = nil
	}
	leaf := filepath.Dir(key)
	check("a new store", "", filepath.Join(temp, "store"), temp)

	ctx := t.Context()
	v, err := s.Create(ctx, "a/b/k", []byte("one"))
	if err != nil {
		t.Fatal(err)
	}
	check("the first create in new directories", "one", filepath.Join(dir, "a"), dir, leaf)

	if _, err := s.Swap(ctx, "a/b/k", []byte("two"), v); err != nil {
		t.Fatal(err)
	}
	check("a swap", "two", leaf)

	if _, err := s.Create(ctx, "a/b/other", []byte("x")); err != nil {
		t.Fatal(err)
	}
	check("a create in known directories", "two", leaf)

	fail = errors.New("the disk went away")
	if _, err := s.Create(ctx, "a/b/third", []byte("x")); !errors.Is(err, fail) {
		t.Errorf("a create whose directory didn't sync returned %v", err)
	}
}
