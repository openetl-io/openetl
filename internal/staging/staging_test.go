package staging_test

import (
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/unsuitable001/openetl/internal/staging"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

func open(t *testing.T, dir string) *staging.Area {
	t.Helper()
	a, err := staging.Open(t.Context(), dir, "org", "p", staging.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

// put writes a staged file the way the dataflow does.
func put(t *testing.T, dir, rel string) destination.StagedFile {
	t.Helper()
	p := filepath.Join(dir, "org", "p", staging.Dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte("parquet"), 0o600); err != nil {
		t.Fatal(err)
	}
	return destination.StagedFile{Path: rel, Rows: 1, Bytes: 7}
}

func TestSignsOnlyHandedFiles(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir)
	if want := "file://" + filepath.ToSlash(filepath.Join(dir, "org", "p", staging.Dir)); a.URL() != want {
		t.Fatalf("URL %s, want %s", a.URL(), want)
	}
	e := destination.Epoch{Generation: 2, N: 5}
	f := put(t, dir, "2/5/s%2Ft/part-00000.parquet")
	if _, _, err := a.SignedURL(t.Context(), f.Path); err == nil {
		t.Fatal("signed a file that wasn't handed over")
	}
	if err := a.Hand(e, "s/t", []destination.StagedFile{{Path: "2/6/x/part-00000.parquet"}}); err == nil {
		t.Fatal("handed another epoch's file")
	}
	if err := a.Hand(e, "s/t", []destination.StagedFile{{Path: "2/5/../6/x"}}); err == nil {
		t.Fatal("handed a path that leaves the epoch")
	}
	if err := a.Hand(e, "s/t", []destination.StagedFile{f}); err != nil {
		t.Fatal(err)
	}
	u, _, err := a.SignedURL(t.Context(), f.Path)
	if err != nil {
		t.Fatal(err)
	}
	pu, err := url.Parse(u)
	if err != nil || pu.Scheme != "file" {
		t.Fatalf("URL %q: %v", u, err)
	}
	if b, err := os.ReadFile(pu.Path); err != nil || string(b) != "parquet" {
		t.Fatalf("read %s: %q, %v", pu.Path, b, err)
	}
}

func TestCursorsOutliveTheAttemptOnceFlushed(t *testing.T) {
	dir := t.TempDir()
	a := open(t, dir)
	e := destination.Epoch{Generation: 1, N: 3}
	if err := a.PutCursor(t.Context(), e, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := a.Cursor(t.Context(), e); err != nil || ok {
		t.Fatalf("kept a cursor for an epoch that wasn't handed over: %v, %v", ok, err)
	}
	if err := a.Hand(e, "s", []destination.StagedFile{put(t, dir, "1/3/s/part-00000.parquet")}); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := a.Cursor(t.Context(), e); err != nil || ok {
		t.Fatalf("a cursor before any was saved: %v, %v", ok, err)
	}
	for _, c := range []string{"a", "b", "c"} {
		if err := a.PutCursor(t.Context(), e, []byte(c)); err != nil {
			t.Fatal(err)
		}
	}
	if c, ok, err := a.Cursor(t.Context(), e); err != nil || !ok || string(c) != "c" {
		t.Fatalf("cursor %q, %v, %v; want c", c, ok, err)
	}
	if err := a.Flush(t.Context(), e); err != nil {
		t.Fatal(err)
	}

	// The next attempt reads what was written.
	b := open(t, dir)
	if err := b.Hand(e, "s", nil); err != nil {
		t.Fatal(err)
	}
	if c, ok, err := b.Cursor(t.Context(), e); err != nil || !ok || string(c) != "c" {
		t.Fatalf("the next attempt's cursor %q, %v, %v; want c", c, ok, err)
	}

	if err := b.Drop(t.Context(), e); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "org", "p", staging.Dir, "1")); !os.IsNotExist(err) {
		t.Fatalf("the dropped epoch's directory is still there: %v", err)
	}
	if err := b.PutCursor(t.Context(), e, []byte("d")); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := b.Cursor(t.Context(), e); err != nil || ok {
		t.Fatalf("kept a cursor for a dropped epoch: %v, %v", ok, err)
	}
}

func TestSweepKeepsTheRestoredEpochAndWhatComesAfter(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{
		"1/4/s/part-00000.parquet", // an older generation's
		"2/6/s/part-00000.parquet", // HEAD's generation, before HEAD
		"2/7/s/part-00000.parquet", // HEAD
		"2/8/s/part-00000.parquet", // never recorded
		"3/8/s/part-00000.parquet", // this attempt's, before HEAD
		"3/9/s/part-00000.parquet", // this attempt's
		"4/9/s/part-00000.parquet", // a newer attempt's
	} {
		put(t, dir, rel)
	}
	a := open(t, dir)
	n, err := a.Sweep(t.Context(), destination.Epoch{Generation: 2, N: 7}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Fatalf("deleted %d, want 3", n)
	}
	if got, want := left(t, dir), []string{"2/7", "3/8", "3/9", "4/9"}; !slices.Equal(got, want) {
		t.Fatalf("left %v, want %v", got, want)
	}

	// Once this attempt records epoch 9, its own earlier epochs go too.
	if _, err := a.Sweep(t.Context(), destination.Epoch{Generation: 3, N: 9}, 3); err != nil {
		t.Fatal(err)
	}
	if got, want := left(t, dir), []string{"3/9", "4/9"}; !slices.Equal(got, want) {
		t.Fatalf("left %v, want %v", got, want)
	}
}

func left(t *testing.T, dir string) []string {
	t.Helper()
	root := filepath.Join(dir, "org", "p", staging.Dir)
	var out []string
	gens, _ := os.ReadDir(root)
	for _, g := range gens {
		epochs, _ := os.ReadDir(filepath.Join(root, g.Name()))
		for _, e := range epochs {
			out = append(out, g.Name()+"/"+e.Name())
		}
	}
	slices.Sort(out)
	return out
}
