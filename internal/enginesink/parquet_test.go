package enginesink

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/openetl-io/openetl/sdk/go/destination"
)

type fixture struct {
	t    *testing.T
	root string
	w    destination.Writer
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	root := t.TempDir()
	cfg, _ := json.Marshal(ParquetConfig{URL: "file://" + filepath.ToSlash(root)})
	p, err := NewParquet(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if got := p.EngineSink().ParquetURL; got != "file://"+filepath.ToSlash(root) {
		t.Errorf("sink URL %q", got)
	}
	w, err := p.Open(t.Context(), destination.OpenConfig{Streams: []destination.Stream{{Name: "a", Mode: destination.Append}}})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{t: t, root: root, w: w}
}

// stage writes the files the engine would stage for (g, n), parts files per
// stream, and returns their descriptor.
func (f *fixture) stage(g, n uint64, parts map[string]int) destination.Staged {
	f.t.Helper()
	d := descriptor{Generation: g, Epoch: n, Staging: fmt.Sprintf("_openetl/staging/%d/%d", g, n)}
	for _, stream := range []string{"a", "b"} {
		k := parts[stream]
		if k == 0 {
			continue
		}
		st := stagedStream{Stream: stream, Marker: fmt.Sprintf("_openetl/loads/%s/%020d-g%020d", stream, n, g)}
		for i := range k {
			file := stagedFile{
				Staged: fmt.Sprintf("%s/%s/part-%05d.parquet", d.Staging, stream, i),
				Target: fmt.Sprintf("%s/%020d-g%020d-%05d.parquet", stream, n, g, i),
				Rows:   1,
			}
			f.write(file.Staged, file.Target)
			st.Files = append(st.Files, file)
		}
		d.Streams = append(d.Streams, st)
	}
	b, err := json.Marshal(d)
	if err != nil {
		f.t.Fatal(err)
	}
	return b
}

func (f *fixture) write(rel, content string) {
	f.t.Helper()
	p := filepath.Join(f.root, rel)
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		f.t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) exists(rel string) bool {
	_, err := os.Stat(filepath.Join(f.root, rel))
	return err == nil
}

func (f *fixture) read(rel string) string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.root, rel))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
}

func (f *fixture) recover(g, n uint64, s destination.Staged) (destination.Recovery, error) {
	return f.w.Recover(f.t.Context(), g+1, destination.Epoch{Generation: g, N: n}, []destination.Staged{s})
}

func (f *fixture) remove(rel string) {
	f.t.Helper()
	if err := os.RemoveAll(filepath.Join(f.root, rel)); err != nil {
		f.t.Fatal(err)
	}
}

func target(stream string, g, n uint64, k int) string {
	return fmt.Sprintf("%s/%020d-g%020d-%05d.parquet", stream, n, g, k)
}

func marker(stream string, g, n uint64) string {
	return fmt.Sprintf("_openetl/loads/%s/%020d-g%020d", stream, n, g)
}

func TestCommitLinksStagedFilesAndMarksEachStream(t *testing.T) {
	f := newFixture(t)
	s := f.stage(2, 7, map[string]int{"a": 2, "b": 1})
	e := destination.Epoch{Generation: 2, N: 7}
	if _, err := f.w.Commit(t.Context(), e, []destination.Staged{s}); err != nil {
		t.Fatal(err)
	}
	for _, x := range []struct {
		stream string
		k      int
	}{{"a", 0}, {"a", 1}, {"b", 0}} {
		if got := f.read(target(x.stream, 2, 7, x.k)); got != target(x.stream, 2, 7, x.k) {
			t.Errorf("%s part %d holds %q", x.stream, x.k, got)
		}
	}
	for _, m := range []string{marker("a", 2, 7), marker("b", 2, 7)} {
		if !f.exists(m) {
			t.Errorf("no marker %s", m)
		}
	}
	if f.exists("_openetl/staging/2") {
		t.Error("staging is still there")
	}
	if _, err := f.w.Commit(t.Context(), e, []destination.Staged{s}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	if r, err := f.recover(2, 7, s); err != nil || r != destination.Applied {
		t.Fatalf("recover: %v, %v", r, err)
	}
}

func TestRecoverRedoesUnmarkedStreams(t *testing.T) {
	f := newFixture(t)
	s := f.stage(3, 1, map[string]int{"a": 2, "b": 1})
	// An earlier commit linked a's first file, then died.
	if err := os.MkdirAll(filepath.Join(f.root, "a"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(filepath.Join(f.root, "_openetl/staging/3/1/a/part-00000.parquet"), filepath.Join(f.root, target("a", 3, 1, 0))); err != nil {
		t.Fatal(err)
	}
	r, err := f.recover(3, 1, s)
	if err != nil || r != destination.Redone {
		t.Fatalf("recover: %v, %v", r, err)
	}
	for _, rel := range []string{target("a", 3, 1, 0), target("a", 3, 1, 1), target("b", 3, 1, 0), marker("a", 3, 1), marker("b", 3, 1)} {
		if !f.exists(rel) {
			t.Errorf("%s is missing", rel)
		}
	}
}

func TestRecoverReportsLostWhenNothingIsLeft(t *testing.T) {
	f := newFixture(t)
	s := f.stage(1, 4, map[string]int{"a": 1, "b": 1})
	f.remove("_openetl/staging")
	if r, err := f.recover(1, 4, s); err != nil || r != destination.Lost {
		t.Fatalf("recover: %v, %v", r, err)
	}
}

func TestRecoverRefusesAPartlyAppliedEpochWithoutStaging(t *testing.T) {
	f := newFixture(t)
	s := f.stage(1, 4, map[string]int{"a": 1, "b": 1})
	f.write(marker("a", 1, 4), "")
	f.remove("_openetl/staging/1/4/b")
	if _, err := f.recover(1, 4, s); err == nil {
		t.Fatal("recover succeeded")
	}
}

func TestRecoverWithoutACheckpoint(t *testing.T) {
	f := newFixture(t)
	if r, err := f.w.Recover(t.Context(), 1, destination.Epoch{}, nil); err != nil || r != destination.Applied {
		t.Fatalf("recover: %v, %v", r, err)
	}
}

func TestAnEmptyEpochCommits(t *testing.T) {
	f := newFixture(t)
	s := f.stage(1, 2, nil)
	if _, err := f.w.Commit(t.Context(), destination.Epoch{Generation: 1, N: 2}, []destination.Staged{s}); err != nil {
		t.Fatal(err)
	}
	if r, err := f.recover(1, 2, s); err != nil || r != destination.Applied {
		t.Fatalf("recover: %v, %v", r, err)
	}
}

func TestRejectsBadDescriptors(t *testing.T) {
	f := newFixture(t)
	e := destination.Epoch{Generation: 1, N: 1}
	cases := map[string]descriptor{
		"other epoch":   {Generation: 1, Epoch: 2, Staging: "s"},
		"escapes":       {Generation: 1, Epoch: 1, Staging: "../s"},
		"absolute":      {Generation: 1, Epoch: 1, Staging: "s", Streams: []stagedStream{{Stream: "a", Marker: "/tmp/m"}}},
		"escape target": {Generation: 1, Epoch: 1, Staging: "s", Streams: []stagedStream{{Stream: "a", Marker: "m", Files: []stagedFile{{Staged: "x", Target: "a/../../y"}}}}},
	}
	for name, d := range cases {
		b, _ := json.Marshal(d)
		if _, err := f.w.Commit(t.Context(), e, []destination.Staged{b}); err == nil {
			t.Errorf("%s: commit succeeded", name)
		}
	}
}

func TestConfig(t *testing.T) {
	for _, u := range []string{"s3://bucket/x", "file://relative/x", "file:///x?y=1", "/x", "file://host/x"} {
		cfg, _ := json.Marshal(ParquetConfig{URL: u})
		if _, err := NewParquet(cfg); err == nil {
			t.Errorf("%s: accepted", u)
		}
	}
	p, err := NewParquet([]byte(`{"url": "file://localhost/tmp/a/../b/"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := p.EngineSink().ParquetURL; got != "file:///tmp/b" {
		t.Errorf("sink URL %q", got)
	}
	if _, err := p.Open(t.Context(), destination.OpenConfig{Streams: []destination.Stream{{Name: "a", Mode: destination.Merge}}}); err == nil {
		t.Error("opened a merge stream")
	}
}
