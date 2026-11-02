package logging

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestOutputRotatesFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "openetld.log")
	w, err := Output{To: path, MaxSize: 1, MaxFiles: 2}.Open()
	if err != nil {
		t.Fatal(err)
	}
	line := append(bytes.Repeat([]byte("x"), 1023), '\n')
	for range 2500 {
		if _, err := w.Write(line); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 3 {
		t.Fatalf("files %v, want the log and two rotated ones", entries)
	}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > 1<<20 {
			t.Errorf("%s is %d bytes, over 1 MB", e.Name(), info.Size())
		}
	}
}

func TestOutputRejects(t *testing.T) {
	if _, err := (Output{To: filepath.Join(t.TempDir(), "x.log")}).Open(); err == nil {
		t.Error("a file without a size opened")
	}
	if _, err := (Output{To: filepath.Join(t.TempDir(), "missing", "x.log"), MaxSize: 1}).Open(); err == nil {
		t.Error("a file in a missing directory opened")
	}
	for _, to := range []string{"", "stderr", "stdout"} {
		w, err := Output{To: to}.Open()
		if err != nil || w.Close() != nil {
			t.Errorf("%q: %v", to, err)
		}
	}
}
