// Package enginesink has the destinations whose data the engine writes. The
// runner only commits and recovers them, from the staged descriptor the
// engine reports for each epoch.
package enginesink

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"syscall"
	"time"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/runner"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type ParquetConfig struct {
	// file:// and an absolute path.
	URL string `json:"url"`
}

// Parquet writes each stream's rows as Parquet files under a directory, and
// commits an epoch by linking its staged files to names derived from (g, N).
// That makes a repeated commit a no-op, so it needs no fence.
type Parquet struct {
	root string
}

var (
	_ runner.EngineSink   = (*Parquet)(nil)
	_ destination.Checker = (*Parquet)(nil)
)

func NewParquet(config []byte) (*Parquet, error) {
	var cfg ParquetConfig
	dec := json.NewDecoder(bytes.NewReader(config))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parquet: config: %w", err)
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || u.Scheme != "file" || (u.Host != "" && u.Host != "localhost") || !path.IsAbs(u.Path) || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("parquet: url %q must be file:// and an absolute path", cfg.URL)
	}
	return &Parquet{root: filepath.Clean(filepath.FromSlash(u.Path))}, nil
}

// Check confirms the root directory exists, or can be created, and takes
// new files.
func (p *Parquet) Check(context.Context) error {
	err := os.MkdirAll(p.root, 0o750)
	if err == nil {
		var f *os.File
		if f, err = os.CreateTemp(p.root, ".openetl-check-*"); err == nil {
			_ = f.Close()
			err = os.Remove(f.Name())
		}
	}
	if errors.Is(err, fs.ErrPermission) || errors.Is(err, syscall.EROFS) {
		return errs.Wrap(errs.PermissionDenied, err, "parquet")
	}
	return err
}

func (p *Parquet) EngineSink() runner.SinkPlan {
	return runner.SinkPlan{ParquetURL: "file://" + filepath.ToSlash(p.root)}
}

func (p *Parquet) Capabilities() destination.Capabilities {
	return destination.Capabilities{
		Class:                 destination.StageAndApply,
		Scopes:                []destination.Scope{destination.ScopeStream},
		DefaultCommitInterval: time.Minute,
		MinCommitInterval:     time.Second,
	}
}

func (p *Parquet) Open(_ context.Context, cfg destination.OpenConfig) (destination.Writer, error) {
	for _, s := range cfg.Streams {
		if s.Mode != destination.Append {
			return nil, fmt.Errorf("parquet: stream %q: only append writes are supported", s.Name)
		}
	}
	return &parquetWriter{root: p.root}, nil
}

// descriptor is openetl_formats::Descriptor. Paths are relative to the root.
type descriptor struct {
	Generation uint64         `json:"generation"`
	Epoch      uint64         `json:"epoch"`
	Staging    string         `json:"staging"`
	Streams    []stagedStream `json:"streams"`
}

type stagedStream struct {
	Stream string       `json:"stream"`
	Marker string       `json:"marker"`
	Files  []stagedFile `json:"files"`
}

type stagedFile struct {
	Staged string `json:"staged"`
	Target string `json:"target"`
	Rows   uint64 `json:"rows"`
	Bytes  uint64 `json:"bytes"`
}

type parquetWriter struct {
	root string
}

var errEngineWrites = errors.New("parquet: the engine writes this destination's data")

func (w *parquetWriter) Write(context.Context, destination.Epoch, string, arrow.RecordBatch) error {
	return errEngineWrites
}

func (w *parquetWriter) PreCommit(context.Context, destination.Epoch) (destination.Staged, error) {
	return nil, errEngineWrites
}

func (w *parquetWriter) Commit(_ context.Context, e destination.Epoch, s destination.Staged) error {
	d, err := w.descriptor(e, s)
	if err != nil {
		return err
	}
	for _, st := range d.Streams {
		if err := w.apply(st); err != nil {
			return err
		}
	}
	w.dropStaging(d)
	return nil
}

func (w *parquetWriter) Recover(_ context.Context, _ uint64, e destination.Epoch, s destination.Staged) (destination.Recovery, error) {
	if e.IsZero() {
		return destination.Applied, nil
	}
	d, err := w.descriptor(e, s)
	if err != nil {
		return 0, err
	}
	var missing []stagedStream
	lost := 0
	for _, st := range d.Streams {
		done, err := exists(w.path(st.Marker))
		if err != nil {
			return 0, err
		}
		if done {
			continue
		}
		missing = append(missing, st)
		have := 0
		for _, f := range st.Files {
			staged, err := exists(w.path(f.Staged))
			if err != nil {
				return 0, err
			}
			linked, err := exists(w.path(f.Target))
			if err != nil {
				return 0, err
			}
			if staged || linked {
				have++
			}
		}
		switch have {
		case len(st.Files):
		case 0:
			lost++
		default:
			return 0, fmt.Errorf("parquet: epoch %d-g%d: stream %s is partly staged", d.Epoch, d.Generation, st.Stream)
		}
	}
	switch {
	case len(missing) == 0:
		w.dropStaging(d)
		return destination.Applied, nil
	case lost == 0:
		for _, st := range missing {
			if err := w.apply(st); err != nil {
				return 0, err
			}
		}
		w.dropStaging(d)
		return destination.Redone, nil
	case lost == len(d.Streams):
		return destination.Lost, nil
	default:
		return 0, fmt.Errorf("parquet: epoch %d-g%d is partly applied and the staging for the rest is gone", d.Epoch, d.Generation)
	}
}

func (w *parquetWriter) Close(context.Context) error { return nil }

func (w *parquetWriter) descriptor(e destination.Epoch, s destination.Staged) (descriptor, error) {
	var d descriptor
	if err := json.Unmarshal(s, &d); err != nil {
		return d, fmt.Errorf("parquet: staged descriptor: %w", err)
	}
	if d.Generation != e.Generation || d.Epoch != e.N {
		return d, fmt.Errorf("parquet: the staged descriptor is for epoch %d-g%d, not %d-g%d", d.Epoch, d.Generation, e.N, e.Generation)
	}
	paths := []string{d.Staging}
	for _, st := range d.Streams {
		paths = append(paths, st.Marker)
		for _, f := range st.Files {
			paths = append(paths, f.Staged, f.Target)
		}
	}
	for _, p := range paths {
		if !filepath.IsLocal(filepath.FromSlash(p)) {
			return d, fmt.Errorf("parquet: staged descriptor path %q leaves the root", p)
		}
	}
	return d, nil
}

func (w *parquetWriter) path(rel string) string {
	return filepath.Join(w.root, filepath.FromSlash(rel))
}

// apply links each staged file to its target, then writes the stream's
// marker. A target that exists was linked by an earlier apply.
func (w *parquetWriter) apply(st stagedStream) error {
	if done, err := exists(w.path(st.Marker)); err != nil || done {
		return err
	}
	dirs := map[string]bool{}
	for _, f := range st.Files {
		target := w.path(f.Target)
		dir := filepath.Dir(target)
		if !dirs[dir] {
			if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // output other tools read
				return fmt.Errorf("parquet: %w", err)
			}
			dirs[dir] = true
		}
		if err := os.Link(w.path(f.Staged), target); err != nil && !errors.Is(err, fs.ErrExist) {
			return fmt.Errorf("parquet: apply %s: %w", st.Stream, err)
		}
	}
	for dir := range dirs {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return touch(w.path(st.Marker))
}

// dropStaging is best effort: staging left behind is never applied twice,
// because its markers exist.
func (w *parquetWriter) dropStaging(d descriptor) {
	dir := w.path(d.Staging)
	_ = os.RemoveAll(dir)
	_ = os.Remove(filepath.Dir(dir)) // the generation's directory, once empty
}

func exists(p string) (bool, error) {
	_, err := os.Lstat(p)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("parquet: %w", err)
	}
	return true, nil
}

func touch(p string) error {
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil { //nolint:gosec // output other tools read
		return fmt.Errorf("parquet: %w", err)
	}
	f, err := os.OpenFile(p, os.O_CREATE|os.O_WRONLY, 0o644) //nolint:gosec // the path is checked to stay under the root
	if err != nil {
		return fmt.Errorf("parquet: write marker: %w", err)
	}
	if err := errors.Join(f.Sync(), f.Close()); err != nil {
		return fmt.Errorf("parquet: write marker: %w", err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // the path is checked to stay under the root
	if err != nil {
		return fmt.Errorf("parquet: %w", err)
	}
	if err := errors.Join(d.Sync(), d.Close()); err != nil {
		return fmt.Errorf("parquet: sync %s: %w", dir, err)
	}
	return nil
}
