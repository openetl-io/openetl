// Package staging keeps a pipeline's staged epochs, for a destination with
// destination.DeliveryStaged, in the data plane's store under
// {org}/{pipeline}/_staging/: each stream's Parquet files, which the
// dataflow writes at {g}/{N}/{stream}/part-{k}.parquet, and the
// destination's delivery cursor at {g}/{N}/cursor.
package staging

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
	"github.com/unsuitable001/openetl/sdk/go/destination"
)

// Dir holds a pipeline's staged epochs, next to its checkpoints.
const Dir = "_staging"

const (
	defaultURLTTL  = 15 * time.Minute
	cursorInterval = time.Second
	maxCursor      = 16 << 10
)

// Area is one attempt's view of a pipeline's staging. It's safe for
// concurrent use.
type Area struct {
	bucket *blob.Bucket
	url    string
	// The directory, for a store that is one; its files are handed out as
	// file:// URLs.
	dir string
	// For a store without credentials, which can't presign: its files' plain
	// URLs, which work where it allows unsigned reads.
	unsigned func(p string) string
	urlTTL time.Duration
	log    *slog.Logger
	now    func() time.Time

	mu      sync.Mutex
	files   map[string]bool // paths handed to the destination
	epochs  map[destination.Epoch]bool
	cursors map[destination.Epoch]*cursor
	// Held while a cursor is written or an epoch dropped, so a write never
	// lands after a newer one or after its epoch is gone.
	writeMu sync.Mutex
	wake    chan struct{}
	done    chan struct{}
	stopped chan struct{}
	// For the background writes, which outlast a cancelled attempt so its
	// last cursors are written.
	ctx context.Context //nolint:containedctx // the writer's, not a request's
}

type cursor struct {
	value            []byte
	version, written uint64
}

type Options struct {
	// How long a signed URL is valid. Zero: 15 minutes.
	URLTTL time.Duration
	// Nil: slog's default.
	Logger *slog.Logger
}

// Open opens the staging of pipeline below location, the data plane's
// store as objectstore.Open takes it. Close it when the attempt ends.
func Open(ctx context.Context, location, org, pipeline string, o Options) (*Area, error) {
	rel := path.Join(org, pipeline, Dir)
	root, err := objectstore.Open(ctx, location)
	if err != nil {
		return nil, err
	}
	a := &Area{
		bucket:  blob.PrefixedBucket(root, rel+"/"),
		urlTTL:  o.URLTTL,
		log:     o.Logger,
		now:     time.Now,
		files:   map[string]bool{},
		epochs:  map[destination.Epoch]bool{},
		cursors: map[destination.Epoch]*cursor{},
		wake:    make(chan struct{}, 1),
		done:    make(chan struct{}),
		stopped: make(chan struct{}),
	}
	if a.urlTTL == 0 {
		a.urlTTL = defaultURLTTL
	}
	if a.log == nil {
		a.log = slog.Default()
	}
	if a.url, a.dir, err = locate(location, rel); err != nil {
		_ = a.bucket.Close()
		return nil, err
	}
	if u, err := url.Parse(location); err == nil && u.Scheme == "s3" && objectstore.S3Anonymous() {
		prefix := objectstore.Prefix(u) + rel + "/"
		a.unsigned = func(p string) string { return objectstore.S3ObjectURL(u, prefix+p) }
	}
	a.ctx = context.WithoutCancel(ctx)
	go a.writeCursors()
	return a, nil
}

// locate returns the URL the dataflow stages below, and the directory for a
// store that is one.
func locate(location, rel string) (u, dir string, err error) {
	if !strings.Contains(location, "://") {
		abs, err := filepath.Abs(location)
		if err != nil {
			return "", "", fmt.Errorf("staging: %w", err)
		}
		dir = filepath.Join(abs, filepath.FromSlash(rel))
		return (&url.URL{Scheme: "file", Path: filepath.ToSlash(dir)}).String(), dir, nil
	}
	pu, err := url.Parse(location)
	if err != nil {
		return "", "", fmt.Errorf("staging: %w", err)
	}
	pu.Path = path.Join("/", pu.Path, rel)
	if pu.Scheme == "file" {
		dir = filepath.FromSlash(pu.Path)
	}
	return pu.String(), dir, nil
}

// Close writes the cursors not yet written, and closes the bucket.
func (a *Area) Close() error {
	close(a.done)
	<-a.stopped
	a.writeDirty(a.ctx)
	return a.bucket.Close()
}

func (a *Area) URL() string { return a.url }

func epochDir(e destination.Epoch) string {
	return strconv.FormatUint(e.Generation, 10) + "/" + strconv.FormatUint(e.N, 10) + "/"
}

func (a *Area) Hand(e destination.Epoch, stream string, files []destination.StagedFile) error {
	dir := epochDir(e)
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, f := range files {
		if !strings.HasPrefix(f.Path, dir) || path.Clean(f.Path) != f.Path {
			return errs.New(errs.Internal, "staging: %s's file %q isn't in epoch %d-g%d", stream, f.Path, e.N, e.Generation)
		}
		a.files[f.Path] = true
	}
	a.epochs[e] = true
	return nil
}

// SignedURL returns a URL to GET a file handed to the destination: a
// presigned one, or one that doesn't expire for a store that can't presign.
func (a *Area) SignedURL(ctx context.Context, p string) (string, time.Time, error) {
	a.mu.Lock()
	handed := a.files[p]
	a.mu.Unlock()
	if !handed {
		return "", time.Time{}, errs.New(errs.Internal, "StagedFileURL: %q isn't a file of an epoch the destination was handed", p)
	}
	switch {
	case a.dir != "":
		u := &url.URL{Scheme: "file", Path: filepath.ToSlash(filepath.Join(a.dir, filepath.FromSlash(p)))}
		return u.String(), time.Time{}, nil
	case a.unsigned != nil:
		return a.unsigned(p), time.Time{}, nil
	}
	expires := a.now().Add(a.urlTTL)
	u, err := a.bucket.SignedURL(ctx, p, &blob.SignedURLOptions{Expiry: a.urlTTL, Method: http.MethodGet})
	if err != nil {
		return "", time.Time{}, errs.Wrap(errs.Internal, err, "StagedFileURL: sign "+p)
	}
	return u, expires, nil
}

// PutCursor keeps e's latest cursor, which a background writer writes at
// most once a cursorInterval, and Flush at once. A cursor for an epoch
// whose files weren't handed over is ignored.
func (a *Area) PutCursor(_ context.Context, e destination.Epoch, c []byte) error {
	if len(c) > maxCursor {
		return errs.New(errs.Internal, "PutDeliveryCursor: a cursor is at most %d bytes", maxCursor)
	}
	a.mu.Lock()
	if !a.epochs[e] {
		// No files, so nothing to resume.
		a.mu.Unlock()
		return nil
	}
	cur := a.cursors[e]
	if cur == nil {
		cur = &cursor{}
		a.cursors[e] = cur
	}
	cur.value = bytes.Clone(c)
	cur.version++
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
	return nil
}

// Cursor returns e's latest cursor: the one this attempt kept, or else the
// one written.
func (a *Area) Cursor(ctx context.Context, e destination.Epoch) ([]byte, bool, error) {
	a.mu.Lock()
	handed := a.epochs[e]
	cur := a.cursors[e]
	var v []byte
	if cur != nil {
		v = bytes.Clone(cur.value)
	}
	a.mu.Unlock()
	if !handed {
		return nil, false, nil
	}
	if cur != nil {
		return v, true, nil
	}
	v, err := a.bucket.ReadAll(ctx, epochDir(e)+"cursor")
	if gcerrors.Code(err) == gcerrors.NotFound {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errs.Wrap(errs.Transient, err, "GetDeliveryCursor")
	}
	return v, true, nil
}

func (a *Area) Flush(ctx context.Context, e destination.Epoch) error {
	return a.writeCursor(ctx, e)
}

func (a *Area) writeCursors() {
	defer close(a.stopped)
	for {
		select {
		case <-a.wake:
		case <-a.done:
			return
		}
		a.writeDirty(a.ctx)
		t := time.NewTimer(cursorInterval)
		select {
		case <-t.C:
		case <-a.done:
			t.Stop()
			return
		}
	}
}

func (a *Area) writeDirty(ctx context.Context) {
	a.mu.Lock()
	var dirty []destination.Epoch
	for e, cur := range a.cursors {
		if cur.written != cur.version {
			dirty = append(dirty, e)
		}
	}
	a.mu.Unlock()
	for _, e := range dirty {
		if err := a.writeCursor(ctx, e); err != nil {
			a.log.Warn("writing a delivery cursor failed; a later write retries it", "epoch", e.N, "generation", e.Generation, "err", err)
		}
	}
}

func (a *Area) writeCursor(ctx context.Context, e destination.Epoch) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	a.mu.Lock()
	cur := a.cursors[e]
	if cur == nil || cur.written == cur.version {
		a.mu.Unlock()
		return nil
	}
	v, version := cur.value, cur.version
	a.mu.Unlock()
	if err := a.bucket.WriteAll(ctx, epochDir(e)+"cursor", v, nil); err != nil {
		return errs.Wrap(errs.Transient, err, "staging: write the delivery cursor")
	}
	a.mu.Lock()
	cur.written = max(cur.written, version)
	a.mu.Unlock()
	return nil
}

func (a *Area) Drop(ctx context.Context, e destination.Epoch) error {
	a.writeMu.Lock()
	defer a.writeMu.Unlock()
	dir := epochDir(e)
	a.mu.Lock()
	delete(a.epochs, e)
	delete(a.cursors, e)
	for p := range a.files {
		if strings.HasPrefix(p, dir) {
			delete(a.files, p)
		}
	}
	a.mu.Unlock()
	_, err := a.deleteAll(ctx, dir)
	return err
}

func (a *Area) Sweep(ctx context.Context, keep destination.Epoch, gen uint64) (int, error) {
	gens, err := a.children(ctx, "")
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, g := range gens {
		var drop func(n uint64) bool
		switch {
		case g == keep.Generation && g == gen:
			drop = func(n uint64) bool { return n < keep.N }
		case g == keep.Generation:
			drop = func(n uint64) bool { return n != keep.N }
		case g < gen:
			drop = func(uint64) bool { return true }
		default:
			continue
		}
		dir := strconv.FormatUint(g, 10) + "/"
		epochs, err := a.children(ctx, dir)
		if err != nil {
			return deleted, err
		}
		for _, n := range epochs {
			if !drop(n) {
				continue
			}
			k, err := a.deleteAll(ctx, dir+strconv.FormatUint(n, 10)+"/")
			deleted += k
			if err != nil {
				return deleted, err
			}
		}
	}
	return deleted, nil
}

// children lists the numbered directories below dir, in order. Other names
// aren't the platform's, and are left alone.
func (a *Area) children(ctx context.Context, dir string) ([]uint64, error) {
	var out []uint64
	it := a.bucket.List(&blob.ListOptions{Prefix: dir, Delimiter: "/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return out, errs.Wrap(errs.Transient, err, "staging: list")
		}
		if !obj.IsDir {
			continue
		}
		if n, err := strconv.ParseUint(strings.TrimSuffix(strings.TrimPrefix(obj.Key, dir), "/"), 10, 64); err == nil {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out, nil
}

// deleteAll deletes everything below dir, and for a store that is a
// directory, the directories left empty.
func (a *Area) deleteAll(ctx context.Context, dir string) (int, error) {
	deleted := 0
	it := a.bucket.List(&blob.ListOptions{Prefix: dir})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			if a.dir != "" {
				local := filepath.Join(a.dir, filepath.FromSlash(dir))
				_ = os.RemoveAll(local)
				_ = os.Remove(filepath.Dir(local)) // the generation's, once empty
			}
			return deleted, nil
		}
		if err != nil {
			return deleted, errs.Wrap(errs.Transient, err, "staging: list")
		}
		if err := a.bucket.Delete(ctx, obj.Key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return deleted, errs.Wrap(errs.Transient, err, "staging: delete "+obj.Key)
		}
		deleted++
	}
}
