// Package spark runs a pipeline's Spark applications on the user's clusters
// (ADR-0022). Everything a cluster reads or writes of the data plane's store
// is below the pipeline's {org}/{pipeline}/_spark/: the checked copies of its
// code, under code/{sha256}/, its syncs' contexts, under context/{sync}/,
// and each generation's transform, under {g}/.
package spark

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"gocloud.dev/blob"
	"gocloud.dev/gcerrors"

	"github.com/unsuitable001/openetl/internal/platform/errs"
	"github.com/unsuitable001/openetl/internal/platform/objectstore"
)

// Dir holds what a pipeline's Spark clusters read, next to its checkpoints.
const Dir = "_spark"

// Area is a pipeline's _spark/ prefix.
type Area struct {
	pipeline string
	bucket   *blob.Bucket
	// How clusters name the area's keys: an object store's URL, or a
	// directory's file:// URL, below which each key is.
	base *url.URL
}

// Open opens pipeline's area below location, the data plane's store as
// objectstore.Open takes it. Only S3 and directories are supported: a
// directory works where the cluster mounts it at the same path.
func Open(ctx context.Context, location, org, pipeline string) (*Area, error) {
	rel := path.Join(org, pipeline, Dir) + "/"
	var base *url.URL
	switch u, err := url.Parse(location); {
	case !strings.Contains(location, "://"):
		abs, err := filepath.Abs(location)
		if err != nil {
			return nil, err
		}
		base = &url.URL{Scheme: "file", Path: filepath.ToSlash(abs) + "/" + rel}
	case err != nil:
		return nil, err
	case u.Scheme == "file":
		base = &url.URL{Scheme: "file", Path: path.Clean(u.Path) + "/" + rel}
	case u.Scheme == "s3":
		base = &url.URL{Scheme: "s3", Host: u.Host, Path: "/" + objectstore.Prefix(u) + rel}
	default:
		return nil, errs.New(errs.ConfigInvalid, "Spark needs the data plane's store on S3 or in a directory, not %s://", u.Scheme)
	}
	root, err := objectstore.Open(ctx, location)
	if err != nil {
		return nil, err
	}
	return &Area{pipeline: pipeline, bucket: blob.PrefixedBucket(root, rel), base: base}, nil
}

func (a *Area) Close() error { return a.bucket.Close() }

func (a *Area) Pipeline() string { return a.pipeline }

// local is a directory store's directory for key; empty for an object
// store.
func (a *Area) local(key string) string {
	if a.base.Scheme != "file" {
		return ""
	}
	return filepath.Join(filepath.FromSlash(a.base.Path), filepath.FromSlash(key))
}

// children are the names of the directories below prefix.
func (a *Area) children(ctx context.Context, prefix string) ([]string, error) {
	if dir := a.local(prefix); dir != "" {
		entries, err := os.ReadDir(dir)
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		var names []string
		for _, e := range entries {
			if e.IsDir() {
				names = append(names, e.Name())
			}
		}
		return names, err
	}
	var names []string
	it := a.bucket.List(&blob.ListOptions{Prefix: prefix, Delimiter: "/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return names, nil
		}
		if err != nil {
			return names, err
		}
		if obj.IsDir {
			names = append(names, strings.TrimSuffix(strings.TrimPrefix(obj.Key, prefix), "/"))
		}
	}
}

// drop deletes the keys below prefix, and a directory store's directory.
func (a *Area) drop(ctx context.Context, prefix string) error {
	if dir := a.local(prefix); dir != "" {
		return os.RemoveAll(dir)
	}
	it := a.bucket.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := a.bucket.Delete(ctx, obj.Key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
			return err
		}
	}
}

// URI is how a cluster reads key: s3:// on EMR, whose EMRFS takes it, and
// s3a:// elsewhere, for Hadoop's S3A.
func (a *Area) URI(key string, emr bool) string {
	u := *a.base
	u.Path += key
	if u.Scheme == "s3" && !emr {
		u.Scheme = "s3a"
	}
	return u.String()
}

// CodeKey is where the checked copy of a file with digest sum is kept.
func CodeKey(sum, uri string) string {
	name := path.Base(strings.SplitN(uri, "?", 2)[0])
	return "code/" + sum + "/" + name
}

// StageCode copies the file at uri into the area, if it isn't there, and
// returns its key. The copy is written only once its SHA-256 matches sum, so
// what's in the area is what the pipeline version pins. client fetches
// https:// URIs; s3:// URIs take the query the data plane's store does, such
// as ?region=.
func (a *Area) StageCode(ctx context.Context, uri, sum string, client *http.Client) (string, error) {
	key := CodeKey(sum, uri)
	switch ok, err := a.bucket.Exists(ctx, key); {
	case err != nil:
		return "", errs.Wrap(errs.Transient, err, "spark: check the code's copy")
	case ok:
		return key, nil
	}
	src, err := fetch(ctx, uri, client)
	if err != nil {
		return "", err
	}
	defer func() { _ = src.Close() }()
	// Cancelling the writer's context discards what it wrote.
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	w, err := a.bucket.NewWriter(wctx, key, nil)
	if err != nil {
		return "", errs.Wrap(errs.Transient, err, "spark: copy the code")
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(w, h), src); err != nil {
		cancel()
		_ = w.Close()
		return "", errs.Wrap(errs.Transient, err, "spark: copy the code from "+redact(uri))
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sum {
		cancel()
		_ = w.Close()
		return "", errs.New(errs.ConfigInvalid, "spark: %s has SHA-256 %s, not the %s the pipeline pins", redact(uri), got, sum)
	}
	if err := w.Close(); err != nil {
		return "", errs.Wrap(errs.Transient, err, "spark: copy the code")
	}
	return key, nil
}

// DropCode deletes the code copies whose keys keep doesn't hold.
func (a *Area) DropCode(ctx context.Context, keep map[string]bool) error {
	it := a.bucket.List(&blob.ListOptions{Prefix: "code/"})
	for {
		obj, err := it.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if !keep[obj.Key] {
			if err := a.bucket.Delete(ctx, obj.Key); err != nil && gcerrors.Code(err) != gcerrors.NotFound {
				return err
			}
		}
	}
}

// WriteContext writes a sync's context for a step, and returns its key.
func (a *Area) WriteContext(ctx context.Context, sync string, step int, doc []byte) (string, error) {
	key := fmt.Sprintf("context/%s/%d.json", sync, step)
	if err := a.bucket.WriteAll(ctx, key, doc, &blob.WriterOptions{ContentType: "application/json"}); err != nil {
		return "", errs.Wrap(errs.Transient, err, "spark: write the sync's context")
	}
	return key, nil
}

func fetch(ctx context.Context, uri string, client *http.Client) (io.ReadCloser, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: code URI")
	}
	switch u.Scheme {
	case "s3", "s3a":
		key := strings.TrimPrefix(u.Path, "/")
		u.Scheme, u.Path = "s3", ""
		b, err := objectstore.Open(ctx, u.String())
		if err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: code bucket")
		}
		r, err := b.NewReader(ctx, key, nil)
		if err != nil {
			_ = b.Close()
			code := errs.Transient
			if gcerrors.Code(err) == gcerrors.NotFound {
				code = errs.ConfigInvalid
			}
			return nil, errs.Wrap(code, err, "spark: read "+redact(uri))
		}
		return closeBoth{r, b}, nil
	case "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, uri, nil)
		if err != nil {
			return nil, errs.Wrap(errs.ConfigInvalid, err, "spark: code URI")
		}
		res, err := client.Do(req)
		if err != nil {
			return nil, errs.Wrap(errs.Transient, err, "spark: fetch "+redact(uri))
		}
		if res.StatusCode != http.StatusOK {
			_ = res.Body.Close()
			code := errs.Transient
			if res.StatusCode == http.StatusNotFound || res.StatusCode == http.StatusForbidden || res.StatusCode == http.StatusUnauthorized {
				code = errs.ConfigInvalid
			}
			return nil, errs.New(code, "spark: fetch %s: %s", redact(uri), res.Status)
		}
		return res.Body, nil
	}
	return nil, errs.New(errs.ConfigInvalid, "spark: %s isn't an s3:// or https:// URI", redact(uri))
}

type closeBoth struct {
	*blob.Reader
	b *blob.Bucket
}

func (c closeBoth) Close() error { return errors.Join(c.Reader.Close(), c.b.Close()) }

// redact drops a URI's query, which can hold a presigned URL's signature.
func redact(uri string) string {
	s, _, _ := strings.Cut(uri, "?")
	return s
}
