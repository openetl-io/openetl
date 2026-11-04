// Package staging reads the files of a staged epoch, for destinations with
// destination.DeliveryStaged.
package staging

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	pqfile "github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	sdk "github.com/unsuitable001/openetl/sdk/go"
	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	protocolv1 "github.com/unsuitable001/openetl/sdk/go/protocol/v1"
)

// Position is a row's place in a staged file: its row group, and its offset
// in that row group.
type Position struct {
	RowGroup int
	Row      int64
}

const (
	batchRows = 64 << 10
	readTries = 4
	// A URL this close to its expiry is replaced before use.
	expirySlack = 30 * time.Second
)

// Read calls fn with the file's rows from from on, in order, as record
// batches, each with the position of its first row. A batch is released
// after fn returns.
//
// It fetches the file itself, with range requests, through URLs from
// h.StagedFileURL, and asks for a fresh one when a URL expires or is
// refused.
func Read(ctx context.Context, h host.Host, file destination.StagedFile, from Position, fn func(at Position, batch arrow.RecordBatch) error) error {
	r, err := open(ctx, h, file)
	if err != nil {
		return err
	}
	pf, err := pqfile.NewParquetReader(r)
	if err != nil {
		_ = r.Close()
		return fmt.Errorf("staging: %s: %w", file.Path, err)
	}
	defer func() { _ = pf.Close() }()
	fr, err := pqarrow.NewFileReader(pf, pqarrow.ArrowReadProperties{BatchSize: batchRows}, memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("staging: %s: %w", file.Path, err)
	}
	for rg := from.RowGroup; rg < pf.NumRowGroups(); rg++ {
		var skip int64
		if rg == from.RowGroup {
			skip = from.Row
		}
		if err := readRowGroup(ctx, fr, rg, skip, fn); err != nil {
			return fmt.Errorf("staging: %s: %w", file.Path, err)
		}
	}
	return nil
}

func readRowGroup(ctx context.Context, fr *pqarrow.FileReader, rg int, skip int64, fn func(Position, arrow.RecordBatch) error) error {
	rr, err := fr.GetRecordReader(ctx, nil, []int{rg})
	if err != nil {
		return err
	}
	defer rr.Release()
	var at int64
	for rr.Next() {
		b := rr.RecordBatch()
		n := b.NumRows()
		switch {
		case at+n <= skip:
		case at < skip:
			s := b.NewSlice(skip-at, n)
			err = fn(Position{RowGroup: rg, Row: skip}, s)
			s.Release()
		default:
			err = fn(Position{RowGroup: rg, Row: at}, b)
		}
		if err != nil {
			return err
		}
		at += n
	}
	return rr.Err()
}

type readerAtSeeker interface {
	io.ReaderAt
	io.Seeker
	io.Closer
}

// open returns a local file for a file:// URL, which a store that is a
// directory hands out, and a remote file otherwise.
func open(ctx context.Context, h host.Host, file destination.StagedFile) (readerAtSeeker, error) {
	r := &remote{ctx: ctx, h: h, path: file.Path, size: int64(file.Bytes)} //nolint:gosec // file sizes fit
	u, err := r.signed(false)
	if err != nil {
		return nil, err
	}
	if pu, err := url.Parse(u); err == nil && pu.Scheme == "file" {
		f, err := os.Open(pu.Path)
		if err != nil {
			return nil, fmt.Errorf("staging: %w", err)
		}
		return f, nil
	}
	return r, nil
}

// remote reads a staged file with range requests.
type remote struct {
	ctx     context.Context //nolint:containedctx // io.ReaderAt takes none
	h       host.Host
	path    string
	size    int64
	off     int64
	url     string
	expires time.Time
}

func (r *remote) signed(fresh bool) (string, error) {
	if !fresh && r.url != "" && (r.expires.IsZero() || time.Until(r.expires) > expirySlack) {
		return r.url, nil
	}
	u, exp, err := r.h.StagedFileURL(r.ctx, r.path)
	if err != nil {
		return "", fmt.Errorf("staging: a URL for %s: %w", r.path, err)
	}
	r.url, r.expires = u, exp
	return u, nil
}

func (r *remote) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, errors.New("staging: negative offset")
	}
	if off >= r.size {
		return 0, io.EOF
	}
	want := min(int64(len(p)), r.size-off)
	backoff := 200 * time.Millisecond
	var err error
	refused := false
	for try := range readTries {
		if try > 0 && !refused {
			t := time.NewTimer(backoff)
			select {
			case <-t.C:
			case <-r.ctx.Done():
				t.Stop()
				return 0, r.ctx.Err()
			}
			backoff *= 2
		}
		var u string
		if u, err = r.signed(refused); err != nil {
			return 0, err
		}
		refused, err = r.get(u, p[:want], off)
		if err == nil {
			if want < int64(len(p)) {
				return int(want), io.EOF
			}
			return int(want), nil
		}
		if r.ctx.Err() != nil {
			return 0, r.ctx.Err()
		}
	}
	return 0, sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_TRANSIENT, err)
}

// get fills p from off. refused says the URL was refused, as an expired one
// is, so a fresh one may work.
func (r *remote) get(u string, p []byte, off int64) (refused bool, err error) {
	req, err := http.NewRequestWithContext(r.ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, fmt.Errorf("staging: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", off, off+int64(len(p))-1))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("staging: %s: %w", r.path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusPartialContent:
	case http.StatusOK:
		// The store ignored the range.
		if _, err := io.CopyN(io.Discard, resp.Body, off); err != nil {
			return false, fmt.Errorf("staging: %s: %w", r.path, err)
		}
	case http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden:
		return true, fmt.Errorf("staging: %s: HTTP %d", r.path, resp.StatusCode)
	default:
		return false, fmt.Errorf("staging: %s: HTTP %d", r.path, resp.StatusCode)
	}
	if _, err := io.ReadFull(resp.Body, p); err != nil {
		return false, fmt.Errorf("staging: %s: %w", r.path, err)
	}
	return false, nil
}

func (r *remote) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		offset += r.off
	case io.SeekEnd:
		offset += r.size
	default:
		return 0, errors.New("staging: bad whence")
	}
	if offset < 0 {
		return 0, errors.New("staging: negative position")
	}
	r.off = offset
	return offset, nil
}

func (r *remote) Close() error { return nil }
