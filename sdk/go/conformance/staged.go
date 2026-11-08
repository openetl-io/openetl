package conformance

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"google.golang.org/protobuf/types/known/timestamppb"

	sdk "github.com/openetl-io/openetl/sdk/go"
	"github.com/openetl-io/openetl/sdk/go/internal/ints"
	"github.com/openetl-io/openetl/sdk/go/internal/rpcerr"
	protocolv1 "github.com/openetl-io/openetl/sdk/go/protocol/v1"
)

// A staged epoch's files hold this many records each, so a delivery has
// parts to resume between.
const recordsPerFile = 2

const urlTTL = time.Minute

// stagingArea is the kit's stand-in for the runner's staging: Parquet files
// in a directory, served over HTTP through URLs that expire, and the
// delivery cursors the destination saves, which outlive a crashed writer.
type stagingArea struct {
	dir  string
	root *os.Root
	ln   net.Listener
	srv  *http.Server
	key  []byte

	mu      sync.Mutex
	handed  map[string]bool
	cursors map[string][]byte
	// Called once a cursor is kept, before the destination hears back.
	onCursor func(e *protocolv1.Epoch)
}

func newStagingArea() (*stagingArea, error) {
	dir, err := os.MkdirTemp("", "conformance-staging-")
	if err != nil {
		return nil, err
	}
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		_ = ln.Close()
		_ = os.RemoveAll(dir)
		return nil, err
	}
	a := &stagingArea{dir: dir, root: root, ln: ln, key: make([]byte, 32), handed: map[string]bool{}, cursors: map[string][]byte{}}
	_, _ = rand.Read(a.key)
	a.srv = &http.Server{Handler: http.HandlerFunc(a.serve), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = a.srv.Serve(ln) }()
	return a, nil
}

// attachStaging serves a new staging area to p's host services, in place of
// any it had.
func attachStaging(p Process) (*stagingArea, error) {
	kp, ok := p.(*proc)
	if !ok {
		return nil, errors.New("a staged destination needs the kit's host services, so a process from Exec or InProcess")
	}
	a, err := newStagingArea()
	if err != nil {
		return nil, err
	}
	kp.hs.mu.Lock()
	kp.hs.staging = a
	kp.hs.mu.Unlock()
	return a, nil
}

func (a *stagingArea) close() {
	_ = a.srv.Close()
	_ = a.root.Close()
	_ = os.RemoveAll(a.dir)
}

func (a *stagingArea) sign(p string, expires int64) string {
	m := hmac.New(sha256.New, a.key)
	fmt.Fprintf(m, "%s\x00%d", p, expires)
	return hex.EncodeToString(m.Sum(nil))
}

// serve answers GETs, ranges included, for URLs it signed that haven't
// expired, as a presigned URL's store does.
func (a *stagingArea) serve(w http.ResponseWriter, r *http.Request) {
	p := strings.TrimPrefix(r.URL.Path, "/")
	expires, err := strconv.ParseInt(r.URL.Query().Get("expires"), 10, 64)
	if r.Method != http.MethodGet || err != nil || time.Now().Unix() >= expires ||
		!hmac.Equal([]byte(r.URL.Query().Get("sig")), []byte(a.sign(p, expires))) {
		http.Error(w, "the URL is expired or not signed", http.StatusForbidden)
		return
	}
	f, err := a.root.Open(filepath.FromSlash(p))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	http.ServeContent(w, r, "", st.ModTime(), f)
}

// stage writes a stream's records on a lane of lanes in epoch e as Parquet
// files, named as the platform's are, and lets the destination read them.
func (a *stagingArea) stage(e *protocolv1.Epoch, stream string, lane, lanes int, batches []arrow.RecordBatch) ([]*protocolv1.StagedFile, error) {
	var files []*protocolv1.StagedFile
	for k, b := range batches {
		rel := fmt.Sprintf("%d/%d/%s/part-%05d.parquet", e.GetGeneration(), e.GetN(), stream, k*lanes+lane)
		var buf bytes.Buffer
		tbl := array.NewTableFromRecords(b.Schema(), []arrow.RecordBatch{b})
		err := pqarrow.WriteTable(tbl, &buf, max(b.NumRows(), 1), parquet.NewWriterProperties(), pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema()))
		tbl.Release()
		if err != nil {
			return nil, fmt.Errorf("stage %s: %w", rel, err)
		}
		p := filepath.Join(a.dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o750); err != nil {
			return nil, err
		}
		if err := os.WriteFile(p, buf.Bytes(), 0o600); err != nil {
			return nil, err
		}
		files = append(files, &protocolv1.StagedFile{Path: rel, Rows: ints.Uint64(b.NumRows()), Bytes: ints.Uint64(buf.Len())})
	}
	a.hand(files)
	return files, nil
}

func (a *stagingArea) hand(files []*protocolv1.StagedFile) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, f := range files {
		a.handed[f.GetPath()] = true
	}
}

func cursorKey(e *protocolv1.Epoch) string { return epochString(e) }

func (h *hostServer) area() (*stagingArea, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.staging == nil {
		return nil, rpcerr.Status(sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, errors.New("the destination wasn't handed staged epochs")))
	}
	return h.staging, nil
}

func (h *hostServer) StagedFileURL(_ context.Context, req *protocolv1.StagedFileURLRequest) (*protocolv1.StagedFileURLResponse, error) {
	a, err := h.area()
	if err != nil {
		return nil, err
	}
	p := req.GetPath()
	a.mu.Lock()
	handed := a.handed[p]
	a.mu.Unlock()
	if !handed {
		return nil, rpcerr.Status(sdk.WithCode(protocolv1.ErrorCode_ERROR_CODE_INTERNAL, fmt.Errorf("%q isn't a file of an epoch the destination was handed", p)))
	}
	expires := time.Now().Add(urlTTL)
	u := url.URL{Scheme: "http", Host: a.ln.Addr().String(), Path: "/" + p, RawQuery: url.Values{
		"expires": {strconv.FormatInt(expires.Unix(), 10)},
		"sig":     {a.sign(p, expires.Unix())},
	}.Encode()}
	return &protocolv1.StagedFileURLResponse{Url: u.String(), Expires: timestamppb.New(expires)}, nil
}

func (h *hostServer) PutDeliveryCursor(_ context.Context, req *protocolv1.PutDeliveryCursorRequest) (*protocolv1.PutDeliveryCursorResponse, error) {
	a, err := h.area()
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.cursors[cursorKey(req.GetEpoch())] = bytes.Clone(req.GetCursor())
	hook := a.onCursor
	a.mu.Unlock()
	if hook != nil {
		hook(req.GetEpoch())
	}
	return &protocolv1.PutDeliveryCursorResponse{}, nil
}

func (h *hostServer) GetDeliveryCursor(_ context.Context, req *protocolv1.GetDeliveryCursorRequest) (*protocolv1.GetDeliveryCursorResponse, error) {
	a, err := h.area()
	if err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.cursors[cursorKey(req.GetEpoch())]
	if !ok {
		return &protocolv1.GetDeliveryCursorResponse{}, nil
	}
	return &protocolv1.GetDeliveryCursorResponse{Cursor: bytes.Clone(c)}, nil
}
