package staging_test

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"

	"github.com/unsuitable001/openetl/sdk/go/destination"
	"github.com/unsuitable001/openetl/sdk/go/host"
	"github.com/unsuitable001/openetl/sdk/go/staging"
)

// urlHost signs URLs with a counter; the store refuses the first one, as it
// does one that expired.
type urlHost struct {
	host.Host
	base   string
	signed atomic.Int32
}

func (h *urlHost) StagedFileURL(context.Context, string) (string, time.Time, error) {
	n := h.signed.Add(1)
	return h.base + "/f?n=" + strconv.Itoa(int(n)), time.Now().Add(time.Hour), nil
}

func TestReadsFromAPositionThroughFreshURLs(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64}}, nil)
	b := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	for i := range int64(10) {
		b.Field(0).(*array.Int64Builder).Append(i)
	}
	rec := b.NewRecordBatch()
	b.Release()
	tbl := array.NewTableFromRecords(schema, []arrow.RecordBatch{rec})
	rec.Release()
	var file bytes.Buffer
	// Row groups of 4 rows: 0–3, 4–7, 8–9.
	if err := pqarrow.WriteTable(tbl, &file, 4, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps()); err != nil {
		t.Fatal(err)
	}
	tbl.Release()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("n") == "1" {
			http.Error(w, "expired", http.StatusForbidden)
			return
		}
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(file.Bytes()))
	}))
	defer srv.Close()
	h := &urlHost{base: srv.URL}

	type at struct {
		pos staging.Position
		ids []int64
	}
	var got []at
	f := destination.StagedFile{Path: "1/2/s/part-00000.parquet", Rows: 10, Bytes: uint64(file.Len())}
	err := staging.Read(t.Context(), h, f, staging.Position{RowGroup: 1, Row: 1}, func(p staging.Position, b arrow.RecordBatch) error {
		got = append(got, at{p, append([]int64(nil), b.Column(0).(*array.Int64).Int64Values()...)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []at{{staging.Position{RowGroup: 1, Row: 1}, []int64{5, 6, 7}}, {staging.Position{RowGroup: 2}, []int64{8, 9}}}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i].pos != want[i].pos || !slices.Equal(got[i].ids, want[i].ids) {
			t.Fatalf("batch %d: got %v, want %v", i, got[i], want[i])
		}
	}
	if h.signed.Load() < 2 {
		t.Fatalf("signed %d URLs; the refused one wasn't replaced", h.signed.Load())
	}
}
