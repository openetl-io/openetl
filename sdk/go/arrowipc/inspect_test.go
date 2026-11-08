package arrowipc

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func batches(t testing.TB) map[string]arrow.RecordBatch {
	mem := memory.NewGoAllocator()
	out := map[string]arrow.RecordBatch{}

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "ref", Type: extensions.NewUUIDType(), Nullable: true},
	}, nil)
	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()
	for i := range 1000 {
		b.Field(0).(*array.Int64Builder).Append(int64(i))
		if i%3 == 0 {
			b.Field(1).AppendNull()
			b.Field(2).AppendNull()
		} else {
			b.Field(1).(*array.StringBuilder).Append("row")
			b.Field(2).(*extensions.UUIDBuilder).AppendBytes([16]byte{byte(i)})
		}
	}
	out["nulls"] = b.NewRecordBatch()
	b.Field(0).(*array.Int64Builder).Append(1)
	b.Field(1).(*array.StringBuilder).Append("x")
	b.Field(2).(*extensions.UUIDBuilder).AppendBytes([16]byte{1})
	out["no nulls"] = b.NewRecordBatch()
	out["empty"] = b.NewRecordBatch()

	dt := &arrow.DictionaryType{IndexType: arrow.PrimitiveTypes.Int32, ValueType: arrow.BinaryTypes.String}
	db := array.NewDictionaryBuilder(mem, dt).(*array.BinaryDictionaryBuilder)
	defer db.Release()
	for _, s := range []string{"a", "b", "a", "c", "b"} {
		if err := db.AppendString(s); err != nil {
			t.Fatal(err)
		}
	}
	col := db.NewArray()
	defer col.Release()
	out["dictionary"] = array.NewRecordBatch(arrow.NewSchema([]arrow.Field{{Name: "d", Type: dt}}, nil), []arrow.Array{col}, int64(col.Len()))
	return out
}

func TestInspect(t *testing.T) {
	for name, rec := range batches(t) {
		t.Run(name, func(t *testing.T) {
			defer rec.Release()
			b, err := EncodeBatch(rec)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Inspect(b)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := DecodeBatch(b, memory.DefaultAllocator)
			if err != nil {
				t.Fatal(err)
			}
			defer decoded.Release()
			if got.Rows != rec.NumRows() {
				t.Errorf("rows: %d, want %d", got.Rows, rec.NumRows())
			}
			// As util.TotalRecordSize counts it, which needs a dependency
			// the SDK doesn't have.
			want := int64(0)
			for _, col := range decoded.Columns() {
				want += int64(size(col.Data()))
			}
			if got.Size != want {
				t.Errorf("size: %d, want %d", got.Size, want)
			}
			if !bytes.Equal(got.Bytes, b) {
				t.Error("Bytes isn't the stream")
			}
		})
	}
}

func TestInspectRejects(t *testing.T) {
	recs := batches(t)
	rec := recs["nulls"]
	for _, r := range recs {
		defer r.Release()
	}
	one, err := EncodeBatch(rec)
	if err != nil {
		t.Fatal(err)
	}
	schemaOnly, err := EncodeSchema(rec.Schema())
	if err != nil {
		t.Fatal(err)
	}
	var two bytes.Buffer
	w := ipc.NewWriter(&two, ipc.WithSchema(rec.Schema()))
	for range 2 {
		if err := w.Write(rec); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	// The batch's message without the schema's.
	headless := one[len(schemaOnly)-8:]

	for name, b := range map[string][]byte{
		"empty":                 nil,
		"only a schema":         schemaOnly,
		"two batches":           two.Bytes(),
		"no schema":             headless,
		"bytes after the end":   append(append([]byte{}, one...), 0),
		"garbage":               []byte("not an arrow stream at all"),
		"a huge metadata size":  {0xFF, 0xFF, 0xFF, 0xFF, 0xF0, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0},
		"a header out of range": {0xFF, 0xFF, 0xFF, 0xFF, 8, 0, 0, 0, 0xF0, 0xFF, 0xFF, 0x7F, 0, 0, 0, 0},
	} {
		if _, err := Inspect(b); err == nil {
			t.Errorf("%s: no error", name)
		}
	}

	// The end-of-stream marker is optional.
	eos := len(one) - 8
	for n := range len(one) {
		_, err := Inspect(one[:n])
		if ok := n == eos; ok != (err == nil) {
			t.Errorf("the first %d of %d bytes: %v", n, len(one), err)
		}
	}
}

func TestSignedReads(t *testing.T) {
	for _, v := range []int64{0, 1, -1, math.MaxInt32, math.MinInt32, 1 << 32, -1 << 32, math.MaxInt64, math.MinInt64} {
		got, err := fb(binary.LittleEndian.AppendUint64(nil, uint64(v))).i64(0)
		if err != nil || got != v {
			t.Errorf("i64 of %d: %d, %v", v, got, err)
		}
	}
	for _, v := range []int32{0, 1, -1, math.MaxInt32, math.MinInt32} {
		if got := signed(uint32(v)); got != int64(v) {
			t.Errorf("signed of %d: %d", v, got)
		}
	}
}

func FuzzInspect(f *testing.F) {
	for _, rec := range batches(f) {
		b, err := EncodeBatch(rec)
		rec.Release()
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
	f.Fuzz(func(t *testing.T, b []byte) {
		got, err := Inspect(b)
		if err == nil && (got.Rows < 0 || got.Size < 0) {
			t.Errorf("rows %d, size %d", got.Rows, got.Size)
		}
	})
}
