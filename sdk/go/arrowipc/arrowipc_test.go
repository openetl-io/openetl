package arrowipc

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// 100,000 rows, as in source-postgres's chunks.
func BenchmarkEncodeBatch(b *testing.B) {
	mem := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "n", Type: arrow.PrimitiveTypes.Int32},
		{Name: "s", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)
	rb := array.NewRecordBuilder(mem, schema)
	defer rb.Release()
	for i := range 100_000 {
		rb.Field(0).(*array.Int64Builder).Append(int64(i))
		rb.Field(1).(*array.Int32Builder).Append(int32(i))
		rb.Field(2).(*array.StringBuilder).Append("0123456789abcdef0123456789abcdef")
	}
	rec := rb.NewRecordBatch()
	defer rec.Release()
	n := 0
	for _, col := range rec.Columns() {
		n += size(col.Data())
	}
	b.SetBytes(int64(n))
	for b.Loop() {
		if _, err := EncodeBatch(rec); err != nil {
			b.Fatal(err)
		}
	}
}
