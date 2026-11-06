package source_test

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"

	"github.com/openetl-io/openetl/sdk/go/arrowipc"
	"github.com/openetl-io/openetl/sdk/go/source"
)

// A changelog stream's declared control columns survive the wire, and come
// apart from its fields.
func TestSplitDeclared(t *testing.T) {
	data := arrow.NewSchema([]arrow.Field{{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	seq := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	b, err := arrowipc.EncodeSchema(arrow.NewSchema(append(data.Fields(), source.OpField(), source.SeqField(seq)), nil))
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := arrowipc.DecodeSchema(b)
	if err != nil {
		t.Fatal(err)
	}
	if got, gotSeq := source.SplitDeclared(decoded); !got.Equal(data) || !arrow.TypeEqual(gotSeq, seq) {
		t.Errorf("split into %v and seq %v", got, gotSeq)
	}
	if got, gotSeq := source.SplitDeclared(data); got != data || gotSeq != nil {
		t.Errorf("a schema without control columns split into %v and seq %v", got, gotSeq)
	}
	if got, gotSeq := source.SplitDeclared(arrow.NewSchema([]arrow.Field{source.OpField(), source.SeqField(seq)}, nil)); got != nil || !arrow.TypeEqual(gotSeq, seq) {
		t.Errorf("control columns alone split into %v and seq %v", got, gotSeq)
	}
}
