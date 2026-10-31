package runner

import (
	"bytes"
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/ipc"

	"github.com/unsuitable001/openetl/sdk/go/destination"
)

type applier struct {
	destination.Writer
	calls [][]string
}

func (a *applier) ApplySchema(_ context.Context, streams []destination.StreamSchema) error {
	var names []string
	for _, s := range streams {
		names = append(names, s.Name)
	}
	a.calls = append(a.calls, names)
	return nil
}

func encode(t *testing.T, s *arrow.Schema) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := ipc.NewWriter(&buf, ipc.WithSchema(s))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestApplySchemasOnlyWhenTheyChange(t *testing.T) {
	x := arrow.Field{Name: "x", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	md := arrow.NewMetadata([]string{"openetl.table", "openetl.source_names"}, []string{"a", "{}"})
	reordered := arrow.NewMetadata([]string{"openetl.source_names", "openetl.table"}, []string{"{}", "a"})
	a := &applier{}
	applied := map[string]*arrow.Schema{}
	steps := []map[string][]byte{
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x}, &md)), "b": nil},
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x}, &reordered))},
		{"a": encode(t, arrow.NewSchema([]arrow.Field{x, {Name: "y", Type: arrow.BinaryTypes.String}}, &md))},
	}
	for _, s := range steps {
		if err := applySchemas(t.Context(), a, applied, s); err != nil {
			t.Fatal(err)
		}
	}
	if len(a.calls) != 2 || a.calls[0][0] != "a" || a.calls[1][0] != "a" || len(a.calls[0]) != 1 {
		t.Errorf("ApplySchema calls %v, want [[a] [a]]", a.calls)
	}
}
